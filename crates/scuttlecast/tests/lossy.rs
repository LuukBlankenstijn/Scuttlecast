mod common;

use std::collections::HashMap;
use std::sync::Mutex;

use scuttlecast::BLOCK_SIZE;
use scuttlecast::Losing;
use scuttlecast::proto::Message;

const BLOCKS_PER_SLICE: u64 = 32;

fn every_nth_block(step: u64, offset: u64) -> Losing {
    Losing::every(move |message| match message {
        Message::Data(data) => data.seq % step == offset,
        _ => false,
    })
}

/// Swallows each block of a slice a fixed number of times, so recovery needs
/// as many repairs. Blocks are keyed by position rather than by transmission,
/// which is what lets a later resend of the same block through.
fn slice_swallowed(slice_no: u32, times: usize) -> Losing {
    let swallowed: Mutex<HashMap<u16, usize>> = Mutex::new(HashMap::new());

    Losing::every(move |message| match message {
        Message::Data(data) if data.slice_no == slice_no => {
            let mut swallowed = swallowed.lock().expect("swallowed blocks");
            let seen = swallowed.entry(data.block_in_slice).or_default();
            *seen += 1;
            *seen <= times
        }
        _ => false,
    })
}

fn first_few(count: usize, rule: impl Fn(&Message) -> bool + Send + Sync + 'static) -> Losing {
    let seen = Mutex::new(0usize);

    Losing::every(move |message| {
        if !rule(message) {
            return false;
        }
        let mut seen = seen.lock().expect("seen count");
        *seen += 1;
        *seen <= count
    })
}

async fn transfer_with_loss(
    group_id: u8,
    port: u16,
    bytes: &[u8],
    losing: Vec<Losing>,
) -> Vec<Vec<u8>> {
    let group = common::group(group_id);
    let output = common::Output::new();

    let receiving: Vec<_> = losing
        .into_iter()
        .enumerate()
        .map(|(index, losing)| {
            let path = output.path(&format!("receiver{index}.bin"));
            let receiver = common::receiver(group, port).losing(losing);
            let read_back = path.clone();
            (
                tokio::spawn(async move { receiver.recv_file(path).await }),
                read_back,
            )
        })
        .collect();

    common::sender(group, port, receiving.len())
        .send_stream(common::source(bytes))
        .await
        .expect("send");

    let mut received = Vec::new();
    for (handle, path) in receiving {
        handle.await.expect("join").expect("receive");
        received.push(std::fs::read(&path).expect("read output"));
    }
    received
}

/// Every parity count the sender published while the transfer ran
async fn parity_over_transfer(group_id: u8, port: u16, bytes: &[u8], losing: Losing) -> Vec<u16> {
    let group = common::group(group_id);
    let output = common::Output::new();
    let path = output.path("receiver.bin");

    let receiver = common::receiver(group, port).losing(losing);
    let receiving = tokio::spawn(async move { receiver.recv_file(path).await });

    let sender = common::sender(group, port, 1);
    let mut progress = sender.progress();
    let watching = tokio::spawn(async move {
        let mut seen = Vec::new();
        while progress.changed().await.is_ok() {
            seen.push(progress.borrow_and_update().parity_shards);
        }
        seen
    });

    sender
        .send_stream(common::source(bytes))
        .await
        .expect("send");
    receiving.await.expect("join").expect("receive");
    drop(sender);

    watching.await.expect("watch")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repairs_a_receiver_losing_a_fifth_of_the_blocks() {
    let sent = common::payload(3 * BLOCKS_PER_SLICE as usize * BLOCK_SIZE);

    let received = transfer_with_loss(50, 16000, &sent, vec![every_nth_block(5, 0)]).await;

    assert_eq!(received, vec![sent]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repairs_three_receivers_losing_different_blocks() {
    let sent = common::payload(2 * BLOCKS_PER_SLICE as usize * BLOCK_SIZE + 11);

    let received = transfer_with_loss(
        51,
        16010,
        &sent,
        vec![
            every_nth_block(4, 0),
            every_nth_block(4, 1),
            every_nth_block(4, 2),
        ],
    )
    .await;

    assert_eq!(received, vec![sent.clone(), sent.clone(), sent]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repairs_the_tail_when_the_repair_is_lost_too() {
    let sent = common::payload(2 * BLOCKS_PER_SLICE as usize * BLOCK_SIZE + 7);

    let received = transfer_with_loss(52, 16020, &sent, vec![slice_swallowed(2, 2)]).await;

    assert_eq!(received, vec![sent]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finishes_when_the_first_announcements_of_the_end_are_lost() {
    let sent = common::payload(BLOCKS_PER_SLICE as usize * BLOCK_SIZE + 3);

    let received = transfer_with_loss(
        53,
        16030,
        &sent,
        vec![first_few(3, |message| matches!(message, Message::Done(_)))],
    )
    .await;

    assert_eq!(received, vec![sent]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repairs_a_gap_that_stalled_the_senders_window() {
    let sent = common::payload(6 * BLOCKS_PER_SLICE as usize * BLOCK_SIZE);
    let group = common::group(55);
    let port = 16050;
    let output = common::Output::new();
    let path = output.path("stalled.bin");

    let receiver = common::receiver(group, port).losing(slice_swallowed(0, 2));
    let read_back = path.clone();
    let receiving = tokio::spawn(async move { receiver.recv_file(path).await });

    common::sender_windowed(group, port, 1, 2)
        .send_stream(common::source(&sent))
        .await
        .expect("send");

    receiving.await.expect("join").expect("receive");

    assert_eq!(std::fs::read(&read_back).expect("read output"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serves_one_repair_for_a_loss_every_receiver_suffered() {
    let sent = common::payload(2 * BLOCKS_PER_SLICE as usize * BLOCK_SIZE);
    let group = common::group(54);
    let port = 16040;
    let output = common::Output::new();

    let receiving: Vec<_> = (0..3)
        .map(|index| {
            let path = output.path(&format!("receiver{index}.bin"));
            let receiver = common::receiver(group, port).losing(every_nth_block(8, 0));
            tokio::spawn(async move { receiver.recv_file(path).await })
        })
        .collect();

    common::sender_without_parity(group, port, 3)
        .send_stream(common::source(&sent))
        .await
        .expect("send");

    for handle in receiving {
        let summary = handle.await.expect("join").expect("receive");

        assert_eq!(
            summary.duplicates, 0,
            "the same loss was repaired more than once: {summary:?}"
        );
        assert!(summary.naks_sent > 0, "nothing was repaired: {summary:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovers_from_parity_without_asking_for_anything() {
    let sent = common::payload(3 * BLOCKS_PER_SLICE as usize * BLOCK_SIZE);
    let group = common::group(56);
    let port = 16060;
    let output = common::Output::new();

    let receiving: Vec<_> = (0..3)
        .map(|index| {
            let path = output.path(&format!("receiver{index}.bin"));
            let receiver = common::receiver(group, port).losing(every_nth_block(8, index));
            let read_back = path.clone();
            (
                tokio::spawn(async move { receiver.recv_file(path).await }),
                read_back,
            )
        })
        .collect();

    common::sender(group, port, 3)
        .send_stream(common::source(&sent))
        .await
        .expect("send");

    for (handle, path) in receiving {
        let summary = handle.await.expect("join").expect("receive");

        assert_eq!(
            summary.naks_sent, 0,
            "parity should have covered the loss: {summary:?}"
        );
        assert!(summary.loss() > 0.0, "nothing was lost: {summary:?}");
        assert_eq!(std::fs::read(&path).expect("read output"), sent);
    }
}

/// Long enough for a receiver to report a loss rate at all
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clean_link_stops_carrying_parity() {
    let sent = common::payload(600 * BLOCK_SIZE);

    let observed = parity_over_transfer(56, 16080, &sent, Losing::default()).await;

    assert_eq!(
        observed.last().copied(),
        Some(0),
        "coverage never fell away: {observed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lossy_link_keeps_carrying_parity() {
    let sent = common::payload(600 * BLOCK_SIZE);

    let observed = parity_over_transfer(57, 16090, &sent, every_nth_block(10, 3)).await;

    let settled = observed.last().copied().expect("a published count");
    assert!(
        settled >= 4,
        "a tenth of the blocks lost should hold coverage up: {observed:?}"
    );
}
