mod common;

use std::time::{Duration, Instant};

const BLOCK_SIZE: usize = scuttlecast::DEFAULT_BLOCK_SIZE as usize;
use tokio::task::JoinHandle;

fn spawn_receiver(group_id: u8, port: u16, path: std::path::PathBuf) -> JoinHandle<()> {
    let receiver = common::receiver(common::group(group_id), port);
    tokio::spawn(async move {
        receiver.recv_file(path).await.expect("receive");
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_receivers_get_the_whole_payload() {
    let sent = common::payload(5 * BLOCK_SIZE);
    let output = common::Output::new();
    let first = output.path("first.bin");
    let second = output.path("second.bin");

    let receiving_first = spawn_receiver(20, 13000, first.clone());
    let receiving_second = spawn_receiver(20, 13000, second.clone());

    common::sender(common::group(20), 13000, 2)
        .send_stream(common::source(&sent))
        .await
        .expect("send");

    receiving_first.await.expect("first");
    receiving_second.await.expect("second");

    assert_eq!(std::fs::read(&first).expect("read first"), sent);
    assert_eq!(std::fs::read(&second).expect("read second"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sender_waits_until_min_receivers_joined() {
    let sent = common::payload(2 * BLOCK_SIZE);
    let output = common::Output::new();
    let first = output.path("first.bin");
    let second = output.path("second.bin");

    let receiving_first = spawn_receiver(21, 13010, first.clone());

    let delay = Duration::from_millis(600);
    let late = {
        let second = second.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            spawn_receiver(21, 13010, second).await.expect("late");
        })
    };

    let started = Instant::now();
    common::sender(common::group(21), 13010, 2)
        .send_stream(common::source(&sent))
        .await
        .expect("send");
    let waited = started.elapsed();

    receiving_first.await.expect("first");
    late.await.expect("late join");

    assert!(
        waited >= delay,
        "sender should have waited for the second receiver, waited {waited:?}"
    );
    assert_eq!(std::fs::read(&second).expect("read second"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sender_proceeds_when_min_receivers_never_arrive() {
    let sent = common::payload(3 * BLOCK_SIZE);
    let output = common::Output::new();
    let path = output.path("only.bin");

    let receiving = spawn_receiver(22, 13020, path.clone());

    common::sender_with(common::group(22), 13020, 5, Duration::from_millis(500))
        .send_stream(common::source(&sent))
        .await
        .expect("send");

    receiving.await.expect("receive");
    assert_eq!(std::fs::read(&path).expect("read"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_receivers_get_identical_payloads() {
    let sent = common::payload(10 * BLOCK_SIZE);
    let output = common::Output::new();
    let paths: Vec<_> = (0..3)
        .map(|index| output.path(&format!("receiver{index}.bin")))
        .collect();

    let receiving: Vec<_> = paths
        .iter()
        .map(|path| spawn_receiver(23, 13030, path.clone()))
        .collect();

    common::sender(common::group(23), 13030, 3)
        .send_stream(common::source(&sent))
        .await
        .expect("send");

    for handle in receiving {
        handle.await.expect("receive");
    }

    for path in &paths {
        assert_eq!(std::fs::read(path).expect("read"), sent);
    }
}

/// No receiver can have joined before the first hello goes out
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sender_with_no_stated_minimum_waits_for_a_receiver() {
    let sent = common::payload(5 * BLOCK_SIZE);
    let output = common::Output::new();
    let path = output.path("late.bin");

    let sending = tokio::spawn({
        let sender = scuttlecast::sender::Sender::builder()
            .socket(common::LOCAL, common::group(24), 13040)
            .expect("bind sender")
            .max_wait(common::MAX_WAIT)
            .build();
        let sent = sent.clone();
        async move { sender.send_stream(common::source(&sent)).await }
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    spawn_receiver(24, 13040, path.clone())
        .await
        .expect("receive");

    sending.await.expect("join").expect("send");
    assert_eq!(std::fs::read(&path).expect("read"), sent);
}
