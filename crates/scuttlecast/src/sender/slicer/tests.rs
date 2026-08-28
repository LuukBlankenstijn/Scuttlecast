use super::Slicer;
use crate::sender::slicer::channel::{Feedback, Outbound};
use crate::{BLOCK_SIZE, error::ProtoError};
use std::num::{NonZeroU16, NonZeroUsize};
use tokio::sync::mpsc;

fn slicer(blocks_per_slice: u16) -> Slicer {
    Slicer::new(
        NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
        NonZeroUsize::new(32).expect("max live slices"),
    )
}

async fn run(input: &[u8], blocks_per_slice: u16) -> Vec<Outbound> {
    let (outbound, mut collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::channel(4);

    let collecting = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(message) = collected.recv().await {
            let end_of_input = matches!(message, Outbound::Eof { .. });
            seen.push(message);
            if end_of_input {
                feedback.send(Feedback::Done).await.expect("done");
            }
        }
        seen
    });

    slicer(blocks_per_slice)
        .run(input, outbound, feedback_rx)
        .await
        .expect("run");

    collecting.await.expect("collect")
}

/// Feeds the requests once the whole input has been sent, then collects the
/// coordinates of everything the slicer sends afterwards
async fn resent_blocks(
    input: &[u8],
    blocks_per_slice: u16,
    mut requests: Vec<Feedback>,
) -> Vec<(u32, u16)> {
    let (outbound, mut collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::channel(8);

    let collecting = tokio::spawn(async move {
        let mut resent = Vec::new();
        let mut past_eof = false;

        while let Some(message) = collected.recv().await {
            match message {
                Outbound::Eof { .. } => {
                    for request in requests.drain(..) {
                        feedback.send(request).await.expect("request");
                    }
                    past_eof = true;
                    feedback.send(Feedback::Done).await.expect("done");
                }
                Outbound::Block {
                    slice_no,
                    block_in_slice,
                    ..
                } if past_eof => resent.push((slice_no, block_in_slice)),
                Outbound::Block { .. } => {}
            }
        }
        resent
    });

    slicer(blocks_per_slice)
        .run(input, outbound, feedback_rx)
        .await
        .expect("run");

    collecting.await.expect("collect")
}

fn totals(messages: &[Outbound]) -> (u64, u64) {
    match messages.last() {
        Some(Outbound::Eof {
            total_bytes,
            total_blocks,
        }) => (*total_bytes, *total_blocks),
        _ => panic!("expected the last message to be Eof"),
    }
}

fn coordinates(messages: &[Outbound]) -> Vec<(u32, u16)> {
    messages
        .iter()
        .filter_map(|message| match message {
            Outbound::Block {
                slice_no,
                block_in_slice,
                ..
            } => Some((*slice_no, *block_in_slice)),
            Outbound::Eof { .. } => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_when_the_last_slice_lands_exactly_full() {
    let messages = run(&vec![7u8; 4 * BLOCK_SIZE], 2).await;

    assert_eq!(totals(&messages), (4 * BLOCK_SIZE as u64, 4));
    assert_eq!(coordinates(&messages), vec![(0, 0), (0, 1), (1, 0), (1, 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_for_a_partial_final_slice() {
    let messages = run(&vec![7u8; 3 * BLOCK_SIZE + 17], 2).await;

    assert_eq!(totals(&messages), (3 * BLOCK_SIZE as u64 + 17, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_for_empty_input() {
    let messages = run(&[], 2).await;

    assert!(coordinates(&messages).is_empty());
    assert_eq!(totals(&messages), (0, 0));
}

#[tokio::test]
async fn a_closed_egress_stops_the_slicer() {
    let (outbound, collected) = mpsc::channel(4);
    let (_feedback, feedback_rx) = mpsc::channel::<Feedback>(4);
    drop(collected);

    let outcome = slicer(2)
        .run(&vec![7u8; BLOCK_SIZE][..], outbound, feedback_rx)
        .await;

    assert!(
        matches!(outcome, Err(ProtoError::EgressClosed)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_closed_feedback_channel_stops_the_slicer() {
    let (outbound, _collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::channel::<Feedback>(4);
    drop(feedback);

    let outcome = slicer(2).run(&[][..], outbound, feedback_rx).await;

    assert!(
        matches!(outcome, Err(ProtoError::EgressClosed)),
        "{outcome:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resends_requested_blocks_and_ignores_the_rest() {
    let resent = resent_blocks(
        &vec![7u8; 2 * BLOCK_SIZE],
        2,
        vec![Feedback::Resend {
            slice_no: 0,
            blocks: vec![1, 9],
        }],
    )
    .await;

    assert_eq!(resent, vec![(0, 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resends_blocks_of_a_short_final_slice() {
    let resent = resent_blocks(
        &vec![7u8; 3 * BLOCK_SIZE],
        2,
        vec![Feedback::Resend {
            slice_no: 1,
            blocks: vec![0],
        }],
    )
    .await;

    assert_eq!(resent, vec![(1, 0)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_completed_slice_is_no_longer_resent() {
    let resent = resent_blocks(
        &vec![7u8; 2 * BLOCK_SIZE],
        2,
        vec![
            Feedback::Completed(0),
            Feedback::Resend {
                slice_no: 0,
                blocks: vec![0],
            },
        ],
    )
    .await;

    assert!(resent.is_empty(), "{resent:?}");
}
