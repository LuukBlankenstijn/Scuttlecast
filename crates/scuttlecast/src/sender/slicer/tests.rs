use super::Slicer;
use crate::sender::slicer::channel::{Feedback, Outbound};
use crate::{BLOCK_SIZE, error::ProtoError};
use std::num::{NonZeroU16, NonZeroUsize};
use tokio::sync::mpsc;

fn slicer(shards_per_slice: u16) -> Slicer {
    Slicer::new(
        NonZeroU16::new(shards_per_slice).expect("shards per slice"),
        NonZeroUsize::new(32).expect("max live slices"),
    )
}

async fn run(input: &[u8], shards_per_slice: u16) -> Vec<Outbound> {
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

    slicer(shards_per_slice)
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

fn coordinates(messages: &[Outbound]) -> Vec<(u32, u16, Option<u32>)> {
    messages
        .iter()
        .filter_map(|message| match message {
            Outbound::Block {
                slice_no,
                shard_in_slice,
                sealed_through,
                ..
            } => Some((*slice_no, *shard_in_slice, *sealed_through)),
            Outbound::Eof { .. } => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_when_the_last_slice_lands_exactly_full() {
    let messages = run(&vec![7u8; 4 * BLOCK_SIZE], 2).await;

    assert_eq!(totals(&messages), (4 * BLOCK_SIZE as u64, 4));
    assert_eq!(
        coordinates(&messages),
        vec![(0, 0, None), (0, 1, None), (1, 0, Some(0)), (1, 1, Some(0)),]
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_final_slice_is_never_sealed() {
    let messages = run(&vec![7u8; 3 * BLOCK_SIZE], 2).await;

    assert_eq!(
        coordinates(&messages).last().copied(),
        Some((1, 0, Some(0)))
    );
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
async fn resends_retained_shards() {
    let (outbound, mut collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::channel(4);

    let requesting = tokio::spawn(async move {
        let mut resent = Vec::new();
        let mut requested = false;

        while let Some(message) = collected.recv().await {
            match message {
                Outbound::Eof { .. } => {
                    feedback
                        .send(Feedback::Resend {
                            slice_no: 0,
                            shards: vec![1, 9],
                        })
                        .await
                        .expect("resend");
                    requested = true;
                }
                Outbound::Block {
                    slice_no,
                    shard_in_slice,
                    ..
                } if requested => {
                    resent.push((slice_no, shard_in_slice));
                    feedback.send(Feedback::Done).await.expect("done");
                }
                Outbound::Block { .. } => {}
            }
        }
        resent
    });

    slicer(2)
        .run(&vec![7u8; 2 * BLOCK_SIZE][..], outbound, feedback_rx)
        .await
        .expect("run");

    assert_eq!(requesting.await.expect("collect"), vec![(0, 1)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_watermark_drops_retained_slices() {
    let (outbound, mut collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::channel(4);

    let requesting = tokio::spawn(async move {
        let mut resent_after_watermark = 0;
        let mut watermarked = false;

        while let Some(message) = collected.recv().await {
            match message {
                Outbound::Eof { .. } => {
                    feedback
                        .send(Feedback::Watermark(1))
                        .await
                        .expect("watermark");
                    feedback
                        .send(Feedback::Resend {
                            slice_no: 0,
                            shards: vec![0],
                        })
                        .await
                        .expect("resend");
                    feedback.send(Feedback::Done).await.expect("done");
                    watermarked = true;
                }
                Outbound::Block { .. } if watermarked => resent_after_watermark += 1,
                Outbound::Block { .. } => {}
            }
        }
        resent_after_watermark
    });

    slicer(2)
        .run(&vec![7u8; 2 * BLOCK_SIZE][..], outbound, feedback_rx)
        .await
        .expect("run");

    assert_eq!(requesting.await.expect("collect"), 0);
}
