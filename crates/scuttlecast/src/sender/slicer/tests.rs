use super::Slicer;
use crate::error::ProtoError;
use crate::sender::slicer::channel::{Feedback, Outbound};
use std::io::Cursor;
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use tokio::sync::mpsc;

const BLOCK_SIZE: usize = crate::DEFAULT_BLOCK_SIZE as usize;

#[derive(Debug, PartialEq)]
enum Resent {
    Block(u32, u16),
    Parity(u32, u16),
}

fn slicer(blocks_per_slice: u16) -> Slicer {
    fec_slicer(blocks_per_slice, 0)
}

fn fec_slicer(blocks_per_slice: u16, parity_per_slice: u16) -> Slicer {
    Slicer::new(
        NonZeroU32::new(crate::DEFAULT_BLOCK_SIZE).expect("block size"),
        NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
        parity_per_slice,
        NonZeroUsize::new(32).expect("max live slices"),
    )
    .expect("slicer")
}

async fn drive(slicer: Slicer, input: &[u8]) -> Vec<Outbound> {
    let (outbound, mut collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::unbounded_channel();

    let collecting = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(message) = collected.recv().await {
            let end_of_input = matches!(message, Outbound::Eof { .. });
            seen.push(message);
            if end_of_input {
                feedback.send(Feedback::Done).expect("done");
            }
        }
        seen
    });

    slicer
        .run(Cursor::new(input.to_vec()), outbound, feedback_rx)
        .await
        .expect("run");

    collecting.await.expect("collect")
}

async fn run(input: &[u8], blocks_per_slice: u16) -> Vec<Outbound> {
    drive(slicer(blocks_per_slice), input).await
}

async fn fec_run(input: &[u8], blocks_per_slice: u16, parity_per_slice: u16) -> Vec<Outbound> {
    drive(fec_slicer(blocks_per_slice, parity_per_slice), input).await
}

/// Feeds the requests once the whole input has been sent, then collects the
/// shards the slicer sends afterwards
async fn resent_shards(
    input: &[u8],
    blocks_per_slice: u16,
    parity_per_slice: u16,
    mut requests: Vec<Feedback>,
) -> Vec<Resent> {
    let (outbound, mut collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::unbounded_channel();

    let collecting = tokio::spawn(async move {
        let mut resent = Vec::new();
        let mut past_eof = false;

        while let Some(message) = collected.recv().await {
            match message {
                Outbound::Eof { .. } => {
                    for request in requests.drain(..) {
                        feedback.send(request).expect("request");
                    }
                    past_eof = true;
                    feedback.send(Feedback::Done).expect("done");
                }
                Outbound::Shard { slice_no, slot, .. } if past_eof => {
                    if slot < blocks_per_slice {
                        resent.push(Resent::Block(slice_no, slot));
                    } else {
                        resent.push(Resent::Parity(slice_no, slot - blocks_per_slice));
                    }
                }
                _ => {}
            }
        }
        resent
    });

    fec_slicer(blocks_per_slice, parity_per_slice)
        .run(Cursor::new(input.to_vec()), outbound, feedback_rx)
        .await
        .expect("run");

    collecting.await.expect("collect")
}

async fn resent_blocks(
    input: &[u8],
    blocks_per_slice: u16,
    requests: Vec<Feedback>,
) -> Vec<(u32, u16)> {
    resent_shards(input, blocks_per_slice, 0, requests)
        .await
        .into_iter()
        .map(|shard| match shard {
            Resent::Block(slice_no, block_in_slice) => (slice_no, block_in_slice),
            Resent::Parity(slice_no, parity_index) => (slice_no, parity_index),
        })
        .collect()
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

fn coordinates(messages: &[Outbound], blocks_per_slice: u16) -> Vec<(u32, u16)> {
    messages
        .iter()
        .filter_map(|message| match message {
            Outbound::Shard { slice_no, slot, .. } if *slot < blocks_per_slice => {
                Some((*slice_no, *slot))
            }
            _ => None,
        })
        .collect()
}

fn parity(messages: &[Outbound], blocks_per_slice: u16) -> Vec<(u32, u16)> {
    messages
        .iter()
        .filter_map(|message| match message {
            Outbound::Shard { slice_no, slot, .. } if *slot >= blocks_per_slice => {
                Some((*slice_no, *slot - blocks_per_slice))
            }
            _ => None,
        })
        .collect()
}

fn floor_of(messages: &[Outbound], slice: u32, slot: u16) -> u32 {
    messages
        .iter()
        .find_map(|message| match message {
            Outbound::Shard {
                slice_no,
                slot: at,
                emit_floor,
                ..
            } if *slice_no == slice && *at == slot => Some(*emit_floor),
            _ => None,
        })
        .expect("shard present")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_when_the_last_slice_lands_exactly_full() {
    let messages = run(&vec![7u8; 4 * BLOCK_SIZE], 2).await;

    assert_eq!(totals(&messages), (4 * BLOCK_SIZE as u64, 4));
    assert_eq!(
        coordinates(&messages, 2),
        vec![(0, 0), (0, 1), (1, 0), (1, 1)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_for_a_partial_final_slice() {
    let messages = run(&vec![7u8; 3 * BLOCK_SIZE + 17], 2).await;

    assert_eq!(totals(&messages), (3 * BLOCK_SIZE as u64 + 17, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_final_block_goes_out_padded_to_full_width() {
    let messages = run(&vec![7u8; 3 * BLOCK_SIZE + 17], 2).await;

    let widths: Vec<usize> = messages
        .iter()
        .filter_map(|message| match message {
            Outbound::Shard { payload, .. } => Some(payload.len()),
            Outbound::Eof { .. } => None,
        })
        .collect();

    assert_eq!(widths, vec![BLOCK_SIZE; 4]);
    assert_eq!(totals(&messages), (3 * BLOCK_SIZE as u64 + 17, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_eof_for_empty_input() {
    let messages = run(&[], 2).await;

    assert!(coordinates(&messages, 2).is_empty());
    assert_eq!(totals(&messages), (0, 0));
}

#[tokio::test]
async fn a_closed_egress_stops_the_slicer() {
    let (outbound, collected) = mpsc::channel(4);
    let (_feedback, feedback_rx) = mpsc::unbounded_channel::<Feedback>();
    drop(collected);

    let outcome = slicer(2)
        .run(Cursor::new(vec![7u8; BLOCK_SIZE]), outbound, feedback_rx)
        .await;

    assert!(
        matches!(outcome, Err(ProtoError::EgressClosed)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_closed_feedback_channel_stops_the_slicer() {
    let (outbound, _collected) = mpsc::channel(256);
    let (feedback, feedback_rx) = mpsc::unbounded_channel::<Feedback>();
    drop(feedback);

    let outcome = slicer(2)
        .run(Cursor::new(Vec::<u8>::new()), outbound, feedback_rx)
        .await;

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
async fn a_slice_nobody_needs_is_no_longer_resent() {
    let resent = resent_blocks(
        &vec![7u8; 2 * BLOCK_SIZE],
        2,
        vec![
            Feedback::Needed(1),
            Feedback::Resend {
                slice_no: 0,
                blocks: vec![0],
            },
        ],
    )
    .await;

    assert!(resent.is_empty(), "{resent:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queues_a_parity_shard_per_configured_shard_after_a_slice() {
    let messages = fec_run(&vec![7u8; 2 * BLOCK_SIZE], 2, 3).await;

    assert_eq!(parity(&messages, 2), vec![(0, 0), (0, 1), (0, 2)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_parity_transfer_queues_no_parity() {
    let messages = fec_run(&vec![7u8; 2 * BLOCK_SIZE], 2, 0).await;

    assert!(parity(&messages, 2).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_emit_floor_advances_only_after_a_slices_parity() {
    let messages = fec_run(&vec![7u8; 3 * BLOCK_SIZE], 2, 1).await;

    assert_eq!(floor_of(&messages, 0, 1), 0);
    assert_eq!(floor_of(&messages, 0, 2), 0);
    assert_eq!(floor_of(&messages, 1, 0), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resends_a_parity_shard_by_its_slot() {
    let resent = resent_shards(
        &vec![7u8; 2 * BLOCK_SIZE],
        2,
        2,
        vec![Feedback::Resend {
            slice_no: 0,
            blocks: vec![3, 0],
        }],
    )
    .await;

    assert_eq!(resent, vec![Resent::Parity(0, 1), Resent::Block(0, 0)]);
}
