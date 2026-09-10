mod common;

use scuttlecast::BLOCK_SIZE;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_empty_file() {
    let sent = common::payload(0);
    let received = common::transfer_to_file(1, 12000, &sent).await;
    assert!(received.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_single_byte() {
    let sent = common::payload(1);
    let received = common::transfer_to_file(2, 12010, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_less_than_one_block() {
    let sent = common::payload(100);
    let received = common::transfer_to_file(3, 12020, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_one_byte_short_of_a_block() {
    let sent = common::payload(BLOCK_SIZE - 1);
    let received = common::transfer_to_file(4, 12030, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_exactly_one_block() {
    let sent = common::payload(BLOCK_SIZE);
    let received = common::transfer_to_file(5, 12040, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_exact_multiple_of_block_size() {
    let sent = common::payload(3 * BLOCK_SIZE);
    let received = common::transfer_to_file(6, 12050, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_exact_multiple_plus_one_byte() {
    let sent = common::payload(3 * BLOCK_SIZE + 1);
    let received = common::transfer_to_file(7, 12060, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_spanning_multiple_slices() {
    let sent = common::payload(40 * BLOCK_SIZE);
    let received = common::transfer_to_file(8, 12070, &sent).await;
    assert_eq!(received, sent);
}

/// A final slice holding at least `blocks_per_slice - parity_per_slice` blocks
/// can reach the shard count of a full slice, so the receiver could once
/// reconstruct a block the transfer never had and write it out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_a_final_slice_parity_alone_could_fill() {
    let sent = common::payload(62 * BLOCK_SIZE + 500);
    let received = common::transfer_to_file(9, 12080, &sent).await;
    assert_eq!(received.len(), sent.len());
    assert_eq!(received, sent);
}
