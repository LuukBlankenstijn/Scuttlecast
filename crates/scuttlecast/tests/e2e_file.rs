mod common;

use scuttlecast::BLOCK_SIZE;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_empty_file() {
    let sent = common::payload(0);
    let received = common::transfer_to_file(1, 46000, &sent).await;
    assert!(received.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_single_byte() {
    let sent = common::payload(1);
    let received = common::transfer_to_file(2, 46010, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_less_than_one_block() {
    let sent = common::payload(100);
    let received = common::transfer_to_file(3, 46020, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_one_byte_short_of_a_block() {
    let sent = common::payload(BLOCK_SIZE - 1);
    let received = common::transfer_to_file(4, 46030, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_exactly_one_block() {
    let sent = common::payload(BLOCK_SIZE);
    let received = common::transfer_to_file(5, 46040, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_exact_multiple_of_block_size() {
    let sent = common::payload(3 * BLOCK_SIZE);
    let received = common::transfer_to_file(6, 46050, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_exact_multiple_plus_one_byte() {
    let sent = common::payload(3 * BLOCK_SIZE + 1);
    let received = common::transfer_to_file(7, 46060, &sent).await;
    assert_eq!(received, sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transfers_spanning_multiple_slices() {
    let sent = common::payload(40 * BLOCK_SIZE);
    let received = common::transfer_to_file(8, 46070, &sent).await;
    assert_eq!(received, sent);
}
