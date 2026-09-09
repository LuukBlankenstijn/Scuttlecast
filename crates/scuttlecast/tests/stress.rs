mod common;

use scuttlecast::BLOCK_SIZE;

const MULTI_MB: usize = 4 * 1024 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repeated_multi_slice_transfers() {
    let sent = common::payload(40 * BLOCK_SIZE);

    for round in 0..5 {
        let received = common::transfer_to_file(40, 49000, &sent).await;
        assert_eq!(received.len(), sent.len(), "round {round} length");
        assert_eq!(received, sent, "round {round} contents");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multi_slice_transfer_to_three_receivers() {
    let sent = common::payload(40 * BLOCK_SIZE);
    let output = common::Output::new();
    let paths: Vec<_> = (0..3)
        .map(|index| output.path(&format!("receiver{index}.bin")))
        .collect();

    let receiving: Vec<_> = paths
        .iter()
        .map(|path| {
            let receiver = common::receiver(common::group(41), 49010);
            let path = path.clone();
            tokio::spawn(async move { receiver.recv_file(path).await.expect("receive") })
        })
        .collect();

    common::sender(common::group(41), 49010, 3)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repeated_multi_megabyte_transfers() {
    let sent = common::payload(MULTI_MB);

    for round in 0..3 {
        let received = common::transfer_to_file(42, 49020, &sent).await;
        assert_eq!(received, sent, "round {round}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn multi_megabyte_transfer_to_three_receivers() {
    let sent = common::payload(MULTI_MB);
    let output = common::Output::new();
    let paths: Vec<_> = (0..3)
        .map(|index| output.path(&format!("receiver{index}.bin")))
        .collect();

    let receiving: Vec<_> = paths
        .iter()
        .map(|path| {
            let receiver = common::receiver(common::group(43), 49030);
            let path = path.clone();
            tokio::spawn(async move { receiver.recv_file(path).await.expect("receive") })
        })
        .collect();

    common::sender(common::group(43), 49030, 3)
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
