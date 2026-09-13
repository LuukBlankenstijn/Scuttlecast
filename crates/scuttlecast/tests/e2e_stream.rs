mod common;

use std::time::Duration;

use bytes::Bytes;
use scuttlecast::SILENCE_TIMEOUT;
use scuttlecast::error::ProtoError;
use scuttlecast::proto::{Done, Frame, HEADER_SIZE, Hello, Message};
const BLOCK_SIZE: usize = scuttlecast::DEFAULT_BLOCK_SIZE as usize;
use tokio::net::UdpSocket;

async fn receive_stream(group_id: u8, port: u16) -> tokio::task::JoinHandle<Vec<u8>> {
    let receiver = common::receiver(common::group(group_id), port);
    tokio::spawn(async move {
        let mut transfer = receiver.recv_stream();
        let mut received = Vec::new();
        while let Some(block) = transfer.recv().await {
            received.extend_from_slice(&block);
        }
        transfer.finish().await.expect("finish");
        received
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_payload_to_a_channel() {
    let sent = common::payload(8 * BLOCK_SIZE);
    let receiving = receive_stream(30, 14000).await;

    common::sender(common::group(30), 14000, 1)
        .send_stream(common::source(&sent), None)
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_partial_final_block() {
    let sent = common::payload(3 * BLOCK_SIZE + 17);
    let receiving = receive_stream(31, 14010).await;

    common::sender(common::group(31), 14010, 1)
        .send_stream(common::source(&sent), None)
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_more_blocks_than_the_channel_holds() {
    let sent = common::payload(512 * BLOCK_SIZE);
    let receiving = receive_stream(32, 14020).await;

    common::sender(common::group(32), 14020, 1)
        .send_stream(common::source(&sent), None)
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_transfer_whose_done_overstates_the_byte_count() {
    let port = 14030;
    let group = common::group(33);
    let receiver = common::receiver(group, port);
    let output = common::Output::new();
    let path = output.path("bad.bin");

    let receiving = tokio::spawn(async move { receiver.recv_file(path).await });

    let liar = UdpSocket::bind((common::LOCAL, 0))
        .await
        .expect("bind liar");
    let destination = (group, port + 1);
    let transfer_id = 7;

    let send = async |message: Message| {
        let bytes = message.encode().expect("encode");
        liar.send_to(&bytes, destination).await.expect("send");
    };

    let nonzero = |n: u16| std::num::NonZeroU16::new(n).expect("nonzero");
    send(Message::Hello(Hello {
        transfer_id,
        block_size: std::num::NonZeroU32::new(scuttlecast::DEFAULT_BLOCK_SIZE).expect("block size"),
        blocks_per_slice: nonzero(32),
        parity_per_slice: 0,
        max_live_slices: nonzero(8),
        total_bytes: None,
    }))
    .await;

    let shard = Frame {
        slot: 0,
        slice_parity: 0,
        transfer_id: transfer_id as u32,
        seq: 0,
        slice_no: 0,
        emit_floor: 0,
        payload: Bytes::from(vec![1; BLOCK_SIZE]),
    };
    let mut datagram = vec![0u8; HEADER_SIZE + BLOCK_SIZE];
    datagram[..HEADER_SIZE].copy_from_slice(&shard.header());
    datagram[HEADER_SIZE..].copy_from_slice(&shard.payload);
    liar.send_to(&datagram, destination).await.expect("send");
    send(Message::Done(Done {
        transfer_id,
        total_bytes: 999_999,
        total_blocks: 1,
    }))
    .await;

    let result = tokio::time::timeout(Duration::from_secs(5), receiving)
        .await
        .expect("receiver finished")
        .expect("join");

    match result {
        Err(ProtoError::ByteCountMismatch { expected, received }) => {
            assert_eq!(expected, 999_999);
            assert_eq!(received, BLOCK_SIZE as u64);
        }
        other => panic!("expected a byte count mismatch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survives_a_consumer_that_stops_reading_past_the_silence_timeout() {
    let sent = common::payload(1024 * BLOCK_SIZE);
    let receiver = common::receiver(common::group(34), 14040);

    let receiving = tokio::spawn(async move {
        let mut transfer = receiver.recv_stream();
        let mut received = Vec::new();

        let first = transfer.recv().await.expect("a block before the pause");
        received.extend_from_slice(&first);
        tokio::time::sleep(SILENCE_TIMEOUT + Duration::from_secs(3)).await;

        while let Some(block) = transfer.recv().await {
            received.extend_from_slice(&block);
        }
        transfer.finish().await.expect("finish");
        received
    });

    common::sender_windowed(common::group(34), 14040, 1, 9)
        .send_stream(common::source(&sent), None)
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delivers_the_payload_whatever_size_the_caller_announced() {
    let sent = common::payload(8 * BLOCK_SIZE + 11);
    let receiving = receive_stream(35, 14050).await;

    common::sender(common::group(35), 14050, 1)
        .send_stream(common::source(&sent), Some(17))
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reports_progress_against_the_announced_size() {
    let sent = common::payload(512 * BLOCK_SIZE);
    let receiver = common::receiver(common::group(36), 14060);
    let mut progress = receiver.progress();

    let receiving = tokio::spawn(async move {
        let mut transfer = receiver.recv_stream();
        let mut received = Vec::new();
        while let Some(block) = transfer.recv().await {
            received.extend_from_slice(&block);
        }
        transfer.finish().await.expect("finish");
        received
    });

    let watching = tokio::spawn(async move {
        let mut seen = Vec::new();
        while progress.changed().await.is_ok() {
            let state = progress.borrow_and_update().clone();
            if let Some(fraction) = state.fraction_complete() {
                seen.push((fraction, state.elapsed));
            }
        }
        seen
    });

    common::sender(common::group(36), 14060, 1)
        .send_stream(common::source(&sent), Some(sent.len() as u64))
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);

    let seen = watching.await.expect("join");
    assert!(
        seen.iter()
            .any(|(fraction, _)| *fraction > 0.0 && *fraction < 1.0)
    );
    assert_eq!(seen.last().map(|(fraction, _)| *fraction), Some(1.0));
    assert!(seen.windows(2).all(|pair| pair[0].1 <= pair[1].1));
    assert!(seen.last().expect("a report").1 > Duration::ZERO);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn times_the_transfer_it_just_finished() {
    let sent = common::payload(64 * BLOCK_SIZE);
    let receiver = common::receiver(common::group(37), 14070);
    let started = std::time::Instant::now();

    let receiving = tokio::spawn(async move {
        let mut transfer = receiver.recv_stream();
        while transfer.recv().await.is_some() {}
        transfer.finish().await.expect("finish")
    });

    common::sender(common::group(37), 14070, 1)
        .send_stream(common::source(&sent), None)
        .await
        .expect("send");

    let summary = receiving.await.expect("join");
    let wall = started.elapsed();

    assert!(summary.duration > Duration::ZERO);
    assert!(
        summary.duration <= wall,
        "{:?} > {wall:?}",
        summary.duration
    );
}
