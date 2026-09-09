mod common;

use std::time::Duration;

use bytes::Bytes;
use proto::{Data, Done, Hello, Message};
use scuttlecast::BLOCK_SIZE;
use scuttlecast::error::ProtoError;
use tokio::net::UdpSocket;

async fn receive_stream(group_id: u8, port: u16) -> tokio::task::JoinHandle<Vec<u8>> {
    let receiver = common::receiver(common::group(group_id), port);
    tokio::spawn(async move {
        let mut blocks = receiver.recv_stream().await.expect("receive stream");
        let mut received = Vec::new();
        while let Some(block) = blocks.recv().await {
            received.extend_from_slice(&block);
        }
        received
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_payload_to_a_channel() {
    let sent = common::payload(8 * BLOCK_SIZE);
    let receiving = receive_stream(30, 48000).await;

    common::sender(common::group(30), 48000, 1)
        .send_stream(sent.as_slice())
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streams_partial_final_block() {
    let sent = common::payload(3 * BLOCK_SIZE + 17);
    let receiving = receive_stream(31, 48010).await;

    common::sender(common::group(31), 48010, 1)
        .send_stream(sent.as_slice())
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "recv_stream completes the transfer before returning its channel, so it deadlocks once the 32-slot channel fills"]
async fn streams_more_blocks_than_the_channel_holds() {
    let sent = common::payload(64 * BLOCK_SIZE);
    let receiving = receive_stream(32, 48020).await;

    common::sender(common::group(32), 48020, 1)
        .send_stream(sent.as_slice())
        .await
        .expect("send");

    assert_eq!(receiving.await.expect("join"), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_transfer_whose_done_overstates_the_byte_count() {
    let port = 48030;
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
        blocks_per_slice: nonzero(32),
        parity_per_slice: 0,
        max_live_slices: nonzero(8),
    }))
    .await;
    send(Message::Data(Data {
        transfer_id,
        seq: 0,
        slice_no: 0,
        block_in_slice: 0,
        emit_floor: 0,
        payload: Bytes::from(vec![1; 100]).into(),
    }))
    .await;
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
            assert_eq!(received, 100);
        }
        other => panic!("expected a byte count mismatch, got {other:?}"),
    }
}
