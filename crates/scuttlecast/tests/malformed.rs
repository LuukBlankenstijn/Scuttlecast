mod common;

use std::num::NonZeroU16;
use std::time::Duration;

use bytes::Bytes;
use proto::{Data, Hello, Message};
use scuttlecast::error::ProtoError;

fn blocks_per_slice(blocks: u16) -> NonZeroU16 {
    NonZeroU16::new(blocks).expect("nonzero blocks per slice")
}

async fn receive_one_transfer(
    group_id: u8,
    port: u16,
) -> (
    common::Rogue,
    tokio::task::JoinHandle<Result<(), ProtoError>>,
    common::Output,
) {
    let group = common::group(group_id);
    let receiver = common::receiver(group, port);
    let output = common::Output::new();
    let path = output.path("out.bin");

    let receiving = tokio::spawn(async move { receiver.recv_file(path).await });
    let rogue = common::Rogue::new(group, port).await;

    (rogue, receiving, output)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_block_index_outside_its_slice() {
    let (rogue, receiving, _output) = receive_one_transfer(80, 53000).await;

    rogue
        .send(Message::Hello(Hello {
            transfer_id: 1,
            blocks_per_slice: blocks_per_slice(32),
        }))
        .await;
    rogue
        .send(Message::Data(Data {
            transfer_id: 1,
            slice_no: 0,
            block_in_slice: 32,
            payload: Bytes::from(vec![1; 10]).into(),
        }))
        .await;

    let result = tokio::time::timeout(Duration::from_secs(5), receiving)
        .await
        .expect("receiver finished")
        .expect("receiver did not panic");

    assert!(
        matches!(
            result,
            Err(ProtoError::Protocol(proto::Error::BlockOutsideSlice {
                block_in_slice: 32,
                blocks_per_slice
            })) if blocks_per_slice.get() == 32
        ),
        "got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_hello_claiming_zero_blocks_per_slice() {
    let (rogue, receiving, _output) = receive_one_transfer(81, 53010).await;

    let mut bytes = Message::Hello(Hello {
        transfer_id: 2,
        blocks_per_slice: blocks_per_slice(1),
    })
    .encode()
    .expect("encode");
    assert_eq!(bytes.pop(), Some(1), "blocks_per_slice trails the message");
    bytes.push(0);

    rogue.send_bytes(&bytes).await;

    let result = tokio::time::timeout(Duration::from_secs(5), receiving)
        .await
        .expect("receiver finished")
        .expect("receiver did not panic");

    assert!(
        matches!(result, Err(ProtoError::Protocol(proto::Error::Decode(_)))),
        "got {result:?}"
    );
}
