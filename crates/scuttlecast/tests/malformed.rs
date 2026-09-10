mod common;

use std::num::NonZeroU16;
use std::time::Duration;

use bytes::Bytes;
use scuttlecast::error::ProtoError;
use scuttlecast::proto::{Data, Hello, Message};

fn blocks_per_slice(blocks: u16) -> NonZeroU16 {
    NonZeroU16::new(blocks).expect("nonzero blocks per slice")
}

fn hello(transfer_id: u64, blocks: u16) -> Hello {
    Hello {
        transfer_id,
        blocks_per_slice: blocks_per_slice(blocks),
        parity_per_slice: 0,
        max_live_slices: blocks_per_slice(8),
    }
}

async fn receive_one_transfer(
    group_id: u8,
    port: u16,
) -> (
    common::Rogue,
    tokio::task::JoinHandle<Result<scuttlecast::receiver::TransferSummary, ProtoError>>,
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
    let (rogue, receiving, _output) = receive_one_transfer(80, 19000).await;

    rogue.send(Message::Hello(hello(1, 32))).await;
    rogue
        .send(Message::Data(Data {
            transfer_id: 1,
            seq: 0,
            slice_no: 0,
            block_in_slice: 32,
            emit_floor: 0,
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
            Err(ProtoError::Protocol(scuttlecast::proto::Error::BlockOutsideSlice {
                block_in_slice: 32,
                blocks_per_slice
            })) if blocks_per_slice.get() == 32
        ),
        "got {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_a_hello_claiming_zero_blocks_per_slice() {
    let (rogue, receiving, _output) = receive_one_transfer(81, 19010).await;

    let mut bytes = Message::Hello(hello(2, 1)).encode().expect("encode");
    let blocks_per_slice_byte = bytes.len() - 3;
    assert_eq!(
        bytes[blocks_per_slice_byte..],
        [1, 0, 8],
        "blocks_per_slice, parity_per_slice and max_live_slices trail the message"
    );
    bytes[blocks_per_slice_byte] = 0;

    rogue.send_bytes(&bytes).await;

    let result = tokio::time::timeout(Duration::from_secs(5), receiving)
        .await
        .expect("receiver finished")
        .expect("receiver did not panic");

    assert!(
        matches!(
            result,
            Err(ProtoError::Protocol(scuttlecast::proto::Error::Decode(_)))
        ),
        "got {result:?}"
    );
}
