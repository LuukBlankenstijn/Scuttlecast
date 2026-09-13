use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::ProtoError;

pub enum Outbound {
    Shard {
        slice_no: u32,
        slot: u16,
        slice_parity: u8,
        emit_floor: u32,
        payload: Bytes,
    },
    Eof {
        total_bytes: u64,
        total_blocks: u64,
    },
}

pub enum Feedback {
    Needed(u32),
    Cover(u8),
    Resend { slice_no: u32, blocks: Vec<u16> },
    Done,
}

pub(super) async fn send(
    channel: &mpsc::Sender<Outbound>,
    message: Outbound,
) -> Result<(), ProtoError> {
    channel
        .send(message)
        .await
        .map_err(|_| ProtoError::EgressClosed)
}
