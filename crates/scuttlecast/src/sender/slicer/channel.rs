use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::ProtoError;

pub enum Outbound {
    Block {
        slice_no: u32,
        block_in_slice: u16,
        payload: Bytes,
    },
    Eof {
        total_bytes: u64,
        total_blocks: u64,
    },
}

pub enum Feedback {
    /// Every participant holds this slice and all slices below it in full
    Completed(u32),
    Resend {
        slice_no: u32,
        blocks: Vec<u16>,
    },
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
