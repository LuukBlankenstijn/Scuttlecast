use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::ProtoError;

pub enum Outbound {
    Block {
        slice_no: u32,
        block_in_slice: u16,
        /// Slices whose every block has been queued at least once. Stamped on
        /// every message rather than announced once, so a receiver that missed
        /// earlier traffic still learns it.
        emit_floor: u32,
        payload: Bytes,
    },
    Parity {
        slice_no: u32,
        parity_index: u16,
        emit_floor: u32,
        payload: Bytes,
    },
    Eof {
        total_bytes: u64,
        total_blocks: u64,
    },
}

pub enum Feedback {
    /// The lowest slice any participant still needs
    Needed(u32),
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
