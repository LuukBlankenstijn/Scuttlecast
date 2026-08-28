use bytes::Bytes;
use tokio::sync::mpsc;

use crate::error::ProtoError;

pub enum Outbound {
    Block {
        slice_no: u32,
        shard_in_slice: u16,
        /// Highest slice whose every shard has been queued
        sealed_through: Option<u32>,
        payload: Bytes,
    },
    Eof {
        total_bytes: u64,
        total_blocks: u64,
    },
}

pub enum Feedback {
    Watermark(u32),
    Resend { slice_no: u32, shards: Vec<u16> },
    Done,
}

pub(super) struct Egress {
    channel: mpsc::Sender<Outbound>,
    sealed_through: Option<u32>,
}

impl Egress {
    pub(super) fn new(channel: mpsc::Sender<Outbound>) -> Self {
        Self {
            channel,
            sealed_through: None,
        }
    }

    pub(super) fn seal(&mut self, slice_no: u32) {
        self.sealed_through = Some(slice_no);
    }

    pub(super) async fn block(
        &self,
        slice_no: u32,
        shard_in_slice: u16,
        payload: Bytes,
    ) -> Result<(), ProtoError> {
        self.send(Outbound::Block {
            slice_no,
            shard_in_slice,
            sealed_through: self.sealed_through,
            payload,
        })
        .await
    }

    pub(super) async fn send(&self, message: Outbound) -> Result<(), ProtoError> {
        self.channel
            .send(message)
            .await
            .map_err(|_| ProtoError::EgressClosed)
    }
}
