use std::num::{NonZeroU16, NonZeroUsize};

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::sender::slicer::channel::{Feedback, Outbound};
use crate::sender::slicer::window::Window;
use crate::{BLOCK_SIZE, error::ProtoError};

pub mod blocks;
pub mod channel;
mod window;

#[cfg(test)]
mod tests;

pub struct Slicer {
    window: Window,
    total_bytes: u64,
    total_blocks: u64,
}

impl Slicer {
    pub fn new(blocks_per_slice: NonZeroU16, max_live_slices: NonZeroUsize) -> Self {
        Self {
            window: Window::new(blocks_per_slice, max_live_slices),
            total_bytes: 0,
            total_blocks: 0,
        }
    }

    pub async fn run(
        mut self,
        reader: impl AsyncRead + Unpin + Send + 'static,
        outbound: mpsc::Sender<Outbound>,
        mut feedback: mpsc::UnboundedReceiver<Feedback>,
    ) -> Result<(), ProtoError> {
        let mut input = Box::pin(blocks::split(reader, BLOCK_SIZE));
        let mut drained = false;

        loop {
            tokio::select! {
                block = input.next(), if !drained && !self.window.is_full() => match block {
                    Some(block) => self.push(block.map_err(ProtoError::File)?, &outbound).await?,
                    None => {
                        drained = true;
                        self.window.seal();
                        channel::send(&outbound, Outbound::Eof {
                            total_bytes: self.total_bytes,
                            total_blocks: self.total_blocks,
                        }).await?;
                    }
                },

                message = feedback.recv() => match message {
                    Some(Feedback::Needed(first_needed)) => self.window.retain_from(first_needed),
                    Some(Feedback::Resend { slice_no, blocks }) => {
                        self.resend(slice_no, &blocks, &outbound).await?
                    }
                    Some(Feedback::Done) => return Ok(()),
                    None => return Err(ProtoError::EgressClosed),
                },
            }
        }
    }

    async fn push(
        &mut self,
        payload: Bytes,
        outbound: &mpsc::Sender<Outbound>,
    ) -> Result<(), ProtoError> {
        self.total_bytes += payload.len() as u64;
        self.total_blocks += 1;
        let (slice_no, block_in_slice) = self.window.push(payload.clone());

        channel::send(
            outbound,
            Outbound::Block {
                slice_no,
                block_in_slice,
                emit_floor: self.window.emit_floor(),
                payload,
            },
        )
        .await
    }

    async fn resend(
        &self,
        slice_no: u32,
        blocks: &[u16],
        outbound: &mpsc::Sender<Outbound>,
    ) -> Result<(), ProtoError> {
        for &block_in_slice in blocks {
            if let Some(payload) = self.window.block(slice_no, block_in_slice) {
                channel::send(
                    outbound,
                    Outbound::Block {
                        slice_no,
                        block_in_slice,
                        emit_floor: self.window.emit_floor(),
                        payload: payload.clone(),
                    },
                )
                .await?;
            }
        }
        Ok(())
    }
}
