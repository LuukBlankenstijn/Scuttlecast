use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::error::ProtoError;
use crate::sender::slicer::channel::{Feedback, Outbound};
use crate::sender::slicer::window::{Sealed, Window};

pub mod blocks;
pub mod channel;
mod window;

#[cfg(test)]
mod tests;

pub struct Slicer {
    window: Window,
    block_size: usize,
    blocks_per_slice: u16,
    emit_floor: u32,
    total_bytes: u64,
    total_blocks: u64,
}

impl Slicer {
    pub fn new(
        block_size: NonZeroU32,
        blocks_per_slice: NonZeroU16,
        parity_per_slice: u8,
        max_live_slices: NonZeroUsize,
    ) -> Result<Self, reed_solomon_simd::Error> {
        let block_size = block_size.get() as usize;

        Ok(Self {
            window: Window::new(
                block_size,
                blocks_per_slice,
                parity_per_slice,
                max_live_slices,
            )?,
            block_size,
            blocks_per_slice: blocks_per_slice.get(),
            emit_floor: 0,
            total_bytes: 0,
            total_blocks: 0,
        })
    }

    pub async fn run(
        mut self,
        reader: impl AsyncRead + Unpin + Send + 'static,
        outbound: mpsc::Sender<Outbound>,
        mut feedback: mpsc::UnboundedReceiver<Feedback>,
    ) -> Result<(), ProtoError> {
        let mut input = Box::pin(blocks::split(reader, self.block_size));
        let mut drained = false;

        loop {
            tokio::select! {
                block = input.next(), if !drained && !self.window.is_full() => match block {
                    Some(block) => self.push(block.map_err(ProtoError::File)?, &outbound).await?,
                    None => {
                        drained = true;
                        if let Some(sealed) = self.window.seal() {
                            self.queue_parity(sealed, &outbound).await?;
                        }
                        channel::send(&outbound, Outbound::Eof {
                            total_bytes: self.total_bytes,
                            total_blocks: self.total_blocks,
                        }).await?;
                    }
                },

                message = feedback.recv() => match message {
                    Some(Feedback::Needed(first_needed)) => self.window.retain_from(first_needed),
                    Some(Feedback::Cover(parity)) => self.window.cover(parity),
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
        block: Bytes,
        outbound: &mpsc::Sender<Outbound>,
    ) -> Result<(), ProtoError> {
        self.total_bytes += block.len() as u64;
        self.total_blocks += 1;
        let payload = self.padded(block);
        let slice_parity = self.window.current_parity();
        let pushed = self.window.push(payload.clone());

        channel::send(
            outbound,
            Outbound::Shard {
                slice_no: pushed.slice_no,
                slot: pushed.block_in_slice,
                slice_parity,
                emit_floor: self.emit_floor,
                payload,
            },
        )
        .await?;

        if let Some(sealed) = pushed.sealed {
            self.queue_parity(sealed, outbound).await?;
        }
        Ok(())
    }

    fn padded(&self, block: Bytes) -> Bytes {
        if block.len() == self.block_size {
            return block;
        }

        let mut full = BytesMut::zeroed(self.block_size);
        full[..block.len()].copy_from_slice(&block);
        full.freeze()
    }

    async fn queue_parity(
        &mut self,
        sealed: Sealed,
        outbound: &mpsc::Sender<Outbound>,
    ) -> Result<(), ProtoError> {
        let slice_no = sealed.slice_no;
        let slice_parity = sealed.parity.len() as u8;
        for (parity_index, payload) in sealed.parity.into_iter().enumerate() {
            channel::send(
                outbound,
                Outbound::Shard {
                    slice_no,
                    slot: self.blocks_per_slice + parity_index as u16,
                    slice_parity,
                    emit_floor: self.emit_floor,
                    payload,
                },
            )
            .await?;
        }
        self.emit_floor = slice_no + 1;
        Ok(())
    }

    async fn resend(
        &self,
        slice_no: u32,
        slots: &[u16],
        outbound: &mpsc::Sender<Outbound>,
    ) -> Result<(), ProtoError> {
        let Some(slice_parity) = self.window.slice_parity(slice_no) else {
            return Ok(());
        };

        for &slot in slots {
            let Some(payload) = self.window.shard(slice_no, slot) else {
                continue;
            };
            channel::send(
                outbound,
                Outbound::Shard {
                    slice_no,
                    slot,
                    slice_parity,
                    emit_floor: self.emit_floor,
                    payload: payload.clone(),
                },
            )
            .await?;
        }
        Ok(())
    }
}
