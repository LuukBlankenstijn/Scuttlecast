use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroUsize};
use std::ops::ControlFlow;

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::sender::slicer::channel::{Egress, Feedback, Outbound};
use crate::{BLOCK_SIZE, error::ProtoError};

pub mod blocks;
pub mod channel;

#[cfg(test)]
mod tests;

struct Slice {
    slice_no: u32,
    shards: Vec<Bytes>,
}

pub struct Slicer {
    shards_per_slice: NonZeroU16,
    max_live_slices: NonZeroUsize,
    live: VecDeque<Slice>,
    current: Slice,
    total_bytes: u64,
    total_blocks: u64,
}

impl Slicer {
    pub fn new(shards_per_slice: NonZeroU16, max_live_slices: NonZeroUsize) -> Self {
        Self {
            shards_per_slice,
            max_live_slices,
            live: VecDeque::new(),
            current: Slice {
                slice_no: 0,
                shards: Vec::new(),
            },
            total_bytes: 0,
            total_blocks: 0,
        }
    }

    pub async fn run(
        mut self,
        reader: impl AsyncRead + Unpin,
        outbound: mpsc::Sender<Outbound>,
        mut feedback: mpsc::Receiver<Feedback>,
    ) -> Result<(), ProtoError> {
        let mut egress = Egress::new(outbound);

        if self
            .fill(reader, &mut egress, &mut feedback)
            .await?
            .is_continue()
        {
            self.repair(&mut egress, &mut feedback).await?;
        }
        Ok(())
    }

    async fn fill(
        &mut self,
        reader: impl AsyncRead + Unpin,
        egress: &mut Egress,
        feedback: &mut mpsc::Receiver<Feedback>,
    ) -> Result<ControlFlow<()>, ProtoError> {
        let mut blocks = Box::pin(blocks::split(reader, BLOCK_SIZE));

        loop {
            tokio::select! {
                block = blocks.next(), if self.live.len() < self.max_live_slices.get() => {
                    let Some(block) = block else {
                        self.seal_current(egress);
                        egress.send(Outbound::Eof {
                            total_bytes: self.total_bytes,
                            total_blocks: self.total_blocks,
                        }).await?;
                        return Ok(ControlFlow::Continue(()));
                    };
                    self.push(block.map_err(ProtoError::File)?, egress).await?;
                }

                message = feedback.recv() => {
                    if self.on_feedback(message, egress).await?.is_break() {
                        return Ok(ControlFlow::Break(()));
                    }
                }
            }
        }
    }

    async fn repair(
        &mut self,
        egress: &mut Egress,
        feedback: &mut mpsc::Receiver<Feedback>,
    ) -> Result<(), ProtoError> {
        while self
            .on_feedback(feedback.recv().await, egress)
            .await?
            .is_continue()
        {}
        Ok(())
    }

    async fn on_feedback(
        &mut self,
        message: Option<Feedback>,
        egress: &Egress,
    ) -> Result<ControlFlow<()>, ProtoError> {
        match message {
            Some(Feedback::Watermark(slice_no)) => {
                while self
                    .live
                    .front()
                    .is_some_and(|slice| slice.slice_no < slice_no)
                {
                    self.live.pop_front();
                }
                Ok(ControlFlow::Continue(()))
            }
            Some(Feedback::Resend { slice_no, shards }) => {
                self.resend(slice_no, &shards, egress).await?;
                Ok(ControlFlow::Continue(()))
            }
            Some(Feedback::Done) => Ok(ControlFlow::Break(())),
            None => Err(ProtoError::EgressClosed),
        }
    }

    async fn push(&mut self, payload: Bytes, egress: &mut Egress) -> Result<(), ProtoError> {
        let shard_in_slice = self.current.shards.len() as u16;

        self.total_bytes += payload.len() as u64;
        self.total_blocks += 1;
        self.current.shards.push(payload.clone());

        egress
            .block(self.current.slice_no, shard_in_slice, payload)
            .await?;

        if self.current.shards.len() == self.shards_per_slice.get() as usize {
            self.seal_current(egress);
        }
        Ok(())
    }

    async fn resend(
        &self,
        slice_no: u32,
        shards: &[u16],
        egress: &Egress,
    ) -> Result<(), ProtoError> {
        let Some(slice) = self.live.iter().find(|slice| slice.slice_no == slice_no) else {
            return Ok(());
        };

        for &shard in shards {
            if let Some(payload) = slice.shards.get(shard as usize) {
                egress.block(slice_no, shard, payload.clone()).await?;
            }
        }
        Ok(())
    }

    fn seal_current(&mut self, egress: &mut Egress) {
        if self.current.shards.is_empty() {
            return;
        }

        let next = Slice {
            slice_no: self.current.slice_no + 1,
            shards: Vec::new(),
        };
        let sealed = std::mem::replace(&mut self.current, next);

        egress.seal(sealed.slice_no);
        self.live.push_back(sealed);
    }
}
