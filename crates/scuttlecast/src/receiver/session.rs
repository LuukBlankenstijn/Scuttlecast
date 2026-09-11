use std::{
    collections::VecDeque,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::proto::{Done, Error as ProtocolError, Frame, Hello, Message, Nak, Stats};
use bytes::Bytes;
use tokio::sync::mpsc::{self, error::TrySendError};
use tracing::debug;

use crate::{
    RENAK_INTERVAL, SILENCE_TIMEOUT, STATS_INTERVAL,
    error::ProtoError,
    receiver::{TransferSummary, assembler::Assembler, naks::Naks},
    transport::{Incoming, MessageSocket},
};

pub(super) async fn join_session(
    socket: &MessageSocket,
    max_wait: Duration,
    receiver_id: u64,
) -> Result<(Hello, SocketAddr), ProtoError> {
    let deadline = tokio::time::Instant::now() + max_wait;

    let (hello, sender) = loop {
        tokio::select! {
            r = socket.recv_control(None) => {
                if let (Message::Hello(hello), sender) = r? {
                    break (hello, sender);
                }
            },

            _ = tokio::time::sleep_until(deadline) => {
                return Err(ProtoError::Timeout(
                    "Timed out listening for hello message".to_string(),
                ));
            },
        }
    };

    socket
        .send_control(
            Message::Join {
                transfer_id: hello.transfer_id,
                receiver_id,
            },
            sender,
        )
        .await?;

    Ok((hello, sender))
}

pub(super) struct Session {
    socket: MessageSocket,
    sender: SocketAddr,
    transfer_id: u64,
    receiver_id: u64,
    block_size: usize,
    blocks_per_slice: u16,
    parity_per_slice: u8,
    assembler: Assembler,
    naks: Naks,
    pending: VecDeque<Bytes>,
    emit_floor: u32,
    received: u64,
    highest_seq: Option<u32>,
    duplicates: u64,
    late: u64,
    total_bytes: Option<u64>,
    written_bytes: u64,
    blocks_written: u64,
    sink_stall: Duration,
    last_handover: Instant,
    last_packet: Instant,
}

impl Session {
    pub(super) fn new(
        socket: MessageSocket,
        sender: SocketAddr,
        receiver_id: u64,
        hello: &Hello,
    ) -> Result<Self, ProtoError> {
        let block_size = hello.block_size.get() as usize;

        Ok(Self {
            socket,
            sender,
            transfer_id: hello.transfer_id,
            receiver_id,
            block_size,
            blocks_per_slice: hello.blocks_per_slice.get(),
            parity_per_slice: hello.parity_per_slice,
            assembler: Assembler::new(
                block_size,
                hello.blocks_per_slice,
                hello.parity_per_slice,
                hello.max_live_slices,
            )?,
            naks: Naks::default(),
            pending: VecDeque::new(),
            emit_floor: 0,
            received: 0,
            highest_seq: None,
            duplicates: 0,
            late: 0,
            total_bytes: None,
            written_bytes: 0,
            blocks_written: 0,
            sink_stall: Duration::ZERO,
            last_handover: Instant::now(),
            last_packet: Instant::now(),
        })
    }

    pub(super) async fn run(
        mut self,
        sink: mpsc::Sender<Bytes>,
    ) -> Result<TransferSummary, ProtoError> {
        let mut stats_tick = tokio::time::interval(STATS_INTERVAL);
        let mut renak_tick = tokio::time::interval(RENAK_INTERVAL);
        let transfer_id = self.transfer_id;
        let mut arrivals = Vec::new();

        while !self.assembler.is_finished() || !self.pending.is_empty() {
            let silence = SILENCE_TIMEOUT.saturating_sub(self.last_packet.elapsed());

            tokio::select! {
                r = self.socket.recv_batch(transfer_id, &mut arrivals) => {
                    r?;
                    self.last_packet = Instant::now();
                }

                _ = sink.reserve(), if !self.pending.is_empty() => {}

                _ = stats_tick.tick() => self.send_stats().await?,

                _ = renak_tick.tick() => self.request_gaps().await?,

                _ = tokio::time::sleep(silence), if !self.assembler.is_finished() => {
                    return Err(self.went_silent());
                }
            }

            for arrival in arrivals.drain(..) {
                self.on_arrival(arrival).await?;
            }
            self.hand_over(&sink)?;
        }

        self.check_byte_total()?;

        self.send_stats().await?;
        self.socket
            .send_control(
                Message::Leave {
                    transfer_id: self.transfer_id,
                    receiver_id: self.receiver_id,
                },
                self.sender,
            )
            .await?;

        Ok(self.summary())
    }

    async fn on_arrival(&mut self, arrival: Incoming) -> Result<(), ProtoError> {
        match arrival {
            Incoming::Shard(frame) => self.on_shard(frame).await,
            Incoming::Control(Message::Done(done)) => self.on_done(done).await,
            Incoming::Control(Message::Hello(_)) if self.received == 0 => {
                self.socket
                    .send_control(
                        Message::Join {
                            transfer_id: self.transfer_id,
                            receiver_id: self.receiver_id,
                        },
                        self.sender,
                    )
                    .await
            }
            Incoming::Control(Message::Evicted(evicted)) if evicted.target == self.receiver_id => {
                Err(ProtoError::Evicted(evicted.reason))
            }
            Incoming::Control(_) => Ok(()),
        }
    }

    async fn on_shard(&mut self, frame: Frame) -> Result<(), ProtoError> {
        self.check_shard(&frame)?;

        self.received += 1;
        self.highest_seq = Some(self.highest_seq.unwrap_or(0).max(frame.seq));

        let floor_advanced = frame.emit_floor > self.emit_floor;
        self.emit_floor = self.emit_floor.max(frame.emit_floor);

        if !self.assembler.insert(
            frame.slice_no,
            frame.slot,
            frame.slice_parity,
            frame.payload,
        ) {
            self.refused(frame.slice_no);
        }

        if floor_advanced {
            self.request_gaps().await?;
        }
        Ok(())
    }

    fn check_shard(&self, frame: &Frame) -> Result<(), ProtocolError> {
        let slots = self.assembler.total_slots();
        if frame.slot >= slots {
            return Err(ProtocolError::SlotOutsideSlice {
                slot: frame.slot,
                slots,
            });
        }
        if frame.slice_parity > self.parity_per_slice {
            return Err(ProtocolError::ParityWiderThanTransfer {
                named: frame.slice_parity,
                allowed: self.parity_per_slice,
            });
        }
        if let Some(index) = frame.slot.checked_sub(self.blocks_per_slice)
            && index >= frame.slice_parity as u16
        {
            return Err(ProtocolError::ParityOutsideSlice {
                slot: frame.slot,
                named: frame.slice_parity,
            });
        }
        if frame.payload.len() != self.block_size {
            return Err(ProtocolError::PayloadSize {
                got: frame.payload.len(),
                expected: self.block_size,
            });
        }

        Ok(())
    }

    /// A shard the assembler would not take is either late, because its slice
    /// was already reconstructed and written, or a genuine duplicate
    fn refused(&mut self, slice_no: u32) {
        if slice_no < self.assembler.next_needed() {
            self.late += 1;
        } else {
            self.duplicates += 1;
        }
    }

    async fn on_done(&mut self, done: Done) -> Result<(), ProtoError> {
        self.total_bytes = Some(done.total_bytes);
        self.assembler.on_done(done.total_blocks);

        self.request_gaps().await
    }

    /// Hands over what the sink will take, never waiting for it
    fn hand_over(&mut self, sink: &mpsc::Sender<Bytes>) -> Result<(), ProtoError> {
        let now = Instant::now();
        if !self.pending.is_empty() {
            self.sink_stall += now - self.last_handover;
        }
        self.last_handover = now;

        if self.pending.is_empty() {
            for block in self.assembler.take_ready() {
                let block = self.within_total(block);
                if !block.is_empty() {
                    self.pending.push_back(block);
                }
            }
        }

        while let Some(block) = self.pending.pop_front() {
            match sink.try_send(block) {
                Ok(()) => self.blocks_written += 1,
                Err(TrySendError::Full(block)) => {
                    self.pending.push_front(block);
                    break;
                }
                Err(TrySendError::Closed(_)) => return Err(ProtoError::SinkClosed),
            }
        }

        self.naks.forget_below(self.assembler.next_needed());

        Ok(())
    }

    /// The last block of a transfer can carry more than the transfer holds,
    /// once reconstruction pads a short slice back to full length
    fn within_total(&mut self, block: Bytes) -> Bytes {
        let block = match self.total_bytes {
            Some(total_bytes) => {
                let room = total_bytes.saturating_sub(self.written_bytes) as usize;
                block.slice(..block.len().min(room))
            }
            None => block,
        };

        self.written_bytes += block.len() as u64;
        block
    }

    async fn send_stats(&mut self) -> Result<(), ProtoError> {
        let message = Message::Stats(Stats {
            transfer_id: self.transfer_id,
            receiver_id: self.receiver_id,
            total_received: self.received,
            total_expected: self.highest_seq.map_or(0, |seq| seq as u64 + 1),
            next_needed_slice: self.assembler.next_needed(),
            sink_stall_ms: std::mem::take(&mut self.sink_stall)
                .as_millis()
                .min(u32::MAX as u128) as u32,
        });

        self.socket.send_control(message, self.sender).await
    }

    async fn request_gaps(&mut self) -> Result<(), ProtoError> {
        let gaps = self.assembler.gaps(self.emit_floor);

        for (slice_no, missing) in self.naks.due(gaps, Instant::now()) {
            debug!(slice_no, missing = missing.len(), "requesting repair");
            let message = Message::Nak(Nak {
                transfer_id: self.transfer_id,
                receiver_id: self.receiver_id,
                slice_no,
                missing,
            });
            self.socket.send_control(message, self.sender).await?;
        }

        Ok(())
    }

    fn went_silent(&self) -> ProtoError {
        ProtoError::SenderSilent {
            held: self.assembler.next_needed(),
            total: self.assembler.total_slices(),
        }
    }

    /// A sender whose `Done` promises more than it sent leaves a short file,
    /// which must never look like success
    fn check_byte_total(&self) -> Result<(), ProtoError> {
        match self.total_bytes {
            Some(expected) if expected != self.written_bytes => {
                Err(ProtoError::ByteCountMismatch {
                    expected,
                    received: self.written_bytes,
                })
            }
            _ => Ok(()),
        }
    }

    fn summary(&self) -> TransferSummary {
        TransferSummary {
            total_bytes: self.written_bytes,
            total_blocks: self.blocks_written,
            received: self.received,
            expected: self.highest_seq.map_or(0, |seq| seq as u64 + 1),
            duplicates: self.duplicates,
            late: self.late,
            naks_sent: self.naks.total(),
        }
    }
}
