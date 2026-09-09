use std::{
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU16,
    path::PathBuf,
    time::{Duration, Instant},
};

use bon::Builder;
use bytes::Bytes;
use proto::{Data, Done, Hello, Message, Nak, Stats};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::{
    RENAK_INTERVAL, SILENCE_TIMEOUT, STATS_INTERVAL,
    error::ProtoError,
    receiver::{assembler::Assembler, naks::Naks},
    transport::{Losing, MessageSocket},
};

mod assembler;
mod naks;
mod sink;

const BLOCK_CHANNEL_CAPACITY: usize = 256;

#[derive(Builder)]
pub struct Receiver {
    #[builder(with = |local_address: Ipv4Addr, group_address: Ipv4Addr, port: u16,| -> Result<_, ProtoError> {
        MessageSocket::receiving(local_address, group_address, port)
    } )]
    socket: MessageSocket,
    max_wait: Duration,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferSummary {
    pub total_bytes: u64,
    pub total_blocks: u64,
    pub received: u64,
    pub expected: u64,
    pub duplicates: u64,
    pub naks_sent: u64,
}

impl TransferSummary {
    pub fn loss(&self) -> f64 {
        if self.expected == 0 {
            return 0.0;
        }
        1.0 - self.received as f64 / self.expected as f64
    }
}

/// A transfer in flight. Blocks arrive in order on the handle; the outcome
/// arrives from `finish`, so a consumer never has to unwrap an error out of
/// the data it is reading.
pub struct Transfer {
    blocks: mpsc::Receiver<Bytes>,
    session: JoinHandle<Result<TransferSummary, ProtoError>>,
}

impl Transfer {
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.blocks.recv().await
    }

    pub async fn finish(self) -> Result<TransferSummary, ProtoError> {
        drop(self.blocks);
        self.session
            .await
            .map_err(|error| ProtoError::Timeout(error.to_string()))?
    }
}

impl Receiver {
    /// Applies a loss rule to this receiver's socket, so a test can decide
    /// exactly which datagrams it never sees
    pub fn losing(mut self, losing: Losing) -> Self {
        self.socket = self.socket.losing(losing);
        self
    }

    pub async fn recv_file(self, path: PathBuf) -> Result<TransferSummary, ProtoError> {
        let file = tokio::fs::File::create(path)
            .await
            .map_err(ProtoError::File)?;
        self.recv_to(file).await
    }

    pub async fn recv_to(
        self,
        writer: impl AsyncWrite + Unpin,
    ) -> Result<TransferSummary, ProtoError> {
        let transfer = self.recv_stream();
        sink::pump(transfer.blocks, writer).await?;

        transfer
            .session
            .await
            .map_err(|error| ProtoError::Timeout(error.to_string()))?
    }

    pub fn recv_stream(self) -> Transfer {
        let (blocks_tx, blocks) = mpsc::channel(BLOCK_CHANNEL_CAPACITY);
        let max_wait = self.max_wait;
        let socket = self.socket;

        let session = tokio::spawn(async move {
            let receiver_id = rand::random();
            let (hello, sender) = join_session(&socket, max_wait, receiver_id).await?;
            info!(
                transfer_id = hello.transfer_id,
                receiver_id, "joined session"
            );

            Session::new(socket, sender, receiver_id, &hello, blocks_tx)
                .run()
                .await
        });

        Transfer { blocks, session }
    }
}

async fn join_session(
    socket: &MessageSocket,
    max_wait: Duration,
    receiver_id: u64,
) -> Result<(Hello, SocketAddr), ProtoError> {
    let deadline = tokio::time::Instant::now() + max_wait;

    let (hello, sender) = loop {
        tokio::select! {
            r = socket.recv_from() => {
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
        .send_to(
            Message::Join {
                transfer_id: hello.transfer_id,
                receiver_id,
            },
            sender,
        )
        .await?;

    Ok((hello, sender))
}

struct Session {
    socket: MessageSocket,
    sender: SocketAddr,
    transfer_id: u64,
    receiver_id: u64,
    blocks_per_slice: NonZeroU16,
    assembler: Assembler,
    naks: Naks,
    blocks: mpsc::Sender<Bytes>,
    emit_floor: u32,
    received: u64,
    highest_seq: Option<u64>,
    duplicates: u64,
    total_bytes: Option<u64>,
    written_bytes: u64,
    blocks_written: u64,
    sink_stall: Duration,
    last_packet: Instant,
}

impl Session {
    fn new(
        socket: MessageSocket,
        sender: SocketAddr,
        receiver_id: u64,
        hello: &Hello,
        blocks: mpsc::Sender<Bytes>,
    ) -> Self {
        Self {
            socket,
            sender,
            transfer_id: hello.transfer_id,
            receiver_id,
            blocks_per_slice: hello.blocks_per_slice,
            assembler: Assembler::new(hello.blocks_per_slice, hello.max_live_slices),
            naks: Naks::default(),
            blocks,
            emit_floor: 0,
            received: 0,
            highest_seq: None,
            duplicates: 0,
            total_bytes: None,
            written_bytes: 0,
            blocks_written: 0,
            sink_stall: Duration::ZERO,
            last_packet: Instant::now(),
        }
    }

    async fn run(mut self) -> Result<TransferSummary, ProtoError> {
        let mut stats_tick = tokio::time::interval(STATS_INTERVAL);
        let mut renak_tick = tokio::time::interval(RENAK_INTERVAL);

        while !self.assembler.is_finished() {
            let silence = SILENCE_TIMEOUT.saturating_sub(self.last_packet.elapsed());

            tokio::select! {
                r = self.socket.recv_in_transfer(self.transfer_id) => {
                    let (message, _) = r?;
                    self.last_packet = Instant::now();
                    self.on_message(message).await?;
                }

                _ = stats_tick.tick() => self.send_stats().await?,

                _ = renak_tick.tick() => self.request_gaps().await?,

                _ = tokio::time::sleep(silence) => return Err(self.went_silent()),
            }
        }

        self.check_byte_total()?;

        self.send_stats().await?;
        self.socket
            .send_to(
                Message::Leave {
                    transfer_id: self.transfer_id,
                    receiver_id: self.receiver_id,
                },
                self.sender,
            )
            .await?;

        Ok(self.summary())
    }

    async fn on_message(&mut self, message: Message) -> Result<(), ProtoError> {
        match message {
            Message::Data(data) => self.on_data(data).await?,
            Message::Done(done) => self.on_done(done).await?,
            Message::Hello(_) if self.received == 0 => {
                self.socket
                    .send_to(
                        Message::Join {
                            transfer_id: self.transfer_id,
                            receiver_id: self.receiver_id,
                        },
                        self.sender,
                    )
                    .await?
            }
            Message::Evicted(evicted) if evicted.target == self.receiver_id => {
                return Err(ProtoError::Evicted(evicted.reason));
            }
            _ => {}
        }

        Ok(())
    }

    async fn on_data(&mut self, data: Data) -> Result<(), ProtoError> {
        data.block_no(self.blocks_per_slice)?;

        self.received += 1;
        self.highest_seq = Some(self.highest_seq.unwrap_or(0).max(data.seq));

        let floor_advanced = data.emit_floor > self.emit_floor;
        self.emit_floor = self.emit_floor.max(data.emit_floor);

        if !self
            .assembler
            .insert(data.slice_no, data.block_in_slice, data.payload.into())
        {
            self.duplicates += 1;
        }

        self.write_ready().await?;

        if floor_advanced {
            self.request_gaps().await?;
        }
        Ok(())
    }

    async fn on_done(&mut self, done: Done) -> Result<(), ProtoError> {
        self.total_bytes = Some(done.total_bytes);
        self.assembler.on_done(done.total_blocks);

        self.write_ready().await?;
        self.request_gaps().await
    }

    async fn write_ready(&mut self) -> Result<(), ProtoError> {
        for block in self.assembler.take_ready() {
            let block = self.within_total(block);
            if block.is_empty() {
                continue;
            }

            let handed_over = Instant::now();
            self.blocks
                .send(block)
                .await
                .map_err(|_| ProtoError::SinkClosed)?;
            self.sink_stall += handed_over.elapsed();
            self.blocks_written += 1;
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
            total_expected: self.highest_seq.map_or(0, |seq| seq + 1),
            next_needed_slice: self.assembler.next_needed(),
            sink_stall_ms: std::mem::take(&mut self.sink_stall)
                .as_millis()
                .min(u32::MAX as u128) as u32,
        });

        self.socket.send_to(message, self.sender).await
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
            self.socket.send_to(message, self.sender).await?;
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
            expected: self.highest_seq.map_or(0, |seq| seq + 1),
            duplicates: self.duplicates,
            naks_sent: self.naks.total(),
        }
    }
}
