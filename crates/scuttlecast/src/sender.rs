use std::{
    net::Ipv4Addr,
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use bon::Builder;
use proto::{Data, Done, Evicted, Hello, Message};
use tokio::{io::AsyncRead, sync::mpsc, time::Instant};
use tracing::{debug, warn};

use crate::{
    LIVENESS_TIMEOUT, STATS_INTERVAL,
    error::ProtoError,
    sender::group::Group,
    sender::pacer::{Pacer, RateController, TICK_INTERVAL},
    sender::slicer::Slicer,
    sender::slicer::channel::{Feedback, Outbound},
    transport::{Losing, MessageSocket},
};

mod group;
mod pacer;
mod slicer;

const DEFAULT_BLOCKS_PER_SLICE: NonZeroU16 = NonZeroU16::new(32).expect("nonzero");
const DEFAULT_MAX_LIVE_SLICES: NonZeroU16 = NonZeroU16::new(8).expect("nonzero");

#[derive(Builder)]
pub struct Sender {
    #[builder(with = |local_ip: Ipv4Addr, group_ip: Ipv4Addr, port: u16,| -> Result<_, ProtoError> {
        MessageSocket::sending(local_ip, group_ip, port)
    } )]
    socket: MessageSocket,
    #[builder(default = Duration::new(5 * 60, 0))]
    max_wait: Duration,
    min_receivers: Option<usize>,
    #[builder(default = DEFAULT_BLOCKS_PER_SLICE)]
    blocks_per_slice: NonZeroU16,
    #[builder(default = DEFAULT_MAX_LIVE_SLICES)]
    max_live_slices: NonZeroU16,
}

impl Sender {
    /// Applies a loss rule to this sender's socket, so a test can decide
    /// exactly which replies it never sees
    pub fn losing(mut self, losing: Losing) -> Self {
        self.socket = self.socket.losing(losing);
        self
    }

    pub async fn send_file(&self, path: PathBuf) -> Result<(), ProtoError> {
        let stream = tokio::fs::File::open(path)
            .await
            .map_err(ProtoError::File)?;
        self.send_stream(stream).await?;
        Ok(())
    }

    pub async fn send_stream(
        &self,
        reader: impl AsyncRead + Unpin + Send + 'static,
    ) -> Result<(), ProtoError> {
        let transfer_id = rand::random();

        let mut group = self.gather_participants(transfer_id).await?;
        if group.len() == 0 {
            return Err(ProtoError::NoParticipants);
        }
        let participants_at_start = group.len();
        debug!("starting send with {participants_at_start} participants");
        group.mark_all_seen(std::time::Instant::now());

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Outbound>(64);
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel::<Feedback>();
        let max_live_slices =
            NonZeroUsize::new(self.max_live_slices.get() as usize).expect("nonzero");
        let slicer = Slicer::new(self.blocks_per_slice, max_live_slices);
        let slicer_task = tokio::spawn(slicer.run(reader, outbound_tx, feedback_rx));

        let mut seq: u64 = 0;
        let mut total_bytes = 0u64;
        let mut total_blocks = 0u64;
        let mut draining = false;
        let mut drain_deadline: Option<Instant> = None;
        let mut rate_controller = RateController::new();
        let mut pacer = Pacer::new();
        let mut tick = tokio::time::interval(TICK_INTERVAL);

        let result = loop {
            if group.len() == 0 {
                break Err(ProtoError::NoParticipants);
            }
            if draining && group.all_complete() {
                break Ok(());
            }

            let rate = rate_controller.rate();
            pacer.refill(rate);
            let has_credit = pacer.has_credit();
            let until_credit = pacer.time_until_credit(rate);
            let drain_left =
                drain_deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));

            tokio::select! {
                outbound = outbound_rx.recv(), if has_credit => match outbound {
                    Some(Outbound::Block { slice_no, block_in_slice, emit_floor, payload }) => {
                        pacer.consume();
                        self.socket.send_to_group(Message::Data(Data {
                            transfer_id,
                            seq,
                            slice_no,
                            block_in_slice,
                            emit_floor,
                            payload: payload.into(),
                        })).await?;
                        seq += 1;
                    }
                    Some(Outbound::Eof { total_bytes: bytes, total_blocks: blocks }) => {
                        total_bytes = bytes;
                        total_blocks = blocks;
                        let total_slices =
                            blocks.div_ceil(self.blocks_per_slice.get() as u64) as u32;
                        group.on_eof(total_slices);
                        draining = true;
                        drain_deadline = Some(Instant::now() + self.max_wait);
                        self.socket.send_to_group(Message::Done(Done {
                            transfer_id,
                            total_bytes,
                            total_blocks,
                        })).await?;
                    }
                    None => break Err(ProtoError::EgressClosed),
                },

                _ = tokio::time::sleep(until_credit), if !has_credit => {}

                _ = tick.tick() => {
                    rate_controller.tick(pacer.take_starvation());
                    let now = std::time::Instant::now();
                    for (target, stuck_at) in group.reap_silent(now, LIVENESS_TIMEOUT) {
                        let reason = format!(
                            "silent for {LIVENESS_TIMEOUT:?} while stuck at slice {stuck_at}"
                        );
                        warn!(target, stuck_at, "evicting silent participant");
                        self.socket.send_to_group(Message::Evicted(Evicted {
                            transfer_id,
                            target,
                            reason,
                        })).await?;
                    }
                    if draining {
                        self.socket.send_to_group(Message::Done(Done {
                            transfer_id,
                            total_bytes,
                            total_blocks,
                        })).await?;
                    }
                }

                _ = tokio::time::sleep(drain_left.unwrap_or_default()), if drain_left.is_some() => {
                    break Err(ProtoError::TransferIncomplete {
                        complete: group.complete_count(),
                        participants: participants_at_start,
                    });
                }

                m = self.socket.recv_in_transfer(transfer_id) => {
                    let (message, _) = m?;
                    match message {
                        Message::Stats(stats) => {
                            if !group.contains(stats.receiver_id) {
                                continue;
                            }
                            group.mark_seen(stats.receiver_id, std::time::Instant::now());
                            if let Some(needed_from) = group.on_stats(&stats) {
                                let _ = feedback_tx.send(Feedback::Needed(needed_from));
                            }
                            if let Some((seen, expected)) = group.report_delta(&stats) {
                                rate_controller.on_report(stats.receiver_id, seen, expected);
                            }
                        }
                        Message::Nak(nak) => {
                            let blocks = group.on_nak(&nak, std::time::Instant::now());
                            if !blocks.is_empty() {
                                let _ = feedback_tx.send(Feedback::Resend {
                                    slice_no: nak.slice_no,
                                    blocks,
                                });
                            }
                        }
                        Message::Leave { receiver_id, .. } => {
                            group.leave(receiver_id);
                        }
                        _ => {}
                    }
                }
            }
        };

        let _ = feedback_tx.send(Feedback::Done);
        let _ = slicer_task.await;
        result
    }

    async fn gather_participants(&self, transfer_id: u64) -> Result<Group, ProtoError> {
        let mut group = Group::default();
        let start = Instant::now();
        let deadline = start + self.max_wait;
        let mut hello_tick = tokio::time::interval(STATS_INTERVAL);

        loop {
            tokio::select! {
                _ = hello_tick.tick() => {
                    self.socket
                        .send_to_group(Message::Hello(Hello {
                            transfer_id,
                            blocks_per_slice: self.blocks_per_slice,
                            parity_per_slice: 0,
                            max_live_slices: self.max_live_slices,
                        }))
                        .await?
                }

                r = self.socket.recv_in_transfer(transfer_id) => {
                    let (message, socket) = r?;
                    if let Message::Join { receiver_id, .. } = message && group.join(receiver_id) {
                        tracing::debug!(socket=%socket, receiver_id, "receiver joined");
                    }
                    if let Message::Leave { receiver_id, .. } = message && group.leave(receiver_id) {
                        tracing::debug!(socket=%socket, receiver_id, "receiver left");
                    }
                },

                _ = tokio::time::sleep_until(deadline) => break,
            }

            if Some(group.len()) >= self.min_receivers {
                break;
            }
        }
        Ok(group)
    }
}
