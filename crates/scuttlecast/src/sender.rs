use std::{
    net::Ipv4Addr,
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use bon::Builder;
use proto::{Data, Done, Evicted, Hello, Message, Parity};
use tokio::{
    io::AsyncRead,
    sync::{mpsc, watch},
    time::Instant,
};
use tracing::{debug, warn};

use crate::{
    SILENCE_TIMEOUT, STATS_INTERVAL,
    error::ProtoError,
    sender::group::Group,
    sender::pacer::{Pacer, REPAIR_THRESHOLD, RateController, TICK_INTERVAL},
    sender::slicer::Slicer,
    sender::slicer::channel::{Feedback, Outbound},
    state::{Bottleneck, TransferState},
    transport::{Losing, MessageSocket},
};

mod group;
mod pacer;
mod slicer;

const DEFAULT_BLOCKS_PER_SLICE: NonZeroU16 = NonZeroU16::new(32).expect("nonzero");
const DEFAULT_MAX_LIVE_SLICES: NonZeroU16 = NonZeroU16::new(512).expect("nonzero");
const DEFAULT_PARITY_PER_SLICE: u16 = 8;

/// Blocks taken from the slicer per pass through the send loop. Each pass
/// arms and drops a handful of timers, which at one block per pass cost more
/// than sending the block did.
const SEND_BATCH: usize = 256;

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
    #[builder(default = DEFAULT_PARITY_PER_SLICE)]
    parity_per_slice: u16,
    /// Blocks per second the sender will not exceed even when nothing is lost
    max_rate: Option<f64>,
    #[builder(default = watch::channel(TransferState::default()).0)]
    progress: watch::Sender<TransferState>,
}

impl Sender {
    /// Applies a loss rule to this sender's socket, so a test can decide
    /// exactly which replies it never sees
    pub fn losing(mut self, losing: Losing) -> Self {
        self.socket = self.socket.losing(losing);
        self
    }

    /// Follows what the transfer is doing and why it is not going faster
    pub fn progress(&self) -> watch::Receiver<TransferState> {
        self.progress.subscribe()
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
        let slicer = Slicer::new(
            self.blocks_per_slice,
            self.parity_per_slice,
            max_live_slices,
        )?;
        let slicer_task = tokio::spawn(slicer.run(reader, outbound_tx, feedback_rx));

        let mut seq: u64 = 0;
        let mut total_bytes = 0u64;
        let mut total_blocks = 0u64;
        let mut draining = false;
        let mut drain_deadline: Option<Instant> = None;
        let mut rate_controller = RateController::new().capped_at(self.max_rate);
        let mut pacer = Pacer::new();
        let mut tick = tokio::time::interval(TICK_INTERVAL);
        let mut blocks_sent = 0u64;
        let mut blocks_at_last_tick = 0u64;
        let mut slices_emitted = 0u32;
        let mut source_wait = Duration::ZERO;
        let mut batch = Vec::with_capacity(SEND_BATCH);

        let result = loop {
            if group.len() == 0 {
                break Err(ProtoError::NoParticipants);
            }
            if draining && group.all_complete() {
                break Ok(());
            }

            let rate = rate_controller.rate();
            pacer.refill(rate);
            let budget = pacer.budget().min(SEND_BATCH);
            let until_credit = pacer.time_until_credit(rate);
            let drain_left =
                drain_deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));

            let waited_since = Instant::now();
            tokio::select! {
                taken = outbound_rx.recv_many(&mut batch, budget), if budget > 0 => {
                    if taken == 0 {
                        break Err(ProtoError::EgressClosed);
                    }
                    source_wait += waited_since.elapsed();

                    for outbound in batch.drain(..) {
                        match outbound {
                            Outbound::Block { slice_no, block_in_slice, emit_floor, payload } => {
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
                                blocks_sent += 1;
                                slices_emitted = slices_emitted.max(emit_floor);
                                group.on_emitted(emit_floor);
                            }
                            Outbound::Parity { slice_no, parity_index, emit_floor, payload } => {
                                pacer.consume();
                                self.socket.send_to_group(Message::Parity(Parity {
                                    transfer_id,
                                    seq,
                                    slice_no,
                                    parity_index,
                                    emit_floor,
                                    payload: payload.into(),
                                })).await?;
                                seq += 1;
                                slices_emitted = slices_emitted.max(emit_floor);
                                group.on_emitted(emit_floor);
                            }
                            Outbound::Eof { total_bytes: bytes, total_blocks: blocks } => {
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
                        }
                    }
                },

                _ = tokio::time::sleep(until_credit), if budget == 0 => {}

                _ = tick.tick() => {
                    let starved = pacer.take_starvation();
                    rate_controller.tick(starved);
                    let now = std::time::Instant::now();
                    for (target, stuck_at) in group.reap_silent(now, SILENCE_TIMEOUT) {
                        let reason = format!(
                            "silent for {SILENCE_TIMEOUT:?} while stuck at slice {stuck_at}"
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

                    let sent_this_tick = blocks_sent - blocks_at_last_tick;
                    blocks_at_last_tick = blocks_sent;
                    if sent_this_tick == 0 && !draining {
                        self.socket.send_to_group(self.hello(transfer_id)).await?;
                    }
                    let limiting = self
                        .bottleneck(
                            &group,
                            &rate_controller,
                            std::mem::take(&mut source_wait),
                            sent_this_tick,
                            !starved,
                            draining,
                        )
                        .attribute();
                    self.progress.send_replace(TransferState {
                        transfer_id,
                        blocks_per_second: rate_controller.rate(),
                        blocks_sent,
                        slices_emitted,
                        total_blocks: draining.then_some(total_blocks),
                        draining,
                        limiting,
                        receivers: group.rows(),
                    });
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
                            if let Some(demand) = group.repair_demand(&stats) {
                                rate_controller.on_report(stats.receiver_id, demand);
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

    fn bottleneck(
        &self,
        group: &Group,
        rate_controller: &RateController,
        source_wait: Duration,
        sent_this_tick: u64,
        credit_unused: bool,
        draining: bool,
    ) -> Bottleneck {
        Bottleneck {
            slowest: group.slowest_participant(),
            worst_demand: rate_controller.worst(),
            demand_threshold: REPAIR_THRESHOLD,
            max_live_slices: self.max_live_slices.get() as u32,
            at_ceiling: rate_controller.at_ceiling(),
            source_wait: source_wait.max(TICK_INTERVAL / 2) - TICK_INTERVAL / 2,
            allowed_rate: rate_controller.rate(),
            achieved_rate: sent_this_tick as f64 / TICK_INTERVAL.as_secs_f64(),
            credit_unused,
            draining,
        }
    }

    fn hello(&self, transfer_id: u64) -> Message {
        Message::Hello(Hello {
            transfer_id,
            blocks_per_slice: self.blocks_per_slice,
            parity_per_slice: self.parity_per_slice,
            max_live_slices: self.max_live_slices,
        })
    }

    async fn gather_participants(&self, transfer_id: u64) -> Result<Group, ProtoError> {
        let mut group = Group::default();
        let start = Instant::now();
        let deadline = start + self.max_wait;
        let mut hello_tick = tokio::time::interval(STATS_INTERVAL);

        loop {
            tokio::select! {
                _ = hello_tick.tick() => self.socket.send_to_group(self.hello(transfer_id)).await?,

                r = self.socket.recv_in_transfer(transfer_id) => {
                    let (message, socket) = r?;
                    if let Message::Join { receiver_id, .. } = message && group.join(receiver_id, socket) {
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
