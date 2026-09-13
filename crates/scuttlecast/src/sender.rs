use std::{
    net::Ipv4Addr,
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use crate::proto::{Done, Evicted, Frame, FrameBatch, Hello, Message};
use bon::Builder;
use tokio::{
    io::AsyncRead,
    sync::{mpsc, watch},
    time::Instant,
};
use tracing::{debug, warn};

use crate::{
    SILENCE_TIMEOUT, STATS_INTERVAL,
    error::ProtoError,
    format::Smoothed,
    sender::group::Group,
    sender::pacer::{Batching, Pacer, REPAIR_THRESHOLD, RateController, TICK_INTERVAL},
    sender::slicer::Slicer,
    sender::slicer::channel::{Feedback, Outbound},
    state::{Bottleneck, TransferState},
    transport::{Losing, MessageSocket},
};

mod group;
mod pacer;
mod slicer;

const DEFAULT_BLOCKS_PER_SLICE: NonZeroU16 = NonZeroU16::new(32).expect("nonzero");
const DEFAULT_MAX_LIVE_SLICES: NonZeroU16 = NonZeroU16::new(2048).expect("nonzero");
const DEFAULT_BLOCK_SIZE: NonZeroU32 = NonZeroU32::new(crate::DEFAULT_BLOCK_SIZE).expect("nonzero");

const PARITY_HEADROOM: f64 = 2.0;

const SOURCE_WAIT_FLOOR: Duration = Duration::from_millis(TICK_INTERVAL.as_millis() as u64 / 8);

const SINK_STALL_FLOOR: u32 = STATS_INTERVAL.as_millis() as u32 / 4;
const DEFAULT_PARITY_PER_SLICE: u8 = 8;

const OUTBOUND_CAPACITY: usize = 4096;

const UNCAPPED_BATCH_SEGMENTS: usize = usize::MAX;

#[derive(Builder)]
pub struct Sender {
    #[builder(with = |local_ip: Ipv4Addr, group_ip: Ipv4Addr, port: u16,| -> Result<_, ProtoError> {
        MessageSocket::sending(local_ip, group_ip, port)
    } )]
    socket: MessageSocket,
    #[builder(default = Duration::new(5 * 60, 0))]
    max_wait: Duration,
    #[builder(default = 1)]
    min_receivers: usize,
    #[builder(default = DEFAULT_BLOCKS_PER_SLICE)]
    blocks_per_slice: NonZeroU16,
    #[builder(default = DEFAULT_MAX_LIVE_SLICES)]
    max_live_slices: NonZeroU16,
    #[builder(default = DEFAULT_PARITY_PER_SLICE)]
    parity_per_slice: u8,
    #[builder(default = DEFAULT_BLOCK_SIZE)]
    block_size: NonZeroU32,
    #[builder(default = UNCAPPED_BATCH_SEGMENTS)]
    max_batch_segments: usize,
    max_rate: Option<f64>,
    #[builder(default = watch::channel(TransferState::default()).0)]
    progress: watch::Sender<TransferState>,
}

impl Sender {
    pub fn losing(mut self, losing: Losing) -> Self {
        self.socket = self.socket.losing(losing);
        self
    }

    pub fn progress(&self) -> watch::Receiver<TransferState> {
        self.progress.subscribe()
    }

    pub async fn send_file(&self, path: PathBuf) -> Result<(), ProtoError> {
        let total_bytes = tokio::fs::metadata(&path)
            .await
            .map_err(ProtoError::File)?
            .len();
        let stream = tokio::fs::File::open(path)
            .await
            .map_err(ProtoError::File)?;
        self.send_stream(stream, Some(total_bytes)).await?;
        Ok(())
    }

    pub async fn send_stream(
        &self,
        reader: impl AsyncRead + Unpin + Send + 'static,
        announced_bytes: Option<u64>,
    ) -> Result<(), ProtoError> {
        let transfer_id = rand::random();

        let mut group = self
            .gather_participants(transfer_id, announced_bytes)
            .await?;
        if group.len() == 0 {
            return Err(ProtoError::NoParticipants);
        }
        let participants_at_start = group.len();
        debug!("starting send with {participants_at_start} participants");
        group.mark_all_seen(std::time::Instant::now());

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Outbound>(OUTBOUND_CAPACITY);
        let (feedback_tx, feedback_rx) = mpsc::unbounded_channel::<Feedback>();
        let max_live_slices =
            NonZeroUsize::new(self.max_live_slices.get() as usize).expect("nonzero");
        let slicer = Slicer::new(
            self.block_size,
            self.blocks_per_slice,
            self.parity_per_slice,
            max_live_slices,
        )?;
        let slicer_task = tokio::spawn(slicer.run(reader, outbound_tx, feedback_rx));

        let mut batching = Batching::default();
        let mut batch = FrameBatch::new(self.block_size.get() as usize, self.max_batch_segments);
        batch.narrow_to(batching.segments(self.max_batch_segments));
        let drain_limit = batch.capacity();

        let mut seq: u32 = 0;
        let mut total_bytes = 0u64;
        let mut total_blocks = 0u64;
        let mut draining = false;
        let mut covering = self.parity_per_slice;
        let mut drain_deadline: Option<Instant> = None;
        let mut rate_controller = RateController::new().capped_at(self.max_rate);
        let mut pacer = Pacer::new();
        let mut tick = tokio::time::interval(TICK_INTERVAL);
        let mut blocks_sent = 0u64;
        let mut blocks_at_last_tick = 0u64;
        let mut blocks_per_second = Smoothed::default();
        let started = std::time::Instant::now();
        let mut last_tick = started;
        let mut slices_emitted = 0u32;
        let mut source_wait = Duration::ZERO;
        let mut queued = Vec::with_capacity(drain_limit);

        let result = loop {
            if group.len() == 0 {
                break Err(ProtoError::NoParticipants);
            }
            if draining && group.all_complete() {
                break Ok(());
            }

            let rate = rate_controller.rate();
            pacer.refill(rate);
            let credit = pacer.budget();
            let budget = credit.min(drain_limit);
            let until_credit = pacer.time_until_credit(rate);
            let drain_left =
                drain_deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()));

            let waited_since = Instant::now();
            tokio::select! {
                taken = outbound_rx.recv_many(&mut queued, budget), if budget > 0 => {
                    if taken == 0 {
                        break Err(ProtoError::EgressClosed);
                    }
                    source_wait += waited_since.elapsed();
                    let pacer_was_binding = credit <= drain_limit;
                    if pacer_was_binding && taken == credit && !outbound_rx.is_empty() {
                        pacer.waited();
                    }

                    for outbound in queued.drain(..) {
                        match outbound {
                            Outbound::Shard { slice_no, slot, slice_parity, emit_floor, payload } => {
                                pacer.consume();
                                batch.push(&Frame {
                                    slot,
                                    slice_parity,
                                    transfer_id: transfer_id as u32,
                                    seq,
                                    slice_no,
                                    emit_floor,
                                    payload,
                                });
                                seq = seq.wrapping_add(1);
                                if slot < self.blocks_per_slice.get() {
                                    blocks_sent += 1;
                                }
                                slices_emitted = slices_emitted.max(emit_floor);
                                group.on_emitted(emit_floor);

                                if batch.is_full() {
                                    self.socket.send_batch(&mut batch).await?;
                                }
                            }
                            Outbound::Eof { total_bytes: bytes, total_blocks: blocks } => {
                                self.socket.send_batch(&mut batch).await?;
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
                    self.socket.send_batch(&mut batch).await?;
                },

                _ = tokio::time::sleep(until_credit), if budget == 0 => {
                    if !outbound_rx.is_empty() {
                        pacer.waited();
                    }
                }

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
                    let since_last_tick = (now - last_tick).as_secs_f64();
                    last_tick = now;
                    let achieved_rate = match since_last_tick > 0.0 {
                        true => sent_this_tick as f64 / since_last_tick,
                        false => blocks_per_second.current(),
                    };
                    if sent_this_tick == 0 && !draining {
                        self.socket
                            .send_to_group(self.hello(transfer_id, announced_bytes))
                            .await?;
                    }

                    let wanted = parity_for(
                        group.worst_loss(),
                        self.blocks_per_slice,
                        self.parity_per_slice,
                    );
                    if wanted != covering {
                        covering = wanted;
                        let _ = feedback_tx.send(Feedback::Cover(wanted));
                    }
                    batching.observe(group.worst_loss());
                    batch.narrow_to(batching.segments(self.max_batch_segments));

                    let limiting = self
                        .bottleneck(
                            &group,
                            &rate_controller,
                            std::mem::take(&mut source_wait),
                            achieved_rate,
                            !starved,
                            draining,
                        )
                        .attribute();
                    self.progress.send_replace(TransferState {
                        block_size: self.block_size.get(),
                        transfer_id,
                        blocks_per_second: blocks_per_second.observe(achieved_rate),
                        blocks_sent,
                        slices_emitted,
                        parity_shards: covering,
                        total_blocks: draining.then_some(total_blocks),
                        announced_bytes,
                        draining,
                        limiting,
                        receivers: group.rows(),
                        elapsed: now - started,
                    });
                }

                _ = tokio::time::sleep(drain_left.unwrap_or_default()), if drain_left.is_some() => {
                    break Err(ProtoError::TransferIncomplete {
                        complete: group.complete_count(),
                        participants: participants_at_start,
                    });
                }

                m = self.socket.recv_control(Some(transfer_id)) => {
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

        self.progress.send_modify(|state| {
            state.transfer_id = transfer_id;
            state.block_size = self.block_size.get();
            state.blocks_sent = blocks_sent;
            state.slices_emitted = slices_emitted;
            state.announced_bytes = announced_bytes;
            state.total_blocks = draining.then_some(total_blocks);
            state.draining = draining;
            state.elapsed = started.elapsed();
        });

        let _ = feedback_tx.send(Feedback::Done);
        let _ = slicer_task.await;
        result
    }

    fn bottleneck(
        &self,
        group: &Group,
        rate_controller: &RateController,
        source_wait: Duration,
        achieved_rate: f64,
        credit_unused: bool,
        draining: bool,
    ) -> Bottleneck {
        Bottleneck {
            slowest: group.slowest_participant(),
            worst_demand: rate_controller.worst(),
            demand_threshold: REPAIR_THRESHOLD,
            max_live_slices: self.max_live_slices.get() as u32,
            at_ceiling: rate_controller.at_ceiling(),
            source_wait: source_wait.saturating_sub(SOURCE_WAIT_FLOOR),
            worst_sink_stall: group.worst_sink_stall(),
            sink_stall_threshold: SINK_STALL_FLOOR,
            allowed_rate: rate_controller.rate(),
            achieved_rate,
            credit_unused,
            draining,
        }
    }

    fn hello(&self, transfer_id: u64, total_bytes: Option<u64>) -> Message {
        Message::Hello(Hello {
            transfer_id,
            block_size: self.block_size,
            blocks_per_slice: self.blocks_per_slice,
            parity_per_slice: self.parity_per_slice,
            max_live_slices: self.max_live_slices,
            total_bytes,
        })
    }

    async fn gather_participants(
        &self,
        transfer_id: u64,
        total_bytes: Option<u64>,
    ) -> Result<Group, ProtoError> {
        let mut group = Group::default();
        let start = Instant::now();
        let deadline = start + self.max_wait;
        let mut hello_tick = tokio::time::interval(STATS_INTERVAL);

        loop {
            tokio::select! {
                _ = hello_tick.tick() => {
                    self.socket
                        .send_to_group(self.hello(transfer_id, total_bytes))
                        .await?
                }

                r = self.socket.recv_control(Some(transfer_id)) => {
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

            if group.len() >= self.min_receivers {
                break;
            }
        }
        Ok(group)
    }
}

fn parity_for(loss: Option<f64>, blocks_per_slice: NonZeroU16, max_parity: u8) -> u8 {
    let Some(loss) = loss else {
        return max_parity;
    };

    let shards = loss * blocks_per_slice.get() as f64 * PARITY_HEADROOM;
    (shards.ceil() as u8).min(max_parity)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_BLOCKS_PER_SLICE, DEFAULT_MAX_LIVE_SLICES, STATS_INTERVAL, parity_for};
    use std::num::NonZeroU16;

    const BLOCK_SIZE: usize = crate::DEFAULT_BLOCK_SIZE as usize;

    #[test]
    fn the_default_window_and_report_interval_clear_a_multi_gigabit_link() {
        let window = DEFAULT_MAX_LIVE_SLICES.get() as f64
            * DEFAULT_BLOCKS_PER_SLICE.get() as f64
            * BLOCK_SIZE as f64;
        let ceiling = window / STATS_INTERVAL.as_secs_f64();

        assert!(
            ceiling > 6.25e8,
            "the defaults cap throughput at {:.2} Gbps",
            ceiling * 8.0 / 1e9
        );
    }

    fn blocks(count: u16) -> NonZeroU16 {
        NonZeroU16::new(count).expect("nonzero")
    }

    #[test]
    fn a_transfer_too_short_to_measure_keeps_its_full_width() {
        assert_eq!(parity_for(None, blocks(32), 8), 8);
        assert_eq!(parity_for(None, blocks(32), 0), 0);
    }

    #[test]
    fn a_clean_link_carries_no_parity() {
        assert_eq!(parity_for(Some(0.0), blocks(32), 8), 0);
    }

    #[test]
    fn a_single_lost_block_in_a_slice_is_worth_a_shard() {
        assert_eq!(parity_for(Some(1.0 / 32.0), blocks(32), 8), 2);
    }

    #[test]
    fn coverage_grows_with_the_loss_it_has_to_absorb() {
        assert_eq!(parity_for(Some(0.01), blocks(32), 8), 1);
        assert_eq!(parity_for(Some(0.05), blocks(32), 8), 4);
        assert_eq!(parity_for(Some(0.10), blocks(32), 8), 7);
    }

    #[test]
    fn loss_past_what_the_transfer_allows_for_is_capped() {
        assert_eq!(parity_for(Some(0.5), blocks(32), 8), 8);
        assert_eq!(parity_for(Some(1.0), blocks(32), 0), 0);
    }

    #[test]
    fn wider_slices_need_more_shards_for_the_same_loss() {
        assert_eq!(parity_for(Some(0.02), blocks(32), 16), 2);
        assert_eq!(parity_for(Some(0.02), blocks(128), 16), 6);
    }
}
