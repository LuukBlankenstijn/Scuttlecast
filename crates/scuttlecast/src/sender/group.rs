use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::{HOLDOFF, state::ReceiverState};
use proto::{Nak, Stats};

struct Participant {
    address: SocketAddr,
    next_needed: u32,
    reached_end: bool,
    last_seen: Option<Instant>,
    last_report: Option<(u64, u64)>,
    windowed_loss: f64,
    cumulative_loss: f64,
    naks: u64,
    sink_stall_ms: u32,
}

impl Participant {
    fn new(address: SocketAddr) -> Self {
        Self {
            address,
            next_needed: 0,
            reached_end: false,
            last_seen: None,
            last_report: None,
            windowed_loss: 0.0,
            cumulative_loss: 0.0,
            naks: 0,
            sink_stall_ms: 0,
        }
    }
}

#[derive(Default)]
pub struct Group {
    participants: HashMap<u64, Participant>,
    needed_from: u32,
    emitted: u32,
    total_slices: Option<u32>,
    suppressed: HashMap<(u32, u16), Instant>,
}

impl Group {
    /// Returns true if the receiver was not already a participant
    pub fn join(&mut self, receiver_id: u64, address: SocketAddr) -> bool {
        self.participants
            .insert(receiver_id, Participant::new(address))
            .is_none()
    }

    /// Returns true if the receiver was a participant
    pub fn leave(&mut self, receiver_id: u64) -> bool {
        self.participants.remove(&receiver_id).is_some()
    }

    pub fn contains(&self, receiver_id: u64) -> bool {
        self.participants.contains_key(&receiver_id)
    }

    pub fn len(&self) -> usize {
        self.participants.len()
    }

    pub fn all_complete(&self) -> bool {
        !self.participants.is_empty()
            && self
                .participants
                .values()
                .all(|participant| participant.reached_end)
    }

    pub fn complete_count(&self) -> usize {
        self.participants
            .values()
            .filter(|participant| participant.reached_end)
            .count()
    }

    pub fn mark_all_seen(&mut self, now: Instant) {
        for participant in self.participants.values_mut() {
            participant.last_seen = Some(now);
        }
    }

    pub fn mark_seen(&mut self, receiver_id: u64, now: Instant) {
        if let Some(participant) = self.participants.get_mut(&receiver_id) {
            participant.last_seen = Some(now);
        }
    }

    /// Removes participants silent for longer than `timeout`, returning each
    /// dropped id with the slice it was stuck at
    pub fn reap_silent(&mut self, now: Instant, timeout: Duration) -> Vec<(u64, u32)> {
        let stale: Vec<(u64, u32)> = self
            .participants
            .iter()
            .filter(|(_, participant)| {
                participant
                    .last_seen
                    .is_some_and(|seen| now.duration_since(seen) > timeout)
            })
            .map(|(receiver_id, participant)| (*receiver_id, participant.next_needed))
            .collect();
        for (receiver_id, _) in &stale {
            self.participants.remove(receiver_id);
        }
        stale
    }

    /// Windowed change in `(received, expected)` since the participant's last
    /// report, or `None` for a first, duplicate, or reordered report
    pub fn report_delta(&mut self, stats: &Stats) -> Option<(u64, u64)> {
        let participant = self.participants.get_mut(&stats.receiver_id)?;
        let current = (stats.total_received, stats.total_expected);
        participant.sink_stall_ms = stats.sink_stall_ms;
        if current.1 > 0 {
            participant.cumulative_loss = 1.0 - current.0 as f64 / current.1 as f64;
        }
        let delta = participant.last_report.and_then(|(seen, expected)| {
            (current.1 > expected && current.0 >= seen)
                .then(|| (current.0 - seen, current.1 - expected))
        });
        if participant.last_report.is_none() || delta.is_some() {
            participant.last_report = Some(current);
        }
        if let Some((seen, expected)) = delta {
            participant.windowed_loss = 1.0 - seen as f64 / expected as f64;
        }
        delta
    }

    /// A snapshot of every participant for reporting, slowest last
    pub fn rows(&self) -> Vec<ReceiverState> {
        let mut rows: Vec<_> = self
            .participants
            .iter()
            .map(|(receiver_id, participant)| ReceiverState {
                receiver_id: *receiver_id,
                address: participant.address,
                windowed_loss: participant.windowed_loss,
                lifetime_loss: participant.cumulative_loss,
                next_needed_slice: participant.next_needed,
                slices_behind: self.slices_behind(participant),
                naks: participant.naks,
                sink_stall_ms: participant.sink_stall_ms,
            })
            .collect();

        rows.sort_by_key(|row| row.slices_behind);
        rows
    }

    /// The participant the retransmit window is waiting on
    pub fn slowest_participant(&self) -> Option<(u64, u32)> {
        self.participants
            .iter()
            .min_by_key(|(_, participant)| participant.next_needed)
            .map(|(receiver_id, participant)| (*receiver_id, self.slices_behind(participant)))
    }

    fn slices_behind(&self, participant: &Participant) -> u32 {
        self.emitted.saturating_sub(participant.next_needed)
    }

    /// Records how many slices the transfer turned out to have. Progress
    /// reported before this was known says nothing about completion: a
    /// receiver cannot finish a transfer whose end it has not heard about, so
    /// every participant has to confirm again afterwards.
    pub fn on_eof(&mut self, total_slices: u32) {
        self.total_slices = Some(total_slices);
        for participant in self.participants.values_mut() {
            participant.reached_end = false;
        }
    }

    /// Returns the lowest slice the group still wants, if that advanced
    pub fn on_stats(&mut self, stats: &Stats) -> Option<u32> {
        let reached_end = self
            .total_slices
            .is_some_and(|total_slices| stats.next_needed_slice >= total_slices);

        let participant = self.participants.get_mut(&stats.receiver_id)?;
        participant.next_needed = stats.next_needed_slice;
        participant.reached_end = reached_end;

        self.advance()
    }

    /// Returns the shards that are worth putting back on the wire
    pub fn on_nak(&mut self, nak: &Nak, now: Instant) -> Vec<u16> {
        if !self.contains(nak.receiver_id) || self.nobody_needs(nak.slice_no) {
            return Vec::new();
        }
        if let Some(participant) = self.participants.get_mut(&nak.receiver_id) {
            participant.naks += 1;
        }

        let mut wanted = Vec::new();
        for &shard in &nak.missing {
            if self.suppress((nak.slice_no, shard), now) {
                wanted.push(shard);
            }
        }
        wanted
    }

    pub fn on_emitted(&mut self, slices: u32) {
        self.emitted = self.emitted.max(slices);
    }

    fn advance(&mut self) -> Option<u32> {
        let needed_from = self.slowest();
        if needed_from <= self.needed_from {
            return None;
        }
        self.needed_from = needed_from;
        self.suppressed
            .retain(|(slice_no, _), _| *slice_no >= needed_from);

        Some(needed_from)
    }

    fn slowest(&self) -> u32 {
        self.participants
            .values()
            .map(|participant| participant.next_needed)
            .min()
            .unwrap_or(self.needed_from)
    }

    fn nobody_needs(&self, slice_no: u32) -> bool {
        slice_no < self.needed_from
    }

    /// Records the block as requested, returning true if it was not suppressed
    fn suppress(&mut self, block: (u32, u16), now: Instant) -> bool {
        let suppressed = self
            .suppressed
            .get(&block)
            .is_some_and(|sent| now.duration_since(*sent) < HOLDOFF);

        if !suppressed {
            self.suppressed.insert(block, now);
        }
        !suppressed
    }
}

#[cfg(test)]
mod tests {
    use super::{Group, HOLDOFF};
    use proto::{Nak, Stats};
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    fn address(receiver_id: u64) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9000 + receiver_id as u16))
    }

    fn group(receiver_ids: &[u64]) -> Group {
        let mut group = Group::default();
        for &receiver_id in receiver_ids {
            group.join(receiver_id, address(receiver_id));
        }
        group
    }

    fn stats(receiver_id: u64, next_needed_slice: u32) -> Stats {
        Stats {
            transfer_id: 1,
            receiver_id,
            total_received: 0,
            total_expected: 0,
            next_needed_slice,
            sink_stall_ms: 0,
        }
    }

    fn report(receiver_id: u64, total_received: u64, total_expected: u64) -> Stats {
        Stats {
            transfer_id: 1,
            receiver_id,
            total_received,
            total_expected,
            next_needed_slice: 0,
            sink_stall_ms: 0,
        }
    }

    fn nak(receiver_id: u64, slice_no: u32, missing: Vec<u16>) -> Nak {
        Nak {
            transfer_id: 1,
            receiver_id,
            slice_no,
            missing,
        }
    }

    #[test]
    fn joining_twice_is_not_a_new_participant() {
        let mut group = Group::default();

        assert!(group.join(7, address(7)));
        assert!(!group.join(7, address(7)));
        assert_eq!(group.len(), 1);
    }

    #[test]
    fn leaving_reports_whether_it_removed_anyone() {
        let mut group = group(&[7]);

        assert!(group.leave(7));
        assert!(!group.leave(7));
        assert!(!group.contains(7));
    }

    #[test]
    fn the_group_needs_whatever_its_slowest_member_needs() {
        let mut group = group(&[7, 8]);

        assert_eq!(group.on_stats(&stats(7, 4)), None);
        assert_eq!(group.on_stats(&stats(8, 1)), Some(1));
    }

    #[test]
    fn a_silent_member_holds_the_group_back() {
        let mut group = group(&[7, 8]);

        assert_eq!(group.on_stats(&stats(7, 4)), None);
        assert_eq!(group.on_stats(&stats(8, 0)), None);
    }

    #[test]
    fn a_member_falling_behind_does_not_move_the_group_back() {
        let mut group = group(&[7]);
        assert_eq!(group.on_stats(&stats(7, 4)), Some(4));

        assert_eq!(group.on_stats(&stats(7, 2)), None);
    }

    #[test]
    fn a_departing_member_stops_holding_the_group_back() {
        let mut group = group(&[7, 8]);
        group.on_stats(&stats(7, 4));
        group.on_stats(&stats(8, 0));

        group.leave(8);

        assert_eq!(group.on_stats(&stats(7, 4)), Some(4));
    }

    #[test]
    fn stats_from_a_stranger_are_ignored() {
        let mut group = group(&[7]);

        assert_eq!(group.on_stats(&stats(99, 4)), None);
        assert_eq!(group.on_stats(&stats(7, 1)), Some(1));
    }

    #[test]
    fn an_empty_group_is_not_complete() {
        assert!(!Group::default().all_complete());
    }

    #[test]
    fn nobody_is_complete_before_the_transfer_has_an_end() {
        let mut group = group(&[7]);

        group.on_stats(&stats(7, 9));

        assert!(!group.all_complete());
    }

    #[test]
    fn the_group_is_complete_once_every_member_reaches_the_end() {
        let mut group = group(&[7, 8]);
        group.on_eof(4);

        group.on_stats(&stats(7, 4));
        assert!(!group.all_complete());

        group.on_stats(&stats(8, 4));
        assert!(group.all_complete());
    }

    #[test]
    fn a_member_short_of_the_end_leaves_the_group_incomplete() {
        let mut group = group(&[7]);
        group.on_eof(4);

        group.on_stats(&stats(7, 3));

        assert!(!group.all_complete());
    }

    #[test]
    fn requests_are_served_once_per_suppression_window() {
        let mut group = group(&[7]);
        let now = Instant::now();

        assert_eq!(group.on_nak(&nak(7, 3, vec![1, 2]), now), vec![1, 2]);
        assert_eq!(group.on_nak(&nak(7, 3, vec![1, 2]), now), Vec::<u16>::new());
        assert_eq!(
            group.on_nak(&nak(7, 3, vec![1, 2]), now + HOLDOFF),
            vec![1, 2]
        );
    }

    #[test]
    fn a_repeated_block_within_one_request_is_served_once() {
        let mut group = group(&[7]);

        assert_eq!(
            group.on_nak(&nak(7, 3, vec![1, 1]), Instant::now()),
            vec![1]
        );
    }

    #[test]
    fn requests_for_slices_nobody_needs_are_dropped() {
        let mut group = group(&[7]);
        group.on_stats(&stats(7, 4));

        assert_eq!(
            group.on_nak(&nak(7, 3, vec![0]), Instant::now()),
            Vec::<u16>::new()
        );
        assert_eq!(group.on_nak(&nak(7, 4, vec![0]), Instant::now()), vec![0]);
    }

    #[test]
    fn requests_from_a_stranger_are_dropped() {
        let mut group = group(&[7]);

        assert_eq!(
            group.on_nak(&nak(99, 3, vec![0]), Instant::now()),
            Vec::<u16>::new()
        );
    }

    #[test]
    fn advancing_past_a_slice_forgets_its_suppressed_blocks() {
        let mut group = group(&[7]);
        let now = Instant::now();
        group.on_nak(&nak(7, 3, vec![0]), now);
        group.on_nak(&nak(7, 4, vec![0]), now);

        group.on_stats(&stats(7, 4));

        assert_eq!(group.suppressed.len(), 1);
        assert_eq!(group.on_nak(&nak(7, 4, vec![0]), now), Vec::<u16>::new());
    }

    #[test]
    fn the_first_report_only_establishes_a_baseline() {
        let mut group = group(&[7]);

        assert_eq!(group.report_delta(&report(7, 100, 100)), None);
    }

    #[test]
    fn successive_reports_yield_the_windowed_delta() {
        let mut group = group(&[7]);
        group.report_delta(&report(7, 100, 100));

        assert_eq!(group.report_delta(&report(7, 190, 200)), Some((90, 100)));
    }

    #[test]
    fn a_repeated_identical_report_yields_no_delta() {
        let mut group = group(&[7]);
        group.report_delta(&report(7, 100, 100));

        assert_eq!(group.report_delta(&report(7, 100, 100)), None);
    }

    #[test]
    fn a_report_that_went_backwards_yields_no_delta() {
        let mut group = group(&[7]);
        group.report_delta(&report(7, 190, 200));

        assert_eq!(group.report_delta(&report(7, 100, 100)), None);
    }

    #[test]
    fn a_backward_report_does_not_move_the_baseline() {
        let mut group = group(&[7]);
        group.report_delta(&report(7, 190, 200));
        group.report_delta(&report(7, 100, 100));

        assert_eq!(group.report_delta(&report(7, 290, 300)), Some((100, 100)));
    }

    #[test]
    fn the_reported_rows_carry_both_loss_measures() {
        let mut group = group(&[7]);
        group.report_delta(&report(7, 150, 200));
        group.report_delta(&report(7, 200, 300));

        let row = &group.rows()[0];

        assert_eq!(row.lifetime_loss, 1.0 - 200.0 / 300.0);
        assert_eq!(row.windowed_loss, 0.5);
    }

    #[test]
    fn a_participant_silent_past_the_timeout_is_reaped() {
        let mut group = group(&[7, 8]);
        let start = Instant::now();
        group.mark_all_seen(start);
        group.on_stats(&stats(7, 5));
        group.mark_seen(8, start + Duration::from_secs(2));

        let reaped = group.reap_silent(start + Duration::from_secs(3), Duration::from_secs(1));

        assert_eq!(reaped, vec![(7, 5)]);
        assert!(!group.contains(7));
        assert!(group.contains(8));
    }

    #[test]
    fn a_recently_seen_participant_survives_reaping() {
        let mut group = group(&[7]);
        let start = Instant::now();
        group.mark_all_seen(start);

        let reaped = group.reap_silent(start + Duration::from_millis(500), Duration::from_secs(1));

        assert!(reaped.is_empty());
        assert!(group.contains(7));
    }
}
