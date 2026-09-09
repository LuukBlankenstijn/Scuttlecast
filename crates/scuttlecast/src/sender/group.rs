use std::collections::HashMap;
use std::time::Instant;

use proto::{Nak, Stats};

use crate::HOLDOFF;

#[derive(Default)]
struct Participant {
    next_needed: u32,
}

#[derive(Default)]
pub struct Group {
    participants: HashMap<u64, Participant>,
    needed_from: u32,
    total_slices: Option<u32>,
    suppressed: HashMap<(u32, u16), Instant>,
}

impl Group {
    /// Returns true if the receiver was not already a participant
    pub fn join(&mut self, receiver_id: u64) -> bool {
        self.participants
            .insert(receiver_id, Participant::default())
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
                .keys()
                .all(|receiver_id| self.is_complete(*receiver_id))
    }

    /// Records how many slices the transfer turned out to have, which is what
    /// makes a participant's progress readable as completion
    pub fn on_eof(&mut self, total_slices: u32) {
        self.total_slices = Some(total_slices);
    }

    /// Returns the lowest slice the group still wants, if that advanced
    pub fn on_stats(&mut self, stats: &Stats) -> Option<u32> {
        self.participants.get_mut(&stats.receiver_id)?.next_needed = stats.next_needed_slice;

        self.advance()
    }

    fn is_complete(&self, receiver_id: u64) -> bool {
        let Some(total_slices) = self.total_slices else {
            return false;
        };

        self.participants
            .get(&receiver_id)
            .is_some_and(|participant| participant.next_needed >= total_slices)
    }

    /// Returns the blocks that are worth putting back on the wire
    pub fn on_nak(&mut self, nak: &Nak, now: Instant) -> Vec<u16> {
        if !self.contains(nak.receiver_id) || self.nobody_needs(nak.slice_no) {
            return Vec::new();
        }

        let mut wanted = Vec::new();
        for &block in &nak.missing {
            if self.suppress((nak.slice_no, block), now) {
                wanted.push(block);
            }
        }
        wanted
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
    use std::time::Instant;

    fn group(receiver_ids: &[u64]) -> Group {
        let mut group = Group::default();
        for &receiver_id in receiver_ids {
            group.join(receiver_id);
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

        assert!(group.join(7));
        assert!(!group.join(7));
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
}
