use std::collections::HashMap;
use std::time::{Duration, Instant};

use proto::{Nack, Stats};

/// How long a resent block suppresses further requests for it. Retransmits get
/// lost too, so suppression has to expire or a second loss wedges the transfer.
const SUPPRESSION: Duration = Duration::from_millis(400);

#[derive(Default)]
struct Participant {
    completed_through: Option<u32>,
    complete: bool,
}

#[derive(Default)]
pub struct Group {
    participants: HashMap<u64, Participant>,
    completed_through: Option<u32>,
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
                .values()
                .all(|participant| participant.complete)
    }

    /// Returns the slice the whole group has completed, if that advanced
    pub fn on_stats(&mut self, stats: &Stats) -> Option<u32> {
        self.participants
            .get_mut(&stats.receiver_id)?
            .completed_through = stats.completed_through;

        self.advance()
    }

    /// Returns true if the whole group now holds the transfer
    pub fn on_complete(&mut self, receiver_id: u64) -> bool {
        match self.participants.get_mut(&receiver_id) {
            Some(participant) => participant.complete = true,
            None => return false,
        }

        self.all_complete()
    }

    /// Returns the blocks that are worth putting back on the wire
    pub fn on_nack(&mut self, nack: &Nack, now: Instant) -> Vec<u16> {
        if !self.contains(nack.receiver_id) || self.is_completed(nack.slice_no) {
            return Vec::new();
        }

        let mut wanted = Vec::new();
        for &block in &nack.missing {
            if self.suppress((nack.slice_no, block), now) {
                wanted.push(block);
            }
        }
        wanted
    }

    fn advance(&mut self) -> Option<u32> {
        let completed_through = self.slowest();
        if completed_through <= self.completed_through {
            return None;
        }
        self.completed_through = completed_through;

        if let Some(completed_through) = completed_through {
            self.suppressed
                .retain(|(slice_no, _), _| *slice_no > completed_through);
        }
        completed_through
    }

    /// A participant holding the whole transfer never needs a slice retained
    fn slowest(&self) -> Option<u32> {
        self.participants
            .values()
            .filter(|participant| !participant.complete)
            .map(|participant| participant.completed_through)
            .min()
            .flatten()
    }

    fn is_completed(&self, slice_no: u32) -> bool {
        self.completed_through
            .is_some_and(|completed_through| slice_no <= completed_through)
    }

    /// Records the block as requested, returning true if it was not suppressed
    fn suppress(&mut self, block: (u32, u16), now: Instant) -> bool {
        let suppressed = self
            .suppressed
            .get(&block)
            .is_some_and(|sent| now.duration_since(*sent) < SUPPRESSION);

        if !suppressed {
            self.suppressed.insert(block, now);
        }
        !suppressed
    }
}

#[cfg(test)]
mod tests {
    use super::{Group, SUPPRESSION};
    use proto::{Nack, Stats};
    use std::time::Instant;

    fn group(receiver_ids: &[u64]) -> Group {
        let mut group = Group::default();
        for &receiver_id in receiver_ids {
            group.join(receiver_id);
        }
        group
    }

    fn stats(receiver_id: u64, completed_through: Option<u32>) -> Stats {
        Stats {
            transfer_id: 1,
            receiver_id,
            blocks_received: 0,
            blocks_expected: 0,
            completed_through,
        }
    }

    fn nack(receiver_id: u64, slice_no: u32, missing: Vec<u16>) -> Nack {
        Nack {
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
    fn the_group_completes_no_further_than_its_slowest_member() {
        let mut group = group(&[7, 8]);

        assert_eq!(group.on_stats(&stats(7, Some(4))), None);
        assert_eq!(group.on_stats(&stats(8, Some(1))), Some(1));
    }

    #[test]
    fn a_silent_member_holds_the_group_back() {
        let mut group = group(&[7, 8]);

        assert_eq!(group.on_stats(&stats(7, Some(4))), None);
        assert_eq!(group.on_stats(&stats(8, None)), None);
    }

    #[test]
    fn a_member_falling_behind_does_not_move_the_group_back() {
        let mut group = group(&[7]);
        assert_eq!(group.on_stats(&stats(7, Some(4))), Some(4));

        assert_eq!(group.on_stats(&stats(7, Some(2))), None);
    }

    #[test]
    fn a_departing_member_stops_holding_the_group_back() {
        let mut group = group(&[7, 8]);
        group.on_stats(&stats(7, Some(4)));
        group.on_stats(&stats(8, None));

        group.leave(8);

        assert_eq!(group.on_stats(&stats(7, Some(4))), Some(4));
    }

    #[test]
    fn a_finished_member_stops_holding_the_group_back() {
        let mut group = group(&[7, 8]);
        group.on_stats(&stats(7, Some(4)));
        group.on_stats(&stats(8, Some(1)));

        group.on_complete(8);

        assert_eq!(group.on_stats(&stats(7, Some(4))), Some(4));
    }

    #[test]
    fn stats_from_a_stranger_are_ignored() {
        let mut group = group(&[7]);

        assert_eq!(group.on_stats(&stats(99, Some(4))), None);
        assert_eq!(group.on_stats(&stats(7, Some(1))), Some(1));
    }

    #[test]
    fn an_empty_group_is_not_complete() {
        assert!(!Group::default().all_complete());
    }

    #[test]
    fn the_group_is_complete_once_every_member_says_so() {
        let mut group = group(&[7, 8]);

        assert!(!group.on_complete(7));
        assert!(group.on_complete(8));
        assert!(group.all_complete());
    }

    #[test]
    fn a_stranger_cannot_complete_the_group() {
        let mut group = group(&[7]);

        assert!(!group.on_complete(99));
        assert!(!group.all_complete());
    }

    #[test]
    fn requests_are_served_once_per_suppression_window() {
        let mut group = group(&[7]);
        let now = Instant::now();

        assert_eq!(group.on_nack(&nack(7, 3, vec![1, 2]), now), vec![1, 2]);
        assert_eq!(
            group.on_nack(&nack(7, 3, vec![1, 2]), now),
            Vec::<u16>::new()
        );
        assert_eq!(
            group.on_nack(&nack(7, 3, vec![1, 2]), now + SUPPRESSION),
            vec![1, 2]
        );
    }

    #[test]
    fn a_repeated_block_within_one_request_is_served_once() {
        let mut group = group(&[7]);

        assert_eq!(
            group.on_nack(&nack(7, 3, vec![1, 1]), Instant::now()),
            vec![1]
        );
    }

    #[test]
    fn requests_for_completed_slices_are_dropped() {
        let mut group = group(&[7]);
        group.on_stats(&stats(7, Some(3)));

        assert_eq!(
            group.on_nack(&nack(7, 3, vec![0]), Instant::now()),
            Vec::<u16>::new()
        );
        assert_eq!(group.on_nack(&nack(7, 4, vec![0]), Instant::now()), vec![0]);
    }

    #[test]
    fn requests_from_a_stranger_are_dropped() {
        let mut group = group(&[7]);

        assert_eq!(
            group.on_nack(&nack(99, 3, vec![0]), Instant::now()),
            Vec::<u16>::new()
        );
    }

    #[test]
    fn completing_a_slice_forgets_its_suppressed_blocks() {
        let mut group = group(&[7]);
        let now = Instant::now();
        group.on_nack(&nack(7, 3, vec![0]), now);
        group.on_nack(&nack(7, 4, vec![0]), now);

        group.on_stats(&stats(7, Some(3)));

        assert_eq!(group.suppressed.len(), 1);
        assert_eq!(group.on_nack(&nack(7, 4, vec![0]), now), Vec::<u16>::new());
    }
}
