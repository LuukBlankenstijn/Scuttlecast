use std::collections::HashMap;
use std::time::Instant;

use crate::RENAK_INTERVAL;

#[derive(Default)]
pub(super) struct Naks {
    sent: HashMap<u32, Instant>,
    total: u64,
}

impl Naks {
    pub(super) fn total(&self) -> u64 {
        self.total
    }

    /// Keeps the slices worth asking about: ones never asked for, and ones
    /// whose last request is old enough that the answer was probably lost too
    pub(super) fn due(&mut self, gaps: Vec<(u32, Vec<u16>)>, now: Instant) -> Vec<(u32, Vec<u16>)> {
        let due: Vec<_> = gaps
            .into_iter()
            .filter(|(slice_no, missing)| !missing.is_empty() && self.is_due(*slice_no, now))
            .collect();

        for (slice_no, _) in &due {
            self.sent.insert(*slice_no, now);
        }
        self.total += due.len() as u64;

        due
    }

    pub(super) fn forget_below(&mut self, next_needed: u32) {
        self.sent.retain(|slice_no, _| *slice_no >= next_needed);
    }

    fn is_due(&self, slice_no: u32, now: Instant) -> bool {
        self.sent
            .get(&slice_no)
            .is_none_or(|sent| now.duration_since(*sent) >= RENAK_INTERVAL)
    }
}

#[cfg(test)]
mod tests {
    use super::Naks;
    use crate::RENAK_INTERVAL;
    use std::time::Instant;

    fn gaps() -> Vec<(u32, Vec<u16>)> {
        vec![(3, vec![1, 2])]
    }

    #[test]
    fn asks_for_a_slice_it_has_not_asked_about() {
        let mut naks = Naks::default();

        assert_eq!(naks.due(gaps(), Instant::now()), gaps());
    }

    #[test]
    fn does_not_ask_twice_within_the_repeat_interval() {
        let mut naks = Naks::default();
        let now = Instant::now();
        naks.due(gaps(), now);

        assert!(naks.due(gaps(), now).is_empty());
    }

    #[test]
    fn asks_again_once_the_repeat_interval_passes() {
        let mut naks = Naks::default();
        let now = Instant::now();
        naks.due(gaps(), now);

        assert_eq!(naks.due(gaps(), now + RENAK_INTERVAL), gaps());
    }

    #[test]
    fn ignores_a_slice_with_nothing_missing() {
        let mut naks = Naks::default();

        assert!(naks.due(vec![(3, Vec::new())], Instant::now()).is_empty());
    }

    #[test]
    fn asks_again_after_a_slice_is_written_out_and_forgotten() {
        let mut naks = Naks::default();
        let now = Instant::now();
        naks.due(gaps(), now);

        naks.forget_below(4);

        assert_eq!(naks.due(gaps(), now), gaps());
    }

    #[test]
    fn counts_every_request_it_makes() {
        let mut naks = Naks::default();
        let now = Instant::now();

        naks.due(vec![(3, vec![1]), (4, vec![0])], now);
        naks.due(gaps(), now);

        assert_eq!(naks.total(), 2);
    }
}
