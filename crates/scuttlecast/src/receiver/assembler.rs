use std::collections::BTreeMap;
use std::num::NonZeroU16;

use bytes::Bytes;

struct Slice {
    blocks: Vec<Option<Bytes>>,
    received: u16,
}

impl Slice {
    fn empty(blocks_per_slice: u16) -> Self {
        Self {
            blocks: vec![None; blocks_per_slice as usize],
            received: 0,
        }
    }

    fn insert(&mut self, block_in_slice: u16, payload: Bytes) -> bool {
        let Some(slot) = self.blocks.get_mut(block_in_slice as usize) else {
            return false;
        };
        if slot.is_some() {
            return false;
        }

        *slot = Some(payload);
        self.received += 1;
        true
    }

    fn missing(&self, target: u16) -> Vec<u16> {
        self.blocks
            .iter()
            .take(target as usize)
            .enumerate()
            .filter(|(_, block)| block.is_none())
            .map(|(index, _)| index as u16)
            .collect()
    }

    fn take(self) -> impl Iterator<Item = Bytes> {
        self.blocks.into_iter().flatten()
    }
}

pub(super) struct Assembler {
    blocks_per_slice: u16,
    max_live_slices: u32,
    slices: BTreeMap<u32, Slice>,
    next_needed: u32,
    total_blocks: Option<u64>,
}

impl Assembler {
    pub(super) fn new(blocks_per_slice: NonZeroU16, max_live_slices: NonZeroU16) -> Self {
        Self {
            blocks_per_slice: blocks_per_slice.get(),
            max_live_slices: max_live_slices.get() as u32,
            slices: BTreeMap::new(),
            next_needed: 0,
            total_blocks: None,
        }
    }

    pub(super) fn next_needed(&self) -> u32 {
        self.next_needed
    }

    pub(super) fn total_slices(&self) -> Option<u32> {
        self.total_blocks
            .map(|total| total.div_ceil(self.blocks_per_slice as u64) as u32)
    }

    pub(super) fn is_finished(&self) -> bool {
        self.total_slices()
            .is_some_and(|total_slices| self.next_needed >= total_slices)
    }

    pub(super) fn on_done(&mut self, total_blocks: u64) {
        self.total_blocks = Some(total_blocks);
    }

    /// Returns true if the block was newly stored. Blocks already written out,
    /// blocks too far ahead to buffer, and duplicates are all refused.
    pub(super) fn insert(&mut self, slice_no: u32, block_in_slice: u16, payload: Bytes) -> bool {
        if slice_no < self.next_needed || slice_no >= self.next_needed + self.max_live_slices {
            return false;
        }

        let blocks_per_slice = self.blocks_per_slice;
        self.slices
            .entry(slice_no)
            .or_insert_with(|| Slice::empty(blocks_per_slice))
            .insert(block_in_slice, payload)
    }

    /// Hands over every block that can now be written, in order, and advances
    /// past the slices it emptied
    pub(super) fn take_ready(&mut self) -> Vec<Bytes> {
        let mut ready = Vec::new();

        while self.is_complete(self.next_needed) {
            let Some(slice) = self.slices.remove(&self.next_needed) else {
                break;
            };
            ready.extend(slice.take());
            self.next_needed += 1;
        }
        ready
    }

    /// Slices that are still short of blocks and lie below `emit_floor`, with
    /// the blocks each one is missing
    pub(super) fn gaps(&self, emit_floor: u32) -> Vec<(u32, Vec<u16>)> {
        let floor = self.total_slices().unwrap_or(emit_floor);

        (self.next_needed..floor)
            .filter(|slice_no| !self.is_complete(*slice_no))
            .map(|slice_no| {
                let missing = match self.slices.get(&slice_no) {
                    Some(slice) => slice.missing(self.target(slice_no)),
                    None => (0..self.target(slice_no)).collect(),
                };
                (slice_no, missing)
            })
            .collect()
    }

    fn is_complete(&self, slice_no: u32) -> bool {
        self.slices
            .get(&slice_no)
            .is_some_and(|slice| slice.received >= self.target(slice_no))
    }

    /// Every slice holds `blocks_per_slice` blocks except the last, whose size
    /// only becomes known when `Done` reports the total
    fn target(&self, slice_no: u32) -> u16 {
        let Some(total_blocks) = self.total_blocks else {
            return self.blocks_per_slice;
        };

        let below = slice_no as u64 * self.blocks_per_slice as u64;
        match total_blocks.checked_sub(below) {
            Some(remaining) if remaining < self.blocks_per_slice as u64 => remaining as u16,
            Some(_) => self.blocks_per_slice,
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Assembler;
    use bytes::Bytes;
    use std::num::NonZeroU16;

    fn assembler(blocks_per_slice: u16, max_live_slices: u16) -> Assembler {
        Assembler::new(
            NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
            NonZeroU16::new(max_live_slices).expect("max live slices"),
        )
    }

    fn block(n: u8) -> Bytes {
        Bytes::from(vec![n])
    }

    fn fill(assembler: &mut Assembler, slice_no: u32, blocks: u16) {
        for block_in_slice in 0..blocks {
            assembler.insert(slice_no, block_in_slice, block(block_in_slice as u8));
        }
    }

    #[test]
    fn holds_a_slice_until_every_block_arrives() {
        let mut assembler = assembler(2, 8);

        assembler.insert(0, 1, block(1));
        assert!(assembler.take_ready().is_empty());

        assembler.insert(0, 0, block(0));

        assert_eq!(assembler.take_ready(), vec![block(0), block(1)]);
        assert_eq!(assembler.next_needed(), 1);
    }

    #[test]
    fn holds_a_complete_slice_until_the_ones_below_it_are_done() {
        let mut assembler = assembler(2, 8);

        fill(&mut assembler, 1, 2);
        assert!(assembler.take_ready().is_empty());

        fill(&mut assembler, 0, 2);

        assert_eq!(assembler.take_ready().len(), 4);
        assert_eq!(assembler.next_needed(), 2);
    }

    #[test]
    fn refuses_a_duplicate_block() {
        let mut assembler = assembler(2, 8);

        assert!(assembler.insert(0, 0, block(0)));
        assert!(!assembler.insert(0, 0, block(9)));
    }

    #[test]
    fn keeps_the_block_it_stored_first() {
        let mut assembler = assembler(1, 8);

        assembler.insert(0, 0, block(1));
        assembler.insert(0, 0, block(2));

        assert_eq!(assembler.take_ready(), vec![block(1)]);
    }

    #[test]
    fn refuses_blocks_of_slices_already_written_out() {
        let mut assembler = assembler(1, 8);
        fill(&mut assembler, 0, 1);
        assembler.take_ready();

        assert!(!assembler.insert(0, 0, block(0)));
    }

    #[test]
    fn refuses_blocks_too_far_ahead_to_buffer() {
        let mut assembler = assembler(2, 4);

        assert!(assembler.insert(3, 0, block(0)));
        assert!(!assembler.insert(4, 0, block(0)));
    }

    #[test]
    fn refuses_a_block_outside_its_slice() {
        let mut assembler = assembler(2, 8);

        assert!(!assembler.insert(0, 2, block(0)));
    }

    #[test]
    fn a_short_final_slice_completes_once_done_reports_the_total() {
        let mut assembler = assembler(4, 8);
        fill(&mut assembler, 0, 4);
        fill(&mut assembler, 1, 2);
        assert_eq!(assembler.take_ready().len(), 4);

        assembler.on_done(6);

        assert_eq!(assembler.take_ready().len(), 2);
        assert!(assembler.is_finished());
    }

    #[test]
    fn a_full_final_slice_needs_no_help_from_done() {
        let mut assembler = assembler(2, 8);
        fill(&mut assembler, 0, 2);

        assert_eq!(assembler.take_ready().len(), 2);
        assert!(!assembler.is_finished());

        assembler.on_done(2);

        assert!(assembler.is_finished());
    }

    #[test]
    fn an_empty_transfer_is_finished_the_moment_done_arrives() {
        let mut assembler = assembler(2, 8);

        assembler.on_done(0);

        assert!(assembler.is_finished());
        assert!(assembler.take_ready().is_empty());
    }

    #[test]
    fn reports_the_blocks_a_partial_slice_is_missing() {
        let mut assembler = assembler(4, 8);
        assembler.insert(0, 1, block(1));

        assert_eq!(assembler.gaps(1), vec![(0, vec![0, 2, 3])]);
    }

    #[test]
    fn reports_a_slice_nothing_arrived_for_as_wholly_missing() {
        let mut assembler = assembler(2, 8);

        assert_eq!(assembler.gaps(1), vec![(0, vec![0, 1])]);
    }

    #[test]
    fn asks_for_nothing_above_the_emit_floor() {
        let mut assembler = assembler(2, 8);

        assert!(assembler.gaps(0).is_empty());
    }

    #[test]
    fn asks_for_nothing_once_a_slice_is_whole() {
        let mut assembler = assembler(2, 8);
        fill(&mut assembler, 0, 2);

        assert!(assembler.gaps(1).is_empty());
    }

    #[test]
    fn asks_for_the_final_slice_once_done_bounds_the_transfer() {
        let mut assembler = assembler(4, 8);
        fill(&mut assembler, 0, 4);
        assembler.take_ready();
        assembler.on_done(6);

        assert_eq!(assembler.gaps(1), vec![(1, vec![0, 1])]);
    }

    #[test]
    fn asks_only_for_the_short_tail_of_the_final_slice() {
        let mut assembler = assembler(4, 8);
        fill(&mut assembler, 0, 4);
        assembler.take_ready();
        assembler.insert(1, 0, block(0));
        assembler.on_done(6);

        assert_eq!(assembler.gaps(1), vec![(1, vec![1])]);
    }
}
