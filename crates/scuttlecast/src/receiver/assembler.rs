use std::collections::BTreeMap;
use std::num::NonZeroU16;

use bytes::Bytes;
use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::BLOCK_SIZE;

const MAX_SHARDS_PER_SLICE: usize = 255;

struct Slice {
    slots: Vec<Option<Bytes>>,
    received: u16,
}

impl Slice {
    fn empty(total_slots: u16) -> Self {
        Self {
            slots: vec![None; total_slots as usize],
            received: 0,
        }
    }

    fn insert(&mut self, slot: u16, payload: Bytes) -> bool {
        let Some(cell) = self.slots.get_mut(slot as usize) else {
            return false;
        };
        if cell.is_some() {
            return false;
        }

        *cell = Some(payload);
        self.received += 1;
        true
    }

    fn data_present(&self, target: u16) -> bool {
        self.slots
            .iter()
            .take(target as usize)
            .all(|slot| slot.is_some())
    }

    /// The lowest missing slots worth asking for: only as many as are still
    /// needed to reach `target` recoverable shards, and never the padding slots
    /// beyond the short final slice.
    fn wanted(&self, target: u16, blocks_per_slice: u16) -> Vec<u16> {
        let shortfall = (target as usize).saturating_sub(self.received as usize);
        self.slots
            .iter()
            .enumerate()
            .filter(|(slot, held)| {
                held.is_none() && (*slot < target as usize || *slot >= blocks_per_slice as usize)
            })
            .map(|(slot, _)| slot as u16)
            .take(shortfall)
            .collect()
    }
}

pub(super) struct Assembler {
    blocks_per_slice: u16,
    parity_per_slice: u16,
    max_live_slices: u32,
    codec: Option<ReedSolomon>,
    slices: BTreeMap<u32, Slice>,
    next_needed: u32,
    total_blocks: Option<u64>,
}

impl Assembler {
    pub(super) fn new(
        blocks_per_slice: NonZeroU16,
        parity_per_slice: u16,
        max_live_slices: NonZeroU16,
    ) -> Self {
        let blocks_per_slice = blocks_per_slice.get();
        let codec = fec_codec(blocks_per_slice, parity_per_slice);
        let parity_per_slice = if codec.is_some() { parity_per_slice } else { 0 };

        Self {
            blocks_per_slice,
            parity_per_slice,
            max_live_slices: max_live_slices.get() as u32,
            codec,
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

    /// Returns true if the shard was newly stored. Shards already written out,
    /// shards too far ahead to buffer, and duplicates are all refused. Data
    /// blocks land in slots `0..k`, parity shards in `k..k + m`.
    pub(super) fn insert(&mut self, slice_no: u32, slot: u16, payload: Bytes) -> bool {
        if slice_no < self.next_needed || slice_no >= self.next_needed + self.max_live_slices {
            return false;
        }

        let total_slots = self.blocks_per_slice + self.parity_per_slice;
        self.slices
            .entry(slice_no)
            .or_insert_with(|| Slice::empty(total_slots))
            .insert(slot, payload)
    }

    /// A slice that had to be reconstructed may only be written once its true
    /// size is settled. Every slice but the last holds `blocks_per_slice`
    /// blocks; the last holds fewer, and which slice is last is only known
    /// from `Done` or from a slice above it. With enough parity a short final
    /// slice reaches the shard count of a full one, and reconstructing it then
    /// would invent a block the transfer never had. A slice holding all of its
    /// data blocks is never in doubt.
    fn can_write(&self, slice_no: u32) -> bool {
        self.total_blocks.is_some()
            || self.slices.keys().any(|seen| *seen > slice_no)
            || self
                .slices
                .get(&slice_no)
                .is_some_and(|slice| slice.data_present(self.blocks_per_slice))
    }

    /// Hands over every block that can now be written, in order, and advances
    /// past the slices it emptied, reconstructing the missing data of any slice
    /// that carries enough parity.
    pub(super) fn take_ready(&mut self) -> Vec<Bytes> {
        let mut ready = Vec::new();

        while self.can_write(self.next_needed) && self.is_complete(self.next_needed) {
            let Some(slice) = self.slices.remove(&self.next_needed) else {
                break;
            };
            let target = self.target(self.next_needed);
            ready.extend(self.recover(slice, target));
            self.next_needed += 1;
        }
        ready
    }

    /// Slices that are still short of shards and lie below `emit_floor`, with
    /// the shards each one still needs to become recoverable
    pub(super) fn gaps(&self, emit_floor: u32) -> Vec<(u32, Vec<u16>)> {
        let floor = self.total_slices().unwrap_or(emit_floor);

        (self.next_needed..floor)
            .filter(|slice_no| !self.is_complete(*slice_no))
            .map(|slice_no| {
                let target = self.target(slice_no);
                let missing = match self.slices.get(&slice_no) {
                    Some(slice) => slice.wanted(target, self.blocks_per_slice),
                    None => (0..target).collect(),
                };
                (slice_no, missing)
            })
            .collect()
    }

    /// A slice is ready once it holds `target` shards of any kind: that is
    /// enough to reconstruct its data, padding slots of a short final slice
    /// included.
    fn is_complete(&self, slice_no: u32) -> bool {
        self.slices
            .get(&slice_no)
            .is_some_and(|slice| slice.received >= self.target(slice_no))
    }

    fn recover(&self, slice: Slice, target: u16) -> Vec<Bytes> {
        let target = target as usize;
        let reconstruct = self.codec.is_some() && !slice.data_present(target as u16);
        if !reconstruct {
            return slice.slots.into_iter().take(target).flatten().collect();
        }

        let codec = self
            .codec
            .as_ref()
            .expect("codec present when reconstructing");
        let blocks_per_slice = self.blocks_per_slice as usize;
        let mut shards: Vec<Option<Vec<u8>>> = slice
            .slots
            .iter()
            .enumerate()
            .map(|(slot, held)| match held {
                Some(payload) => Some(to_block(payload)),
                None if (target..blocks_per_slice).contains(&slot) => Some(vec![0u8; BLOCK_SIZE]),
                None => None,
            })
            .collect();

        codec
            .reconstruct_data(&mut shards)
            .expect("target shards present guarantees the data recovers");

        (0..target)
            .map(|slot| match &slice.slots[slot] {
                Some(payload) => payload.clone(),
                None => {
                    Bytes::copy_from_slice(shards[slot].as_ref().expect("data shard recovered"))
                }
            })
            .collect()
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

fn fec_codec(blocks_per_slice: u16, parity_per_slice: u16) -> Option<ReedSolomon> {
    if parity_per_slice == 0
        || blocks_per_slice as usize + parity_per_slice as usize > MAX_SHARDS_PER_SLICE
    {
        return None;
    }

    ReedSolomon::new(blocks_per_slice as usize, parity_per_slice as usize).ok()
}

fn to_block(payload: &Bytes) -> Vec<u8> {
    if payload.len() == BLOCK_SIZE {
        return payload.to_vec();
    }

    let mut shard = vec![0u8; BLOCK_SIZE];
    shard[..payload.len()].copy_from_slice(payload);
    shard
}

#[cfg(test)]
mod tests {
    use super::Assembler;
    use crate::BLOCK_SIZE;
    use bytes::Bytes;
    use reed_solomon_erasure::galois_8::ReedSolomon;
    use std::num::NonZeroU16;

    fn assembler(blocks_per_slice: u16, max_live_slices: u16) -> Assembler {
        fec_assembler(blocks_per_slice, 0, max_live_slices)
    }

    fn fec_assembler(
        blocks_per_slice: u16,
        parity_per_slice: u16,
        max_live_slices: u16,
    ) -> Assembler {
        Assembler::new(
            NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
            parity_per_slice,
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

    fn shard(byte: u8) -> Vec<u8> {
        vec![byte; BLOCK_SIZE]
    }

    fn padded(bytes: &[u8]) -> Vec<u8> {
        let mut shard = vec![0u8; BLOCK_SIZE];
        shard[..bytes.len()].copy_from_slice(bytes);
        shard
    }

    fn parity_for(
        blocks_per_slice: usize,
        parity_per_slice: usize,
        data: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let codec = ReedSolomon::new(blocks_per_slice, parity_per_slice).expect("codec");
        let mut parity = vec![vec![0u8; BLOCK_SIZE]; parity_per_slice];
        codec
            .encode_sep(data, parity.as_mut_slice())
            .expect("encode");
        parity
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
        let assembler = assembler(2, 8);

        assert_eq!(assembler.gaps(1), vec![(0, vec![0, 1])]);
    }

    #[test]
    fn asks_for_nothing_above_the_emit_floor() {
        let assembler = assembler(2, 8);

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

    #[test]
    fn reconstructs_a_missing_data_block_from_parity() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);

        let mut assembler = fec_assembler(3, 2, 8);
        assembler.on_done(3);
        assembler.insert(0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 2, Bytes::from(data[2].clone()));
        assembler.insert(0, 3, Bytes::from(parity[0].clone()));
        let ready = assembler.take_ready();

        assert_eq!(
            ready,
            vec![
                Bytes::from(data[0].clone()),
                Bytes::from(data[1].clone()),
                Bytes::from(data[2].clone()),
            ]
        );
    }

    #[test]
    fn a_slice_missing_more_than_the_parity_budget_is_not_recoverable() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();

        let mut assembler = fec_assembler(3, 2, 8);
        assembler.insert(0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 1, Bytes::from(data[1].clone()));

        assert!(assembler.take_ready().is_empty());
    }

    #[test]
    fn asks_only_for_the_shortfall_to_reach_recovery() {
        let mut assembler = fec_assembler(3, 2, 8);
        assembler.insert(0, 0, block(0));
        assembler.insert(0, 1, block(1));

        assert_eq!(assembler.gaps(1), vec![(0, vec![2])]);
    }

    #[test]
    fn reconstructs_a_short_final_slice() {
        let block4 = shard(4);
        let block5 = vec![9u8; 500];
        let encode_data = vec![
            block4.clone(),
            padded(&block5),
            vec![0u8; BLOCK_SIZE],
            vec![0u8; BLOCK_SIZE],
        ];
        let parity = parity_for(4, 2, &encode_data);

        let mut assembler = fec_assembler(4, 2, 8);
        for slot in 0..4 {
            assembler.insert(0, slot, Bytes::from(shard(slot as u8)));
        }
        assert_eq!(assembler.take_ready().len(), 4);

        assembler.on_done(6);
        assembler.insert(1, 0, Bytes::from(block4.clone()));
        assembler.insert(1, 4, Bytes::from(parity[0].clone()));

        let ready = assembler.take_ready();

        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0], Bytes::from(block4));
        assert_eq!(ready[1], Bytes::from(padded(&block5)));
        assert!(assembler.is_finished());
    }

    #[test]
    fn zero_parity_still_needs_every_data_block() {
        let mut assembler = fec_assembler(2, 0, 8);

        assembler.insert(0, 1, block(1));
        assert!(assembler.take_ready().is_empty());

        assembler.insert(0, 0, block(0));
        assert_eq!(assembler.take_ready(), vec![block(0), block(1)]);
    }

    #[test]
    fn a_reconstructable_slice_waits_until_its_size_is_settled() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);
        let mut assembler = fec_assembler(3, 2, 8);

        assembler.insert(0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 2, Bytes::from(data[2].clone()));
        assembler.insert(0, 3, Bytes::from(parity[0].clone()));

        assert!(assembler.take_ready().is_empty());

        assembler.on_done(3);

        assert_eq!(assembler.take_ready().len(), 3);
    }

    #[test]
    fn a_slice_with_a_successor_is_written_without_waiting_for_done() {
        let mut assembler = fec_assembler(4, 2, 8);
        for slot in 0..4 {
            assembler.insert(0, slot, block(slot as u8));
        }
        assembler.insert(1, 0, block(9));

        assert_eq!(assembler.take_ready().len(), 4);
    }

    #[test]
    fn parity_cannot_conjure_a_block_the_final_slice_never_held() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);
        let mut assembler = fec_assembler(3, 2, 8);

        assembler.insert(0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 1, Bytes::from(data[1].clone()));
        assembler.insert(0, 3, Bytes::from(parity[0].clone()));
        assert!(assembler.take_ready().is_empty());

        assembler.on_done(2);

        assert_eq!(
            assembler.take_ready(),
            vec![Bytes::from(data[0].clone()), Bytes::from(data[1].clone())]
        );
    }
}
