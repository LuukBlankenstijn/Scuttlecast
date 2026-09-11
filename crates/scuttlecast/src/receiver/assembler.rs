use std::collections::BTreeMap;
use std::num::NonZeroU16;

use bytes::Bytes;
use reed_solomon_simd::{Error, ReedSolomonDecoder};

struct Slice {
    slots: Vec<Option<Bytes>>,
    received: u16,
    parity_width: u8,
}

impl Slice {
    fn empty(total_slots: u16) -> Self {
        Self {
            slots: vec![None; total_slots as usize],
            received: 0,
            parity_width: 0,
        }
    }

    /// A slice is reconstructed against the width its shards name, so a shard
    /// naming a different one is refused rather than allowed to contradict
    /// what is already stored under the first.
    fn insert(&mut self, slot: u16, parity_width: u8, payload: Bytes) -> bool {
        let Some(cell) = self.slots.get_mut(slot as usize) else {
            return false;
        };
        if cell.is_some() {
            return false;
        }
        if parity_width > 0 && self.parity_width > 0 && self.parity_width != parity_width {
            return false;
        }

        *cell = Some(payload);
        self.received += 1;
        if parity_width > 0 {
            self.parity_width = parity_width;
        }
        true
    }

    fn data_present(&self, target: u16) -> bool {
        self.slots
            .iter()
            .take(target as usize)
            .all(|slot| slot.is_some())
    }

    /// The lowest missing data blocks worth asking for, capped by the shortfall
    fn wanted(&self, target: u16) -> Vec<u16> {
        let shortfall = (target as usize).saturating_sub(self.received as usize);
        self.slots
            .iter()
            .enumerate()
            .take(target as usize)
            .filter(|(_, held)| held.is_none())
            .map(|(slot, _)| slot as u16)
            .take(shortfall)
            .collect()
    }
}

pub(super) struct Assembler {
    block_size: usize,
    blocks_per_slice: u16,
    parity_per_slice: u8,
    max_live_slices: u32,
    codec: Option<ReedSolomonDecoder>,
    codec_width: u8,
    slices: BTreeMap<u32, Slice>,
    next_needed: u32,
    total_blocks: Option<u64>,
}

impl Assembler {
    /// Builds the decoder for the widest parity the transfer allows, so that
    /// an announcement the codec cannot serve is refused here rather than
    /// when a slice first needs repairing.
    pub(super) fn new(
        block_size: usize,
        blocks_per_slice: NonZeroU16,
        parity_per_slice: u8,
        max_live_slices: NonZeroU16,
    ) -> Result<Self, Error> {
        let codec = match parity_per_slice {
            0 => None,
            parity => Some(ReedSolomonDecoder::new(
                blocks_per_slice.get() as usize,
                parity as usize,
                block_size,
            )?),
        };

        Ok(Self {
            block_size,
            blocks_per_slice: blocks_per_slice.get(),
            parity_per_slice,
            max_live_slices: max_live_slices.get() as u32,
            codec,
            codec_width: parity_per_slice,
            slices: BTreeMap::new(),
            next_needed: 0,
            total_blocks: None,
        })
    }

    pub(super) fn total_slots(&self) -> u16 {
        self.blocks_per_slice + self.parity_per_slice as u16
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
    pub(super) fn insert(
        &mut self,
        slice_no: u32,
        slot: u16,
        parity_width: u8,
        payload: Bytes,
    ) -> bool {
        if slice_no < self.next_needed || slice_no >= self.next_needed + self.max_live_slices {
            return false;
        }

        let total_slots = self.total_slots();
        self.slices
            .entry(slice_no)
            .or_insert_with(|| Slice::empty(total_slots))
            .insert(slot, parity_width, payload)
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
                    Some(slice) => slice.wanted(target),
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

    fn recover(&mut self, slice: Slice, target: u16) -> Vec<Bytes> {
        let target = target as usize;
        if slice.data_present(target as u16) {
            return slice.slots.into_iter().take(target).flatten().collect();
        }

        let blocks_per_slice = self.blocks_per_slice as usize;
        let absent = vec![0u8; self.block_size];
        let codec = self.decoder_for(slice.parity_width);

        for (slot, held) in slice.slots.iter().enumerate() {
            match held {
                Some(payload) if slot < blocks_per_slice => codec.add_original_shard(slot, payload),
                Some(payload) => codec.add_recovery_shard(slot - blocks_per_slice, payload),
                None if (target..blocks_per_slice).contains(&slot) => {
                    codec.add_original_shard(slot, &absent)
                }
                None => Ok(()),
            }
            .expect("every shard addresses a slot of this slice exactly once");
        }

        let mut recovered: Vec<Option<Bytes>> = slice.slots.iter().take(target).cloned().collect();
        let decoded = codec
            .decode()
            .expect("target shards present guarantees the data recovers");

        for (slot, shard) in decoded.restored_original_iter() {
            if slot < target {
                recovered[slot] = Some(Bytes::copy_from_slice(shard));
            }
        }
        drop(decoded);

        recovered
            .into_iter()
            .map(|block| block.expect("every data shard of the slice recovered"))
            .collect()
    }

    /// Recovery shards depend on how many of them a slice was encoded with, so
    /// a decoder is only valid for slices of that same width.
    fn decoder_for(&mut self, parity_width: u8) -> &mut ReedSolomonDecoder {
        if self.codec.is_none() || self.codec_width != parity_width {
            self.codec = ReedSolomonDecoder::new(
                self.blocks_per_slice as usize,
                parity_width as usize,
                self.block_size,
            )
            .ok();
            self.codec_width = parity_width;
        }

        self.codec
            .as_mut()
            .expect("the widest parity the transfer allows built a decoder")
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
    const BLOCK_SIZE: usize = crate::DEFAULT_BLOCK_SIZE as usize;
    use bytes::Bytes;
    use reed_solomon_simd::ReedSolomonEncoder;
    use std::num::NonZeroU16;

    fn assembler(blocks_per_slice: u16, max_live_slices: u16) -> Assembler {
        fec_assembler(blocks_per_slice, 0, max_live_slices)
    }

    fn fec_assembler(
        blocks_per_slice: u16,
        parity_per_slice: u8,
        max_live_slices: u16,
    ) -> Assembler {
        Assembler::new(
            BLOCK_SIZE,
            NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
            parity_per_slice,
            NonZeroU16::new(max_live_slices).expect("max live slices"),
        )
        .expect("valid shard counts")
    }

    fn block(n: u8) -> Bytes {
        Bytes::from(vec![n])
    }

    fn fill(assembler: &mut Assembler, slice_no: u32, blocks: u16) {
        for block_in_slice in 0..blocks {
            assembler.insert(slice_no, block_in_slice, 0, block(block_in_slice as u8));
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
        let mut codec =
            ReedSolomonEncoder::new(blocks_per_slice, parity_per_slice, BLOCK_SIZE).expect("codec");
        for shard in data {
            codec.add_original_shard(shard).expect("add shard");
        }

        codec
            .encode()
            .expect("encode")
            .recovery_iter()
            .map(<[u8]>::to_vec)
            .collect()
    }

    #[test]
    fn holds_a_slice_until_every_block_arrives() {
        let mut assembler = assembler(2, 8);

        assembler.insert(0, 1, 0, block(1));
        assert!(assembler.take_ready().is_empty());

        assembler.insert(0, 0, 0, block(0));

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

        assert!(assembler.insert(0, 0, 0, block(0)));
        assert!(!assembler.insert(0, 0, 0, block(9)));
    }

    #[test]
    fn keeps_the_block_it_stored_first() {
        let mut assembler = assembler(1, 8);

        assembler.insert(0, 0, 0, block(1));
        assembler.insert(0, 0, 0, block(2));

        assert_eq!(assembler.take_ready(), vec![block(1)]);
    }

    #[test]
    fn refuses_blocks_of_slices_already_written_out() {
        let mut assembler = assembler(1, 8);
        fill(&mut assembler, 0, 1);
        assembler.take_ready();

        assert!(!assembler.insert(0, 0, 0, block(0)));
    }

    #[test]
    fn refuses_blocks_too_far_ahead_to_buffer() {
        let mut assembler = assembler(2, 4);

        assert!(assembler.insert(3, 0, 0, block(0)));
        assert!(!assembler.insert(4, 0, 0, block(0)));
    }

    #[test]
    fn refuses_a_block_outside_its_slice() {
        let mut assembler = assembler(2, 8);

        assert!(!assembler.insert(0, 2, 0, block(0)));
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
        assembler.insert(0, 1, 0, block(1));

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
        assembler.insert(1, 0, 0, block(0));
        assembler.on_done(6);

        assert_eq!(assembler.gaps(1), vec![(1, vec![1])]);
    }

    #[test]
    fn reconstructs_a_missing_data_block_from_parity() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);

        let mut assembler = fec_assembler(3, 2, 8);
        assembler.on_done(3);
        assembler.insert(0, 0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 2, 0, Bytes::from(data[2].clone()));
        assembler.insert(0, 3, 2, Bytes::from(parity[0].clone()));
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
    fn refuses_a_shard_naming_a_different_parity_width() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);

        let mut assembler = fec_assembler(3, 2, 8);
        assembler.on_done(3);

        assert!(assembler.insert(0, 0, 2, Bytes::from(data[0].clone())));
        assert!(!assembler.insert(0, 2, 1, Bytes::from(data[2].clone())));
        assert!(assembler.insert(0, 2, 2, Bytes::from(data[2].clone())));
        assert!(assembler.insert(0, 3, 2, Bytes::from(parity[0].clone())));

        let expected: Vec<Bytes> = data.iter().map(|b| Bytes::from(b.clone())).collect();
        assert_eq!(assembler.take_ready(), expected);
    }

    /// Recovery shards depend on how many of them a slice was encoded with,
    /// so a slice sealed at one width has to be reconstructed against that
    /// width and not against whatever the transfer allowed at its widest.
    #[test]
    fn reconstructs_slices_sealed_at_different_parity_widths() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let wide = parity_for(3, 4, &data);
        let narrow = parity_for(3, 1, &data);

        let mut assembler = fec_assembler(3, 4, 8);
        assembler.on_done(6);

        for (slice_no, parity, width) in [(0, &wide, 4u8), (1, &narrow, 1u8)] {
            assembler.insert(slice_no, 0, width, Bytes::from(data[0].clone()));
            assembler.insert(slice_no, 2, width, Bytes::from(data[2].clone()));
            assembler.insert(slice_no, 3, width, Bytes::from(parity[0].clone()));
        }

        let ready = assembler.take_ready();
        let expected: Vec<Bytes> = data
            .iter()
            .chain(data.iter())
            .map(|block| Bytes::from(block.clone()))
            .collect();

        assert_eq!(ready, expected);
    }

    #[test]
    fn a_slice_missing_more_than_the_parity_budget_is_not_recoverable() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();

        let mut assembler = fec_assembler(3, 2, 8);
        assembler.insert(0, 0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 1, 0, Bytes::from(data[1].clone()));

        assert!(assembler.take_ready().is_empty());
    }

    #[test]
    fn asks_only_for_the_shortfall_to_reach_recovery() {
        let mut assembler = fec_assembler(3, 2, 8);
        assembler.insert(0, 0, 0, block(0));
        assembler.insert(0, 1, 0, block(1));

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
            assembler.insert(0, slot, 0, Bytes::from(shard(slot as u8)));
        }
        assert_eq!(assembler.take_ready().len(), 4);

        assembler.on_done(6);
        assembler.insert(1, 0, 0, Bytes::from(block4.clone()));
        assembler.insert(1, 4, 2, Bytes::from(parity[0].clone()));

        let ready = assembler.take_ready();

        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0], Bytes::from(block4));
        assert_eq!(ready[1], Bytes::from(padded(&block5)));
        assert!(assembler.is_finished());
    }

    #[test]
    fn zero_parity_still_needs_every_data_block() {
        let mut assembler = fec_assembler(2, 0, 8);

        assembler.insert(0, 1, 0, block(1));
        assert!(assembler.take_ready().is_empty());

        assembler.insert(0, 0, 0, block(0));
        assert_eq!(assembler.take_ready(), vec![block(0), block(1)]);
    }

    #[test]
    fn a_reconstructable_slice_waits_until_its_size_is_settled() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);
        let mut assembler = fec_assembler(3, 2, 8);

        assembler.insert(0, 0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 2, 0, Bytes::from(data[2].clone()));
        assembler.insert(0, 3, 2, Bytes::from(parity[0].clone()));

        assert!(assembler.take_ready().is_empty());

        assembler.on_done(3);

        assert_eq!(assembler.take_ready().len(), 3);
    }

    #[test]
    fn a_slice_with_a_successor_is_written_without_waiting_for_done() {
        let mut assembler = fec_assembler(4, 2, 8);
        for slot in 0..4 {
            assembler.insert(0, slot, 0, block(slot as u8));
        }
        assembler.insert(1, 0, 0, block(9));

        assert_eq!(assembler.take_ready().len(), 4);
    }

    #[test]
    fn parity_cannot_conjure_a_block_the_final_slice_never_held() {
        let data: Vec<Vec<u8>> = (0..3).map(shard).collect();
        let parity = parity_for(3, 2, &data);
        let mut assembler = fec_assembler(3, 2, 8);

        assembler.insert(0, 0, 0, Bytes::from(data[0].clone()));
        assembler.insert(0, 1, 0, Bytes::from(data[1].clone()));
        assembler.insert(0, 3, 2, Bytes::from(parity[0].clone()));
        assert!(assembler.take_ready().is_empty());

        assembler.on_done(2);

        assert_eq!(
            assembler.take_ready(),
            vec![Bytes::from(data[0].clone()), Bytes::from(data[1].clone())]
        );
    }
}
