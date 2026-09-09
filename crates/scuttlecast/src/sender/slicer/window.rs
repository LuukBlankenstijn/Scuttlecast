use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroUsize};

use bytes::Bytes;
use reed_solomon_erasure::Error;
use reed_solomon_erasure::galois_8::ReedSolomon;

use crate::BLOCK_SIZE;

const MAX_SHARDS_PER_SLICE: usize = 255;

struct Slice {
    slice_no: u32,
    data: Vec<Bytes>,
    parity: Vec<Bytes>,
}

impl Slice {
    fn empty(slice_no: u32) -> Self {
        Self {
            slice_no,
            data: Vec::new(),
            parity: Vec::new(),
        }
    }
}

/// A slice that has just been sealed, together with the parity it produced.
/// Empty when the slice carries no parity.
pub(super) struct Sealed {
    pub(super) slice_no: u32,
    pub(super) parity: Vec<Bytes>,
}

/// The outcome of pushing one block: its coordinates, plus the seal that block
/// triggered when it filled the slice.
pub(super) struct Pushed {
    pub(super) slice_no: u32,
    pub(super) block_in_slice: u16,
    pub(super) sealed: Option<Sealed>,
}

pub(super) struct Window {
    blocks_per_slice: usize,
    max_parity: usize,
    parity: usize,
    max_live_slices: usize,
    codec: Option<ReedSolomon>,
    live: VecDeque<Slice>,
    current: Slice,
}

impl Window {
    pub(super) fn new(
        blocks_per_slice: NonZeroU16,
        max_parity: u16,
        max_live_slices: NonZeroUsize,
    ) -> Result<Self, Error> {
        let blocks_per_slice = blocks_per_slice.get() as usize;
        let max_parity = max_parity as usize;
        if blocks_per_slice + max_parity > MAX_SHARDS_PER_SLICE {
            return Err(Error::TooManyShards);
        }

        let mut window = Self {
            blocks_per_slice,
            max_parity,
            parity: 0,
            max_live_slices: max_live_slices.get(),
            codec: None,
            live: VecDeque::new(),
            current: Slice::empty(0),
        };
        window.cover(max_parity as u16)?;

        Ok(window)
    }

    /// Sets how many parity shards the slices sealed from now on carry, capped
    /// by the maximum the transfer announced. Shard `j` comes out the same
    /// whether two or twenty are asked for, so receivers go on decoding
    /// against the announced maximum however little the sender is sending.
    pub(super) fn cover(&mut self, wanted: u16) -> Result<(), Error> {
        let wanted = (wanted as usize).min(self.max_parity);
        if wanted == self.parity {
            return Ok(());
        }

        self.codec = match wanted {
            0 => None,
            parity => Some(ReedSolomon::new(self.blocks_per_slice, parity)?),
        };
        self.parity = wanted;

        Ok(())
    }

    pub(super) fn is_full(&self) -> bool {
        self.live.len() >= self.max_live_slices
    }

    pub(super) fn push(&mut self, payload: Bytes) -> Pushed {
        let slice_no = self.current.slice_no;
        let block_in_slice = self.current.data.len() as u16;
        self.current.data.push(payload);

        let sealed = if self.current.data.len() == self.blocks_per_slice {
            self.seal()
        } else {
            None
        };

        Pushed {
            slice_no,
            block_in_slice,
            sealed,
        }
    }

    /// Seals the slice being filled and returns the parity it produced, or
    /// `None` when nothing was buffered. Parity shards are always `BLOCK_SIZE`,
    /// even for a short final slice whose data is zero-padded for the encode.
    pub(super) fn seal(&mut self) -> Option<Sealed> {
        if self.current.data.is_empty() {
            return None;
        }

        self.current.parity = self.encode_parity();
        let slice_no = self.current.slice_no;
        let parity = self.current.parity.clone();
        let next = Slice::empty(slice_no + 1);
        self.live
            .push_back(std::mem::replace(&mut self.current, next));

        Some(Sealed { slice_no, parity })
    }

    pub(super) fn retain_from(&mut self, first_needed: u32) {
        while self
            .live
            .front()
            .is_some_and(|slice| slice.slice_no < first_needed)
        {
            self.live.pop_front();
        }
    }

    /// Serves any shard of a slice by slot: data blocks occupy `0..k`, parity
    /// shards `k..k + m`.
    pub(super) fn shard(&self, slice_no: u32, slot: u16) -> Option<&Bytes> {
        let slice = if self.current.slice_no == slice_no {
            &self.current
        } else {
            self.live.iter().find(|slice| slice.slice_no == slice_no)?
        };

        let slot = slot as usize;
        if slot < self.blocks_per_slice {
            slice.data.get(slot)
        } else {
            slice.parity.get(slot - self.blocks_per_slice)
        }
    }

    fn encode_parity(&self) -> Vec<Bytes> {
        let Some(codec) = &self.codec else {
            return Vec::new();
        };

        let mut parity = vec![vec![0u8; BLOCK_SIZE]; self.parity];
        let full = self.current.data.len() == self.blocks_per_slice
            && self
                .current
                .data
                .iter()
                .all(|block| block.len() == BLOCK_SIZE);

        if full {
            codec
                .encode_sep(self.current.data.as_slice(), parity.as_mut_slice())
                .expect("k data and m parity shards of equal length");
        } else {
            let data = self.padded_data();
            codec
                .encode_sep(data.as_slice(), parity.as_mut_slice())
                .expect("k data and m parity shards of equal length");
        }

        parity.into_iter().map(Bytes::from).collect()
    }

    fn padded_data(&self) -> Vec<Vec<u8>> {
        (0..self.blocks_per_slice)
            .map(|index| {
                let mut shard = vec![0u8; BLOCK_SIZE];
                if let Some(block) = self.current.data.get(index) {
                    shard[..block.len()].copy_from_slice(block);
                }
                shard
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Pushed, Window};
    use crate::BLOCK_SIZE;
    use bytes::Bytes;
    use std::num::{NonZeroU16, NonZeroUsize};

    fn window(blocks_per_slice: u16, max_live_slices: usize) -> Window {
        fec_window(blocks_per_slice, 0, max_live_slices)
    }

    fn fec_window(blocks_per_slice: u16, parity_per_slice: u16, max_live_slices: usize) -> Window {
        Window::new(
            NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
            parity_per_slice,
            NonZeroUsize::new(max_live_slices).expect("max live slices"),
        )
        .expect("window")
    }

    fn push(window: &mut Window, count: usize) -> Vec<(u32, u16)> {
        (0..count)
            .map(|n| {
                let Pushed {
                    slice_no,
                    block_in_slice,
                    ..
                } = window.push(Bytes::from(vec![n as u8]));
                (slice_no, block_in_slice)
            })
            .collect()
    }

    fn full_block(byte: u8) -> Bytes {
        Bytes::from(vec![byte; BLOCK_SIZE])
    }

    #[test]
    fn numbers_blocks_within_their_slice() {
        let mut window = window(2, 8);

        assert_eq!(
            push(&mut window, 5),
            vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0)]
        );
    }

    #[test]
    fn fills_up_once_max_live_slices_are_sealed() {
        let mut window = window(2, 2);
        assert!(!window.is_full());

        push(&mut window, 3);
        assert!(!window.is_full());

        push(&mut window, 1);

        assert!(window.is_full());
    }

    #[test]
    fn a_partial_slice_does_not_count_against_the_limit() {
        let mut window = window(4, 1);

        push(&mut window, 3);

        assert!(!window.is_full());
    }

    #[test]
    fn serves_blocks_of_the_slice_still_being_filled() {
        let mut window = window(4, 8);

        push(&mut window, 2);

        assert_eq!(window.shard(0, 1), Some(&Bytes::from(vec![1])));
    }

    #[test]
    fn serves_blocks_of_sealed_slices() {
        let mut window = window(2, 8);

        push(&mut window, 4);

        assert_eq!(window.shard(0, 0), Some(&Bytes::from(vec![0])));
        assert_eq!(window.shard(1, 1), Some(&Bytes::from(vec![3])));
    }

    #[test]
    fn has_no_block_beyond_a_slice() {
        let mut window = window(2, 8);

        push(&mut window, 2);

        assert_eq!(window.shard(0, 2), None);
        assert_eq!(window.shard(7, 0), None);
    }

    #[test]
    fn retaining_drops_every_slice_below_the_one_needed() {
        let mut window = window(2, 8);
        push(&mut window, 6);

        window.retain_from(2);

        assert_eq!(window.shard(0, 0), None);
        assert_eq!(window.shard(1, 0), None);
        assert_eq!(window.shard(2, 0), Some(&Bytes::from(vec![4])));
    }

    #[test]
    fn retaining_frees_room_for_more_slices() {
        let mut window = window(2, 2);
        push(&mut window, 4);
        assert!(window.is_full());

        window.retain_from(1);

        assert!(!window.is_full());
    }

    #[test]
    fn sealing_a_short_final_slice_keeps_it_servable() {
        let mut window = window(4, 8);
        push(&mut window, 2);

        window.seal();

        assert_eq!(window.shard(0, 1), Some(&Bytes::from(vec![1])));
    }

    #[test]
    fn sealing_an_empty_slice_does_nothing() {
        let mut window = window(2, 1);

        window.seal();
        window.seal();

        assert!(!window.is_full());
        assert_eq!(push(&mut window, 1), vec![(0, 0)]);
    }

    #[test]
    fn a_full_slice_produces_a_parity_shard_per_configured_shard() {
        let mut window = fec_window(2, 3, 8);
        window.push(full_block(1));
        let sealed = window.push(full_block(2)).sealed.expect("sealed");

        assert_eq!(sealed.slice_no, 0);
        assert_eq!(sealed.parity.len(), 3);
        assert!(sealed.parity.iter().all(|shard| shard.len() == BLOCK_SIZE));
    }

    #[test]
    fn serves_parity_shards_by_their_slot() {
        let mut window = fec_window(2, 2, 8);
        window.push(full_block(5));
        let sealed = window.push(full_block(6)).sealed.expect("sealed");

        assert_eq!(window.shard(0, 2), Some(&sealed.parity[0]));
        assert_eq!(window.shard(0, 3), Some(&sealed.parity[1]));
    }

    #[test]
    fn a_short_final_slice_still_produces_full_length_parity() {
        let mut window = fec_window(4, 2, 8);
        window.push(full_block(1));
        window.push(Bytes::from(vec![2u8; 10]));

        let sealed = window.seal().expect("sealed");

        assert_eq!(sealed.parity.len(), 2);
        assert!(sealed.parity.iter().all(|shard| shard.len() == BLOCK_SIZE));
    }

    #[test]
    fn rejects_a_shard_budget_over_the_field_limit() {
        let result = Window::new(
            NonZeroU16::new(250).expect("blocks per slice"),
            6,
            NonZeroUsize::new(8).expect("max live slices"),
        );

        assert!(result.is_err());
    }

    #[test]
    fn zero_parity_produces_no_parity() {
        let mut window = fec_window(2, 0, 8);
        window.push(full_block(7));
        let sealed = window.push(full_block(8)).sealed.expect("sealed");

        assert!(sealed.parity.is_empty());
        assert_eq!(window.shard(0, 2), None);
    }

    #[test]
    fn covering_less_seals_fewer_shards() {
        let mut window = fec_window(2, 4, 8);
        window.cover(1).expect("cover");

        window.push(full_block(1));
        let sealed = window.push(full_block(2)).sealed.expect("sealed");

        assert_eq!(sealed.parity.len(), 1);
    }

    #[test]
    fn covering_more_than_the_transfer_announced_is_capped() {
        let mut window = fec_window(2, 2, 8);
        window.cover(50).expect("cover");

        window.push(full_block(1));
        let sealed = window.push(full_block(2)).sealed.expect("sealed");

        assert_eq!(sealed.parity.len(), 2);
    }

    #[test]
    fn covering_nothing_stops_parity_without_disturbing_the_slices() {
        let mut window = fec_window(2, 2, 8);
        window.cover(0).expect("cover");

        window.push(full_block(1));
        let sealed = window.push(full_block(2)).sealed.expect("sealed");

        assert!(sealed.parity.is_empty());
        assert_eq!(window.shard(0, 0), Some(&full_block(1)));
    }

    /// Receivers decode against the maximum the transfer announced whatever
    /// the sender is currently sending, which only holds because shard `j` is
    /// the same either way. A codec that stopped honouring that would corrupt
    /// every reconstruction rather than fail, so it is worth pinning.
    #[test]
    fn a_shard_is_the_same_however_many_were_asked_for() {
        let mut wide = fec_window(4, 4, 8);
        let mut narrow = fec_window(4, 4, 8);
        narrow.cover(1).expect("cover");

        let mut wide_sealed = None;
        let mut narrow_sealed = None;
        for byte in 1..=4 {
            wide_sealed = wide.push(full_block(byte)).sealed;
            narrow_sealed = narrow.push(full_block(byte)).sealed;
        }

        let wide = wide_sealed.expect("sealed");
        let narrow = narrow_sealed.expect("sealed");

        assert_eq!(wide.parity.len(), 4);
        assert_eq!(narrow.parity, wide.parity[..1]);
    }
}
