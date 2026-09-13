use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroUsize};

use bytes::Bytes;
use reed_solomon_simd::{Error, ReedSolomonEncoder};

struct Slice {
    slice_no: u32,
    parity_width: usize,
    data: Vec<Bytes>,
    parity: Vec<Bytes>,
}

impl Slice {
    fn empty(slice_no: u32, parity_width: usize) -> Self {
        Self {
            slice_no,
            parity_width,
            data: Vec::new(),
            parity: Vec::new(),
        }
    }
}

pub(super) struct Sealed {
    pub(super) slice_no: u32,
    pub(super) parity: Vec<Bytes>,
}

pub(super) struct Pushed {
    pub(super) slice_no: u32,
    pub(super) block_in_slice: u16,
    pub(super) sealed: Option<Sealed>,
}

pub(super) struct Window {
    block_size: usize,
    blocks_per_slice: usize,
    max_parity: usize,
    wanted_parity: usize,
    max_live_slices: usize,
    codec: Option<ReedSolomonEncoder>,
    codec_width: usize,
    live: VecDeque<Slice>,
    current: Slice,
}

impl Window {
    pub(super) fn new(
        block_size: usize,
        blocks_per_slice: NonZeroU16,
        max_parity: u8,
        max_live_slices: NonZeroUsize,
    ) -> Result<Self, Error> {
        let blocks_per_slice = blocks_per_slice.get() as usize;
        let max_parity = max_parity as usize;
        let codec = match max_parity {
            0 => None,
            parity => Some(ReedSolomonEncoder::new(
                blocks_per_slice,
                parity,
                block_size,
            )?),
        };

        Ok(Self {
            block_size,
            blocks_per_slice,
            max_parity,
            wanted_parity: max_parity,
            max_live_slices: max_live_slices.get(),
            codec,
            codec_width: max_parity,
            live: VecDeque::new(),
            current: Slice::empty(0, max_parity),
        })
    }

    pub(super) fn cover(&mut self, wanted: u8) {
        self.wanted_parity = (wanted as usize).min(self.max_parity);
    }

    pub(super) fn current_parity(&self) -> u8 {
        self.current.parity_width as u8
    }

    pub(super) fn slice_parity(&self, slice_no: u32) -> Option<u8> {
        self.slice(slice_no).map(|slice| slice.parity_width as u8)
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

    pub(super) fn seal(&mut self) -> Option<Sealed> {
        if self.current.data.is_empty() {
            return None;
        }

        self.current.parity = self.encode_parity();
        let slice_no = self.current.slice_no;
        let parity = self.current.parity.clone();
        let next = Slice::empty(slice_no + 1, self.wanted_parity);
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

    pub(super) fn shard(&self, slice_no: u32, slot: u16) -> Option<&Bytes> {
        let slice = self.slice(slice_no)?;

        let slot = slot as usize;
        if slot < self.blocks_per_slice {
            slice.data.get(slot)
        } else {
            slice.parity.get(slot - self.blocks_per_slice)
        }
    }

    fn slice(&self, slice_no: u32) -> Option<&Slice> {
        if self.current.slice_no == slice_no {
            return Some(&self.current);
        }

        self.live.iter().find(|slice| slice.slice_no == slice_no)
    }

    fn encode_parity(&mut self) -> Vec<Bytes> {
        let width = self.current.parity_width;
        if width == 0 {
            return Vec::new();
        }

        let blocks_per_slice = self.blocks_per_slice;
        let absent = vec![0u8; self.block_size];
        if self.codec_width != width {
            self.codec
                .as_mut()
                .expect("a transfer allowing parity built a codec")
                .reset(blocks_per_slice, width, self.block_size)
                .expect("a narrower slice than the widest the transfer allows");
            self.codec_width = width;
        }

        let codec = self
            .codec
            .as_mut()
            .expect("a transfer allowing parity built a codec");
        for slot in 0..blocks_per_slice {
            let block = self
                .current
                .data
                .get(slot)
                .map_or(&absent[..], |block| &block[..]);

            codec
                .add_original_shard(block)
                .expect("a slice never holds more blocks than it was sized for");
        }

        codec
            .encode()
            .expect("every block of the slice was added")
            .recovery_iter()
            .map(Bytes::copy_from_slice)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Pushed, Sealed, Window};
    use bytes::Bytes;
    use std::num::{NonZeroU16, NonZeroUsize};

    const BLOCK_SIZE: usize = crate::DEFAULT_BLOCK_SIZE as usize;

    fn window(blocks_per_slice: u16, max_live_slices: usize) -> Window {
        fec_window(blocks_per_slice, 0, max_live_slices)
    }

    fn fec_window(blocks_per_slice: u16, parity_per_slice: u8, max_live_slices: usize) -> Window {
        Window::new(
            BLOCK_SIZE,
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
    fn a_slice_short_of_blocks_still_produces_full_length_parity() {
        let mut window = fec_window(4, 2, 8);
        window.push(full_block(1));
        window.push(full_block(2));

        let sealed = window.seal().expect("sealed");

        assert_eq!(sealed.parity.len(), 2);
        assert!(sealed.parity.iter().all(|shard| shard.len() == BLOCK_SIZE));
    }

    #[test]
    fn rejects_a_shard_budget_the_codec_cannot_serve() {
        let result = Window::new(
            BLOCK_SIZE,
            NonZeroU16::new(u16::MAX).expect("blocks per slice"),
            u8::MAX,
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

    fn seal_two_slices(window: &mut Window) -> (Sealed, Sealed) {
        window.push(full_block(1));
        let first = window.push(full_block(2)).sealed.expect("first sealed");
        window.push(full_block(3));
        let second = window.push(full_block(4)).sealed.expect("second sealed");

        (first, second)
    }

    #[test]
    fn covering_less_takes_effect_on_the_slice_after_the_one_being_filled() {
        let mut window = fec_window(2, 4, 8);
        window.cover(1);

        let (first, second) = seal_two_slices(&mut window);

        assert_eq!(first.parity.len(), 4);
        assert_eq!(second.parity.len(), 1);
    }

    #[test]
    fn covering_more_than_the_transfer_announced_is_capped() {
        let mut window = fec_window(2, 2, 8);
        window.cover(50);

        let (_, second) = seal_two_slices(&mut window);

        assert_eq!(second.parity.len(), 2);
    }

    #[test]
    fn covering_nothing_stops_parity_without_disturbing_the_slices() {
        let mut window = fec_window(2, 2, 8);
        window.cover(0);

        let (first, second) = seal_two_slices(&mut window);

        assert_eq!(first.parity.len(), 2);
        assert!(second.parity.is_empty());
        assert_eq!(window.shard(0, 0), Some(&full_block(1)));
    }

    #[test]
    fn every_shard_of_a_slice_names_the_width_the_slice_was_created_with() {
        let mut window = fec_window(2, 4, 8);
        window.cover(1);

        assert_eq!(window.current_parity(), 4);
        seal_two_slices(&mut window);

        assert_eq!(window.slice_parity(0), Some(4));
        assert_eq!(window.slice_parity(1), Some(1));
        assert_eq!(window.slice_parity(9), None);
    }
}
