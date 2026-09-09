use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroUsize};

use bytes::Bytes;

struct Slice {
    slice_no: u32,
    blocks: Vec<Bytes>,
}

impl Slice {
    fn empty(slice_no: u32) -> Self {
        Self {
            slice_no,
            blocks: Vec::new(),
        }
    }
}

pub(super) struct Window {
    blocks_per_slice: usize,
    max_live_slices: usize,
    live: VecDeque<Slice>,
    current: Slice,
}

impl Window {
    pub(super) fn new(blocks_per_slice: NonZeroU16, max_live_slices: NonZeroUsize) -> Self {
        Self {
            blocks_per_slice: blocks_per_slice.get() as usize,
            max_live_slices: max_live_slices.get(),
            live: VecDeque::new(),
            current: Slice::empty(0),
        }
    }

    pub(super) fn is_full(&self) -> bool {
        self.live.len() >= self.max_live_slices
    }

    /// Slices whose every block has been queued at least once. Dropping slices
    /// the group no longer needs never moves it back.
    pub(super) fn emit_floor(&self) -> u32 {
        self.current.slice_no
    }

    pub(super) fn push(&mut self, payload: Bytes) -> (u32, u16) {
        let coordinates = (self.current.slice_no, self.current.blocks.len() as u16);
        self.current.blocks.push(payload);

        if self.current.blocks.len() == self.blocks_per_slice {
            self.seal();
        }
        coordinates
    }

    pub(super) fn seal(&mut self) {
        if self.current.blocks.is_empty() {
            return;
        }

        let next = Slice::empty(self.current.slice_no + 1);
        self.live
            .push_back(std::mem::replace(&mut self.current, next));
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

    pub(super) fn block(&self, slice_no: u32, block_in_slice: u16) -> Option<&Bytes> {
        let slice = if self.current.slice_no == slice_no {
            &self.current
        } else {
            self.live.iter().find(|slice| slice.slice_no == slice_no)?
        };

        slice.blocks.get(block_in_slice as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::Window;
    use bytes::Bytes;
    use std::num::{NonZeroU16, NonZeroUsize};

    fn window(blocks_per_slice: u16, max_live_slices: usize) -> Window {
        Window::new(
            NonZeroU16::new(blocks_per_slice).expect("blocks per slice"),
            NonZeroUsize::new(max_live_slices).expect("max live slices"),
        )
    }

    fn push(window: &mut Window, count: usize) -> Vec<(u32, u16)> {
        (0..count)
            .map(|n| window.push(Bytes::from(vec![n as u8])))
            .collect()
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

        assert_eq!(window.block(0, 1), Some(&Bytes::from(vec![1])));
    }

    #[test]
    fn serves_blocks_of_sealed_slices() {
        let mut window = window(2, 8);

        push(&mut window, 4);

        assert_eq!(window.block(0, 0), Some(&Bytes::from(vec![0])));
        assert_eq!(window.block(1, 1), Some(&Bytes::from(vec![3])));
    }

    #[test]
    fn has_no_block_beyond_a_slice() {
        let mut window = window(2, 8);

        push(&mut window, 2);

        assert_eq!(window.block(0, 2), None);
        assert_eq!(window.block(7, 0), None);
    }

    #[test]
    fn retaining_drops_every_slice_below_the_one_needed() {
        let mut window = window(2, 8);
        push(&mut window, 6);

        window.retain_from(2);

        assert_eq!(window.block(0, 0), None);
        assert_eq!(window.block(1, 0), None);
        assert_eq!(window.block(2, 0), Some(&Bytes::from(vec![4])));
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

        assert_eq!(window.block(0, 1), Some(&Bytes::from(vec![1])));
    }

    #[test]
    fn sealing_an_empty_slice_does_nothing() {
        let mut window = window(2, 1);

        window.seal();
        window.seal();

        assert!(!window.is_full());
        assert_eq!(push(&mut window, 1), vec![(0, 0)]);
    }
}
