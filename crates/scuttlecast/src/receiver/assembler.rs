use std::{collections::BTreeMap, num::NonZeroU16};

use bytes::Bytes;

pub struct Slice {
    slice_no: u32,
    blocks: Vec<Option<Bytes>>,
    block_count: u16,
    target_block_count: u16,
}

impl Slice {
    fn new(slice_no: u32, blocks_per_slice: NonZeroU16) -> Self {
        Self {
            slice_no,
            blocks: vec![None; blocks_per_slice.get() as usize],
            block_count: 0,
            target_block_count: blocks_per_slice.get(),
        }
    }

    /// Return true if the slice is full
    fn insert(&mut self, block_no: u16, data: Bytes) -> bool {
        if self.blocks[block_no as usize].is_none() {
            self.blocks[block_no as usize] = Some(data);
            self.block_count += 1;
        }
        self.is_complete()
    }

    fn flush(&self) -> impl IntoIterator<Item = Bytes> {
        self.blocks.iter().filter_map(|b| b.clone())
    }

    fn is_complete(&self) -> bool {
        self.target_block_count == self.block_count
    }

    fn get_missing(&self) -> impl IntoIterator<Item = u16> {
        self.blocks
            .iter()
            .enumerate()
            .filter_map(|(i, item)| item.as_ref().map(|_| i as u16))
    }
}

pub struct Assembler {
    slices: BTreeMap<u32, Slice>,
    completed: Option<u32>,
    blocks_per_slice: NonZeroU16,
}

impl Assembler {
    pub fn new(blocks_per_slice: NonZeroU16) -> Self {
        Self {
            slices: BTreeMap::new(),
            completed: None,
            blocks_per_slice,
        }
    }

    /// Returns true if the slice was finished, indicating a drain could be needed
    pub fn insert(&mut self, slice_no: u32, block_in_slice: u16, data: Bytes) -> bool {
        self.slices
            .entry(slice_no)
            .or_insert(Slice::new(slice_no, self.blocks_per_slice))
            .insert(block_in_slice, data)
    }

    pub fn drain(&mut self) -> impl IntoIterator<Item = Bytes> {
        let mut all = Vec::new();
        while let Some(entry) = self.slices.first_entry() {
            if !entry.get().is_complete() {
                break;
            }
            let (_, slice) = entry.remove_entry();
            all.extend(slice.flush());
        }

        all.into_iter()
    }
}
