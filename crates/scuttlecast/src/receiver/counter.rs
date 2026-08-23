#[derive(Debug, Default)]
pub struct BlockCounter {
    words: Vec<u64>,
    highest_block_seen: Option<u64>,
    blocks_seen: u64,
}

impl BlockCounter {
    /// Returns true if the block was newly inserted
    pub fn insert(&mut self, block_number: u64) -> bool {
        if self.contains(block_number) {
            return false;
        }
        let word_idx = (block_number / 64) as usize;
        if word_idx >= self.words.len() {
            self.words.resize(word_idx + 1, 0);
        }
        self.words[word_idx] |= 1 << (block_number % 64);
        self.blocks_seen += 1;
        if self.highest_block_seen.is_none_or(|h| h < block_number) {
            self.highest_block_seen = Some(block_number)
        }
        true
    }

    pub fn contains(&self, block_number: u64) -> bool {
        self.words
            .get((block_number / 64) as usize)
            .is_some_and(|w| w & (1 << (block_number % 64)) != 0)
    }

    pub fn highest_block_seen(&self) -> Option<u64> {
        self.highest_block_seen
    }

    pub fn number_of_blocks_seen(&self) -> u64 {
        self.blocks_seen
    }
}
