use std::num::NonZeroU16;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to encode message: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("failed to decode message: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    #[error(
        "block_in_slice {block_in_slice} is out of range for slices of {blocks_per_slice} blocks"
    )]
    BlockOutsideSlice {
        block_in_slice: u16,
        blocks_per_slice: NonZeroU16,
    },
}
