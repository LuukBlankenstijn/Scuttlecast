use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to encode message: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    #[error("failed to decode message: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    #[error("datagram of {0} bytes is too short to carry a shard")]
    FrameTooShort(usize),

    #[error("datagram tagged {0:#04x} is neither a shard nor a control message")]
    UnknownTag(u8),

    #[error("shard header sets reserved flags {0:#04x}")]
    ReservedFlags(u8),

    #[error("shard payload of {got} bytes in a transfer of {expected}-byte blocks")]
    PayloadSize { got: usize, expected: usize },

    #[error("slot {slot} is out of range for slices of {slots} shards")]
    SlotOutsideSlice { slot: u16, slots: u16 },
}
