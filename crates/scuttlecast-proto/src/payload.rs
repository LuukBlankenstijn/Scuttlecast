use bincode::{Decode, Encode};
use derive_more::Display;

/// Sender -> Group, announces the transfer to the group
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display("Hello(transfer_id={transfer_id}, blocks_per_slice={blocks_per_slice})")]
pub struct Hello {
    pub transfer_id: u64,
    pub blocks_per_slice: u32,
}

/// Sender -> Group, one block of data
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Data(transfer_id={transfer_id}, slice_no={slice_no}, block_in_slice={block_in_slice}, payload={} bytes)",
    payload.len()
)]
pub struct Data {
    pub transfer_id: u64,
    pub slice_no: u32,
    pub block_in_slice: u16,
    pub payload: Vec<u8>,
}

impl Data {
    pub fn slice_no(block_no: u32, blocks_per_slice: u32) -> u32 {
        block_no / blocks_per_slice
    }

    pub fn block_in_slice(block_no: u32, blocks_per_slice: u32) -> u16 {
        (block_no % blocks_per_slice) as u16
    }

    pub fn block_no(&self, blocks_per_slice: u32) -> u32 {
        (self.slice_no * blocks_per_slice) + self.block_in_slice as u32
    }
}

/// Sender -> Group, one block of parity data
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Parity(transfer_id={transfer_id}, slice_no={slice_no}, parity_index={parity_index}, payload={} bytes)",
    payload.len()
)]
pub struct Parity {
    pub transfer_id: u64,
    pub slice_no: u32,
    pub parity_index: u16,
    pub payload: Vec<u8>,
}

/// Receiver -> Sender, used to feed the rate controller
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Stats(transfer_id={transfer_id}, receiver_id={receiver_id}, received_since_last={received_since_last}, expected_since_last={expected_since_last})"
)]
pub struct Stats {
    pub transfer_id: u64,
    pub receiver_id: u64,
    pub received_since_last: u32,
    pub expected_since_last: u32,
}

/// Receiver -> Sender, request a missing block from the Sender
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Nack(transfer_id={transfer_id}, receiver_id={receiver_id}, slice_no={slice_no}, missing={} blocks)",
    missing.len()
)]
pub struct Nack {
    pub transfer_id: u64,
    pub receiver_id: u64,
    pub slice_no: u32,
    pub missing: Vec<u16>,
}

/// Sender -> Group, announces the transfer is done
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Done(transfer_id={transfer_id}, total_bytes={total_bytes}, total_blocks={total_blocks})"
)]
pub struct Done {
    pub transfer_id: u64,
    pub total_bytes: u64,
    pub total_blocks: u32,
}

#[cfg(test)]
mod tests {
    use super::Data;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn block_no_inverts_slice_coordinates(block_no: u32, blocks_per_slice in 1u32..=u16::MAX as u32) {
            let data = Data {
                transfer_id: 0,
                slice_no: Data::slice_no(block_no, blocks_per_slice),
                block_in_slice: Data::block_in_slice(block_no, blocks_per_slice),
                payload: Vec::new(),
            };

            prop_assert_eq!(data.block_no(blocks_per_slice), block_no);
        }
    }
}
