use std::num::{NonZeroU16, NonZeroU32};

use bincode::{Decode, Encode};
use derive_more::Display;

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Hello(transfer_id={transfer_id}, block_size={block_size}, blocks_per_slice={blocks_per_slice}, parity_per_slice={parity_per_slice}, max_live_slices={max_live_slices}, total_bytes={total_bytes:?})"
)]
pub struct Hello {
    pub transfer_id: u64,
    pub block_size: NonZeroU32,
    pub blocks_per_slice: NonZeroU16,
    pub parity_per_slice: u8,
    pub max_live_slices: NonZeroU16,
    pub total_bytes: Option<u64>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Stats(transfer_id={transfer_id}, receiver_id={receiver_id}, total_received={total_received}, total_expected={total_expected}, next_needed_slice={next_needed_slice}, sink_stall_ms={sink_stall_ms})"
)]
pub struct Stats {
    pub transfer_id: u64,
    pub receiver_id: u64,
    pub total_received: u64,
    pub total_expected: u64,
    pub next_needed_slice: u32,
    pub sink_stall_ms: u32,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Nak(transfer_id={transfer_id}, receiver_id={receiver_id}, slice_no={slice_no}, missing={} shards)",
    missing.len()
)]
pub struct Nak {
    pub transfer_id: u64,
    pub receiver_id: u64,
    pub slice_no: u32,
    pub missing: Vec<u16>,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display("Evicted(transfer_id={transfer_id}, target={target}, reason={reason})")]
pub struct Evicted {
    pub transfer_id: u64,
    pub target: u64,
    pub reason: String,
}

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Done(transfer_id={transfer_id}, total_bytes={total_bytes}, total_blocks={total_blocks})"
)]
pub struct Done {
    pub transfer_id: u64,
    pub total_bytes: u64,
    pub total_blocks: u64,
}
