use std::num::NonZeroU16;

use bincode::{
    Decode, Encode,
    de::Decoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;
use derive_more::{Deref, Display, From, Into};

use crate::proto::error::Error;

/// Block payload, encoded as a length-prefixed byte slice
#[derive(Debug, Clone, PartialEq, Deref, From, Into, Display)]
#[display("{} bytes", _0.len())]
pub struct Payload(Bytes);

impl Encode for Payload {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.0.as_ref().encode(encoder)
    }
}

impl<Context> Decode<Context> for Payload {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self(Vec::<u8>::decode(decoder)?.into()))
    }
}

bincode::impl_borrow_decode!(Payload);

#[cfg(test)]
impl proptest::arbitrary::Arbitrary for Payload {
    type Parameters = ();
    type Strategy = proptest::strategy::BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::{Strategy, any};

        any::<Vec<u8>>()
            .prop_map(|bytes| Self(bytes.into()))
            .boxed()
    }
}

/// Sender -> Group, announces the transfer to the group. `max_live_slices` is
/// how many slices the sender keeps available for repair, which bounds how far
/// ahead of its own progress a receiver has to buffer.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Hello(transfer_id={transfer_id}, blocks_per_slice={blocks_per_slice}, parity_per_slice={parity_per_slice}, max_live_slices={max_live_slices})"
)]
pub struct Hello {
    pub transfer_id: u64,
    pub blocks_per_slice: NonZeroU16,
    pub parity_per_slice: u16,
    pub max_live_slices: NonZeroU16,
}

/// Sender -> Group, one block of data
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Data(transfer_id={transfer_id}, seq={seq}, slice_no={slice_no}, block_in_slice={block_in_slice}, emit_floor={emit_floor}, payload={} bytes)",
    payload.len()
)]
pub struct Data {
    pub transfer_id: u64,
    pub seq: u64,
    pub slice_no: u32,
    pub block_in_slice: u16,
    pub emit_floor: u32,
    pub payload: Payload,
}

impl Data {
    pub fn slice_no(block_no: u32, blocks_per_slice: NonZeroU16) -> u32 {
        block_no / blocks_per_slice.get() as u32
    }

    pub fn block_in_slice(block_no: u32, blocks_per_slice: NonZeroU16) -> u16 {
        (block_no % blocks_per_slice.get() as u32) as u16
    }

    pub fn block_no(&self, blocks_per_slice: NonZeroU16) -> Result<u64, Error> {
        if self.block_in_slice >= blocks_per_slice.get() {
            return Err(Error::BlockOutsideSlice {
                block_in_slice: self.block_in_slice,
                blocks_per_slice,
            });
        }

        Ok(self.slice_no as u64 * blocks_per_slice.get() as u64 + self.block_in_slice as u64)
    }
}

/// Sender -> Group, one parity shard, occupying shard slot
/// `blocks_per_slice + parity_index`
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Parity(transfer_id={transfer_id}, seq={seq}, slice_no={slice_no}, parity_index={parity_index}, emit_floor={emit_floor}, payload={} bytes)",
    payload.len()
)]
pub struct Parity {
    pub transfer_id: u64,
    pub seq: u64,
    pub slice_no: u32,
    pub parity_index: u16,
    pub emit_floor: u32,
    pub payload: Payload,
}

/// Receiver -> Sender, feeds the rate controller and the retransmit window.
/// Every counter is cumulative and never resets, so a lost report only widens
/// the span the next delta covers. `total_expected` is the highest transmission
/// seen plus one, which makes loss a property of the wire rather than of block
/// numbering. `next_needed_slice` is the lowest slice not yet handed to the
/// sink.
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

/// Receiver -> Sender, request missing shards of a slice
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

/// Sender -> Group, drops one receiver from the transfer
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display("Evicted(transfer_id={transfer_id}, target={target}, reason={reason})")]
pub struct Evicted {
    pub transfer_id: u64,
    pub target: u64,
    pub reason: String,
}

/// Sender -> Group, announces every block has been sent at least once
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

#[cfg(test)]
mod tests {
    use super::{Data, NonZeroU16, Payload};
    use crate::proto::error::Error;
    use proptest::prelude::*;

    fn blocks_per_slice(blocks: u16) -> NonZeroU16 {
        NonZeroU16::new(blocks).expect("nonzero blocks per slice")
    }

    fn data(slice_no: u32, block_in_slice: u16) -> Data {
        Data {
            transfer_id: 0,
            seq: 0,
            slice_no,
            block_in_slice,
            emit_floor: 0,
            payload: Payload::from(bytes::Bytes::new()),
        }
    }

    proptest! {
        #[test]
        fn block_no_inverts_slice_coordinates(block_no: u32, blocks_per_slice: NonZeroU16) {
            let data = data(
                Data::slice_no(block_no, blocks_per_slice),
                Data::block_in_slice(block_no, blocks_per_slice),
            );

            prop_assert_eq!(data.block_no(blocks_per_slice).unwrap(), block_no as u64);
        }

        #[test]
        fn any_slice_number_is_addressable(slice_no: u32, blocks_per_slice: NonZeroU16) {
            let block_in_slice = blocks_per_slice.get() - 1;
            let expected = slice_no as u64 * blocks_per_slice.get() as u64 + block_in_slice as u64;

            prop_assert_eq!(data(slice_no, block_in_slice).block_no(blocks_per_slice).unwrap(), expected);
        }
    }

    #[test]
    fn largest_coordinates_do_not_overflow() {
        let blocks_per_slice = blocks_per_slice(u16::MAX);
        let block_in_slice = u16::MAX - 1;
        let expected = u32::MAX as u64 * blocks_per_slice.get() as u64 + block_in_slice as u64;

        assert_eq!(
            data(u32::MAX, block_in_slice)
                .block_no(blocks_per_slice)
                .unwrap(),
            expected
        );
    }

    #[test]
    fn rejects_block_index_outside_its_slice() {
        assert!(matches!(
            data(0, 32).block_no(blocks_per_slice(32)),
            Err(Error::BlockOutsideSlice {
                block_in_slice: 32,
                blocks_per_slice
            }) if blocks_per_slice.get() == 32
        ));
    }
}
