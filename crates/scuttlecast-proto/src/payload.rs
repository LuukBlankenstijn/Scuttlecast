use bincode::{
    Decode, Encode,
    de::Decoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};
use bytes::Bytes;
use derive_more::{Deref, Display, From, Into};

use crate::error::Error;

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
    pub payload: Payload,
}

impl Data {
    pub fn slice_no(block_no: u32, blocks_per_slice: u32) -> u32 {
        block_no / blocks_per_slice
    }

    pub fn block_in_slice(block_no: u32, blocks_per_slice: u32) -> u16 {
        (block_no % blocks_per_slice) as u16
    }

    pub fn block_no(&self, blocks_per_slice: u32) -> Result<u64, Error> {
        if blocks_per_slice == 0 {
            return Err(Error::ZeroBlocksPerSlice);
        }
        if self.block_in_slice as u32 >= blocks_per_slice {
            return Err(Error::BlockOutsideSlice {
                block_in_slice: self.block_in_slice,
                blocks_per_slice,
            });
        }

        Ok(self.slice_no as u64 * blocks_per_slice as u64 + self.block_in_slice as u64)
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
    pub payload: Payload,
}

/// Receiver -> Sender, feeds the rate controller and the retransmit window.
/// `completed_through` is the highest slice the receiver holds in full, with
/// every slice below it complete as well.
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display(
    "Stats(transfer_id={transfer_id}, receiver_id={receiver_id}, blocks_received={blocks_received}, blocks_expected={blocks_expected}, completed_through={completed_through:?})"
)]
pub struct Stats {
    pub transfer_id: u64,
    pub receiver_id: u64,
    pub blocks_received: u64,
    pub blocks_expected: u64,
    pub completed_through: Option<u32>,
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

/// Receiver -> Sender, announces the receiver holds the whole transfer
#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[display("Complete(transfer_id={transfer_id}, receiver_id={receiver_id})")]
pub struct Complete {
    pub transfer_id: u64,
    pub receiver_id: u64,
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
    pub total_blocks: u64,
}

#[cfg(test)]
mod tests {
    use super::{Data, Payload};
    use crate::error::Error;
    use proptest::prelude::*;

    fn data(slice_no: u32, block_in_slice: u16) -> Data {
        Data {
            transfer_id: 0,
            slice_no,
            block_in_slice,
            payload: Payload::from(bytes::Bytes::new()),
        }
    }

    proptest! {
        #[test]
        fn block_no_inverts_slice_coordinates(block_no: u32, blocks_per_slice in 1u32..=u16::MAX as u32) {
            let data = data(
                Data::slice_no(block_no, blocks_per_slice),
                Data::block_in_slice(block_no, blocks_per_slice),
            );

            prop_assert_eq!(data.block_no(blocks_per_slice).unwrap(), block_no as u64);
        }

        #[test]
        fn any_slice_number_is_addressable(slice_no: u32, blocks_per_slice in 1u32..=u16::MAX as u32) {
            let block_in_slice = (blocks_per_slice - 1).min(u16::MAX as u32) as u16;
            let expected = slice_no as u64 * blocks_per_slice as u64 + block_in_slice as u64;

            prop_assert_eq!(data(slice_no, block_in_slice).block_no(blocks_per_slice).unwrap(), expected);
        }
    }

    #[test]
    fn largest_coordinates_do_not_overflow() {
        let blocks_per_slice = u16::MAX as u32;
        let block_in_slice = u16::MAX - 1;
        let expected = u32::MAX as u64 * blocks_per_slice as u64 + block_in_slice as u64;

        assert_eq!(
            data(u32::MAX, block_in_slice)
                .block_no(blocks_per_slice)
                .unwrap(),
            expected
        );
    }

    #[test]
    fn rejects_zero_blocks_per_slice() {
        assert!(matches!(
            data(0, 0).block_no(0),
            Err(Error::ZeroBlocksPerSlice)
        ));
    }

    #[test]
    fn rejects_block_index_outside_its_slice() {
        assert!(matches!(
            data(0, 32).block_no(32),
            Err(Error::BlockOutsideSlice {
                block_in_slice: 32,
                blocks_per_slice: 32
            })
        ));
    }
}
