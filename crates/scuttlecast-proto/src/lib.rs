use bincode::{Decode, Encode};

#[derive(Encode, Decode, Debug, Clone, PartialEq)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Message {
    /// Sender -> Group, announces the transfer to the group
    Hello {
        transfer_id: u64,
        file_name: String,
        file_size: u64,
        block_size: u32,
        blocks_per_slice: u32,
        total_blocks: u32,
    },
    /// Receiver -> Sender, tells the sender it joined the group
    Join { transfer_id: u64 },
    /// Receiver -> Sender, tells the sender it left the group
    Leave { transfer_id: u64 },
    /// Sender -> Group, one block of data
    Data {
        transfer_id: u64,
        slice_no: u32,
        block_in_slice: u16,
        payload: Vec<u8>,
    },
    /// Sender -> Group, one block of parity data
    Parity {
        transfer_id: u64,
        slice_no: u32,
        parity_index: u16,
        payload: Vec<u8>,
    },
    /// Receiver -> Sender, used to feed the rate controller
    Stats {
        transfer_id: u64,
        received_since_last: u32,
        expected_since_last: u32,
    },
    /// Receiver -> Sender, request a missing block from the Sender
    Nack {
        transfer_id: u64,
        slice_no: u32,
        missing: Vec<u16>,
    },
    /// Sender -> Group, announces the transfer is done
    Done { transfer_id: u64 },
}

#[cfg(test)]
mod tests {
    use super::Message;
    use bincode::config::standard;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip(msg: Message) {
            let bytes = bincode::encode_to_vec(&msg, standard()).unwrap();
            let (decoded, _): (Message, usize) =
                bincode::decode_from_slice(&bytes, standard()).unwrap();
            prop_assert_eq!(msg, decoded);
        }
    }
}
