use bincode::config::standard;
use bincode::{Decode, Encode};
use derive_more::Display;

use crate::error::Error;
use crate::payload::{Data, Done, Hello, Nack, Parity, Stats};

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Message {
    Hello(Hello),
    /// Receiver -> Sender, tells the sender it joined the group
    #[display("Join(transfer_id={_0})")]
    Join(u64),
    /// Receiver -> Sender, tells the sender it left the group
    #[display("Leave(transfer_id={_0})")]
    Leave(u64),
    Data(Data),
    Parity(Parity),
    Stats(Stats),
    Nack(Nack),
    Done(Done),
}

impl Message {
    pub fn transfer_id(&self) -> u64 {
        match self {
            Message::Hello(hello) => hello.transfer_id,
            Message::Join(transfer_id) => *transfer_id,
            Message::Leave(transfer_id) => *transfer_id,
            Message::Data(data) => data.transfer_id,
            Message::Parity(parity) => parity.transfer_id,
            Message::Stats(stats) => stats.transfer_id,
            Message::Nack(nack) => nack.transfer_id,
            Message::Done(done) => done.transfer_id,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        Ok(bincode::encode_to_vec(self, standard())?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let (message, _) = bincode::decode_from_slice(bytes, standard())?;
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::Message;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip(message: Message) {
            prop_assert_eq!(Message::decode(&message.encode().unwrap()).unwrap(), message);
        }
    }
}
