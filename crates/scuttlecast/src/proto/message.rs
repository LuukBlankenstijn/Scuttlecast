use bincode::config::standard;
use bincode::{Decode, Encode};
use derive_more::Display;

use super::MAX_DATAGRAM_SIZE;
use super::error::Error;
use super::payload::{Data, Done, Evicted, Hello, Nak, Parity, Stats};

#[derive(Encode, Decode, Debug, Clone, PartialEq, Display)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Message {
    Hello(Hello),
    /// Receiver -> Sender, tells the sender it joined the group
    #[display("Join(transfer_id={transfer_id}, receiver_id={receiver_id})")]
    Join {
        transfer_id: u64,
        receiver_id: u64,
    },
    /// Receiver -> Sender, tells the sender it left the group
    #[display("Leave(transfer_id={transfer_id}, receiver_id={receiver_id})")]
    Leave {
        transfer_id: u64,
        receiver_id: u64,
    },
    Data(Data),
    Parity(Parity),
    Stats(Stats),
    Nak(Nak),
    Evicted(Evicted),
    Done(Done),
}

impl Message {
    pub fn transfer_id(&self) -> u64 {
        match self {
            Message::Hello(hello) => hello.transfer_id,
            Message::Join { transfer_id, .. } => *transfer_id,
            Message::Leave { transfer_id, .. } => *transfer_id,
            Message::Data(data) => data.transfer_id,
            Message::Parity(parity) => parity.transfer_id,
            Message::Stats(stats) => stats.transfer_id,
            Message::Nak(nak) => nak.transfer_id,
            Message::Evicted(evicted) => evicted.transfer_id,
            Message::Done(done) => done.transfer_id,
        }
    }

    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        Ok(bincode::encode_into_slice(self, buf, standard())?)
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
        let len = self.encode_into(&mut buf)?;
        buf.truncate(len);
        Ok(buf)
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
