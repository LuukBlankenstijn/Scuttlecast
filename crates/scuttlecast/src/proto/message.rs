use bincode::config::standard;
use bincode::{Decode, Encode};
use derive_more::Display;

use super::error::Error;
use super::frame::CONTROL_TAG;
use super::payload::{Done, Evicted, Hello, Nak, Stats};

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
            Message::Stats(stats) => stats.transfer_id,
            Message::Nak(nak) => nak.transfer_id,
            Message::Evicted(evicted) => evicted.transfer_id,
            Message::Done(done) => done.transfer_id,
        }
    }

    pub fn encode_into(&self, buf: &mut [u8]) -> Result<usize, Error> {
        buf[0] = CONTROL_TAG;
        Ok(1 + bincode::encode_into_slice(self, &mut buf[1..], standard())?)
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![0u8; super::MAX_CONTROL_SIZE];
        let len = self.encode_into(&mut buf)?;
        buf.truncate(len);
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let Some((&tag, body)) = bytes.split_first() else {
            return Err(Error::FrameTooShort(0));
        };
        if tag != CONTROL_TAG {
            return Err(Error::UnknownTag(tag));
        }

        let (message, _) = bincode::decode_from_slice(body, standard())?;
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::Message;
    use crate::proto::error::Error;
    use crate::proto::frame::FRAME_TAG;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn roundtrip(message: Message) {
            prop_assert_eq!(Message::decode(&message.encode().unwrap()).unwrap(), message);
        }
    }

    #[test]
    fn refuses_to_read_a_shard_as_a_control_message() {
        assert!(
            matches!(Message::decode(&[FRAME_TAG, 0, 0, 0]), Err(Error::UnknownTag(tag)) if tag == FRAME_TAG)
        );
    }

    #[test]
    fn refuses_an_empty_datagram() {
        assert!(matches!(Message::decode(&[]), Err(Error::FrameTooShort(0))));
    }
}
