pub mod error;
pub mod message;
pub mod payload;

pub use error::Error;
pub use message::Message;
pub use payload::{Data, Done, Evicted, Hello, Nak, Parity, Payload, Stats};

pub const MAX_DATAGRAM_SIZE: usize = 2048;
