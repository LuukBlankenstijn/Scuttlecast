pub mod error;
pub mod message;
pub mod payload;

pub use error::Error;
pub use message::Message;
pub use payload::{Data, Done, Hello, Nack, Parity, Stats};

pub const MAX_DATAGRAM_SIZE: usize = 2048;
