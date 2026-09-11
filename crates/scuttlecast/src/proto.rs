pub mod error;
pub mod frame;
pub mod message;
pub mod payload;

pub use error::Error;
pub(crate) use frame::{CONTROL_TAG, FrameBatch};
pub use frame::{Frame, HEADER_SIZE};
pub use message::Message;
pub use payload::{Done, Evicted, Hello, Nak, Stats};

pub(crate) const MAX_CONTROL_SIZE: usize = 2048;
