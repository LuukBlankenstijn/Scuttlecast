use std::time::Duration;

pub mod error;
pub mod proto;
pub mod receiver;
pub mod sender;
pub mod state;
mod transport;

pub use transport::{Incoming, Losing};

pub const DEFAULT_BLOCK_SIZE: u32 = 1452;

/// How often a receiver reports, which is also what frees the sender's window
pub(crate) const STATS_INTERVAL: Duration = Duration::from_millis(100);

/// A receiver repeats a request this long after the last one
pub(crate) const RENAK_INTERVAL: Duration = STATS_INTERVAL.saturating_mul(2);

/// How long a repair suppresses further requests for the same shard. Below
/// `RENAK_INTERVAL`, or a receiver's genuine repeat lands inside the window and
/// is swallowed.
pub(crate) const HOLDOFF: Duration =
    Duration::from_millis(RENAK_INTERVAL.as_millis() as u64 * 3 / 4);

/// Nothing heard from the far end for this long means the machine is gone
pub const SILENCE_TIMEOUT: Duration = Duration::from_secs(10);
