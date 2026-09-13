use std::time::Duration;

pub mod error;
mod format;
pub mod proto;
pub mod receiver;
pub mod sender;
pub mod state;
mod transport;

pub use transport::{Incoming, Losing};

pub const DEFAULT_BLOCK_SIZE: u32 = 1452;

pub(crate) const STATS_INTERVAL: Duration = Duration::from_millis(100);

pub(crate) const RENAK_INTERVAL: Duration = STATS_INTERVAL.saturating_mul(2);

pub(crate) const HOLDOFF: Duration =
    Duration::from_millis(RENAK_INTERVAL.as_millis() as u64 * 3 / 4);

pub const SILENCE_TIMEOUT: Duration = Duration::from_secs(10);
