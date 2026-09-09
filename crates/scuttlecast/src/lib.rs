use std::time::Duration;

pub mod error;
pub mod receiver;
pub mod sender;
pub mod state;
pub mod transport;

pub const BLOCK_SIZE: usize = 1400;

pub const STATS_INTERVAL: Duration = Duration::from_millis(200);

/// A receiver repeats a request this long after the last one
pub const RENAK_INTERVAL: Duration = STATS_INTERVAL.saturating_mul(2);

/// How long a repair suppresses further requests for the same shard. Below
/// `RENAK_INTERVAL`, or a receiver's genuine repeat lands inside the window and
/// is swallowed.
pub const HOLDOFF: Duration = Duration::from_millis(RENAK_INTERVAL.as_millis() as u64 * 3 / 4);

/// Nothing heard from the far end for this long means the machine is gone.
/// Both sides give up on the same rule: a receiver reports every
/// `STATS_INTERVAL` and a sender sends far more often, so silence this long is
/// a death rather than a slow disk.
pub const SILENCE_TIMEOUT: Duration = Duration::from_secs(10);
