use std::time::Duration;

pub mod error;
pub mod receiver;
pub mod sender;
pub mod transport;

pub const BLOCK_SIZE: usize = 1400;

pub const STATS_INTERVAL: Duration = Duration::from_millis(200);

/// A receiver repeats a request this long after the last one
pub const RENAK_INTERVAL: Duration = STATS_INTERVAL.saturating_mul(2);

/// How long a repair suppresses further requests for the same shard. Below
/// `RENAK_INTERVAL`, or a receiver's genuine repeat lands inside the window and
/// is swallowed.
pub const HOLDOFF: Duration = Duration::from_millis(RENAK_INTERVAL.as_millis() as u64 * 3 / 4);

/// No `Stats` from a participant for this long means the machine is gone
pub const LIVENESS_TIMEOUT: Duration = Duration::from_secs(3);

/// No packet of any kind for this long means the sender is gone
pub const SILENCE_TIMEOUT: Duration = Duration::from_secs(10);
