use std::net::SocketAddr;
use std::time::Duration;

use crate::format::{Bytes, Elapsed, Eta, Percent, Rate};

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum LimitingFactor {
    #[default]
    Unconstrained,
    AtConfiguredMax,
    RateLimited {
        worst: u64,
        demand: f64,
    },
    WindowStalled {
        blocked_by: u64,
        slices_behind: u32,
    },
    SourceStarved {
        read_wait_ms: u32,
    },
    SinkStalled {
        receiver: u64,
        stall_ms: u32,
    },
    SenderBound {
        allowed: f64,
        achieved: f64,
    },
}

impl std::fmt::Display for LimitingFactor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unconstrained => write!(f, "unconstrained"),
            Self::AtConfiguredMax => write!(f, "at configured maximum"),
            Self::RateLimited { worst, demand } => write!(
                f,
                "rate limited by {worst}, needing repair for {:.2}% of what it was sent",
                demand * 100.0
            ),
            Self::WindowStalled {
                blocked_by,
                slices_behind,
            } => write!(
                f,
                "window held by {blocked_by}, {slices_behind} slices behind"
            ),
            Self::SourceStarved { read_wait_ms } => {
                write!(f, "input starved, waited {read_wait_ms}ms")
            }
            Self::SinkStalled { receiver, stall_ms } => write!(
                f,
                "sink of {receiver} stalled, {stall_ms}ms of the last tick spent waiting to write"
            ),
            Self::SenderBound { allowed, achieved } => write!(
                f,
                "sender bound, sending {achieved:.0} of {allowed:.0} allowed blocks per second"
            ),
        }
    }
}

pub(crate) struct Bottleneck {
    pub slowest: Option<(u64, u32)>,
    pub worst_demand: Option<(u64, f64)>,
    pub demand_threshold: f64,
    pub max_live_slices: u32,
    pub at_ceiling: bool,
    pub source_wait: Duration,
    pub worst_sink_stall: Option<(u64, u32)>,
    pub sink_stall_threshold: u32,
    pub allowed_rate: f64,
    pub achieved_rate: f64,
    pub credit_unused: bool,
    pub draining: bool,
}

const UNUSED_ALLOWANCE: f64 = 0.8;

impl Bottleneck {
    pub fn attribute(&self) -> LimitingFactor {
        if let Some((blocked_by, slices_behind)) = self.slowest
            && slices_behind >= self.max_live_slices
        {
            return LimitingFactor::WindowStalled {
                blocked_by,
                slices_behind,
            };
        }

        if let Some((worst, demand)) = self.worst_demand
            && demand > self.demand_threshold
        {
            return LimitingFactor::RateLimited { worst, demand };
        }

        if !self.draining && !self.source_wait.is_zero() {
            return LimitingFactor::SourceStarved {
                read_wait_ms: self.source_wait.as_millis().min(u32::MAX as u128) as u32,
            };
        }

        if let Some((receiver, stall_ms)) = self.worst_sink_stall
            && stall_ms >= self.sink_stall_threshold
        {
            return LimitingFactor::SinkStalled { receiver, stall_ms };
        }
        if self.at_ceiling {
            return LimitingFactor::AtConfiguredMax;
        }

        if self.credit_unused && self.achieved_rate < self.allowed_rate * UNUSED_ALLOWANCE {
            return LimitingFactor::SenderBound {
                allowed: self.allowed_rate,
                achieved: self.achieved_rate,
            };
        }

        LimitingFactor::Unconstrained
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReceiverState {
    pub receiver_id: u64,
    pub address: SocketAddr,
    pub windowed_loss: f64,
    pub unrecovered_loss: f64,
    pub lifetime_loss: f64,
    pub next_needed_slice: u32,
    pub slices_behind: u32,
    pub naks: u64,
    pub sink_stall_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct TransferState {
    pub transfer_id: u64,
    pub block_size: u32,
    pub blocks_per_second: f64,
    pub blocks_sent: u64,
    pub slices_emitted: u32,
    pub parity_shards: u8,
    pub total_blocks: Option<u64>,
    pub announced_bytes: Option<u64>,
    pub draining: bool,
    pub limiting: LimitingFactor,
    pub receivers: Vec<ReceiverState>,
    pub elapsed: Duration,
}

impl TransferState {
    pub fn bytes_per_second(&self) -> f64 {
        self.blocks_per_second * self.block_size as f64
    }

    pub fn bytes_sent(&self) -> u64 {
        self.blocks_sent * self.block_size as u64
    }

    pub fn fraction_complete(&self) -> Option<f64> {
        self.announced_bytes
            .filter(|announced| *announced > 0)
            .map(|announced| (self.bytes_sent() as f64 / announced as f64).min(1.0))
    }

    pub fn progress(&self) -> impl std::fmt::Display {
        Percent(self.fraction_complete())
    }

    pub fn sent(&self) -> impl std::fmt::Display {
        Bytes(self.bytes_sent())
    }

    pub fn rate(&self) -> impl std::fmt::Display {
        Rate(self.bytes_per_second())
    }

    pub fn eta(&self) -> impl std::fmt::Display {
        Eta {
            bytes_left: bytes_left(self.announced_bytes, self.bytes_sent()),
            bytes_per_second: self.bytes_per_second(),
        }
    }

    pub fn running_for(&self) -> impl std::fmt::Display {
        Elapsed(self.elapsed)
    }

    pub fn slowest(&self) -> Option<&ReceiverState> {
        self.receivers
            .iter()
            .max_by_key(|receiver| receiver.slices_behind)
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReceiveState {
    pub transfer_id: u64,
    pub announced_bytes: Option<u64>,
    pub received_bytes: u64,
    pub next_needed_slice: u32,
    pub duplicates: u64,
    pub naks: u64,
    pub bytes_per_second: f64,
    pub elapsed: Duration,
}

impl ReceiveState {
    pub fn fraction_complete(&self) -> Option<f64> {
        self.announced_bytes
            .filter(|announced| *announced > 0)
            .map(|announced| (self.received_bytes as f64 / announced as f64).min(1.0))
    }

    pub fn progress(&self) -> impl std::fmt::Display {
        Percent(self.fraction_complete())
    }

    pub fn received(&self) -> impl std::fmt::Display {
        Bytes(self.received_bytes)
    }

    pub fn rate(&self) -> impl std::fmt::Display {
        Rate(self.bytes_per_second)
    }

    pub fn eta(&self) -> impl std::fmt::Display {
        Eta {
            bytes_left: bytes_left(self.announced_bytes, self.received_bytes),
            bytes_per_second: self.bytes_per_second,
        }
    }

    pub fn running_for(&self) -> impl std::fmt::Display {
        Elapsed(self.elapsed)
    }
}

fn bytes_left(announced_bytes: Option<u64>, done: u64) -> Option<u64> {
    Some(announced_bytes?.saturating_sub(done))
}

#[cfg(test)]
mod tests {
    use super::{Bottleneck, LimitingFactor};
    use std::time::Duration;

    fn healthy() -> Bottleneck {
        Bottleneck {
            slowest: Some((7, 3)),
            worst_demand: Some((7, 0.0)),
            demand_threshold: 0.005,
            max_live_slices: 512,
            at_ceiling: false,
            source_wait: Duration::ZERO,
            worst_sink_stall: Some((7, 0)),
            sink_stall_threshold: 25,
            allowed_rate: 1000.0,
            achieved_rate: 1000.0,
            credit_unused: true,
            draining: false,
        }
    }

    #[test]
    fn an_allowance_the_sender_cannot_use_is_the_senders_own_limit() {
        let bottleneck = Bottleneck {
            allowed_rate: 1000.0,
            achieved_rate: 300.0,
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::SenderBound {
                allowed: 1000.0,
                achieved: 300.0
            }
        );
    }

    #[test]
    fn a_receiver_that_cannot_write_outranks_the_senders_own_socket() {
        let bottleneck = Bottleneck {
            allowed_rate: 1000.0,
            achieved_rate: 300.0,
            worst_sink_stall: Some((9, 60)),
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::SinkStalled {
                receiver: 9,
                stall_ms: 60
            }
        );
    }

    #[test]
    fn a_sink_pausing_briefly_between_writes_is_not_a_limit() {
        let bottleneck = Bottleneck {
            worst_sink_stall: Some((9, 24)),
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn repairs_outrank_a_stalled_sink() {
        let bottleneck = Bottleneck {
            worst_demand: Some((9, 0.2)),
            worst_sink_stall: Some((9, 60)),
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::RateLimited {
                worst: 9,
                demand: 0.2
            }
        );
    }

    #[test]
    fn an_allowance_nearly_used_up_is_not_a_limit() {
        let bottleneck = Bottleneck {
            allowed_rate: 1000.0,
            achieved_rate: 900.0,
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn an_allowance_the_pacer_itself_used_up_is_not_the_senders_limit() {
        let bottleneck = Bottleneck {
            allowed_rate: 1000.0,
            achieved_rate: 300.0,
            credit_unused: false,
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn a_healthy_transfer_is_unconstrained() {
        assert_eq!(healthy().attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn a_receiver_a_whole_window_behind_holds_the_window() {
        let bottleneck = Bottleneck {
            slowest: Some((7, 512)),
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::WindowStalled {
                blocked_by: 7,
                slices_behind: 512
            }
        );
    }

    #[test]
    fn loss_past_the_threshold_names_the_worst_receiver() {
        let bottleneck = Bottleneck {
            worst_demand: Some((8, 0.2)),
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::RateLimited {
                worst: 8,
                demand: 0.2
            }
        );
    }

    #[test]
    fn loss_below_the_threshold_is_not_the_limit() {
        let bottleneck = Bottleneck {
            worst_demand: Some((8, 0.001)),
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn a_stalled_window_outranks_loss() {
        let bottleneck = Bottleneck {
            slowest: Some((7, 600)),
            worst_demand: Some((8, 0.2)),
            ..healthy()
        };

        assert!(matches!(
            bottleneck.attribute(),
            LimitingFactor::WindowStalled { .. }
        ));
    }

    #[test]
    fn waiting_on_the_input_is_reported_in_milliseconds() {
        let bottleneck = Bottleneck {
            source_wait: Duration::from_millis(40),
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::SourceStarved { read_wait_ms: 40 }
        );
    }

    #[test]
    fn draining_never_looks_like_a_starved_input() {
        let bottleneck = Bottleneck {
            source_wait: Duration::from_millis(40),
            draining: true,
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn a_configured_ceiling_is_reported_when_nothing_else_binds() {
        let bottleneck = Bottleneck {
            at_ceiling: true,
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::AtConfiguredMax);
    }
}
