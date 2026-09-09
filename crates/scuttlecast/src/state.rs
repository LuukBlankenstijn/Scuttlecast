use std::net::SocketAddr;
use std::time::Duration;

/// Why the transfer is not going faster. Computed every control tick, always
/// attributable: the window is held by one receiver and the rate by the
/// worst-conditioned one.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum LimitingFactor {
    #[default]
    Unconstrained,
    AtConfiguredMax,
    RateLimited {
        worst: u64,
        loss: f64,
    },
    WindowStalled {
        blocked_by: u64,
        slices_behind: u32,
    },
    SourceStarved {
        read_wait_ms: u32,
    },
    /// The sender cannot push its own socket any faster, so the allowance
    /// goes unused
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
            Self::RateLimited { worst, loss } => {
                write!(f, "rate limited by {worst} losing {:.2}%", loss * 100.0)
            }
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
            Self::SenderBound { allowed, achieved } => write!(
                f,
                "sender bound, sending {achieved:.0} of {allowed:.0} allowed blocks per second"
            ),
        }
    }
}

/// What the sender knows about its own bottleneck at one control tick
pub struct Bottleneck {
    /// The participant with the lowest progress, and how far behind it is
    pub slowest: Option<(u64, u32)>,
    /// The participant losing the most, and its recent loss
    pub worst_loss: Option<(u64, f64)>,
    pub loss_threshold: f64,
    pub max_live_slices: u32,
    pub at_ceiling: bool,
    /// Time spent with sending credit but nothing to send
    pub source_wait: Duration,
    pub allowed_rate: f64,
    pub achieved_rate: f64,
    /// The pacer had permission to send and did not use it, which rules out
    /// the pacer itself being the constraint
    pub credit_unused: bool,
    pub draining: bool,
}

/// How much of its allowance the sender has to be missing before its own
/// socket counts as the constraint
const UNUSED_ALLOWANCE: f64 = 0.8;

impl Bottleneck {
    /// Order matters. A window held open by one receiver still shows a healthy
    /// rate, and an input that cannot keep up looks like everything being
    /// idle, so both have to be ruled out before believing the rate.
    pub fn attribute(&self) -> LimitingFactor {
        if let Some((blocked_by, slices_behind)) = self.slowest
            && slices_behind >= self.max_live_slices
        {
            return LimitingFactor::WindowStalled {
                blocked_by,
                slices_behind,
            };
        }

        if let Some((worst, loss)) = self.worst_loss
            && loss > self.loss_threshold
        {
            return LimitingFactor::RateLimited { worst, loss };
        }

        if !self.draining && !self.source_wait.is_zero() {
            return LimitingFactor::SourceStarved {
                read_wait_ms: self.source_wait.as_millis().min(u32::MAX as u128) as u32,
            };
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
    /// Loss over the last reporting window, which is what drives the rate
    pub windowed_loss: f64,
    /// Loss over the whole transfer so far
    pub lifetime_loss: f64,
    pub next_needed_slice: u32,
    pub slices_behind: u32,
    pub naks: u64,
    pub sink_stall_ms: u32,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct TransferState {
    pub transfer_id: u64,
    pub blocks_per_second: f64,
    pub blocks_sent: u64,
    pub slices_emitted: u32,
    pub total_blocks: Option<u64>,
    pub draining: bool,
    pub limiting: LimitingFactor,
    pub receivers: Vec<ReceiverState>,
}

impl TransferState {
    pub fn bytes_per_second(&self) -> f64 {
        self.blocks_per_second * crate::BLOCK_SIZE as f64
    }

    /// The receiver holding the group back, which is the one worth looking at
    pub fn slowest(&self) -> Option<&ReceiverState> {
        self.receivers
            .iter()
            .max_by_key(|receiver| receiver.slices_behind)
    }
}

#[cfg(test)]
mod tests {
    use super::{Bottleneck, LimitingFactor};
    use std::time::Duration;

    fn healthy() -> Bottleneck {
        Bottleneck {
            slowest: Some((7, 3)),
            worst_loss: Some((7, 0.0)),
            loss_threshold: 0.005,
            max_live_slices: 512,
            at_ceiling: false,
            source_wait: Duration::ZERO,
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
            worst_loss: Some((8, 0.2)),
            ..healthy()
        };

        assert_eq!(
            bottleneck.attribute(),
            LimitingFactor::RateLimited {
                worst: 8,
                loss: 0.2
            }
        );
    }

    #[test]
    fn loss_below_the_threshold_is_not_the_limit() {
        let bottleneck = Bottleneck {
            worst_loss: Some((8, 0.001)),
            ..healthy()
        };

        assert_eq!(bottleneck.attribute(), LimitingFactor::Unconstrained);
    }

    #[test]
    fn a_stalled_window_outranks_loss() {
        let bottleneck = Bottleneck {
            slowest: Some((7, 600)),
            worst_loss: Some((8, 0.2)),
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
