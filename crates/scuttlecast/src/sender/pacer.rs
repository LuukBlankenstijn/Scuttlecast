use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;

const LOSS_THRESHOLD: f64 = 0.005;
const K: f64 = 4.0;
const MIN_FACTOR: f64 = 0.5;
const GOOD_TICKS_NEEDED: u32 = 3;
const SLOW_START_FACTOR: f64 = 2.0;
const RECOVERY_TARGET: Duration = Duration::from_secs(10);
pub const TICK_INTERVAL: Duration = Duration::from_millis(200);
const RATE_RECOVERY_FRACTION: f64 =
    TICK_INTERVAL.as_millis() as f64 / RECOVERY_TARGET.as_millis() as f64;
const MIN_RATE: f64 = 50.0;
const INITIAL_RATE: f64 = 200.0;
const STALENESS_LIMIT: Duration = Duration::from_secs(1);
const TAU: Duration = Duration::from_millis(300);
const BURST_QUANTUM: Duration = Duration::from_millis(2);
const MIN_BURST: f64 = 4.0;
const MAX_BURST: f64 = 4096.0;

#[derive(Debug)]
pub struct Pacer {
    credit: f64,
    last_refill: Instant,
    starved: bool,
}

impl Pacer {
    pub fn new() -> Self {
        Self {
            credit: 0.0,
            last_refill: Instant::now(),
            starved: false,
        }
    }

    pub fn refill(&mut self, rate: f64) {
        let now = Instant::now();
        let earned = now.duration_since(self.last_refill).as_secs_f64() * rate;
        self.credit = (self.credit + earned).min(burst_cap(rate));
        self.last_refill = now;
        self.starved |= self.credit < 1.0;
    }

    pub fn has_credit(&self) -> bool {
        self.credit >= 1.0
    }

    pub fn consume(&mut self) {
        self.credit -= 1.0;
    }

    pub fn time_until_credit(&self, rate: f64) -> Duration {
        if self.has_credit() {
            return Duration::ZERO;
        }
        Duration::from_secs_f64((1.0 - self.credit) / rate)
    }

    pub fn take_starvation(&mut self) -> bool {
        std::mem::take(&mut self.starved)
    }
}

fn burst_cap(rate: f64) -> f64 {
    (rate * BURST_QUANTUM.as_secs_f64()).clamp(MIN_BURST, MAX_BURST)
}

struct ClientLoss {
    ewma: f64,
    last_report_at: Instant,
}

pub struct RateController {
    clients: HashMap<u64, ClientLoss>,
    rate: f64,
    good_ticks: u32,
    slow_start_limit: Option<f64>,
}

impl RateController {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            rate: INITIAL_RATE,
            good_ticks: 0,
            slow_start_limit: None,
        }
    }

    pub fn on_report(&mut self, id: u64, blocks_seen: u64, blocks_expected: u64) {
        if blocks_expected == 0 {
            return;
        }
        let now = Instant::now();
        let loss = 1f64 - blocks_seen as f64 / blocks_expected as f64;
        let entry = self.clients.entry(id).or_insert(ClientLoss {
            ewma: loss,
            last_report_at: now,
        });
        let delta = now.duration_since(entry.last_report_at).as_secs_f64();
        let alpha = 1f64 - (-delta / TAU.as_secs_f64()).exp();
        entry.ewma = alpha * loss + (1f64 - alpha) * entry.ewma;
        entry.last_report_at = now;
    }

    pub fn tick(&mut self, rate_limited: bool) {
        let Some(worst) = self.worst_recent_loss() else {
            return;
        };

        if worst > LOSS_THRESHOLD {
            self.good_ticks = 0;
            self.slow_start_limit = Some(self.rate * self.reduction_factor(worst));
            self.rate = (self.rate * self.reduction_factor(worst)).max(MIN_RATE);
            return;
        }

        if !rate_limited {
            return;
        }

        self.good_ticks += 1;
        if self.in_slow_start() || self.good_ticks >= GOOD_TICKS_NEEDED {
            self.rate = self.grown_rate();
        }
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    fn reduction_factor(&self, worst: f64) -> f64 {
        (1.0 - K * worst).clamp(MIN_FACTOR, 1.0)
    }

    fn worst_recent_loss(&self) -> Option<f64> {
        let now = Instant::now();
        self.clients
            .values()
            .filter(|c| now.duration_since(c.last_report_at) < STALENESS_LIMIT)
            .map(|c| c.ewma)
            .reduce(f64::max)
    }

    fn in_slow_start(&self) -> bool {
        self.slow_start_limit.is_none_or(|limit| self.rate < limit)
    }

    fn additive_step(&self) -> f64 {
        let operating_point = self.slow_start_limit.unwrap_or(self.rate);
        (operating_point * RATE_RECOVERY_FRACTION).max(1.0)
    }

    fn grown_rate(&self) -> f64 {
        if self.in_slow_start() {
            self.rate * SLOW_START_FACTOR
        } else {
            self.rate + self.additive_step()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BURST_QUANTUM, INITIAL_RATE, MAX_BURST, MIN_BURST, MIN_FACTOR, MIN_RATE, Pacer,
        RATE_RECOVERY_FRACTION, RateController, SLOW_START_FACTOR,
    };
    use std::time::Duration;

    fn ticks(controller: &mut RateController, count: usize) {
        for _ in 0..count {
            controller.tick(true);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn doubles_every_tick_during_slow_start() {
        let mut controller = RateController::new();

        controller.on_report(1, 100, 100);

        controller.tick(true);
        assert_eq!(controller.rate(), INITIAL_RATE * SLOW_START_FACTOR);

        controller.tick(true);
        assert_eq!(
            controller.rate(),
            INITIAL_RATE * SLOW_START_FACTOR * SLOW_START_FACTOR
        );
    }

    #[tokio::test(start_paused = true)]
    async fn grows_additively_after_a_loss() {
        let mut controller = RateController::new();

        controller.on_report(1, 50, 100);
        controller.tick(true);
        let after_loss = controller.rate();
        assert_eq!(after_loss, INITIAL_RATE * MIN_FACTOR);

        tokio::time::advance(Duration::from_secs(5)).await;
        controller.on_report(1, 100, 100);
        ticks(&mut controller, 3);

        let expected_step = after_loss * RATE_RECOVERY_FRACTION;
        assert_eq!(controller.rate(), after_loss + expected_step);
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_loss_never_drops_below_the_floor() {
        let mut controller = RateController::new();

        for _ in 0..10 {
            controller.on_report(1, 50, 100);
            controller.tick(true);
            tokio::time::advance(Duration::from_millis(200)).await;
        }

        assert_eq!(controller.rate(), MIN_RATE);
    }

    #[tokio::test(start_paused = true)]
    async fn holds_the_rate_while_no_reports_are_fresh() {
        let mut controller = RateController::new();

        controller.on_report(1, 0, 100);
        controller.tick(true);
        let after_loss = controller.rate();

        tokio::time::advance(Duration::from_secs(2)).await;
        ticks(&mut controller, 5);

        assert_eq!(controller.rate(), after_loss);
    }

    #[tokio::test(start_paused = true)]
    async fn reports_without_expected_blocks_are_ignored() {
        let mut controller = RateController::new();

        controller.on_report(1, 0, 0);
        controller.on_report(1, 100, 100);
        controller.tick(true);

        assert_eq!(controller.rate(), INITIAL_RATE * SLOW_START_FACTOR);
    }

    #[tokio::test(start_paused = true)]
    async fn credit_accrues_at_the_rate() {
        let mut pacer = Pacer::new();
        assert!(!pacer.has_credit());

        tokio::time::advance(Duration::from_millis(100)).await;
        pacer.refill(50.0);

        assert!(pacer.has_credit());
        for _ in 0..5 {
            pacer.consume();
        }
        assert!(!pacer.has_credit());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_cannot_bank_more_than_one_quantum() {
        let mut pacer = Pacer::new();
        let rate = 100_000.0;

        tokio::time::advance(Duration::from_secs(60)).await;
        pacer.refill(rate);

        let banked = (rate * BURST_QUANTUM.as_secs_f64()) as usize;
        for _ in 0..banked {
            assert!(pacer.has_credit());
            pacer.consume();
        }
        assert!(!pacer.has_credit());
    }

    #[tokio::test(start_paused = true)]
    async fn burst_scales_with_the_rate() {
        assert_eq!(super::burst_cap(100_000.0), 200.0);
        assert_eq!(super::burst_cap(844_000.0), 1688.0);
        assert_eq!(super::burst_cap(10_000_000.0), MAX_BURST);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_rates_still_get_a_whole_block_of_credit() {
        assert_eq!(super::burst_cap(MIN_RATE), MIN_BURST);

        let mut pacer = Pacer::new();
        tokio::time::advance(Duration::from_secs(1)).await;
        pacer.refill(MIN_RATE);

        assert!(pacer.has_credit());
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_grow_while_the_sender_is_not_rate_limited() {
        let mut controller = RateController::new();
        let start = controller.rate();

        controller.on_report(1, 100, 100);
        for _ in 0..10 {
            controller.tick(false);
        }

        assert_eq!(controller.rate(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn loss_still_reduces_when_not_rate_limited() {
        let mut controller = RateController::new();

        controller.on_report(1, 50, 100);
        controller.tick(false);

        assert_eq!(controller.rate(), INITIAL_RATE * MIN_FACTOR);
    }

    #[tokio::test(start_paused = true)]
    async fn starvation_is_recorded_and_cleared() {
        let mut pacer = Pacer::new();

        pacer.refill(INITIAL_RATE);
        assert!(pacer.take_starvation());
        assert!(!pacer.take_starvation());

        tokio::time::advance(Duration::from_secs(1)).await;
        pacer.refill(INITIAL_RATE);
        assert!(!pacer.take_starvation());
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_step_scales_with_the_operating_point() {
        assert_eq!(RATE_RECOVERY_FRACTION, 0.02);
    }
}
