use std::{collections::HashMap, time::Duration};

use tokio::time::Instant;

/// Repair demand a healthy transfer stays under: the fraction of
/// transmissions receivers have to ask for again. Below this the rate grows;
/// above it the rate backs off.
pub const REPAIR_THRESHOLD: f64 = 0.005;

/// How much a reduction has to lower demand to count as having helped
const DEMAND_IMPROVEMENT: f64 = 0.8;
const K: f64 = 4.0;
const MIN_FACTOR: f64 = 0.5;
const GOOD_TICKS_NEEDED: u32 = 3;
const SLOW_START_FACTOR: f64 = 4.0;
const RECOVERY_TARGET: Duration = Duration::from_secs(10);
pub const TICK_INTERVAL: Duration = Duration::from_millis(200);
const RATE_RECOVERY_FRACTION: f64 =
    TICK_INTERVAL.as_millis() as f64 / RECOVERY_TARGET.as_millis() as f64;
const MIN_RATE: f64 = 50.0;
const INITIAL_RATE: f64 = 6000.0;
const STALENESS_LIMIT: Duration = Duration::from_secs(1);
const TAU: Duration = Duration::from_millis(300);
const BURST_QUANTUM: Duration = Duration::from_millis(2);
const MIN_BURST: f64 = 4.0;
const MAX_BURST: f64 = 4096.0;

#[derive(Debug)]
pub struct Pacer {
    credit: f64,
    last_refill: Instant,
    waited: bool,
}

impl Pacer {
    pub fn new() -> Self {
        Self {
            credit: 0.0,
            last_refill: Instant::now(),
            waited: false,
        }
    }

    pub fn refill(&mut self, rate: f64) {
        let now = Instant::now();
        let earned = now.duration_since(self.last_refill).as_secs_f64() * rate;
        self.credit = (self.credit + earned).min(burst_cap(rate));
        self.last_refill = now;
    }

    /// Records that traffic was ready and the pacer made it wait
    pub fn waited(&mut self) {
        self.waited = true;
    }

    pub fn budget(&self) -> usize {
        self.credit as usize
    }

    pub fn consume(&mut self) {
        self.credit -= 1.0;
    }

    pub fn time_until_credit(&self, rate: f64) -> Duration {
        let wanted = 1.0 - self.credit;
        if wanted <= 0.0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(wanted / rate)
    }

    pub fn take_starvation(&mut self) -> bool {
        std::mem::take(&mut self.waited)
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
    ceiling: Option<f64>,
    good_ticks: u32,
    slow_start_limit: Option<f64>,
    demand_when_reduced: Option<f64>,
}

impl RateController {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            rate: INITIAL_RATE,
            ceiling: None,
            good_ticks: 0,
            slow_start_limit: None,
            demand_when_reduced: None,
        }
    }

    pub fn capped_at(mut self, ceiling: Option<f64>) -> Self {
        self.ceiling = ceiling;
        self
    }

    pub fn at_ceiling(&self) -> bool {
        self.ceiling.is_some_and(|ceiling| self.rate >= ceiling)
    }

    /// The receiver needing the most repair right now, which is the one the
    /// rate follows
    pub fn worst(&self) -> Option<(u64, f64)> {
        let now = Instant::now();
        self.clients
            .iter()
            .filter(|(_, client)| now.duration_since(client.last_report_at) < STALENESS_LIMIT)
            .max_by(|(_, left), (_, right)| left.ewma.total_cmp(&right.ewma))
            .map(|(id, client)| (*id, client.ewma))
    }

    /// `demand` is the fraction of transmissions a receiver had to ask for
    /// again, so loss the parity absorbed never reaches the rate.
    pub fn on_report(&mut self, id: u64, demand: f64) {
        let now = Instant::now();
        let entry = self.clients.entry(id).or_insert(ClientLoss {
            ewma: demand,
            last_report_at: now,
        });
        let delta = now.duration_since(entry.last_report_at).as_secs_f64();
        let alpha = 1f64 - (-delta / TAU.as_secs_f64()).exp();
        entry.ewma = alpha * demand + (1f64 - alpha) * entry.ewma;
        entry.last_report_at = now;
    }

    pub fn tick(&mut self, rate_limited: bool) {
        let Some(worst) = self.worst_recent_loss() else {
            return;
        };

        if worst > REPAIR_THRESHOLD {
            if self.backing_off_helps(worst) {
                self.good_ticks = 0;
                self.slow_start_limit = Some(self.rate * self.reduction_factor(worst));
                self.rate = (self.rate * self.reduction_factor(worst)).max(MIN_RATE);
            }
            self.demand_when_reduced = Some(worst);
            return;
        }
        self.demand_when_reduced = None;

        if !rate_limited {
            return;
        }

        self.good_ticks += 1;
        if self.in_slow_start() || self.good_ticks >= GOOD_TICKS_NEEDED {
            self.rate = match self.ceiling {
                Some(ceiling) => self.grown_rate().min(ceiling),
                None => self.grown_rate(),
            };
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

    /// Reducing the rate only helps when the loss came from sending too fast.
    /// A link that drops a steady fraction of packets drops the same fraction
    /// however slowly it is fed, so a reduction that did not lower demand is
    /// not repeated: otherwise the rate ratchets to the floor and the transfer
    /// crawls for nothing.
    fn backing_off_helps(&self, demand: f64) -> bool {
        self.demand_when_reduced
            .is_none_or(|before| demand < before * DEMAND_IMPROVEMENT)
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

        controller.on_report(1, 0.0);

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

        controller.on_report(1, 0.5);
        controller.tick(true);
        let after_loss = controller.rate();
        assert_eq!(after_loss, INITIAL_RATE * MIN_FACTOR);

        tokio::time::advance(Duration::from_secs(5)).await;
        controller.on_report(1, 0.0);
        ticks(&mut controller, 3);

        let expected_step = after_loss * RATE_RECOVERY_FRACTION;
        assert_eq!(controller.rate(), after_loss + expected_step);
    }

    #[tokio::test(start_paused = true)]
    async fn demand_that_a_slower_rate_does_not_relieve_is_not_chased_down() {
        let mut controller = RateController::new();

        for _ in 0..10 {
            controller.on_report(1, 0.5);
            controller.tick(true);
            tokio::time::advance(Duration::from_millis(200)).await;
        }

        assert_eq!(controller.rate(), INITIAL_RATE * MIN_FACTOR);
    }

    #[tokio::test(start_paused = true)]
    async fn demand_that_falls_as_the_rate_falls_keeps_being_chased_down() {
        let mut controller = RateController::new();
        let mut demand = 0.5;

        for _ in 0..4 {
            controller.on_report(1, demand);
            controller.tick(true);
            tokio::time::advance(Duration::from_millis(200)).await;
            demand /= 4.0;
        }

        assert!(
            controller.rate() < INITIAL_RATE * MIN_FACTOR,
            "rate was {}",
            controller.rate()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn holds_the_rate_while_no_reports_are_fresh() {
        let mut controller = RateController::new();

        controller.on_report(1, 1.0);
        controller.tick(true);
        let after_loss = controller.rate();

        tokio::time::advance(Duration::from_secs(2)).await;
        ticks(&mut controller, 5);

        assert_eq!(controller.rate(), after_loss);
    }

    #[tokio::test(start_paused = true)]
    async fn a_report_asking_for_nothing_leaves_the_rate_growing() {
        let mut controller = RateController::new();

        controller.on_report(1, 0.0);
        controller.tick(true);

        assert_eq!(controller.rate(), INITIAL_RATE * SLOW_START_FACTOR);
    }

    #[tokio::test(start_paused = true)]
    async fn credit_accrues_at_the_rate() {
        let mut pacer = Pacer::new();
        let rate = 5_000.0;
        assert_eq!(pacer.budget(), 0);

        tokio::time::advance(Duration::from_millis(2)).await;
        pacer.refill(rate);

        assert_eq!(pacer.budget(), 10);
        for _ in 0..10 {
            pacer.consume();
        }
        assert_eq!(pacer.budget(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_cannot_bank_more_than_one_quantum() {
        let mut pacer = Pacer::new();
        let rate = 100_000.0;

        tokio::time::advance(Duration::from_secs(60)).await;
        pacer.refill(rate);

        let banked = (rate * BURST_QUANTUM.as_secs_f64()) as usize;
        assert_eq!(pacer.budget(), banked);
        for _ in 0..banked {
            pacer.consume();
        }
        assert_eq!(pacer.budget(), 0);
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

        assert_eq!(pacer.budget(), MIN_BURST as usize);
    }

    #[tokio::test(start_paused = true)]
    async fn does_not_grow_while_the_sender_is_not_rate_limited() {
        let mut controller = RateController::new();
        let start = controller.rate();

        controller.on_report(1, 0.0);
        for _ in 0..10 {
            controller.tick(false);
        }

        assert_eq!(controller.rate(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn loss_still_reduces_when_not_rate_limited() {
        let mut controller = RateController::new();

        controller.on_report(1, 0.5);
        controller.tick(false);

        assert_eq!(controller.rate(), INITIAL_RATE * MIN_FACTOR);
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_for_credit_is_recorded_and_cleared() {
        let mut pacer = Pacer::new();
        assert!(!pacer.take_starvation());

        pacer.waited();
        assert!(pacer.take_starvation());
        assert!(!pacer.take_starvation());
    }

    #[tokio::test(start_paused = true)]
    async fn unspent_credit_is_not_a_wait() {
        let mut pacer = Pacer::new();

        tokio::time::advance(Duration::from_secs(1)).await;
        pacer.refill(INITIAL_RATE);
        pacer.consume();

        assert!(!pacer.take_starvation());
    }

    #[tokio::test(start_paused = true)]
    async fn recovery_step_scales_with_the_operating_point() {
        assert_eq!(RATE_RECOVERY_FRACTION, 0.02);
    }
}
