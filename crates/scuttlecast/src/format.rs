use std::fmt;
use std::time::Duration;

const UNITS: [(&str, u64); 4] = [
    ("GiB", 1 << 30),
    ("MiB", 1 << 20),
    ("KiB", 1 << 10),
    ("B", 1),
];

const UNKNOWN: &str = "unknown";

fn scaled(bytes: f64) -> (&'static str, f64) {
    let (unit, scale) = UNITS
        .into_iter()
        .find(|(_, scale)| bytes >= *scale as f64)
        .unwrap_or(("B", 1));

    (unit, bytes / scale as f64)
}

pub(crate) struct Bytes(pub(crate) u64);

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (unit, value) = scaled(self.0 as f64);
        write!(f, "{value:.1} {unit}")
    }
}

pub(crate) struct Rate(pub(crate) f64);

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (unit, value) = scaled(self.0.max(0.0));
        write!(f, "{value:.1} {unit}/s")
    }
}

pub(crate) struct Percent(pub(crate) Option<f64>);

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(fraction) => write!(f, "{:.1}%", fraction * 100.0),
            None => f.write_str(UNKNOWN),
        }
    }
}

pub(crate) struct Elapsed(pub(crate) Duration);

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.0.as_secs();
        match seconds / 3600 {
            0 => write!(f, "{}m{:02}s", seconds / 60, seconds % 60),
            hours => write!(f, "{hours}h{:02}m{:02}s", seconds / 60 % 60, seconds % 60),
        }
    }
}

pub(crate) struct Eta {
    pub(crate) bytes_left: Option<u64>,
    pub(crate) bytes_per_second: f64,
}

impl fmt::Display for Eta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.bytes_left {
            Some(left) if self.bytes_per_second > 0.0 => {
                Elapsed(Duration::from_secs_f64(left as f64 / self.bytes_per_second)).fmt(f)
            }
            _ => f.write_str(UNKNOWN),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub(crate) struct Smoothed(Option<f64>);

const SMOOTHING: f64 = 0.2;

impl Smoothed {
    pub(crate) fn observe(&mut self, sample: f64) -> f64 {
        let value = match self.0 {
            Some(previous) => previous + SMOOTHING * (sample - previous),
            None => sample,
        };
        self.0 = Some(value);
        value
    }

    pub(crate) fn current(&self) -> f64 {
        self.0.unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{Bytes, Elapsed, Eta, Percent, Rate, Smoothed};
    use std::time::Duration;

    #[test]
    fn scales_bytes_to_the_largest_fitting_unit() {
        assert_eq!(Bytes(0).to_string(), "0.0 B");
        assert_eq!(Bytes(1536).to_string(), "1.5 KiB");
        assert_eq!(Bytes(3 << 30).to_string(), "3.0 GiB");
        assert_eq!(Rate(16.0 * (1 << 20) as f64).to_string(), "16.0 MiB/s");
    }

    #[test]
    fn reads_unknown_when_the_size_was_never_announced() {
        assert_eq!(Percent(None).to_string(), "unknown");
        assert_eq!(Percent(Some(0.421)).to_string(), "42.1%");
        assert_eq!(
            Eta {
                bytes_left: None,
                bytes_per_second: 1.0
            }
            .to_string(),
            "unknown"
        );
        assert_eq!(
            Eta {
                bytes_left: Some(90),
                bytes_per_second: 0.0
            }
            .to_string(),
            "unknown"
        );
        assert_eq!(
            Eta {
                bytes_left: Some(90),
                bytes_per_second: 1.0
            }
            .to_string(),
            "1m30s"
        );
    }

    #[test]
    fn spells_out_hours_only_once_there_are_any() {
        assert_eq!(Elapsed(Duration::from_secs(9)).to_string(), "0m09s");
        assert_eq!(Elapsed(Duration::from_secs(615)).to_string(), "10m15s");
        assert_eq!(Elapsed(Duration::from_secs(7565)).to_string(), "2h06m05s");
    }

    #[test]
    fn takes_the_first_sample_as_the_rate_and_then_follows_it() {
        let mut smoothed = Smoothed::default();
        assert_eq!(smoothed.observe(100.0), 100.0);

        let next = smoothed.observe(200.0);
        assert!(next > 100.0 && next < 200.0, "{next}");

        let mut settling = smoothed;
        for _ in 0..100 {
            settling.observe(200.0);
        }
        assert!((settling.observe(200.0) - 200.0).abs() < 0.01);
    }
}
