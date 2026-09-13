use std::time::Duration;

pub const REPORT_INTERVAL: Duration = Duration::from_secs(1);

pub fn parse_bytes(text: &str) -> Result<u64, String> {
    let text = text.trim();
    let digits = text
        .trim_end_matches(|c: char| c.is_ascii_alphabetic())
        .trim_end();
    let suffix = text[digits.len()..].trim().to_ascii_lowercase();

    let scale: u64 = match suffix.as_str() {
        "" | "b" => 1,
        "k" | "kib" | "kb" => 1 << 10,
        "m" | "mib" | "mb" => 1 << 20,
        "g" | "gib" | "gb" => 1 << 30,
        "t" | "tib" | "tb" => 1u64 << 40,
        other => return Err(format!("unknown size suffix {other:?}")),
    };

    let value: f64 = digits
        .parse()
        .map_err(|_| format!("{digits:?} is not a number"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("{digits:?} is not a size"));
    }

    Ok((value * scale as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::parse_bytes;

    #[test]
    fn parses_plain_and_suffixed_sizes() {
        assert_eq!(parse_bytes("512"), Ok(512));
        assert_eq!(parse_bytes("4G"), Ok(4 << 30));
        assert_eq!(parse_bytes("512MiB"), Ok(512 << 20));
        assert_eq!(parse_bytes(" 1.5 gb "), Ok(1024 * 1024 * 1024 * 3 / 2));
    }

    #[test]
    fn refuses_nonsense_sizes() {
        assert!(parse_bytes("12x").is_err());
        assert!(parse_bytes("-1").is_err());
        assert!(parse_bytes("").is_err());
    }
}
