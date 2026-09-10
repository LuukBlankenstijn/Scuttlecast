use clap::Parser;
use scuttlecast::error::ProtoError;
use std::{net::Ipv4Addr, path::PathBuf, time::Duration};

/// Receive a transfer from a multicast group
#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// File to write to, if omitted written to stdout
    #[arg(short, long, value_parser = clap::value_parser!(PathBuf))]
    file: Option<PathBuf>,

    /// Interface to use for multicast, defaults to the default interface
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    local_ip: Ipv4Addr,

    /// Port to use. The sender uses this port, receivers use port + 1
    #[arg(short, long, default_value_t = 5000)]
    port: u16,

    /// Multicast address
    #[arg(short, long, default_value_t = Ipv4Addr::from([239, 1, 1, 1]))]
    group_ip: Ipv4Addr,

    /// Seconds to wait for a sender to announce a transfer
    #[arg(short, long, default_value = "300", value_parser = parse_seconds)]
    wait: Duration,
}

fn parse_seconds(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| e.to_string())
}

pub async fn receive(args: Args) -> Result<(), ProtoError> {
    let receiver = scuttlecast::receiver::Receiver::builder()
        .socket(args.local_ip, args.group_ip, args.port)?
        .max_wait(args.wait)
        .build();

    let summary = match args.file {
        Some(path) => receiver.recv_file(path).await?,
        None => receiver.recv_to(tokio::io::stdout()).await?,
    };

    tracing::info!(
        bytes = summary.total_bytes,
        blocks = summary.total_blocks,
        duplicates = summary.duplicates,
        late = summary.late,
        naks = summary.naks_sent,
        loss = format!("{:.2}%", summary.loss() * 100.0),
        "transfer complete"
    );

    Ok(())
}
