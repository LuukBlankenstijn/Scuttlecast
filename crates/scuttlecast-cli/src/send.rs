use std::{net::Ipv4Addr, path::PathBuf, time::Duration};

use clap::Parser;
use scuttlecast::error::ProtoError;
use scuttlecast::state::TransferState;
use tokio::sync::watch;
use tracing::{debug, info};

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// File to read from
    #[arg(short, long, value_parser = clap::value_parser!(PathBuf))]
    file: Option<PathBuf>,

    /// interface to use for multicast, defaults to the default interface
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    local_ip: Ipv4Addr,

    /// Port to use. sender uses port and receiver uses port+1
    #[arg(short, long, default_value_t = 5000)]
    port: u16,

    /// Multicast address
    #[arg(short, long, default_value_t = Ipv4Addr::from([239, 1, 1, 1]))]
    group_ip: Ipv4Addr,

    /// Minimum amount of receivers to wait for before starting sending
    #[arg(short, long)]
    min_receivers: Option<usize>,

    /// Time in seconds the sender waits for clients to join
    #[arg(short, long, default_value = "300", value_parser = parse_seconds)]
    wait: Duration,

    /// Blocks per second the sender will not exceed, even with no loss
    #[arg(long)]
    max_rate: Option<f64>,

    /// Parity shards per slice. Each one lets a receiver lose one more shard
    /// without asking for it, and costs its share of the bandwidth.
    #[arg(long)]
    parity: Option<u16>,
}

fn parse_seconds(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| e.to_string())
}

pub async fn send(args: Args) -> Result<(), ProtoError> {
    let sender = scuttlecast::sender::Sender::builder()
        .socket(args.local_ip, args.group_ip, args.port)?
        .maybe_min_receivers(args.min_receivers)
        .maybe_max_rate(args.max_rate)
        .maybe_parity_per_slice(args.parity)
        .max_wait(args.wait)
        .build();

    let reporting = tokio::spawn(report(sender.progress()));

    let outcome = match args.file {
        Some(filepath) => sender.send_file(filepath).await,
        None => sender.send_stream(tokio::io::stdin()).await,
    };

    reporting.abort();
    outcome
}

async fn report(mut progress: watch::Receiver<TransferState>) {
    while progress.changed().await.is_ok() {
        let state = progress.borrow_and_update().clone();

        info!(
            rate = format!("{:.1} MiB/s", state.bytes_per_second() / (1024.0 * 1024.0)),
            blocks = state.blocks_sent,
            slices = state.slices_emitted,
            draining = state.draining,
            limited_by = %state.limiting,
            "sending"
        );

        for receiver in &state.receivers {
            debug!(
                receiver_id = receiver.receiver_id,
                address = %receiver.address,
                behind = receiver.slices_behind,
                wire_loss = format!("{:.2}%", receiver.windowed_loss * 100.0),
                needing_repair = format!("{:.2}%", receiver.unrecovered_loss * 100.0),
                lifetime_loss = format!("{:.2}%", receiver.lifetime_loss * 100.0),
                naks = receiver.naks,
                sink_stall_ms = receiver.sink_stall_ms,
                "receiver"
            );
        }
    }
}
