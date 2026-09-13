use std::{net::Ipv4Addr, path::PathBuf, time::Duration};

use clap::Parser;
use scuttlecast::error::ProtoError;
use scuttlecast::state::TransferState;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::report::{REPORT_INTERVAL, parse_bytes};

/// Send a file or stream to a multicast group
#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// File to read from
    #[arg(short, long, value_parser = clap::value_parser!(PathBuf))]
    file: Option<PathBuf>,

    /// Size of the piped stream, such as 4G or 512MiB, reported to receivers
    /// as progress only
    #[arg(short, long, conflicts_with = "file", value_parser = parse_bytes)]
    size: Option<u64>,

    /// Interface to use for multicast, defaults to the default interface
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    local_ip: Ipv4Addr,

    /// Port to use. The sender uses this port, receivers use port + 1
    #[arg(short, long, default_value_t = 5000)]
    port: u16,

    /// Multicast address
    #[arg(short, long, default_value_t = Ipv4Addr::from([239, 1, 1, 1]))]
    group_ip: Ipv4Addr,

    /// Receivers to wait for before sending, defaults to one
    #[arg(short, long)]
    min_receivers: Option<usize>,

    /// Seconds to wait for receivers to join, and afterwards for stragglers
    /// to finish
    #[arg(short, long, default_value = "300", value_parser = parse_seconds)]
    wait: Duration,

    /// Mebibytes per second the sender will not exceed, even with no loss
    #[arg(long)]
    max_rate: Option<f64>,
}

const MIB: f64 = 1024.0 * 1024.0;

fn parse_seconds(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| e.to_string())
}

fn blocks_per_second(mib_per_second: f64) -> f64 {
    mib_per_second * MIB / scuttlecast::DEFAULT_BLOCK_SIZE as f64
}

pub async fn send(args: Args) -> Result<(), ProtoError> {
    let sender = scuttlecast::sender::Sender::builder()
        .socket(args.local_ip, args.group_ip, args.port)?
        .maybe_min_receivers(args.min_receivers)
        .maybe_max_rate(args.max_rate.map(blocks_per_second))
        .max_wait(args.wait)
        .build();

    let progress = sender.progress();
    let reporting = tokio::spawn(report(sender.progress()));

    let outcome = match args.file {
        Some(filepath) => sender.send_file(filepath).await,
        None => sender.send_stream(tokio::io::stdin(), args.size).await,
    };

    reporting.abort();
    outcome?;

    let state = progress.borrow();
    info!(
        sent = %state.sent(),
        took = %state.running_for(),
        receivers = state.receivers.len(),
        "transfer complete"
    );

    Ok(())
}

async fn report(progress: watch::Receiver<TransferState>) {
    let mut ticker = tokio::time::interval(REPORT_INTERVAL);

    loop {
        ticker.tick().await;
        let state = progress.borrow().clone();
        if state.transfer_id == 0 {
            continue;
        }
        info!(
            progress = %state.progress(),
            sent = %state.sent(),
            rate = %state.rate(),
            eta = %state.eta(),
            running_for = %state.running_for(),
            draining = state.draining,
            limited_by = %state.limiting,
            "sending"
        );

        debug!(
            blocks = state.blocks_sent,
            slices = state.slices_emitted,
            parity = state.parity_shards,
            "shards"
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
