use clap::Parser;
use scuttlecast::error::ProtoError;
use std::{net::Ipv4Addr, path::PathBuf, time::Duration};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// File to write to, if omitted written to stdout
    #[arg(short, long, value_parser = clap::value_parser!(PathBuf))]
    file: Option<PathBuf>,

    /// interface to use for multicast, defaults to the default interface
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    local_ip: Ipv4Addr,

    /// Ports to use. sender uses port and receiver uses port+1
    #[arg(short, long, default_value_t = 5000)]
    port: u16,

    /// Multicast ip
    #[arg(short, long, default_value_t = Ipv4Addr::from([239, 1, 1, 1]))]
    group_ip: Ipv4Addr,

    /// Time in seconds the receiver waits for an initial hello message
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
    match args.file {
        Some(path) => {
            receiver.recv_file(path).await?;
            println!("successfully received file");
        }
        None => {
            let mut rx = receiver.recv_stream().await?;
            let mut stdout = tokio::io::stdout();
            while let Some(block) = rx.recv().await {
                stdout.write_all(&block).await?;
            }
            stdout.flush().await?;
            println!();
            println!("successfully received stream");
        }
    }

    Ok(())
}
