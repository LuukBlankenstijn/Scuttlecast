use std::{net::Ipv4Addr, path::PathBuf};

use clap::Parser;
use scuttlecast::error::ProtoError;

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
}

pub async fn send(args: Args) -> Result<(), ProtoError> {
    let sender = scuttlecast::sender::Sender::builder()
        .socket(args.local_ip, args.group_ip, args.port)?
        .build();
    match args.file {
        Some(filepath) => sender.send_file(filepath).await,
        None => sender.send_stream(tokio::io::stdin()).await,
    }
}
