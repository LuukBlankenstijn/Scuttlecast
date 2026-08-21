use std::{net::Ipv4Addr, path::PathBuf};

use clap::Parser;
use scuttlecast::error::ProtoError;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// File to read from
    #[arg(short, long, value_parser = clap::value_parser!(PathBuf))]
    file: Option<PathBuf>,

    /// interface to use for multicast
    #[arg(short, long, default_value_t = Ipv4Addr::UNSPECIFIED)]
    interface: Ipv4Addr,

    /// Ports to use. sender uses portbase and receiver uses portbase+1
    #[arg(short, long, default_value_t = 5000)]
    portbase: u16,

    /// Multicast address
    #[arg(short, long)]
    address: Ipv4Addr,
}

pub async fn send(args: Args) -> Result<(), ProtoError> {
    let sender =
        scuttlecast::sender::Sender::new(args.interface, args.address, args.portbase).unwrap();
    match args.file {
        Some(filepath) => sender.send_file(filepath).await,
        None => sender.send(tokio::io::stdin()).await,
    }
}
