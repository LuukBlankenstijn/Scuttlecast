use clap::Parser;
use scuttlecast::error::ProtoError;
use std::{net::Ipv4Addr, path::PathBuf};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Parser)]
pub struct Args {
    /// File to write to, if omitted written to stdout
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

pub async fn receive(args: Args) -> Result<(), ProtoError> {
    let receiver =
        scuttlecast::receiver::Receiver::new(args.interface, args.address, args.portbase).unwrap();
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
