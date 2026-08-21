use clap::Parser;
use clap::Subcommand;

mod receive;
mod send;

#[derive(Debug, Clone, Subcommand)]
enum Mode {
    Send(send::Args),
    Receive(receive::Args),
}

#[derive(Debug, Clone, Parser)]
struct Args {
    #[command(subcommand)]
    mode: Mode,
}

#[tokio::main()]
async fn main() {
    let args = Args::parse();
    if let Err(e) = match args.mode {
        Mode::Send(args) => send::send(args).await,
        Mode::Receive(args) => receive::receive(args).await,
    } {
        println!("failed to run: {}", e)
    };
}
