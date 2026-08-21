use clap::Parser;
use clap::Subcommand;
use tracing_subscriber::EnvFilter;

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

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main()]
async fn main() {
    init_tracing();
    let args = Args::parse();
    if let Err(e) = match args.mode {
        Mode::Send(args) => send::send(args).await,
        Mode::Receive(args) => receive::receive(args).await,
    } {
        println!("failed to run: {}", e)
    };
}
