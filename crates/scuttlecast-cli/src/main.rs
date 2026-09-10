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
#[command(
    name = "scuttle",
    version,
    about = "Reliable multicast file and stream transfer"
)]
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
async fn main() -> std::process::ExitCode {
    init_tracing();
    let args = Args::parse();

    let outcome = match args.mode {
        Mode::Send(args) => send::send(args).await,
        Mode::Receive(args) => receive::receive(args).await,
    };

    match outcome {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}
