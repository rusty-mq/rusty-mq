//! rusty-mq broker executable (development alpha).
//!
//! M1 scope: AMQP 0-9-1 connection handshake (SASL PLAIN), channel
//! lifecycle, heartbeats, and the strict error profile. Queue/exchange
//! methods beyond channel lifecycle reply 540 NOT_IMPLEMENTED until their
//! milestone lands. The broker is memory-backed and makes no persistence
//! claim (see docs/implementation-status.md).

use clap::{Parser, Subcommand};
use std::net::SocketAddr;

#[derive(Debug, Parser)]
#[command(
    name = "rusty-mq",
    version,
    about = "rusty-mq AMQP 0-9-1 message broker (development alpha)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the broker.
    Serve {
        /// Listen address; loopback only until M7 (PRD early safety constraint).
        #[arg(long, default_value = "127.0.0.1:5672")]
        listen: SocketAddr,
        /// Data directory for the durable journal; absent = memory-backed
        /// development mode (no persistence claim).
        #[arg(long)]
        data_dir: Option<std::path::PathBuf>,
        /// Development username for SASL PLAIN (M1 test auth only).
        #[arg(long, default_value = "guest")]
        user: String,
        /// Development password (M1 test auth only; never logged).
        #[arg(long, default_value = "guest")]
        password: String,
    },
    /// Print version information.
    Version,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            listen,
            data_dir,
            user,
            password,
        } => {
            if user == "guest" && password == "guest" {
                tracing::warn!(
                    "development credentials guest/guest in use; real authentication lands in M7"
                );
            }
            let broker = match &data_dir {
                Some(dir) => rusty_mq::Broker::open_persistent(user, password, dir),
                None => rusty_mq::Broker::new(user, password),
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            if let Err(e) = runtime.block_on(rusty_mq::server::serve(listen, broker)) {
                tracing::error!("server failed: {e}");
                std::process::exit(1);
            }
        }
        Command::Version => {
            println!("rusty-mq {}", env!("CARGO_PKG_VERSION"));
            println!(
                "storage format major: {} (journal lands in M4)",
                rusty_mq_storage::FORMAT_MAJOR
            );
        }
    }
}
