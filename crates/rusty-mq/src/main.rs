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
        /// Native HTTP management listener (§12.1; None disables).
        #[arg(long)]
        management_listen: Option<std::net::SocketAddr>,
        /// TLS AMQP listener (FR-S02). Requires --tls-cert/--tls-key.
        #[arg(long, requires_all = ["tls_cert", "tls_key"])]
        tls_listen: Option<std::net::SocketAddr>,
        #[arg(long, requires = "tls_listen")]
        tls_cert: Option<std::path::PathBuf>,
        #[arg(long, requires = "tls_listen")]
        tls_key: Option<std::path::PathBuf>,
        /// Development username for SASL PLAIN (M1 test auth only).
        #[arg(long, default_value = "guest")]
        user: String,
        /// Development password (M1 test auth only; never logged).
        #[arg(long, default_value = "guest")]
        password: String,
    },
    /// Offline backup of a stopped broker's data directory (§9.10).
    BackupCreate {
        #[arg(long)]
        data_dir: std::path::PathBuf,
        /// Output directory; must not exist.
        #[arg(long)]
        output: std::path::PathBuf,
    },
    /// Verify a backup is a complete, recoverable recovery root.
    BackupVerify {
        #[arg(long)]
        input: std::path::PathBuf,
    },
    /// Restore a backup into an empty data directory.
    BackupRestore {
        #[arg(long)]
        input: std::path::PathBuf,
        #[arg(long)]
        data_dir: std::path::PathBuf,
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
            management_listen,
            tls_listen,
            tls_cert,
            tls_key,
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
            let broker = std::sync::Arc::new(broker);
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            if let Some(addr) = management_listen {
                let broker = broker.clone();
                runtime.spawn(async move {
                    let app = rusty_mq_management::router(broker);
                    let listener = match tokio::net::TcpListener::bind(addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!("management bind {addr}: {e}");
                            std::process::exit(1);
                        }
                    };
                    tracing::info!(%addr, "management API listening");
                    if let Err(e) = axum::serve(listener, app).await {
                        tracing::error!("management server failed: {e}");
                    }
                });
            }
            if let Some(tls_addr) = tls_listen {
                let broker = broker.clone();
                let acceptor = match rusty_mq::tls::load(
                    tls_cert.as_ref().expect("clap requires"),
                    tls_key.as_ref().expect("clap requires"),
                ) {
                    Ok(setup) => setup.acceptor,
                    Err(e) => {
                        eprintln!("TLS material error: {e}");
                        std::process::exit(1);
                    }
                };
                runtime.spawn(async move {
                    let listener = match tokio::net::TcpListener::bind(tls_addr).await {
                        Ok(l) => l,
                        Err(e) => {
                            tracing::error!("tls bind {tls_addr}: {e}");
                            std::process::exit(1);
                        }
                    };
                    tracing::info!(%tls_addr, "rusty-mq TLS listener up");
                    rusty_mq::server::serve_tls_shared(listener, broker, acceptor).await;
                });
            }
            if let Err(e) = runtime.block_on(rusty_mq::server::serve_shared(listen, broker)) {
                tracing::error!("server failed: {e}");
                std::process::exit(1);
            }
        }
        Command::BackupCreate { data_dir, output } => {
            match rusty_mq_storage::backup::create(&data_dir, &output) {
                Ok(()) => println!(
                    "backup created: {} (verify with `rusty-mq backup verify --input {}`)",
                    output.display(),
                    output.display()
                ),
                Err(e) => {
                    eprintln!("backup create failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::BackupVerify { input } => match rusty_mq_storage::backup::verify(&input) {
            Ok(summary) => println!(
                "backup verified: {} records replayed, {} durable queues",
                summary.replayed, summary.queues
            ),
            Err(e) => {
                eprintln!("backup verify failed: {e}");
                std::process::exit(1);
            }
        },
        Command::BackupRestore { input, data_dir } => {
            match rusty_mq_storage::backup::restore(&input, &data_dir) {
                Ok(()) => println!(
                    "restored into {} (start the broker with --data-dir {})",
                    data_dir.display(),
                    data_dir.display()
                ),
                Err(e) => {
                    eprintln!("backup restore failed: {e}");
                    std::process::exit(1);
                }
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
