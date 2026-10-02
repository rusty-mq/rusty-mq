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
    /// Offline RabbitMQ definitions preflight (§19.1, T29).
    MigrationInspect {
        #[arg(long)]
        definitions: std::path::PathBuf,
        /// Report destination; omit for stdout.
        #[arg(long)]
        output: Option<std::path::PathBuf>,
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
    /// Admin subcommands against the native HTTP API (§13.1).
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
        /// Base URL of the management API.
        #[arg(long, default_value = "http://127.0.0.1:15672")]
        url: String,
        #[arg(long, default_value = "admin")]
        user: String,
        /// From --password or RUSTY_MQ_ADMIN_PASSWORD (never logged).
        #[arg(long)]
        password: Option<String>,
        /// Raw JSON output.
        #[arg(long)]
        json: bool,
    },
    /// Print version information.
    Version,
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    Status,
    Users {
        #[command(subcommand)]
        action: Option<UsersAction>,
    },
    Permissions {
        #[command(subcommand)]
        action: Option<PermissionsAction>,
    },
    Queues {
        #[arg(long, default_value = "/")]
        vhost: String,
    },
    Connections,
    Definitions {
        #[command(subcommand)]
        action: DefinitionsAction,
    },
}

#[derive(Debug, Subcommand)]
enum DefinitionsAction {
    /// Write the export to a file (or stdout with --json).
    Export {
        #[arg(long)]
        out: Option<std::path::PathBuf>,
    },
    /// Import from a file; --dry-run reports without mutating.
    Import {
        #[arg(long)]
        file: std::path::PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
enum UsersAction {
    Create {
        username: String,
        password: String,
        #[arg(default_value = "ordinary")]
        role: String,
    },
    Delete {
        username: String,
    },
    SetPassword {
        username: String,
        password: String,
    },
}

#[derive(Debug, Subcommand)]
enum PermissionsAction {
    Set {
        username: String,
        vhost: String,
        configure: String,
        write: String,
        read: String,
    },
    Delete {
        username: String,
        vhost: String,
    },
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
        Command::MigrationInspect {
            definitions,
            output,
        } => {
            let raw = match std::fs::read_to_string(&definitions) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("cannot read {}: {e}", definitions.display());
                    std::process::exit(1);
                }
            };
            let defs: rusty_mq::migration::Definitions = match serde_json::from_str(&raw) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("definitions not valid JSON: {e}");
                    std::process::exit(1);
                }
            };
            let report = rusty_mq::migration::inspect(&defs);
            let json = serde_json::to_string_pretty(&report).expect("report serializes");
            match output {
                Some(path) => {
                    if let Err(e) = std::fs::write(&path, &json) {
                        eprintln!("cannot write {}: {e}", path.display());
                        std::process::exit(1);
                    }
                    println!(
                        "report written to {} (ready={}, blocking={}, warning={}, unknown={})",
                        path.display(),
                        report.ready,
                        report.summary.blocking,
                        report.summary.warning,
                        report.summary.unknown
                    );
                }
                None => println!("{json}"),
            }
            if !report.ready {
                std::process::exit(2);
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
        Command::Admin {
            command,
            url,
            user,
            password,
            json,
        } => {
            let password = password
                .or_else(rusty_mq::admin_client::env_password)
                .unwrap_or_default();
            let creds = rusty_mq::admin_client::AdminCredentials { user, password };
            let base = url.trim_end_matches('/').to_string();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            runtime.block_on(async move {
                use rusty_mq::admin_client::request;
                let outcome: Result<rusty_mq::admin_client::ApiResponse, String> = match command {
                    AdminCommand::Status => request(&base, "GET", "/v1/status", &creds, None).await,
                    AdminCommand::Connections => {
                        request(&base, "GET", "/v1/connections", &creds, None).await
                    }
                    AdminCommand::Definitions {
                        action: DefinitionsAction::Export { out },
                    } => match request(&base, "GET", "/v1/definitions", &creds, None).await {
                        Ok(resp) => {
                            match out {
                                Some(path) => {
                                    if let Err(e) = std::fs::write(&path, resp.body.trim()) {
                                        eprintln!("cannot write {}: {e}", path.display());
                                        std::process::exit(1);
                                    }
                                    println!("definitions written to {}", path.display());
                                }
                                None => println!("{}", resp.body.trim()),
                            }
                            // Early return: printing already handled.
                            return;
                        }
                        Err(e) => {
                            eprintln!("admin command failed: {e}");
                            std::process::exit(1);
                        }
                    },
                    AdminCommand::Definitions {
                        action: DefinitionsAction::Import { file, dry_run },
                    } => {
                        let raw = match std::fs::read_to_string(&file) {
                            Ok(r) => r,
                            Err(e) => {
                                eprintln!("cannot read {}: {e}", file.display());
                                std::process::exit(1);
                            }
                        };
                        let payload: serde_json::Value = match serde_json::from_str(&raw) {
                            Ok(v) => v,
                            Err(e) => {
                                eprintln!("definitions not valid JSON: {e}");
                                std::process::exit(1);
                            }
                        };
                        let path = if dry_run {
                            "/v1/definitions?dry_run=true"
                        } else {
                            "/v1/definitions"
                        };
                        request(&base, "POST", path, &creds, Some(&payload)).await
                    }
                    AdminCommand::Queues { vhost } => {
                        let path = format!("/v1/vhosts/{}/queues", urlencode(&vhost));
                        request(&base, "GET", &path, &creds, None).await
                    }
                    AdminCommand::Users { action: None } => {
                        request(&base, "GET", "/v1/users", &creds, None).await
                    }
                    AdminCommand::Users {
                        action:
                            Some(UsersAction::Create {
                                username,
                                password,
                                role,
                            }),
                    } => {
                        request(
                            &base,
                            "POST",
                            "/v1/users",
                            &creds,
                            Some(&serde_json::json!({
                                "username": username, "password": password, "role": role
                            })),
                        )
                        .await
                    }
                    AdminCommand::Users {
                        action: Some(UsersAction::Delete { username }),
                    } => {
                        let path = format!("/v1/users/{}", urlencode(&username));
                        request(&base, "DELETE", &path, &creds, None).await
                    }
                    AdminCommand::Users {
                        action: Some(UsersAction::SetPassword { username, password }),
                    } => {
                        let path = format!("/v1/users/{}/credentials", urlencode(&username));
                        request(
                            &base,
                            "PUT",
                            &path,
                            &creds,
                            Some(&serde_json::json!({ "password": password })),
                        )
                        .await
                    }
                    AdminCommand::Permissions { action: None } => {
                        request(&base, "GET", "/v1/permissions", &creds, None).await
                    }
                    AdminCommand::Permissions {
                        action:
                            Some(PermissionsAction::Set {
                                username,
                                vhost,
                                configure,
                                write,
                                read,
                            }),
                    } => {
                        let path = format!(
                            "/v1/permissions/{}/{}",
                            urlencode(&username),
                            urlencode(&vhost)
                        );
                        request(
                            &base,
                            "PUT",
                            &path,
                            &creds,
                            Some(&serde_json::json!({
                                "configure": configure, "write": write, "read": read
                            })),
                        )
                        .await
                    }
                    AdminCommand::Permissions {
                        action: Some(PermissionsAction::Delete { username, vhost }),
                    } => {
                        let path = format!(
                            "/v1/permissions/{}/{}",
                            urlencode(&username),
                            urlencode(&vhost)
                        );
                        request(&base, "DELETE", &path, &creds, None).await
                    }
                };
                match outcome {
                    Ok(resp) => {
                        if json {
                            println!("{}", resp.body.trim());
                        } else {
                            // Human rendering: pretty-print JSON for now;
                            // tabular views arrive with the report polish.
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&resp.body) {
                                println!(
                                    "{}",
                                    serde_json::to_string_pretty(&v).unwrap_or_default()
                                );
                            } else if !resp.body.trim().is_empty() {
                                println!("{}", resp.body.trim());
                            } else {
                                println!("OK ({})", resp.status);
                            }
                        }
                        if resp.status >= 400 {
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("admin command failed: {e}");
                        std::process::exit(1);
                    }
                }
            });
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

/// Percent-encode a path segment (vhost names contain '/').
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
