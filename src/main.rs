mod auth;
mod config;
mod db;
mod feed;
mod rollup;
mod server;

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use auth::AuthManager;
use db::DbManager;
use feed::FeedCoordinator;
use rollup::downsampler::start_downsampler;
use server::run_servers;

#[derive(Parser)]
#[command(
    name = "mso",
    version = "0.1.0",
    about = "Metasquare Oracle (mso) - High-performance multi-tier oracle microservice"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the oracle service (REST + gRPC servers, multi-tier feeds, continuous rollups)
    Serve {
        #[arg(long, env = "MSO_HTTP_ADDR", default_value = "0.0.0.0:4000")]
        http_addr: String,

        #[arg(long, env = "MSO_GRPC_ADDR", default_value = "0.0.0.0:50051")]
        grpc_addr: String,

        #[arg(long, env = "MSO_DB_PATH", default_value = "oracle.db")]
        db: String,
    },
    /// Manage API keys (create, revoke, list)
    Key {
        #[command(subcommand)]
        action: KeyCommands,
    },
    /// Check health of running MSO instance (used for container HEALTHCHECK)
    Health {
        #[arg(long, default_value = "http://127.0.0.1:4000/health")]
        url: String,
    },
}

#[derive(Subcommand)]
enum KeyCommands {
    /// Create a new scoped API key
    Create {
        #[arg(long, help = "Consumer application name (e.g. pnl-backend, staked-pvp)")]
        app: String,

        #[arg(long, help = "Time-to-live in days (omit for unlimited duration)")]
        ttl_days: Option<i64>,

        #[arg(long, default_value = "oracle.db")]
        db: String,
    },
    /// Revoke an existing API key by its Key ID
    Revoke {
        #[arg(long, help = "The Key ID to revoke (e.g. key_xxx)")]
        id: String,

        #[arg(long, default_value = "oracle.db")]
        db: String,
    },
    /// List all registered API keys
    List {
        #[arg(long, default_value = "oracle.db")]
        db: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Commands::Serve {
        http_addr: std::env::var("MSO_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:4000".to_string()),
        grpc_addr: std::env::var("MSO_GRPC_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string()),
        db: std::env::var("MSO_DB_PATH").unwrap_or_else(|_| "oracle.db".to_string()),
    });

    match command {
        Commands::Serve {
            http_addr,
            grpc_addr,
            db,
        } => {
            info!("=====================================================");
            info!("  Starting Metasquare Oracle (mso) v0.1.0            ");
            info!("  Host: Raspberry Pi 5 / NVMe SSD / Cloudflare Edge   ");
            info!("=====================================================");

            let db_mgr = DbManager::new(&db)?;
            let auth_mgr = AuthManager::new(db_mgr.clone())?;

            // Check if any keys exist. If not, auto-generate initial bootstrap key
            let existing_keys = auth_mgr.list_keys()?;
            if existing_keys.is_empty() {
                let (id, secret) = auth_mgr.create_key("pnl-backend-bootstrap", None)?;
                warn!("---------------------------------------------------------------");
                warn!("⚠️  No API keys found. Generated initial bootstrap key:");
                warn!("   App: pnl-backend-bootstrap");
                warn!("   ID:  {}", id);
                warn!("   KEY: {}", secret);
                warn!("   TTL: Unlimited");
                warn!("   Add this to your downstream apps (e.g. pnl-backend ORACLE_API_KEY)");
                warn!("---------------------------------------------------------------");
            } else {
                info!("Loaded {} registered API keys into in-memory auth cache.", existing_keys.len());
            }

            let feed_coord = FeedCoordinator::new(db_mgr.clone());

            // Start Ingestion Feeds
            feed_coord.start_all();

            // Start Continuous Downsampling Worker
            start_downsampler(db_mgr);

            let http_sock: SocketAddr = http_addr.parse()?;
            let grpc_sock: SocketAddr = grpc_addr.parse()?;

            run_servers(feed_coord, auth_mgr, http_sock, grpc_sock).await?;
        }

        Commands::Key { action } => match action {
            KeyCommands::Create { app, ttl_days, db } => {
                let db_mgr = DbManager::new(&db)?;
                let auth_mgr = AuthManager::new(db_mgr)?;

                let ttl_ms = ttl_days.map(|d| d * 24 * 60 * 60 * 1000);
                match auth_mgr.create_key(&app, ttl_ms) {
                    Ok((id, secret)) => {
                        println!("\n✅ API Key Created Successfully!");
                        println!("==================================================");
                        println!("App Name:   {}", app);
                        println!("Key ID:     {}", id);
                        println!("API Secret: {}", secret);
                        if let Some(days) = ttl_days {
                            println!("Expires In: {} days", days);
                        } else {
                            println!("Expires In: Never (Unlimited)");
                        }
                        println!("==================================================");
                        println!("Store this key securely! It will NOT be shown again.\n");
                    }
                    Err(e) => {
                        eprintln!("\n❌ Failed to create API key: {}\n", e);
                        std::process::exit(1);
                    }
                }
            }
            KeyCommands::Revoke { id, db } => {
                let db_mgr = DbManager::new(&db)?;
                let auth_mgr = AuthManager::new(db_mgr)?;

                match auth_mgr.revoke_key(&id) {
                    Ok(true) => {
                        println!("\n✅ API Key '{}' was successfully revoked.\n", id);
                    }
                    Ok(false) => {
                        println!("\n⚠️  Key ID '{}' not found or was already revoked.\n", id);
                    }
                    Err(e) => {
                        eprintln!("\n❌ Error revoking key: {}\n", e);
                        std::process::exit(1);
                    }
                }
            }
            KeyCommands::List { db } => {
                let db_mgr = DbManager::new(&db)?;
                let auth_mgr = AuthManager::new(db_mgr)?;

                match auth_mgr.list_keys() {
                    Ok(keys) => {
                        println!("\nRegistered API Keys (Quota: {}/100 active)", keys.iter().filter(|k| !k.is_revoked).count());
                        println!("---------------------------------------------------------------------------------------------------------");
                        println!("{:<36} {:<24} {:<10} {:<20}", "Key ID", "App Name", "Status", "Expires At");
                        println!("---------------------------------------------------------------------------------------------------------");
                        for k in keys {
                            let status = if k.is_revoked { "REVOKED" } else { "ACTIVE" };
                            let exp = match k.expires_at {
                                Some(ts) => {
                                    let dt = chrono_placeholder(ts);
                                    dt
                                }
                                None => "Never".to_string(),
                            };
                            println!("{:<36} {:<24} {:<10} {:<20}", k.id, k.app_name, status, exp);
                        }
                        println!("---------------------------------------------------------------------------------------------------------\n");
                    }
                    Err(e) => {
                        eprintln!("\n❌ Error listing keys: {}\n", e);
                        std::process::exit(1);
                    }
                }
            }
        },

        Commands::Health { url } => {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()?;
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    println!("mso healthy (status: {})", resp.status());
                    std::process::exit(0);
                }
                Ok(resp) => {
                    eprintln!("mso unhealthy (status: {})", resp.status());
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("mso unreachable: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

fn chrono_placeholder(ts_millis: i64) -> String {
    let secs = ts_millis / 1000;
    format!("{} (epoch sec)", secs)
}
