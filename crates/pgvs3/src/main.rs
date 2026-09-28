use anyhow::Result;
use clap::{Parser, Subcommand};
use pgvs3::{db, server};

#[derive(Parser)]
#[command(
    name = "pgvs3",
    about = "Lowest-overhead S3-compatible service over PostgreSQL byte rows"
)]
struct Cli {
    /// PostgreSQL URL. Every setting also reads from the environment, so a
    /// container can be configured with `docker run -e PGVS3_URL=...` alone;
    /// a flag overrides the env var.
    #[arg(
        long,
        global = true,
        env = "PGVS3_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench"
    )]
    url: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the S3 gateway (SigV4 auth).
    Serve {
        /// Listen address. In a container use `PGVS3_ADDR=0.0.0.0:8014`.
        #[arg(long, env = "PGVS3_ADDR", default_value = "127.0.0.1:8014")]
        addr: String,
        #[arg(long, env = "PGVS3_ACCESS_KEY")]
        access_key: String,
        #[arg(long, env = "PGVS3_SECRET_KEY")]
        secret_key: String,
        /// PEM certificate and private key for HTTPS. Both must be configured.
        #[arg(long, env = "PGVS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "PGVS3_TLS_KEY")]
        tls_key: Option<String>,
        /// Allow plaintext HTTP on a non-loopback address (isolated benchmark rigs only).
        #[arg(long, env = "PGVS3_ALLOW_HTTP", default_value_t = false)]
        allow_http: bool,
    },
    /// Storage overhead summary (logical vs physical).
    Stat,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Serve {
            addr,
            access_key,
            secret_key,
            tls_cert,
            tls_key,
            allow_http,
        } => {
            let pool = db::connect(&cli.url).await?;
            db::init(&pool).await?;
            db::ensure_maintenance(&pool).await?;
            server::serve(
                pool,
                server::ServeConfig {
                    addr,
                    access_key,
                    secret_key,
                    tls_cert,
                    tls_key,
                    allow_http,
                },
            )
            .await
        }
        Cmd::Stat => {
            let pool = db::connect(&cli.url).await?;
            db::sizes(&pool).await?;
            Ok(())
        }
    }
}
