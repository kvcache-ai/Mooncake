use std::error::Error;
use std::sync::mpsc;

use _store_rs::admin::{AdminHttpServerHandle, AdminService};
use clap::Parser;
use mooncake_store_client::init_tracing;
use tracing::info;
use url::Url;

#[derive(Parser, Debug)]
#[command(name = "mooncake-store-admin-server")]
#[command(about = "Run Mooncake store admin as a long-lived HTTP service")]
struct Args {
    #[arg(long)]
    metadata_url: String,
    #[arg(long)]
    keyspace: Option<String>,
    #[arg(long, default_value = "127.0.0.1:0")]
    bind_addr: String,
    #[arg(long)]
    trace_filter: Option<String>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    init_tracing(args.trace_filter.as_deref())?;
    let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
    let mut server = AdminHttpServerHandle::start(&args.bind_addr, service)?;
    info!(address = %server.address(), "admin http server listening");

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = shutdown_tx.send(());
    })?;
    let _ = shutdown_rx.recv();

    server.shutdown()?;
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn Error>> {
    Url::parse(&args.metadata_url)?;
    Ok(())
}
