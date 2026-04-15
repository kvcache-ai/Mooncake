use std::error::Error;

use _store_rs::admin::{AdminHttpServerHandle, AdminService};
use clap::Parser;
use mooncake_store_client::init_tracing;

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
    init_tracing(args.trace_filter.as_deref())?;
    let service = AdminService::from_config(&args.metadata_url, args.keyspace.clone())?;
    let mut server = AdminHttpServerHandle::start(&args.bind_addr, service)?;
    println!("admin http server listening on {}", server.address());
    std::thread::park();
    server.shutdown()?;
    Ok(())
}
