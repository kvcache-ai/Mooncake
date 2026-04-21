mod bench;
mod cli;
mod datagen;
mod fault;
mod latency;
mod reporter;
mod setup;
mod soak;
mod verify;

use std::error::Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use clap::Parser;
use mooncake_store_client::{init_tracing, start_metrics_http_server, stop_metrics_http_server};
use tracing::{error, info, warn};

use cli::{Cli, Command};

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    init_tracing(cli.global.trace_filter.as_deref())?;

    let metrics_addr = if let Some(ref addr) = cli.global.metrics_addr {
        let bound = start_metrics_http_server(addr)?;
        info!("metrics server: http://{bound}/metrics");
        Some(bound)
    } else {
        None
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_ctrlc = Arc::clone(&shutdown);
    ctrlc::set_handler(move || {
        if shutdown_ctrlc.load(Ordering::Relaxed) {
            error!("forced exit");
            std::process::exit(1);
        }
        warn!("shutting down... (Ctrl-C again to force)");
        shutdown_ctrlc.store(true, Ordering::Relaxed);
    })?;

    let result = match cli.command {
        Command::Bench(args) => {
            cli::validate_bench_args(&args)?;
            bench::run_bench(cli.global, args, Arc::clone(&shutdown))
        }
        Command::Verify(args) => verify::run_verify(cli.global, args),
        Command::Soak(args) => {
            cli::validate_soak_args(&args)?;
            soak::run_soak(cli.global, args, Arc::clone(&shutdown))
        }
    };

    if metrics_addr.is_some() {
        let _ = stop_metrics_http_server();
    }

    result
}
