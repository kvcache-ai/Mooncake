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
use std::fs::{File, OpenOptions};
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use clap::Parser;
use mooncake_store_client::{start_metrics_http_server, stop_metrics_http_server};
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::{fmt, EnvFilter};

use cli::{Cli, Command};

static BENCH_TRACING_STATE: OnceLock<()> = OnceLock::new();
const TRACE_FILE_ENV: &str = "MC_BENCH_TRACE_FILE";

fn main() -> Result<(), Box<dyn Error>> {
    let raw_args = std::env::args_os().collect::<Vec<_>>();
    let combined_interfaces = std::env::var(cli::COMBINED_INTERFACE_ENV).ok();
    let mut cli = Cli::parse_from(raw_args.clone());
    cli::apply_combined_interface_env_from_raw_args(
        &raw_args,
        &mut cli,
        combined_interfaces.as_deref(),
    )?;

    init_bench_tracing(normalize_trace_filter(cli.global.trace_filter.as_deref()))?;

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

#[derive(Clone)]
struct TraceFileMakeWriter {
    file: Arc<Mutex<File>>,
}

struct TraceFileWriter {
    file: Arc<Mutex<File>>,
}

impl<'a> MakeWriter<'a> for TraceFileMakeWriter {
    type Writer = TraceFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TraceFileWriter {
            file: self.file.clone(),
        }
    }
}

impl Write for TraceFileWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.file
            .lock()
            .expect("trace file lock poisoned")
            .write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.lock().expect("trace file lock poisoned").flush()
    }
}

fn init_bench_tracing(filter: Option<&str>) -> Result<(), Box<dyn Error>> {
    if BENCH_TRACING_STATE.get().is_some() {
        return Ok(());
    }

    let env_filter = match filter {
        Some(filter) => EnvFilter::try_new(filter)?,
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    let trace_file = trace_file_from_env();
    let use_ansi = trace_file.is_none() && std::io::stderr().is_terminal();

    let init_result = match trace_file {
        Some(trace_file) => {
            let writer = TraceFileMakeWriter {
                file: Arc::new(Mutex::new(open_trace_file(&trace_file)?)),
            };
            fmt()
                .with_env_filter(env_filter)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)
                .with_span_events(FmtSpan::NONE)
                .with_writer(writer)
                .try_init()
        }
        None => fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_ansi(use_ansi)
            .with_span_events(FmtSpan::NONE)
            .with_writer(std::io::stderr)
            .try_init(),
    };

    match init_result {
        Ok(()) => {
            let _ = BENCH_TRACING_STATE.set(());
            Ok(())
        }
        Err(error) => {
            let _ = BENCH_TRACING_STATE.set(());
            if error
                .to_string()
                .contains("global default trace dispatcher has already been set")
            {
                return Ok(());
            }
            Err(format!("tracing init failed: {error}").into())
        }
    }
}

fn normalize_trace_filter(filter: Option<&str>) -> Option<&str> {
    filter.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn trace_file_from_env() -> Option<PathBuf> {
    std::env::var_os(TRACE_FILE_ENV).map(PathBuf::from)
}

fn open_trace_file(path: &Path) -> Result<File, Box<dyn Error>> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(OpenOptions::new().create(true).append(true).open(path)?)
}
