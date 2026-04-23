use std::error::Error;

use clap::{Args, Parser, Subcommand, ValueEnum};

pub const DEFAULT_STORAGE_BYTES: usize = 0;

#[derive(Parser)]
#[command(name = "mooncake-store-bench")]
#[command(about = "Benchmark and verification tool for mooncake store")]
#[command(arg_required_else_help = true)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Args, Clone)]
pub struct GlobalArgs {
    #[arg(long, env = "MC_STORE_RS_TRANSPORT_METADATA_URL")]
    pub metadata_url: String,
    #[arg(long, default_value = "", env = "MC_STORE_RS_KEYSPACE")]
    pub keyspace: String,
    #[arg(long, value_enum, default_value_t = Protocol::Tcp, env = "MOONCAKE_PROTOCOL")]
    pub protocol: Protocol,
    #[arg(long, value_enum, default_value_t = TransportBackend::ClassicTe, env = "MC_STORE_RS_TRANSPORT_BACKEND")]
    pub transport_backend: TransportBackend,
    #[arg(long, default_value = "127.0.0.1", env = "MOONCAKE_LOCAL_HOSTNAME")]
    pub local_hostname: String,
    #[arg(long, default_value_t = DEFAULT_STORAGE_BYTES, env = "MC_BENCH_STORAGE_BYTES")]
    pub storage_bytes: usize,
    #[arg(long, default_value_t = 16 * 1024 * 1024, env = "MC_STORE_RS_SCRATCH_BYTES")]
    pub scratch_bytes: usize,
    #[arg(long, default_value = "bench", env = "MC_STORE_RS_TENANT")]
    pub tenant: String,
    #[arg(long, env = "MC_STORE_RS_TRACE_FILTER")]
    pub trace_filter: Option<String>,
    #[arg(long, env = "MC_STORE_RS_METRICS_ADDR")]
    pub metrics_addr: Option<String>,
    #[arg(long, default_value_t = 42, env = "MC_BENCH_SEED")]
    pub seed: u64,
    #[arg(long, value_enum, default_value_t = RouteControl::EmbeddedWrh, env = "MC_STORE_RS_ROUTE_CONTROL")]
    pub route_control: RouteControl,
    #[arg(long, default_value_t = 2, env = "MC_STORE_RS_ROUTE_TOPK")]
    pub route_topk: usize,
    #[arg(long, default_value_t = 1, env = "MC_STORE_RS_REPLICA_COUNT")]
    pub replica_count: usize,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum Protocol {
    Tcp,
    Rdma,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum TransportBackend {
    #[value(alias = "classic_te", alias = "classic", alias = "te")]
    ClassicTe,
    Tent,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum RouteControl {
    EmbeddedWrh,
    MetadataOnly,
}

#[derive(Subcommand)]
pub enum Command {
    Bench(BenchArgs),
    Verify(VerifyArgs),
    Soak(SoakArgs),
}

#[derive(Args)]
pub struct BenchArgs {
    #[arg(long, value_enum, default_value_t = BenchMode::Mixed, env = "MC_BENCH_MODE")]
    pub mode: BenchMode,
    #[arg(long, default_value_t = 4, env = "MC_BENCH_CONCURRENCY")]
    pub concurrency: usize,
    #[arg(long, default_value_t = 4096, env = "MC_BENCH_VALUE_SIZE")]
    pub value_size: usize,
    #[arg(long, default_value_t = 8, env = "MC_BENCH_BATCH_SIZE")]
    pub batch_size: usize,
    #[arg(long, default_value_t = 1024, env = "MC_BENCH_ITERATIONS")]
    pub iterations: usize,
    #[arg(long, env = "MC_BENCH_DURATION")]
    pub duration: Option<u64>,
    #[arg(long, default_value_t = 32, env = "MC_BENCH_WARMUP")]
    pub warmup: usize,
    #[arg(long, default_value_t = 70, env = "MC_BENCH_READ_RATIO")]
    pub read_ratio: u8,
    #[arg(long, default_value_t = 5, env = "MC_BENCH_REPORT_INTERVAL")]
    pub report_interval: u64,
    #[arg(long, default_value_t = 10000, env = "MC_BENCH_KEY_SPACE_SIZE")]
    pub key_space_size: usize,
    #[arg(long, default_value_t = 1, env = "MC_BENCH_WRITERS")]
    pub writers: usize,
    #[arg(long, default_value_t = 1, env = "MC_BENCH_READERS")]
    pub readers: usize,
    #[arg(long, value_enum, default_value_t = OutputFormat::Text, env = "MC_BENCH_OUTPUT_FORMAT")]
    pub output_format: OutputFormat,
}

#[derive(Clone, Debug, ValueEnum)]
pub enum BenchMode {
    Put,
    Get,
    Mixed,
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
}

#[derive(Args)]
pub struct VerifyArgs {
    #[arg(long, default_value_t = 4096, env = "MC_BENCH_VALUE_SIZE")]
    pub value_size: usize,
    #[arg(long, default_value_t = 256, env = "MC_BENCH_KEY_COUNT")]
    pub key_count: usize,
    #[arg(long, default_value_t = 8, env = "MC_BENCH_BATCH_SIZE")]
    pub batch_size: usize,
    #[arg(long, default_value_t = false, env = "MC_BENCH_VERIFY_OVERWRITE")]
    pub verify_overwrite: bool,
    #[arg(long, default_value_t = false, env = "MC_BENCH_VERIFY_DELETE")]
    pub verify_delete: bool,
    #[arg(long, default_value_t = false, env = "MC_BENCH_VERIFY_MULTI_TENANT")]
    pub verify_multi_tenant: bool,
}

#[derive(Args)]
pub struct SoakArgs {
    #[arg(long, default_value_t = 3600, env = "MC_BENCH_SOAK_DURATION")]
    pub duration: u64,
    #[arg(long, default_value_t = 2, env = "MC_BENCH_CONCURRENCY")]
    pub concurrency: usize,
    #[arg(long, default_value_t = 4096, env = "MC_BENCH_VALUE_SIZE")]
    pub value_size: usize,
    #[arg(long, default_value_t = 8, env = "MC_BENCH_BATCH_SIZE")]
    pub batch_size: usize,
    #[arg(long, default_value_t = 30, env = "MC_BENCH_REPORT_INTERVAL")]
    pub report_interval: u64,
    #[arg(long, value_parser = parse_fault_spec)]
    pub fault: Vec<FaultSpec>,
    #[arg(long, default_value_t = true, env = "MC_BENCH_VERIFY_READS")]
    pub verify_reads: bool,
    #[arg(long, default_value_t = 30000, env = "MC_BENCH_HEARTBEAT_INTERVAL_MS")]
    pub heartbeat_interval_ms: u64,
    #[arg(long, default_value_t = 70, env = "MC_BENCH_READ_RATIO")]
    pub read_ratio: u8,
    #[arg(long, default_value_t = 10000, env = "MC_BENCH_KEY_SPACE_SIZE")]
    pub key_space_size: usize,
}

#[derive(Clone, Debug)]
pub enum FaultSpec {
    RedisJitter { min_ms: u64, max_ms: u64 },
    MetadataDrop { percent: u8 },
    TransportDelay { min_ms: u64, max_ms: u64 },
    TransportError { percent: u8 },
}

fn parse_fault_spec(s: &str) -> Result<FaultSpec, Box<dyn Error + Send + Sync>> {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.first().copied() {
        Some("redis-jitter") => {
            if parts.len() != 3 {
                return Err("expected redis-jitter:<min_ms>:<max_ms>".into());
            }
            Ok(FaultSpec::RedisJitter {
                min_ms: parts[1].parse()?,
                max_ms: parts[2].parse()?,
            })
        }
        Some("metadata-drop") => {
            if parts.len() != 2 {
                return Err("expected metadata-drop:<percent>".into());
            }
            Ok(FaultSpec::MetadataDrop {
                percent: parts[1].parse()?,
            })
        }
        Some("transport-delay") => {
            if parts.len() != 3 {
                return Err("expected transport-delay:<min_ms>:<max_ms>".into());
            }
            Ok(FaultSpec::TransportDelay {
                min_ms: parts[1].parse()?,
                max_ms: parts[2].parse()?,
            })
        }
        Some("transport-error") => {
            if parts.len() != 2 {
                return Err("expected transport-error:<percent>".into());
            }
            Ok(FaultSpec::TransportError {
                percent: parts[1].parse()?,
            })
        }
        _ => Err(format!(
            "unknown fault type '{}': expected redis-jitter, metadata-drop, transport-delay, or transport-error",
            parts.first().unwrap_or(&"")
        )
        .into()),
    }
}

pub fn validate_bench_args(args: &BenchArgs) -> Result<(), Box<dyn Error>> {
    if args.concurrency == 0 {
        return Err("--concurrency must be > 0".into());
    }
    if args.value_size == 0 {
        return Err("--value-size must be > 0".into());
    }
    if args.batch_size == 0 {
        return Err("--batch-size must be > 0".into());
    }
    if args.read_ratio > 100 {
        return Err("--read-ratio must be 0..100".into());
    }
    if args.writers == 0 {
        return Err("--writers must be > 0".into());
    }
    if args.readers == 0 {
        return Err("--readers must be > 0".into());
    }
    Ok(())
}

pub fn validate_soak_args(args: &SoakArgs) -> Result<(), Box<dyn Error>> {
    if args.concurrency == 0 {
        return Err("--concurrency must be > 0".into());
    }
    if args.duration == 0 {
        return Err("--duration must be > 0".into());
    }
    if args.read_ratio > 100 {
        return Err("--read-ratio must be 0..100".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_bench_defaults() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "bench",
        ]);
        assert_eq!(cli.global.metadata_url, "redis://127.0.0.1:6379/0");
        assert_eq!(cli.global.trace_filter.as_deref(), None);
        assert!(matches!(
            cli.global.transport_backend,
            TransportBackend::ClassicTe
        ));
        assert!(matches!(cli.command, Command::Bench(_)));
        if let Command::Bench(args) = cli.command {
            assert!(matches!(args.mode, BenchMode::Mixed));
            assert_eq!(args.concurrency, 4);
            assert_eq!(args.value_size, 4096);
        }
    }

    #[test]
    fn parse_global_defaults_include_local_storage() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "bench",
        ]);

        assert_eq!(cli.global.storage_bytes, 0);
    }

    #[test]
    fn parse_transport_backend_aliases() {
        let classic = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "--transport-backend",
            "classic_te",
            "bench",
        ]);
        assert!(matches!(
            classic.global.transport_backend,
            TransportBackend::ClassicTe
        ));

        let tent = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "--transport-backend",
            "tent",
            "bench",
        ]);
        assert!(matches!(
            tent.global.transport_backend,
            TransportBackend::Tent
        ));
    }

    #[test]
    fn parse_verify_subcommand() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://localhost/0",
            "verify",
            "--verify-overwrite",
            "--key-count",
            "64",
        ]);
        if let Command::Verify(args) = cli.command {
            assert!(args.verify_overwrite);
            assert_eq!(args.key_count, 64);
        } else {
            panic!("expected verify command");
        }
    }

    #[test]
    fn parse_soak_with_faults() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://localhost/0",
            "soak",
            "--fault",
            "redis-jitter:10:50",
            "--fault",
            "metadata-drop:5",
        ]);
        if let Command::Soak(args) = cli.command {
            assert_eq!(args.fault.len(), 2);
            assert!(matches!(
                args.fault[0],
                FaultSpec::RedisJitter {
                    min_ms: 10,
                    max_ms: 50
                }
            ));
            assert!(matches!(
                args.fault[1],
                FaultSpec::MetadataDrop { percent: 5 }
            ));
        } else {
            panic!("expected soak command");
        }
    }

    #[test]
    fn parse_fault_spec_errors() {
        assert!(parse_fault_spec("unknown:1:2").is_err());
        assert!(parse_fault_spec("redis-jitter:1").is_err());
        assert!(parse_fault_spec("metadata-drop:abc").is_err());
    }

    #[test]
    fn validate_bench_zero_concurrency() {
        let args = BenchArgs {
            mode: BenchMode::Put,
            concurrency: 0,
            value_size: 4096,
            batch_size: 8,
            iterations: 1024,
            duration: None,
            warmup: 0,
            read_ratio: 70,
            report_interval: 5,
            key_space_size: 10000,
            writers: 1,
            readers: 1,
            output_format: OutputFormat::Text,
        };
        assert!(validate_bench_args(&args).is_err());
    }
}
