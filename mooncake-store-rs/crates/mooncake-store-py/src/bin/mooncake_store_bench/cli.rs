use std::error::Error;

use clap::{Args, Parser, Subcommand, ValueEnum};

pub const DEFAULT_STORAGE_BYTES: usize = 0;
pub const COMBINED_INTERFACE_ENV: &str = "MC_BENCH_INTERFACES";

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
    #[arg(long, env = "MC_STORE_RS_METADATA_URL")]
    pub metadata_url: String,
    #[arg(long, env = "MC_STORE_RS_TRANSPORT_METADATA_URL")]
    pub transport_metadata_url: Option<String>,
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
    #[arg(long, env = "MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT")]
    pub eviction_high_watermark_percent: Option<u8>,
    #[arg(long, env = "MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT")]
    pub eviction_low_watermark_percent: Option<u8>,
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
    #[arg(long, value_enum, default_value_t = WriteInterface::BatchPutFrom, env = "MC_BENCH_WRITE_INTERFACE")]
    pub write_interface: WriteInterface,
    #[arg(long, value_enum, default_value_t = ReadInterface::BatchGetInto, env = "MC_BENCH_READ_INTERFACE")]
    pub read_interface: ReadInterface,
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
    #[arg(long, default_value_t = false, env = "MC_BENCH_NO_PREFILL")]
    pub no_prefill: bool,
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
    #[arg(long, default_value_t = false, env = "MC_BENCH_CLEANUP")]
    pub cleanup: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum BenchMode {
    Put,
    Get,
    Mixed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum WriteInterface {
    Put,
    #[value(alias = "batch_put")]
    BatchPut,
    #[value(alias = "batch_put_from")]
    BatchPutFrom,
}

impl WriteInterface {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::BatchPut => "batch_put",
            Self::BatchPutFrom => "batch_put_from",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ReadInterface {
    Get,
    #[value(alias = "batch_get")]
    BatchGet,
    #[value(alias = "batch_get_into")]
    BatchGetInto,
}

impl ReadInterface {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::BatchGet => "batch_get",
            Self::BatchGetInto => "batch_get_into",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InterfaceSelection {
    pub write: WriteInterface,
    pub read: ReadInterface,
}

#[derive(Clone, ValueEnum)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
}

#[derive(Args)]
pub struct VerifyArgs {
    #[arg(long, value_enum, default_value_t = WriteInterface::BatchPutFrom, env = "MC_BENCH_WRITE_INTERFACE")]
    pub write_interface: WriteInterface,
    #[arg(long, value_enum, default_value_t = ReadInterface::BatchGetInto, env = "MC_BENCH_READ_INTERFACE")]
    pub read_interface: ReadInterface,
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
    #[arg(long, value_enum, default_value_t = WriteInterface::BatchPutFrom, env = "MC_BENCH_WRITE_INTERFACE")]
    pub write_interface: WriteInterface,
    #[arg(long, value_enum, default_value_t = ReadInterface::BatchGetInto, env = "MC_BENCH_READ_INTERFACE")]
    pub read_interface: ReadInterface,
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

fn parse_write_interface(value: &str) -> Result<WriteInterface, String> {
    WriteInterface::from_str(value.trim(), true).map_err(|_| {
        format!("invalid write interface '{value}': expected put, batch-put, or batch-put-from")
    })
}

fn parse_read_interface(value: &str) -> Result<ReadInterface, String> {
    ReadInterface::from_str(value.trim(), true).map_err(|_| {
        format!("invalid read interface '{value}': expected get, batch-get, or batch-get-into")
    })
}

pub fn parse_combined_interface_env(value: &str) -> Result<InterfaceSelection, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("combined interface env must not be empty".to_string());
    }

    let parts = if trimmed.contains(',') {
        trimmed
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    } else {
        trimmed
            .split(':')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    };

    if parts.iter().any(|part| part.contains('=')) {
        let mut write = None;
        let mut read = None;
        for part in &parts {
            let Some((key, interface)) = part.split_once('=') else {
                return Err(
                    "combined interface env named format must use write=<...>,read=<...>"
                        .to_string(),
                );
            };
            match key.trim() {
                "write" => write = Some(parse_write_interface(interface)?),
                "read" => read = Some(parse_read_interface(interface)?),
                other => {
                    return Err(format!(
                        "unknown combined interface env key '{other}': expected write or read"
                    ));
                }
            }
        }
        return Ok(InterfaceSelection {
            write: write.ok_or_else(|| {
                "combined interface env named format requires a write=<...> entry".to_string()
            })?,
            read: read.ok_or_else(|| {
                "combined interface env named format requires a read=<...> entry".to_string()
            })?,
        });
    }

    if parts.len() != 2 {
        return Err(
            "combined interface env must be '<write>,<read>', '<write>:<read>', or 'write=<...>,read=<...>'"
                .to_string(),
        );
    }

    Ok(InterfaceSelection {
        write: parse_write_interface(parts[0])?,
        read: parse_read_interface(parts[1])?,
    })
}

fn has_explicit_cli_flag(raw_args: &[std::ffi::OsString], flag: &str) -> bool {
    let prefix = format!("{flag}=");
    raw_args.iter().any(|arg| {
        let arg = arg.to_string_lossy();
        arg == flag || arg.starts_with(&prefix)
    })
}

pub fn apply_combined_interface_env_from_raw_args(
    raw_args: &[std::ffi::OsString],
    cli: &mut Cli,
    combined_env: Option<&str>,
) -> Result<(), Box<dyn Error>> {
    let Some(combined_env) = combined_env
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    let selection = parse_combined_interface_env(combined_env).map_err(std::io::Error::other)?;
    let write_explicit = has_explicit_cli_flag(raw_args, "--write-interface");
    let read_explicit = has_explicit_cli_flag(raw_args, "--read-interface");

    match &mut cli.command {
        Command::Bench(args) => {
            if !write_explicit {
                args.write_interface = selection.write;
            }
            if !read_explicit {
                args.read_interface = selection.read;
            }
        }
        Command::Soak(args) => {
            if !write_explicit {
                args.write_interface = selection.write;
            }
            if !read_explicit {
                args.read_interface = selection.read;
            }
        }
        Command::Verify(args) => {
            if !write_explicit {
                args.write_interface = selection.write;
            }
            if !read_explicit {
                args.read_interface = selection.read;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};

    use clap::CommandFactory;

    use super::*;

    #[test]
    fn bench_cli_exposes_interface_selection_defaults() {
        let mut command = Cli::command();
        let bench = command
            .find_subcommand_mut("bench")
            .expect("bench subcommand should exist");

        let write = bench
            .get_arguments()
            .find(|arg| arg.get_long() == Some("write-interface"))
            .expect("bench should expose --write-interface");
        assert_eq!(
            write.get_env(),
            Some(OsStr::new("MC_BENCH_WRITE_INTERFACE"))
        );
        assert_eq!(write.get_default_values(), [OsStr::new("batch-put-from")]);

        let read = bench
            .get_arguments()
            .find(|arg| arg.get_long() == Some("read-interface"))
            .expect("bench should expose --read-interface");
        assert_eq!(read.get_env(), Some(OsStr::new("MC_BENCH_READ_INTERFACE")));
        assert_eq!(read.get_default_values(), [OsStr::new("batch-get-into")]);
    }

    #[test]
    fn global_cli_reads_metadata_url_from_mc_store_rs_metadata_url() {
        let command = Cli::command();
        let metadata = command
            .get_arguments()
            .find(|arg| arg.get_long() == Some("metadata-url"))
            .expect("global args should expose --metadata-url");

        assert_eq!(
            metadata.get_env(),
            Some(OsStr::new("MC_STORE_RS_METADATA_URL"))
        );
    }

    #[test]
    fn global_cli_reads_transport_metadata_url_from_mc_store_rs_transport_metadata_url() {
        let command = Cli::command();
        let transport_metadata = command
            .get_arguments()
            .find(|arg| arg.get_long() == Some("transport-metadata-url"))
            .expect("global args should expose --transport-metadata-url");

        assert_eq!(
            transport_metadata.get_env(),
            Some(OsStr::new("MC_STORE_RS_TRANSPORT_METADATA_URL"))
        );
    }

    #[test]
    fn parse_bench_supports_explicit_interface_selection() {
        let parsed = Cli::try_parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "bench",
            "--write-interface",
            "batch-put-from",
            "--read-interface",
            "batch-get-into",
        ]);

        assert!(parsed.is_ok(), "bench should accept explicit interfaces");
        let cli = parsed.expect("cli should parse explicit interface selection");
        if let Command::Bench(args) = cli.command {
            assert_eq!(args.write_interface, WriteInterface::BatchPutFrom);
            assert_eq!(args.read_interface, ReadInterface::BatchGetInto);
        } else {
            panic!("expected bench command");
        }
    }

    #[test]
    fn parse_bench_defaults() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "bench",
        ]);
        assert_eq!(cli.global.metadata_url, "redis://127.0.0.1:6379/0");
        assert_eq!(cli.global.transport_metadata_url, None);
        assert_eq!(cli.global.trace_filter.as_deref(), None);
        assert!(matches!(
            cli.global.transport_backend,
            TransportBackend::ClassicTe
        ));
        assert!(matches!(cli.command, Command::Bench(_)));
        if let Command::Bench(args) = cli.command {
            assert!(matches!(args.mode, BenchMode::Mixed));
            assert_eq!(args.write_interface, WriteInterface::BatchPutFrom);
            assert_eq!(args.read_interface, ReadInterface::BatchGetInto);
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
    fn parse_global_supports_explicit_transport_metadata_url() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "--transport-metadata-url",
            "redis://127.0.0.1:6380/1",
            "bench",
        ]);

        assert_eq!(
            cli.global.transport_metadata_url.as_deref(),
            Some("redis://127.0.0.1:6380/1")
        );
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
            assert_eq!(args.write_interface, WriteInterface::BatchPutFrom);
            assert_eq!(args.read_interface, ReadInterface::BatchGetInto);
            assert!(args.verify_overwrite);
            assert_eq!(args.key_count, 64);
        } else {
            panic!("expected verify command");
        }
    }

    #[test]
    fn verify_cli_exposes_interface_selection_defaults() {
        let mut command = Cli::command();
        let verify = command
            .find_subcommand_mut("verify")
            .expect("verify subcommand should exist");

        let write = verify
            .get_arguments()
            .find(|arg| arg.get_long() == Some("write-interface"))
            .expect("verify should expose --write-interface");
        assert_eq!(
            write.get_env(),
            Some(OsStr::new("MC_BENCH_WRITE_INTERFACE"))
        );
        assert_eq!(write.get_default_values(), [OsStr::new("batch-put-from")]);

        let read = verify
            .get_arguments()
            .find(|arg| arg.get_long() == Some("read-interface"))
            .expect("verify should expose --read-interface");
        assert_eq!(read.get_env(), Some(OsStr::new("MC_BENCH_READ_INTERFACE")));
        assert_eq!(read.get_default_values(), [OsStr::new("batch-get-into")]);
    }

    #[test]
    fn parse_verify_supports_explicit_interface_selection() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://localhost/0",
            "verify",
            "--write-interface",
            "put",
            "--read-interface",
            "get",
        ]);
        if let Command::Verify(args) = cli.command {
            assert_eq!(args.write_interface, WriteInterface::Put);
            assert_eq!(args.read_interface, ReadInterface::Get);
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
            assert_eq!(args.write_interface, WriteInterface::BatchPutFrom);
            assert_eq!(args.read_interface, ReadInterface::BatchGetInto);
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
    fn soak_cli_exposes_interface_selection_defaults() {
        let mut command = Cli::command();
        let soak = command
            .find_subcommand_mut("soak")
            .expect("soak subcommand should exist");

        let write = soak
            .get_arguments()
            .find(|arg| arg.get_long() == Some("write-interface"))
            .expect("soak should expose --write-interface");
        assert_eq!(
            write.get_env(),
            Some(OsStr::new("MC_BENCH_WRITE_INTERFACE"))
        );
        assert_eq!(write.get_default_values(), [OsStr::new("batch-put-from")]);

        let read = soak
            .get_arguments()
            .find(|arg| arg.get_long() == Some("read-interface"))
            .expect("soak should expose --read-interface");
        assert_eq!(read.get_env(), Some(OsStr::new("MC_BENCH_READ_INTERFACE")));
        assert_eq!(read.get_default_values(), [OsStr::new("batch-get-into")]);
    }

    #[test]
    fn parse_soak_supports_explicit_interface_selection() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://localhost/0",
            "soak",
            "--write-interface",
            "put",
            "--read-interface",
            "get",
        ]);

        if let Command::Soak(args) = cli.command {
            assert_eq!(args.write_interface, WriteInterface::Put);
            assert_eq!(args.read_interface, ReadInterface::Get);
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
    fn parse_combined_interface_env_supports_positional_format() {
        let selection = parse_combined_interface_env("batch-put-from,batch-get-into")
            .expect("positional format should parse");
        assert_eq!(selection.write, WriteInterface::BatchPutFrom);
        assert_eq!(selection.read, ReadInterface::BatchGetInto);
    }

    #[test]
    fn parse_combined_interface_env_supports_existing_aliases() {
        let selection = parse_combined_interface_env("put,batch-get")
            .expect("legacy positional format should parse");
        assert_eq!(selection.write, WriteInterface::Put);
        assert_eq!(selection.read, ReadInterface::BatchGet);
    }

    #[test]
    fn parse_combined_interface_env_supports_named_format() {
        let selection = parse_combined_interface_env("write=batch_put,read=get")
            .expect("named format should parse");
        assert_eq!(selection.write, WriteInterface::BatchPut);
        assert_eq!(selection.read, ReadInterface::Get);
    }

    #[test]
    fn parse_combined_interface_env_rejects_incomplete_values() {
        assert!(parse_combined_interface_env("write=put").is_err());
        assert!(parse_combined_interface_env("put").is_err());
    }

    #[test]
    fn combined_interface_env_overrides_defaults_for_bench() {
        let raw_args = vec![
            OsString::from("mooncake-store-bench"),
            OsString::from("--metadata-url"),
            OsString::from("redis://127.0.0.1:6379/0"),
            OsString::from("bench"),
        ];
        let mut cli = Cli::parse_from(raw_args.clone());

        apply_combined_interface_env_from_raw_args(&raw_args, &mut cli, Some("put,get"))
            .expect("combined env should apply");

        if let Command::Bench(args) = cli.command {
            assert_eq!(args.write_interface, WriteInterface::Put);
            assert_eq!(args.read_interface, ReadInterface::Get);
        } else {
            panic!("expected bench command");
        }
    }

    #[test]
    fn combined_interface_env_respects_explicit_cli_overrides() {
        let raw_args = vec![
            OsString::from("mooncake-store-bench"),
            OsString::from("--metadata-url"),
            OsString::from("redis://127.0.0.1:6379/0"),
            OsString::from("soak"),
            OsString::from("--write-interface"),
            OsString::from("put"),
        ];
        let mut cli = Cli::parse_from(raw_args.clone());

        apply_combined_interface_env_from_raw_args(
            &raw_args,
            &mut cli,
            Some("batch-put-from,batch-get-into"),
        )
        .expect("combined env should apply");

        if let Command::Soak(args) = cli.command {
            assert_eq!(args.write_interface, WriteInterface::Put);
            assert_eq!(args.read_interface, ReadInterface::BatchGetInto);
        } else {
            panic!("expected soak command");
        }
    }

    #[test]
    fn combined_interface_env_overrides_defaults_for_verify() {
        let raw_args = vec![
            OsString::from("mooncake-store-bench"),
            OsString::from("--metadata-url"),
            OsString::from("redis://127.0.0.1:6379/0"),
            OsString::from("verify"),
        ];
        let mut cli = Cli::parse_from(raw_args.clone());

        apply_combined_interface_env_from_raw_args(&raw_args, &mut cli, Some("write=put,read=get"))
            .expect("combined env should apply to verify");

        if let Command::Verify(args) = cli.command {
            assert_eq!(args.write_interface, WriteInterface::Put);
            assert_eq!(args.read_interface, ReadInterface::Get);
        } else {
            panic!("expected verify command");
        }
    }

    #[test]
    fn validate_bench_zero_concurrency() {
        let args = BenchArgs {
            mode: BenchMode::Put,
            write_interface: WriteInterface::BatchPutFrom,
            read_interface: ReadInterface::BatchGetInto,
            concurrency: 0,
            value_size: 4096,
            batch_size: 8,
            iterations: 1024,
            duration: None,
            warmup: 0,
            no_prefill: false,
            read_ratio: 70,
            report_interval: 5,
            key_space_size: 10000,
            writers: 1,
            readers: 1,
            output_format: OutputFormat::Text,
            cleanup: false,
        };
        assert!(validate_bench_args(&args).is_err());
    }

    #[test]
    fn bench_cleanup_flag_defaults_to_false() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "bench",
        ]);
        if let Command::Bench(args) = cli.command {
            assert!(!args.cleanup, "cleanup should default to false");
        } else {
            panic!("expected bench command");
        }
    }

    #[test]
    fn bench_cleanup_flag_can_be_enabled() {
        let cli = Cli::parse_from([
            "mooncake-store-bench",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "bench",
            "--cleanup",
        ]);
        if let Command::Bench(args) = cli.command {
            assert!(args.cleanup, "cleanup should be true when flag is set");
        } else {
            panic!("expected bench command");
        }
    }
}
