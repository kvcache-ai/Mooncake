use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use _store_rs::build_info;
use _store_rs::dispatcher::{CompatNamespaceScope, StoreDispatcher};
use _store_rs::dummy_service::start_dummy_store_server;
use _store_rs::runtime::{
    CompatRuntimeArgs, CompatSetupArgs, CompatTimeoutCliOverrides, CompatTimeoutConfig,
};
use _store_rs::DEFAULT_COMPAT_WORKER_SCOPE;
use clap::{builder::FalseyValueParser, Args as ClapArgs, Parser, Subcommand, ValueEnum};
use mooncake_store_client::{
    init_tracing, stable_phase_spread_ms, start_metrics_http_server, stop_metrics_http_server,
    ColdTierKind, ColdTierSsdEngine, ColdTierTargetSpec, RouteControlMode,
};
use mooncake_store_core::{
    parse_hugepage_size, ClientEpoch, ClientLifecycleState, ClientRuntimeId, HandoffKind,
    METRICS_PORT_LABEL,
};
use tracing::{debug, info, trace, warn};

fn dummy_worker_scope(keyspace: Option<&str>) -> String {
    keyspace
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_COMPAT_WORKER_SCOPE)
        .to_string()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum RouteControlArg {
    EmbeddedWrh,
    MetadataOnly,
}

impl From<RouteControlArg> for RouteControlMode {
    fn from(value: RouteControlArg) -> Self {
        match value {
            RouteControlArg::EmbeddedWrh => RouteControlMode::EmbeddedWrh,
            RouteControlArg::MetadataOnly => RouteControlMode::MetadataOnly,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum InitialStateArg {
    Standby,
    Active,
    Draining,
    Sealed,
    Offline,
}

impl From<InitialStateArg> for ClientLifecycleState {
    fn from(value: InitialStateArg) -> Self {
        match value {
            InitialStateArg::Standby => ClientLifecycleState::Standby,
            InitialStateArg::Active => ClientLifecycleState::Active,
            InitialStateArg::Draining => ClientLifecycleState::Draining,
            InitialStateArg::Sealed => ClientLifecycleState::Sealed,
            InitialStateArg::Offline => ClientLifecycleState::Offline,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TransportBackendArg {
    Tent,
    ClassicTe,
}

impl TransportBackendArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tent => "tent",
            Self::ClassicTe => "classic_te",
        }
    }
}

#[derive(ClapArgs, Debug)]
struct RunArgs {
    #[arg(long, env = "MOONCAKE_LOCAL_HOSTNAME")]
    local_hostname: String,
    /// Store-RS metadata URL (`redis://...` or `etcd://...`).
    #[arg(long, alias = "metadata_url", env = "MC_STORE_RS_METADATA_URL")]
    metadata_url: String,
    /// Transfer Engine metadata input (`redis://...` or `P2PHANDSHAKE`).
    /// Defaults to `P2PHANDSHAKE` (classic_te peer handshake) when unset.
    #[arg(
        long = "transport-metadata-url",
        alias = "transport_metadata_url",
        env = "MC_STORE_RS_TRANSPORT_METADATA_URL"
    )]
    transport_metadata_url: Option<String>,
    #[arg(long, default_value_t = 64 * 1024 * 1024, env = "MC_STORE_RS_STORAGE_BYTES")]
    storage_bytes: usize,
    #[arg(long, default_value_t = 4 * 1024 * 1024, env = "MC_STORE_RS_SCRATCH_BYTES")]
    scratch_bytes: usize,
    #[arg(long, env = "MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT")]
    eviction_high_watermark_percent: Option<u8>,
    #[arg(long, env = "MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT")]
    eviction_low_watermark_percent: Option<u8>,
    #[arg(long, default_value = "tcp", env = "MOONCAKE_PROTOCOL")]
    protocol: String,
    #[arg(long, default_value = "", env = "MC_STORE_RS_RDMA_DEVICES")]
    rdma_devices: String,
    #[arg(
        long,
        alias = "rpc-server-port",
        env = "MC_STORE_RS_TRANSPORT_RPC_PORT"
    )]
    transport_rpc_port: Option<u16>,
    #[arg(
        long,
        value_parser = parse_transport_backend_arg,
        env = "MC_STORE_RS_TRANSPORT_BACKEND",
        help = "Real transport backend; defaults to classic_te"
    )]
    transport_backend: Option<TransportBackendArg>,
    #[arg(long, env = "MC_STORE_RS_STABLE_ID")]
    stable_id: Option<String>,
    #[arg(long, value_enum, default_value_t = InitialStateArg::Active, env = "MC_STORE_RS_INITIAL_STATE")]
    initial_state: InitialStateArg,
    #[arg(
        long,
        default_value = "default",
        env = "MC_STORE_RS_TENANT",
        help = "Default tenant scope for startup policy lookup and request defaults"
    )]
    tenant: String,
    #[arg(
        long,
        env = "MC_STORE_RS_DOMAIN",
        help = "Default domain scope for compatibility read/write requests"
    )]
    domain: Option<String>,
    #[arg(
        long = "object-set",
        env = "MC_STORE_RS_OBJECT_SET",
        help = "Default object-set scope for compatibility read/write requests"
    )]
    object_set: Option<String>,
    #[arg(long = "label", value_parser = parse_label, value_delimiter = ',', env = "MC_STORE_RS_LABELS", help = "Runtime identity and placement labels; use admin-managed tenant policy for tenant-scoped routing/resource policy")]
    labels: Vec<(String, String)>,
    #[arg(long, default_value_t = false, value_parser = FalseyValueParser::new(), env = "MC_STORE_RS_ROUTED_WRITES")]
    routed_writes: bool,
    #[arg(long, default_value_t = 1, env = "MC_STORE_RS_REPLICA_COUNT")]
    replica_count: usize,
    #[arg(
        long,
        default_value_t = 2,
        env = "MC_STORE_RS_ROUTE_TOPK",
        help = "Compatibility fallback WRH route-authority fanout; prefer admin-managed tenant policy in metadata"
    )]
    route_topk: usize,
    #[arg(long, env = "MC_STORE_RS_KEYSPACE")]
    keyspace: Option<String>,
    #[arg(long, env = "MC_STORE_RS_LOCAL_SEGMENT_NAME")]
    local_segment_name: Option<String>,
    #[arg(long, default_value_t = 30_000, env = "MC_STORE_RS_LEASE_TTL_MS")]
    lease_ttl_ms: u64,
    #[arg(
        long,
        default_value_t = 30_000,
        env = "MC_STORE_RS_HEARTBEAT_INTERVAL_MS"
    )]
    heartbeat_interval_ms: u64,
    #[arg(long, env = "MC_STORE_RS_REQUEST_TIMEOUT_MS")]
    request_timeout_ms: Option<u64>,
    #[arg(long, env = "MC_STORE_RS_STARTUP_TIMEOUT_MS")]
    startup_timeout_ms: Option<u64>,
    #[arg(long, env = "MC_STORE_RS_HEARTBEAT_TIMEOUT_MS")]
    heartbeat_timeout_ms: Option<u64>,
    #[arg(long, env = "MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS")]
    transfer_stall_timeout_ms: Option<u64>,
    #[arg(long, env = "MC_STORE_RS_METRICS_ADDR")]
    metrics_addr: Option<String>,
    #[arg(long, env = "MC_STORE_RS_CLIENT_SERVER_ADDRESS")]
    client_server_address: Option<String>,
    #[arg(long, default_value_t = false, value_parser = FalseyValueParser::new(), env = "MC_STORE_USE_HUGEPAGE")]
    use_hugepage: bool,
    #[arg(long, value_parser = parse_hugepage_size_arg, env = "MC_STORE_HUGEPAGE_SIZE")]
    hugepage_size: Option<usize>,
    #[arg(long, env = "MC_STORE_RS_TRACE_FILTER")]
    trace_filter: Option<String>,
    #[arg(long, value_parser = parse_route_control_arg, default_value = "embedded-wrh", env = "MC_STORE_RS_ROUTE_CONTROL", help = "Cluster-level route storage mode: embedded-wrh (WRH authority mesh) or metadata-only (Redis-only)")]
    route_control: RouteControlArg,
    #[arg(long, default_value_t = false, value_parser = FalseyValueParser::new(), env = "MC_STORE_RS_DRAIN_ON_EXIT")]
    drain_on_exit: bool,

    // -- Cold tier bootstrap --
    #[arg(
        long,
        env = "MC_STORE_RS_COLD_TIER_TARGETS",
        help = "Cold tier targets as JSON array of ColdTierTargetSpec objects"
    )]
    cold_tier_targets_json: Option<String>,
    #[arg(long, help = "Bootstrap cold tier device ID")]
    cold_tier_id: Option<String>,
    #[arg(
        long,
        value_enum,
        help = "Bootstrap cold tier device kind (ssd or nfs); used with --cold-tier-id"
    )]
    cold_tier_kind: Option<ColdTierKindArg>,
    #[arg(long, help = "Bootstrap cold tier directory path")]
    cold_tier_directory: Option<String>,
    #[arg(long, help = "Bootstrap cold tier device UUID")]
    cold_tier_uuid: Option<String>,
    #[arg(
        long,
        value_enum,
        help = "SSD engine (local-dir or extent-store); default: local-dir"
    )]
    cold_tier_ssd_engine: Option<ColdTierSsdEngineArg>,
    #[arg(
        long,
        use_value_delimiter = true,
        help = "Bootstrap cold tier device tags (comma-separated)"
    )]
    cold_tier_tags: Vec<String>,
    #[arg(long, help = "Bootstrap cold tier device capacity override in bytes")]
    cold_tier_capacity_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ColdTierKindArg {
    Ssd,
    Nfs,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ColdTierSsdEngineArg {
    LocalDir,
    ExtentStore,
}

#[derive(ClapArgs, Debug)]
struct StatsArgs {
    #[arg(long, env = "MC_STORE_RS_STATS_SERVER")]
    server: String,
    #[arg(long, value_parser = FalseyValueParser::new(), env = "MC_STORE_RS_STATS_JSON", help = "Emit compact JSON instead of pretty JSON")]
    json: bool,
    #[arg(long, value_parser = FalseyValueParser::new(), help = "Fetch Store-RS performance breakdown instead of raw stats")]
    breakdown: bool,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(about = "Start a standalone Mooncake store-rs client runtime")]
    Run(Box<RunArgs>),
    #[command(about = "Fetch stats from a running Mooncake store-rs client")]
    Stats(StatsArgs),
}

#[derive(Parser, Debug)]
#[command(name = "mooncake-store-client")]
#[command(about = "Standalone Mooncake store-rs client commands")]
#[command(version = build_info::build::PKG_VERSION, long_version = build_info::long_version_static())]
#[command(arg_required_else_help = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone)]
struct ShutdownSignal {
    state: Arc<AtomicU8>,
}

impl ShutdownSignal {
    fn requested(&self) -> bool {
        self.state.load(Ordering::SeqCst) >= 1
    }

    fn forced(&self) -> bool {
        self.state.load(Ordering::SeqCst) >= 2
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    match parse_cli() {
        Ok(cli) => match cli.command {
            Command::Run(args) => run_client(*args),
            Command::Stats(args) => run_stats_command(args),
        },
        Err(error) => error.exit(),
    }
}

fn run_client(args: RunArgs) -> Result<(), Box<dyn Error>> {
    validate_args(&args)?;
    init_tracing(normalize_trace_filter(args.trace_filter.as_deref()))?;
    build_info::log_build_info();
    info!(config = ?args, "mooncake-store-client configuration");
    if args.keyspace.is_some() {
        warn!(
            keyspace = ?args.keyspace,
            "custom keyspace is set — make sure you understand its effect on tenant isolation; \
             misconfigured keyspace can cause nodes to register in different metadata prefixes"
        );
    }
    emit_compat_warnings(&args);

    let timeouts = resolve_timeout_config(&args)?;
    let metrics_addr = start_metrics_if_needed(args.metrics_addr.as_deref())?;
    let shutdown = install_signal_handler()?;
    let heartbeat_interval =
        effective_heartbeat_interval(args.heartbeat_interval_ms, args.lease_ttl_ms);
    let requested_initial_state = requested_initial_state(&args);
    let runtime = build_runtime_args(&args, timeouts, metrics_addr.as_deref()).build()?;

    let stable_id = runtime.stable_id.clone();
    let epoch = runtime.epoch;
    let startup_state = runtime.initial_state;
    let segment_name = runtime.segment_name.clone();
    let default_scope = CompatNamespaceScope::new(
        args.tenant.clone(),
        args.domain.clone(),
        args.object_set.clone(),
    );
    let client = Arc::new(
        StoreDispatcher::spawn_with_timeout_config_and_namespace_scope(
            runtime.client,
            format!("mooncake-store-dispatcher-{stable_id}"),
            timeouts,
            args.keyspace
                .clone()
                .unwrap_or_else(|| "default".to_string()),
            default_scope,
        )?,
    );
    client.register_local_memory()?;
    let dummy_server = match args.client_server_address.as_deref() {
        Some(address) => {
            let worker_scope = dummy_worker_scope(args.keyspace.as_deref());
            Some(start_dummy_store_server(
                client.clone(),
                address,
                &worker_scope,
            )?)
        }
        None => None,
    };
    if should_activate_after_ready(requested_initial_state, startup_state) {
        client.activate()?;
    }

    eprintln!(
        "{}",
        started_message(
            &stable_id,
            epoch,
            requested_initial_state,
            &segment_name,
            args.lease_ttl_ms,
            heartbeat_interval,
            timeouts,
            client.startup_timeout(),
            metrics_addr.as_deref(),
            dummy_server.as_ref().map(|server| server.address()),
        )
    );
    debug!(
        stable_id,
        runtime = %format_args!("{stable_id}:{}", epoch.0),
        epoch = epoch.0,
        state = %lifecycle_state_label(requested_initial_state),
        segment = %segment_name,
        storage_bytes = args.storage_bytes,
        scratch_bytes = args.scratch_bytes,
        eviction_high_watermark_percent = ?args.eviction_high_watermark_percent,
        eviction_low_watermark_percent = ?args.eviction_low_watermark_percent,
        lease_ttl_ms = args.lease_ttl_ms,
        heartbeat_interval_ms = heartbeat_interval,
        request_timeout_ms = timeouts.request_timeout.as_millis(),
        startup_timeout_ms = client.startup_timeout().as_millis(),
        heartbeat_timeout_ms = timeouts.heartbeat_timeout.as_millis(),
        transfer_stall_timeout_ms = timeouts.transfer_stall_timeout.as_millis(),
        metrics_addr = metrics_addr.as_deref().unwrap_or("disabled"),
        client_server_address = dummy_server
            .as_ref()
            .map(|server| server.address())
            .unwrap_or("disabled"),
        route_control = ?args.route_control,
        replica_count = args.replica_count,
        "mooncake-store-client state snapshot"
    );
    info!(stable_id = %stable_id, "mooncake-store-client ready");

    let mut heartbeat_state = HeartbeatLoopState::new(now_ms());
    let mut next_heartbeat = now_ms().saturating_add(initial_heartbeat_delay_ms(
        &stable_id,
        epoch,
        heartbeat_interval,
    ));
    while !shutdown.requested() {
        let now = now_ms();
        if should_follow_handoff(requested_initial_state, epoch) {
            if let Some(plan) = client.activate_if_targeted_handoff()? {
                eprintln!(
                    "{}",
                    promoted_message(&stable_id, epoch, plan.from.epoch, plan.kind)
                );
            }
        }
        if now >= next_heartbeat {
            next_heartbeat = refresh_lease_or_retry(
                &client,
                &stable_id,
                now,
                args.lease_ttl_ms,
                heartbeat_interval,
                &mut heartbeat_state,
            );
            continue;
        }
        let wait_ms = next_heartbeat.saturating_sub(now).max(1);
        thread::sleep(Duration::from_millis(wait_ms.min(100)));
    }

    if args.drain_on_exit {
        let shutdown_summary = graceful_shutdown(
            &client,
            &shutdown,
            &stable_id,
            epoch,
            args.lease_ttl_ms,
            heartbeat_interval,
        )?;
        eprintln!("{shutdown_summary}");
    }
    if let Err(error) = client.enter_offline() {
        eprintln!(
            "mooncake-store-client offline publish failed stable_id={stable_id} error={error}"
        );
    }
    drop(dummy_server);
    client.shutdown();
    if metrics_addr.is_some() {
        stop_metrics_http_server()?;
    }
    eprintln!("{}", stopped_message(&stable_id));
    Ok(())
}

fn normalize_trace_filter(filter: Option<&str>) -> Option<&str> {
    filter.map(str::trim).filter(|value| !value.is_empty())
}

fn parse_cli() -> Result<Cli, clap::Error> {
    parse_cli_from(std::env::args_os())
}

fn parse_cli_from<I, T>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let mut argv: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let should_insert_run = argv
        .get(1)
        .and_then(|arg| arg.to_str())
        .is_some_and(should_insert_legacy_run_subcommand);
    if should_insert_run {
        argv.insert(1, OsString::from("run"));
    }
    Cli::try_parse_from(argv)
}

fn should_insert_legacy_run_subcommand(arg: &str) -> bool {
    !matches!(
        arg,
        "run" | "stats" | "help" | "-h" | "--help" | "-V" | "--version"
    )
}

fn run_stats_command(args: StatsArgs) -> Result<(), Box<dyn Error>> {
    let path = if args.breakdown {
        "/breakdown"
    } else {
        "/stats"
    };
    let body = fetch_http_body(&args.server, path)?;
    if args.json {
        println!("{body}");
    } else if args.breakdown {
        let value: serde_json::Value = serde_json::from_str(&body)
            .map_err(|error| format!("invalid breakdown json: {error}"))?;
        print_breakdown_summary(&value)?;
    } else {
        let value: serde_json::Value =
            serde_json::from_str(&body).map_err(|error| format!("invalid stats json: {error}"))?;
        println!(
            "{}",
            serde_json::to_string_pretty(&value)
                .map_err(|error| format!("failed to format stats json: {error}"))?
        );
    }
    Ok(())
}

fn fetch_http_body(server: &str, path: &str) -> Result<String, Box<dyn Error>> {
    let mut stream = TcpStream::connect(server)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {server}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or("invalid stats http response")?;
    let status_line = headers.lines().next().ok_or("missing stats http status")?;
    if !status_line.contains("200 OK") {
        return Err(format!("{path} request failed: {status_line}").into());
    }
    Ok(body.to_string())
}

fn print_breakdown_summary(value: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    println!(
        "Store-RS breakdown tenant={} note={}",
        value
            .get("tenant")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown"),
        value
            .get("note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("correlation only")
    );
    if let Some(segments) = value.get("segments") {
        println!(
            "segments count={} used_bytes={} capacity_bytes={}",
            json_u64(segments, "count"),
            json_u64(segments, "used_bytes"),
            json_u64(segments, "capacity_bytes")
        );
    }
    println!("top bottleneck candidates:");
    for item in value
        .get("bottlenecks")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(10)
    {
        println!(
            "- source={} name={} result={} calls={} total_ms={:.3} p99_ms={:.3}",
            item.get("source")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            item.get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            item.get("result")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            json_u64(item, "calls_total"),
            json_u64(item, "latency_total_us") as f64 / 1000.0,
            json_u64(item, "latency_p99_us") as f64 / 1000.0
        );
    }
    println!("sglang api focus:");
    for item in value
        .get("operations")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("kind").and_then(serde_json::Value::as_str) == Some("api"))
    {
        let name = item
            .get("operation")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        if !matches!(
            name,
            "batch_put_from"
                | "batch_get_into"
                | "batch_is_exist"
                | "get_size"
                | "query_route"
                | "register_buffer"
                | "unregister_buffer"
        ) {
            continue;
        }
        println!(
            "- {} result={} calls={} bytes_in={} bytes_out={} avg_ms={:.3} max_ms={:.3} p99_ms={:.3}",
            name,
            item.get("result")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            json_u64(item, "calls_total"),
            json_u64(item, "bytes_in_total"),
            json_u64(item, "bytes_out_total"),
            json_u64(item, "latency_avg_us") as f64 / 1000.0,
            json_u64(item, "latency_max_us") as f64 / 1000.0,
            json_u64(item, "latency_p99_us") as f64 / 1000.0
        );
    }
    println!("metadata operations:");
    for item in value
        .get("metadata_operations")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(12)
    {
        println!(
            "- backend={} operation={} result={} calls={} avg_ms={:.3} p99_ms={:.3}",
            item.get("backend")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            item.get("operation")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            item.get("result")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            json_u64(item, "calls_total"),
            json_u64(item, "latency_avg_us") as f64 / 1000.0,
            json_u64(item, "latency_p99_us") as f64 / 1000.0
        );
    }
    println!("transport:");
    for item in value
        .get("transport")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(12)
    {
        println!(
            "- direction={} peer={} result={} operations={} bytes={}",
            item.get("direction")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            item.get("peer_kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown"),
            item.get("result")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("-"),
            json_u64(item, "operations_total"),
            json_u64(item, "bytes_total")
        );
    }
    Ok(())
}

fn json_u64(value: &serde_json::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default()
}

fn graceful_shutdown(
    client: &StoreDispatcher,
    shutdown: &ShutdownSignal,
    stable_id: &str,
    epoch: ClientEpoch,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
) -> Result<String, Box<dyn Error>> {
    client.enter_draining()?;
    let Some(successor) = client.find_hot_upgrade_successor()? else {
        let evacuated = client.evacuate_owned_replicas()?;
        return Ok(drained_message(stable_id, evacuated));
    };

    let created_at_ms = now_ms();
    let deadline_ms = created_at_ms.saturating_add(
        lease_ttl_ms
            .min(30_000)
            .max(heartbeat_interval_ms.max(1_000)),
    );
    let plan = client.plan_handoff(
        successor.runtime.epoch,
        HandoffKind::HotUpgrade,
        created_at_ms,
        created_at_ms,
        Some(deadline_ms),
    )?;
    eprintln!(
        "{}",
        handoff_message(stable_id, epoch, &successor.runtime, deadline_ms)
    );

    wait_for_successor_activation(
        client,
        shutdown,
        &successor.runtime,
        lease_ttl_ms,
        heartbeat_interval_ms,
        deadline_ms,
    )?;
    let migrated = client.evacuate_owned_replicas_to_runtime(successor.runtime.clone())?;
    Ok(upgraded_message(
        &plan.stable_id.0,
        epoch,
        successor.runtime.epoch,
        migrated,
    ))
}

fn compat_warnings(args: &RunArgs) -> Vec<&'static str> {
    let mut warnings = Vec::new();
    if args.route_topk != 2 {
        warnings.push(
            "[WARN] --route-topk is accepted as a compatibility fallback; prefer admin-managed tenant policy in metadata",
        );
    }
    if args.route_control != RouteControlArg::EmbeddedWrh {
        warnings.push(
            "[WARN] --route-control=metadata-only uses Redis-only route storage; this is a cluster-level deployment setting",
        );
    }
    warnings
}

fn emit_compat_warnings(args: &RunArgs) {
    for warning in compat_warnings(args) {
        eprintln!("{warning}");
    }
}

fn validate_args(args: &RunArgs) -> Result<(), Box<dyn Error>> {
    if args.lease_ttl_ms == 0 {
        return Err("--lease-ttl-ms must be greater than zero".into());
    }
    if args.scratch_bytes == 0 {
        return Err("--scratch-bytes must be greater than zero".into());
    }
    if args.storage_bytes == 0
        && args
            .labels
            .iter()
            .any(|(key, value)| key == "storage" && value == "true")
    {
        return Err("--label storage=true requires --storage-bytes > 0".into());
    }
    if args.route_topk < 2 {
        return Err("--route-topk must be greater than or equal to 2".into());
    }
    if args.cold_tier_id.is_some()
        && args.cold_tier_directory.is_none()
        && args.cold_tier_uuid.is_none()
    {
        return Err(
            "--cold-tier-id requires either --cold-tier-directory or --cold-tier-uuid".into(),
        );
    }
    if args.cold_tier_directory.is_some() && args.cold_tier_uuid.is_some() {
        return Err("--cold-tier-directory and --cold-tier-uuid are mutually exclusive".into());
    }
    Ok(())
}

fn resolve_timeout_config(args: &RunArgs) -> Result<CompatTimeoutConfig, Box<dyn Error>> {
    CompatTimeoutConfig::from_env_and_overrides(CompatTimeoutCliOverrides {
        request_timeout_ms: args.request_timeout_ms,
        startup_timeout_ms: args.startup_timeout_ms,
        heartbeat_timeout_ms: args.heartbeat_timeout_ms,
        transfer_stall_timeout_ms: args.transfer_stall_timeout_ms,
        dummy_rpc_timeout_ms: None,
    })
    .map_err(Into::into)
}

fn parse_label(input: &str) -> Result<(String, String), String> {
    let (key, value) = input
        .split_once('=')
        .ok_or_else(|| "labels must use key=value format".to_string())?;
    let key = key.trim();
    if key.is_empty() {
        return Err("label key must not be empty".to_string());
    }
    Ok((key.to_string(), value.trim().to_string()))
}

fn parse_route_control_arg(input: &str) -> Result<RouteControlArg, String> {
    match input.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "embedded-wrh" => Ok(RouteControlArg::EmbeddedWrh),
        "metadata-only" => Ok(RouteControlArg::MetadataOnly),
        other => Err(format!(
            "unsupported route control {other:?}; expected embedded-wrh or metadata-only"
        )),
    }
}

fn parse_transport_backend_arg(input: &str) -> Result<TransportBackendArg, String> {
    match input.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "tent" => Ok(TransportBackendArg::Tent),
        "classic" | "classic-te" | "te" => Ok(TransportBackendArg::ClassicTe),
        other => Err(format!(
            "unsupported transport backend {other:?}; expected tent or classic-te"
        )),
    }
}

fn parse_hugepage_size_arg(input: &str) -> Result<usize, String> {
    parse_hugepage_size(input).map_err(|error| error.to_string())
}

fn explicit_hugepage_setting(args: &RunArgs) -> Option<bool> {
    if args.use_hugepage {
        return Some(true);
    }
    std::env::var("MC_STORE_USE_HUGEPAGE")
        .ok()
        .map(|value| parse_falsey_env_bool(&value))
}

fn parse_falsey_env_bool(value: &str) -> bool {
    !matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "" | "0" | "f" | "false" | "n" | "no" | "off"
    )
}

fn build_runtime_args(
    args: &RunArgs,
    timeouts: CompatTimeoutConfig,
    metrics_addr: Option<&str>,
) -> CompatRuntimeArgs {
    // TE metadata defaults to the classic_te peer-handshake mode when
    // --transport-metadata-url is unset.
    let transport_metadata_url = args
        .transport_metadata_url
        .clone()
        .unwrap_or_else(|| "P2PHANDSHAKE".to_string());
    let mut labels = args.labels.iter().cloned().collect::<BTreeMap<_, _>>();
    labels.remove(METRICS_PORT_LABEL);
    if let Some(port) = metrics_port_label(metrics_addr) {
        labels.insert(METRICS_PORT_LABEL.to_string(), port);
    }
    CompatRuntimeArgs {
        setup: CompatSetupArgs {
            local_hostname: args.local_hostname.clone(),
            transport_metadata_url,
            metadata_url: args.metadata_url.clone(),
            global_segment_size: args.storage_bytes,
            local_buffer_size: args.scratch_bytes,
            eviction_high_watermark_percent: args.eviction_high_watermark_percent,
            eviction_low_watermark_percent: args.eviction_low_watermark_percent,
            protocol: args.protocol.clone(),
            _rdma_devices: args.rdma_devices.clone(),
            transport_rpc_port: args.transport_rpc_port,
            transport_backend: args
                .transport_backend
                .map(|backend| backend.as_str().to_string()),
            stable_id: args.stable_id.clone(),
            tenant: args.tenant.clone(),
            domain: args.domain.clone(),
            object_set: args.object_set.clone(),
            labels,
            routed_writes: args.routed_writes,
            replica_count: args.replica_count,
            route_topk: args.route_topk,
            keyspace: args.keyspace.clone(),
            expires_at_ms: Some(now_ms().saturating_add(args.lease_ttl_ms)),
            use_hugepage: explicit_hugepage_setting(args),
            hugepage_size_bytes: args.hugepage_size,
            timeouts: Some(timeouts),
            cold_tier_targets: build_cold_tier_specs(args),
        },
        local_segment_name: args.local_segment_name.clone(),
        initial_state: startup_initial_state(requested_initial_state(args)),
        route_control: args.route_control.into(),
    }
}

/// Build cold tier target specs from CLI flags.
///
/// Priority: `--cold-tier-targets-json` (full JSON) > individual `--cold-tier-*` flags.
/// The JSON env var (`MC_STORE_RS_COLD_TIER_TARGETS`) is handled downstream by
/// `resolve_cold_tier_target_config` in config.rs, so we only pass explicit CLI input here.
fn build_cold_tier_specs(args: &RunArgs) -> Option<Vec<ColdTierTargetSpec>> {
    if let Some(ref json) = args.cold_tier_targets_json {
        let specs: Vec<ColdTierTargetSpec> = serde_json::from_str(json)
            .unwrap_or_else(|e| panic!("--cold-tier-targets-json is not valid JSON: {e}"));
        return Some(specs);
    }

    let cold_tier_id = args.cold_tier_id.as_deref()?;
    let kind = match args.cold_tier_kind {
        Some(ColdTierKindArg::Ssd) => ColdTierKind::Ssd,
        Some(ColdTierKindArg::Nfs) => ColdTierKind::Nfs,
        None => ColdTierKind::Ssd,
    };
    let ssd_engine = args.cold_tier_ssd_engine.map(|e| match e {
        ColdTierSsdEngineArg::LocalDir => ColdTierSsdEngine::LocalDir,
        ColdTierSsdEngineArg::ExtentStore => ColdTierSsdEngine::ExtentStore,
    });

    let spec = ColdTierTargetSpec {
        cold_tier_id: cold_tier_id.to_string(),
        kind,
        directory: args
            .cold_tier_directory
            .as_ref()
            .map(std::path::PathBuf::from),
        uuid: args.cold_tier_uuid.clone(),
        ssd_engine,
        capacity_override_bytes: args.cold_tier_capacity_bytes,
        tags: args.cold_tier_tags.clone(),
    };
    Some(vec![spec])
}

fn metrics_port_label(metrics_addr: Option<&str>) -> Option<String> {
    let addr = metrics_addr?.parse::<SocketAddr>().ok()?;
    Some(addr.port().to_string())
}

fn requested_initial_state(args: &RunArgs) -> ClientLifecycleState {
    args.initial_state.into()
}

fn startup_initial_state(requested: ClientLifecycleState) -> ClientLifecycleState {
    match requested {
        ClientLifecycleState::Active => ClientLifecycleState::Standby,
        other => other,
    }
}

fn should_activate_after_ready(
    requested: ClientLifecycleState,
    startup: ClientLifecycleState,
) -> bool {
    requested == ClientLifecycleState::Active && startup == ClientLifecycleState::Standby
}

#[allow(clippy::too_many_arguments)]
fn started_message(
    stable_id: &str,
    epoch: ClientEpoch,
    initial_state: ClientLifecycleState,
    segment_name: &str,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    timeouts: CompatTimeoutConfig,
    startup_timeout: Duration,
    metrics_addr: Option<&str>,
    client_server_address: Option<&str>,
) -> String {
    format!(
        "mooncake-store-client started stable_id={stable_id} epoch={} initial_state={} segment={segment_name} lease_ttl_ms={lease_ttl_ms} heartbeat_interval_ms={heartbeat_interval_ms} request_timeout_ms={} startup_timeout_ms={} heartbeat_timeout_ms={} transfer_stall_timeout_ms={} metrics_addr={} client_server_address={} ",
        epoch.0,
        lifecycle_state_label(initial_state),
        timeouts.request_timeout.as_millis(),
        startup_timeout.as_millis(),
        timeouts.heartbeat_timeout.as_millis(),
        timeouts.transfer_stall_timeout.as_millis(),
        metrics_addr.unwrap_or("disabled"),
        client_server_address.unwrap_or("disabled"),
    )
}

fn handoff_message(
    stable_id: &str,
    from_epoch: ClientEpoch,
    successor: &ClientRuntimeId,
    deadline_ms: u64,
) -> String {
    format!(
        "mooncake-store-client handoff stable_id={stable_id} from_epoch={} to_runtime={} deadline_ms={deadline_ms}",
        from_epoch.0, successor
    )
}

fn promoted_message(
    stable_id: &str,
    epoch: ClientEpoch,
    from_epoch: ClientEpoch,
    kind: HandoffKind,
) -> String {
    format!(
        "mooncake-store-client promoted stable_id={stable_id} epoch={} from_epoch={} kind={kind:?}",
        epoch.0, from_epoch.0
    )
}

fn upgraded_message(
    stable_id: &str,
    from_epoch: ClientEpoch,
    to_epoch: ClientEpoch,
    migrated_routes: usize,
) -> String {
    format!(
        "mooncake-store-client upgraded stable_id={stable_id} from_epoch={} to_epoch={} migrated_routes={migrated_routes}",
        from_epoch.0, to_epoch.0
    )
}

fn lifecycle_state_label(state: ClientLifecycleState) -> &'static str {
    match state {
        ClientLifecycleState::Standby => "standby",
        ClientLifecycleState::Active => "active",
        ClientLifecycleState::Draining => "draining",
        ClientLifecycleState::Sealed => "sealed",
        ClientLifecycleState::Offline => "offline",
    }
}

fn drained_message(stable_id: &str, evacuated_routes: usize) -> String {
    format!(
        "mooncake-store-client drained stable_id={stable_id} evacuated_routes={evacuated_routes}"
    )
}

fn stopped_message(stable_id: &str) -> String {
    format!("mooncake-store-client stopped stable_id={stable_id}")
}

fn install_signal_handler() -> Result<ShutdownSignal, Box<dyn Error>> {
    let shutdown = Arc::new(AtomicU8::new(0));
    let handle = shutdown.clone();
    ctrlc::set_handler(move || {
        let _ = handle.fetch_add(1, Ordering::SeqCst);
    })?;
    Ok(ShutdownSignal { state: shutdown })
}

fn start_metrics_if_needed(bind_addr: Option<&str>) -> Result<Option<String>, Box<dyn Error>> {
    match bind_addr {
        Some(bind_addr) => Ok(Some(start_metrics_http_server(bind_addr)?)),
        None => Ok(None),
    }
}

fn effective_heartbeat_interval(requested_ms: u64, lease_ttl_ms: u64) -> u64 {
    if requested_ms == 0 || requested_ms >= lease_ttl_ms {
        return (lease_ttl_ms / 3).max(1_000);
    }
    requested_ms
}

fn initial_heartbeat_delay_ms(
    stable_id: &str,
    epoch: ClientEpoch,
    heartbeat_interval_ms: u64,
) -> u64 {
    stable_phase_spread_ms(
        &format!("{stable_id}:{}", epoch.0),
        heartbeat_interval_ms,
        "heartbeat",
    )
}

#[derive(Clone, Copy, Debug)]
struct HeartbeatLoopState {
    consecutive_failures: u64,
    last_success_ms: u64,
}

impl HeartbeatLoopState {
    fn new(now_ms: u64) -> Self {
        Self {
            consecutive_failures: 0,
            last_success_ms: now_ms,
        }
    }

    fn record_success(&mut self, now_ms: u64) -> u64 {
        let recovered = self.consecutive_failures;
        self.consecutive_failures = 0;
        self.last_success_ms = now_ms;
        recovered
    }

    fn record_failure(&mut self) -> u64 {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.consecutive_failures
    }
}

fn heartbeat_retry_delay_ms(heartbeat_interval_ms: u64) -> u64 {
    heartbeat_interval_ms.clamp(500, 1_000)
}

fn refresh_lease_or_retry(
    client: &StoreDispatcher,
    stable_id: &str,
    now_ms: u64,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    state: &mut HeartbeatLoopState,
) -> u64 {
    match client.heartbeat(now_ms.saturating_add(lease_ttl_ms)) {
        Ok(()) => {
            trace!(
                stable_id,
                expires_at_ms = now_ms.saturating_add(lease_ttl_ms),
                "mooncake-store-client heartbeat refreshed"
            );
            let recovered = state.record_success(now_ms);
            if recovered != 0 {
                eprintln!(
                    "mooncake-store-client heartbeat recovered stable_id={stable_id} recovered_after_failures={recovered}"
                );
            }
            now_ms.saturating_add(heartbeat_interval_ms)
        }
        Err(error) => {
            let failures = state.record_failure();
            let retry_after_ms = heartbeat_retry_delay_ms(heartbeat_interval_ms);
            eprintln!(
                "mooncake-store-client heartbeat failed stable_id={stable_id} consecutive_failures={failures} last_success_age_ms={} retry_after_ms={retry_after_ms} error={error}",
                now_ms.saturating_sub(state.last_success_ms),
            );
            now_ms.saturating_add(retry_after_ms)
        }
    }
}

fn should_follow_handoff(initial_state: ClientLifecycleState, epoch: ClientEpoch) -> bool {
    initial_state == ClientLifecycleState::Standby && epoch.0 > 1
}

fn wait_for_successor_activation(
    client: &StoreDispatcher,
    shutdown: &ShutdownSignal,
    successor: &ClientRuntimeId,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    deadline_ms: u64,
) -> Result<(), Box<dyn Error>> {
    let poll_interval_ms = heartbeat_interval_ms.clamp(50, 500);
    loop {
        if shutdown.forced() {
            return Err("graceful hot-upgrade interrupted by repeated signal".into());
        }
        if let Some(ClientLifecycleState::Active) = client.runtime_state(successor.clone())? {
            return Ok(());
        }
        let now = now_ms();
        if now >= deadline_ms {
            return Err(format!(
                "successor {} did not become active before deadline",
                successor
            )
            .into());
        }
        client.heartbeat(now.saturating_add(lease_ttl_ms))?;
        thread::sleep(Duration::from_millis(poll_interval_ms));
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};
    use std::time::Duration;

    use clap::error::ErrorKind;
    use mooncake_store_client::{
        record_heartbeat_health, stable_phase_spread_ms, start_metrics_http_server,
        stop_metrics_http_server, OperationTracker, RouteControlMode,
    };
    use mooncake_store_core::{ClientEpoch, ClientLifecycleState, METRICS_PORT_LABEL};

    use _store_rs::runtime::CompatTimeoutConfig;

    use std::path::PathBuf;

    use mooncake_store_client::{ColdTierKind, ColdTierSsdEngine};

    use super::{
        build_cold_tier_specs, build_runtime_args, compat_warnings, drained_message,
        dummy_worker_scope, effective_heartbeat_interval, emit_compat_warnings, fetch_http_body,
        heartbeat_retry_delay_ms, initial_heartbeat_delay_ms, normalize_trace_filter, now_ms,
        parse_cli_from, parse_falsey_env_bool, parse_hugepage_size_arg, parse_label,
        parse_route_control_arg, parse_transport_backend_arg, requested_initial_state,
        resolve_timeout_config, should_activate_after_ready, start_metrics_if_needed,
        started_message, startup_initial_state, stopped_message, validate_args, ColdTierKindArg,
        ColdTierSsdEngineArg, Command, HeartbeatLoopState, InitialStateArg, RouteControlArg,
        RunArgs, TransportBackendArg,
    };
    use _store_rs::DEFAULT_COMPAT_WORKER_SCOPE;

    fn sample_timeouts() -> CompatTimeoutConfig {
        CompatTimeoutConfig {
            request_timeout: Duration::from_millis(65_000),
            startup_timeout_override: None,
            heartbeat_timeout: Duration::from_millis(15_000),
            transfer_stall_timeout: Duration::from_millis(10_000),
            dummy_rpc_timeout: Duration::from_millis(65_000),
        }
    }

    fn sample_args() -> RunArgs {
        RunArgs {
            local_hostname: "127.0.0.1".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            storage_bytes: 1024,
            scratch_bytes: 512,
            eviction_high_watermark_percent: None,
            eviction_low_watermark_percent: None,
            protocol: "tcp".to_string(),
            rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("sample".to_string()),
            initial_state: InitialStateArg::Active,
            tenant: "default".to_string(),
            domain: None,
            object_set: None,
            labels: vec![],
            routed_writes: false,
            replica_count: 1,
            route_topk: 2,
            keyspace: None,
            local_segment_name: None,
            lease_ttl_ms: 10_000,
            heartbeat_interval_ms: 3_000,
            request_timeout_ms: None,
            startup_timeout_ms: None,
            heartbeat_timeout_ms: None,
            transfer_stall_timeout_ms: None,
            metrics_addr: None,
            client_server_address: None,
            use_hugepage: false,
            hugepage_size: None,
            trace_filter: None,
            route_control: RouteControlArg::EmbeddedWrh,
            drain_on_exit: false,
            cold_tier_targets_json: None,
            cold_tier_id: None,
            cold_tier_kind: None,
            cold_tier_directory: None,
            cold_tier_uuid: None,
            cold_tier_ssd_engine: None,
            cold_tier_tags: vec![],
            cold_tier_capacity_bytes: None,
        }
    }

    #[test]
    fn build_runtime_args_publishes_metrics_port_without_reachable_host() {
        let mut args = sample_args();
        args.labels.push(("pool".to_string(), "fast".to_string()));
        args.labels
            .push((METRICS_PORT_LABEL.to_string(), "bad-user-value".to_string()));

        let runtime_args = build_runtime_args(&args, sample_timeouts(), Some("0.0.0.0:19300"));

        assert_eq!(
            runtime_args
                .setup
                .labels
                .get(METRICS_PORT_LABEL)
                .map(String::as_str),
            Some("19300")
        );
        assert_eq!(
            runtime_args.setup.labels.get("pool").map(String::as_str),
            Some("fast")
        );
        assert!(!runtime_args
            .setup
            .labels
            .values()
            .any(|value| value.contains("0.0.0.0")));
    }

    #[test]
    fn build_runtime_args_drops_stale_metrics_port_when_metrics_disabled() {
        let mut args = sample_args();
        args.labels
            .push((METRICS_PORT_LABEL.to_string(), "19300".to_string()));

        let runtime_args = build_runtime_args(&args, sample_timeouts(), None);

        assert!(!runtime_args.setup.labels.contains_key(METRICS_PORT_LABEL));
    }

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env_var<T>(key: &str, value: Option<&str>, f: impl FnOnce() -> T) -> T {
        with_env_vars([(key, value)], f)
    }

    fn with_env_vars<'a, T>(
        vars: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
        f: impl FnOnce() -> T,
    ) -> T {
        let _guard = env_test_lock().lock().expect("env test lock poisoned");
        let vars = vars.into_iter().collect::<Vec<_>>();
        let old_values = vars
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect::<Vec<_>>();
        for (key, value) in &vars {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        let result = f();
        for (key, old_value) in old_values {
            match old_value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        result
    }

    #[test]
    fn route_control_arg_maps_to_runtime_mode() {
        assert_eq!(
            RouteControlMode::from(RouteControlArg::EmbeddedWrh),
            RouteControlMode::EmbeddedWrh
        );
        assert_eq!(
            RouteControlMode::from(RouteControlArg::MetadataOnly),
            RouteControlMode::MetadataOnly
        );
    }

    #[test]
    fn args_parser_accepts_core_flags() {
        with_env_var("MC_STORE_RS_INITIAL_STATE", None, || {
            let cli = parse_cli_from([
                "mooncake-store-client",
                "--local-hostname",
                "127.0.0.1",
                "--metadata-url",
                "redis://127.0.0.1:6379/0",
                "--storage-bytes",
                "2048",
                "--scratch-bytes",
                "1024",
                "--protocol",
                "tcp",
                "--tenant",
                "tenant-a",
                "--label",
                "pool=pool-a",
                "--routed-writes",
                "--replica-count",
                "2",
                "--route-topk",
                "4",
                "--route-control",
                "metadata-only",
            ])
            .expect("legacy root run args should parse");
            let Command::Run(args) = cli.command else {
                panic!("expected run command");
            };
            assert_eq!(args.local_hostname, "127.0.0.1");
            assert_eq!(args.metadata_url, "redis://127.0.0.1:6379/0");
            assert_eq!(args.storage_bytes, 2048);
            assert_eq!(args.scratch_bytes, 1024);
            assert_eq!(args.tenant, "tenant-a");
            assert_eq!(
                args.labels,
                vec![("pool".to_string(), "pool-a".to_string())]
            );
            assert!(args.routed_writes);
            assert_eq!(args.replica_count, 2);
            assert_eq!(args.route_topk, 4);
            assert_eq!(args.route_control, RouteControlArg::MetadataOnly);
            assert_eq!(args.initial_state, InitialStateArg::Active);
        });
    }

    #[test]
    fn args_parser_accepts_extended_optional_flags() {
        let cli = parse_cli_from([
            "mooncake-store-client",
            "--local-hostname",
            "10.0.0.1",
            "--metadata-url",
            "etcd://127.0.0.1:2379",
            "--transport-metadata-url",
            "redis://127.0.0.1:6379/9",
            "--stable-id",
            "node-a",
            "--transport-rpc-port",
            "17111",
            "--initial-state",
            "standby",
            "--tenant",
            "tenant-b",
            "--keyspace",
            "ks-a",
            "--local-segment-name",
            "segment-a",
            "--lease-ttl-ms",
            "9000",
            "--heartbeat-interval-ms",
            "2500",
            "--request-timeout-ms",
            "70000",
            "--startup-timeout-ms",
            "180000",
            "--heartbeat-timeout-ms",
            "20000",
            "--transfer-stall-timeout-ms",
            "12000",
            "--metrics-addr",
            "127.0.0.1:0",
            "--client-server-address",
            "127.0.0.1:7001",
            "--use-hugepage",
            "--hugepage-size",
            "2M",
            "--trace-filter",
            "info",
            "--drain-on-exit",
        ])
        .expect("extended legacy root run args should parse");
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(
            args.transport_metadata_url.as_deref(),
            Some("redis://127.0.0.1:6379/9")
        );
        assert_eq!(args.stable_id.as_deref(), Some("node-a"));
        assert_eq!(args.transport_rpc_port, Some(17111));
        assert_eq!(args.initial_state, InitialStateArg::Standby);
        assert_eq!(args.keyspace.as_deref(), Some("ks-a"));
        assert_eq!(args.local_segment_name.as_deref(), Some("segment-a"));
        assert_eq!(args.request_timeout_ms, Some(70_000));
        assert_eq!(args.startup_timeout_ms, Some(180_000));
        assert_eq!(args.heartbeat_timeout_ms, Some(20_000));
        assert_eq!(args.transfer_stall_timeout_ms, Some(12_000));
        assert_eq!(args.metrics_addr.as_deref(), Some("127.0.0.1:0"));
        assert_eq!(
            args.client_server_address.as_deref(),
            Some("127.0.0.1:7001")
        );
        assert!(args.use_hugepage);
        assert_eq!(args.hugepage_size, Some(2 * 1024 * 1024));
        assert_eq!(args.trace_filter.as_deref(), Some("info"));
        assert!(args.drain_on_exit);
    }

    #[test]
    fn args_parser_reads_runtime_config_from_env_when_cli_omits_flags() {
        with_env_vars(
            [
                ("MOONCAKE_LOCAL_HOSTNAME", Some("10.1.0.5")),
                ("MC_STORE_RS_METADATA_URL", Some("redis://127.0.0.1:6380/4")),
                (
                    "MC_STORE_RS_TRANSPORT_METADATA_URL",
                    Some("redis://127.0.0.1:6380/5"),
                ),
                ("MC_STORE_RS_STORAGE_BYTES", Some("4096")),
                ("MC_STORE_RS_SCRATCH_BYTES", Some("2048")),
                ("MOONCAKE_PROTOCOL", Some("rdma")),
                ("MC_STORE_RS_RDMA_DEVICES", Some("mlx5_0,mlx5_1")),
                ("MC_STORE_RS_TRANSPORT_RPC_PORT", Some("17121")),
                ("MC_STORE_RS_TRANSPORT_BACKEND", Some("classic_te")),
                ("MC_STORE_RS_STABLE_ID", Some("env-node")),
                ("MC_STORE_RS_INITIAL_STATE", Some("standby")),
                ("MC_STORE_RS_TENANT", Some("tenant-env")),
                ("MC_STORE_RS_DOMAIN", Some("domain-env")),
                ("MC_STORE_RS_OBJECT_SET", Some("object-set-env")),
                ("MC_STORE_RS_LABELS", Some("pool=env,storage=true")),
                ("MC_STORE_RS_ROUTED_WRITES", Some("1")),
                ("MC_STORE_RS_REPLICA_COUNT", Some("3")),
                ("MC_STORE_RS_ROUTE_TOPK", Some("5")),
                ("MC_STORE_RS_KEYSPACE", Some("env/keyspace")),
                ("MC_STORE_RS_LOCAL_SEGMENT_NAME", Some("env-segment")),
                ("MC_STORE_RS_LEASE_TTL_MS", Some("12000")),
                ("MC_STORE_RS_HEARTBEAT_INTERVAL_MS", Some("4000")),
                ("MC_STORE_RS_REQUEST_TIMEOUT_MS", Some("70000")),
                ("MC_STORE_RS_STARTUP_TIMEOUT_MS", Some("180000")),
                ("MC_STORE_RS_HEARTBEAT_TIMEOUT_MS", Some("20000")),
                ("MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS", Some("9000")),
                ("MC_STORE_RS_METRICS_ADDR", Some("127.0.0.1:19090")),
                ("MC_STORE_RS_CLIENT_SERVER_ADDRESS", Some("127.0.0.1:19091")),
                ("MC_STORE_USE_HUGEPAGE", Some("on")),
                ("MC_STORE_HUGEPAGE_SIZE", Some("2M")),
                ("MC_STORE_RS_TRACE_FILTER", Some("debug")),
                ("MC_STORE_RS_ROUTE_CONTROL", Some("metadata_only")),
                ("MC_STORE_RS_DRAIN_ON_EXIT", Some("yes")),
            ],
            || {
                let cli = parse_cli_from(["mooncake-store-client", "run"])
                    .expect("env-backed run args should parse");
                let Command::Run(args) = cli.command else {
                    panic!("expected run command");
                };
                assert_eq!(args.local_hostname, "10.1.0.5");
                assert_eq!(args.metadata_url, "redis://127.0.0.1:6380/4");
                assert_eq!(
                    args.transport_metadata_url.as_deref(),
                    Some("redis://127.0.0.1:6380/5")
                );
                assert_eq!(args.storage_bytes, 4096);
                assert_eq!(args.scratch_bytes, 2048);
                assert_eq!(args.protocol, "rdma");
                assert_eq!(args.rdma_devices, "mlx5_0,mlx5_1");
                assert_eq!(args.transport_rpc_port, Some(17121));
                assert_eq!(args.transport_backend, Some(TransportBackendArg::ClassicTe));
                assert_eq!(args.stable_id.as_deref(), Some("env-node"));
                assert_eq!(args.initial_state, InitialStateArg::Standby);
                assert_eq!(args.tenant, "tenant-env");
                assert_eq!(args.domain.as_deref(), Some("domain-env"));
                assert_eq!(args.object_set.as_deref(), Some("object-set-env"));
                assert_eq!(
                    args.labels,
                    vec![
                        ("pool".to_string(), "env".to_string()),
                        ("storage".to_string(), "true".to_string())
                    ]
                );
                assert!(args.routed_writes);
                assert_eq!(args.replica_count, 3);
                assert_eq!(args.route_topk, 5);
                assert_eq!(args.keyspace.as_deref(), Some("env/keyspace"));
                assert_eq!(args.local_segment_name.as_deref(), Some("env-segment"));
                assert_eq!(args.lease_ttl_ms, 12_000);
                assert_eq!(args.heartbeat_interval_ms, 4_000);
                assert_eq!(args.request_timeout_ms, Some(70_000));
                assert_eq!(args.startup_timeout_ms, Some(180_000));
                assert_eq!(args.heartbeat_timeout_ms, Some(20_000));
                assert_eq!(args.transfer_stall_timeout_ms, Some(9_000));
                assert_eq!(args.metrics_addr.as_deref(), Some("127.0.0.1:19090"));
                assert_eq!(
                    args.client_server_address.as_deref(),
                    Some("127.0.0.1:19091")
                );
                assert!(args.use_hugepage);
                assert_eq!(args.hugepage_size, Some(2 * 1024 * 1024));
                assert_eq!(args.trace_filter.as_deref(), Some("debug"));
                assert_eq!(args.route_control, RouteControlArg::MetadataOnly);
                assert!(args.drain_on_exit);
            },
        );
    }

    #[test]
    fn cli_values_override_env_backed_runtime_config() {
        with_env_vars(
            [
                ("MOONCAKE_LOCAL_HOSTNAME", Some("10.1.0.5")),
                ("MC_STORE_RS_METADATA_URL", Some("redis://127.0.0.1:6380/4")),
                ("MC_STORE_RS_ROUTE_CONTROL", Some("metadata_only")),
                ("MC_STORE_RS_TRANSPORT_BACKEND", Some("tent")),
                ("MC_STORE_RS_LABELS", Some("pool=env")),
            ],
            || {
                let cli = parse_cli_from([
                    "mooncake-store-client",
                    "run",
                    "--local-hostname",
                    "127.0.0.1",
                    "--metadata-url",
                    "redis://127.0.0.1:6379/0",
                    "--route-control",
                    "embedded-wrh",
                    "--transport-backend",
                    "classic-te",
                    "--label",
                    "pool=cli",
                ])
                .expect("cli args should override env");
                let Command::Run(args) = cli.command else {
                    panic!("expected run command");
                };
                assert_eq!(args.local_hostname, "127.0.0.1");
                assert_eq!(args.metadata_url, "redis://127.0.0.1:6379/0");
                assert_eq!(args.route_control, RouteControlArg::EmbeddedWrh);
                assert_eq!(args.transport_backend, Some(TransportBackendArg::ClassicTe));
                assert_eq!(args.labels, vec![("pool".to_string(), "cli".to_string())]);
            },
        );
    }

    #[test]
    fn env_backed_bool_flags_accept_falsey_values() {
        with_env_vars(
            [
                ("MOONCAKE_LOCAL_HOSTNAME", Some("10.1.0.5")),
                ("MC_STORE_RS_METADATA_URL", Some("redis://127.0.0.1:6380/4")),
                ("MC_STORE_RS_ROUTED_WRITES", Some("0")),
                ("MC_STORE_USE_HUGEPAGE", Some("off")),
                ("MC_STORE_RS_DRAIN_ON_EXIT", Some("no")),
                ("MC_STORE_RS_STATS_SERVER", Some("127.0.0.1:19090")),
                ("MC_STORE_RS_STATS_JSON", Some("false")),
            ],
            || {
                let cli = parse_cli_from(["mooncake-store-client", "run"])
                    .expect("falsey env-backed run args should parse");
                let Command::Run(args) = cli.command else {
                    panic!("expected run command");
                };
                assert!(!args.routed_writes);
                assert!(!args.use_hugepage);
                assert!(!args.drain_on_exit);
                let runtime_args = build_runtime_args(&args, sample_timeouts(), None);
                assert_eq!(runtime_args.setup.use_hugepage, Some(false));

                let cli = parse_cli_from(["mooncake-store-client", "stats"])
                    .expect("falsey env-backed stats args should parse");
                let Command::Stats(args) = cli.command else {
                    panic!("expected stats command");
                };
                assert_eq!(args.server, "127.0.0.1:19090");
                assert!(!args.json);
            },
        );
    }

    #[test]
    fn args_parser_reads_trace_filter_from_env_when_cli_omits_it() {
        with_env_var("MC_STORE_RS_TRACE_FILTER", Some("debug"), || {
            let cli = parse_cli_from([
                "mooncake-store-client",
                "--local-hostname",
                "10.0.0.1",
                "--metadata-url",
                "redis://127.0.0.1:6379/0",
            ])
            .expect("legacy root run args should parse");
            let Command::Run(args) = cli.command else {
                panic!("expected run command");
            };
            assert_eq!(args.trace_filter.as_deref(), Some("debug"));
        });
    }

    #[test]
    fn blank_trace_filter_normalizes_to_default() {
        assert_eq!(normalize_trace_filter(Some("  ")), None);
        assert_eq!(normalize_trace_filter(Some(" debug ")), Some("debug"));
        assert_eq!(normalize_trace_filter(None), None);
    }

    #[test]
    fn stats_subcommand_parses_server_and_json_flag() {
        let cli = parse_cli_from([
            "mooncake-store-client",
            "stats",
            "--server",
            "127.0.0.1:19090",
            "--json",
        ])
        .expect("stats cli should parse");

        match cli.command {
            Command::Stats(args) => {
                assert_eq!(args.server, "127.0.0.1:19090");
                assert!(args.json);
            }
            Command::Run(_) => panic!("expected stats subcommand"),
        }
    }

    #[test]
    fn explicit_run_subcommand_parses_runtime_args() {
        let cli = parse_cli_from([
            "mooncake-store-client",
            "run",
            "--local-hostname",
            "127.0.0.1",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
        ])
        .expect("run subcommand should parse");

        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };
        assert_eq!(args.local_hostname, "127.0.0.1");
        assert_eq!(args.metadata_url, "redis://127.0.0.1:6379/0");
    }

    #[test]
    fn root_help_returns_display_help_error() {
        let error = parse_cli_from(["mooncake-store-client", "--help"])
            .expect_err("help should short-circuit clap parsing");
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
    }

    #[test]
    fn no_args_returns_help_instead_of_missing_required_flags() {
        let error = parse_cli_from(["mooncake-store-client"])
            .expect_err("empty argv should short-circuit to help");
        assert_eq!(
            error.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn hot_upgrade_startup_flags_flow_into_runtime_args() {
        let cli = parse_cli_from([
            "mooncake-store-client",
            "run",
            "--local-hostname",
            "10.0.0.2",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "--stable-id",
            "store-a",
            "--initial-state",
            "standby",
            "--local-segment-name",
            "store-a-next",
        ])
        .expect("hot-upgrade args should parse");
        let Command::Run(args) = cli.command else {
            panic!("expected run command");
        };

        validate_args(&args).expect("hot-upgrade args should validate");
        let runtime_args = build_runtime_args(&args, sample_timeouts(), None);
        assert_eq!(runtime_args.setup.stable_id.as_deref(), Some("store-a"));
        assert_eq!(runtime_args.initial_state, ClientLifecycleState::Standby);
        assert_eq!(
            runtime_args.local_segment_name.as_deref(),
            Some("store-a-next")
        );
    }

    #[test]
    fn compat_warnings_only_trigger_for_non_default_route_flags() {
        assert!(compat_warnings(&sample_args()).is_empty());

        let mut args = sample_args();
        args.route_topk = 4;
        assert_eq!(
            compat_warnings(&args),
            vec![
                "[WARN] --route-topk is accepted as a compatibility fallback; prefer admin-managed tenant policy in metadata"
            ]
        );

        let mut args = sample_args();
        args.route_control = RouteControlArg::MetadataOnly;
        assert_eq!(
            compat_warnings(&args),
            vec![
                "[WARN] --route-control=metadata-only uses Redis-only route storage; this is a cluster-level deployment setting"
            ]
        );

        let mut args = sample_args();
        args.route_topk = 4;
        args.route_control = RouteControlArg::MetadataOnly;
        assert_eq!(compat_warnings(&args).len(), 2);
        emit_compat_warnings(&args);
    }

    #[test]
    fn validate_args_rejects_invalid_ttl_and_storage_role() {
        validate_args(&sample_args()).expect("baseline args should validate");

        let mut args = sample_args();
        args.lease_ttl_ms = 0;
        assert!(validate_args(&args).is_err());

        let mut args = sample_args();
        args.storage_bytes = 0;
        validate_args(&args).expect("scratch-only args should validate");

        let mut args = sample_args();
        args.scratch_bytes = 0;
        assert!(validate_args(&args).is_err());

        let mut args = sample_args();
        args.storage_bytes = 0;
        args.labels = vec![("storage".to_string(), "true".to_string())];
        assert!(validate_args(&args).is_err());

        let mut args = sample_args();
        args.route_topk = 1;
        assert!(validate_args(&args).is_err());
    }

    #[test]
    fn timeout_config_prefers_cli_overrides() {
        let mut args = sample_args();
        args.request_timeout_ms = Some(44_000);
        args.startup_timeout_ms = Some(180_000);
        args.heartbeat_timeout_ms = Some(11_000);
        args.transfer_stall_timeout_ms = Some(9_000);

        let timeouts = resolve_timeout_config(&args).expect("timeout config should resolve");
        assert_eq!(timeouts.request_timeout, Duration::from_millis(44_000));
        assert_eq!(
            timeouts.startup_timeout_override,
            Some(Duration::from_millis(180_000))
        );
        assert_eq!(timeouts.heartbeat_timeout, Duration::from_millis(11_000));
        assert_eq!(
            timeouts.transfer_stall_timeout,
            Duration::from_millis(9_000)
        );
        assert_eq!(timeouts.dummy_rpc_timeout, Duration::from_millis(44_000));
    }

    #[test]
    fn label_and_hugepage_parsers_cover_success_and_error_paths() {
        assert_eq!(
            parse_label("pool=pool-a").expect("label should parse"),
            ("pool".to_string(), "pool-a".to_string())
        );
        assert_eq!(
            parse_label(" tenant = value ").expect("trimmed label should parse"),
            ("tenant".to_string(), "value".to_string())
        );
        assert!(parse_label("missing-delimiter").is_err());
        assert!(parse_label(" =value").is_err());

        assert_eq!(
            parse_route_control_arg("embedded_wrh").expect("underscore route mode should parse"),
            RouteControlArg::EmbeddedWrh
        );
        assert_eq!(
            parse_route_control_arg("metadata-only").expect("hyphen route mode should parse"),
            RouteControlArg::MetadataOnly
        );
        assert!(parse_route_control_arg("other").is_err());

        assert_eq!(
            parse_transport_backend_arg("classic_te").expect("underscore backend should parse"),
            TransportBackendArg::ClassicTe
        );
        assert_eq!(
            parse_transport_backend_arg("te").expect("te alias should parse"),
            TransportBackendArg::ClassicTe
        );
        assert_eq!(
            parse_transport_backend_arg("tent").expect("tent should parse"),
            TransportBackendArg::Tent
        );
        assert!(parse_transport_backend_arg("other").is_err());

        assert!(!parse_falsey_env_bool("0"));
        assert!(!parse_falsey_env_bool("off"));
        assert!(!parse_falsey_env_bool(""));
        assert!(parse_falsey_env_bool("1"));

        assert_eq!(
            parse_hugepage_size_arg("2M").expect("2M should parse"),
            2 * 1024 * 1024
        );
        assert!(parse_hugepage_size_arg("not-a-size").is_err());
    }

    #[test]
    fn heartbeat_and_metrics_helpers_cover_edge_cases() {
        assert_eq!(effective_heartbeat_interval(0, 9_000), 3_000);
        assert_eq!(effective_heartbeat_interval(15_000, 9_000), 3_000);
        assert_eq!(effective_heartbeat_interval(500, 2_000), 500);
        assert_eq!(effective_heartbeat_interval(1_500, 9_000), 1_500);
        assert_eq!(stable_phase_spread_ms("runtime-a:1", 0, "heartbeat"), 0);
        let first_delay = stable_phase_spread_ms("runtime-a:1", 3_000, "heartbeat");
        assert!((1..=3_000).contains(&first_delay));
        assert_eq!(
            first_delay,
            stable_phase_spread_ms("runtime-a:1", 3_000, "heartbeat")
        );
        let heartbeat_delay = initial_heartbeat_delay_ms("runtime-a", ClientEpoch(3), 3_000);
        assert!((1..=3_000).contains(&heartbeat_delay));
        assert_eq!(heartbeat_retry_delay_ms(10_000), 1_000);
        assert_eq!(heartbeat_retry_delay_ms(800), 800);
        assert_eq!(heartbeat_retry_delay_ms(100), 500);

        let mut state = HeartbeatLoopState::new(100);
        assert_eq!(state.record_failure(), 1);
        assert_eq!(state.record_failure(), 2);
        assert_eq!(state.record_success(200), 2);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.last_success_ms, 200);

        assert_eq!(
            start_metrics_if_needed(None).expect("disabled metrics should succeed"),
            None
        );

        let first = now_ms();
        let second = now_ms();
        assert!(second >= first);
    }

    #[test]
    fn stats_command_fetches_json_from_metrics_server() {
        stop_metrics_http_server().expect("metrics server cleanup should succeed");
        let result = Ok(());
        OperationTracker::new("stats_helper_metric")
            .input_bytes(8)
            .finish(&result, 13);
        record_heartbeat_health("runtime-stats-test:1", 3, 123_456);

        let address = start_metrics_http_server("127.0.0.1:0")
            .expect("metrics server should start on an ephemeral port");
        let body = fetch_http_body(&address, "/stats").expect("stats body should fetch");
        let value: serde_json::Value =
            serde_json::from_str(&body).expect("stats body should be valid json");
        assert_eq!(
            value["operations"]
                .as_array()
                .expect("operations should be an array")
                .iter()
                .any(|entry| entry["operation"] == "stats_helper_metric"),
            true
        );
        assert_eq!(
            value["runtimes"]
                .as_array()
                .expect("runtimes should be an array")
                .iter()
                .any(|entry| {
                    entry["runtime"] == "runtime-stats-test:1"
                        && entry["heartbeat_consecutive_failures"] == 3
                }),
            true
        );
        stop_metrics_http_server().expect("metrics server should stop");
    }

    #[test]
    fn runtime_arg_builder_preserves_config_shape() {
        let mut args = sample_args();
        args.transport_metadata_url = Some("redis://127.0.0.1:6380/1".to_string());
        args.rdma_devices = "mlx5_0".to_string();
        args.transport_rpc_port = Some(17112);
        args.keyspace = Some("tenant/keyspace".to_string());
        args.local_segment_name = Some("segment-a".to_string());
        args.metrics_addr = Some("127.0.0.1:9090".to_string());
        args.client_server_address = Some("127.0.0.1:7001".to_string());
        args.use_hugepage = true;
        args.hugepage_size = Some(2 * 1024 * 1024);
        args.trace_filter = Some("debug".to_string());
        args.labels = vec![
            ("pool".to_string(), "a".to_string()),
            ("storage".to_string(), "true".to_string()),
        ];
        args.routed_writes = true;
        args.replica_count = 3;
        args.route_topk = 5;
        args.route_control = RouteControlArg::MetadataOnly;
        args.initial_state = InitialStateArg::Draining;

        let runtime_args = build_runtime_args(&args, sample_timeouts(), None);
        assert_eq!(runtime_args.setup.local_hostname, "127.0.0.1");
        assert_eq!(
            runtime_args.setup.transport_metadata_url,
            "redis://127.0.0.1:6380/1"
        );
        assert_eq!(runtime_args.setup.metadata_url, "redis://127.0.0.1:6379/0");
        assert_eq!(runtime_args.setup._rdma_devices, "mlx5_0");
        assert_eq!(runtime_args.setup.transport_rpc_port, Some(17112));
        assert_eq!(
            runtime_args.setup.keyspace.as_deref(),
            Some("tenant/keyspace")
        );
        assert_eq!(
            runtime_args.local_segment_name.as_deref(),
            Some("segment-a")
        );
        assert_eq!(runtime_args.setup.use_hugepage, Some(true));
        assert_eq!(
            runtime_args.setup.hugepage_size_bytes,
            Some(2 * 1024 * 1024)
        );
        assert_eq!(runtime_args.setup.replica_count, 3);
        assert_eq!(runtime_args.setup.route_topk, 5);
        assert!(runtime_args.setup.routed_writes);
        assert_eq!(runtime_args.route_control, RouteControlMode::MetadataOnly);
        assert_eq!(runtime_args.initial_state, ClientLifecycleState::Draining);
        assert_eq!(
            runtime_args.setup.labels.get("storage").map(String::as_str),
            Some("true")
        );
        assert!(runtime_args.setup.expires_at_ms.is_some());
        assert_eq!(runtime_args.setup.timeouts, Some(sample_timeouts()));
    }

    #[test]
    fn active_startup_is_staged_as_standby_until_runtime_is_ready() {
        let args = sample_args();
        let runtime_args = build_runtime_args(&args, sample_timeouts(), None);
        assert_eq!(requested_initial_state(&args), ClientLifecycleState::Active);
        assert_eq!(runtime_args.initial_state, ClientLifecycleState::Standby);
        assert!(should_activate_after_ready(
            ClientLifecycleState::Active,
            runtime_args.initial_state
        ));
        assert_eq!(
            startup_initial_state(ClientLifecycleState::Draining),
            ClientLifecycleState::Draining
        );
    }

    #[test]
    fn dummy_worker_scope_matches_python_dummy_default_without_keyspace() {
        assert_eq!(dummy_worker_scope(None), DEFAULT_COMPAT_WORKER_SCOPE);
        assert_eq!(
            dummy_worker_scope(Some("tenant/keyspace")),
            "tenant/keyspace"
        );
        assert_eq!(dummy_worker_scope(Some("   ")), DEFAULT_COMPAT_WORKER_SCOPE);
    }

    #[test]
    fn lifecycle_messages_stay_human_readable() {
        let started = started_message(
            "node-a",
            ClientEpoch(2),
            ClientLifecycleState::Standby,
            "segment-a",
            9_000,
            3_000,
            sample_timeouts(),
            Duration::from_millis(300_000),
            Some("127.0.0.1:9090"),
            None,
        );
        assert!(started.contains("stable_id=node-a"));
        assert!(started.contains("epoch=2"));
        assert!(started.contains("initial_state=standby"));
        assert!(started.contains("segment=segment-a"));
        assert!(started.contains("request_timeout_ms=65000"));
        assert!(started.contains("startup_timeout_ms=300000"));
        assert!(started.contains("heartbeat_timeout_ms=15000"));
        assert!(started.contains("transfer_stall_timeout_ms=10000"));
        assert!(started.contains("metrics_addr=127.0.0.1:9090"));
        assert!(started.contains("client_server_address=disabled"));

        assert_eq!(
            drained_message("node-a", 7),
            "mooncake-store-client drained stable_id=node-a evacuated_routes=7"
        );
        assert_eq!(
            stopped_message("node-a"),
            "mooncake-store-client stopped stable_id=node-a"
        );
    }

    #[test]
    fn build_cold_tier_specs_maps_individual_flags() {
        let mut args = sample_args();
        args.cold_tier_id = Some("ssd-a".to_string());
        args.cold_tier_kind = Some(ColdTierKindArg::Ssd);
        args.cold_tier_directory = Some("/tmp/ssd-a".to_string());
        args.cold_tier_ssd_engine = Some(ColdTierSsdEngineArg::ExtentStore);
        args.cold_tier_tags = vec!["fast".to_string(), "local".to_string()];
        args.cold_tier_capacity_bytes = Some(4096);

        let specs = build_cold_tier_specs(&args).expect("cold tier flags should produce specs");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].cold_tier_id, "ssd-a");
        assert_eq!(specs[0].kind, ColdTierKind::Ssd);
        assert_eq!(specs[0].directory, Some(PathBuf::from("/tmp/ssd-a")));
        assert_eq!(specs[0].uuid, None);
        assert_eq!(specs[0].ssd_engine, Some(ColdTierSsdEngine::ExtentStore));
        assert_eq!(specs[0].capacity_override_bytes, Some(4096));
        assert_eq!(specs[0].tags, vec!["fast".to_string(), "local".to_string()]);
    }

    #[test]
    fn build_cold_tier_specs_maps_json_with_multiple_targets() {
        let mut args = sample_args();
        args.cold_tier_targets_json = Some(
            r#"[
                {"cold_tier_id":"ssd-a","kind":"ssd","directory":"/tmp/ssd-a"},
                {"cold_tier_id":"ssd-b","kind":"ssd","directory":"/tmp/ssd-b"}
            ]"#
            .to_string(),
        );

        let specs = build_cold_tier_specs(&args).expect("cold tier json should produce specs");
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].cold_tier_id, "ssd-a");
        assert_eq!(specs[1].cold_tier_id, "ssd-b");
        assert_eq!(specs[0].directory, Some(PathBuf::from("/tmp/ssd-a")));
        assert_eq!(specs[1].directory, Some(PathBuf::from("/tmp/ssd-b")));
    }

    #[test]
    fn build_cold_tier_specs_json_takes_priority_over_individual_flags() {
        let mut args = sample_args();
        args.cold_tier_targets_json = Some(
            r#"[{"cold_tier_id":"from-json","kind":"nfs","directory":"/mnt/nfs"}]"#.to_string(),
        );
        args.cold_tier_id = Some("from-flag".to_string());
        args.cold_tier_directory = Some("/tmp/flag".to_string());

        let specs = build_cold_tier_specs(&args).expect("json should take priority");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].cold_tier_id, "from-json");
        assert_eq!(specs[0].kind, ColdTierKind::Nfs);
    }

    #[test]
    fn build_cold_tier_specs_returns_none_when_no_flags() {
        let args = sample_args();
        assert!(build_cold_tier_specs(&args).is_none());
    }

    #[test]
    fn build_runtime_args_maps_cold_tier_flags_into_setup() {
        let mut args = sample_args();
        args.cold_tier_id = Some("ssd-a".to_string());
        args.cold_tier_kind = Some(ColdTierKindArg::Ssd);
        args.cold_tier_directory = Some("/tmp/ssd-a".to_string());
        args.cold_tier_ssd_engine = Some(ColdTierSsdEngineArg::ExtentStore);
        args.cold_tier_tags = vec!["fast".to_string()];
        args.cold_tier_capacity_bytes = Some(4096);

        let runtime_args = build_runtime_args(&args, sample_timeouts(), None);
        let targets = runtime_args
            .setup
            .cold_tier_targets
            .expect("cold tier flags should flow into setup args");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].cold_tier_id, "ssd-a");
        assert_eq!(targets[0].kind, ColdTierKind::Ssd);
        assert_eq!(targets[0].directory, Some(PathBuf::from("/tmp/ssd-a")));
        assert_eq!(targets[0].ssd_engine, Some(ColdTierSsdEngine::ExtentStore));
        assert_eq!(targets[0].capacity_override_bytes, Some(4096));
        assert_eq!(targets[0].tags, vec!["fast".to_string()]);
    }

    #[test]
    fn validate_args_rejects_cold_tier_id_without_target() {
        let mut args = sample_args();
        args.cold_tier_id = Some("ssd-a".to_string());

        let error = validate_args(&args).expect_err("cold-tier-id without target should fail");
        assert!(error.to_string().contains("--cold-tier-directory"));
    }

    #[test]
    fn validate_args_rejects_conflicting_cold_tier_targets() {
        let mut args = sample_args();
        args.cold_tier_id = Some("ssd-a".to_string());
        args.cold_tier_directory = Some("/tmp/ssd-a".to_string());
        args.cold_tier_uuid = Some("uuid-123".to_string());

        let error = validate_args(&args).expect_err("directory+uuid should conflict");
        assert!(error.to_string().contains("mutually exclusive"));
    }
}
