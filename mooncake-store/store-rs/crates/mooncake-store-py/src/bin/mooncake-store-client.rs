use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use _store_rs::dispatcher::StoreDispatcher;
use _store_rs::dummy_service::start_dummy_store_server;
use _store_rs::runtime::{
    CompatRuntimeArgs, CompatSetupArgs, CompatTimeoutCliOverrides, CompatTimeoutConfig,
};
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use mooncake_store_client::{
    init_tracing, stable_phase_spread_ms, start_metrics_http_server, stop_metrics_http_server,
    RouteControlMode,
};
use mooncake_store_core::{
    parse_hugepage_size, ClientEpoch, ClientLifecycleState, ClientRuntimeId, HandoffKind,
};
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
    #[arg(long)]
    local_hostname: String,
    #[arg(long)]
    metadata_url: String,
    #[arg(long)]
    transport_metadata_url: Option<String>,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    storage_bytes: usize,
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    scratch_bytes: usize,
    #[arg(long, default_value = "tcp")]
    protocol: String,
    #[arg(long, default_value = "")]
    rdma_devices: String,
    #[arg(long, alias = "rpc-server-port")]
    transport_rpc_port: Option<u16>,
    #[arg(
        long,
        value_enum,
        help = "Real transport backend; defaults to classic_te"
    )]
    transport_backend: Option<TransportBackendArg>,
    #[arg(long)]
    stable_id: Option<String>,
    #[arg(long, value_enum, default_value_t = InitialStateArg::Active)]
    initial_state: InitialStateArg,
    #[arg(
        long,
        default_value = "default",
        help = "Default tenant scope for startup policy lookup and request defaults"
    )]
    tenant: String,
    #[arg(long = "label", value_parser = parse_label, help = "Runtime identity and placement labels; use admin-managed tenant policy for tenant-scoped routing/resource policy")]
    labels: Vec<(String, String)>,
    #[arg(long, default_value_t = false)]
    routed_writes: bool,
    #[arg(long, default_value_t = 1)]
    replica_count: usize,
    #[arg(
        long,
        default_value_t = 2,
        help = "Compatibility fallback WRH route-authority fanout; prefer admin-managed tenant policy in metadata"
    )]
    route_topk: usize,
    #[arg(long)]
    keyspace: Option<String>,
    #[arg(long)]
    local_segment_name: Option<String>,
    #[arg(long, default_value_t = 30_000)]
    lease_ttl_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    heartbeat_interval_ms: u64,
    #[arg(long)]
    request_timeout_ms: Option<u64>,
    #[arg(long)]
    startup_timeout_ms: Option<u64>,
    #[arg(long)]
    heartbeat_timeout_ms: Option<u64>,
    #[arg(long)]
    transfer_stall_timeout_ms: Option<u64>,
    #[arg(long)]
    metrics_addr: Option<String>,
    #[arg(long)]
    client_server_address: Option<String>,
    #[arg(long, default_value_t = false)]
    use_hugepage: bool,
    #[arg(long, value_parser = parse_hugepage_size_arg)]
    hugepage_size: Option<usize>,
    #[arg(long)]
    trace_filter: Option<String>,
    #[arg(long, value_enum, default_value_t = RouteControlArg::EmbeddedWrh, help = "Compatibility fallback route-control mode; prefer admin-managed tenant policy in metadata")]
    route_control: RouteControlArg,
    #[arg(long, default_value_t = false)]
    drain_on_exit: bool,
}

#[derive(ClapArgs, Debug)]
struct StatsArgs {
    #[arg(long)]
    server: String,
    #[arg(long, help = "Emit compact JSON instead of pretty JSON")]
    json: bool,
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
    init_tracing(args.trace_filter.as_deref())?;
    emit_compat_warnings(&args);

    let timeouts = resolve_timeout_config(&args)?;
    let metrics_addr = start_metrics_if_needed(args.metrics_addr.as_deref())?;
    let shutdown = install_signal_handler()?;
    let heartbeat_interval =
        effective_heartbeat_interval(args.heartbeat_interval_ms, args.lease_ttl_ms);
    let requested_initial_state = requested_initial_state(&args);
    let runtime = build_runtime_args(&args, timeouts).build()?;

    let stable_id = runtime.stable_id.clone();
    let epoch = runtime.epoch;
    let startup_state = runtime.initial_state;
    let segment_name = runtime.segment_name.clone();
    let client = Arc::new(StoreDispatcher::spawn_with_timeout_config(
        runtime.client,
        format!("mooncake-store-dispatcher-{stable_id}"),
        timeouts,
    )?);
    client.register_local_memory()?;
    let dummy_server = match args.client_server_address.as_deref() {
        Some(address) => Some(start_dummy_store_server(client.clone(), address)?),
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
    let body = fetch_stats_body(&args.server)?;
    if args.json {
        println!("{body}");
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

fn fetch_stats_body(server: &str) -> Result<String, Box<dyn Error>> {
    let mut stream = TcpStream::connect(server)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let request = format!("GET /stats HTTP/1.1\r\nHost: {server}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .ok_or("invalid stats http response")?;
    let status_line = headers.lines().next().ok_or("missing stats http status")?;
    if !status_line.contains("200 OK") {
        return Err(format!("stats request failed: {status_line}").into());
    }
    Ok(body.to_string())
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
            "[WARN] --route-control is accepted as a compatibility fallback; prefer admin-managed tenant policy in metadata",
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

fn parse_hugepage_size_arg(input: &str) -> Result<usize, String> {
    parse_hugepage_size(input).map_err(|error| error.to_string())
}

fn build_runtime_args(args: &RunArgs, timeouts: CompatTimeoutConfig) -> CompatRuntimeArgs {
    CompatRuntimeArgs {
        setup: CompatSetupArgs {
            local_hostname: args.local_hostname.clone(),
            metadata_url: args.metadata_url.clone(),
            transport_metadata_url: args.transport_metadata_url.clone(),
            global_segment_size: args.storage_bytes,
            local_buffer_size: args.scratch_bytes,
            protocol: args.protocol.clone(),
            _rdma_devices: args.rdma_devices.clone(),
            transport_rpc_port: args.transport_rpc_port,
            transport_backend: args
                .transport_backend
                .map(|backend| backend.as_str().to_string()),
            stable_id: args.stable_id.clone(),
            tenant: args.tenant.clone(),
            labels: args.labels.iter().cloned().collect::<BTreeMap<_, _>>(),
            routed_writes: args.routed_writes,
            replica_count: args.replica_count,
            route_topk: args.route_topk,
            keyspace: args.keyspace.clone(),
            expires_at_ms: Some(now_ms().saturating_add(args.lease_ttl_ms)),
            use_hugepage: args.use_hugepage.then_some(true),
            hugepage_size_bytes: args.hugepage_size,
            timeouts: Some(timeouts),
        },
        local_segment_name: args.local_segment_name.clone(),
        initial_state: startup_initial_state(requested_initial_state(args)),
        route_control: args.route_control.into(),
    }
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
    use std::time::Duration;

    use clap::error::ErrorKind;
    use mooncake_store_client::{
        record_heartbeat_health, stable_phase_spread_ms, start_metrics_http_server,
        stop_metrics_http_server, OperationTracker, RouteControlMode,
    };
    use mooncake_store_core::{ClientEpoch, ClientLifecycleState};

    use _store_rs::runtime::CompatTimeoutConfig;

    use super::{
        build_runtime_args, compat_warnings, drained_message, effective_heartbeat_interval,
        emit_compat_warnings, fetch_stats_body, heartbeat_retry_delay_ms,
        initial_heartbeat_delay_ms, now_ms, parse_cli_from, parse_hugepage_size_arg, parse_label,
        requested_initial_state, resolve_timeout_config, should_activate_after_ready,
        start_metrics_if_needed, started_message, startup_initial_state, stopped_message,
        validate_args, Command, HeartbeatLoopState, InitialStateArg, RouteControlArg, RunArgs,
    };

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
            protocol: "tcp".to_string(),
            rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("sample".to_string()),
            initial_state: InitialStateArg::Active,
            tenant: "default".to_string(),
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
        }
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
        let runtime_args = build_runtime_args(&args, sample_timeouts());
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
                "[WARN] --route-control is accepted as a compatibility fallback; prefer admin-managed tenant policy in metadata"
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
        let body = fetch_stats_body(&address).expect("stats body should fetch");
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

        let runtime_args = build_runtime_args(&args, sample_timeouts());
        assert_eq!(runtime_args.setup.local_hostname, "127.0.0.1");
        assert_eq!(runtime_args.setup.metadata_url, "redis://127.0.0.1:6379/0");
        assert_eq!(
            runtime_args.setup.transport_metadata_url.as_deref(),
            Some("redis://127.0.0.1:6380/1")
        );
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
        let runtime_args = build_runtime_args(&args, sample_timeouts());
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
}
