use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use _store_rs::dispatcher::StoreDispatcher;
use _store_rs::dummy_service::start_dummy_store_server;
use _store_rs::runtime::{CompatRuntimeArgs, CompatSetupArgs};
use clap::{Parser, ValueEnum};
use mooncake_store_client::{
    init_tracing, start_metrics_http_server, stop_metrics_http_server, RouteControlMode,
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

#[derive(Parser, Debug)]
#[command(name = "mooncake-store-client")]
#[command(about = "Start a standalone Mooncake store-rs client runtime")]
struct Args {
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
    #[arg(long)]
    stable_id: Option<String>,
    #[arg(long, default_value_t = 1)]
    epoch: u64,
    #[arg(long, value_enum, default_value_t = InitialStateArg::Active)]
    initial_state: InitialStateArg,
    #[arg(long, default_value = "default")]
    tenant: String,
    #[arg(long = "label", value_parser = parse_label)]
    labels: Vec<(String, String)>,
    #[arg(long, default_value_t = false)]
    routed_writes: bool,
    #[arg(long, default_value_t = 1)]
    replica_count: usize,
    #[arg(long)]
    keyspace: Option<String>,
    #[arg(long)]
    local_segment_name: Option<String>,
    #[arg(long, default_value_t = 600_000)]
    lease_ttl_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    heartbeat_interval_ms: u64,
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
    #[arg(long, value_enum, default_value_t = RouteControlArg::EmbeddedWrh)]
    route_control: RouteControlArg,
    #[arg(long, default_value_t = false)]
    drain_on_exit: bool,
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
    let args = Args::parse();
    validate_args(&args)?;
    init_tracing(args.trace_filter.as_deref())?;

    let metrics_addr = start_metrics_if_needed(args.metrics_addr.as_deref())?;
    let shutdown = install_signal_handler()?;
    let heartbeat_interval =
        effective_heartbeat_interval(args.heartbeat_interval_ms, args.lease_ttl_ms);
    let runtime = build_runtime_args(&args).build()?;

    let stable_id = runtime.stable_id.clone();
    let epoch = runtime.epoch;
    let initial_state = runtime.initial_state;
    let segment_name = runtime.segment_name.clone();
    let client = Arc::new(StoreDispatcher::spawn(
        runtime.client,
        format!("mooncake-store-dispatcher-{stable_id}"),
    )?);
    client.register_local_memory()?;
    let dummy_server = match args.client_server_address.as_deref() {
        Some(address) => Some(start_dummy_store_server(client.clone(), address)?),
        None => None,
    };

    eprintln!(
        "{}",
        started_message(
            &stable_id,
            epoch,
            initial_state,
            &segment_name,
            args.lease_ttl_ms,
            heartbeat_interval,
            metrics_addr.as_deref(),
            dummy_server.as_ref().map(|server| server.address()),
        )
    );

    let mut next_heartbeat = now_ms().saturating_add(heartbeat_interval);
    while !shutdown.requested() {
        let now = now_ms();
        if should_follow_handoff(initial_state, epoch) {
            if let Some(plan) = client.activate_if_targeted_handoff()? {
                eprintln!(
                    "{}",
                    promoted_message(&stable_id, epoch, plan.from.epoch, plan.kind)
                );
            }
        }
        if now >= next_heartbeat {
            client.heartbeat(now.saturating_add(args.lease_ttl_ms))?;
            next_heartbeat = now.saturating_add(heartbeat_interval);
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
    drop(dummy_server);
    client.shutdown();
    if metrics_addr.is_some() {
        stop_metrics_http_server()?;
    }
    eprintln!("{}", stopped_message(&stable_id));
    Ok(())
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

fn validate_args(args: &Args) -> Result<(), Box<dyn Error>> {
    if args.lease_ttl_ms == 0 {
        return Err("--lease-ttl-ms must be greater than zero".into());
    }
    if args.epoch == 0 {
        return Err("--epoch must be greater than zero".into());
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
    Ok(())
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

fn build_runtime_args(args: &Args) -> CompatRuntimeArgs {
    CompatRuntimeArgs {
        setup: CompatSetupArgs {
            local_hostname: args.local_hostname.clone(),
            metadata_url: args.metadata_url.clone(),
            transport_metadata_url: args.transport_metadata_url.clone(),
            global_segment_size: args.storage_bytes,
            local_buffer_size: args.scratch_bytes,
            protocol: args.protocol.clone(),
            _rdma_devices: args.rdma_devices.clone(),
            stable_id: args.stable_id.clone(),
            tenant: args.tenant.clone(),
            labels: args.labels.iter().cloned().collect::<BTreeMap<_, _>>(),
            routed_writes: args.routed_writes,
            replica_count: args.replica_count,
            keyspace: args.keyspace.clone(),
            expires_at_ms: Some(now_ms().saturating_add(args.lease_ttl_ms)),
            use_hugepage: args.use_hugepage.then_some(true),
            hugepage_size_bytes: args.hugepage_size,
        },
        local_segment_name: args.local_segment_name.clone(),
        epoch: ClientEpoch(args.epoch),
        initial_state: args.initial_state.into(),
        route_control: args.route_control.into(),
    }
}

fn started_message(
    stable_id: &str,
    epoch: ClientEpoch,
    initial_state: ClientLifecycleState,
    segment_name: &str,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    metrics_addr: Option<&str>,
    client_server_address: Option<&str>,
) -> String {
    format!(
        "mooncake-store-client started stable_id={stable_id} epoch={} initial_state={} segment={segment_name} lease_ttl_ms={lease_ttl_ms} heartbeat_interval_ms={heartbeat_interval_ms} metrics_addr={} client_server_address={} ",
        epoch.0,
        lifecycle_state_label(initial_state),
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
    let poll_interval_ms = heartbeat_interval_ms.min(500).max(50);
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
    use clap::Parser;
    use mooncake_store_client::RouteControlMode;
    use mooncake_store_core::{ClientEpoch, ClientLifecycleState};

    use super::{
        build_runtime_args, drained_message, effective_heartbeat_interval, now_ms,
        parse_hugepage_size_arg, parse_label, start_metrics_if_needed, started_message,
        stopped_message, validate_args, Args, InitialStateArg, RouteControlArg,
    };

    fn sample_args() -> Args {
        Args {
            local_hostname: "127.0.0.1".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            storage_bytes: 1024,
            scratch_bytes: 512,
            protocol: "tcp".to_string(),
            rdma_devices: String::new(),
            stable_id: Some("sample".to_string()),
            epoch: 1,
            initial_state: InitialStateArg::Active,
            tenant: "default".to_string(),
            labels: vec![],
            routed_writes: false,
            replica_count: 1,
            keyspace: None,
            local_segment_name: None,
            lease_ttl_ms: 10_000,
            heartbeat_interval_ms: 3_000,
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
        let args = Args::try_parse_from([
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
            "--route-control",
            "metadata-only",
        ])
        .expect("args should parse");
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
        assert_eq!(args.route_control, RouteControlArg::MetadataOnly);
        assert_eq!(args.epoch, 1);
        assert_eq!(args.initial_state, InitialStateArg::Active);
    }

    #[test]
    fn args_parser_accepts_extended_optional_flags() {
        let args = Args::try_parse_from([
            "mooncake-store-client",
            "--local-hostname",
            "10.0.0.1",
            "--metadata-url",
            "etcd://127.0.0.1:2379",
            "--transport-metadata-url",
            "redis://127.0.0.1:6379/9",
            "--stable-id",
            "node-a",
            "--epoch",
            "2",
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
        .expect("extended args should parse");
        assert_eq!(
            args.transport_metadata_url.as_deref(),
            Some("redis://127.0.0.1:6379/9")
        );
        assert_eq!(args.stable_id.as_deref(), Some("node-a"));
        assert_eq!(args.epoch, 2);
        assert_eq!(args.initial_state, InitialStateArg::Standby);
        assert_eq!(args.keyspace.as_deref(), Some("ks-a"));
        assert_eq!(args.local_segment_name.as_deref(), Some("segment-a"));
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
    fn hot_upgrade_startup_flags_flow_into_runtime_args() {
        let args = Args::try_parse_from([
            "mooncake-store-client",
            "--local-hostname",
            "10.0.0.2",
            "--metadata-url",
            "redis://127.0.0.1:6379/0",
            "--stable-id",
            "store-a",
            "--epoch",
            "7",
            "--initial-state",
            "standby",
            "--local-segment-name",
            "store-a-next",
        ])
        .expect("hot-upgrade args should parse");

        validate_args(&args).expect("hot-upgrade args should validate");
        let runtime_args = build_runtime_args(&args);
        assert_eq!(runtime_args.setup.stable_id.as_deref(), Some("store-a"));
        assert_eq!(runtime_args.epoch, ClientEpoch(7));
        assert_eq!(runtime_args.initial_state, ClientLifecycleState::Standby);
        assert_eq!(
            runtime_args.local_segment_name.as_deref(),
            Some("store-a-next")
        );
    }

    #[test]
    fn validate_args_rejects_invalid_ttl_epoch_and_storage_role() {
        validate_args(&sample_args()).expect("baseline args should validate");

        let mut args = sample_args();
        args.lease_ttl_ms = 0;
        assert!(validate_args(&args).is_err());

        let mut args = sample_args();
        args.epoch = 0;
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

        assert_eq!(
            start_metrics_if_needed(None).expect("disabled metrics should succeed"),
            None
        );

        let first = now_ms();
        let second = now_ms();
        assert!(second >= first);
    }

    #[test]
    fn runtime_arg_builder_preserves_config_shape() {
        let mut args = sample_args();
        args.transport_metadata_url = Some("redis://127.0.0.1:6380/1".to_string());
        args.rdma_devices = "mlx5_0".to_string();
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
        args.route_control = RouteControlArg::MetadataOnly;
        args.epoch = 11;
        args.initial_state = InitialStateArg::Draining;

        let runtime_args = build_runtime_args(&args);
        assert_eq!(runtime_args.setup.local_hostname, "127.0.0.1");
        assert_eq!(runtime_args.setup.metadata_url, "redis://127.0.0.1:6379/0");
        assert_eq!(
            runtime_args.setup.transport_metadata_url.as_deref(),
            Some("redis://127.0.0.1:6380/1")
        );
        assert_eq!(runtime_args.setup._rdma_devices, "mlx5_0");
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
        assert!(runtime_args.setup.routed_writes);
        assert_eq!(runtime_args.route_control, RouteControlMode::MetadataOnly);
        assert_eq!(runtime_args.epoch, ClientEpoch(11));
        assert_eq!(runtime_args.initial_state, ClientLifecycleState::Draining);
        assert_eq!(
            runtime_args.setup.labels.get("storage").map(String::as_str),
            Some("true")
        );
        assert!(runtime_args.setup.expires_at_ms.is_some());
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
            Some("127.0.0.1:9090"),
            None,
        );
        assert!(started.contains("stable_id=node-a"));
        assert!(started.contains("epoch=2"));
        assert!(started.contains("initial_state=standby"));
        assert!(started.contains("segment=segment-a"));
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
