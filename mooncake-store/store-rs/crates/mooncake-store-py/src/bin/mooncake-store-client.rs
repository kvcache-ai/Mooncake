use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use _store_rs::dummy_service::{SharedStoreClient, start_dummy_store_server};
use _store_rs::runtime::{CompatRuntimeArgs, CompatSetupArgs};
use clap::{Parser, ValueEnum};
use mooncake_store_core::parse_hugepage_size;
use mooncake_store_client::{
    init_tracing, start_metrics_http_server, stop_metrics_http_server, MooncakeCompatibilityFacade,
    RouteControlMode,
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

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    validate_args(&args)?;
    init_tracing(args.trace_filter.as_deref())?;

    let metrics_addr = start_metrics_if_needed(args.metrics_addr.as_deref())?;
    let shutdown = install_signal_handler()?;
    let heartbeat_interval = effective_heartbeat_interval(args.heartbeat_interval_ms, args.lease_ttl_ms);
    let labels = args.labels.into_iter().collect::<BTreeMap<_, _>>();
    let runtime = CompatRuntimeArgs {
        setup: CompatSetupArgs {
            local_hostname: args.local_hostname,
            metadata_url: args.metadata_url,
            transport_metadata_url: args.transport_metadata_url,
            global_segment_size: args.storage_bytes,
            local_buffer_size: args.scratch_bytes,
            protocol: args.protocol,
            _rdma_devices: args.rdma_devices,
            stable_id: args.stable_id,
            tenant: args.tenant,
            labels,
            routed_writes: args.routed_writes,
            replica_count: args.replica_count,
            keyspace: args.keyspace,
            expires_at_ms: Some(now_ms().saturating_add(args.lease_ttl_ms)),
            use_hugepage: args.use_hugepage.then_some(true),
            hugepage_size_bytes: args.hugepage_size,
        },
        local_segment_name: args.local_segment_name,
        route_control: args.route_control.into(),
    }
    .build()?;

    let stable_id = runtime.stable_id.clone();
    let segment_name = runtime.segment_name.clone();
    let mut store_client = runtime.client;
    store_client.register_local_memory()?;
    let (shared_client, mut shared_client_loop) = SharedStoreClient::new();
    let client = Arc::new(shared_client);
    let dummy_server = match args.client_server_address.as_deref() {
        Some(address) => Some(start_dummy_store_server(client.clone(), address)?),
        None => None,
    };

    eprintln!(
        "mooncake-store-client started stable_id={} segment={} lease_ttl_ms={} heartbeat_interval_ms={} metrics_addr={} client_server_address={} ",
        stable_id,
        segment_name,
        args.lease_ttl_ms,
        heartbeat_interval,
        metrics_addr.as_deref().unwrap_or("disabled"),
        dummy_server
            .as_ref()
            .map(|server| server.address())
            .unwrap_or("disabled"),
    );

    let mut next_heartbeat = now_ms().saturating_add(heartbeat_interval);
    while !shutdown.load(Ordering::Relaxed) {
        let now = now_ms();
        let wait_ms = next_heartbeat.saturating_sub(now).max(1);
        shared_client_loop.pump(
            &mut store_client,
            Duration::from_millis(wait_ms.min(heartbeat_interval.max(1))),
        );
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let now = now_ms();
        if now >= next_heartbeat {
            store_client.heartbeat(now.saturating_add(args.lease_ttl_ms))?;
            next_heartbeat = now.saturating_add(heartbeat_interval);
        }
    }

    drop(dummy_server);
    drop(client);
    shared_client_loop.drain(&mut store_client);

    if args.drain_on_exit {
        store_client.enter_draining()?;
        let evacuated = store_client.evacuate_owned_replicas()?;
        eprintln!(
            "mooncake-store-client drained stable_id={} evacuated_routes={}",
            stable_id, evacuated
        );
    }
    if metrics_addr.is_some() {
        stop_metrics_http_server()?;
    }
    eprintln!("mooncake-store-client stopped stable_id={}", stable_id);
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), Box<dyn Error>> {
    if args.lease_ttl_ms == 0 {
        return Err("--lease-ttl-ms must be greater than zero".into());
    }
    if args.storage_bytes == 0 {
        return Err("--storage-bytes must be greater than zero".into());
    }
    if args.scratch_bytes == 0 {
        return Err("--scratch-bytes must be greater than zero".into());
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
    parse_hugepage_size(input)
        .map_err(|error| error.to_string())
}

fn install_signal_handler() -> Result<Arc<AtomicBool>, Box<dyn Error>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let handle = shutdown.clone();
    ctrlc::set_handler(move || {
        handle.store(true, Ordering::SeqCst);
    })?;
    Ok(shutdown)
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_millis() as u64
}
