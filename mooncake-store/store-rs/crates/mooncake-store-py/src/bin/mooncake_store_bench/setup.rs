use std::collections::BTreeMap;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use _store_rs::runtime::{CompatRuntime, CompatRuntimeArgs, CompatSetupArgs};
use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{MooncakeCompatibilityFacade, StoreClient};
use mooncake_store_core::{ClientLease, ClientLifecycleState, MetadataBackend, RouteControlMode};
use tracing::{debug, info, trace, warn};
use url::Url;

use crate::cli::{GlobalArgs, Protocol, RouteControl, TransportBackend};

pub const LEASE_MS: u64 = 600_000;
const STORAGE_CANDIDATE_WAIT: Duration = Duration::from_secs(15);
const STORAGE_CANDIDATE_POLL: Duration = Duration::from_millis(100);
const REDIS_DEBUG_TIMEOUT: Duration = Duration::from_secs(2);
const REDIS_DEBUG_VALUE_LIMIT: usize = 2048;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn unique_suffix() -> u64 {
    now_ms() ^ (std::process::id() as u64)
}

fn bench_client_stable_id(role: &str, index: usize, run_id: u64) -> String {
    format!("bench-{role}-{index}-{run_id}")
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum StorageSegmentNamingMode {
    LogicalSegmentName,
    RpcAddressSegmentName,
}

impl StorageSegmentNamingMode {
    fn label(self) -> &'static str {
        match self {
            Self::LogicalSegmentName => "metadata-backed logical segment names",
            Self::RpcAddressSegmentName => "P2P-handshake rpc-address segment names",
        }
    }
}

#[derive(Debug)]
struct StorageSegmentObservation {
    runtime: String,
    segment_name: String,
    rpc_address: String,
    mode: StorageSegmentNamingMode,
}

fn expected_storage_segment_naming_mode(transport_metadata_url: &str) -> StorageSegmentNamingMode {
    if transport_metadata_url
        .trim()
        .eq_ignore_ascii_case("P2PHANDSHAKE")
    {
        StorageSegmentNamingMode::RpcAddressSegmentName
    } else {
        StorageSegmentNamingMode::LogicalSegmentName
    }
}

fn observe_active_storage_segment_modes(
    live_clients: &[ClientLease],
) -> Vec<StorageSegmentObservation> {
    live_clients
        .iter()
        .filter(|lease| lease.state == ClientLifecycleState::Active)
        .filter(|lease| {
            lease
                .endpoints
                .labels
                .get("storage")
                .is_some_and(|value| value == "true")
        })
        .filter_map(|lease| {
            let segment_name = lease.endpoints.segment_name.as_ref()?.0.clone();
            let rpc_address = lease.endpoints.rpc_address.trim().to_string();
            if rpc_address.is_empty() {
                return None;
            }
            let mode = if segment_name == rpc_address {
                StorageSegmentNamingMode::RpcAddressSegmentName
            } else {
                StorageSegmentNamingMode::LogicalSegmentName
            };
            Some(StorageSegmentObservation {
                runtime: lease.runtime.to_string(),
                segment_name,
                rpc_address,
                mode,
            })
        })
        .collect()
}

fn validate_storage_segment_naming_consistency(
    live_clients: &[ClientLease],
    keyspace_prefix: &str,
    transport_metadata_url: &str,
) -> Result<(), String> {
    let observations = observe_active_storage_segment_modes(live_clients);
    if observations.is_empty() {
        return Ok(());
    }

    let mut distinct_modes = observations
        .iter()
        .map(|entry| entry.mode)
        .collect::<Vec<_>>();
    distinct_modes.sort();
    distinct_modes.dedup();

    let observed = observations
        .iter()
        .map(|entry| {
            format!(
                "runtime={} segment={} rpc={} mode={}",
                entry.runtime,
                entry.segment_name,
                entry.rpc_address,
                entry.mode.label()
            )
        })
        .collect::<Vec<_>>()
        .join("; ");

    if distinct_modes.len() > 1 {
        return Err(format!(
            "scratch-only bench refuses to start because active storage daemons in keyspace {keyspace_prefix} publish mixed segment naming modes under transport_metadata_url={transport_metadata_url}: {observed}. Restart every storage daemon and the bench with one transport metadata mode before benchmarking."
        ));
    }

    let expected_mode = expected_storage_segment_naming_mode(transport_metadata_url);
    let observed_mode = distinct_modes[0];
    if observed_mode != expected_mode {
        return Err(format!(
            "scratch-only bench refuses to start because bench transport metadata expects {} in keyspace {keyspace_prefix}, but active storage daemons publish {} under transport_metadata_url={transport_metadata_url}: {observed}. Restart every storage daemon and the bench with one transport metadata mode before benchmarking.",
            expected_mode.label(),
            observed_mode.label(),
        ));
    }

    Ok(())
}

fn ensure_storage_candidates(
    metadata: &dyn MetadataBackend,
    keyspace_prefix: &str,
    transport_metadata_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + STORAGE_CANDIDATE_WAIT;
    let mut logged_snapshot = false;
    loop {
        let live_clients = metadata.list_live_clients()?;
        let live_count = live_clients.len();
        let active_count = live_clients
            .iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .count();
        if !logged_snapshot {
            log_live_client_snapshot(&live_clients, keyspace_prefix);
            log_segment_snapshot(metadata, &live_clients);
            log_transfer_engine_metadata_snapshot(transport_metadata_url, &live_clients);
            logged_snapshot = true;
        }
        if let Err(error) = validate_storage_segment_naming_consistency(
            &live_clients,
            keyspace_prefix,
            transport_metadata_url,
        ) {
            return Err(error.into());
        }
        let storage_candidates = live_clients
            .iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .filter(|lease| {
                lease
                    .endpoints
                    .labels
                    .get("storage")
                    .is_some_and(|value| value == "true")
            })
            .count();
        if storage_candidates > 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "scratch-only bench requires at least one active storage=true daemon in the same metadata keyspace; keyspace={keyspace_prefix} live_clients={live_count} active_clients={active_count} active_storage_candidates=0. Start mooncake-store-client with --storage-bytes >0 --label storage=true --keyspace {keyspace_prefix}, or pass --keyspace matching existing storage daemons."
            )
            .into());
        }
        thread::sleep(STORAGE_CANDIDATE_POLL);
    }
}

fn log_bench_startup_context(
    global: &GlobalArgs,
    keyspace_prefix: &str,
    writer_count: usize,
    reader_count: usize,
) {
    info!(
        "[bench] startup keyspace={} metadata_url={} transport_metadata_url={} backend={:?} protocol={:?} local_hostname={} storage_bytes={} scratch_bytes={} replica_count={} route_control={:?} route_topk={} writers={} readers={}",
        keyspace_prefix,
        redact_url_for_log(&global.metadata_url),
        redact_url_for_log(bench_transfer_metadata_debug_url(global)),
        global.transport_backend,
        global.protocol,
        global.local_hostname,
        global.storage_bytes,
        global.scratch_bytes,
        global.replica_count,
        global.route_control,
        global.route_topk,
        writer_count,
        reader_count
    );
    if !global.keyspace.is_empty() {
        warn!(
            keyspace = global.keyspace,
            "custom keyspace is set — make sure you understand its effect on tenant isolation; \
             misconfigured keyspace can cause nodes to register in different metadata prefixes"
        );
    }
    log_relevant_env();
}

fn log_relevant_env() {
    const KEYS: &[&str] = &[
        "MC_STORE_RS_METADATA_URL",
        "MC_STORE_RS_TRANSPORT_METADATA_URL",
        "MC_STORE_RS_TRACE_FILTER",
        "MC_STORE_RS_TRACE_FILE",
        "MC_BENCH_TRACE_FILE",
        "MC_BENCH_INTERFACES",
        "MC_STORE_RS_KEYSPACE",
        "MC_STORE_RS_TRANSPORT_BACKEND",
        "MOONCAKE_PROTOCOL",
        "MOONCAKE_LOCAL_HOSTNAME",
        "MC_BENCH_STORAGE_BYTES",
        "MC_BENCH_WRITE_INTERFACE",
        "MC_BENCH_READ_INTERFACE",
        "MC_STORE_RS_SCRATCH_BYTES",
        "MC_METADATA_CLUSTER_ID",
        "MC_STORE_RS_GID_INDEX",
        "MC_IB_PORT",
        "MC_GID_INDEX",
        "NCCL_IB_GID_INDEX",
        "MC_IB_TC",
        "MC_IB_PCI_RELAXED_ORDERING",
        "MC_SLICE_TIMEOUT",
    ];
    for key in KEYS {
        match std::env::var(key) {
            Ok(value) => debug!("[bench] env {key}={}", redact_env_value(key, &value)),
            Err(_) => debug!("[bench] env {key}=<unset>"),
        }
    }
}

fn bench_transfer_metadata_debug_url(global: &GlobalArgs) -> &str {
    // Mirror the fallback chain used when building the CompatSetupArgs.
    global
        .transport_metadata_url
        .as_deref()
        .unwrap_or("P2PHANDSHAKE")
}

fn redact_env_value(key: &str, value: &str) -> String {
    if key.contains("URL") {
        redact_url_for_log(value)
    } else {
        value.to_string()
    }
}

fn redact_url_for_log(value: &str) -> String {
    let Ok(url) = Url::parse(value) else {
        return value.to_string();
    };
    if url.password().is_none() && url.username().is_empty() {
        return value.to_string();
    }
    let mut redacted = url;
    let _ = redacted.set_username(if redacted.username().is_empty() {
        ""
    } else {
        "<redacted>"
    });
    let _ = redacted.set_password(redacted.password().map(|_| "<redacted>"));
    redacted.to_string()
}

fn log_live_client_snapshot(live_clients: &[ClientLease], keyspace_prefix: &str) {
    info!(
        "[bench] live clients snapshot keyspace={keyspace_prefix} count={}",
        live_clients.len()
    );
    for line in format_live_client_snapshot(live_clients) {
        debug!("[bench] {line}");
    }
}

fn log_segment_snapshot(metadata: &dyn MetadataBackend, live_clients: &[ClientLease]) {
    debug!("[bench] segment snapshot for live clients");
    if live_clients.is_empty() {
        debug!("[bench] segments: (none)");
        return;
    }
    for lease in live_clients {
        match metadata.list_segments(Some(&lease.runtime)) {
            Ok(segments) if segments.is_empty() => {
                debug!("[bench] segments owner={} count=0", lease.runtime);
            }
            Ok(segments) => {
                debug!(
                    "[bench] segments owner={} count={}",
                    lease.runtime,
                    segments.len()
                );
                for segment in segments.iter().take(8) {
                    let tags = if segment.tags.is_empty() {
                        "-".to_string()
                    } else {
                        segment.tags.join(",")
                    };
                    trace!(
                        "[bench] segment owner={} name={} state={:?} used={} capacity={} alignment={} tags={}",
                        segment.owner,
                        segment.segment_name.0,
                        segment.state,
                        segment.used_bytes,
                        segment.capacity_bytes,
                        segment.alignment_bytes,
                        tags
                    );
                }
                if segments.len() > 8 {
                    trace!(
                        "[bench] segments owner={} truncated={} omitted={}",
                        lease.runtime,
                        8,
                        segments.len() - 8
                    );
                }
            }
            Err(error) => {
                warn!("[bench] segments owner={} error={}", lease.runtime, error);
            }
        }
    }
}

#[derive(Debug)]
struct RedisDebugTarget {
    host: String,
    port: u16,
    db_index: u32,
    username: Option<String>,
    password: Option<String>,
}

fn log_transfer_engine_metadata_snapshot(
    transport_metadata_url: &str,
    live_clients: &[ClientLease],
) {
    let Ok(target) = parse_redis_debug_target(transport_metadata_url) else {
        debug!(
            "[bench] transfer metadata snapshot skipped: unsupported transport metadata url {}",
            redact_url_for_log(transport_metadata_url)
        );
        return;
    };
    match open_redis_debug_stream(&target) {
        Ok(mut stream) => {
            debug!(
                "[bench] transfer metadata snapshot redis={} db={}",
                target.host, target.db_index
            );
            for lease in live_clients {
                let Some(segment) = lease.endpoints.segment_name.as_ref() else {
                    continue;
                };
                let keys = [
                    transfer_engine_metadata_key("ram", &segment.0),
                    transfer_engine_metadata_key("rpc_meta", &segment.0),
                ];
                for key in keys {
                    match redis_get(&mut stream, &key) {
                        Ok(Some(value)) => trace!(
                            "[bench] te-meta key={} value={}",
                            key,
                            truncate_for_log(&value)
                        ),
                        Ok(None) => trace!("[bench] te-meta key={} value=<nil>", key),
                        Err(error) => {
                            warn!("[bench] te-meta key={} error={}", key, error);
                            return;
                        }
                    }
                }
            }
        }
        Err(error) => {
            debug!("[bench] transfer metadata snapshot skipped: {}", error);
        }
    }
}

fn transfer_engine_metadata_key(kind: &str, name: &str) -> String {
    let mut prefix = String::from("mooncake/");
    if let Ok(cluster_id) = std::env::var("MC_METADATA_CLUSTER_ID") {
        if !cluster_id.is_empty() {
            prefix.push_str(&cluster_id);
            if !prefix.ends_with('/') {
                prefix.push('/');
            }
        }
    }
    prefix.push_str(kind);
    prefix.push('/');
    prefix.push_str(name);
    prefix
}

fn truncate_for_log(value: &str) -> String {
    if value.len() <= REDIS_DEBUG_VALUE_LIMIT {
        value.to_string()
    } else {
        format!(
            "{}...(truncated {} bytes)",
            &value[..REDIS_DEBUG_VALUE_LIMIT],
            value.len() - REDIS_DEBUG_VALUE_LIMIT
        )
    }
}

fn parse_redis_debug_target(metadata_url: &str) -> Result<RedisDebugTarget, String> {
    let redis = Url::parse(metadata_url).map_err(|error| error.to_string())?;
    if redis.scheme() != "redis" {
        return Err(format!("scheme {} is not redis", redis.scheme()));
    }
    let host = redis
        .host_str()
        .ok_or_else(|| "redis url missing host".to_string())?
        .to_string();
    let port = redis.port().unwrap_or(6379);
    let db_index = redis
        .path_segments()
        .and_then(|mut segments| segments.next())
        .filter(|segment| !segment.is_empty())
        .unwrap_or("0")
        .parse::<u32>()
        .map_err(|error| format!("invalid redis db index: {error}"))?;
    let username = if redis.username().is_empty() {
        std::env::var("MC_REDIS_USERNAME").ok()
    } else {
        Some(redis.username().to_string())
    };
    let password = redis
        .password()
        .map(str::to_string)
        .or_else(|| std::env::var("MC_REDIS_PASSWORD").ok());
    Ok(RedisDebugTarget {
        host,
        port,
        db_index,
        username,
        password,
    })
}

fn open_redis_debug_stream(target: &RedisDebugTarget) -> Result<TcpStream, String> {
    let mut addrs = (target.host.as_str(), target.port)
        .to_socket_addrs()
        .map_err(|error| format!("resolve redis address failed: {error}"))?;
    let addr = addrs
        .next()
        .ok_or_else(|| "redis address resolution returned no entries".to_string())?;
    let mut stream = TcpStream::connect_timeout(&addr, REDIS_DEBUG_TIMEOUT)
        .map_err(|error| format!("connect redis failed: {error}"))?;
    stream
        .set_read_timeout(Some(REDIS_DEBUG_TIMEOUT))
        .map_err(|error| format!("set redis read timeout failed: {error}"))?;
    stream
        .set_write_timeout(Some(REDIS_DEBUG_TIMEOUT))
        .map_err(|error| format!("set redis write timeout failed: {error}"))?;
    if let Some(password) = target.password.as_deref() {
        let mut command = vec!["AUTH".to_string()];
        if let Some(username) = target.username.as_deref() {
            command.push(username.to_string());
        }
        command.push(password.to_string());
        redis_simple_command(&mut stream, &command).map_err(|error| error.to_string())?;
    }
    if target.db_index != 0 {
        redis_simple_command(
            &mut stream,
            &["SELECT".to_string(), target.db_index.to_string()],
        )
        .map_err(|error| error.to_string())?;
    }
    Ok(stream)
}

fn redis_get(stream: &mut TcpStream, key: &str) -> Result<Option<String>, String> {
    redis_command(stream, &["GET".to_string(), key.to_string()]).map_err(|error| error.to_string())
}

fn redis_simple_command(stream: &mut TcpStream, parts: &[String]) -> Result<(), io::Error> {
    let _ = redis_command(stream, parts)?;
    Ok(())
}

fn redis_command(stream: &mut TcpStream, parts: &[String]) -> Result<Option<String>, io::Error> {
    let mut request = format!("*{}\r\n", parts.len());
    for part in parts {
        request.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    stream.write_all(request.as_bytes())?;
    stream.flush()?;

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut prefix = [0u8; 1];
    reader.read_exact(&mut prefix)?;
    match prefix[0] {
        b'+' => {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            Ok(Some(line.trim_end_matches(['\r', '\n']).to_string()))
        }
        b'-' => {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            Err(io::Error::other(
                line.trim_end_matches(['\r', '\n']).to_string(),
            ))
        }
        b'$' => {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            let length = line
                .trim_end_matches(['\r', '\n'])
                .parse::<isize>()
                .map_err(|error| io::Error::other(format!("invalid bulk length: {error}")))?;
            if length < 0 {
                return Ok(None);
            }
            let mut buffer = vec![0u8; length as usize + 2];
            reader.read_exact(&mut buffer)?;
            buffer.truncate(length as usize);
            Ok(Some(String::from_utf8_lossy(&buffer).into_owned()))
        }
        other => Err(io::Error::other(format!(
            "unsupported redis reply prefix: {}",
            other as char
        ))),
    }
}

fn format_live_client_snapshot(live_clients: &[ClientLease]) -> Vec<String> {
    if live_clients.is_empty() {
        return vec!["(none)".to_string()];
    }
    live_clients
        .iter()
        .map(|lease| {
            let segment = lease
                .endpoints
                .segment_name
                .as_ref()
                .map(|name| name.0.as_str())
                .unwrap_or("-");
            let storage = lease
                .endpoints
                .labels
                .get("storage")
                .map(String::as_str)
                .unwrap_or("-");
            let route = lease
                .endpoints
                .labels
                .get("route")
                .map(String::as_str)
                .unwrap_or("-");
            let rpc = if lease.endpoints.rpc_address.is_empty() {
                "-"
            } else {
                lease.endpoints.rpc_address.as_str()
            };
            let labels = if lease.endpoints.labels.is_empty() {
                "-".to_string()
            } else {
                lease
                    .endpoints
                    .labels
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            format!(
                "runtime={} state={:?} storage={} route={} segment={} rpc={} labels={}",
                lease.runtime, lease.state, storage, route, segment, rpc, labels
            )
        })
        .collect()
}

pub struct ClientHandle {
    pub runtime: CompatRuntime,
}

struct RuntimeBuildSpec<'a> {
    role: &'a str,
    stable_id: String,
    segment: String,
    keyspace: Option<String>,
    routed_writes: bool,
    route_control: RouteControlMode,
    storage_label: &'a str,
}

pub struct BenchCluster {
    pub writers: Vec<ClientHandle>,
    pub readers: Vec<ClientHandle>,
}

impl BenchCluster {
    pub fn new(
        global: &GlobalArgs,
        writer_count: usize,
        reader_count: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let has_storage = global.storage_bytes > 0;
        let (keyspace, runtime_keyspace) = if global.keyspace.is_empty() {
            if has_storage {
                let generated = format!("mc/store-rs/bench/{}", unique_suffix());
                (MetadataKeyspace::new(generated.clone()), Some(generated))
            } else {
                (MetadataKeyspace::default(), None)
            }
        } else {
            (
                MetadataKeyspace::new(global.keyspace.clone()),
                Some(global.keyspace.clone()),
            )
        };
        let keyspace_prefix = keyspace.prefix().to_string();
        let metadata = Arc::new(RedisMetadataBackend::new(
            RedisMetadataConfig::new(global.metadata_url.clone())
                .keyspace(keyspace)
                .tenant(&global.tenant),
        )?);
        log_bench_startup_context(global, &keyspace_prefix, writer_count, reader_count);
        validate_storage_segment_naming_consistency(
            &metadata.list_live_clients()?,
            &keyspace_prefix,
            bench_transfer_metadata_debug_url(global),
        )
        .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;

        let storage_label = if has_storage { "true" } else { "false" };
        let route_control = match global.route_control {
            RouteControl::EmbeddedWrh => RouteControlMode::EmbeddedWrh,
            RouteControl::MetadataOnly => RouteControlMode::MetadataOnly,
        };
        if !has_storage {
            ensure_storage_candidates(
                metadata.as_ref(),
                &keyspace_prefix,
                bench_transfer_metadata_debug_url(global),
            )?;
        }

        let run_id = unique_suffix();
        let mut writers = Vec::with_capacity(writer_count);
        for i in 0..writer_count {
            let stable_id = bench_client_stable_id("writer", i, run_id);
            let segment = format!("{stable_id}-{}", unique_suffix());
            let runtime = build_runtime(
                global,
                RuntimeBuildSpec {
                    role: "writer",
                    stable_id,
                    segment,
                    keyspace: runtime_keyspace.clone(),
                    routed_writes: !has_storage,
                    route_control,
                    storage_label,
                },
            )?;
            runtime.client.register_local_memory()?;
            writers.push(ClientHandle { runtime });
        }

        let mut readers = Vec::with_capacity(reader_count);
        for i in 0..reader_count {
            let stable_id = bench_client_stable_id("reader", i, run_id);
            let segment = format!("{stable_id}-{}", unique_suffix());
            let runtime = build_runtime(
                global,
                RuntimeBuildSpec {
                    role: "reader",
                    stable_id,
                    segment,
                    keyspace: runtime_keyspace.clone(),
                    routed_writes: false,
                    route_control,
                    storage_label,
                },
            )?;
            runtime.client.register_local_memory()?;
            readers.push(ClientHandle { runtime });
        }

        Ok(Self { writers, readers })
    }

    pub fn heartbeat_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let expires_at = now_ms().saturating_add(LEASE_MS);
        for h in &mut self.writers {
            h.runtime.client.heartbeat(expires_at)?;
        }
        for h in &mut self.readers {
            h.runtime.client.heartbeat(expires_at)?;
        }
        Ok(())
    }

    pub fn writer(&self, index: usize) -> &StoreClient {
        &self.writers[index % self.writers.len()].runtime.client
    }

    pub fn reader(&self, index: usize) -> &StoreClient {
        &self.readers[index % self.readers.len()].runtime.client
    }
}

fn build_runtime(
    global: &GlobalArgs,
    spec: RuntimeBuildSpec<'_>,
) -> Result<CompatRuntime, Box<dyn std::error::Error>> {
    let mut labels = BTreeMap::new();
    labels.insert("role".to_string(), spec.role.to_string());
    labels.insert("storage".to_string(), spec.storage_label.to_string());

    // TE metadata defaults to the classic_te peer-handshake mode when
    // --transport-metadata-url is unset.
    let transport_metadata_url = global
        .transport_metadata_url
        .clone()
        .unwrap_or_else(|| "P2PHANDSHAKE".to_string());
    let setup = CompatSetupArgs {
        local_hostname: global.local_hostname.clone(),
        transport_metadata_url,
        metadata_url: global.metadata_url.clone(),
        global_segment_size: global.storage_bytes,
        local_buffer_size: global.scratch_bytes,
        eviction_high_watermark_percent: global.eviction_high_watermark_percent,
        eviction_low_watermark_percent: global.eviction_low_watermark_percent,
        protocol: protocol_name(&global.protocol).to_string(),
        _rdma_devices: String::new(),
        transport_rpc_port: None,
        transport_backend: Some(transport_backend_name(&global.transport_backend).to_string()),
        stable_id: Some(spec.stable_id),
        tenant: global.tenant.clone(),
        domain: None,
        object_set: None,
        labels,
        routed_writes: spec.routed_writes,
        replica_count: global.replica_count,
        route_topk: global.route_topk,
        keyspace: spec.keyspace,
        expires_at_ms: Some(now_ms().saturating_add(LEASE_MS)),
        use_hugepage: None,
        hugepage_size_bytes: None,
        timeouts: None,
    };
    Ok(CompatRuntimeArgs {
        setup,
        local_segment_name: Some(spec.segment),
        initial_state: ClientLifecycleState::Active,
        route_control: spec.route_control,
    }
    .build()?)
}

fn protocol_name(protocol: &Protocol) -> &'static str {
    match protocol {
        Protocol::Tcp => "tcp",
        Protocol::Rdma => "rdma",
    }
}

fn transport_backend_name(transport_backend: &TransportBackend) -> &'static str {
    match transport_backend {
        TransportBackend::ClassicTe => "classic_te",
        TransportBackend::Tent => "tent",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientRuntimeId, CompatibilityDescriptor, SegmentName,
    };

    fn publish_storage_lease(
        metadata: &InMemoryMetadataBackend,
        stable_id: &str,
        segment_name: &str,
        rpc_address: &str,
    ) {
        let mut labels = BTreeMap::new();
        labels.insert("storage".to_string(), "true".to_string());
        let template = ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(0)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet {
                rpc_address: rpc_address.to_string(),
                segment_name: Some(SegmentName::new(segment_name)),
                labels,
            },
            expires_at_ms: now_ms() + 60_000,
        };
        metadata
            .allocate_client_lease(&template)
            .expect("lease should publish");
    }

    #[test]
    fn bench_client_stable_id_contains_run_identity() {
        assert_eq!(
            bench_client_stable_id("writer", 3, 1777768097580),
            "bench-writer-3-1777768097580"
        );
        assert_eq!(
            bench_client_stable_id("reader", 7, 1777768097580),
            "bench-reader-7-1777768097580"
        );
    }

    #[test]
    fn formats_live_client_snapshot() {
        let mut endpoints = ClientEndpointSet {
            rpc_address: "127.0.0.1:17001".to_string(),
            ..ClientEndpointSet::default()
        };
        endpoints
            .labels
            .insert("storage".to_string(), "true".to_string());
        endpoints
            .labels
            .insert("route".to_string(), "true".to_string());
        let leases = vec![ClientLease {
            runtime: ClientRuntimeId::new("storage-a", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints,
            expires_at_ms: 1_000,
        }];

        assert_eq!(
            format_live_client_snapshot(&leases),
            vec!["runtime=storage-a:1 state=Active storage=true route=true segment=- rpc=127.0.0.1:17001 labels=route=true,storage=true".to_string()]
        );
        assert_eq!(format_live_client_snapshot(&[]), vec!["(none)".to_string()]);
    }

    #[test]
    fn redacts_url_credentials_for_logs() {
        assert_eq!(
            redact_url_for_log("redis://user:pass@redis.example:6379/0"),
            "redis://%3Credacted%3E:%3Credacted%3E@redis.example:6379/0"
        );
        assert_eq!(
            redact_url_for_log("redis://redis.example:6379/0"),
            "redis://redis.example:6379/0"
        );
    }

    #[test]
    fn transport_metadata_debug_url_prefers_explicit_transport_metadata_url() {
        let global = GlobalArgs {
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: Some("redis://127.0.0.1:6380/1".to_string()),
            keyspace: String::new(),
            protocol: Protocol::Tcp,
            transport_backend: TransportBackend::ClassicTe,
            local_hostname: "127.0.0.1".to_string(),
            storage_bytes: 0,
            scratch_bytes: 16 * 1024 * 1024,
            eviction_high_watermark_percent: None,
            eviction_low_watermark_percent: None,
            tenant: "bench".to_string(),
            trace_filter: None,
            metrics_addr: None,
            seed: 42,
            route_control: RouteControl::EmbeddedWrh,
            route_topk: 2,
            replica_count: 1,
        };

        assert_eq!(
            bench_transfer_metadata_debug_url(&global),
            "redis://127.0.0.1:6380/1"
        );
    }

    #[test]
    fn transport_metadata_debug_url_defaults_to_p2phandshake() {
        let global = GlobalArgs {
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            keyspace: String::new(),
            protocol: Protocol::Tcp,
            transport_backend: TransportBackend::ClassicTe,
            local_hostname: "127.0.0.1".to_string(),
            storage_bytes: 0,
            scratch_bytes: 16 * 1024 * 1024,
            eviction_high_watermark_percent: None,
            eviction_low_watermark_percent: None,
            tenant: "bench".to_string(),
            trace_filter: None,
            metrics_addr: None,
            seed: 42,
            route_control: RouteControl::EmbeddedWrh,
            route_topk: 2,
            replica_count: 1,
        };

        assert_eq!(bench_transfer_metadata_debug_url(&global), "P2PHANDSHAKE");
    }

    #[test]
    fn ensure_storage_candidates_rejects_mixed_segment_naming_modes() {
        let metadata = InMemoryMetadataBackend::new();
        publish_storage_lease(&metadata, "storage-a", "segment-a", "192.168.1.10:17001");
        publish_storage_lease(
            &metadata,
            "storage-b",
            "192.168.1.11:17002",
            "192.168.1.11:17002",
        );

        let error = ensure_storage_candidates(&metadata, "mc/store-rs/test", "P2PHANDSHAKE")
            .expect_err("mixed naming should hard-fail")
            .to_string();

        assert!(error.contains("mixed segment naming modes"));
    }

    #[test]
    fn ensure_storage_candidates_rejects_cluster_mode_mismatch_for_p2p() {
        let metadata = InMemoryMetadataBackend::new();
        publish_storage_lease(&metadata, "storage-a", "segment-a", "192.168.1.10:17001");

        let error = ensure_storage_candidates(&metadata, "mc/store-rs/test", "P2PHANDSHAKE")
            .expect_err("named segments should not satisfy p2p bench startup")
            .to_string();

        assert!(error.contains("expects P2P-handshake rpc-address segment names"));
    }

    #[test]
    fn ensure_storage_candidates_accepts_uniform_p2p_segment_naming() {
        let metadata = InMemoryMetadataBackend::new();
        publish_storage_lease(
            &metadata,
            "storage-a",
            "192.168.1.10:17001",
            "192.168.1.10:17001",
        );

        ensure_storage_candidates(&metadata, "mc/store-rs/test", "P2PHANDSHAKE")
            .expect("uniform p2p naming should pass");
    }

    #[test]
    fn ensure_storage_candidates_accepts_uniform_logical_segment_naming() {
        let metadata = InMemoryMetadataBackend::new();
        publish_storage_lease(&metadata, "storage-a", "segment-a", "192.168.1.10:17001");

        ensure_storage_candidates(&metadata, "mc/store-rs/test", "redis://127.0.0.1:6379/0")
            .expect("uniform logical naming should pass");
    }
}
