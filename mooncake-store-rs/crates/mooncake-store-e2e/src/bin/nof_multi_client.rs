use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::c_void;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{
    init_tracing_from_env, ColdTierOffloadMode, ExtentStoreExecutor, ExtentStoreExecutorConfig,
    GetRequest, LocalMemoryConfig, MooncakeCompatibilityFacade, NofBackend, NofTargetConfig,
    PutRequest, RouteControlMode, SpdkNofBlockDevice, SpdkNofBlockDeviceConfig, StoreClientBuilder,
    StoreTransport,
};
use mooncake_store_core::{ClientLifecycleState, ColdBackingState, Result, StoreError};
use mooncake_transport::{
    SegmentBuffer, SegmentInfo, SegmentKind, TransferProgress, TransferRequest,
};
use redis::Commands;

struct NoopTransport {
    segment_name: String,
    rpc_host: String,
    rpc_port: u16,
    allocations: Mutex<BTreeMap<usize, Box<[u8]>>>,
}

impl StoreTransport for NoopTransport {
    fn segment_name(&self) -> Result<String> {
        Ok(self.segment_name.clone())
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        Ok((self.rpc_host.clone(), self.rpc_port))
    }

    fn open_segment(&self, _segment_name: &str) -> Result<u64> {
        Ok(1)
    }

    fn close_segment(&self, _handle: u64) -> Result<()> {
        Ok(())
    }

    fn get_segment_info(&self, _handle: u64) -> Result<SegmentInfo> {
        let allocations = self.allocations.lock().expect("allocation mutex poisoned");
        Ok(SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: allocations
                .values()
                .map(|buffer| SegmentBuffer {
                    base: buffer.as_ptr() as u64,
                    length: buffer.len() as u64,
                    location: "cpu:0".to_string(),
                })
                .collect(),
        })
    }

    fn adopt_local_memory(&self, _addr: *mut c_void, _size: usize, _location: &str) -> Result<()> {
        Ok(())
    }

    fn allocate_memory(&self, size: usize, _location: &str) -> Result<*mut c_void> {
        let mut allocation = vec![0u8; size].into_boxed_slice();
        let address = allocation.as_mut_ptr() as usize;
        self.allocations
            .lock()
            .expect("allocation mutex poisoned")
            .insert(address, allocation);
        Ok(address as *mut c_void)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        self.allocations
            .lock()
            .expect("allocation mutex poisoned")
            .remove(&(addr as usize));
        Ok(())
    }

    fn register_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
        Ok(())
    }

    fn unregister_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
        Ok(())
    }

    fn allocate_batch(&self, _batch_size: usize) -> Result<u64> {
        Ok(1)
    }

    fn free_batch(&self, _batch_id: u64) -> Result<()> {
        Ok(())
    }

    fn submit(&self, _batch_id: u64, _requests: &[TransferRequest]) -> Result<()> {
        Ok(())
    }

    fn task_status(&self, _batch_id: u64, _task_id: usize) -> Result<TransferProgress> {
        Ok(TransferProgress {
            status: mooncake_transport::TransferStatus::Completed,
            transferred_bytes: 0,
        })
    }

    fn overall_status(&self, _batch_id: u64) -> Result<TransferProgress> {
        Ok(TransferProgress {
            status: mooncake_transport::TransferStatus::Completed,
            transferred_bytes: 0,
        })
    }
}

const DEFAULT_TARGETS: &str =
    "127.0.0.1|127.0.0.1|nof-local|nqn.2026-09.io.mooncake:nof-local|4420";
const DEFAULT_CLIENT_IDS: &str = "client-0,client-1,client-2,client-3";
const DEFAULT_HOST_NQN: &str = "nqn.2026-09.io.mooncake:client-dev-5";
const DEFAULT_DEVICE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_OBJECTS_PER_CLIENT: usize = 16;
const DEFAULT_VALUE_BYTES: usize = 64 * 1024;
const DEFAULT_REPLICA_COUNT: usize = 2;
const DEFAULT_SUBMIT_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_BARRIER_TIMEOUT_SECONDS: u64 = 120;
const DEFAULT_READ_RETRIES: usize = 20;
const DEFAULT_READ_RETRY_DELAY_MS: u64 = 250;
const DEFAULT_BATCH_SIZE: usize = 32;

#[derive(Debug)]
struct TargetSpec {
    traddr: String,
    target_id: String,
    subnqn: String,
    port: String,
}

#[derive(Debug)]
struct TestConfig {
    targets: Vec<TargetSpec>,
    client_ids: Vec<String>,
    host_nqn: String,
    device_bytes: u64,
    objects_per_client: usize,
    value_bytes: usize,
    batch_size: usize,
    replica_count: usize,
    submit_chunk_bytes: u64,
    test_prefix: String,
    redis_url: String,
    keyspace: String,
    bind_ip: String,
    barrier_dir: PathBuf,
    barrier_redis_url: Option<String>,
    barrier_timeout: Duration,
    read_retries: usize,
    read_retry_delay: Duration,
    startup_settle: Duration,
    route_control: RouteControlMode,
}

fn invalid(message: impl Into<String>) -> StoreError {
    StoreError::InvalidState(message.into())
}

fn parse_env<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|error| invalid(format!("{name} must be valid: {error}"))),
        Err(_) => Ok(default),
    }
}

fn parse_targets(raw: &str) -> Result<Vec<TargetSpec>> {
    let mut targets = Vec::new();
    let mut ids = BTreeSet::new();
    for (index, item) in raw.split(',').enumerate() {
        let fields = item.split('|').map(str::trim).collect::<Vec<_>>();
        let (traddr, target_id, subnqn, port) = match fields.as_slice() {
            [_public_host, traddr, target_id, subnqn, port] => {
                (*traddr, *target_id, *subnqn, *port)
            }
            [target_id, traddr, subnqn, port] => (*traddr, *target_id, *subnqn, *port),
            _ => {
                return Err(invalid(format!(
                    "NOF_TARGETS entry {index} must be public_ip|traddr|target_id|subnqn|port"
                )))
            }
        };
        if [traddr, target_id, subnqn, port]
            .iter()
            .any(|field| field.is_empty())
        {
            return Err(invalid(format!(
                "NOF_TARGETS entry {index} contains an empty field"
            )));
        }
        if !ids.insert(target_id.to_string()) {
            return Err(invalid(format!("duplicate NoF target id {target_id}")));
        }
        targets.push(TargetSpec {
            traddr: traddr.to_string(),
            target_id: target_id.to_string(),
            subnqn: subnqn.to_string(),
            port: port.to_string(),
        });
    }
    if targets.is_empty() {
        return Err(invalid("NOF_TARGETS must contain at least one target"));
    }
    Ok(targets)
}

fn parse_client_ids(raw: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut seen = BTreeSet::new();
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        if !seen.insert(item.to_string()) {
            return Err(invalid(format!("duplicate NoF client id {item}")));
        }
        ids.push(item.to_string());
    }
    if ids.is_empty() {
        return Err(invalid("NOF_CLIENT_IDS must contain at least one client"));
    }
    Ok(ids)
}

impl TestConfig {
    fn from_env() -> Result<Self> {
        let targets =
            parse_targets(&env::var("NOF_TARGETS").unwrap_or_else(|_| DEFAULT_TARGETS.into()))?;
        let client_ids = parse_client_ids(
            &env::var("NOF_CLIENT_IDS").unwrap_or_else(|_| DEFAULT_CLIENT_IDS.into()),
        )?;
        let objects_per_client = parse_env("NOF_OBJECTS_PER_CLIENT", DEFAULT_OBJECTS_PER_CLIENT)?;
        let value_bytes = parse_env("NOF_VALUE_BYTES", DEFAULT_VALUE_BYTES)?;
        let batch_size = parse_env("NOF_BATCH_SIZE", DEFAULT_BATCH_SIZE)?;
        if objects_per_client == 0 || value_bytes == 0 || batch_size == 0 {
            return Err(invalid(
                "NOF_OBJECTS_PER_CLIENT, NOF_VALUE_BYTES, and NOF_BATCH_SIZE must be non-zero",
            ));
        }
        for client_id in &client_ids {
            let marker = format!("{client_id}:{}:", objects_per_client - 1);
            if marker.len() > value_bytes {
                return Err(invalid(format!(
                    "NOF_VALUE_BYTES={value_bytes} is smaller than marker for client {client_id}"
                )));
            }
        }
        let replica_count = parse_env("NOF_REPLICA_COUNT", DEFAULT_REPLICA_COUNT)?;
        if replica_count == 0 || replica_count > targets.len() {
            return Err(invalid(format!(
                "NOF_REPLICA_COUNT={replica_count} must be between 1 and {}",
                targets.len()
            )));
        }
        let route_control = match env::var("NOF_ROUTE_CONTROL").as_deref() {
            Ok("MetadataOnly") => RouteControlMode::MetadataOnly,
            Ok("EmbeddedWrh") | Err(_) => RouteControlMode::EmbeddedWrh,
            Ok(value) => {
                return Err(invalid(format!(
                    "NOF_ROUTE_CONTROL must be MetadataOnly or EmbeddedWrh, got {value}"
                )))
            }
        };
        Ok(Self {
            targets,
            client_ids,
            host_nqn: env::var("NOF_HOST_NQN").unwrap_or_else(|_| DEFAULT_HOST_NQN.into()),
            device_bytes: parse_env("NOF_DEVICE_BYTES", DEFAULT_DEVICE_BYTES)?,
            objects_per_client,
            value_bytes,
            batch_size,
            replica_count,
            submit_chunk_bytes: parse_env(
                "NOF_SUBMIT_CHUNK_BYTES",
                DEFAULT_SUBMIT_CHUNK_BYTES as u64,
            )?,
            test_prefix: env::var("NOF_TEST_PREFIX")
                .unwrap_or_else(|_| "nof-multi-client".to_string()),
            redis_url: env::var("NOF_REDIS_URL")
                .map_err(|_| invalid("NOF_REDIS_URL is required"))?,
            keyspace: env::var("NOF_KEYSPACE").map_err(|_| invalid("NOF_KEYSPACE is required"))?,
            bind_ip: env::var("NOF_BIND_IP").map_err(|_| invalid("NOF_BIND_IP is required"))?,
            barrier_dir: env::var("NOF_BARRIER_DIR")
                .map(PathBuf::from)
                .map_err(|_| invalid("NOF_BARRIER_DIR is required"))?,
            barrier_redis_url: env::var("NOF_BARRIER_REDIS_URL").ok(),
            barrier_timeout: Duration::from_secs(parse_env(
                "NOF_BARRIER_TIMEOUT_SECONDS",
                DEFAULT_BARRIER_TIMEOUT_SECONDS,
            )?),
            read_retries: parse_env("NOF_READ_RETRIES", DEFAULT_READ_RETRIES)?,
            read_retry_delay: Duration::from_millis(parse_env(
                "NOF_READ_RETRY_DELAY_MS",
                DEFAULT_READ_RETRY_DELAY_MS,
            )?),
            startup_settle: Duration::from_secs(parse_env("NOF_STARTUP_SETTLE_SECONDS", 2u64)?),
            route_control,
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

fn value(client_id: &str, index: usize, value_bytes: usize) -> Vec<u8> {
    let mut value = vec![0u8; value_bytes];
    let marker = format!("{client_id}:{index}:");
    value[..marker.len()].copy_from_slice(marker.as_bytes());
    for (offset, byte) in value[marker.len()..].iter_mut().enumerate() {
        *byte = (offset as u8).wrapping_add(index as u8);
    }
    value
}

fn logical_mib_per_sec(bytes: usize, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    bytes as f64 / (1024.0 * 1024.0) / seconds
}

fn barrier_key(config: &TestConfig, name: &str) -> String {
    format!("{}:barrier:{name}", config.keyspace)
}

fn redis_connection(config: &TestConfig) -> Result<Option<redis::Connection>> {
    let Some(url) = config.barrier_redis_url.as_ref() else {
        return Ok(None);
    };
    let client = redis::Client::open(url.as_str()).map_err(|error| {
        StoreError::Transport(format!("failed to open NoF barrier Redis {url}: {error}"))
    })?;
    client.get_connection().map(Some).map_err(|error| {
        StoreError::Transport(format!(
            "failed to connect NoF barrier Redis {url}: {error}"
        ))
    })
}

fn wait_for_phase(config: &TestConfig, client_id: &str, phase: &str) -> Result<()> {
    if let Some(mut connection) = redis_connection(config)? {
        let _: () = connection
            .set(
                barrier_key(config, &format!("{phase}-{client_id}.ready")),
                client_id,
            )
            .map_err(|error| {
                StoreError::Transport(format!(
                    "failed to publish NoF {phase} Redis barrier for {client_id}: {error}"
                ))
            })?;
        let deadline = Instant::now() + config.barrier_timeout;
        while Instant::now() < deadline {
            let all_ready = config.client_ids.iter().try_fold(true, |ready, client| {
                let exists: bool = connection
                    .exists(barrier_key(config, &format!("{phase}-{client}.ready")))
                    .map_err(|error| {
                        StoreError::Transport(format!(
                            "failed to read NoF {phase} Redis barrier for {client}: {error}"
                        ))
                    })?;
                Ok::<_, StoreError>(ready && exists)
            })?;
            if all_ready {
                return Ok(());
            }
            sleep(Duration::from_millis(100));
        }
        return Err(StoreError::Transport(format!(
            "timed out waiting for {phase} Redis barrier"
        )));
    }

    std::fs::create_dir_all(&config.barrier_dir).map_err(|error| {
        mooncake_store_core::StoreError::Transport(format!(
            "failed to create NoF test barrier {}: {error}",
            config.barrier_dir.display()
        ))
    })?;
    let ready_file = config
        .barrier_dir
        .join(format!("{phase}-{client_id}.ready"));
    std::fs::write(&ready_file, client_id.as_bytes()).map_err(|error| {
        mooncake_store_core::StoreError::Transport(format!(
            "failed to publish NoF {phase} barrier for {client_id}: {error}"
        ))
    })?;
    let deadline = Instant::now() + config.barrier_timeout;
    while Instant::now() < deadline {
        if config.client_ids.iter().all(|client| {
            config
                .barrier_dir
                .join(format!("{phase}-{client}.ready"))
                .is_file()
        }) {
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }
    Err(mooncake_store_core::StoreError::Transport(format!(
        "timed out waiting for {phase} barrier in {}",
        config.barrier_dir.display()
    )))
}

fn signal_phase_done(config: &TestConfig, phase: &str) -> Result<()> {
    if let Some(mut connection) = redis_connection(config)? {
        let _: () = connection
            .set(barrier_key(config, &format!("{phase}.done")), now_ms())
            .map_err(|error| {
                StoreError::Transport(format!(
                    "failed to publish NoF {phase} Redis completion barrier: {error}"
                ))
            })?;
        return Ok(());
    }

    std::fs::write(
        config.barrier_dir.join(format!("{phase}.done")),
        now_ms().to_string(),
    )
    .map_err(|error| {
        mooncake_store_core::StoreError::Transport(format!(
            "failed to publish NoF {phase} completion barrier: {error}"
        ))
    })
}

fn phase_done_timestamp(config: &TestConfig, phase: &str) -> Result<u64> {
    if let Some(mut connection) = redis_connection(config)? {
        let value: Option<u64> = connection
            .get(barrier_key(config, &format!("{phase}.done")))
            .map_err(|error| {
                StoreError::Transport(format!(
                    "failed to read NoF {phase} Redis completion barrier: {error}"
                ))
            })?;
        return value.ok_or_else(|| {
            StoreError::Transport(format!("NoF {phase} Redis completion barrier is missing"))
        });
    }

    let path = config.barrier_dir.join(format!("{phase}.done"));
    let contents = std::fs::read_to_string(&path).map_err(|error| {
        mooncake_store_core::StoreError::Transport(format!(
            "failed to read NoF {phase} completion barrier {}: {error}",
            path.display()
        ))
    })?;
    contents.trim().parse::<u64>().map_err(|error| {
        mooncake_store_core::StoreError::Transport(format!(
            "invalid NoF {phase} completion timestamp in {}: {error}",
            path.display()
        ))
    })
}

fn wait_for_phase_done(config: &TestConfig, phase: &str) -> Result<u64> {
    let deadline = Instant::now() + config.barrier_timeout;
    while Instant::now() < deadline {
        if let Some(mut connection) = redis_connection(config)? {
            let exists: bool = connection
                .exists(barrier_key(config, &format!("{phase}.done")))
                .map_err(|error| {
                    StoreError::Transport(format!(
                        "failed to read NoF {phase} Redis completion barrier: {error}"
                    ))
                })?;
            if exists {
                return phase_done_timestamp(config, phase);
            }
            sleep(Duration::from_millis(100));
            continue;
        }
        if config.barrier_dir.join(format!("{phase}.done")).is_file() {
            return phase_done_timestamp(config, phase);
        }
        sleep(Duration::from_millis(100));
    }
    Err(mooncake_store_core::StoreError::Transport(format!(
        "timed out waiting for {phase} completion barrier in {}",
        config.barrier_dir.display()
    )))
}

fn wait_for_cold_only_routes(
    config: &TestConfig,
    client: &mooncake_store_client::StoreClient,
) -> Result<()> {
    let deadline = Instant::now() + config.barrier_timeout;
    let expected = config
        .client_ids
        .iter()
        .flat_map(|owner| {
            (0..config.objects_per_client)
                .map(move |index| format!("{}/{owner}/{index}", config.test_prefix))
        })
        .collect::<Vec<_>>();

    loop {
        let mut pending = Vec::new();
        for key in &expected {
            let route = client.query_route(key)?;
            let ready = route.as_ref().is_some_and(|route| {
                route.replicas.is_empty()
                    && route
                        .nof_backing
                        .as_ref()
                        .is_some_and(|backing| backing.state == ColdBackingState::Materialized)
            });
            if !ready {
                pending.push(key.clone());
            }
        }
        if pending.is_empty() {
            println!(
                "{}: all {} routes converged to Managed NoF cold-only state",
                env::var("NOF_CLIENT_ID").unwrap_or_else(|_| "client".to_string()),
                expected.len(),
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(StoreError::Transport(format!(
                "timed out waiting for {} routes to become Managed NoF cold-only; first pending keys: {:?}",
                pending.len(),
                pending.into_iter().take(8).collect::<Vec<_>>(),
            )));
        }
        sleep(Duration::from_millis(100));
    }
}

fn wait_for_all_phase_done(config: &TestConfig, phase: &str) -> Result<u64> {
    let deadline = Instant::now() + config.barrier_timeout;
    while Instant::now() < deadline {
        if let Some(mut connection) = redis_connection(config)? {
            let mut latest = 0;
            let mut all_done = true;
            for client in &config.client_ids {
                let client_phase = format!("{phase}-{client}");
                let key = barrier_key(config, &format!("{client_phase}.done"));
                let value: Option<u64> = connection.get(key).map_err(|error| {
                    StoreError::Transport(format!(
                        "failed to read NoF {client_phase} Redis completion barrier: {error}"
                    ))
                })?;
                match value {
                    Some(timestamp) => latest = latest.max(timestamp),
                    None => {
                        all_done = false;
                        break;
                    }
                }
            }
            if all_done {
                return Ok(latest);
            }
            sleep(Duration::from_millis(100));
            continue;
        }
        if config.client_ids.iter().all(|client| {
            config
                .barrier_dir
                .join(format!("{phase}-{client}.done"))
                .is_file()
        }) {
            let mut latest = 0;
            for client in &config.client_ids {
                let client_phase = format!("{phase}-{client}");
                latest = latest.max(phase_done_timestamp(config, &client_phase)?);
            }
            return Ok(latest);
        }
        sleep(Duration::from_millis(100));
    }
    Err(mooncake_store_core::StoreError::Transport(format!(
        "timed out waiting for all {phase} completion barriers in {}",
        config.barrier_dir.display()
    )))
}

fn read_and_verify_all(
    config: &TestConfig,
    client: &mooncake_store_client::StoreClient,
    client_id: &str,
    all_objects: &BTreeMap<String, Vec<u8>>,
) -> Result<usize> {
    let mut read = 0usize;
    for batch_start in (0..all_objects.len()).step_by(config.batch_size) {
        let entries = all_objects
            .iter()
            .skip(batch_start)
            .take(config.batch_size)
            .collect::<Vec<_>>();
        let mut last_error = None;
        for _ in 0..config.read_retries {
            let mut buffers = entries
                .iter()
                .map(|(_, expected)| vec![0u8; expected.len()])
                .collect::<Vec<_>>();
            let mut requests = entries
                .iter()
                .zip(buffers.iter_mut())
                .map(|((key, _), buffer)| GetRequest::new(key.as_str(), buffer.as_mut_slice()))
                .collect::<Vec<_>>();
            match client.batch_get_into(&mut requests) {
                Ok(sizes) => {
                    if sizes.len() != entries.len() {
                        return Err(mooncake_store_core::StoreError::Transport(format!(
                            "{client_id}: batch_get_into returned {} sizes for {} requests",
                            sizes.len(),
                            entries.len()
                        )));
                    }
                    for (index, ((key, expected), size)) in
                        entries.iter().zip(sizes.iter()).enumerate()
                    {
                        if *size != expected.len()
                            || buffers[index].as_slice() != expected.as_slice()
                        {
                            return Err(mooncake_store_core::StoreError::Transport(format!(
                                "{client_id}: data mismatch for {key}: got {} bytes, expected {}",
                                size,
                                expected.len()
                            )));
                        }
                    }
                    read += entries.len();
                    last_error = None;
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    sleep(config.read_retry_delay);
                }
            }
        }
        if let Some(error) = last_error {
            return Err(mooncake_store_core::StoreError::Transport(format!(
                "{client_id}: failed to batch-read {} objects after {} retries: {error}",
                entries.len(),
                config.read_retries
            )));
        }
    }
    Ok(read)
}

fn build_client(
    config: &TestConfig,
    client_id: &str,
    rpc_port: u16,
) -> Result<mooncake_store_client::StoreClient> {
    let metadata = Arc::new(RedisMetadataBackend::new(
        RedisMetadataConfig::new(config.redis_url.clone())
            .keyspace(MetadataKeyspace::new(config.keyspace.clone())),
    )?);
    let mut targets = Vec::with_capacity(config.targets.len());
    for target in &config.targets {
        let mut device_config =
            SpdkNofBlockDeviceConfig::tcp(&target.traddr, &target.port, &target.subnqn, 1);
        device_config.hostnqn = Some(config.host_nqn.clone());
        device_config.no_huge = true;
        device_config.submit_chunk_bytes = config.submit_chunk_bytes;
        let device = Arc::new(SpdkNofBlockDevice::connect(device_config)?);
        let executor = Arc::new(ExtentStoreExecutor::new(
            device,
            ExtentStoreExecutorConfig::new(0, config.device_bytes),
        )?);
        targets.push(NofTargetConfig::new(
            target.target_id.clone(),
            NofBackend::new(executor)?,
        )?);
    }
    let transport = Arc::new(NoopTransport {
        segment_name: format!("{}-{client_id}-segment", config.test_prefix),
        rpc_host: config.bind_ip.clone(),
        rpc_port,
        allocations: Mutex::new(BTreeMap::new()),
    });
    let local_memory = LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(16 * 1024 * 1024)
        .scratch_bytes(16 * 1024 * 1024)
        .alignment(1)
        .reclaim_grace_ms(0)
        .eviction_poll_interval(Duration::ZERO);
    let client = StoreClientBuilder::new(metadata, client_id.to_string())
        .state(ClientLifecycleState::Active)
        .tenant("default")
        .label("storage", "true")
        .transport(transport)
        .local_memory(local_memory)
        .nof_targets(targets)
        .nof_replica_count(config.replica_count)
        .cold_tier_offload_mode(ColdTierOffloadMode::EvictTriggered)
        .route_control(config.route_control)
        .build(now_ms() + 600_000)?;
    client.register_local_memory()?;
    Ok(client)
}

fn run() -> Result<()> {
    let _ = init_tracing_from_env("MC_STORE_RS_TRACE", "MC_STORE_RS_TRACE_FILTER");
    let config = TestConfig::from_env()?;
    let client_id = env::var("NOF_CLIENT_ID").map_err(|_| invalid("NOF_CLIENT_ID is required"))?;
    if !config
        .client_ids
        .iter()
        .any(|candidate| candidate == &client_id)
    {
        return Err(invalid(format!(
            "NOF_CLIENT_ID={client_id} is not in NOF_CLIENT_IDS={:?}",
            config.client_ids
        )));
    }
    let rpc_port = env::var("NOF_RPC_PORT")
        .map_err(|_| invalid("NOF_RPC_PORT is required"))?
        .parse::<u16>()
        .map_err(|error| invalid(format!("NOF_RPC_PORT must be a port: {error}")))?;
    println!(
        "{client_id}: connecting to {} NoF targets; {} clients participate; batch_size={}",
        config.targets.len(),
        config.client_ids.len(),
        config.batch_size
    );
    let client = build_client(&config, &client_id, rpc_port)?;
    println!(
        "{client_id}: runtime={} local memory registered; waiting for all clients",
        client.runtime_id()
    );
    wait_for_phase(&config, &client_id, "registered")?;
    if !config.startup_settle.is_zero() {
        sleep(config.startup_settle);
    }
    println!("{client_id}: all clients registered; starting writes");

    let write_started = Instant::now();
    for batch_start in (0..config.objects_per_client).step_by(config.batch_size) {
        let batch = (batch_start..(batch_start + config.batch_size).min(config.objects_per_client))
            .map(|index| {
                (
                    format!("{}/{client_id}/{index}", config.test_prefix),
                    value(&client_id, index, config.value_bytes),
                )
            })
            .collect::<Vec<_>>();
        let requests = batch
            .iter()
            .map(|(key, payload)| PutRequest::new(key.as_str(), payload.as_slice()))
            .collect::<Vec<_>>();
        let routes = client.batch_put(&requests)?;
        if routes.len() != requests.len() {
            return Err(mooncake_store_core::StoreError::Transport(format!(
                "{client_id}: batch_put returned {} routes for {} requests",
                routes.len(),
                requests.len()
            )));
        }
        if batch_start == 0 {
            let route = routes.first().ok_or_else(|| {
                mooncake_store_core::StoreError::Transport(
                    "batch_put returned no route for the first request".to_string(),
                )
            })?;
            let placements = route
                .replicas
                .iter()
                .map(|replica| format!("{}:{}", replica.owner, replica.segment_name.0))
                .collect::<Vec<_>>();
            println!("{client_id}: first hot route placements={placements:?}");
        }
    }
    let hot_write_elapsed = write_started.elapsed();
    println!(
        "{client_id}: batch-wrote {} objects; batch_size={}; hot_write_elapsed_ms={} logical_mib_per_sec={:.2}",
        config.objects_per_client,
        config.batch_size,
        hot_write_elapsed.as_millis(),
        logical_mib_per_sec(
            config.objects_per_client * config.value_bytes,
            hot_write_elapsed,
        ),
    );
    let offload_started = Instant::now();
    let evicted = client.debug_evict_all()?;
    if !evicted.completed
        || evicted.remaining_hot_replicas != 0
        || evicted.dropped_without_cold != 0
    {
        return Err(mooncake_store_core::StoreError::Transport(format!(
            "{client_id}: debug eviction did not preserve all local replicas: {evicted:?}"
        )));
    }
    println!(
        "{client_id}: evicted {} local objects to Managed NoF; offload_elapsed_ms={} logical_mib_per_sec={:.2}",
        evicted.evicted,
        offload_started.elapsed().as_millis(),
        logical_mib_per_sec(
            config.objects_per_client * config.value_bytes,
            offload_started.elapsed()
        ),
    );
    wait_for_phase(&config, &client_id, "offloaded")?;
    println!("{client_id}: all clients completed write/offload phase");
    wait_for_cold_only_routes(&config, &client)?;

    let all_objects = config
        .client_ids
        .iter()
        .flat_map(|owner| (0..config.objects_per_client).map(move |index| (owner, index)))
        .map(|(owner, index)| {
            (
                format!("{}/{owner}/{index}", config.test_prefix),
                value(owner, index, config.value_bytes),
            )
        })
        .collect::<BTreeMap<_, _>>();

    // Every client issues batch_get_into concurrently. This matches the production batch-get
    // path: one API call per request batch, with the client grouping cold reads by owner and
    // dispatching those owner batches in parallel.
    wait_for_phase(&config, &client_id, "read-ready")?;
    if client_id == config.client_ids[0] {
        signal_phase_done(&config, "read-start")?;
    }
    let read_start_ms = wait_for_phase_done(&config, "read-start")?;
    let read_started = Instant::now();
    let read = read_and_verify_all(&config, &client, &client_id, &all_objects)?;
    let cold_read_elapsed = read_started.elapsed();
    println!(
        "{client_id}: parallel batch-read and verified {read}/{} objects; batch_size={}; cold_read_elapsed_ms={} logical_mib_per_sec={:.2}",
        all_objects.len(),
        config.batch_size,
        cold_read_elapsed.as_millis(),
        logical_mib_per_sec(all_objects.len() * config.value_bytes, cold_read_elapsed),
    );
    signal_phase_done(&config, &format!("read-done-{client_id}"))?;
    let read_end_ms = wait_for_all_phase_done(&config, "read-done")?;
    if client_id == config.client_ids[0] {
        let aggregate_bytes = config
            .client_ids
            .len()
            .saturating_mul(all_objects.len())
            .saturating_mul(config.value_bytes);
        let aggregate_elapsed = Duration::from_millis(read_end_ms.saturating_sub(read_start_ms));
        println!(
            "parallel batch-get aggregate: clients={}; logical_bytes={}; elapsed_ms={}; logical_mib_per_sec={:.2}",
            config.client_ids.len(),
            aggregate_bytes,
            aggregate_elapsed.as_millis(),
            logical_mib_per_sec(aggregate_bytes, aggregate_elapsed),
        );
    }

    let evicted = client.debug_evict_all()?;
    if !evicted.completed
        || evicted.remaining_hot_replicas != 0
        || evicted.dropped_without_cold != 0
    {
        return Err(mooncake_store_core::StoreError::Transport(format!(
            "{client_id}: post-read eviction did not preserve all replicas: {evicted:?}"
        )));
    }
    Ok(())
}

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            // Let StoreClient drop and join SPDK workers before DPDK cleanup unmaps qpair memory.
            std::process::ExitCode::FAILURE
        }
    }
}
