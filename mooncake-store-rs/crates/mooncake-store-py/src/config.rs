use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_metadata::{
    resolve_redis_auth, EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace,
    RedisMetadataBackend, RedisMetadataConfig,
};
use mooncake_store_core::{MetadataBackend, Result, StoreError};
use mooncake_transport::{ClassicEngineConfig, ClassicTransportProtocol, TentEngineConfig};
use url::Url;

pub const DEFAULT_COMPAT_LEASE_TTL_MS: u64 = 30_000;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(65);
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_TRANSFER_STALL_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_DUMMY_RPC_TIMEOUT: Duration = Duration::from_secs(65);
const DEFAULT_REGISTRATION_TIMEOUT_FLOOR: Duration = Duration::from_secs(20);
const STARTUP_TIMEOUT_BYTES_PER_SECOND: u64 = 1024 * 1024 * 1024;
const REQUEST_TIMEOUT_ENV: &str = "MC_STORE_RS_REQUEST_TIMEOUT_MS";
const STARTUP_TIMEOUT_ENV: &str = "MC_STORE_RS_STARTUP_TIMEOUT_MS";
const HEARTBEAT_TIMEOUT_ENV: &str = "MC_STORE_RS_HEARTBEAT_TIMEOUT_MS";
const TRANSFER_STALL_TIMEOUT_ENV: &str = "MC_STORE_RS_TRANSFER_STALL_TIMEOUT_MS";
const LEGACY_TRANSFER_TIMEOUT_ENV: &str = "MC_STORE_RS_TRANSFER_TIMEOUT_MS";
const DUMMY_RPC_TIMEOUT_ENV: &str = "MC_STORE_RS_DUMMY_RPC_TIMEOUT_MS";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompatTimeoutCliOverrides {
    pub request_timeout_ms: Option<u64>,
    pub startup_timeout_ms: Option<u64>,
    pub heartbeat_timeout_ms: Option<u64>,
    pub transfer_stall_timeout_ms: Option<u64>,
    pub dummy_rpc_timeout_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompatTimeoutConfig {
    pub request_timeout: Duration,
    pub startup_timeout_override: Option<Duration>,
    pub heartbeat_timeout: Duration,
    pub transfer_stall_timeout: Duration,
    pub dummy_rpc_timeout: Duration,
}

impl CompatTimeoutConfig {
    pub fn default_registration_timeout_for_bytes(registration_bytes: u64) -> Duration {
        adaptive_registration_timeout(registration_bytes)
    }

    pub fn from_env() -> Self {
        let request_timeout =
            duration_from_env_ms(&[REQUEST_TIMEOUT_ENV]).unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        let startup_timeout_override = duration_from_env_ms(&[STARTUP_TIMEOUT_ENV]);
        let heartbeat_timeout =
            duration_from_env_ms(&[HEARTBEAT_TIMEOUT_ENV]).unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT);
        let transfer_stall_timeout =
            duration_from_env_ms(&[TRANSFER_STALL_TIMEOUT_ENV, LEGACY_TRANSFER_TIMEOUT_ENV])
                .unwrap_or(DEFAULT_TRANSFER_STALL_TIMEOUT);
        let dummy_rpc_timeout = duration_from_env_ms(&[DUMMY_RPC_TIMEOUT_ENV])
            .or_else(|| duration_from_env_ms(&[REQUEST_TIMEOUT_ENV]))
            .unwrap_or(DEFAULT_DUMMY_RPC_TIMEOUT);
        Self {
            request_timeout,
            startup_timeout_override,
            heartbeat_timeout,
            transfer_stall_timeout,
            dummy_rpc_timeout,
        }
    }

    pub fn from_env_and_overrides(overrides: CompatTimeoutCliOverrides) -> Result<Self> {
        Self::from_env().apply_overrides(overrides)
    }

    pub fn apply_overrides(mut self, overrides: CompatTimeoutCliOverrides) -> Result<Self> {
        if let Some(timeout_ms) = overrides.request_timeout_ms {
            self.request_timeout = parse_timeout_override("--request-timeout-ms", timeout_ms)?;
            if overrides.dummy_rpc_timeout_ms.is_none() {
                self.dummy_rpc_timeout = self.request_timeout;
            }
        }
        if let Some(timeout_ms) = overrides.startup_timeout_ms {
            self.startup_timeout_override =
                Some(parse_timeout_override("--startup-timeout-ms", timeout_ms)?);
        }
        if let Some(timeout_ms) = overrides.heartbeat_timeout_ms {
            self.heartbeat_timeout = parse_timeout_override("--heartbeat-timeout-ms", timeout_ms)?;
        }
        if let Some(timeout_ms) = overrides.transfer_stall_timeout_ms {
            self.transfer_stall_timeout =
                parse_timeout_override("--transfer-stall-timeout-ms", timeout_ms)?;
        }
        if let Some(timeout_ms) = overrides.dummy_rpc_timeout_ms {
            self.dummy_rpc_timeout = parse_timeout_override("--dummy-rpc-timeout-ms", timeout_ms)?;
        }
        Ok(self)
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout_override = Some(timeout.max(Duration::from_millis(1)));
        self
    }

    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout.max(Duration::from_millis(1));
        self
    }

    pub fn registration_timeout_for_bytes(&self, registration_bytes: u64) -> Duration {
        self.startup_timeout_override
            .unwrap_or_else(|| Self::default_registration_timeout_for_bytes(registration_bytes))
    }
}

fn adaptive_registration_timeout(registration_bytes: u64) -> Duration {
    let units = registration_bytes
        .max(1)
        .div_ceil(STARTUP_TIMEOUT_BYTES_PER_SECOND);
    Duration::from_secs(units).max(DEFAULT_REGISTRATION_TIMEOUT_FLOOR)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransportBackend {
    Tent,
    ClassicTe,
    Http,
}

#[derive(Clone, Debug)]
pub enum CompatTransportConfig {
    Tent(TentEngineConfig),
    ClassicTe(ClassicEngineConfig),
    Http,
}

/// Compatibility-layer build output for Python and standalone client entrypoints.
///
/// Tenant-scoped policy should be authored through admin-managed metadata. Fields such as
/// `route_topk` remain here as compatibility fallbacks so existing integrations continue to work.
pub struct CompatBuildPlan {
    pub metadata: Arc<dyn MetadataBackend>,
    pub transport_backend: TransportBackend,
    pub transport_config: CompatTransportConfig,
    pub timeouts: CompatTimeoutConfig,
    pub stable_id: String,
    pub tenant: String,
    pub labels: BTreeMap<String, String>,
    pub routed_writes: bool,
    pub replica_count: usize,
    pub route_topk: usize,
    pub storage_bytes: usize,
    pub scratch_bytes: usize,
    pub expires_at_ms: u64,
    pub lease_ttl_ms: u64,
    pub use_hugepage: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
}

#[derive(Clone, Debug)]
/// Compatibility setup inputs accepted by the Python wrapper and standalone client.
///
/// Admin-managed tenant policy in metadata is the preferred authoring surface for tenant-scoped
/// routing and resource policy. These inputs are kept as bootstrap/fallback knobs.
pub struct CompatSetupArgs {
    pub local_hostname: String,
    pub metadata_url: String,
    pub transport_metadata_url: Option<String>,
    pub global_segment_size: usize,
    pub local_buffer_size: usize,
    pub protocol: String,
    pub _rdma_devices: String,
    pub transport_rpc_port: Option<u16>,
    pub transport_backend: Option<String>,
    pub stable_id: Option<String>,
    pub tenant: String,
    pub labels: BTreeMap<String, String>,
    pub routed_writes: bool,
    pub replica_count: usize,
    pub route_topk: usize,
    pub keyspace: Option<String>,
    pub expires_at_ms: Option<u64>,
    pub use_hugepage: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
    pub timeouts: Option<CompatTimeoutConfig>,
}

impl CompatSetupArgs {
    pub fn build(self) -> Result<CompatBuildPlan> {
        let Self {
            local_hostname,
            metadata_url,
            transport_metadata_url,
            global_segment_size,
            local_buffer_size,
            protocol,
            _rdma_devices: _,
            transport_rpc_port,
            transport_backend,
            stable_id,
            tenant,
            labels,
            routed_writes,
            replica_count,
            route_topk,
            keyspace,
            expires_at_ms,
            use_hugepage,
            hugepage_size_bytes,
            timeouts,
        } = self;

        let keyspace = keyspace.map(MetadataKeyspace::new).unwrap_or_default();
        let stable_id = stable_id.unwrap_or_else(|| format!("py-store-{}", now_ms()));
        let transport_backend = resolve_transport_backend(transport_backend.as_deref())?;
        let timeouts = timeouts.unwrap_or_else(CompatTimeoutConfig::from_env);
        let (metadata, transport_redis_url) =
            build_metadata_backend(&metadata_url, transport_metadata_url, keyspace)?;
        let transport_config = build_transport_config(
            transport_backend,
            &local_hostname,
            &transport_redis_url,
            &protocol,
            transport_rpc_port,
            timeouts,
        )?;
        let mut labels = labels;
        if route_topk < 2 {
            return Err(StoreError::InvalidState(
                "route_topk must be greater than or equal to 2".to_string(),
            ));
        }
        match labels.get("storage").map(String::as_str) {
            Some("true") if global_segment_size == 0 => {
                return Err(StoreError::InvalidState(
                    "label storage=true requires storage_bytes > 0".to_string(),
                ));
            }
            None => {
                labels.insert(
                    "storage".to_string(),
                    (global_segment_size != 0).to_string(),
                );
            }
            _ => {}
        }
        labels.entry("route".to_string()).or_insert_with(|| {
            if global_segment_size == 0 {
                "false".to_string()
            } else {
                "true".to_string()
            }
        });
        let now_ms = now_ms();
        let (expires_at_ms, lease_ttl_ms) = match expires_at_ms {
            Some(expires_at_ms) => (expires_at_ms, expires_at_ms.saturating_sub(now_ms).max(1)),
            None => (
                now_ms.saturating_add(DEFAULT_COMPAT_LEASE_TTL_MS),
                DEFAULT_COMPAT_LEASE_TTL_MS,
            ),
        };

        Ok(CompatBuildPlan {
            metadata,
            transport_backend,
            transport_config,
            timeouts,
            stable_id,
            tenant,
            labels,
            routed_writes,
            replica_count: replica_count.max(1),
            route_topk,
            storage_bytes: global_segment_size,
            scratch_bytes: local_buffer_size,
            expires_at_ms,
            lease_ttl_ms,
            use_hugepage,
            hugepage_size_bytes,
        })
    }
}

fn resolve_transport_backend(explicit: Option<&str>) -> Result<TransportBackend> {
    let value = explicit
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| std::env::var("MC_STORE_RS_TRANSPORT_BACKEND").ok())
        .unwrap_or_else(|| "classic_te".to_string());
    match value.to_ascii_lowercase().as_str() {
        "tent" => Ok(TransportBackend::Tent),
        "classic" | "classic_te" | "classic-te" | "te" => Ok(TransportBackend::ClassicTe),
        "http" => Ok(TransportBackend::Http),
        other => Err(StoreError::Unsupported(format!(
            "unsupported transport backend: {other}"
        ))),
    }
}

fn build_transport_config(
    backend: TransportBackend,
    local_hostname: &str,
    transport_redis_url: &str,
    protocol: &str,
    transport_rpc_port: Option<u16>,
    timeouts: CompatTimeoutConfig,
) -> Result<CompatTransportConfig> {
    match backend {
        TransportBackend::Tent => Ok(CompatTransportConfig::Tent(build_tent_config(
            local_hostname,
            transport_redis_url,
            protocol,
            transport_rpc_port,
            timeouts,
        )?)),
        TransportBackend::ClassicTe => Ok(CompatTransportConfig::ClassicTe(build_classic_config(
            local_hostname,
            transport_redis_url,
            protocol,
            transport_rpc_port,
            timeouts,
        )?)),
        TransportBackend::Http => match protocol.to_ascii_lowercase().as_str() {
            "http" | "tcp" | "" | "auto" => Ok(CompatTransportConfig::Http),
            other => Err(StoreError::Unsupported(format!(
                "unsupported transport protocol for http backend: {other}"
            ))),
        },
    }
}

pub fn build_metadata_backend(
    metadata_url: &str,
    transport_metadata_url: Option<String>,
    keyspace: MetadataKeyspace,
) -> Result<(Arc<dyn MetadataBackend>, String)> {
    if metadata_url.starts_with("redis://") {
        let backend = Arc::new(RedisMetadataBackend::new(
            RedisMetadataConfig::new(metadata_url.to_string()).keyspace(keyspace),
        )?);
        return Ok((backend, metadata_url.to_string()));
    }

    if let Some(rest) = metadata_url.strip_prefix("etcd://") {
        let endpoints = rest
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(|entry| normalize_etcd_endpoint(entry.trim()))
            .collect::<Vec<_>>();
        if endpoints.is_empty() {
            return Err(StoreError::Metadata(
                "etcd metadata url must contain at least one endpoint".to_string(),
            ));
        }
        let backend = Arc::new(EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new(endpoints).keyspace(keyspace),
        )?);
        let transport_redis_url = transport_metadata_url
            .or_else(|| std::env::var("MC_STORE_RS_TENT_REDIS_URL").ok())
            .unwrap_or_else(|| "redis://127.0.0.1:6380/0".to_string());
        if !transport_redis_url.starts_with("redis://") {
            return Err(StoreError::Metadata(
                "transport metadata url must be redis:// when store metadata uses etcd".to_string(),
            ));
        }
        return Ok((backend, transport_redis_url));
    }

    if metadata_url.starts_with("http://") || metadata_url.starts_with("https://") {
        return Err(StoreError::Unsupported(
            "http metadata endpoints are not supported in store-rs; use redis:// or etcd://"
                .to_string(),
        ));
    }

    Err(StoreError::Metadata(format!(
        "unsupported metadata url scheme: {metadata_url}"
    )))
}

fn build_tent_config(
    local_hostname: &str,
    transport_redis_url: &str,
    protocol: &str,
    transport_rpc_port: Option<u16>,
    timeouts: CompatTimeoutConfig,
) -> Result<TentEngineConfig> {
    let (local_hostname, transport_rpc_port) =
        normalize_transport_listen_endpoint(local_hostname, transport_rpc_port)?;
    let redis = Url::parse(transport_redis_url)
        .map_err(|error| StoreError::Metadata(format!("invalid redis url: {error}")))?;
    let host = redis
        .host_str()
        .ok_or_else(|| StoreError::Metadata("redis url is missing host".to_string()))?;
    let port = redis.port().unwrap_or(6379);
    let db_index = redis
        .path_segments()
        .and_then(|mut segments| segments.next())
        .filter(|segment| !segment.is_empty())
        .unwrap_or("0");
    let auth = resolve_redis_auth(transport_redis_url)?;

    let (tcp_enable, rdma_enable) = match protocol.to_ascii_lowercase().as_str() {
        "tcp" | "" => ("true", "false"),
        "rdma" => ("false", "true"),
        "auto" => ("true", "false"),
        other => {
            return Err(StoreError::Unsupported(format!(
                "unsupported transport protocol: {other}"
            )))
        }
    };

    let mut config = TentEngineConfig::new()
        .set("metadata_type", "redis")
        .set("metadata_servers", format!("{host}:{port}"))
        .set("redis_db_index", db_index)
        .set("rpc_server_hostname", local_hostname)
        .set(
            "rpc_server_port",
            transport_rpc_port.unwrap_or_default().to_string(),
        )
        .set("log_level", "warning")
        .set("transports/tcp/enable", tcp_enable)
        .set("transports/shm/enable", "false")
        .set("transports/rdma/enable", rdma_enable)
        .set(
            "transports/rdma/max_timeout_ns",
            timeouts.transfer_stall_timeout.as_nanos().to_string(),
        )
        .set("transports/io_uring/enable", "false")
        .redis_db_index(db_index);
    if let Some(username) = auth.username {
        config = config.redis_username(username);
    }
    if let Some(password) = auth.password {
        config = config.redis_password(password);
    }
    Ok(config)
}

fn build_classic_config(
    local_hostname: &str,
    transport_redis_url: &str,
    protocol: &str,
    transport_rpc_port: Option<u16>,
    timeouts: CompatTimeoutConfig,
) -> Result<ClassicEngineConfig> {
    let (rpc_bind_host, rpc_port) =
        normalize_transport_listen_endpoint(local_hostname, transport_rpc_port)?;
    let redis = Url::parse(transport_redis_url)
        .map_err(|error| StoreError::Metadata(format!("invalid redis url: {error}")))?;
    let host = redis
        .host_str()
        .ok_or_else(|| StoreError::Metadata("redis url is missing host".to_string()))?;
    let port = redis.port().unwrap_or(6379);
    let db_index = redis
        .path_segments()
        .and_then(|mut segments| segments.next())
        .filter(|segment| !segment.is_empty())
        .unwrap_or("0");
    let auth = resolve_redis_auth(transport_redis_url)?;
    let protocol = match protocol.to_ascii_lowercase().as_str() {
        "tcp" | "" | "auto" => ClassicTransportProtocol::Tcp,
        "rdma" => ClassicTransportProtocol::Rdma,
        other => {
            return Err(StoreError::Unsupported(format!(
                "unsupported transport protocol: {other}"
            )))
        }
    };
    let metadata_uri = format!("redis://{}:{port}", format_redis_host(host));
    let mut config = ClassicEngineConfig::new(metadata_uri, rpc_bind_host)
        .protocol(protocol)
        .slice_timeout(timeouts.transfer_stall_timeout);
    if let Some(rpc_port) = rpc_port {
        config = config.rpc_port(rpc_port);
    }
    config = config.redis_db_index(db_index);
    if let Some(username) = auth.username {
        config = config.redis_username(username);
    }
    if let Some(password) = auth.password {
        config = config.redis_password(password);
    }
    if let Ok(gid_index) = std::env::var("MC_STORE_RS_GID_INDEX") {
        if !gid_index.trim().is_empty() {
            config = config.gid_index(gid_index);
        }
    }
    Ok(config)
}

fn duration_from_env_ms(keys: &[&str]) -> Option<Duration> {
    keys.iter()
        .find_map(|key| std::env::var(key).ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
}

fn parse_timeout_override(flag: &str, timeout_ms: u64) -> Result<Duration> {
    if timeout_ms == 0 {
        return Err(StoreError::InvalidState(format!(
            "{flag} must be greater than zero"
        )));
    }
    Ok(Duration::from_millis(timeout_ms))
}

fn normalize_transport_listen_endpoint(
    local_hostname: &str,
    transport_rpc_port: Option<u16>,
) -> Result<(String, Option<u16>)> {
    let hostname = local_hostname.trim();
    let (embedded_host, embedded_port) = parse_embedded_transport_port(hostname);
    if let (Some(embedded), Some(explicit)) = (embedded_port, transport_rpc_port) {
        if embedded != explicit {
            return Err(StoreError::InvalidState(format!(
                "local_hostname embedded transport port {embedded} conflicts with transport_rpc_port {explicit}"
            )));
        }
    }
    Ok((
        embedded_host.to_string(),
        transport_rpc_port.or(embedded_port),
    ))
}

fn parse_embedded_transport_port(local_hostname: &str) -> (&str, Option<u16>) {
    if let Some(inner) = local_hostname.strip_prefix('[') {
        if let Some((host, suffix)) = inner.split_once(']') {
            if let Some(port_text) = suffix.strip_prefix(':') {
                if let Ok(port) = port_text.parse::<u16>() {
                    return (host, Some(port));
                }
            }
        }
        return (local_hostname, None);
    }

    if local_hostname.matches(':').count() == 1 {
        if let Some((host, port_text)) = local_hostname.rsplit_once(':') {
            if !host.is_empty() {
                if let Ok(port) = port_text.parse::<u16>() {
                    return (host, Some(port));
                }
            }
        }
    }

    (local_hostname, None)
}

fn normalize_etcd_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn format_redis_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
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
    use std::collections::BTreeMap;

    use super::*;
    use crate::test_support::env_test_lock;

    fn sample_timeouts() -> CompatTimeoutConfig {
        CompatTimeoutConfig {
            request_timeout: Duration::from_millis(65_000),
            startup_timeout_override: None,
            heartbeat_timeout: Duration::from_millis(15_000),
            transfer_stall_timeout: Duration::from_millis(10_000),
            dummy_rpc_timeout: Duration::from_millis(65_000),
        }
    }

    #[test]
    fn normalize_etcd_endpoint_adds_http_scheme() {
        assert_eq!(
            normalize_etcd_endpoint("127.0.0.1:2379"),
            "http://127.0.0.1:2379"
        );
        assert_eq!(
            normalize_etcd_endpoint("https://etcd.example:2379"),
            "https://etcd.example:2379"
        );
    }

    #[test]
    fn build_tent_config_uses_redis_url_parts() {
        let config = build_tent_config(
            "127.0.0.1",
            "redis://cache.local:6381/3",
            "tcp",
            Some(17111),
            sample_timeouts(),
        )
        .expect("tent config should build");
        let debug = format!("{config:?}");
        assert!(debug.contains("cache.local:6381"));
        assert!(debug.contains("3"));
        assert!(debug.contains("17111"));
    }

    #[test]
    fn build_classic_config_normalizes_metadata_uri_and_auth() {
        let _guard = env_test_lock().lock();
        let config = build_classic_config(
            "node-a:17112",
            "redis://user:pass@cache.local:6381/4",
            "tcp",
            None,
            sample_timeouts(),
        )
        .expect("classic config should build");
        assert_eq!(config.metadata_uri(), "redis://cache.local:6381");
        assert_eq!(config.rpc_bind_host(), "node-a");
        assert_eq!(config.rpc_port_value(), Some(17112));
        assert_eq!(config.transport_protocol(), ClassicTransportProtocol::Tcp);
        assert_eq!(config.redis_username_value(), Some("user"));
        assert_eq!(config.redis_password_value(), Some("pass"));
        assert_eq!(config.redis_db_index_value(), Some("4"));
    }

    #[test]
    fn resolve_transport_backend_prefers_explicit_value() {
        let _guard = env_test_lock().lock();
        std::env::set_var("MC_STORE_RS_TRANSPORT_BACKEND", "tent");
        let backend =
            resolve_transport_backend(Some("classic_te")).expect("explicit backend should parse");
        assert_eq!(backend, TransportBackend::ClassicTe);
        std::env::remove_var("MC_STORE_RS_TRANSPORT_BACKEND");
    }

    #[test]
    fn resolve_transport_backend_reads_env_when_unspecified() {
        let _guard = env_test_lock().lock();
        std::env::set_var("MC_STORE_RS_TRANSPORT_BACKEND", "te");
        let backend = resolve_transport_backend(None).expect("env backend should parse");
        assert_eq!(backend, TransportBackend::ClassicTe);
        std::env::remove_var("MC_STORE_RS_TRANSPORT_BACKEND");
    }

    #[test]
    fn resolve_transport_backend_rejects_unknown_values() {
        let error =
            resolve_transport_backend(Some("mystery")).expect_err("unknown backend should fail");
        assert!(matches!(error, StoreError::Unsupported(_)));
    }

    #[test]
    fn resolve_transport_redis_auth_prefers_url_credentials() {
        let _guard = env_test_lock().lock();
        std::env::set_var("MC_REDIS_USERNAME", "env-user");
        std::env::set_var("MC_REDIS_PASSWORD", "env-pass");
        let auth = resolve_redis_auth("redis://url-user:url-pass@cache.local:6381/3")
            .expect("redis auth should resolve");
        assert_eq!(auth.username.as_deref(), Some("url-user"));
        assert_eq!(auth.password.as_deref(), Some("url-pass"));
        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
    }

    #[test]
    fn resolve_transport_redis_auth_falls_back_to_env_password() {
        let _guard = env_test_lock().lock();
        std::env::set_var("MC_REDIS_USERNAME", "env-user");
        std::env::set_var("MC_REDIS_PASSWORD", "env-pass");
        let auth =
            resolve_redis_auth("redis://cache.local:6381/3").expect("redis auth should resolve");
        assert_eq!(auth.username.as_deref(), Some("env-user"));
        assert_eq!(auth.password.as_deref(), Some("env-pass"));
        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
    }

    #[test]
    fn build_metadata_backend_rejects_http_metadata() {
        let result = build_metadata_backend(
            "http://127.0.0.1:8080/metadata",
            None,
            MetadataKeyspace::default(),
        );
        let error = match result {
            Ok(_) => panic!("http metadata should be unsupported"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::Unsupported(_)));
    }

    #[test]
    fn build_plan_defaults_storage_false_when_storage_bytes_is_zero() {
        let plan = CompatSetupArgs {
            local_hostname: "127.0.0.1".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            global_segment_size: 0,
            local_buffer_size: 1024,
            protocol: "tcp".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("sample".to_string()),
            tenant: "default".to_string(),
            labels: BTreeMap::new(),
            routed_writes: false,
            replica_count: 1,
            route_topk: 2,
            keyspace: None,
            expires_at_ms: Some(1),
            use_hugepage: None,
            hugepage_size_bytes: None,
            timeouts: None,
        }
        .build()
        .expect("build plan should succeed");
        assert_eq!(
            plan.labels.get("storage").map(String::as_str),
            Some("false")
        );
        assert_eq!(plan.labels.get("route").map(String::as_str), Some("false"));
        assert_eq!(plan.transport_backend, TransportBackend::ClassicTe);
    }

    #[test]
    fn build_plan_rejects_storage_true_without_storage_bytes() {
        let result = CompatSetupArgs {
            local_hostname: "127.0.0.1".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            global_segment_size: 0,
            local_buffer_size: 1024,
            protocol: "tcp".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("sample".to_string()),
            tenant: "default".to_string(),
            labels: BTreeMap::from([("storage".to_string(), "true".to_string())]),
            routed_writes: false,
            replica_count: 1,
            route_topk: 2,
            keyspace: None,
            expires_at_ms: Some(1),
            use_hugepage: None,
            hugepage_size_bytes: None,
            timeouts: None,
        }
        .build();
        let error = match result {
            Ok(_) => panic!("build plan should reject storage=true without storage bytes"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::InvalidState(_)));
    }

    #[test]
    fn build_plan_rejects_route_topk_below_two() {
        let result = CompatSetupArgs {
            local_hostname: "127.0.0.1".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            global_segment_size: 4096,
            local_buffer_size: 1024,
            protocol: "tcp".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("sample".to_string()),
            tenant: "default".to_string(),
            labels: BTreeMap::new(),
            routed_writes: false,
            replica_count: 1,
            route_topk: 1,
            keyspace: None,
            expires_at_ms: Some(1),
            use_hugepage: None,
            hugepage_size_bytes: None,
            timeouts: None,
        }
        .build();
        let error = match result {
            Ok(_) => panic!("build plan should reject route_topk < 2"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::InvalidState(_)));
    }

    #[test]
    fn compat_setup_build_supports_etcd_metadata_and_defaults() {
        let plan = CompatSetupArgs {
            local_hostname: "node-a".to_string(),
            metadata_url: "etcd://127.0.0.1:2379,https://etcd.example:32379".to_string(),
            transport_metadata_url: Some("redis://cache.local:6381/4".to_string()),
            global_segment_size: 4096,
            local_buffer_size: 1024,
            protocol: "rdma".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: Some(17112),
            transport_backend: None,
            stable_id: None,
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::from([("pool".to_string(), "pool-a".to_string())]),
            routed_writes: true,
            replica_count: 0,
            route_topk: 3,
            keyspace: Some("py/test".to_string()),
            expires_at_ms: None,
            use_hugepage: Some(true),
            hugepage_size_bytes: Some(2 * 1024 * 1024),
            timeouts: None,
        }
        .build()
        .expect("compat build plan should succeed");

        assert!(plan.stable_id.starts_with("py-store-"));
        assert_eq!(plan.tenant, "tenant-a");
        assert_eq!(plan.replica_count, 1);
        assert_eq!(plan.route_topk, 3);
        assert_eq!(plan.storage_bytes, 4096);
        assert_eq!(plan.scratch_bytes, 1024);
        assert_eq!(plan.use_hugepage, Some(true));
        assert_eq!(plan.hugepage_size_bytes, Some(2 * 1024 * 1024));
        assert_eq!(plan.labels.get("pool").map(String::as_str), Some("pool-a"));
        assert_eq!(plan.labels.get("storage").map(String::as_str), Some("true"));
        assert!(plan.expires_at_ms > 0);

        match plan.transport_config {
            CompatTransportConfig::Tent(config) => {
                let debug = format!("{config:?}");
                assert!(debug.contains("cache.local:6381"));
                assert!(debug.contains("4"));
                assert!(debug.contains("17112"));
                assert!(debug.contains("transports/rdma/enable"));
            }
            CompatTransportConfig::ClassicTe(_) | CompatTransportConfig::Http => {}
        }
    }

    #[test]
    fn compat_setup_can_build_classic_te_plan() {
        let plan = CompatSetupArgs {
            local_hostname: "node-a:17112".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            global_segment_size: 4096,
            local_buffer_size: 1024,
            protocol: "tcp".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: Some("classic_te".to_string()),
            stable_id: Some("classic".to_string()),
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::new(),
            routed_writes: false,
            replica_count: 1,
            route_topk: 2,
            keyspace: None,
            expires_at_ms: Some(10_000),
            use_hugepage: None,
            hugepage_size_bytes: None,
            timeouts: None,
        }
        .build()
        .expect("classic plan should build");

        assert_eq!(plan.transport_backend, TransportBackend::ClassicTe);
        match plan.transport_config {
            CompatTransportConfig::ClassicTe(config) => {
                assert_eq!(config.rpc_bind_host(), "node-a");
                assert_eq!(config.transport_protocol(), ClassicTransportProtocol::Tcp);
            }
            CompatTransportConfig::Tent(_) => panic!("classic backend should build classic config"),
            CompatTransportConfig::Http => panic!("classic backend should not build http config"),
        }
    }

    #[test]
    fn compat_setup_defaults_storage_label_to_false_for_rw_only_clients() {
        let plan = CompatSetupArgs {
            local_hostname: "node-a".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            global_segment_size: 0,
            local_buffer_size: 1024,
            protocol: "tcp".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("rw-only".to_string()),
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::new(),
            routed_writes: true,
            replica_count: 1,
            route_topk: 2,
            keyspace: None,
            expires_at_ms: Some(10_000),
            use_hugepage: None,
            hugepage_size_bytes: None,
            timeouts: None,
        }
        .build()
        .expect("rw-only plan should build");

        assert_eq!(plan.storage_bytes, 0);
        assert_eq!(
            plan.labels.get("storage").map(String::as_str),
            Some("false")
        );
        assert_eq!(plan.labels.get("route").map(String::as_str), Some("false"));
    }

    #[test]
    fn compat_setup_keeps_explicit_route_true_for_rw_only_clients() {
        let plan = CompatSetupArgs {
            local_hostname: "node-a".to_string(),
            metadata_url: "redis://127.0.0.1:6379/0".to_string(),
            transport_metadata_url: None,
            global_segment_size: 0,
            local_buffer_size: 1024,
            protocol: "tcp".to_string(),
            _rdma_devices: String::new(),
            transport_rpc_port: None,
            transport_backend: None,
            stable_id: Some("rw-only-explicit-route".to_string()),
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::from([("route".to_string(), "true".to_string())]),
            routed_writes: true,
            replica_count: 1,
            route_topk: 2,
            keyspace: None,
            expires_at_ms: Some(10_000),
            use_hugepage: None,
            hugepage_size_bytes: None,
            timeouts: None,
        }
        .build()
        .expect("rw-only explicit-route plan should build");

        assert_eq!(
            plan.labels.get("storage").map(String::as_str),
            Some("false")
        );
        assert_eq!(plan.labels.get("route").map(String::as_str), Some("true"));
    }

    #[test]
    fn normalize_transport_listen_endpoint_extracts_embedded_port() {
        let (host, port) = normalize_transport_listen_endpoint("node-a:17112", None)
            .expect("embedded port should normalize");
        assert_eq!(host, "node-a");
        assert_eq!(port, Some(17112));

        let (ipv6_host, ipv6_port) = normalize_transport_listen_endpoint("[::1]:17113", None)
            .expect("ipv6 embedded port should normalize");
        assert_eq!(ipv6_host, "::1");
        assert_eq!(ipv6_port, Some(17113));
    }

    #[test]
    fn normalize_transport_listen_endpoint_rejects_conflicting_ports() {
        let error = match normalize_transport_listen_endpoint("node-a:17112", Some(17113)) {
            Ok(_) => panic!("conflicting embedded port should fail"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::InvalidState(_)));
    }

    #[test]
    fn build_metadata_backend_validates_etcd_transport_url() {
        let error = match build_metadata_backend(
            "etcd://127.0.0.1:2379",
            Some("http://bad-transport".to_string()),
            MetadataKeyspace::default(),
        ) {
            Ok(_) => panic!("etcd metadata requires redis transport metadata"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::Metadata(_)));
    }

    #[test]
    fn build_metadata_backend_rejects_unknown_scheme() {
        let error =
            match build_metadata_backend("file:///tmp/metadata", None, MetadataKeyspace::default())
            {
                Ok(_) => panic!("unknown metadata scheme should fail"),
                Err(error) => error,
            };
        assert!(matches!(error, StoreError::Metadata(_)));
    }

    #[test]
    fn build_tent_config_rejects_invalid_redis_inputs() {
        let invalid_url = build_tent_config(
            "127.0.0.1",
            "not-a-redis-url",
            "tcp",
            None,
            sample_timeouts(),
        )
        .expect_err("bad url must fail");
        assert!(matches!(invalid_url, StoreError::Metadata(_)));

        let missing_host =
            build_tent_config("127.0.0.1", "redis:///0", "auto", None, sample_timeouts())
                .expect_err("host is required");
        assert!(matches!(missing_host, StoreError::Metadata(_)));

        let auto = build_tent_config(
            "127.0.0.1",
            "redis://cache.local",
            "auto",
            None,
            sample_timeouts(),
        )
        .expect("auto config should succeed");
        let debug = format!("{auto:?}");
        assert!(debug.contains("cache.local:6379"));
        assert!(debug.contains("transports/tcp/enable"));
    }

    #[test]
    fn timeout_config_uses_request_timeout_for_dummy_by_default() {
        let _guard = env_test_lock().lock();
        std::env::set_var(REQUEST_TIMEOUT_ENV, "42000");
        std::env::remove_var(DUMMY_RPC_TIMEOUT_ENV);
        let timeouts = CompatTimeoutConfig::from_env();
        assert_eq!(timeouts.request_timeout, Duration::from_millis(42_000));
        assert_eq!(timeouts.startup_timeout_override, None);
        assert_eq!(timeouts.dummy_rpc_timeout, Duration::from_millis(42_000));
        std::env::remove_var(REQUEST_TIMEOUT_ENV);
    }

    #[test]
    fn timeout_config_reads_startup_timeout_from_env() {
        let _guard = env_test_lock().lock();
        std::env::set_var(STARTUP_TIMEOUT_ENV, "91000");
        let timeouts = CompatTimeoutConfig::from_env();
        assert_eq!(
            timeouts.startup_timeout_override,
            Some(Duration::from_millis(91_000))
        );
        std::env::remove_var(STARTUP_TIMEOUT_ENV);
    }

    #[test]
    fn timeout_config_cli_overrides_update_request_and_dummy_together() {
        let timeouts = CompatTimeoutConfig::from_env()
            .apply_overrides(CompatTimeoutCliOverrides {
                request_timeout_ms: Some(9_000),
                startup_timeout_ms: Some(120_000),
                heartbeat_timeout_ms: Some(11_000),
                transfer_stall_timeout_ms: Some(7_000),
                dummy_rpc_timeout_ms: None,
            })
            .expect("cli overrides should apply");
        assert_eq!(timeouts.request_timeout, Duration::from_millis(9_000));
        assert_eq!(
            timeouts.startup_timeout_override,
            Some(Duration::from_millis(120_000))
        );
        assert_eq!(timeouts.heartbeat_timeout, Duration::from_millis(11_000));
        assert_eq!(
            timeouts.transfer_stall_timeout,
            Duration::from_millis(7_000)
        );
        assert_eq!(timeouts.dummy_rpc_timeout, Duration::from_millis(9_000));
    }

    #[test]
    fn adaptive_registration_timeout_scales_with_registration_bytes() {
        assert_eq!(
            sample_timeouts().registration_timeout_for_bytes(1),
            Duration::from_secs(20)
        );
        assert_eq!(
            sample_timeouts().registration_timeout_for_bytes(1024 * 1024 * 1024),
            Duration::from_secs(20)
        );
        assert_eq!(
            sample_timeouts().registration_timeout_for_bytes(500 * 1024 * 1024 * 1024_u64),
            Duration::from_secs(500)
        );
        assert_eq!(
            sample_timeouts()
                .with_startup_timeout(Duration::from_secs(7))
                .registration_timeout_for_bytes(500 * 1024 * 1024 * 1024_u64),
            Duration::from_secs(7)
        );
    }
}
