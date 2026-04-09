use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_metadata::{
    EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace, RedisMetadataBackend,
    RedisMetadataConfig,
};
use mooncake_store_core::{MetadataBackend, Result, StoreError};
use mooncake_transport::TentEngineConfig;
use url::Url;

pub struct CompatBuildPlan {
    pub metadata: Arc<dyn MetadataBackend>,
    pub tent_config: TentEngineConfig,
    pub stable_id: String,
    pub tenant: String,
    pub labels: BTreeMap<String, String>,
    pub routed_writes: bool,
    pub replica_count: usize,
    pub storage_bytes: usize,
    pub scratch_bytes: usize,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct CompatSetupArgs {
    pub local_hostname: String,
    pub metadata_url: String,
    pub transport_metadata_url: Option<String>,
    pub global_segment_size: usize,
    pub local_buffer_size: usize,
    pub protocol: String,
    pub _rdma_devices: String,
    pub stable_id: Option<String>,
    pub tenant: String,
    pub labels: BTreeMap<String, String>,
    pub routed_writes: bool,
    pub replica_count: usize,
    pub keyspace: Option<String>,
    pub expires_at_ms: Option<u64>,
}

impl CompatSetupArgs {
    pub fn build(self) -> Result<CompatBuildPlan> {
        let keyspace = self.keyspace.map(MetadataKeyspace::new).unwrap_or_default();
        let stable_id = self
            .stable_id
            .unwrap_or_else(|| format!("py-store-{}", now_ms()));
        let (metadata, transport_redis_url) =
            build_metadata_backend(&self.metadata_url, self.transport_metadata_url, keyspace)?;
        let tent_config =
            build_tent_config(&self.local_hostname, &transport_redis_url, &self.protocol)?;
        let mut labels = self.labels;
        labels
            .entry("storage".to_string())
            .or_insert_with(|| "true".to_string());
        Ok(CompatBuildPlan {
            metadata,
            tent_config,
            stable_id,
            tenant: self.tenant,
            labels,
            routed_writes: self.routed_writes,
            replica_count: self.replica_count.max(1),
            storage_bytes: self.global_segment_size,
            scratch_bytes: self.local_buffer_size,
            expires_at_ms: self.expires_at_ms.unwrap_or_else(|| now_ms() + 600_000),
        })
    }
}

fn build_metadata_backend(
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
) -> Result<TentEngineConfig> {
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

    Ok(TentEngineConfig::new()
        .set("metadata_type", "redis")
        .set("metadata_servers", format!("{host}:{port}"))
        .set("redis_db_index", db_index)
        .set("rpc_server_hostname", local_hostname)
        .set("rpc_server_port", "0")
        .set("log_level", "warning")
        .set("transports/tcp/enable", tcp_enable)
        .set("transports/shm/enable", "false")
        .set("transports/rdma/enable", rdma_enable)
        .set("transports/io_uring/enable", "false"))
}

fn normalize_etcd_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
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
    use super::*;

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
        let config = build_tent_config("127.0.0.1", "redis://cache.local:6381/3", "tcp")
            .expect("tent config should build");
        let debug = format!("{config:?}");
        assert!(debug.contains("cache.local:6381"));
        assert!(debug.contains("3"));
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
}
