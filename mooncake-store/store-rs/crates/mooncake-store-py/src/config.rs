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
    pub use_hugepage: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
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
    pub use_hugepage: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
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
        match labels.get("storage").map(String::as_str) {
            Some("true") if self.global_segment_size == 0 => {
                return Err(StoreError::InvalidState(
                    "label storage=true requires storage_bytes > 0".to_string(),
                ));
            }
            None => {
                labels.insert(
                    "storage".to_string(),
                    (self.global_segment_size != 0).to_string(),
                );
            }
            _ => {}
        }
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
            use_hugepage: self.use_hugepage,
            hugepage_size_bytes: self.hugepage_size_bytes,
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
    use std::collections::BTreeMap;

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
            stable_id: Some("sample".to_string()),
            tenant: "default".to_string(),
            labels: BTreeMap::new(),
            routed_writes: false,
            replica_count: 1,
            keyspace: None,
            expires_at_ms: Some(1),
            use_hugepage: None,
            hugepage_size_bytes: None,
        }
        .build()
        .expect("build plan should succeed");
        assert_eq!(
            plan.labels.get("storage").map(String::as_str),
            Some("false")
        );
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
            stable_id: Some("sample".to_string()),
            tenant: "default".to_string(),
            labels: BTreeMap::from([("storage".to_string(), "true".to_string())]),
            routed_writes: false,
            replica_count: 1,
            keyspace: None,
            expires_at_ms: Some(1),
            use_hugepage: None,
            hugepage_size_bytes: None,
        }
        .build();
        let error = match result {
            Ok(_) => panic!("build plan should reject storage=true without storage bytes"),
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
            stable_id: None,
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::from([("pool".to_string(), "pool-a".to_string())]),
            routed_writes: true,
            replica_count: 0,
            keyspace: Some("py/test".to_string()),
            expires_at_ms: None,
            use_hugepage: Some(true),
            hugepage_size_bytes: Some(2 * 1024 * 1024),
        }
        .build()
        .expect("compat build plan should succeed");

        assert!(plan.stable_id.starts_with("py-store-"));
        assert_eq!(plan.tenant, "tenant-a");
        assert_eq!(plan.replica_count, 1);
        assert_eq!(plan.storage_bytes, 4096);
        assert_eq!(plan.scratch_bytes, 1024);
        assert_eq!(plan.use_hugepage, Some(true));
        assert_eq!(plan.hugepage_size_bytes, Some(2 * 1024 * 1024));
        assert_eq!(plan.labels.get("pool").map(String::as_str), Some("pool-a"));
        assert_eq!(plan.labels.get("storage").map(String::as_str), Some("true"));
        assert!(plan.expires_at_ms > 0);

        let debug = format!("{:?}", plan.tent_config);
        assert!(debug.contains("cache.local:6381"));
        assert!(debug.contains("4"));
        assert!(debug.contains("transports/rdma/enable"));
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
            stable_id: Some("rw-only".to_string()),
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::new(),
            routed_writes: true,
            replica_count: 1,
            keyspace: None,
            expires_at_ms: Some(10_000),
            use_hugepage: None,
            hugepage_size_bytes: None,
        }
        .build()
        .expect("rw-only plan should build");

        assert_eq!(plan.storage_bytes, 0);
        assert_eq!(
            plan.labels.get("storage").map(String::as_str),
            Some("false")
        );
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
        let invalid_url = build_tent_config("127.0.0.1", "not-a-redis-url", "tcp")
            .expect_err("bad url must fail");
        assert!(matches!(invalid_url, StoreError::Metadata(_)));

        let missing_host =
            build_tent_config("127.0.0.1", "redis:///0", "auto").expect_err("host is required");
        assert!(matches!(missing_host, StoreError::Metadata(_)));

        let auto = build_tent_config("127.0.0.1", "redis://cache.local", "auto")
            .expect("auto config should succeed");
        let debug = format!("{auto:?}");
        assert!(debug.contains("cache.local:6379"));
        assert!(debug.contains("transports/tcp/enable"));
    }
}
