use std::sync::Arc;

use crate::{
    EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace, RedisMetadataBackend,
    RedisMetadataConfig,
};
use mooncake_store_core::{MetadataBackend, Result, StoreError};

pub fn build_store_metadata_backend(
    metadata_url: &str,
    keyspace: MetadataKeyspace,
) -> Result<Arc<dyn MetadataBackend>> {
    if metadata_url.starts_with("redis://") {
        return Ok(Arc::new(RedisMetadataBackend::new(
            RedisMetadataConfig::new(metadata_url.to_string()).keyspace(keyspace),
        )?));
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
        return Ok(Arc::new(EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new(endpoints).keyspace(keyspace),
        )?));
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

fn normalize_etcd_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

#[cfg(test)]
mod tests {
    use super::{build_store_metadata_backend, normalize_etcd_endpoint};
    use crate::MetadataKeyspace;
    use mooncake_store_core::StoreError;

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
    fn rejects_http_metadata() {
        let error = match build_store_metadata_backend(
            "http://127.0.0.1:8080/metadata",
            MetadataKeyspace::default(),
        ) {
            Ok(_) => panic!("http metadata should be unsupported"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::Unsupported(_)));
    }

    #[test]
    fn rejects_unknown_scheme() {
        let error =
            match build_store_metadata_backend("file:///tmp/metadata", MetadataKeyspace::default())
            {
                Ok(_) => panic!("unknown metadata scheme should fail"),
                Err(error) => error,
            };
        assert!(matches!(error, StoreError::Metadata(_)));
    }
}
