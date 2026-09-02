use std::sync::Arc;

use mooncake_store_core::{NamespaceScope, Result, StoreError};

use crate::client::cold_tier::layout::{PhysicalKeyCodec, Sha256PhysicalKeyCodec, ValueChunkPlan};
use crate::client::{PersistentStorageBackendHealth, PersistentStorageManagement};

use super::backing::NofBacking;
use super::ensure_batch_len;
use super::object::{
    NofObject, NofObjectLimits, NofObjectMetadata, NofObjectShardWrite, NofObjectState,
};

/// Unified NoF facade and capability container.
///
/// A backing may expose logical-object I/O, physical-KV I/O, health and storage maintenance
/// independently. The facade never infers one capability from another.
#[derive(Clone)]
pub struct NofBackend {
    pub(crate) backing: Arc<dyn NofBacking>,
    pub(crate) object_limits: Option<NofObjectLimits>,
    pub(crate) key_codec: Arc<dyn PhysicalKeyCodec>,
    pub(crate) key_domain: Vec<u8>,
}

impl NofBackend {
    pub fn new(backing: Arc<dyn NofBacking>) -> Result<Self> {
        let object_limits = backing
            .object_limits()
            .map(NofObjectLimits::validate)
            .transpose()?;
        let has_object_io = backing.object_write().is_some()
            || backing.object_read().is_some()
            || backing.object_query().is_some()
            || backing.object_delete().is_some();
        if has_object_io && object_limits.is_none() {
            return Err(StoreError::InvalidState(
                "NoF logical-object capabilities require object limits".to_string(),
            ));
        }
        Ok(Self {
            backing,
            object_limits,
            key_codec: Arc::new(Sha256PhysicalKeyCodec),
            key_domain: b"mooncake:nof:physical:v1".to_vec(),
        })
    }

    pub fn key_codec(mut self, codec: Arc<dyn PhysicalKeyCodec>) -> Self {
        self.key_codec = codec;
        self
    }

    pub fn key_domain(mut self, domain: impl Into<Vec<u8>>) -> Result<Self> {
        let domain = domain.into();
        if domain.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF physical key domain must not be empty".to_string(),
            ));
        }
        self.key_domain = domain;
        Ok(self)
    }

    pub(in crate::client) fn management_mode(&self) -> PersistentStorageManagement {
        if self.backing.storage_management().is_some() {
            PersistentStorageManagement::MooncakeManaged
        } else {
            PersistentStorageManagement::BackendManaged
        }
    }

    pub(in crate::client) fn health_snapshot(&self) -> Result<PersistentStorageBackendHealth> {
        let health = if let Some(storage) = self.backing.storage_management() {
            storage.storage_health()?.validate(true)?
        } else if let Some(health) = self.backing.health_capability() {
            health.health()?.validate(false)?
        } else {
            Default::default()
        };
        Ok(PersistentStorageBackendHealth {
            capacity_bytes: health.capacity_bytes,
            available_bytes: health.available_bytes,
        })
    }

    pub fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()> {
        self.backing
            .object_write()
            .ok_or_else(|| unsupported("logical-object writes"))?
            .init_namespace(namespace)
    }

    pub fn put_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
        value: &[u8],
    ) -> Result<NofObjectMetadata> {
        self.validate_key(key)?;
        let limits = self
            .object_limits
            .ok_or_else(|| unsupported("object limits"))?;
        let writer = self
            .backing
            .object_write()
            .ok_or_else(|| unsupported("logical-object writes"))?;
        let value_len = u64::try_from(value.len()).map_err(|_| {
            StoreError::InvalidState("NoF value length does not fit u64".to_string())
        })?;
        let plan = ValueChunkPlan::new(value_len, limits.max_value_size)?;
        let total_shards = u32::try_from(plan.chunk_count()).map_err(|_| {
            StoreError::InvalidState("NoF shard count exceeds provider ABI".to_string())
        })?;
        let mut requests = Vec::with_capacity(total_shards as usize);
        for (shard_id, value) in plan.slices(value)? {
            requests.push(NofObjectShardWrite {
                namespace,
                key,
                value,
                shard_id: u32::try_from(shard_id).map_err(|_| {
                    StoreError::InvalidState("NoF shard index exceeds provider ABI".to_string())
                })?,
                total_shards,
            });
        }

        let results = writer.put_shards(&requests);
        ensure_batch_len("object put", requests.len(), results.len())?;
        for result in results {
            result?;
        }
        let expected = NofObjectMetadata {
            length: value_len,
            total_shards,
        };
        let Some(query) = self.backing.object_query() else {
            return Ok(expected);
        };
        match query.query_object(namespace, key)? {
            NofObjectState::Found(metadata) if metadata == expected => Ok(metadata),
            NofObjectState::Found(_) => Err(StoreError::InvalidState(
                "NoF provider published inconsistent object metadata".to_string(),
            )),
            NofObjectState::Missing => Err(StoreError::NotFound(
                "NoF object missing after successful put".to_string(),
            )),
            NofObjectState::Incomplete => Err(StoreError::Backpressure(
                "NoF object manifest is incomplete after shard put".to_string(),
            )),
        }
    }

    pub fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObjectMetadata>> {
        self.validate_key(key)?;
        self.backing
            .object_query()
            .ok_or_else(|| unsupported("logical-object query"))?
            .query_object(namespace, key)
    }

    pub fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObject>> {
        self.validate_key(key)?;
        self.backing
            .object_read()
            .ok_or_else(|| unsupported("logical-object reads"))?
            .get_object(namespace, key)
    }

    pub fn delete_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<()>> {
        self.validate_key(key)?;
        self.backing
            .object_delete()
            .ok_or_else(|| unsupported("logical-object deletion"))?
            .delete_object(namespace, key)
    }

    fn validate_key(&self, key: &str) -> Result<()> {
        let limits = self
            .object_limits
            .ok_or_else(|| unsupported("object limits"))?;
        let key_len = u64::try_from(key.len())
            .map_err(|_| StoreError::InvalidState("NoF key length does not fit u64".to_string()))?;
        if key_len > limits.max_key_size {
            return Err(StoreError::InvalidState(format!(
                "NoF key is {key_len} bytes, exceeding provider limit {}",
                limits.max_key_size
            )));
        }
        Ok(())
    }
}

fn unsupported(capability: &str) -> StoreError {
    StoreError::Unsupported(format!("NoF backing does not expose {capability}"))
}

/// Collision-free namespace projection for string-based provider APIs.
#[cfg(any(feature = "kvcs-capi", test))]
pub(crate) fn encode_namespace(namespace: &NamespaceScope) -> Result<String> {
    let fields = [
        namespace.tenant.as_str(),
        namespace.domain.as_str(),
        namespace.object_set.as_str(),
    ];
    if fields.iter().any(|field| field.is_empty()) {
        return Err(StoreError::InvalidState(
            "NoF namespace components must not be empty".to_string(),
        ));
    }
    Ok(format!(
        "moon:v1:{}:{}:{}:{}:{}:{}",
        fields[0].len(),
        fields[0],
        fields[1].len(),
        fields[1],
        fields[2].len(),
        fields[2]
    ))
}

#[cfg(test)]
#[path = "backend_tests.rs"]
mod tests;
