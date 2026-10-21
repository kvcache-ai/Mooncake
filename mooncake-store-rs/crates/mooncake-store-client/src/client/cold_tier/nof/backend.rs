use std::sync::Arc;

use mooncake_store_core::{NamespaceScope, Result, StoreError};

use crate::client::cold_tier::layout::ValueChunkPlan;
use crate::client::PersistentStorageBackendHealth;

use super::backing::NofBacking;
use super::ensure_batch_len;
use super::managed::NofManagedLimits;
use super::object::{NofObjectLimits, NofObjectShardWrite, NofObjectState};

/// Unified NoF facade and capability container.
///
/// A backing may expose logical-object I/O, provider-addressed physical-KV I/O and health
/// independently. The facade never infers one capability from another.
#[derive(Clone)]
pub struct NofBackend {
    pub(crate) backing: Arc<dyn NofBacking>,
    pub(crate) object_limits: Option<NofObjectLimits>,
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
        let managed_capabilities = [
            backing.managed_allocator().is_some(),
            backing.managed_write().is_some(),
            backing.managed_read().is_some(),
        ];
        let managed_count = managed_capabilities
            .iter()
            .filter(|present| **present)
            .count();
        if managed_count != 0 && managed_count != managed_capabilities.len() {
            return Err(StoreError::InvalidState(
                "managed NoF requires allocator, read and write capabilities together".to_string(),
            ));
        }
        let managed_limits = backing
            .managed_limits()
            .map(NofManagedLimits::validate)
            .transpose()?;
        if managed_count != 0 && managed_limits.is_none() {
            return Err(StoreError::InvalidState(
                "managed NoF capabilities require managed limits".to_string(),
            ));
        }
        if managed_count != 0 && has_object_io {
            return Err(StoreError::InvalidState(
                "one NoF backing cannot mix managed and provider-owned object capabilities"
                    .to_string(),
            ));
        }
        Ok(Self {
            backing,
            object_limits,
        })
    }

    pub(in crate::client) fn health_snapshot(&self) -> Result<PersistentStorageBackendHealth> {
        let health = if let Some(health) = self.backing.health_capability() {
            health.health()?.validate()?
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

    pub fn put_object(&self, namespace: &NamespaceScope, key: &str, value: &[u8]) -> Result<()> {
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
        Ok(())
    }

    pub fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<u64>> {
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
    ) -> Result<NofObjectState<Vec<u8>>> {
        self.validate_key(key)?;
        self.backing
            .object_read()
            .ok_or_else(|| unsupported("logical-object reads"))?
            .get_object(namespace, key, None)
    }

    pub(crate) fn get_object_with_known_length(
        &self,
        namespace: &NamespaceScope,
        key: &str,
        length: u64,
    ) -> Result<NofObjectState<Vec<u8>>> {
        self.validate_key(key)?;
        self.backing
            .object_read()
            .ok_or_else(|| unsupported("logical-object reads"))?
            .get_object(namespace, key, Some(length))
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
