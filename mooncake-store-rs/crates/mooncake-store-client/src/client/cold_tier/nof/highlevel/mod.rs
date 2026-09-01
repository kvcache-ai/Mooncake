use std::sync::Arc;

use mooncake_store_core::{NamespaceScope, Result, StoreError};

use crate::client::cold_tier::layout::ValueChunkPlan;

use super::{
    ensure_batch_len, NofDeviceManagement, NofMetadataOwnership, NofOwnership,
    NofStorageMaintenance,
};

mod executor;
#[cfg(feature = "kvcs-capi")]
pub(crate) use executor::repeated_error;
pub use executor::{
    NofHighLevelCapabilities, NofHighLevelExecutor, NofHighLevelObject, NofHighLevelObjectMetadata,
    NofHighLevelRead, NofHighLevelShardPut,
};
#[cfg(feature = "kvcs-capi")]
mod kvcs_executor;
#[cfg(feature = "kvcs-capi")]
pub use kvcs_executor::KvcsCapiStandardExecutor;

/// High-level facade that performs provider-independent shard planning only.
pub struct NofHighLevelBackend {
    executor: Arc<dyn NofHighLevelExecutor>,
    capabilities: NofHighLevelCapabilities,
}

impl NofHighLevelBackend {
    pub fn new(executor: Arc<dyn NofHighLevelExecutor>) -> Result<Self> {
        let capabilities = executor.capabilities().validate()?;
        Ok(Self {
            executor,
            capabilities,
        })
    }

    pub fn ownership(&self) -> NofOwnership {
        NofOwnership {
            metadata: NofMetadataOwnership::ProviderManaged,
            maintenance: NofStorageMaintenance::ProviderManaged,
            devices: NofDeviceManagement::ProviderManaged,
        }
    }

    pub fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()> {
        self.executor.init_namespace(namespace)
    }

    pub fn put_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
        value: &[u8],
    ) -> Result<NofHighLevelObjectMetadata> {
        self.validate_key(key)?;
        if value.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF high-level objects must not contain an empty shard".to_string(),
            ));
        }
        let value_len = u64::try_from(value.len()).map_err(|_| {
            StoreError::InvalidState("NoF high-level value length does not fit u64".to_string())
        })?;
        let plan = ValueChunkPlan::new(value_len, self.capabilities.max_value_size)?;
        let total_shards = if plan.chunk_count() <= 1 {
            1
        } else {
            u32::try_from(plan.chunk_count()).map_err(|_| {
                StoreError::InvalidState(
                    "NoF high-level shard count exceeds provider ABI".to_string(),
                )
            })?
        };
        let mut requests = Vec::with_capacity(total_shards as usize);
        if plan.chunk_count() == 0 {
            requests.push(NofHighLevelShardPut {
                namespace,
                key,
                value,
                shard_id: 0,
                total_shards: 1,
            });
        } else {
            for shard_id in 0..plan.chunk_count() {
                let range = plan.range(shard_id)?;
                let start = usize::try_from(range.start).map_err(|_| {
                    StoreError::InvalidState("NoF high-level shard start overflow".to_string())
                })?;
                let end = usize::try_from(range.end).map_err(|_| {
                    StoreError::InvalidState("NoF high-level shard end overflow".to_string())
                })?;
                requests.push(NofHighLevelShardPut {
                    namespace,
                    key,
                    value: &value[start..end],
                    shard_id: u32::try_from(shard_id).map_err(|_| {
                        StoreError::InvalidState(
                            "NoF high-level shard index exceeds provider ABI".to_string(),
                        )
                    })?,
                    total_shards,
                });
            }
        }

        let results = self.executor.put_shards(&requests);
        ensure_batch_len("high-level put", requests.len(), results.len())?;
        for result in results {
            result?;
        }
        match self.executor.query_object(namespace, key)? {
            NofHighLevelRead::Found(metadata) => {
                if metadata.length != value_len || metadata.total_shards != total_shards {
                    return Err(StoreError::InvalidState(
                        "NoF high-level provider published inconsistent object metadata"
                            .to_string(),
                    ));
                }
                Ok(metadata)
            }
            NofHighLevelRead::Missing => Err(StoreError::NotFound(
                "NoF high-level object missing after successful put".to_string(),
            )),
            NofHighLevelRead::Incomplete => Err(StoreError::Backpressure(
                "NoF high-level object manifest is incomplete after shard put".to_string(),
            )),
        }
    }

    pub fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObjectMetadata>> {
        self.validate_key(key)?;
        self.executor.query_object(namespace, key)
    }

    pub fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObject>> {
        self.validate_key(key)?;
        self.executor.get_object(namespace, key)
    }

    pub fn delete_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<()>> {
        self.validate_key(key)?;
        self.executor.delete_object(namespace, key)
    }

    fn validate_key(&self, key: &str) -> Result<()> {
        let key_len = u64::try_from(key.len()).map_err(|_| {
            StoreError::InvalidState("NoF high-level key length does not fit u64".to_string())
        })?;
        if key_len > self.capabilities.max_key_size {
            return Err(StoreError::InvalidState(format!(
                "NoF high-level key is {key_len} bytes, exceeding provider limit {}",
                self.capabilities.max_key_size
            )));
        }
        Ok(())
    }
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
mod tests;
