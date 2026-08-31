//! Provider-neutral low-level NoF executor contract.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::OpaquePhysicalKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofLowLevelCapabilities {
    pub max_value_size: u64,
    pub max_batch_items: usize,
    pub max_batch_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct NofLowLevelGetRequest {
    pub key: OpaquePhysicalKey,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug)]
pub struct NofLowLevelDeleteRequest {
    pub key: OpaquePhysicalKey,
}

impl NofLowLevelCapabilities {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_value_size == 0 || self.max_batch_items == 0 || self.max_batch_bytes == 0 {
            return Err(StoreError::InvalidState(
                "NoF low-level executor must advertise non-zero value and batch limits".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Physical KV executor. Results are positional and must match the request count.
pub trait NofLowLevelExecutor: Send + Sync {
    fn capabilities(&self) -> NofLowLevelCapabilities;

    fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>>;

    fn get_batch(&self, requests: &[NofLowLevelGetRequest]) -> Vec<Result<Option<Vec<u8>>>>;

    fn delete_batch(&self, requests: &[NofLowLevelDeleteRequest]) -> Vec<Result<()>>;

    /// Successful synchronous providers can keep this no-op durability boundary.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn repeated_error<T>(len: usize, error: StoreError) -> Vec<Result<T>> {
    std::iter::repeat_with(|| Err(error.clone()))
        .take(len)
        .collect()
}
