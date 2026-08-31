//! Provider-neutral high-level NoF executor contract.

use mooncake_store_core::{NamespaceScope, Result, StoreError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofHighLevelCapabilities {
    pub max_value_size: u64,
    pub max_key_size: u64,
}

impl NofHighLevelCapabilities {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_value_size == 0 || self.max_key_size == 0 {
            return Err(StoreError::InvalidState(
                "NoF high-level executor must advertise non-zero key/value limits".to_string(),
            ));
        }
        Ok(self)
    }
}

pub struct NofHighLevelShardPut<'a> {
    pub namespace: &'a NamespaceScope,
    pub key: &'a str,
    pub value: &'a [u8],
    pub shard_id: u32,
    pub total_shards: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofHighLevelObjectMetadata {
    pub length: u64,
    pub total_shards: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofHighLevelObject {
    pub metadata: NofHighLevelObjectMetadata,
    pub value: Vec<u8>,
}

/// `Incomplete` is never equivalent to `Missing`; callers should retry it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NofHighLevelRead<T> {
    Found(T),
    Missing,
    Incomplete,
}

impl<T> NofHighLevelRead<T> {
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> NofHighLevelRead<U> {
        match self {
            Self::Found(value) => NofHighLevelRead::Found(map(value)),
            Self::Missing => NofHighLevelRead::Missing,
            Self::Incomplete => NofHighLevelRead::Incomplete,
        }
    }
}

/// High-level object executor. The provider is the sole metadata/placement authority.
pub trait NofHighLevelExecutor: Send + Sync {
    fn capabilities(&self) -> NofHighLevelCapabilities;
    fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()>;
    fn put_shards(&self, requests: &[NofHighLevelShardPut<'_>]) -> Vec<Result<()>>;
    fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObjectMetadata>>;
    fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObject>>;
    fn delete_object(&self, namespace: &NamespaceScope, key: &str) -> Result<NofHighLevelRead<()>>;
}

pub(crate) fn repeated_error<T>(len: usize, error: StoreError) -> Vec<Result<T>> {
    std::iter::repeat_with(|| Err(error.clone()))
        .take(len)
        .collect()
}
