//! Provider-neutral logical-object capabilities for NoF backings.

use mooncake_store_core::{NamespaceScope, Result, StoreError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofObjectLimits {
    pub max_value_size: u64,
    pub max_key_size: u64,
}

impl NofObjectLimits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_value_size == 0 || self.max_key_size == 0 {
            return Err(StoreError::InvalidState(
                "NoF object backing must advertise non-zero key/value limits".to_string(),
            ));
        }
        Ok(self)
    }
}

pub struct NofObjectShardWrite<'a> {
    pub namespace: &'a NamespaceScope,
    pub key: &'a str,
    pub value: &'a [u8],
    pub shard_id: u32,
    pub total_shards: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofObjectMetadata {
    pub length: u64,
    pub total_shards: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofObject {
    pub metadata: NofObjectMetadata,
    pub value: Vec<u8>,
}

/// `Incomplete` is never equivalent to `Missing`; callers should retry it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NofObjectState<T> {
    Found(T),
    Missing,
    Incomplete,
}

/// Logical-object writes exposed by a NoF backing.
pub trait NofObjectWrite: Send + Sync {
    fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()>;
    fn put_shards(&self, requests: &[NofObjectShardWrite<'_>]) -> Vec<Result<()>>;
}

/// Logical-object reads exposed by a NoF backing.
pub trait NofObjectRead: Send + Sync {
    fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObject>>;
}

/// Logical-object metadata queries exposed by a NoF backing.
pub trait NofObjectQuery: Send + Sync {
    fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObjectMetadata>>;
}

/// Logical-object deletion exposed by a NoF backing.
pub trait NofObjectDelete: Send + Sync {
    fn delete_object(&self, namespace: &NamespaceScope, key: &str) -> Result<NofObjectState<()>>;
}

pub(crate) fn repeated_error<T>(len: usize, error: StoreError) -> Vec<Result<T>> {
    std::iter::repeat_with(|| Err(error.clone()))
        .take(len)
        .collect()
}
