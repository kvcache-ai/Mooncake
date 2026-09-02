//! Provider-neutral physical-KV and management capabilities for NoF backings.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::OpaquePhysicalKey;

const MAX_OPAQUE_LOCATOR_LEN: usize = 4096;
pub(crate) const MAX_ENCODED_LOCATOR_HEX_LEN: usize = MAX_OPAQUE_LOCATOR_LEN * 2;

pub struct NofPhysicalWriteRequest<'a> {
    pub key: OpaquePhysicalKey,
    pub value: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct NofPhysicalReadRequest {
    pub key: OpaquePhysicalKey,
    /// Executor-owned layout descriptor persisted opaquely in the Cold Tier route.
    pub locator: NofPhysicalLocator,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug)]
pub struct NofPhysicalDeleteRequest {
    pub key: OpaquePhysicalKey,
    /// Executor-owned layout descriptor persisted opaquely in the Cold Tier route.
    pub locator: NofPhysicalLocator,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NofPhysicalLocator(Vec<u8>);

impl NofPhysicalLocator {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > MAX_OPAQUE_LOCATOR_LEN {
            return Err(StoreError::InvalidState(format!(
                "NoF physical locator length must be in 1..={MAX_OPAQUE_LOCATOR_LEN}"
            )));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofPhysicalRecoveredRecord {
    pub key: OpaquePhysicalKey,
    pub locator: NofPhysicalLocator,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug)]
pub struct NofPhysicalQueryRequest {
    pub key: OpaquePhysicalKey,
    pub locator: NofPhysicalLocator,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofPhysicalObject {
    pub key: OpaquePhysicalKey,
    pub value_size: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NofPhysicalListPage {
    pub objects: Vec<NofPhysicalObject>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NofStorageHealth {
    pub capacity_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
}

impl NofStorageHealth {
    pub(crate) fn validate(self, require_capacity: bool) -> Result<Self> {
        if self.capacity_bytes.is_some() != self.available_bytes.is_some() {
            return Err(StoreError::InvalidState(
                "NoF storage health must report capacity and available bytes together".to_string(),
            ));
        }
        if let (Some(capacity), Some(available)) = (self.capacity_bytes, self.available_bytes) {
            if available > capacity {
                return Err(StoreError::InvalidState(format!(
                    "NoF backing reported available bytes {available} above capacity {capacity}"
                )));
            }
        }
        if require_capacity && self.capacity_bytes.is_none() {
            return Err(StoreError::InvalidState(
                "Mooncake-managed NoF storage must report capacity and available bytes".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Optional backing-level liveness and capacity snapshot.
///
/// A provider may expose liveness without exposing physical inventory or device lifecycle control.
pub trait NofHealth: Send + Sync {
    fn health(&self) -> Result<NofStorageHealth>;
}

/// Complete physical-object writes exposed by a NoF backing. Results are positional.
///
/// The executor owns the persistent layout behind each returned opaque locator: a value may be a
/// single KV record, multiple chunks, one aligned extent, or any other executor-specific format.
pub trait NofPhysicalWrite: Send + Sync {
    fn put_batch(
        &self,
        requests: &[NofPhysicalWriteRequest<'_>],
    ) -> Vec<Result<NofPhysicalLocator>>;

    /// Successful synchronous providers can keep this no-op durability boundary.
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Releases successful writes that cannot be published after a later batch failure.
    ///
    /// Provider-managed engines may keep this no-op and reclaim unpublished objects internally.
    /// Allocator-backed executors should release the referenced allocation immediately.
    fn discard_unpublished(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>> {
        requests.iter().map(|_| Ok(())).collect()
    }
}

/// Optional route-authoritative recovery for allocator-backed executors.
///
/// Provider-managed engines normally omit this capability because placement and physical GC live
/// below their API. Allocator-backed executors use their opaque locators to rebuild allocation
/// state from the live NoF route set without introducing a second metadata journal.
pub trait NofPhysicalRecovery: Send + Sync {
    fn recover(&self, records: &[NofPhysicalRecoveredRecord]) -> Result<()>;
}

/// Complete physical-object reads exposed by a NoF backing. Results are positional.
pub trait NofPhysicalRead: Send + Sync {
    fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>>;
}

/// Complete physical-object deletion exposed by a NoF backing. Results are positional.
pub trait NofPhysicalDelete: Send + Sync {
    fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>>;
}

/// Physical existence queries exposed by a NoF backing. Results are positional.
pub trait NofPhysicalQuery: Send + Sync {
    fn query_batch(&self, requests: &[NofPhysicalQueryRequest]) -> Vec<Result<bool>>;
}

/// Optional Mooncake-facing storage maintenance capability.
///
/// Its presence means Mooncake may enumerate physical objects and apply the existing Cold Tier
/// reconciliation, watermark and compaction policies. Provider-managed storage omits it.
pub trait NofStorageManagement: Send + Sync {
    fn list_objects(&self, cursor: Option<&[u8]>, limit: usize) -> Result<NofPhysicalListPage>;
    fn storage_health(&self) -> Result<NofStorageHealth>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_storage_requires_capacity() {
        assert!(matches!(
            NofStorageHealth::default().validate(true),
            Err(StoreError::InvalidState(message)) if message.contains("capacity")
        ));
    }

    #[test]
    fn health_rejects_available_bytes_above_capacity() {
        assert!(matches!(
            NofStorageHealth {
                capacity_bytes: Some(10),
                available_bytes: Some(11),
            }
            .validate(false),
            Err(StoreError::InvalidState(message)) if message.contains("above capacity")
        ));
    }

    #[test]
    fn health_rejects_partial_capacity_pair() {
        assert!(matches!(
            NofStorageHealth {
                capacity_bytes: Some(10),
                available_bytes: None,
            }
            .validate(false),
            Err(StoreError::InvalidState(message)) if message.contains("together")
        ));
    }
}
