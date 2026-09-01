//! Provider-neutral physical-KV and management capabilities for NoF backings.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::OpaquePhysicalKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofPhysicalLimits {
    pub max_value_size: u64,
    pub max_batch_items: usize,
    pub max_batch_bytes: u64,
}

impl NofPhysicalLimits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_value_size == 0 || self.max_batch_items == 0 || self.max_batch_bytes == 0 {
            return Err(StoreError::InvalidState(
                "NoF physical backing must advertise non-zero value and batch limits".to_string(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct NofPhysicalReadRequest {
    pub key: OpaquePhysicalKey,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug)]
pub struct NofPhysicalDeleteRequest {
    pub key: OpaquePhysicalKey,
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

/// Physical writes exposed by a NoF backing. Results are positional.
pub trait NofPhysicalWrite: Send + Sync {
    fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>>;

    /// Successful synchronous providers can keep this no-op durability boundary.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// Physical reads exposed by a NoF backing. Results are positional.
pub trait NofPhysicalRead: Send + Sync {
    fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>>;
}

/// Physical deletion exposed by a NoF backing. Results are positional.
pub trait NofPhysicalDelete: Send + Sync {
    fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>>;
}

/// Physical existence queries exposed by a NoF backing. Results are positional.
pub trait NofPhysicalQuery: Send + Sync {
    fn query_batch(&self, keys: &[OpaquePhysicalKey]) -> Vec<Result<bool>>;
}

/// Optional Mooncake-facing storage maintenance capability.
///
/// Its presence means Mooncake may enumerate physical objects and apply the existing Cold Tier
/// reconciliation, watermark and compaction policies. Provider-managed storage omits it.
pub trait NofStorageManagement: Send + Sync {
    fn list_objects(&self, cursor: Option<&[u8]>, limit: usize) -> Result<NofPhysicalListPage>;
    fn storage_health(&self) -> Result<NofStorageHealth>;
}

/// Optional Mooncake-facing device lifecycle capability.
///
/// Provider-managed discovery, offline/recovery and rebuild are represented by the absence of this
/// capability; Mooncake must not emulate them through an undocumented side channel.
pub trait NofDeviceManagement: Send + Sync {
    fn device_health(&self) -> Result<NofStorageHealth>;
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
