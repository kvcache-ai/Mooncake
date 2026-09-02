//! Provider-addressed physical-KV capabilities for NoF backings.
//!
//! These operations address complete objects by a deterministic key and return no allocation
//! locator. They fit providers such as KVCS that own their lookup metadata. An executor that
//! allocates extents and returns an opaque location requires Mooncake-managed route metadata and
//! must not be adapted through these traits.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::OpaquePhysicalKey;

pub struct NofPhysicalWriteRequest<'a> {
    pub key: OpaquePhysicalKey,
    pub value: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct NofPhysicalObjectRequest {
    pub key: OpaquePhysicalKey,
}

pub type NofPhysicalReadRequest = NofPhysicalObjectRequest;
pub type NofPhysicalDeleteRequest = NofPhysicalObjectRequest;
pub type NofPhysicalQueryRequest = NofPhysicalObjectRequest;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NofStorageHealth {
    pub capacity_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
}

impl NofStorageHealth {
    pub(crate) fn validate(self) -> Result<Self> {
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
        Ok(self)
    }
}

/// Optional backing-level liveness and capacity snapshot.
///
/// A provider may expose liveness without exposing physical inventory or device lifecycle control.
pub trait NofHealth: Send + Sync {
    fn health(&self) -> Result<NofStorageHealth>;
}

/// Key-addressed complete-object writes exposed by a NoF backing. Results are positional.
///
/// The executor derives its persistent layout from the opaque object key. A value may be one
/// record or multiple executor-private records.
pub trait NofPhysicalWrite: Send + Sync {
    fn put_batch(&self, requests: &[NofPhysicalWriteRequest<'_>]) -> Vec<Result<()>>;
}

/// Key-addressed complete-object reads exposed by a NoF backing. Results are positional.
pub trait NofPhysicalRead: Send + Sync {
    fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>>;
}

/// Key-addressed complete-object deletion exposed by a NoF backing. Results are positional.
pub trait NofPhysicalDelete: Send + Sync {
    fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>>;
}

/// Key-addressed existence queries exposed by a NoF backing. Results are positional.
pub trait NofPhysicalQuery: Send + Sync {
    fn query_batch(&self, requests: &[NofPhysicalQueryRequest]) -> Vec<Result<bool>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_rejects_available_bytes_above_capacity() {
        assert!(matches!(
            NofStorageHealth {
                capacity_bytes: Some(10),
                available_bytes: Some(11),
            }
            .validate(),
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
            .validate(),
            Err(StoreError::InvalidState(message)) if message.contains("together")
        ));
    }
}
