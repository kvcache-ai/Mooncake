//! Capability composition for a NoF backing.

use mooncake_store_core::{ColdBackingRoute, Result, StoreError};

use super::object::{
    NofObjectDelete, NofObjectLimits, NofObjectQuery, NofObjectRead, NofObjectWrite,
};
use super::physical::{
    NofHealth, NofPhysicalDelete, NofPhysicalQuery, NofPhysicalRead, NofPhysicalRecovery,
    NofPhysicalWrite, NofStorageManagement,
};

/// Runtime NoF backing assembled from optional, provider-neutral capabilities.
///
/// Capability absence is authoritative: it means the provider owns that responsibility internally
/// or does not expose it through a supported API. Callers must not infer missing capabilities from
/// a provider mode or emulate them through provider CLIs. The advertised capability set and limits
/// must remain stable for the lifetime of the backing.
pub trait NofBacking: Send + Sync {
    fn object_limits(&self) -> Option<NofObjectLimits> {
        None
    }

    fn object_write(&self) -> Option<&dyn NofObjectWrite> {
        None
    }

    fn object_read(&self) -> Option<&dyn NofObjectRead> {
        None
    }

    fn object_query(&self) -> Option<&dyn NofObjectQuery> {
        None
    }

    fn object_delete(&self) -> Option<&dyn NofObjectDelete> {
        None
    }

    fn physical_write(&self) -> Option<&dyn NofPhysicalWrite> {
        None
    }

    fn physical_read(&self) -> Option<&dyn NofPhysicalRead> {
        None
    }

    fn physical_query(&self) -> Option<&dyn NofPhysicalQuery> {
        None
    }

    fn physical_delete(&self) -> Option<&dyn NofPhysicalDelete> {
        None
    }

    fn physical_recovery(&self) -> Option<&dyn NofPhysicalRecovery> {
        None
    }

    fn health_capability(&self) -> Option<&dyn NofHealth> {
        None
    }

    fn storage_management(&self) -> Option<&dyn NofStorageManagement> {
        None
    }
}

pub(crate) fn validate_payload(route: &ColdBackingRoute, payload: &[u8]) -> Result<()> {
    let actual_len = u64::try_from(payload.len())
        .map_err(|_| StoreError::InvalidState("NoF payload length does not fit u64".to_string()))?;
    if actual_len != route.length {
        return Err(StoreError::InvalidState(format!(
            "NoF payload length {actual_len} does not match route length {}",
            route.length
        )));
    }
    if route
        .checksum
        .is_some_and(|expected| expected != crate::client::payload_checksum(payload))
    {
        return Err(StoreError::InvalidState(
            "NoF payload checksum does not match route checksum".to_string(),
        ));
    }
    Ok(())
}
