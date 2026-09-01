//! Capability composition for a NoF backing.

use super::external_metadata::NofExternalMetadata;
use super::object::{
    NofObjectDelete, NofObjectLimits, NofObjectQuery, NofObjectRead, NofObjectWrite,
};
use super::physical::{
    NofDeviceManagement, NofHealth, NofPhysicalDelete, NofPhysicalLimits, NofPhysicalQuery,
    NofPhysicalRead, NofPhysicalWrite, NofStorageManagement,
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

    fn physical_limits(&self) -> Option<NofPhysicalLimits> {
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

    fn metadata(&self) -> Option<&dyn NofExternalMetadata> {
        None
    }

    fn health_capability(&self) -> Option<&dyn NofHealth> {
        None
    }

    fn storage_management(&self) -> Option<&dyn NofStorageManagement> {
        None
    }

    fn device_management(&self) -> Option<&dyn NofDeviceManagement> {
        None
    }
}
