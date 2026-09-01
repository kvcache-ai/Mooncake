//! Cold Tier NoF provider seams and their high-level/low-level execution paths.
//!
//! High-level providers own namespaces, placement, manifests, replicas, and object visibility.
//! Low-level providers expose physical KV I/O and use Mooncake's existing Cold Tier metadata.
//! Storage maintenance is an independent capability: a provider such as KVCS may own physical
//! GC and watermarks, while another low-level executor may delegate them to Mooncake.

use mooncake_store_core::{Result, StoreError};

pub(in crate::client) mod external_metadata;
pub mod highlevel;
pub mod lowlevel;

pub use external_metadata::NofExternalMetadata;

#[cfg(feature = "kvcs-capi")]
pub use highlevel::KvcsCapiStandardExecutor;
pub use highlevel::{
    NofHighLevelBackend, NofHighLevelCapabilities, NofHighLevelExecutor, NofHighLevelObject,
    NofHighLevelObjectMetadata, NofHighLevelRead, NofHighLevelShardPut,
};
#[cfg(feature = "kvcs-capi")]
pub use lowlevel::KvcsCapiLowLevelExecutor;
pub use lowlevel::{
    NofLowLevelCapabilities, NofLowLevelDeleteRequest, NofLowLevelExecutor, NofLowLevelGetRequest,
    NofLowLevelListPage, NofLowLevelObject, NofLowLevelStorageHealth, NofLowLevelTarget,
    OpaquePhysicalKey, PhysicalKeyCodec, PhysicalKeyInput, Sha256PhysicalKeyCodec,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NofMetadataOwnership {
    /// The provider owns logical object metadata, placement, replicas, and visibility.
    ProviderManaged,
    /// Mooncake stores logical routes in its existing external MetadataBackend.
    ExternalMetadata,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NofStorageMaintenance {
    /// The provider owns physical inventory, garbage collection, and storage watermarks.
    ProviderManaged,
    /// Mooncake owns physical reconciliation, garbage collection, and storage watermarks.
    MooncakeManaged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NofDeviceManagement {
    /// The provider owns physical disk discovery, health, offline/recovery, and rebuild.
    ProviderManaged,
    /// Mooncake owns physical device discovery, health, retirement, and rebuild.
    MooncakeManaged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofOwnership {
    pub metadata: NofMetadataOwnership,
    pub maintenance: NofStorageMaintenance,
    pub devices: NofDeviceManagement,
}

pub(crate) fn ensure_batch_len(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(StoreError::Transport(format!(
            "NoF executor returned {actual} {operation} results for {expected} requests"
        )))
    }
}

// The raw C ABI is shared by the two KVCS executors. It is included in this parent module so
// highlevel/kvcs_executor.rs and lowlevel/kvcs_executor.rs can reuse one set of declarations
// without exposing FFI details as a public NoF layer.
#[cfg(feature = "kvcs-capi")]
include!("kvcs_ffi.rs");

#[cfg(all(test, feature = "kvcs-capi"))]
mod kvcs_ffi_tests;
