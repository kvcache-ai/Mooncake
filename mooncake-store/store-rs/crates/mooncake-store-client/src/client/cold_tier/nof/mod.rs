//! Capability-based NoF integration within the existing Cold Tier subsystem.
//!
//! NoF backings are not divided into high-level and low-level framework branches. A backing
//! advertises only the logical-object, physical-KV, query, metadata, health, storage-maintenance
//! and device-management traits that Mooncake may call.

use mooncake_store_core::{Result, StoreError};

mod backend;
mod backing;
pub(in crate::client) mod external_metadata;
mod object;
mod physical;
pub(in crate::client) mod physical_adapter;

#[cfg(feature = "kvcs-capi")]
mod kvcs;

pub use backend::NofBackend;
pub use backing::NofBacking;
pub use external_metadata::NofExternalMetadata;
pub use object::{
    NofObject, NofObjectDelete, NofObjectLimits, NofObjectMetadata, NofObjectQuery, NofObjectRead,
    NofObjectShardWrite, NofObjectState, NofObjectWrite,
};
pub use physical::{
    NofDeviceManagement, NofHealth, NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalLimits,
    NofPhysicalListPage, NofPhysicalObject, NofPhysicalQuery, NofPhysicalRead,
    NofPhysicalReadRequest, NofPhysicalWrite, NofStorageHealth, NofStorageManagement,
};

pub use crate::client::cold_tier::layout::{
    OpaquePhysicalKey, PhysicalKeyCodec, PhysicalKeyInput, Sha256PhysicalKeyCodec,
};

#[cfg(feature = "kvcs-capi")]
pub use kvcs::{KvcsCapiExecutor, KvcsMode};

pub(crate) fn ensure_batch_len(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(StoreError::Transport(format!(
            "NoF backing returned {actual} {operation} results for {expected} requests"
        )))
    }
}
