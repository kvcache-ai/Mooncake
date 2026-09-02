//! Capability-based NoF integration within the existing Cold Tier subsystem.
//!
//! NoF backings are not divided into high-level and low-level framework branches. A backing
//! advertises only the logical-object, provider-addressed physical-KV, query and health traits
//! that Mooncake may call.

use mooncake_store_core::{Result, StoreError};

mod backend;
mod backing;
mod object;
mod physical;
pub(in crate::client) mod physical_backend;
mod runtime;

#[cfg(feature = "kvcs-capi")]
mod kvcs;

pub(in crate::client) use super::owner::{
    upsert_client_lease_preserving_nof_labels, NOF_TARGET_SET_LABEL,
};
pub use backend::NofBackend;
pub use backing::NofBacking;
pub use object::{
    NofObject, NofObjectDelete, NofObjectLimits, NofObjectMetadata, NofObjectQuery, NofObjectRead,
    NofObjectShardWrite, NofObjectState, NofObjectWrite,
};
pub use physical::{
    NofHealth, NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalQuery,
    NofPhysicalQueryRequest, NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalWrite,
    NofPhysicalWriteRequest, NofStorageHealth,
};
pub use runtime::NofTargetConfig;
pub(in crate::client) use runtime::{target_set_fingerprint, NofTargetManager};

pub use crate::client::cold_tier::layout::{
    derive_physical_key, OpaquePhysicalKey, PhysicalKeyInput,
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
