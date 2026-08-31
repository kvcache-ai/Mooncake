//! Cold Tier NoF provider seams and their high-level/low-level execution paths.
//!
//! High-level providers own namespaces, placement, manifests, replicas, and object visibility.
//! Low-level providers expose physical KV I/O and are wrapped by Mooncake's existing Cold Tier
//! backend contract, so route CAS, replica selection, admission, GC, and rebuild stay in one place.

use mooncake_store_core::{Result, StoreError};

pub mod highlevel;
pub mod lowlevel;

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
    NofLowLevelTarget, OpaquePhysicalKey, PhysicalKeyCodec, PhysicalKeyInput,
    Sha256PhysicalKeyCodec,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NofOwnership {
    /// The SDK owns object metadata, placement, replicas, and maintenance.
    Provider,
    /// Mooncake Cold Tier owns routes, replicas, and maintenance around physical I/O.
    MooncakeColdTier,
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
