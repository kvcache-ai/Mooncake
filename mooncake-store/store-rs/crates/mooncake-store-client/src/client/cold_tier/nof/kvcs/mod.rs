//! KVCS-specific C API bindings and the mode-selecting NoF executor.

use mooncake_store_core::{Result, StoreError};

use super::OpaquePhysicalKey;

include!("capi.rs");

mod executor;
mod physical_layout;
pub use executor::{KvcsCapiExecutor, KvcsMode};

#[cfg(test)]
mod capi_tests;
