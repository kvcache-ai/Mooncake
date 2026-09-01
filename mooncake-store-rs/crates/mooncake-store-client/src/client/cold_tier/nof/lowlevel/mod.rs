use std::sync::Arc;

use mooncake_store_core::{Result, StoreError};

pub use crate::client::cold_tier::layout::{
    OpaquePhysicalKey, PhysicalKeyCodec, PhysicalKeyInput, Sha256PhysicalKeyCodec,
};

use super::NofOwnership;

pub(in crate::client) mod backend;
mod executor;
pub(crate) use executor::repeated_error;
pub use executor::{
    NofLowLevelCapabilities, NofLowLevelDeleteRequest, NofLowLevelExecutor, NofLowLevelGetRequest,
    NofLowLevelListPage, NofLowLevelObject, NofLowLevelStorageHealth,
};
#[cfg(feature = "kvcs-capi")]
mod kvcs_executor;
#[cfg(feature = "kvcs-capi")]
pub use kvcs_executor::KvcsCapiLowLevelExecutor;

/// Configuration that binds one physical executor to an existing Cold Tier device ID.
#[derive(Clone)]
pub struct NofLowLevelTarget {
    pub(crate) executor: Arc<dyn NofLowLevelExecutor>,
    pub(crate) key_codec: Arc<dyn PhysicalKeyCodec>,
    pub(crate) key_domain: Vec<u8>,
}

impl NofLowLevelTarget {
    pub fn new(executor: Arc<dyn NofLowLevelExecutor>) -> Self {
        Self {
            executor,
            key_codec: Arc::new(Sha256PhysicalKeyCodec),
            key_domain: b"mooncake:nof-low-level:v1".to_vec(),
        }
    }

    pub fn key_codec(mut self, codec: Arc<dyn PhysicalKeyCodec>) -> Self {
        self.key_codec = codec;
        self
    }

    pub fn key_domain(mut self, domain: impl Into<Vec<u8>>) -> Result<Self> {
        let domain = domain.into();
        if domain.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF low-level key domain must not be empty".to_string(),
            ));
        }
        self.key_domain = domain;
        Ok(self)
    }

    pub fn ownership(&self) -> NofOwnership {
        self.executor.ownership()
    }
}
