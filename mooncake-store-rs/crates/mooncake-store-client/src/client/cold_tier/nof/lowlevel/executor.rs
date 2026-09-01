//! Provider-neutral low-level NoF executor contract.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::OpaquePhysicalKey;

use super::super::{
    NofDeviceManagement, NofMetadataOwnership, NofOwnership, NofStorageMaintenance,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofLowLevelCapabilities {
    pub max_value_size: u64,
    pub max_batch_items: usize,
    pub max_batch_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct NofLowLevelGetRequest {
    pub key: OpaquePhysicalKey,
    pub expected_value_size: usize,
}

#[derive(Clone, Debug)]
pub struct NofLowLevelDeleteRequest {
    pub key: OpaquePhysicalKey,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NofLowLevelObject {
    pub key: OpaquePhysicalKey,
    pub value_size: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NofLowLevelListPage {
    pub objects: Vec<NofLowLevelObject>,
    pub next_cursor: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NofLowLevelStorageHealth {
    pub capacity_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
}

impl NofLowLevelStorageHealth {
    pub(crate) fn validate(self, ownership: NofOwnership) -> Result<Self> {
        if let (Some(capacity), Some(available)) = (self.capacity_bytes, self.available_bytes) {
            if available > capacity {
                return Err(StoreError::InvalidState(format!(
                    "NoF low-level executor reported available bytes {available} above capacity {capacity}"
                )));
            }
        }
        if (ownership.maintenance == NofStorageMaintenance::MooncakeManaged
            || ownership.devices == NofDeviceManagement::MooncakeManaged)
            && (self.capacity_bytes.is_none() || self.available_bytes.is_none())
        {
            return Err(StoreError::InvalidState(
                "Mooncake-managed NoF maintenance or devices must report capacity and available bytes"
                    .to_string(),
            ));
        }
        Ok(self)
    }
}

impl NofLowLevelCapabilities {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_value_size == 0 || self.max_batch_items == 0 || self.max_batch_bytes == 0 {
            return Err(StoreError::InvalidState(
                "NoF low-level executor must advertise non-zero value and batch limits".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Physical KV executor. Results are positional and must match the request count.
pub trait NofLowLevelExecutor: Send + Sync {
    fn capabilities(&self) -> NofLowLevelCapabilities;

    fn ownership(&self) -> NofOwnership {
        // All management capabilities are optional. The minimal raw-KV contract keeps logical
        // routes in Mooncake and assumes the provider owns physical maintenance and disks.
        NofOwnership {
            metadata: NofMetadataOwnership::ExternalMetadata,
            maintenance: NofStorageMaintenance::ProviderManaged,
            devices: NofDeviceManagement::ProviderManaged,
        }
    }

    fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>>;

    fn get_batch(&self, requests: &[NofLowLevelGetRequest]) -> Vec<Result<Option<Vec<u8>>>>;

    fn delete_batch(&self, requests: &[NofLowLevelDeleteRequest]) -> Vec<Result<()>>;

    /// Returns physical keys in this executor target's Mooncake-owned scope.
    ///
    /// Mooncake-managed executors must provide a stable, exhaustive paged scan. Provider-managed
    /// executors may keep the default unsupported implementation because their backend owns
    /// physical orphan cleanup and storage watermarks. This is deliberately not emulated by
    /// spawning a provider CLI that is outside the executor's supported SDK contract.
    fn list_objects(&self, _cursor: Option<&[u8]>, _limit: usize) -> Result<NofLowLevelListPage> {
        Err(StoreError::Unsupported(
            "NoF low-level executor does not expose physical object listing".to_string(),
        ))
    }

    /// Checks whether physical keys exist without reading their values.
    fn query_batch(&self, _keys: &[OpaquePhysicalKey]) -> Vec<Result<bool>> {
        repeated_error(
            _keys.len(),
            StoreError::Unsupported(
                "NoF low-level executor does not expose physical key queries".to_string(),
            ),
        )
    }

    /// Checks target liveness and reports capacity when Mooncake manages devices.
    fn storage_health(&self) -> Result<NofLowLevelStorageHealth> {
        Err(StoreError::Unsupported(
            "NoF low-level executor does not expose storage health".to_string(),
        ))
    }

    /// Successful synchronous providers can keep this no-op durability boundary.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn repeated_error<T>(len: usize, error: StoreError) -> Vec<Result<T>> {
    std::iter::repeat_with(|| Err(error.clone()))
        .take(len)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ownership(maintenance: NofStorageMaintenance, devices: NofDeviceManagement) -> NofOwnership {
        NofOwnership {
            metadata: NofMetadataOwnership::ExternalMetadata,
            maintenance,
            devices,
        }
    }

    #[test]
    fn provider_managed_health_may_omit_physical_capacity() {
        assert!(NofLowLevelStorageHealth::default()
            .validate(ownership(
                NofStorageMaintenance::ProviderManaged,
                NofDeviceManagement::ProviderManaged,
            ))
            .is_ok());
    }

    #[test]
    fn mooncake_managed_maintenance_requires_physical_capacity() {
        assert!(matches!(
            NofLowLevelStorageHealth::default().validate(ownership(
                NofStorageMaintenance::MooncakeManaged,
                NofDeviceManagement::ProviderManaged,
            )),
            Err(StoreError::InvalidState(message)) if message.contains("capacity")
        ));
    }

    #[test]
    fn health_rejects_available_bytes_above_capacity() {
        assert!(matches!(
            NofLowLevelStorageHealth {
                capacity_bytes: Some(10),
                available_bytes: Some(11),
            }
            .validate(ownership(
                NofStorageMaintenance::MooncakeManaged,
                NofDeviceManagement::MooncakeManaged,
            )),
            Err(StoreError::InvalidState(message)) if message.contains("above capacity")
        ));
    }
}
