use mooncake_store_core::{
    ColdTierDeviceFilter, ColdTierDeviceRecord, ColdTierDeviceUpdate, ColdTierPutDeviceResult,
    ColdTierUsageDelta, Result, StoreError,
};

use crate::cold_tier::{apply_cold_tier_usage_delta_to_record, cold_tier_device_matches_filter};

use super::InMemoryMetadataBackend;

impl InMemoryMetadataBackend {
    pub(super) fn in_memory_put_cold_tier_device_if_absent(
        &self,
        device: &ColdTierDeviceRecord,
    ) -> Result<ColdTierPutDeviceResult> {
        let key = self.scoped_key(&device.device_id);
        let mut state = self.state.write();
        if let Some(existing) = state.cold_tier_devices.get(&key) {
            return Ok(ColdTierPutDeviceResult::Existing(existing.clone()));
        }
        state.cold_tier_devices.insert(key, device.clone());
        Ok(ColdTierPutDeviceResult::Created(device.clone()))
    }

    pub(super) fn in_memory_get_cold_tier_device(
        &self,
        device_id: &str,
    ) -> Result<Option<ColdTierDeviceRecord>> {
        Ok(self
            .state
            .read()
            .cold_tier_devices
            .get(&self.scoped_key(device_id))
            .cloned())
    }

    pub(super) fn in_memory_list_cold_tier_devices(
        &self,
        filter: &ColdTierDeviceFilter,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        let storage_prefix = self.tenant_storage_prefix();
        let mut devices = self
            .state
            .read()
            .cold_tier_devices
            .iter()
            .filter(|(key, _)| match storage_prefix.as_deref() {
                Some(prefix) => key
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('/')),
                None if self.default_tenant_legacy_mode => true,
                None => !key.starts_with("tenant:"),
            })
            .map(|(_, device)| device)
            .filter(|device| cold_tier_device_matches_filter(device, filter))
            .cloned()
            .collect::<Vec<_>>();
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        Ok(devices)
    }

    pub(super) fn in_memory_update_cold_tier_device(
        &self,
        device_id: &str,
        update: ColdTierDeviceUpdate,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.scoped_key(device_id);
        let mut state = self.state.write();
        let device = state
            .cold_tier_devices
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(device_id.to_string()))?;
        if update
            .expected_updated_at_ms
            .is_some_and(|expected| device.updated_at_ms != expected)
        {
            return Err(StoreError::Conflict(format!(
                "cold tier device {device_id} update timestamp mismatch"
            )));
        }
        update.apply(device);
        Ok(device.clone())
    }

    pub(super) fn in_memory_apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        delta: ColdTierUsageDelta,
        updated_at_ms: u64,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.scoped_key(device_id);
        let mut state = self.state.write();
        let device = state
            .cold_tier_devices
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(device_id.to_string()))?;
        apply_cold_tier_usage_delta_to_record(device, &delta, updated_at_ms)?;
        Ok(device.clone())
    }
}
