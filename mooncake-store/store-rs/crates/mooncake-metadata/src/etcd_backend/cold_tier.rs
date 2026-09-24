use etcd_client::{Compare, CompareOp, GetOptions, Txn, TxnOp};
use mooncake_store_core::{
    ColdTierDeviceFilter, ColdTierDeviceRecord, ColdTierDeviceUpdate, ColdTierPutDeviceResult,
    ColdTierUsageDelta, Result, StoreError,
};

use crate::cold_tier::{apply_cold_tier_usage_delta_to_record, cold_tier_device_matches_filter};

use super::{etcd_error, json_error, EtcdMetadataBackend};

impl EtcdMetadataBackend {
    pub(super) fn etcd_put_cold_tier_device_if_absent(
        &self,
        device: &ColdTierDeviceRecord,
    ) -> Result<ColdTierPutDeviceResult> {
        let key = self.config.keyspace.cold_tier_device(&device.device_id);
        let payload = serde_json::to_string(device).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .txn(
                    Txn::new()
                        .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
                        .and_then([TxnOp::put(key.clone(), payload, None)]),
                )
                .await
                .map_err(etcd_error("etcd create cold tier device"))?;
            if response.succeeded() {
                return Ok(ColdTierPutDeviceResult::Created(device.clone()));
            }
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get existing cold tier device"))?;
            let existing = response.kvs().first().ok_or_else(|| {
                StoreError::Conflict("cold tier device create raced with delete".to_string())
            })?;
            Ok(ColdTierPutDeviceResult::Existing(
                serde_json::from_slice(existing.value()).map_err(json_error)?,
            ))
        })
    }

    pub(super) fn etcd_get_cold_tier_device(
        &self,
        device_id: &str,
    ) -> Result<Option<ColdTierDeviceRecord>> {
        let key = self.config.keyspace.cold_tier_device(device_id);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get cold tier device"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    pub(super) fn etcd_list_cold_tier_devices(
        &self,
        filter: &ColdTierDeviceFilter,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        let prefix = self.config.keyspace.cold_tier_device_prefix();
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list cold tier devices"))?;
            let mut devices = Vec::with_capacity(response.kvs().len());
            for kv in response.kvs() {
                let device: ColdTierDeviceRecord =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                if cold_tier_device_matches_filter(&device, filter) {
                    devices.push(device);
                }
            }
            devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
            Ok(devices)
        })
    }

    pub(super) fn etcd_update_cold_tier_device(
        &self,
        device_id: &str,
        update: ColdTierDeviceUpdate,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.config.keyspace.cold_tier_device(device_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get cold tier device for update"))?;
                let Some(kv) = response.kvs().first() else {
                    return Err(StoreError::NotFound(format!(
                        "cold tier device {device_id} not found"
                    )));
                };
                let mut device: ColdTierDeviceRecord =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                if let Some(expected) = update.expected_updated_at_ms {
                    if device.updated_at_ms != expected {
                        return Err(StoreError::Conflict(format!(
                            "cold tier device {device_id} changed concurrently"
                        )));
                    }
                }
                update.clone().apply(&mut device);
                let payload = serde_json::to_string(&device).map_err(json_error)?;
                let txn_response = client
                    .txn(
                        Txn::new()
                            .when([Compare::mod_revision(
                                key.clone(),
                                CompareOp::Equal,
                                kv.mod_revision(),
                            )])
                            .and_then([TxnOp::put(key.clone(), payload, None)]),
                    )
                    .await
                    .map_err(etcd_error("etcd update cold tier device"))?;
                if txn_response.succeeded() {
                    return Ok(device);
                }
            }
        })
    }

    pub(super) fn etcd_apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        delta: ColdTierUsageDelta,
        updated_at_ms: u64,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.config.keyspace.cold_tier_device(device_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get cold tier device for usage delta"))?;
                let Some(kv) = response.kvs().first() else {
                    return Err(StoreError::NotFound(format!(
                        "cold tier device {device_id} not found"
                    )));
                };
                let mut device: ColdTierDeviceRecord =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                apply_cold_tier_usage_delta_to_record(&mut device, &delta, updated_at_ms)?;
                let payload = serde_json::to_string(&device).map_err(json_error)?;
                let txn_response = client
                    .txn(
                        Txn::new()
                            .when([Compare::mod_revision(
                                key.clone(),
                                CompareOp::Equal,
                                kv.mod_revision(),
                            )])
                            .and_then([TxnOp::put(key.clone(), payload, None)]),
                    )
                    .await
                    .map_err(etcd_error("etcd apply cold tier usage delta"))?;
                if txn_response.succeeded() {
                    return Ok(device);
                }
            }
        })
    }
}
