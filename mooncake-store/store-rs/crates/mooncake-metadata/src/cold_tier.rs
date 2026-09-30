use mooncake_store_core::{
    ColdTierDeviceFilter, ColdTierDeviceRecord, ColdTierUsageDelta, Result, StoreError,
};

pub(crate) fn cold_tier_device_matches_filter(
    device: &ColdTierDeviceRecord,
    filter: &ColdTierDeviceFilter,
) -> bool {
    filter
        .stable_id
        .as_ref()
        .is_none_or(|stable_id| device.stable_id == *stable_id)
        && filter.state.is_none_or(|state| device.state == state)
        && filter.kind.as_ref().is_none_or(|kind| device.kind == *kind)
        && filter
            .schedulable
            .is_none_or(|schedulable| device.schedulable() == schedulable)
}

pub(crate) fn apply_cold_tier_usage_delta_to_record(
    device: &mut ColdTierDeviceRecord,
    delta: &ColdTierUsageDelta,
    updated_at_ms: u64,
) -> Result<()> {
    device.used_bytes = apply_nonnegative_i64_delta(device.used_bytes, delta.used_bytes)?;
    device.reserved_bytes =
        apply_nonnegative_i64_delta(device.reserved_bytes, delta.reserved_bytes)?;
    device.updated_at_ms = updated_at_ms;
    Ok(())
}

fn apply_nonnegative_i64_delta(current: u64, delta: i64) -> Result<u64> {
    if delta >= 0 {
        Ok(current.saturating_add(delta as u64))
    } else {
        current.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
            StoreError::InvalidState("cold tier usage delta would go negative".to_string())
        })
    }
}
