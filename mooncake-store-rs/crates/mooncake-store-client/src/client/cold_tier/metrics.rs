use std::time::Duration;

#[allow(dead_code)]
pub(in super::super) fn record_cold_restore_singleflight(
    _event: &'static str,
    _result: &'static str,
) {
}

#[allow(dead_code)]
pub(in super::super) fn set_cold_restore_max_concurrent_io_per_object(_count: usize) {}

#[allow(dead_code)]
pub(in super::super) fn record_cold_tier_ssd_read(_result: &'static str, _duration: Duration) {}

#[allow(dead_code)]
pub(in super::super) fn record_cold_restore_batch_duration(
    _phase: &'static str,
    _duration: Duration,
) {
}

#[allow(dead_code)]
pub(in super::super) fn record_cold_restore_batch_items(_result: &'static str, _count: u64) {}
