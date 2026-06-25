pub(crate) const COLD_TIER_DEVICE_TOTAL: &str = "mooncake_store_cold_tier_device_total";
pub(crate) const COLD_TIER_DEVICE_SCHEDULABLE_TOTAL: &str =
    "mooncake_store_cold_tier_device_schedulable_total";
pub(crate) const COLD_TIER_DEVICE_USED_BYTES: &str = "mooncake_store_cold_tier_device_used_bytes";
pub(crate) const COLD_TIER_DEVICE_RESERVED_BYTES: &str =
    "mooncake_store_cold_tier_device_reserved_bytes";
pub(crate) const COLD_TIER_DEVICE_CAPACITY_BYTES: &str =
    "mooncake_store_cold_tier_device_capacity_bytes";
pub(crate) const COLD_TIER_PENDING_OFFLOAD_TOTAL: &str =
    "mooncake_store_cold_tier_pending_offload_total";
pub(crate) const COLD_TIER_PENDING_OFFLOAD_READY: &str =
    "mooncake_store_cold_tier_pending_offload_ready";
pub(crate) const COLD_TIER_PENDING_OFFLOAD_DELAYED: &str =
    "mooncake_store_cold_tier_pending_offload_delayed";
pub(crate) const COLD_TIER_PENDING_OFFLOAD_ATTEMPTS_TOTAL: &str =
    "mooncake_store_cold_tier_pending_offload_attempts_total";
pub(crate) const COLD_TIER_PENDING_OFFLOAD_MAX_ATTEMPTS: &str =
    "mooncake_store_cold_tier_pending_offload_max_attempts";
pub(crate) const COLD_TIER_RECLAIM_PENDING_TOTAL: &str =
    "mooncake_store_cold_tier_reclaim_pending_total";
pub(crate) const COLD_TIER_RECLAIM_DUE_TOTAL: &str = "mooncake_store_cold_tier_reclaim_due_total";
pub(crate) const COLD_TIER_RECLAIM_BY_KIND: &str = "mooncake_store_cold_tier_reclaim_by_kind";
pub(crate) const COLD_TIER_RECLAIM_BY_QOS_TIER: &str =
    "mooncake_store_cold_tier_reclaim_by_qos_tier";
pub(crate) const COLD_TIER_RECLAIM_BY_POLICY_RANK: &str =
    "mooncake_store_cold_tier_reclaim_by_policy_rank";
pub(crate) const COLD_TIER_OPERATION_TOTAL: &str = "mooncake_store_cold_tier_operation_total";
pub(crate) const COLD_RESTORE_SINGLEFLIGHT_TOTAL: &str =
    "mooncake_store_cold_restore_singleflight_total";
pub(crate) const COLD_RESTORE_MAX_CONCURRENT_IO_PER_OBJECT: &str =
    "mooncake_store_cold_restore_max_concurrent_io_per_object";
pub(crate) const COLD_TIER_SSD_READ_DURATION: &str =
    "mooncake_store_cold_tier_ssd_read_duration_seconds";
pub(crate) const COLD_TIER_SSD_WRITE_DURATION: &str =
    "mooncake_store_cold_tier_ssd_write_duration_seconds";
pub(crate) const EXTENT_STORE_IO_PRIORITY_WAIT: &str =
    "mooncake_store_extent_store_io_priority_wait_seconds";
pub(crate) const STAGING_POOL_WAIT: &str = "mooncake_store_staging_pool_wait_seconds";
pub(crate) const STAGING_POOL_EXHAUSTION_TOTAL: &str =
    "mooncake_store_staging_pool_exhaustion_total";
pub(crate) const EXTENT_STORE_QUEUE_WAIT: &str = "mooncake_store_extent_store_queue_wait_seconds";
pub(crate) const EXTENT_STORE_PIPELINE_DURATION: &str =
    "mooncake_store_extent_store_pipeline_duration_seconds";

// --- Batch Get Phase Profiling Metrics ---
pub(crate) const BATCH_GET_PHASE_DURATION: &str = "mooncake_store_batch_get_phase_duration_seconds";
pub(crate) const BATCH_GET_PATH_ITEMS_TOTAL: &str = "mooncake_store_batch_get_path_items_total";
pub(crate) const FACADE_PHASE_DURATION: &str = "mooncake_store_facade_phase_duration_seconds";
pub(crate) const RDMA_TRANSFER_DURATION: &str = "mooncake_store_rdma_transfer_duration_seconds";
pub(crate) const RDMA_BYTES_TOTAL: &str = "mooncake_store_rdma_bytes_total";
pub(crate) const COLD_READ_BATCH_WALL_DURATION: &str =
    "mooncake_store_cold_read_batch_wall_duration_seconds";
pub(crate) const IO_URING_PHASE_DURATION: &str = "mooncake_store_io_uring_phase_duration_seconds";
pub(crate) const IO_URING_OPS_TOTAL: &str = "mooncake_store_io_uring_ops_total";
pub(crate) const COLD_RESTORE_BATCH_DURATION: &str =
    "mooncake_store_cold_restore_batch_duration_seconds";
pub(crate) const COLD_RESTORE_BATCH_ITEMS: &str = "mooncake_store_cold_restore_batch_items";
pub(crate) const COLD_PREFETCH_WORKER_DURATION: &str =
    "mooncake_store_cold_prefetch_worker_duration_seconds";
pub(crate) const COLD_PREFETCH_WORKER_ITEMS: &str =
    "mooncake_store_cold_prefetch_worker_items_total";
pub(crate) const BATCH_IS_EXIST_DURATION: &str = "mooncake_store_batch_is_exist_duration_seconds";

/// Bucket boundaries for SSD IO latency histograms.
/// Covers normal SSD (~1ms) through throttled cloud disk (~1s+).
pub(crate) const COLD_TIER_IO_BUCKETS: &[f64] = &[
    0.000_1, 0.000_5, 0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 0.200, 0.500, 1.0, 2.0, 5.0,
    10.0,
];
