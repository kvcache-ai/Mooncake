#![allow(dead_code)]

#[cfg(test)]
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use mooncake_store_core::{
    ClientLease, ClientLifecycleState, ColdTierDeviceState, ObjectKey, ObjectRoute, ReplicaTier,
    SegmentAnnouncement, SegmentLifecycleState,
};

use super::process::ProcessSnapshot;

pub(crate) const REQUEST_DURATION_BUCKETS: &[f64] = &[
    0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001, 0.0025, 0.005, 0.010, 0.025, 0.050, 0.100,
    0.250, 0.500, 1.0,
];

pub(crate) const REQUEST_TOTAL: &str = "mooncake_store_request_total";
pub(crate) const REQUEST_DURATION: &str = "mooncake_store_request_duration_seconds";
pub(crate) const REQUEST_INFLIGHT: &str = "mooncake_store_request_inflight";
pub(crate) const REQUEST_BYTES: &str = "mooncake_store_request_bytes_total";
pub(crate) const SEGMENT_CAPACITY_BYTES: &str = "mooncake_store_segment_capacity_bytes";
pub(crate) const SEGMENT_USED_BYTES: &str = "mooncake_store_segment_used_bytes";
pub(crate) const OBJECT_ROUTES: &str = "mooncake_store_object_routes";
pub(crate) const REPLICA_DISTRIBUTION: &str = "mooncake_store_replica_distribution";
pub(crate) const RUNTIME_STATUS: &str = "mooncake_store_runtime_status";
pub(crate) const RUNTIME_LEASE_EXPIRES_AT_MS: &str = "mooncake_store_runtime_lease_expires_at_ms";
pub(crate) const HEARTBEAT_CONSECUTIVE_FAILURES: &str =
    "mooncake_store_heartbeat_consecutive_failures";
pub(crate) const HEARTBEAT_LAST_SUCCESS_MS: &str = "mooncake_store_heartbeat_last_success_ms";
pub(crate) const MEMBERSHIP_REFRESH_TOTAL: &str = "mooncake_store_membership_refresh_total";
pub(crate) const MEMBERSHIP_REFRESH_DURATION: &str =
    "mooncake_store_membership_refresh_duration_seconds";
pub(crate) const ROUTE_CAS_TOTAL: &str = "mooncake_store_route_cas_total";
pub(crate) const REPLICATION_PUBLISH_TOTAL: &str = "mooncake_store_replication_publish_total";
pub(crate) const REPLICATION_PUBLISH_DURATION: &str =
    "mooncake_store_replication_publish_duration_seconds";
pub(crate) const CHECKSUM_VALIDATION_TOTAL: &str = "mooncake_store_checksum_validation_total";
pub(crate) const TENANT_QUOTA_RESERVATION_TOTAL: &str =
    "mooncake_store_tenant_quota_reservation_total";
pub(crate) const TENANT_QUOTA_FINALIZE_TOTAL: &str = "mooncake_store_tenant_quota_finalize_total";
pub(crate) const TENANT_QUOTA_ABORT_TOTAL: &str = "mooncake_store_tenant_quota_abort_total";
pub(crate) const TENANT_QUOTA_RECONCILE_TOTAL: &str = "mooncake_store_tenant_quota_reconcile_total";
pub(crate) const TENANT_LOCAL_EVICTION_TOTAL: &str = "mooncake_store_tenant_local_eviction_total";
pub(crate) const PREFERRED_SEGMENT_SKIP_TOTAL: &str = "mooncake_store_preferred_segment_skip_total";
pub(crate) const REBALANCE_ROUTES_TOTAL: &str = "mooncake_store_rebalance_routes_total";
pub(crate) const REBALANCE_BYTES_TOTAL: &str = "mooncake_store_rebalance_bytes_total";
pub(crate) const SEGMENT_LIFECYCLE_TOTAL: &str = "mooncake_store_segment_lifecycle_total";
pub(crate) const RECLAIM_RELEASE_TOTAL: &str = "mooncake_store_reclaim_release_total";
pub(crate) const EVICTION_TOTAL: &str = "mooncake_store_eviction_total";
pub(crate) const EVICTION_DURATION: &str = "mooncake_store_eviction_duration_seconds";
pub(crate) const TRANSPORT_OPERATION_TOTAL: &str = "mooncake_store_transport_operation_total";
pub(crate) const TRANSPORT_BYTES_TOTAL: &str = "mooncake_store_transport_bytes_total";
pub(crate) const METADATA_OPERATION_TOTAL: &str = "mooncake_store_metadata_operation_total";
pub(crate) const METADATA_OPERATION_INFLIGHT: &str = "mooncake_store_metadata_operation_inflight";
pub(crate) const METADATA_OPERATION_DURATION: &str =
    "mooncake_store_metadata_operation_duration_seconds";
pub(crate) use super::cold_tier_registry::{
    BATCH_GET_PATH_ITEMS_TOTAL, BATCH_GET_PHASE_DURATION, BATCH_IS_EXIST_DURATION,
    COLD_PREFETCH_WORKER_DURATION, COLD_PREFETCH_WORKER_ITEMS, COLD_READ_BATCH_WALL_DURATION,
    COLD_RESTORE_BATCH_DURATION, COLD_RESTORE_BATCH_ITEMS,
    COLD_RESTORE_MAX_CONCURRENT_IO_PER_OBJECT, COLD_RESTORE_SINGLEFLIGHT_TOTAL,
    COLD_TIER_DEVICE_CAPACITY_BYTES, COLD_TIER_DEVICE_RESERVED_BYTES,
    COLD_TIER_DEVICE_SCHEDULABLE_TOTAL, COLD_TIER_DEVICE_TOTAL, COLD_TIER_DEVICE_USED_BYTES,
    COLD_TIER_IO_BUCKETS, COLD_TIER_OPERATION_TOTAL, COLD_TIER_PENDING_OFFLOAD_ATTEMPTS_TOTAL,
    COLD_TIER_PENDING_OFFLOAD_DELAYED, COLD_TIER_PENDING_OFFLOAD_MAX_ATTEMPTS,
    COLD_TIER_PENDING_OFFLOAD_READY, COLD_TIER_PENDING_OFFLOAD_TOTAL, COLD_TIER_RECLAIM_BY_KIND,
    COLD_TIER_RECLAIM_BY_POLICY_RANK, COLD_TIER_RECLAIM_BY_QOS_TIER, COLD_TIER_RECLAIM_DUE_TOTAL,
    COLD_TIER_RECLAIM_PENDING_TOTAL, COLD_TIER_SSD_READ_DURATION, COLD_TIER_SSD_WRITE_DURATION,
    EXTENT_STORE_IO_PRIORITY_WAIT, EXTENT_STORE_PIPELINE_DURATION, EXTENT_STORE_QUEUE_WAIT,
    FACADE_PHASE_DURATION, IO_URING_OPS_TOTAL, IO_URING_PHASE_DURATION, RDMA_BYTES_TOTAL,
    RDMA_TRANSFER_DURATION, STAGING_POOL_EXHAUSTION_TOTAL, STAGING_POOL_WAIT,
};
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OperationMetricSnapshot {
    pub operation: &'static str,
    pub status: &'static str,
    pub calls_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    pub latency_total_us: u64,
    pub latency_max_us: u64,
}

#[derive(Clone, Debug, Default)]
pub struct MetricsSnapshot {
    pub tenant: String,
    pub operations: Vec<OperationMetricSnapshot>,
    pub request_totals: Vec<CounterSample<RequestKey>>,
    pub request_bytes: Vec<CounterSample<RequestBytesKey>>,
    pub request_inflight: Vec<GaugeSample<RequestInflightKey>>,
    pub request_duration: Vec<HistogramSample<RequestKey>>,
    pub segments: Vec<SegmentSample>,
    pub object_routes: Vec<GaugeSample<TenantKey>>,
    pub replica_distribution: Vec<GaugeSample<ReplicaDistributionKey>>,
    pub runtime_status: Vec<GaugeSample<RuntimeStatusKey>>,
    pub runtime_lease_expires_at_ms: Vec<GaugeSample<RuntimeKey>>,
    pub heartbeat_consecutive_failures: Vec<GaugeSample<RuntimeKey>>,
    pub heartbeat_last_success_ms: Vec<GaugeSample<RuntimeKey>>,
    pub membership_refresh: Vec<CounterSample<ResultKey>>,
    pub membership_refresh_duration: Vec<HistogramSample<ResultKey>>,
    pub route_cas: Vec<CounterSample<ResultKey>>,
    pub replication_publish: Vec<CounterSample<ResultKey>>,
    pub replication_publish_duration: Vec<HistogramSample<ResultKey>>,
    pub checksum_validation: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_reservation: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_finalize: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_abort: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_reconcile: Vec<CounterSample<ResultKey>>,
    pub tenant_local_eviction: Vec<CounterSample<ResultKey>>,
    pub preferred_segment_skip: Vec<CounterSample<PreferredSegmentSkipKey>>,
    pub rebalance_routes: Vec<CounterSample<PhaseResultKey>>,
    pub rebalance_bytes: Vec<CounterSample<PhaseKey>>,
    pub segment_lifecycle: Vec<CounterSample<ActionResultKey>>,
    pub reclaim_release: Vec<CounterSample<ActionResultKey>>,
    pub eviction: Vec<CounterSample<ResultKey>>,
    pub eviction_duration: Vec<HistogramSample<ResultKey>>,
    pub transport_operations: Vec<CounterSample<TransportOperationKey>>,
    pub transport_bytes: Vec<CounterSample<TransportBytesKey>>,
    pub metadata_operations: Vec<CounterSample<MetadataOperationKey>>,
    pub metadata_inflight: Vec<GaugeSample<MetadataInflightKey>>,
    pub metadata_duration: Vec<HistogramSample<MetadataOperationKey>>,
    pub cold_tier_device_states: Vec<GaugeSample<ColdTierDeviceStateKey>>,
    pub cold_tier_device_totals: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_device_schedulable: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_device_used_bytes: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_device_reserved_bytes: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_device_capacity_bytes: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_pending_offload_total: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_pending_offload_ready: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_pending_offload_delayed: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_pending_offload_attempts_total: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_pending_offload_max_attempts: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_reclaim_pending_total: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_reclaim_due_total: Vec<GaugeSample<ColdTierRuntimeKey>>,
    pub cold_tier_reclaim_by_kind: Vec<GaugeSample<ColdTierReclaimKindKey>>,
    pub cold_tier_reclaim_by_qos_tier: Vec<GaugeSample<ColdTierReclaimQosKey>>,
    pub cold_tier_reclaim_by_policy_rank: Vec<GaugeSample<ColdTierReclaimPolicyRankKey>>,
    pub cold_tier_operations: Vec<CounterSample<ColdTierOperationKey>>,
    pub cold_restore_singleflight: Vec<CounterSample<ColdRestoreSingleflightKey>>,
    pub cold_restore_max_concurrent_io_per_object: f64,
    pub cold_tier_ssd_read_duration: Vec<HistogramSample<ResultKey>>,
    pub cold_tier_ssd_write_duration: Vec<HistogramSample<ResultKey>>,
    pub io_priority_wait: Vec<HistogramSample<ResultKey>>,
    pub staging_pool_wait: Vec<HistogramSample<ResultKey>>,
    pub staging_pool_exhaustion: Vec<CounterSample<ResultKey>>,
    pub extent_store_queue_wait: Vec<HistogramSample<ResultKey>>,
    pub extent_store_pipeline_duration: Vec<HistogramSample<ResultKey>>,
    // Batch get phase profiling metrics
    pub batch_get_phase_duration: Vec<HistogramSample<PhaseKey>>,
    pub batch_get_path_items: Vec<CounterSample<PhaseKey>>,
    pub facade_phase_duration: Vec<HistogramSample<PhaseKey>>,
    pub rdma_transfer_duration: Vec<HistogramSample<PhaseKey>>,
    pub rdma_bytes: Vec<CounterSample<PhaseKey>>,
    pub cold_read_batch_wall_duration: Vec<HistogramSample<ResultKey>>,
    pub io_uring_phase_duration: Vec<HistogramSample<PhaseKey>>,
    pub io_uring_ops: Vec<CounterSample<ResultKey>>,
    pub cold_restore_batch_duration: Vec<HistogramSample<PhaseKey>>,
    pub cold_restore_batch_items: Vec<CounterSample<ResultKey>>,
    pub cold_prefetch_worker_duration: Vec<HistogramSample<ResultKey>>,
    pub cold_prefetch_worker_items: Vec<CounterSample<ResultKey>>,
    pub batch_is_exist_duration: Vec<HistogramSample<PhaseKey>>,
    pub process: ProcessSnapshot,
}

impl MetricsSnapshot {
    pub fn iter(&self) -> std::slice::Iter<'_, OperationMetricSnapshot> {
        self.operations.iter()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RequestKey {
    pub operation: &'static str,
    pub scope: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RequestBytesKey {
    pub operation: &'static str,
    pub direction: &'static str,
    pub scope: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RequestInflightKey {
    pub operation: &'static str,
    pub scope: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ResultKey {
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PhaseKey {
    pub phase: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PhaseResultKey {
    pub phase: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ActionResultKey {
    pub action: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TransportBytesKey {
    pub direction: &'static str,
    pub peer_kind: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TransportOperationKey {
    pub direction: &'static str,
    pub peer_kind: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MetadataOperationKey {
    pub backend: &'static str,
    pub operation: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct MetadataInflightKey {
    pub backend: &'static str,
    pub operation: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PreferredSegmentSkipKey {
    pub source: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdTierRuntimeKey {
    pub runtime: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdTierDeviceStateKey {
    pub runtime: String,
    pub state: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdTierReclaimQosKey {
    pub runtime: String,
    pub qos_tier: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdTierReclaimKindKey {
    pub runtime: String,
    pub kind: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdTierReclaimPolicyRankKey {
    pub runtime: String,
    pub policy_rank: u8,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdTierOperationKey {
    pub operation: &'static str,
    pub result: &'static str,
    pub error_kind: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColdRestoreSingleflightKey {
    pub event: &'static str,
    pub result: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TenantKey {
    pub tenant: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ReplicaDistributionKey {
    pub runtime: String,
    pub tier: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RuntimeKey {
    pub runtime: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RuntimeStatusKey {
    pub runtime: String,
    pub state: &'static str,
}

#[derive(Clone, Debug)]
pub struct SegmentSample {
    pub runtime: String,
    pub segment: String,
    pub state: &'static str,
    pub tier: &'static str,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct CounterSample<K> {
    pub key: K,
    pub value: u64,
}

#[derive(Clone, Debug)]
pub struct GaugeSample<K> {
    pub key: K,
    pub value: f64,
}

#[derive(Clone, Debug)]
pub struct HistogramSample<K> {
    pub key: K,
    pub buckets: Vec<u64>,
    pub count: u64,
    pub sum: f64,
}

pub(crate) struct CounterFamily<K> {
    inner: BTreeMap<K, u64>,
}

impl<K> Default for CounterFamily<K> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K: Ord + Clone> CounterFamily<K> {
    fn add(&mut self, key: K, value: u64) {
        let counter = self.inner.entry(key).or_default();
        *counter = counter.saturating_add(value);
    }

    fn snapshot_with_defaults<I>(&self, defaults: I) -> Vec<CounterSample<K>>
    where
        I: IntoIterator<Item = K>,
    {
        let mut inner = self.inner.clone();
        for key in defaults {
            inner.entry(key).or_default();
        }
        inner
            .iter()
            .map(|(key, value)| CounterSample {
                key: key.clone(),
                value: *value,
            })
            .collect()
    }

    fn snapshot(&self) -> Vec<CounterSample<K>> {
        self.inner
            .iter()
            .map(|(key, value)| CounterSample {
                key: key.clone(),
                value: *value,
            })
            .collect()
    }
}

pub(crate) struct GaugeFamily<K> {
    inner: BTreeMap<K, f64>,
}

impl<K> Default for GaugeFamily<K> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K: Ord + Clone> GaugeFamily<K> {
    fn add(&mut self, key: K, delta: f64) {
        let gauge = self.inner.entry(key).or_default();
        *gauge = (*gauge + delta).max(0.0);
    }

    fn set(&mut self, key: K, value: f64) {
        self.inner.insert(key, value.max(0.0));
    }

    fn snapshot(&self) -> Vec<GaugeSample<K>> {
        self.inner
            .iter()
            .map(|(key, value)| GaugeSample {
                key: key.clone(),
                value: *value,
            })
            .collect()
    }
}

pub(crate) struct HistogramFamily<K> {
    inner: BTreeMap<K, HistogramState>,
    bucket_boundaries: &'static [f64],
}

impl<K> Default for HistogramFamily<K> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
            bucket_boundaries: REQUEST_DURATION_BUCKETS,
        }
    }
}

impl<K> HistogramFamily<K> {
    fn with_buckets(bucket_boundaries: &'static [f64]) -> Self {
        Self {
            inner: BTreeMap::new(),
            bucket_boundaries,
        }
    }
}

impl<K: Ord + Clone> HistogramFamily<K> {
    fn observe(&mut self, key: K, value: f64) {
        let boundaries = self.bucket_boundaries;
        self.inner
            .entry(key)
            .or_insert_with(|| HistogramState::with_buckets(boundaries))
            .observe(value, boundaries);
    }

    fn snapshot(&self) -> Vec<HistogramSample<K>> {
        self.inner
            .iter()
            .map(|(key, state)| HistogramSample {
                key: key.clone(),
                buckets: state.buckets.clone(),
                count: state.count,
                sum: state.sum,
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HistogramState {
    buckets: Vec<u64>,
    count: u64,
    sum: f64,
}

impl Default for HistogramState {
    fn default() -> Self {
        Self {
            buckets: vec![0; REQUEST_DURATION_BUCKETS.len()],
            count: 0,
            sum: 0.0,
        }
    }
}

impl HistogramState {
    fn with_buckets(boundaries: &[f64]) -> Self {
        Self {
            buckets: vec![0; boundaries.len()],
            count: 0,
            sum: 0.0,
        }
    }

    fn observe(&mut self, value: f64, boundaries: &[f64]) {
        for (index, bucket) in boundaries.iter().enumerate() {
            if value <= *bucket {
                self.buckets[index] = self.buckets[index].saturating_add(1);
            }
        }
        self.count = self.count.saturating_add(1);
        self.sum += value;
    }
}

#[derive(Default)]
struct OperationMetricState {
    calls_total: u64,
    bytes_in_total: u64,
    bytes_out_total: u64,
    latency_total_us: u64,
    latency_max_us: u64,
}

type OperationKey = (&'static str, &'static str);

struct MetricsRegistry {
    tenant: String,
    operations: BTreeMap<OperationKey, OperationMetricState>,
    request_totals: CounterFamily<RequestKey>,
    request_bytes: CounterFamily<RequestBytesKey>,
    request_inflight: GaugeFamily<RequestInflightKey>,
    request_duration: HistogramFamily<RequestKey>,
    segments: BTreeMap<(String, String), SegmentSample>,
    routes: BTreeMap<String, ObjectRoute>,
    runtime_leases: BTreeMap<String, RuntimeLeaseMetric>,
    heartbeat_consecutive_failures: GaugeFamily<RuntimeKey>,
    heartbeat_last_success_ms: GaugeFamily<RuntimeKey>,
    membership_refresh: CounterFamily<ResultKey>,
    membership_refresh_duration: HistogramFamily<ResultKey>,
    route_cas: CounterFamily<ResultKey>,
    replication_publish: CounterFamily<ResultKey>,
    replication_publish_duration: HistogramFamily<ResultKey>,
    checksum_validation: CounterFamily<ResultKey>,
    tenant_quota_reservation: CounterFamily<ResultKey>,
    tenant_quota_finalize: CounterFamily<ResultKey>,
    tenant_quota_abort: CounterFamily<ResultKey>,
    tenant_quota_reconcile: CounterFamily<ResultKey>,
    tenant_local_eviction: CounterFamily<ResultKey>,
    preferred_segment_skip: CounterFamily<PreferredSegmentSkipKey>,
    rebalance_routes: CounterFamily<PhaseResultKey>,
    rebalance_bytes: CounterFamily<PhaseKey>,
    segment_lifecycle: CounterFamily<ActionResultKey>,
    reclaim_release: CounterFamily<ActionResultKey>,
    eviction: CounterFamily<ResultKey>,
    eviction_duration: HistogramFamily<ResultKey>,
    transport_operations: CounterFamily<TransportOperationKey>,
    transport_bytes: CounterFamily<TransportBytesKey>,
    metadata_operations: CounterFamily<MetadataOperationKey>,
    metadata_inflight: GaugeFamily<MetadataInflightKey>,
    metadata_duration: HistogramFamily<MetadataOperationKey>,
    cold_tier_device_states: GaugeFamily<ColdTierDeviceStateKey>,
    cold_tier_device_totals: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_device_schedulable: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_device_used_bytes: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_device_reserved_bytes: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_device_capacity_bytes: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_pending_offload_total: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_pending_offload_ready: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_pending_offload_delayed: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_pending_offload_attempts_total: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_pending_offload_max_attempts: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_reclaim_pending_total: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_reclaim_due_total: GaugeFamily<ColdTierRuntimeKey>,
    cold_tier_reclaim_by_kind: GaugeFamily<ColdTierReclaimKindKey>,
    cold_tier_reclaim_by_qos_tier: GaugeFamily<ColdTierReclaimQosKey>,
    cold_tier_reclaim_by_policy_rank: GaugeFamily<ColdTierReclaimPolicyRankKey>,
    cold_tier_operations: CounterFamily<ColdTierOperationKey>,
    cold_restore_singleflight: CounterFamily<ColdRestoreSingleflightKey>,
    cold_restore_max_concurrent_io_per_object: f64,
    cold_tier_ssd_read_duration: HistogramFamily<ResultKey>,
    cold_tier_ssd_write_duration: HistogramFamily<ResultKey>,
    io_priority_wait: HistogramFamily<ResultKey>,
    staging_pool_wait: HistogramFamily<ResultKey>,
    staging_pool_exhaustion: CounterFamily<ResultKey>,
    extent_store_queue_wait: HistogramFamily<ResultKey>,
    extent_store_pipeline_duration: HistogramFamily<ResultKey>,
    // Batch get phase profiling metrics
    batch_get_phase_duration: HistogramFamily<PhaseKey>,
    batch_get_path_items: CounterFamily<PhaseKey>,
    facade_phase_duration: HistogramFamily<PhaseKey>,
    rdma_transfer_duration: HistogramFamily<PhaseKey>,
    rdma_bytes: CounterFamily<PhaseKey>,
    cold_read_batch_wall_duration: HistogramFamily<ResultKey>,
    io_uring_phase_duration: HistogramFamily<PhaseKey>,
    io_uring_ops: CounterFamily<ResultKey>,
    cold_restore_batch_duration: HistogramFamily<PhaseKey>,
    cold_restore_batch_items: CounterFamily<ResultKey>,
    cold_prefetch_worker_duration: HistogramFamily<ResultKey>,
    cold_prefetch_worker_items: CounterFamily<ResultKey>,
    batch_is_exist_duration: HistogramFamily<PhaseKey>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self {
            tenant: String::new(),
            operations: BTreeMap::new(),
            request_totals: CounterFamily::default(),
            request_bytes: CounterFamily::default(),
            request_inflight: GaugeFamily::default(),
            request_duration: HistogramFamily::default(),
            segments: BTreeMap::new(),
            routes: BTreeMap::new(),
            runtime_leases: BTreeMap::new(),
            heartbeat_consecutive_failures: GaugeFamily::default(),
            heartbeat_last_success_ms: GaugeFamily::default(),
            membership_refresh: CounterFamily::default(),
            membership_refresh_duration: HistogramFamily::default(),
            route_cas: CounterFamily::default(),
            replication_publish: CounterFamily::default(),
            replication_publish_duration: HistogramFamily::default(),
            checksum_validation: CounterFamily::default(),
            tenant_quota_reservation: CounterFamily::default(),
            tenant_quota_finalize: CounterFamily::default(),
            tenant_quota_abort: CounterFamily::default(),
            tenant_quota_reconcile: CounterFamily::default(),
            tenant_local_eviction: CounterFamily::default(),
            preferred_segment_skip: CounterFamily::default(),
            rebalance_routes: CounterFamily::default(),
            rebalance_bytes: CounterFamily::default(),
            segment_lifecycle: CounterFamily::default(),
            reclaim_release: CounterFamily::default(),
            eviction: CounterFamily::default(),
            eviction_duration: HistogramFamily::default(),
            transport_operations: CounterFamily::default(),
            transport_bytes: CounterFamily::default(),
            metadata_operations: CounterFamily::default(),
            metadata_inflight: GaugeFamily::default(),
            metadata_duration: HistogramFamily::default(),
            cold_tier_device_states: GaugeFamily::default(),
            cold_tier_device_totals: GaugeFamily::default(),
            cold_tier_device_schedulable: GaugeFamily::default(),
            cold_tier_device_used_bytes: GaugeFamily::default(),
            cold_tier_device_reserved_bytes: GaugeFamily::default(),
            cold_tier_device_capacity_bytes: GaugeFamily::default(),
            cold_tier_pending_offload_total: GaugeFamily::default(),
            cold_tier_pending_offload_ready: GaugeFamily::default(),
            cold_tier_pending_offload_delayed: GaugeFamily::default(),
            cold_tier_pending_offload_attempts_total: GaugeFamily::default(),
            cold_tier_pending_offload_max_attempts: GaugeFamily::default(),
            cold_tier_reclaim_pending_total: GaugeFamily::default(),
            cold_tier_reclaim_due_total: GaugeFamily::default(),
            cold_tier_reclaim_by_kind: GaugeFamily::default(),
            cold_tier_reclaim_by_qos_tier: GaugeFamily::default(),
            cold_tier_reclaim_by_policy_rank: GaugeFamily::default(),
            cold_tier_operations: CounterFamily::default(),
            cold_restore_singleflight: CounterFamily::default(),
            cold_restore_max_concurrent_io_per_object: 1.0,
            cold_tier_ssd_read_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            cold_tier_ssd_write_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            io_priority_wait: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            staging_pool_wait: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            staging_pool_exhaustion: CounterFamily::default(),
            extent_store_queue_wait: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            extent_store_pipeline_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            batch_get_phase_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            batch_get_path_items: CounterFamily::default(),
            facade_phase_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            rdma_transfer_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            rdma_bytes: CounterFamily::default(),
            cold_read_batch_wall_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            io_uring_phase_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            io_uring_ops: CounterFamily::default(),
            cold_restore_batch_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            cold_restore_batch_items: CounterFamily::default(),
            cold_prefetch_worker_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
            cold_prefetch_worker_items: CounterFamily::default(),
            batch_is_exist_duration: HistogramFamily::with_buckets(COLD_TIER_IO_BUCKETS),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SharedMetricsRegistry {
    inner: Arc<Mutex<MetricsRegistry>>,
}

impl Default for SharedMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedMetricsRegistry {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsRegistry::default())),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MetricsRegistry> {
        self.inner.lock().expect("metrics lock poisoned")
    }

    #[cfg(test)]
    fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl MetricsRegistry {
    fn tenant_label(&self) -> String {
        if self.tenant.is_empty() {
            DEFAULT_PROCESS_TENANT.to_string()
        } else {
            self.tenant.clone()
        }
    }

    fn snapshot_with_tenant(
        &self,
        process: ProcessSnapshot,
        tenant_override: Option<String>,
    ) -> MetricsSnapshot {
        let tenant = tenant_override.unwrap_or_else(|| self.tenant_label());
        self.snapshot_inner(process, tenant)
    }

    fn record_request(
        &mut self,
        operation: &'static str,
        scope: &'static str,
        result: &'static str,
        bytes_in: u64,
        bytes_out: u64,
        duration: Duration,
    ) {
        let latency_us = duration.as_micros() as u64;
        let state = self.operations.entry((operation, result)).or_default();
        state.calls_total = state.calls_total.saturating_add(1);
        state.bytes_in_total = state.bytes_in_total.saturating_add(bytes_in);
        state.bytes_out_total = state.bytes_out_total.saturating_add(bytes_out);
        state.latency_total_us = state.latency_total_us.saturating_add(latency_us);
        state.latency_max_us = state.latency_max_us.max(latency_us);

        let request_key = RequestKey {
            operation,
            scope,
            result,
        };
        self.request_totals.add(request_key.clone(), 1);
        self.request_duration
            .observe(request_key, duration.as_secs_f64());
        if bytes_in != 0 {
            self.request_bytes.add(
                RequestBytesKey {
                    operation,
                    direction: "in",
                    scope,
                },
                bytes_in,
            );
        }
        if bytes_out != 0 {
            self.request_bytes.add(
                RequestBytesKey {
                    operation,
                    direction: "out",
                    scope,
                },
                bytes_out,
            );
        }
    }

    fn snapshot(&self, process: ProcessSnapshot) -> MetricsSnapshot {
        self.snapshot_with_tenant(process, None)
    }

    fn snapshot_inner(&self, process: ProcessSnapshot, tenant: String) -> MetricsSnapshot {
        MetricsSnapshot {
            tenant,
            operations: self
                .operations
                .iter()
                .map(|((operation, status), state)| OperationMetricSnapshot {
                    operation,
                    status,
                    calls_total: state.calls_total,
                    bytes_in_total: state.bytes_in_total,
                    bytes_out_total: state.bytes_out_total,
                    latency_total_us: state.latency_total_us,
                    latency_max_us: state.latency_max_us,
                })
                .collect(),
            request_totals: self.request_totals.snapshot(),
            request_bytes: self.request_bytes.snapshot(),
            request_inflight: self.request_inflight.snapshot(),
            request_duration: self.request_duration.snapshot(),
            segments: self.segments.values().cloned().collect(),
            object_routes: self.object_route_snapshot(),
            replica_distribution: self.replica_distribution_snapshot(),
            runtime_status: self.runtime_status_snapshot(),
            runtime_lease_expires_at_ms: self.runtime_lease_snapshot(),
            heartbeat_consecutive_failures: self.heartbeat_consecutive_failures.snapshot(),
            heartbeat_last_success_ms: self.heartbeat_last_success_ms.snapshot(),
            membership_refresh: self.membership_refresh.snapshot(),
            membership_refresh_duration: self.membership_refresh_duration.snapshot(),
            route_cas: self.route_cas.snapshot(),
            replication_publish: self.replication_publish.snapshot(),
            replication_publish_duration: self.replication_publish_duration.snapshot(),
            checksum_validation: self.checksum_validation.snapshot(),
            tenant_quota_reservation: self
                .tenant_quota_reservation
                .snapshot_with_defaults(result_keys(RESULT_OK_CONFLICT_ERROR)),
            tenant_quota_finalize: self
                .tenant_quota_finalize
                .snapshot_with_defaults(result_keys(RESULT_OK_CONFLICT_ERROR)),
            tenant_quota_abort: self
                .tenant_quota_abort
                .snapshot_with_defaults(result_keys(RESULT_OK_CONFLICT_ERROR)),
            tenant_quota_reconcile: self
                .tenant_quota_reconcile
                .snapshot_with_defaults(result_keys(RESULT_TENANT_QUOTA_RECONCILE)),
            tenant_local_eviction: self
                .tenant_local_eviction
                .snapshot_with_defaults(result_keys(RESULT_TENANT_LOCAL_EVICTION)),
            preferred_segment_skip: self
                .preferred_segment_skip
                .snapshot_with_defaults(preferred_segment_skip_keys()),
            rebalance_routes: self
                .rebalance_routes
                .snapshot_with_defaults(phase_result_keys("migrate", RESULT_OK_CONFLICT_ERROR)),
            rebalance_bytes: self
                .rebalance_bytes
                .snapshot_with_defaults(phase_keys(REBALANCE_PHASES)),
            segment_lifecycle: self
                .segment_lifecycle
                .snapshot_with_defaults(segment_lifecycle_keys()),
            reclaim_release: self
                .reclaim_release
                .snapshot_with_defaults(reclaim_release_keys()),
            eviction: self.eviction.snapshot(),
            eviction_duration: self.eviction_duration.snapshot(),
            transport_operations: self.transport_operations.snapshot(),
            transport_bytes: self.transport_bytes.snapshot(),
            metadata_operations: self.metadata_operations.snapshot(),
            metadata_inflight: self.metadata_inflight.snapshot(),
            metadata_duration: self.metadata_duration.snapshot(),
            cold_tier_device_states: self.cold_tier_device_states.snapshot(),
            cold_tier_device_totals: self.cold_tier_device_totals.snapshot(),
            cold_tier_device_schedulable: self.cold_tier_device_schedulable.snapshot(),
            cold_tier_device_used_bytes: self.cold_tier_device_used_bytes.snapshot(),
            cold_tier_device_reserved_bytes: self.cold_tier_device_reserved_bytes.snapshot(),
            cold_tier_device_capacity_bytes: self.cold_tier_device_capacity_bytes.snapshot(),
            cold_tier_pending_offload_total: self.cold_tier_pending_offload_total.snapshot(),
            cold_tier_pending_offload_ready: self.cold_tier_pending_offload_ready.snapshot(),
            cold_tier_pending_offload_delayed: self.cold_tier_pending_offload_delayed.snapshot(),
            cold_tier_pending_offload_attempts_total: self
                .cold_tier_pending_offload_attempts_total
                .snapshot(),
            cold_tier_pending_offload_max_attempts: self
                .cold_tier_pending_offload_max_attempts
                .snapshot(),
            cold_tier_reclaim_pending_total: self.cold_tier_reclaim_pending_total.snapshot(),
            cold_tier_reclaim_due_total: self.cold_tier_reclaim_due_total.snapshot(),
            cold_tier_reclaim_by_kind: self.cold_tier_reclaim_by_kind.snapshot(),
            cold_tier_reclaim_by_qos_tier: self.cold_tier_reclaim_by_qos_tier.snapshot(),
            cold_tier_reclaim_by_policy_rank: self.cold_tier_reclaim_by_policy_rank.snapshot(),
            cold_tier_operations: self.cold_tier_operations.snapshot(),
            cold_restore_singleflight: self.cold_restore_singleflight.snapshot(),
            cold_restore_max_concurrent_io_per_object: self
                .cold_restore_max_concurrent_io_per_object,
            cold_tier_ssd_read_duration: self.cold_tier_ssd_read_duration.snapshot(),
            cold_tier_ssd_write_duration: self.cold_tier_ssd_write_duration.snapshot(),
            io_priority_wait: self.io_priority_wait.snapshot(),
            staging_pool_wait: self.staging_pool_wait.snapshot(),
            staging_pool_exhaustion: self.staging_pool_exhaustion.snapshot(),
            extent_store_queue_wait: self.extent_store_queue_wait.snapshot(),
            extent_store_pipeline_duration: self.extent_store_pipeline_duration.snapshot(),
            batch_get_phase_duration: self.batch_get_phase_duration.snapshot(),
            batch_get_path_items: self.batch_get_path_items.snapshot(),
            facade_phase_duration: self.facade_phase_duration.snapshot(),
            rdma_transfer_duration: self.rdma_transfer_duration.snapshot(),
            rdma_bytes: self.rdma_bytes.snapshot(),
            cold_read_batch_wall_duration: self.cold_read_batch_wall_duration.snapshot(),
            io_uring_phase_duration: self.io_uring_phase_duration.snapshot(),
            io_uring_ops: self.io_uring_ops.snapshot(),
            cold_restore_batch_duration: self.cold_restore_batch_duration.snapshot(),
            cold_restore_batch_items: self.cold_restore_batch_items.snapshot(),
            cold_prefetch_worker_duration: self.cold_prefetch_worker_duration.snapshot(),
            cold_prefetch_worker_items: self.cold_prefetch_worker_items.snapshot(),
            batch_is_exist_duration: self.batch_is_exist_duration.snapshot(),
            process,
        }
    }

    fn object_route_snapshot(&self) -> Vec<GaugeSample<TenantKey>> {
        let mut by_tenant = BTreeMap::<String, u64>::new();
        for route in self.routes.values() {
            let tenant = object_key_tenant(&route.key.0);
            *by_tenant.entry(tenant.to_string()).or_default() += 1;
        }
        by_tenant
            .into_iter()
            .map(|(tenant, value)| GaugeSample {
                key: TenantKey { tenant },
                value: value as f64,
            })
            .collect()
    }

    fn replica_distribution_snapshot(&self) -> Vec<GaugeSample<ReplicaDistributionKey>> {
        let mut by_runtime = BTreeMap::<ReplicaDistributionKey, u64>::new();
        for route in self.routes.values() {
            for replica in &route.replicas {
                *by_runtime
                    .entry(ReplicaDistributionKey {
                        runtime: replica.owner.to_string(),
                        tier: replica_tier_label(replica.tier),
                    })
                    .or_default() += 1;
            }
        }
        by_runtime
            .into_iter()
            .map(|(key, value)| GaugeSample {
                key,
                value: value as f64,
            })
            .collect()
    }

    fn runtime_status_snapshot(&self) -> Vec<GaugeSample<RuntimeStatusKey>> {
        let states = ["active", "standby", "draining", "sealed", "offline"];
        let mut samples = Vec::new();
        for lease in self.runtime_leases.values() {
            for state in states {
                samples.push(GaugeSample {
                    key: RuntimeStatusKey {
                        runtime: lease.runtime.clone(),
                        state,
                    },
                    value: if lease.state == state { 1.0 } else { 0.0 },
                });
            }
        }
        samples
    }

    fn runtime_lease_snapshot(&self) -> Vec<GaugeSample<RuntimeKey>> {
        self.runtime_leases
            .values()
            .map(|lease| GaugeSample {
                key: RuntimeKey {
                    runtime: lease.runtime.clone(),
                },
                value: lease.expires_at_ms as f64,
            })
            .collect()
    }
}

const RESULT_OK_CONFLICT_ERROR: &[&str] = &["ok", "conflict", "error"];
const RESULT_TENANT_LOCAL_EVICTION: &[&str] = &["ok", "miss"];
const RESULT_TENANT_QUOTA_RECONCILE: &[&str] = &["aborted", "finalized", "error"];
const PREFERRED_SEGMENT_SKIP_REASONS: &[&str] = &[
    "not_found",
    "owner_unavailable",
    "allocator",
    "transport",
    "other",
];
const REBALANCE_PHASES: &[&str] = &["migrate"];
const SEGMENT_LIFECYCLE_ACTIONS: &[&str] = &[
    "mount_segment",
    "expand_local_memory",
    "drain_segment",
    "retire_segment",
];
const RECLAIM_RELEASE_ACTIONS: &[&str] = &["flush_due_reclaims", "flush_all_reclaims"];
const RECLAIM_RELEASE_RESULTS: &[&str] = &[
    "skipped_unavailable_runtime",
    "skipped_missing_runtime_lease",
    "error",
];

fn result_keys(results: &'static [&'static str]) -> impl Iterator<Item = ResultKey> {
    results.iter().copied().map(|result| ResultKey { result })
}

fn phase_keys(phases: &'static [&'static str]) -> impl Iterator<Item = PhaseKey> {
    phases.iter().copied().map(|phase| PhaseKey { phase })
}

fn phase_result_keys(
    phase: &'static str,
    results: &'static [&'static str],
) -> impl Iterator<Item = PhaseResultKey> {
    results
        .iter()
        .copied()
        .map(move |result| PhaseResultKey { phase, result })
}

fn preferred_segment_skip_keys() -> impl Iterator<Item = PreferredSegmentSkipKey> {
    PREFERRED_SEGMENT_SKIP_REASONS
        .iter()
        .copied()
        .map(|reason| PreferredSegmentSkipKey {
            source: "tenant_policy",
            reason,
        })
}

fn segment_lifecycle_keys() -> impl Iterator<Item = ActionResultKey> {
    SEGMENT_LIFECYCLE_ACTIONS
        .iter()
        .copied()
        .flat_map(|action| {
            ["ok", "error"]
                .into_iter()
                .map(move |result| ActionResultKey { action, result })
        })
}

fn reclaim_release_keys() -> impl Iterator<Item = ActionResultKey> {
    RECLAIM_RELEASE_ACTIONS.iter().copied().flat_map(|action| {
        RECLAIM_RELEASE_RESULTS
            .iter()
            .copied()
            .map(move |result| ActionResultKey { action, result })
    })
}

static METRICS: OnceLock<SharedMetricsRegistry> = OnceLock::new();
const DEFAULT_PROCESS_TENANT: &str = "default";

#[cfg(test)]
thread_local! {
    static TEST_PROCESS_TENANT: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug)]
struct RuntimeLeaseMetric {
    runtime: String,
    state: &'static str,
    expires_at_ms: u64,
}

pub(crate) fn record_request_with_registry(
    registry: &SharedMetricsRegistry,
    operation: &'static str,
    scope: &'static str,
    result: &'static str,
    bytes_in: u64,
    bytes_out: u64,
    duration: Duration,
) {
    registry
        .lock()
        .record_request(operation, scope, result, bytes_in, bytes_out, duration);
}

#[cfg(not(test))]
pub(crate) fn set_process_tenant(tenant: &str) {
    global_metrics_registry().lock().tenant = tenant.to_string();
}

#[cfg(test)]
pub(crate) fn set_process_tenant(tenant: &str) {
    set_test_process_tenant(Some(tenant));
}

#[cfg(test)]
pub(crate) fn set_process_tenant_with_registry(registry: &SharedMetricsRegistry, tenant: &str) {
    registry.lock().tenant = tenant.to_string();
}

#[cfg(test)]
fn set_test_process_tenant(tenant: Option<&str>) {
    TEST_PROCESS_TENANT.with(|current| {
        *current.borrow_mut() = tenant.map(ToString::to_string);
    });
}

#[cfg(test)]
fn test_process_tenant_label() -> Option<String> {
    TEST_PROCESS_TENANT.with(|current| {
        current.borrow().as_ref().map(|tenant| {
            if tenant.is_empty() {
                DEFAULT_PROCESS_TENANT.to_string()
            } else {
                tenant.clone()
            }
        })
    })
}

pub(crate) fn record_request_bytes(
    operation: &'static str,
    direction: &'static str,
    scope: &'static str,
    bytes: u64,
) {
    if bytes == 0 {
        return;
    }
    global_metrics_registry().lock().request_bytes.add(
        RequestBytesKey {
            operation,
            direction,
            scope,
        },
        bytes,
    );
}

pub(crate) fn increment_inflight_with_registry(
    registry: &SharedMetricsRegistry,
    operation: &'static str,
    scope: &'static str,
) {
    registry
        .lock()
        .request_inflight
        .add(RequestInflightKey { operation, scope }, 1.0);
}

pub(crate) fn decrement_inflight_with_registry(
    registry: &SharedMetricsRegistry,
    operation: &'static str,
    scope: &'static str,
) {
    registry
        .lock()
        .request_inflight
        .add(RequestInflightKey { operation, scope }, -1.0);
}

pub(crate) fn record_route_cas(result: &'static str) {
    global_metrics_registry()
        .lock()
        .route_cas
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_replication_publish(result: &'static str, duration: Duration) {
    let mut registry = global_metrics_registry().lock();
    let key = ResultKey { result };
    registry.replication_publish.add(key.clone(), 1);
    registry
        .replication_publish_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_checksum_validation(result: &'static str) {
    global_metrics_registry()
        .lock()
        .checksum_validation
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_checksum_validations(result: &'static str, count: u64) {
    if count == 0 {
        return;
    }
    global_metrics_registry()
        .lock()
        .checksum_validation
        .add(ResultKey { result }, count);
}

pub(crate) fn record_tenant_quota_reservation(result: &'static str) {
    global_metrics_registry()
        .lock()
        .tenant_quota_reservation
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_finalize(result: &'static str) {
    global_metrics_registry()
        .lock()
        .tenant_quota_finalize
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_abort(result: &'static str) {
    global_metrics_registry()
        .lock()
        .tenant_quota_abort
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_reconcile(result: &'static str) {
    global_metrics_registry()
        .lock()
        .tenant_quota_reconcile
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_local_eviction(result: &'static str) {
    global_metrics_registry()
        .lock()
        .tenant_local_eviction
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_preferred_segment_skip(source: &'static str, reason: &'static str) {
    global_metrics_registry()
        .lock()
        .preferred_segment_skip
        .add(PreferredSegmentSkipKey { source, reason }, 1);
}

pub(crate) fn record_rebalance_route(phase: &'static str, result: &'static str) {
    global_metrics_registry()
        .lock()
        .rebalance_routes
        .add(PhaseResultKey { phase, result }, 1);
}

pub(crate) fn record_rebalance_bytes(phase: &'static str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    global_metrics_registry()
        .lock()
        .rebalance_bytes
        .add(PhaseKey { phase }, bytes);
}

pub(crate) fn record_transport_bytes(direction: &'static str, peer_kind: &'static str, bytes: u64) {
    record_transport_bytes_with_registry(global_metrics_registry(), direction, peer_kind, bytes);
}

pub(crate) fn record_transport_bytes_with_registry(
    registry: &SharedMetricsRegistry,
    direction: &'static str,
    peer_kind: &'static str,
    bytes: u64,
) {
    if bytes == 0 {
        return;
    }
    registry.lock().transport_bytes.add(
        TransportBytesKey {
            direction,
            peer_kind,
        },
        bytes,
    );
}

pub(crate) fn record_transport_operation(
    direction: &'static str,
    peer_kind: &'static str,
    result: &'static str,
) {
    record_transport_operation_with_registry(
        global_metrics_registry(),
        direction,
        peer_kind,
        result,
    );
}

pub(crate) fn record_transport_operation_with_registry(
    registry: &SharedMetricsRegistry,
    direction: &'static str,
    peer_kind: &'static str,
    result: &'static str,
) {
    registry.lock().transport_operations.add(
        TransportOperationKey {
            direction,
            peer_kind,
            result,
        },
        1,
    );
}

pub(crate) fn record_metadata_operation_with_registry(
    registry: &SharedMetricsRegistry,
    backend: &'static str,
    operation: &'static str,
    result: &'static str,
    duration: Duration,
) {
    let mut registry = registry.lock();
    let key = MetadataOperationKey {
        backend,
        operation,
        result,
    };
    registry.metadata_operations.add(key.clone(), 1);
    registry
        .metadata_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn increment_metadata_inflight_with_registry(
    registry: &SharedMetricsRegistry,
    backend: &'static str,
    operation: &'static str,
) {
    registry
        .lock()
        .metadata_inflight
        .add(MetadataInflightKey { backend, operation }, 1.0);
}

pub(crate) fn decrement_metadata_inflight_with_registry(
    registry: &SharedMetricsRegistry,
    backend: &'static str,
    operation: &'static str,
) {
    registry
        .lock()
        .metadata_inflight
        .add(MetadataInflightKey { backend, operation }, -1.0);
}

pub(crate) fn record_membership_refresh(result: &'static str, duration: Duration) {
    let mut registry = global_metrics_registry().lock();
    let key = ResultKey { result };
    registry.membership_refresh.add(key.clone(), 1);
    registry
        .membership_refresh_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_runtime_leases(leases: &[ClientLease]) {
    record_runtime_leases_with_registry(global_metrics_registry(), leases);
}

pub(crate) fn record_runtime_leases_with_registry(
    registry: &SharedMetricsRegistry,
    leases: &[ClientLease],
) {
    let mut registry = registry.lock();
    replace_runtime_leases(&mut registry, leases);
}

pub(crate) fn record_heartbeat_health(
    runtime: &str,
    consecutive_failures: u64,
    last_success_ms: u64,
) {
    record_heartbeat_health_with_registry(
        global_metrics_registry(),
        runtime,
        consecutive_failures,
        last_success_ms,
    );
}

pub(crate) fn record_heartbeat_health_with_registry(
    registry: &SharedMetricsRegistry,
    runtime: &str,
    consecutive_failures: u64,
    last_success_ms: u64,
) {
    let mut registry = registry.lock();
    let key = RuntimeKey {
        runtime: runtime.to_string(),
    };
    registry
        .heartbeat_consecutive_failures
        .set(key.clone(), consecutive_failures as f64);
    registry
        .heartbeat_last_success_ms
        .set(key, last_success_ms as f64);
}

pub(crate) fn record_segment(announcement: &SegmentAnnouncement) {
    global_metrics_registry()
        .lock()
        .segments
        .insert(segment_identity(announcement), segment_sample(announcement));
}

pub(crate) fn record_segment_removed(announcement: &SegmentAnnouncement) {
    let mut sample = segment_sample(announcement);
    sample.state = "retired";
    sample.used_bytes = 0;
    sample.capacity_bytes = 0;
    global_metrics_registry()
        .lock()
        .segments
        .insert((sample.runtime.clone(), sample.segment.clone()), sample);
}

pub(crate) fn record_route(route: &ObjectRoute) {
    global_metrics_registry()
        .lock()
        .routes
        .insert(route.key.0.clone(), route.clone());
}

pub(crate) fn remove_route(key: &ObjectKey) {
    global_metrics_registry().lock().routes.remove(&key.0);
}

pub(crate) fn record_segment_lifecycle(action: &'static str, result: &'static str) {
    global_metrics_registry()
        .lock()
        .segment_lifecycle
        .add(ActionResultKey { action, result }, 1);
}

#[allow(dead_code)]
pub(crate) fn record_reclaim_release(action: &'static str, result: &'static str) {
    global_metrics_registry()
        .lock()
        .reclaim_release
        .add(ActionResultKey { action, result }, 1);
}

pub(crate) struct ColdTierDeviceMetrics<'a> {
    pub runtime: &'a str,
    pub total_devices: usize,
    pub schedulable_devices: usize,
    pub total_used_bytes: u64,
    pub total_reserved_bytes: u64,
    pub total_capacity_bytes: Option<u64>,
    pub by_state: Vec<(ColdTierDeviceState, usize)>,
}

pub(crate) struct ColdTierPendingOffloadMetrics<'a> {
    pub runtime: &'a str,
    pub total_pending: usize,
    pub ready: usize,
    pub delayed: usize,
    pub max_attempts: u32,
    pub total_attempts: u64,
}

pub(crate) struct ColdTierReclaimMetrics<'a> {
    pub runtime: &'a str,
    pub total_pending: usize,
    pub due: usize,
    pub cold_backing_reclaims: usize,
    pub hot_segment_reclaims: usize,
    pub by_qos_tier: Vec<(&'a str, usize)>,
    pub by_policy_rank: Vec<(u8, usize)>,
}

pub(crate) fn record_eviction(result: &'static str, duration: Duration) {
    let mut registry = global_metrics_registry().lock();
    let key = ResultKey { result };
    registry.eviction.add(key.clone(), 1);
    registry
        .eviction_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_cold_tier_device_metrics(metrics: ColdTierDeviceMetrics<'_>) {
    let mut registry = global_metrics_registry().lock();
    let key = ColdTierRuntimeKey {
        runtime: metrics.runtime.to_string(),
    };
    registry
        .cold_tier_device_totals
        .set(key.clone(), metrics.total_devices as f64);
    registry
        .cold_tier_device_schedulable
        .set(key.clone(), metrics.schedulable_devices as f64);
    registry
        .cold_tier_device_used_bytes
        .set(key.clone(), metrics.total_used_bytes as f64);
    registry
        .cold_tier_device_reserved_bytes
        .set(key.clone(), metrics.total_reserved_bytes as f64);
    if let Some(capacity) = metrics.total_capacity_bytes {
        registry
            .cold_tier_device_capacity_bytes
            .set(key, capacity as f64);
    }
    for (state, count) in metrics.by_state {
        registry.cold_tier_device_states.set(
            ColdTierDeviceStateKey {
                runtime: metrics.runtime.to_string(),
                state: cold_tier_device_state_label(state),
            },
            count as f64,
        );
    }
}

pub(crate) fn record_cold_tier_pending_offload_metrics(metrics: ColdTierPendingOffloadMetrics<'_>) {
    let mut registry = global_metrics_registry().lock();
    let key = ColdTierRuntimeKey {
        runtime: metrics.runtime.to_string(),
    };
    registry
        .cold_tier_pending_offload_total
        .set(key.clone(), metrics.total_pending as f64);
    registry
        .cold_tier_pending_offload_ready
        .set(key.clone(), metrics.ready as f64);
    registry
        .cold_tier_pending_offload_delayed
        .set(key.clone(), metrics.delayed as f64);
    registry
        .cold_tier_pending_offload_attempts_total
        .set(key.clone(), metrics.total_attempts as f64);
    registry
        .cold_tier_pending_offload_max_attempts
        .set(key, metrics.max_attempts as f64);
}

pub(crate) fn record_cold_tier_reclaim_metrics(metrics: ColdTierReclaimMetrics<'_>) {
    let mut registry = global_metrics_registry().lock();
    let key = ColdTierRuntimeKey {
        runtime: metrics.runtime.to_string(),
    };
    registry
        .cold_tier_reclaim_pending_total
        .set(key.clone(), metrics.total_pending as f64);
    registry
        .cold_tier_reclaim_due_total
        .set(key.clone(), metrics.due as f64);
    registry.cold_tier_reclaim_by_kind.set(
        ColdTierReclaimKindKey {
            runtime: metrics.runtime.to_string(),
            kind: "cold_backing",
        },
        metrics.cold_backing_reclaims as f64,
    );
    registry.cold_tier_reclaim_by_kind.set(
        ColdTierReclaimKindKey {
            runtime: metrics.runtime.to_string(),
            kind: "hot_segment",
        },
        metrics.hot_segment_reclaims as f64,
    );
    for (qos_tier, count) in metrics.by_qos_tier {
        registry.cold_tier_reclaim_by_qos_tier.set(
            ColdTierReclaimQosKey {
                runtime: metrics.runtime.to_string(),
                qos_tier: qos_tier.to_string(),
            },
            count as f64,
        );
    }
    for (policy_rank, count) in metrics.by_policy_rank {
        registry.cold_tier_reclaim_by_policy_rank.set(
            ColdTierReclaimPolicyRankKey {
                runtime: metrics.runtime.to_string(),
                policy_rank,
            },
            count as f64,
        );
    }
}

pub(crate) fn record_cold_tier_operation(
    operation: &'static str,
    result: &'static str,
    error_kind: &'static str,
) {
    global_metrics_registry().lock().cold_tier_operations.add(
        ColdTierOperationKey {
            operation,
            result,
            error_kind,
        },
        1,
    );
}

pub(crate) fn record_cold_tier_operation_result<T>(
    operation: &'static str,
    result: &std::result::Result<T, mooncake_store_core::StoreError>,
) {
    match result {
        Ok(_) => record_cold_tier_operation(operation, "ok", "none"),
        Err(error) => record_cold_tier_operation(operation, "error", cold_tier_error_kind(error)),
    }
}

pub(crate) fn record_cold_restore_singleflight(event: &'static str, result: &'static str) {
    global_metrics_registry()
        .lock()
        .cold_restore_singleflight
        .add(ColdRestoreSingleflightKey { event, result }, 1);
}

pub(crate) fn set_cold_restore_max_concurrent_io_per_object(count: usize) {
    let mut registry = global_metrics_registry().lock();
    let current = registry.cold_restore_max_concurrent_io_per_object;
    let new_val = count as f64;
    if new_val > current {
        registry.cold_restore_max_concurrent_io_per_object = new_val;
    }
}

pub(crate) fn record_cold_tier_ssd_read(result: &'static str, duration: Duration) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .cold_tier_ssd_read_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_cold_tier_ssd_write(result: &'static str, duration: Duration) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .cold_tier_ssd_write_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_io_priority_wait(kind: &'static str, duration: Duration) {
    let key = ResultKey { result: kind };
    global_metrics_registry()
        .lock()
        .io_priority_wait
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_staging_pool_wait(result: &'static str, duration: Duration) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .staging_pool_wait
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_staging_pool_exhaustion() {
    let key = ResultKey {
        result: "try_allocate",
    };
    global_metrics_registry()
        .lock()
        .staging_pool_exhaustion
        .add(key, 1);
}

pub(crate) fn record_extent_store_queue_wait(direction: &'static str, duration: Duration) {
    let key = ResultKey { result: direction };
    global_metrics_registry()
        .lock()
        .extent_store_queue_wait
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_extent_store_pipeline_duration(direction: &'static str, duration: Duration) {
    let key = ResultKey { result: direction };
    global_metrics_registry()
        .lock()
        .extent_store_pipeline_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_batch_get_phase_duration(phase: &'static str, duration: Duration) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .batch_get_phase_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_batch_get_path_items(phase: &'static str, count: u64) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .batch_get_path_items
        .add(key, count);
}

pub(crate) fn record_facade_phase_duration(phase: &'static str, duration: Duration) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .facade_phase_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_rdma_transfer_duration(phase: &'static str, duration: Duration) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .rdma_transfer_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_rdma_bytes(phase: &'static str, bytes: u64) {
    let key = PhaseKey { phase };
    global_metrics_registry().lock().rdma_bytes.add(key, bytes);
}

pub(crate) fn record_cold_read_batch_wall_duration(result: &'static str, duration: Duration) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .cold_read_batch_wall_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_io_uring_phase_duration(phase: &'static str, duration: Duration) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .io_uring_phase_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_io_uring_ops(result: &'static str, count: u64) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .io_uring_ops
        .add(key, count);
}

pub(crate) fn record_cold_restore_batch_duration(phase: &'static str, duration: Duration) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .cold_restore_batch_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_cold_restore_batch_items(result: &'static str, count: u64) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .cold_restore_batch_items
        .add(key, count);
}

pub(crate) fn record_cold_prefetch_worker_duration(result: &'static str, duration: Duration) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .cold_prefetch_worker_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_cold_prefetch_worker_items(result: &'static str, count: u64) {
    let key = ResultKey { result };
    global_metrics_registry()
        .lock()
        .cold_prefetch_worker_items
        .add(key, count);
}

pub(crate) fn record_batch_is_exist_duration(phase: &'static str, duration: Duration) {
    let key = PhaseKey { phase };
    global_metrics_registry()
        .lock()
        .batch_is_exist_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn cold_tier_error_kind(error: &mooncake_store_core::StoreError) -> &'static str {
    match error {
        mooncake_store_core::StoreError::NotFound(_) => "not_found",
        mooncake_store_core::StoreError::InvalidState(_) => "invalid_state",
        mooncake_store_core::StoreError::Conflict(_) => "conflict",
        mooncake_store_core::StoreError::StaleEpoch(_) => "stale_epoch",
        mooncake_store_core::StoreError::Transport(_) => "transport",
        mooncake_store_core::StoreError::Allocator(_) => "allocator",
        mooncake_store_core::StoreError::Metadata(_) => "metadata",
        mooncake_store_core::StoreError::Unsupported(_) => "unsupported",
        mooncake_store_core::StoreError::QuotaExceeded { .. } => "quota_exceeded",
        mooncake_store_core::StoreError::Backpressure(_) => "backpressure",
    }
}

pub(crate) fn snapshot_metrics_with_registry(
    registry: &SharedMetricsRegistry,
    process: ProcessSnapshot,
) -> MetricsSnapshot {
    #[cfg(test)]
    if registry.ptr_eq(global_metrics_registry()) {
        return registry
            .lock()
            .snapshot_with_tenant(process, test_process_tenant_label());
    }

    registry.lock().snapshot(process)
}

#[cfg(test)]
pub(crate) fn reset_metrics() {
    set_test_process_tenant(None);
    reset_metrics_with_registry(global_metrics_registry());
}

#[cfg(test)]
pub(crate) fn reset_metrics_with_registry(registry: &SharedMetricsRegistry) {
    *registry.lock() = MetricsRegistry::default();
}

fn replace_runtime_leases(registry: &mut MetricsRegistry, leases: &[ClientLease]) {
    registry.runtime_leases = leases
        .iter()
        .map(|lease| {
            (
                lease.runtime.to_string(),
                RuntimeLeaseMetric {
                    runtime: lease.runtime.to_string(),
                    state: client_state_label(lease.state),
                    expires_at_ms: lease.expires_at_ms,
                },
            )
        })
        .collect();
}

#[cfg(test)]
pub(crate) fn snapshot_runtime_leases_after_updates(updates: &[&[ClientLease]]) -> MetricsSnapshot {
    let mut registry = MetricsRegistry::default();
    for leases in updates {
        replace_runtime_leases(&mut registry, leases);
    }
    registry.snapshot(ProcessSnapshot::default())
}

#[cfg(test)]
pub(crate) fn new_metrics_registry() -> SharedMetricsRegistry {
    SharedMetricsRegistry::new()
}

pub(crate) fn global_metrics_registry() -> &'static SharedMetricsRegistry {
    METRICS.get_or_init(SharedMetricsRegistry::new)
}

fn segment_identity(announcement: &SegmentAnnouncement) -> (String, String) {
    (
        announcement.owner.to_string(),
        announcement.segment_name.0.clone(),
    )
}

fn object_key_tenant(key: &str) -> &str {
    key.split_once("::")
        .or_else(|| key.split_once('/'))
        .map(|(tenant, _)| tenant)
        .unwrap_or("default")
}

fn segment_sample(announcement: &SegmentAnnouncement) -> SegmentSample {
    SegmentSample {
        runtime: announcement.owner.to_string(),
        segment: announcement.segment_name.0.clone(),
        state: segment_state_label(announcement.state),
        tier: "dram",
        capacity_bytes: announcement.capacity_bytes,
        used_bytes: announcement.used_bytes,
    }
}

fn segment_state_label(state: SegmentLifecycleState) -> &'static str {
    match state {
        SegmentLifecycleState::Active => "active",
        SegmentLifecycleState::Draining => "draining",
        SegmentLifecycleState::Retired => "retired",
    }
}

fn client_state_label(state: ClientLifecycleState) -> &'static str {
    match state {
        ClientLifecycleState::Active => "active",
        ClientLifecycleState::Standby => "standby",
        ClientLifecycleState::Draining => "draining",
        ClientLifecycleState::Sealed => "sealed",
        ClientLifecycleState::Offline => "offline",
    }
}

fn replica_tier_label(tier: ReplicaTier) -> &'static str {
    match tier {
        ReplicaTier::Dram => "dram",
        ReplicaTier::Nvme => "nvme",
        ReplicaTier::File => "file",
        ReplicaTier::Unknown => "unknown",
    }
}

fn cold_tier_device_state_label(state: ColdTierDeviceState) -> &'static str {
    match state {
        ColdTierDeviceState::Unregistered => "unregistered",
        ColdTierDeviceState::Healthy => "healthy",
        ColdTierDeviceState::Full => "full",
        ColdTierDeviceState::DisabledByAdmin => "disabled_by_admin",
        ColdTierDeviceState::Draining => "draining",
        ColdTierDeviceState::Failed => "failed",
    }
}
