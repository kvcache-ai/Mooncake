use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use mooncake_store_core::{
    ClientLease, ClientLifecycleState, ObjectKey, ObjectRoute, ReplicaTier, SegmentAnnouncement,
    SegmentLifecycleState,
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
pub(crate) const EVICTION_TOTAL: &str = "mooncake_store_eviction_total";
pub(crate) const EVICTION_DURATION: &str = "mooncake_store_eviction_duration_seconds";
pub(crate) const TRANSPORT_OPERATION_TOTAL: &str = "mooncake_store_transport_operation_total";
pub(crate) const TRANSPORT_BYTES_TOTAL: &str = "mooncake_store_transport_bytes_total";
pub(crate) const METADATA_OPERATION_TOTAL: &str = "mooncake_store_metadata_operation_total";
pub(crate) const METADATA_OPERATION_INFLIGHT: &str = "mooncake_store_metadata_operation_inflight";
pub(crate) const METADATA_OPERATION_DURATION: &str =
    "mooncake_store_metadata_operation_duration_seconds";

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
    pub eviction: Vec<CounterSample<ResultKey>>,
    pub eviction_duration: Vec<HistogramSample<ResultKey>>,
    pub transport_operations: Vec<CounterSample<TransportOperationKey>>,
    pub transport_bytes: Vec<CounterSample<TransportBytesKey>>,
    pub metadata_operations: Vec<CounterSample<MetadataOperationKey>>,
    pub metadata_inflight: Vec<GaugeSample<MetadataInflightKey>>,
    pub metadata_duration: Vec<HistogramSample<MetadataOperationKey>>,
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
}

impl<K> Default for HistogramFamily<K> {
    fn default() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }
}

impl<K: Ord + Clone> HistogramFamily<K> {
    fn observe(&mut self, key: K, value: f64) {
        self.inner.entry(key).or_default().observe(value);
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
    fn observe(&mut self, value: f64) {
        for (index, bucket) in REQUEST_DURATION_BUCKETS.iter().enumerate() {
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

#[derive(Default)]
struct MetricsRegistry {
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
    eviction: CounterFamily<ResultKey>,
    eviction_duration: HistogramFamily<ResultKey>,
    transport_operations: CounterFamily<TransportOperationKey>,
    transport_bytes: CounterFamily<TransportBytesKey>,
    metadata_operations: CounterFamily<MetadataOperationKey>,
    metadata_inflight: GaugeFamily<MetadataInflightKey>,
    metadata_duration: HistogramFamily<MetadataOperationKey>,
}

#[derive(Clone, Default)]
pub(crate) struct SharedMetricsRegistry {
    inner: Arc<Mutex<MetricsRegistry>>,
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
}

impl MetricsRegistry {
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
        MetricsSnapshot {
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
            eviction: self.eviction.snapshot(),
            eviction_duration: self.eviction_duration.snapshot(),
            transport_operations: self.transport_operations.snapshot(),
            transport_bytes: self.transport_bytes.snapshot(),
            metadata_operations: self.metadata_operations.snapshot(),
            metadata_inflight: self.metadata_inflight.snapshot(),
            metadata_duration: self.metadata_duration.snapshot(),
            process,
        }
    }

    fn object_route_snapshot(&self) -> Vec<GaugeSample<TenantKey>> {
        let mut by_tenant = BTreeMap::<String, u64>::new();
        for route in self.routes.values() {
            let tenant = route
                .key
                .0
                .split_once("::")
                .map(|(tenant, _)| tenant)
                .unwrap_or("default");
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

static METRICS: OnceLock<SharedMetricsRegistry> = OnceLock::new();

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
    if bytes == 0 {
        return;
    }
    global_metrics_registry().lock().transport_bytes.add(
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
    global_metrics_registry().lock().transport_operations.add(
        TransportOperationKey {
            direction,
            peer_kind,
            result,
        },
        1,
    );
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

pub(crate) fn record_eviction(result: &'static str, duration: Duration) {
    let mut registry = global_metrics_registry().lock();
    let key = ResultKey { result };
    registry.eviction.add(key.clone(), 1);
    registry
        .eviction_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn snapshot_metrics_with_registry(
    registry: &SharedMetricsRegistry,
    process: ProcessSnapshot,
) -> MetricsSnapshot {
    registry.lock().snapshot(process)
}

#[cfg(test)]
pub(crate) fn reset_metrics() {
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
