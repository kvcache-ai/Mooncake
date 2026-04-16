use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
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
pub(crate) const REPLICATION_PUBLISH_DURATION: &str =
    "mooncake_store_replication_publish_duration_seconds";
pub(crate) const CHECKSUM_VALIDATION_TOTAL: &str = "mooncake_store_checksum_validation_total";
pub(crate) const TENANT_QUOTA_RESERVATION_TOTAL: &str =
    "mooncake_store_tenant_quota_reservation_total";
pub(crate) const TENANT_QUOTA_FINALIZE_TOTAL: &str = "mooncake_store_tenant_quota_finalize_total";
pub(crate) const TENANT_QUOTA_ABORT_TOTAL: &str = "mooncake_store_tenant_quota_abort_total";
pub(crate) const TENANT_QUOTA_RECONCILE_TOTAL: &str = "mooncake_store_tenant_quota_reconcile_total";
pub(crate) const REBALANCE_ROUTES_TOTAL: &str = "mooncake_store_rebalance_routes_total";
pub(crate) const REBALANCE_BYTES_TOTAL: &str = "mooncake_store_rebalance_bytes_total";
pub(crate) const SEGMENT_LIFECYCLE_TOTAL: &str = "mooncake_store_segment_lifecycle_total";
pub(crate) const EVICTION_TOTAL: &str = "mooncake_store_eviction_total";
pub(crate) const EVICTION_DURATION: &str = "mooncake_store_eviction_duration_seconds";
pub(crate) const TRANSPORT_BYTES_TOTAL: &str = "mooncake_store_transport_bytes_total";

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
    pub replication_publish_duration: Vec<HistogramSample<ResultKey>>,
    pub checksum_validation: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_reservation: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_finalize: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_abort: Vec<CounterSample<ResultKey>>,
    pub tenant_quota_reconcile: Vec<CounterSample<ResultKey>>,
    pub rebalance_routes: Vec<CounterSample<PhaseResultKey>>,
    pub rebalance_bytes: Vec<CounterSample<PhaseKey>>,
    pub segment_lifecycle: Vec<CounterSample<ActionResultKey>>,
    pub eviction: Vec<CounterSample<ResultKey>>,
    pub eviction_duration: Vec<HistogramSample<ResultKey>>,
    pub transport_bytes: Vec<CounterSample<TransportBytesKey>>,
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
    replication_publish_duration: HistogramFamily<ResultKey>,
    checksum_validation: CounterFamily<ResultKey>,
    tenant_quota_reservation: CounterFamily<ResultKey>,
    tenant_quota_finalize: CounterFamily<ResultKey>,
    tenant_quota_abort: CounterFamily<ResultKey>,
    tenant_quota_reconcile: CounterFamily<ResultKey>,
    rebalance_routes: CounterFamily<PhaseResultKey>,
    rebalance_bytes: CounterFamily<PhaseKey>,
    segment_lifecycle: CounterFamily<ActionResultKey>,
    eviction: CounterFamily<ResultKey>,
    eviction_duration: HistogramFamily<ResultKey>,
    transport_bytes: CounterFamily<TransportBytesKey>,
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
            replication_publish_duration: self.replication_publish_duration.snapshot(),
            checksum_validation: self.checksum_validation.snapshot(),
            tenant_quota_reservation: self.tenant_quota_reservation.snapshot(),
            tenant_quota_finalize: self.tenant_quota_finalize.snapshot(),
            tenant_quota_abort: self.tenant_quota_abort.snapshot(),
            tenant_quota_reconcile: self.tenant_quota_reconcile.snapshot(),
            rebalance_routes: self.rebalance_routes.snapshot(),
            rebalance_bytes: self.rebalance_bytes.snapshot(),
            segment_lifecycle: self.segment_lifecycle.snapshot(),
            eviction: self.eviction.snapshot(),
            eviction_duration: self.eviction_duration.snapshot(),
            transport_bytes: self.transport_bytes.snapshot(),
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

static METRICS: OnceLock<Mutex<MetricsRegistry>> = OnceLock::new();

#[derive(Clone, Debug)]
struct RuntimeLeaseMetric {
    runtime: String,
    state: &'static str,
    expires_at_ms: u64,
}

pub(crate) fn record_request(
    operation: &'static str,
    scope: &'static str,
    result: &'static str,
    bytes_in: u64,
    bytes_out: u64,
    duration: Duration,
) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .record_request(operation, scope, result, bytes_in, bytes_out, duration);
}

pub(crate) fn increment_inflight(operation: &'static str, scope: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .request_inflight
        .add(RequestInflightKey { operation, scope }, 1.0);
}

pub(crate) fn decrement_inflight(operation: &'static str, scope: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .request_inflight
        .add(RequestInflightKey { operation, scope }, -1.0);
}

pub(crate) fn record_route_cas(result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .route_cas
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_replication_publish(result: &'static str, duration: Duration) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .replication_publish_duration
        .observe(ResultKey { result }, duration.as_secs_f64());
}

pub(crate) fn record_checksum_validation(result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .checksum_validation
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_reservation(result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .tenant_quota_reservation
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_finalize(result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .tenant_quota_finalize
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_abort(result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .tenant_quota_abort
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_tenant_quota_reconcile(result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .tenant_quota_reconcile
        .add(ResultKey { result }, 1);
}

pub(crate) fn record_rebalance_route(phase: &'static str, result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .rebalance_routes
        .add(PhaseResultKey { phase, result }, 1);
}

pub(crate) fn record_rebalance_bytes(phase: &'static str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .rebalance_bytes
        .add(PhaseKey { phase }, bytes);
}

pub(crate) fn record_transport_bytes(direction: &'static str, peer_kind: &'static str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .transport_bytes
        .add(
            TransportBytesKey {
                direction,
                peer_kind,
            },
            bytes,
        );
}

pub(crate) fn record_membership_refresh(result: &'static str, duration: Duration) {
    let mut registry = metrics_registry().lock().expect("metrics lock poisoned");
    let key = ResultKey { result };
    registry.membership_refresh.add(key.clone(), 1);
    registry
        .membership_refresh_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn record_runtime_leases(leases: &[ClientLease]) {
    let mut registry = metrics_registry().lock().expect("metrics lock poisoned");
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

pub(crate) fn record_heartbeat_health(
    runtime: &str,
    consecutive_failures: u64,
    last_success_ms: u64,
) {
    let mut registry = metrics_registry().lock().expect("metrics lock poisoned");
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
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .segments
        .insert(segment_identity(announcement), segment_sample(announcement));
}

pub(crate) fn record_segment_removed(announcement: &SegmentAnnouncement) {
    let mut sample = segment_sample(announcement);
    sample.state = "retired";
    sample.used_bytes = 0;
    sample.capacity_bytes = 0;
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .segments
        .insert((sample.runtime.clone(), sample.segment.clone()), sample);
}

pub(crate) fn record_route(route: &ObjectRoute) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .routes
        .insert(route.key.0.clone(), route.clone());
}

pub(crate) fn remove_route(key: &ObjectKey) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .routes
        .remove(&key.0);
}

pub(crate) fn record_segment_lifecycle(action: &'static str, result: &'static str) {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .segment_lifecycle
        .add(ActionResultKey { action, result }, 1);
}

pub(crate) fn record_eviction(result: &'static str, duration: Duration) {
    let mut registry = metrics_registry().lock().expect("metrics lock poisoned");
    let key = ResultKey { result };
    registry.eviction.add(key.clone(), 1);
    registry
        .eviction_duration
        .observe(key, duration.as_secs_f64());
}

pub(crate) fn snapshot_metrics(process: ProcessSnapshot) -> MetricsSnapshot {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .snapshot(process)
}

#[cfg(test)]
pub(crate) fn reset_metrics() {
    *metrics_registry().lock().expect("metrics lock poisoned") = MetricsRegistry::default();
}

fn metrics_registry() -> &'static Mutex<MetricsRegistry> {
    METRICS.get_or_init(|| Mutex::new(MetricsRegistry::default()))
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
