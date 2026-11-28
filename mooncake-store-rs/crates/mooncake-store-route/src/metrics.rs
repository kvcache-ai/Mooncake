use std::sync::{Arc, OnceLock};

use mooncake_store_core::CasResult;
use parking_lot::RwLock;

/// Event counters for routing operations.
///
/// Authority gauges are owned by route tables and collected through
/// [`crate::snapshot_route_gauges`], independently of the event sink.
pub trait RouteMetricsSink: Send + Sync {
    fn record_route_repair(&self, operation: &'static str);

    fn record_route_cas(&self, outcome: &'static str);

    fn record_delete_marker_state(&self, entries: u64, bytes: u64, budget_bytes: u64);

    fn record_delete_marker_event(&self, event: &'static str, count: u64);
}

struct NoopRouteMetricsSink;

impl RouteMetricsSink for NoopRouteMetricsSink {
    fn record_route_repair(&self, _operation: &'static str) {}

    fn record_route_cas(&self, _outcome: &'static str) {}

    fn record_delete_marker_state(&self, _entries: u64, _bytes: u64, _budget_bytes: u64) {}

    fn record_delete_marker_event(&self, _event: &'static str, _count: u64) {}
}

pub fn set_route_metrics_sink(sink: Arc<dyn RouteMetricsSink>) {
    *route_metrics_sink().write() = sink;
}

fn route_metrics_sink() -> &'static RwLock<Arc<dyn RouteMetricsSink>> {
    static SINK: OnceLock<RwLock<Arc<dyn RouteMetricsSink>>> = OnceLock::new();
    SINK.get_or_init(|| RwLock::new(Arc::new(NoopRouteMetricsSink)))
}

pub(crate) fn record_route_repair_metric(operation: &'static str) {
    route_metrics_sink().read().record_route_repair(operation);
}

pub(crate) fn record_cas_outcome(cas: &CasResult) {
    if cas.applied {
        route_metrics_sink().read().record_route_cas("ok");
        return;
    }
    route_metrics_sink().read().record_route_cas("conflict");
}

pub(crate) fn record_delete_marker_state(entries: u64, bytes: u64, budget_bytes: u64) {
    route_metrics_sink()
        .read()
        .record_delete_marker_state(entries, bytes, budget_bytes);
}

pub(crate) fn record_delete_marker_event(event: &'static str, count: u64) {
    if count == 0 {
        return;
    }
    route_metrics_sink()
        .read()
        .record_delete_marker_event(event, count);
}
