use std::sync::{Arc, OnceLock};

use mooncake_store_core::{CasResult, ObjectKey, ObjectRoute};
use parking_lot::RwLock;

pub trait RouteMetricsSink: Send + Sync {
    fn record_route_repair(&self, operation: &'static str);

    fn record_route_cas(&self, outcome: &'static str);

    fn record_route(&self, route: &ObjectRoute);

    fn remove_route(&self, key: &ObjectKey);
}

struct NoopRouteMetricsSink;

impl RouteMetricsSink for NoopRouteMetricsSink {
    fn record_route_repair(&self, _operation: &'static str) {}

    fn record_route_cas(&self, _outcome: &'static str) {}

    fn record_route(&self, _route: &ObjectRoute) {}

    fn remove_route(&self, _key: &ObjectKey) {}
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

pub(crate) fn record_cas_outcome(cas: &CasResult, next: Option<&ObjectRoute>, key: &ObjectKey) {
    if cas.applied {
        route_metrics_sink().read().record_route_cas("ok");
        match next {
            Some(route) => route_metrics_sink().read().record_route(route),
            None => route_metrics_sink().read().remove_route(key),
        }
        return;
    }
    route_metrics_sink().read().record_route_cas("conflict");
}
