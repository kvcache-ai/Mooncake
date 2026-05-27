use std::sync::Arc;
use std::time::Instant;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, ClientStableId, MetadataBackend, ObjectKey,
    ObjectRoute, Result, RouteCasRequest, RouteControlMode, RouteDirectory,
};
pub(crate) use mooncake_store_route::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_get, authority_get_many,
    authority_list_routes, authority_list_routes_by_replica_owner, authority_replace,
    authority_replace_many, bind_local_authority_service,
};
use mooncake_store_route::{
    set_route_metrics_sink, RouteAuthorityClient, RouteMembershipProvider, RouteMetricsSink,
};

use crate::client::{
    cached_live_client_snapshot, refresh_live_client_cache, SharedLiveClientCache,
    SharedSuspectRuntimeCache,
};
use crate::control_plane::ControlPlaneClient;
use crate::observability::{registry, OperationTracker};

pub(crate) fn build_route_directory(
    mode: RouteControlMode,
    route_topk: usize,
    metadata: Arc<dyn MetadataBackend>,
    lease: &ClientLease,
    control_plane: Arc<ControlPlaneClient>,
    live_client_cache: SharedLiveClientCache,
    suspect_runtime_cache: SharedSuspectRuntimeCache,
) -> Arc<dyn RouteDirectory> {
    set_route_metrics_sink(Arc::new(ClientRouteMetricsSink));
    let membership = Arc::new(ClientRouteMembership {
        metadata: metadata.clone(),
        live_client_cache,
        suspect_runtime_cache,
    });
    mooncake_store_route::build_route_directory(
        mode,
        route_topk,
        metadata,
        lease,
        control_plane,
        membership,
    )
}

struct ClientRouteMembership {
    metadata: Arc<dyn MetadataBackend>,
    live_client_cache: SharedLiveClientCache,
    suspect_runtime_cache: SharedSuspectRuntimeCache,
}

impl RouteMembershipProvider for ClientRouteMembership {
    fn live_clients(
        &self,
        force_refresh: bool,
        operation: &'static str,
    ) -> Result<Vec<ClientLease>> {
        if force_refresh {
            return refresh_live_client_cache(
                self.metadata.as_ref(),
                &self.live_client_cache,
                operation,
            );
        }
        cached_live_client_snapshot(&self.live_client_cache)
    }

    fn reconcile_suspects(&self, leases: &[ClientLease]) {
        self.suspect_runtime_cache
            .lock()
            .reconcile_with_leases(leases);
    }

    fn is_suspect(&self, runtime: &ClientRuntimeId) -> bool {
        self.suspect_runtime_cache.lock().contains(runtime)
    }

    fn mark_suspect(
        &self,
        runtime: ClientRuntimeId,
        quarantine_until: Instant,
        observed: Option<&ClientLease>,
    ) {
        self.suspect_runtime_cache
            .lock()
            .mark(runtime, quarantine_until, observed);
    }
}

impl RouteAuthorityClient for ControlPlaneClient {
    fn batch_get_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<Option<ObjectRoute>>>> {
        ControlPlaneClient::batch_get_routes(self, lease, namespace, authority, keys)
    }

    fn batch_compare_and_swap_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>> {
        ControlPlaneClient::batch_compare_and_swap_routes(
            self, lease, namespace, authority, requests,
        )
    }

    fn batch_replace_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<()>>> {
        ControlPlaneClient::batch_replace_routes(self, lease, namespace, authority, requests)
    }

    fn list_routes_by_replica_owner(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        ControlPlaneClient::list_routes_by_replica_owner(self, lease, namespace, authority, owner)
    }
}

struct ClientRouteMetricsSink;

impl RouteMetricsSink for ClientRouteMetricsSink {
    fn record_route_repair(&self, operation: &'static str) {
        let result: Result<()> = Ok(());
        OperationTracker::new(operation).finish(&result, 0);
    }

    fn record_route_cas(&self, outcome: &'static str) {
        registry::record_route_cas(outcome);
    }

    fn record_route(&self, route: &ObjectRoute) {
        registry::record_route(route);
    }

    fn remove_route(&self, key: &ObjectKey) {
        registry::remove_route(key);
    }
}
