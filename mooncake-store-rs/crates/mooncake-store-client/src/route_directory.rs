use std::sync::Arc;
use std::time::Instant;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientLease, ClientRuntimeId, ClientStableId, MetadataBackend, ObjectKey, ObjectRoute, Result,
    RouteCasRequest, RouteControlMode, RouteDirectory, RouteVersion,
};
use mooncake_store_route::{
    set_route_metrics_sink, LocalRouteAuthority, RouteAuthorityRecord, RouteAuthorityService,
    RouteMembershipProvider, RouteMetricsSink, RouteOwnerPage,
};

use crate::client::{
    cached_live_client_snapshot, cached_retained_live_client_snapshot, refresh_live_client_cache,
    SharedLiveClientCache, SharedSuspectRuntimeCache,
};
use crate::control_plane::ControlPlaneClient;
use crate::observability::{registry, OperationTracker};

pub(crate) struct RouteDirectoryBuildConfig {
    pub(crate) mode: RouteControlMode,
    pub(crate) route_topk: usize,
    pub(crate) delete_marker_budget_bytes: usize,
}

pub(crate) fn build_route_directory(
    config: RouteDirectoryBuildConfig,
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
        config.mode,
        config.route_topk,
        config.delete_marker_budget_bytes,
        metadata,
        lease,
        control_plane,
        membership,
    )
}

pub(crate) fn bind_local_authority_service(
    namespace: &str,
    authority: &ClientStableId,
    service: Arc<dyn RouteAuthorityService>,
) {
    LocalRouteAuthority::new(namespace, authority.clone()).bind_service(service);
}

pub(crate) fn authority_get(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Result<Option<ObjectRoute>> {
    LocalRouteAuthority::new(namespace, authority.clone()).get_route(key)
}

pub(crate) fn authority_get_record(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Result<RouteAuthorityRecord> {
    LocalRouteAuthority::new(namespace, authority.clone()).get_route_record(key)
}

pub(crate) fn authority_get_records(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<RouteAuthorityRecord>> {
    LocalRouteAuthority::new(namespace, authority.clone()).get_route_records(keys)
}

pub(crate) fn authority_get_allocation_route_references(
    namespace: &str,
    authority: &ClientStableId,
    identities: &[AllocationRouteIdentity],
) -> Result<Vec<AllocationRouteReferenceState>> {
    LocalRouteAuthority::new(namespace, authority.clone())
        .get_allocation_route_references(identities)
}

pub(crate) fn authority_get_allocation_route_reference_keys(
    namespace: &str,
    authority: &ClientStableId,
    identities: &[AllocationRouteIdentity],
) -> Result<Vec<AllocationRouteReference>> {
    LocalRouteAuthority::new(namespace, authority.clone())
        .get_allocation_route_reference_keys(identities)
}

pub(crate) fn authority_contains_many(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<bool>> {
    LocalRouteAuthority::new(namespace, authority.clone()).contains_routes(keys)
}

pub(crate) fn authority_list_routes_by_replica_owner(
    namespace: &str,
    authority: &ClientStableId,
    owner: &ClientRuntimeId,
) -> Result<Vec<ObjectRoute>> {
    LocalRouteAuthority::new(namespace, authority.clone()).list_routes_by_replica_owner(owner)
}

pub(crate) fn authority_list_routes_by_replica_owner_page(
    namespace: &str,
    authority: &ClientStableId,
    owner: &ClientRuntimeId,
    cursor: Option<&str>,
    snapshot_epoch: Option<u64>,
    limit: usize,
) -> Result<RouteOwnerPage> {
    LocalRouteAuthority::new(namespace, authority.clone()).list_routes_by_replica_owner_page(
        owner,
        cursor,
        snapshot_epoch,
        limit,
    )
}

pub(crate) fn authority_compare_and_swap(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    expected: Option<RouteVersion>,
    next: Option<&ObjectRoute>,
) -> Result<CasResult> {
    LocalRouteAuthority::new(namespace, authority.clone())
        .compare_and_swap_route(key, expected, next)
}

pub(crate) fn authority_compare_and_swap_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<Vec<CasResult>> {
    LocalRouteAuthority::new(namespace, authority.clone()).compare_and_swap_routes(requests)
}

pub(crate) fn authority_replace(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    next: Option<&ObjectRoute>,
) -> Result<()> {
    LocalRouteAuthority::new(namespace, authority.clone()).replace_route(key, next)
}

pub(crate) fn authority_replace_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<()> {
    LocalRouteAuthority::new(namespace, authority.clone()).replace_routes(requests)
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

    fn retained_live_clients(&self, _operation: &'static str) -> Result<Vec<ClientLease>> {
        cached_retained_live_client_snapshot(&self.live_client_cache)
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

    fn record_delete_marker_state(&self, entries: u64, bytes: u64, budget_bytes: u64) {
        registry::record_delete_marker_state(entries, bytes, budget_bytes);
    }

    fn record_delete_marker_event(&self, event: &'static str, count: u64) {
        registry::record_delete_marker_event(event, count);
    }
}
