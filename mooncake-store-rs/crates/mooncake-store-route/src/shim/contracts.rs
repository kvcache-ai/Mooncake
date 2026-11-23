//! Public integration contracts for the route table shim.

use std::time::Instant;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientLease, ClientRuntimeId, ClientStableId, ObjectKey, ObjectRoute, Result, RouteCasRequest,
    RouteVersion, StoreError,
};

pub const DEFAULT_ROUTE_OWNER_PAGE_SIZE: usize = 256;

/// Authority-local state for one object key.
///
/// A record may temporarily contain both a live route and a delete version while
/// authorities converge. Callers must prefer the delete version when it is
/// greater than or equal to the live route version.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteAuthorityRecord {
    /// The live route currently retained by this authority, if any.
    pub route: Option<ObjectRoute>,
    /// The highest retained delete version for the key, if any.
    pub delete_version: Option<RouteVersion>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteOwnerPage {
    pub routes: Vec<ObjectRoute>,
    /// The final route key returned by this page. Callers resume strictly after it.
    pub next_cursor: Option<String>,
    /// The owner-local route epoch used to validate the complete paged scan.
    pub snapshot_epoch: Option<u64>,
}

/// Parameters for one bounded owner-route page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteOwnerPageQuery {
    pub owner: ClientRuntimeId,
    pub cursor: Option<String>,
    pub snapshot_epoch: Option<u64>,
    pub limit: usize,
}

impl RouteOwnerPage {
    pub fn empty() -> Self {
        Self {
            routes: Vec::new(),
            next_cursor: None,
            snapshot_epoch: None,
        }
    }
}

pub(crate) trait RouteAuthorityClient: Send + Sync {
    fn batch_get_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<RouteAuthorityRecord>>>;

    fn batch_contains_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<bool>>>;

    fn batch_compare_and_swap_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>>;

    fn batch_replace_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<()>>>;

    fn list_routes_by_replica_owner_page(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        query: &RouteOwnerPageQuery,
    ) -> Result<RouteOwnerPage>;

    fn get_allocation_route_references(
        &self,
        _lease: &ClientLease,
        _namespace: &str,
        _authority: &ClientStableId,
        _identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        Err(StoreError::Unsupported(
            "route authority client does not support allocation reference queries".to_string(),
        ))
    }

    fn get_allocation_route_reference_keys(
        &self,
        _lease: &ClientLease,
        _namespace: &str,
        _authority: &ClientStableId,
        _identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        Err(StoreError::Unsupported(
            "route authority client does not support allocation reference key queries".to_string(),
        ))
    }
}

pub trait RouteAuthorityService: Send + Sync {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>>;

    fn get_route_record(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<RouteAuthorityRecord> {
        self.get_route(namespace, authority, key)
            .map(|route| RouteAuthorityRecord {
                route,
                delete_version: None,
            })
    }

    fn list_routes_by_replica_owner(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>>;

    fn list_routes_by_replica_owner_page(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        snapshot_epoch: Option<u64>,
        limit: usize,
    ) -> Result<RouteOwnerPage> {
        let _ = (namespace, authority, owner, cursor, snapshot_epoch, limit);
        Err(StoreError::Unsupported(
            "route authority does not implement bounded owner-route pagination".to_string(),
        ))
    }

    fn get_allocation_route_references(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        _identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        Err(StoreError::Unsupported(
            "route authority does not implement allocation reference queries".to_string(),
        ))
    }

    fn get_allocation_route_reference_keys(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        _identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        Err(StoreError::Unsupported(
            "route authority does not implement allocation reference key queries".to_string(),
        ))
    }

    fn compare_and_swap_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

    fn replace_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()>;

    fn contains_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<bool> {
        Ok(self.get_route(namespace, authority, key)?.is_some())
    }

    fn batch_contains_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<bool>> {
        keys.iter()
            .map(|key| self.contains_route(namespace, authority, key))
            .collect()
    }

    fn batch_get_route_records(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<RouteAuthorityRecord>> {
        self.batch_get_routes(namespace, authority, keys)
            .into_iter()
            .map(|result| {
                result.map(|route| RouteAuthorityRecord {
                    route,
                    delete_version: None,
                })
            })
            .collect()
    }

    fn batch_get_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<Option<ObjectRoute>>> {
        keys.iter()
            .map(|key| self.get_route(namespace, authority, key))
            .collect()
    }

    fn batch_compare_and_swap_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<CasResult>> {
        requests
            .iter()
            .map(|request| {
                self.compare_and_swap_route(
                    namespace,
                    authority,
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                )
            })
            .collect()
    }

    fn batch_replace_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| {
                if request.next.is_none() && request.expected.is_some() {
                    return Err(StoreError::Unsupported(
                        "route authority must implement versioned delete marker replacement"
                            .to_string(),
                    ));
                }
                self.replace_route(namespace, authority, &request.key, request.next.as_ref())
            })
            .collect()
    }
}

pub trait RouteMembershipProvider: Send + Sync {
    fn live_clients(
        &self,
        force_refresh: bool,
        operation: &'static str,
    ) -> Result<Vec<ClientLease>>;

    fn retained_live_clients(&self, operation: &'static str) -> Result<Vec<ClientLease>> {
        self.live_clients(false, operation)
    }

    fn reconcile_suspects(&self, leases: &[ClientLease]);

    fn is_suspect(&self, runtime: &ClientRuntimeId) -> bool;

    fn mark_suspect(
        &self,
        runtime: ClientRuntimeId,
        quarantine_until: Instant,
        observed: Option<&ClientLease>,
    );

    fn clear_suspect(&self, runtime: &ClientRuntimeId);
}
