//! Public integration contracts for the route table shim.

use std::time::Instant;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, ClientStableId, ObjectKey, ObjectRoute, Result,
    RouteCasRequest, RouteVersion, StoreError,
};

pub const DEFAULT_ROUTE_OWNER_PAGE_SIZE: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteOwnerPage {
    pub routes: Vec<ObjectRoute>,
    /// The final route key returned by this page. Callers resume strictly after it.
    pub next_cursor: Option<String>,
}

impl RouteOwnerPage {
    pub fn empty() -> Self {
        Self {
            routes: Vec::new(),
            next_cursor: None,
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
    ) -> Result<Vec<Result<Option<ObjectRoute>>>>;

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

    fn list_routes_by_replica_owner(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>>;

    fn list_routes_by_replica_owner_page(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RouteOwnerPage>;
}

pub trait RouteAuthorityService: Send + Sync {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>>;

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
        limit: usize,
    ) -> Result<RouteOwnerPage> {
        // Legacy service implementations remain source-compatible. Bounded callers fail closed
        // until the service supplies an indexed pagination implementation.
        let _ = (namespace, authority, owner, cursor, limit);
        Err(StoreError::Unsupported(
            "route authority does not implement bounded owner-route pagination".to_string(),
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

    fn reconcile_suspects(&self, leases: &[ClientLease]);

    fn is_suspect(&self, runtime: &ClientRuntimeId) -> bool;

    fn mark_suspect(
        &self,
        runtime: ClientRuntimeId,
        quarantine_until: Instant,
        observed: Option<&ClientLease>,
    );
}
