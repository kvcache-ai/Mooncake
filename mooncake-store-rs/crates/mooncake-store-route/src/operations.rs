use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, ClientStableId, NamespaceScope, ObjectKey,
    ObjectRoute, Result, ReuseIdentity, RouteCasRequest, RouteDirectory, RouteState, RouteVersion,
};

use crate::local_authority::LocalRouteAuthority;

pub trait RouteHitReporter {
    fn report_route_hits(&self, routes: &[&ObjectRoute]);
}

#[derive(Clone)]
pub struct RouteOperations {
    directory: Arc<dyn RouteDirectory>,
    observer: ClientLease,
}

impl RouteOperations {
    pub fn new(directory: Arc<dyn RouteDirectory>, observer: ClientLease) -> Self {
        Self {
            directory,
            observer,
        }
    }

    pub fn load_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        self.directory.get_object_route(&self.observer, key)
    }

    pub fn load_routes(&self, keys: &[ObjectKey]) -> Result<Vec<Option<ObjectRoute>>> {
        self.directory.get_object_routes(&self.observer, keys)
    }

    pub fn load_routes_bounded(&self, keys: &[ObjectKey]) -> Result<Vec<Option<ObjectRoute>>> {
        self.directory
            .get_object_routes_bounded(&self.observer, keys)
    }

    pub fn load_active_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        Ok(self
            .load_route(key)?
            .filter(|route| route.state == RouteState::Active))
    }

    pub fn load_active_routes_bounded(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        Ok(self
            .load_routes_bounded(keys)?
            .into_iter()
            .map(|route| route.filter(|route| route.state == RouteState::Active))
            .collect())
    }

    pub fn load_active_route_and_report<R: RouteHitReporter>(
        &self,
        key: &ObjectKey,
        reporter: &R,
    ) -> Result<Option<ObjectRoute>> {
        let route = self.load_active_route(key)?;
        if let Some(route) = route.as_ref() {
            reporter.report_route_hits(&[route]);
        }
        Ok(route)
    }

    pub fn report_route_hits<R: RouteHitReporter>(
        &self,
        routes: &[Option<ObjectRoute>],
        reporter: &R,
    ) {
        let hits = routes.iter().filter_map(Option::as_ref).collect::<Vec<_>>();
        reporter.report_route_hits(&hits);
    }

    pub fn publish_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: &ObjectRoute,
    ) -> Result<CasResult> {
        self.compare_and_swap_route(key, expected, Some(next))
    }

    pub fn publish_routes(&self, requests: &[RouteCasRequest]) -> Result<Vec<Result<CasResult>>> {
        self.directory
            .compare_and_swap_object_routes(&self.observer, requests)
    }

    pub fn repair_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: &ObjectRoute,
    ) -> Result<CasResult> {
        self.compare_and_swap_route(key, expected, Some(next))
    }

    pub fn prune_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: &ObjectRoute,
    ) -> Result<CasResult> {
        self.compare_and_swap_route(key, expected, Some(next))
    }

    pub fn delete_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
    ) -> Result<CasResult> {
        self.compare_and_swap_route(key, expected, None)
    }

    pub fn compare_and_swap_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.directory
            .compare_and_swap_object_route(&self.observer, key, expected, next)
    }

    pub fn list_routes_by_replica_owner(
        &self,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        self.directory
            .list_routes_by_replica_owner(&self.observer, owner)
    }

    pub fn list_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        self.directory.list_routes_in_scope(&self.observer, scope)
    }

    pub fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        self.directory.list_reuse_candidates(&self.observer, reuse)
    }

    pub fn load_authority_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        LocalRouteAuthority::new(namespace, authority.clone()).get_route(key)
    }

    pub fn replace_authority_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        route: Option<&ObjectRoute>,
    ) -> Result<()> {
        LocalRouteAuthority::new(namespace, authority.clone()).replace_route(key, route)
    }

    pub fn list_authority_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
    ) -> Result<Vec<ObjectRoute>> {
        LocalRouteAuthority::new(namespace, authority.clone()).list_routes()
    }

    pub fn next_route_version(
        &self,
        current: Option<&ObjectRoute>,
        key: &ObjectKey,
    ) -> RouteVersion {
        current
            .map(|route| route.version.next())
            .unwrap_or_else(|| {
                self.directory
                    .get_version_floor(&self.observer, key)
                    .map(|version| version.next())
                    .unwrap_or(RouteVersion(1))
            })
    }
}
