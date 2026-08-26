//! Object route CRUD/CAS operation facade.

use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, ColdBackingRouteFilter, MetadataBackend,
    NamespaceScope, ObjectKey, ObjectRoute, Result, ReuseIdentity, RouteCasRequest, RouteDirectory,
    RouteState, RouteVersion,
};

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

    pub fn observer(&self) -> &ClientLease {
        &self.observer
    }

    pub fn directory(&self) -> &Arc<dyn RouteDirectory> {
        &self.directory
    }

    pub fn metadata(&self) -> &dyn MetadataBackend {
        self.directory.metadata()
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

    pub fn contains_active_route(&self, key: &ObjectKey) -> Result<bool> {
        self.directory.contains_object_route(&self.observer, key)
    }

    pub fn contains_active_routes_bounded(&self, keys: &[ObjectKey]) -> Result<Vec<bool>> {
        self.directory
            .contains_object_routes_bounded(&self.observer, keys)
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

    pub fn visit_routes_by_replica_owner(
        &self,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
    ) -> Result<()> {
        self.directory
            .visit_routes_by_replica_owner(&self.observer, owner, visitor)
    }

    pub fn list_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        self.directory.list_routes_in_scope(&self.observer, scope)
    }

    pub fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        self.directory.list_reuse_candidates(&self.observer, reuse)
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

    pub fn list_routes_by_cold_backing(
        &self,
        filter: &ColdBackingRouteFilter,
    ) -> Result<Vec<ObjectRoute>> {
        self.directory
            .list_routes_by_cold_backing(&self.observer, filter)
    }

    pub fn next_route_versions(
        &self,
        current: &[Option<ObjectRoute>],
        keys: &[ObjectKey],
    ) -> Vec<RouteVersion> {
        debug_assert_eq!(current.len(), keys.len());
        let mut versions = vec![None; current.len()];
        let mut missing_indices = Vec::new();
        let mut missing_keys = Vec::new();
        for (index, (route, key)) in current.iter().zip(keys.iter()).enumerate() {
            if let Some(route) = route.as_ref() {
                versions[index] = Some(route.version.next());
            } else {
                missing_indices.push(index);
                missing_keys.push(key.clone());
            }
        }
        let floors = self
            .directory
            .get_version_floors(&self.observer, &missing_keys);
        for (index, floor) in missing_indices.into_iter().zip(floors) {
            versions[index] = Some(
                floor
                    .map(|version| version.next())
                    .unwrap_or(RouteVersion(1)),
            );
        }
        versions
            .into_iter()
            .map(|version| version.unwrap_or(RouteVersion(1)))
            .collect()
    }
}
