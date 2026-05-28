use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use mooncake_store_core::{
    route_reuse_identity, CasResult, ClientRuntimeId, ClientStableId, NamespaceScope, ObjectKey,
    ObjectRoute, Result, ReuseIdentity, RouteCasRequest, RouteVersion, StoreError,
};
use parking_lot::Mutex;

use crate::metrics::record_cas_outcome;
use crate::table::RouteAuthorityService;

pub(crate) fn bind_local_authority_service(
    namespace: &str,
    authority: &ClientStableId,
    service: Arc<dyn RouteAuthorityService>,
) {
    route_mesh(namespace)
        .lock()
        .bind_local_service(authority, service);
}

pub(crate) fn authority_get(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Result<Option<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    Ok(guard
        .routes_by_authority
        .get(&authority.0)
        .and_then(|routes| routes.get(&key.0))
        .cloned())
}

pub(crate) fn authority_get_many(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<Option<ObjectRoute>>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let routes = guard.routes_by_authority.get(&authority.0);
    Ok(keys
        .iter()
        .map(|key| routes.and_then(|routes| routes.get(&key.0)).cloned())
        .collect())
}

pub(crate) fn authority_get_version_floor(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Option<RouteVersion> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    guard
        .version_floors
        .get(&authority.0)
        .and_then(|floors| floors.get(&key.0))
        .copied()
}

pub(crate) fn authority_list_routes_by_replica_owner(
    namespace: &str,
    authority: &ClientStableId,
    owner: &ClientRuntimeId,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    Ok(guard
        .routes_by_authority
        .get(&authority.0)
        .into_iter()
        .flat_map(|routes| routes.values())
        .filter(|route| route.replicas.iter().any(|replica| replica.owner == *owner))
        .cloned()
        .collect())
}

pub(crate) fn authority_list_routes(
    namespace: &str,
    authority: &ClientStableId,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    Ok(guard
        .routes_by_authority
        .get(&authority.0)
        .into_iter()
        .flat_map(|routes| routes.values())
        .cloned()
        .collect())
}

pub(crate) fn authority_list_routes_in_scope(
    namespace: &str,
    authority: &ClientStableId,
    scope: &NamespaceScope,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let routes = guard.routes_by_authority.get(&authority.0);
    Ok(guard
        .scope_index
        .get(&authority.0)
        .and_then(|index| index.get(scope))
        .into_iter()
        .flat_map(|keys| keys.iter())
        .filter_map(|key| routes.and_then(|routes| routes.get(key)).cloned())
        .collect())
}

pub(crate) fn authority_list_reuse_candidates(
    namespace: &str,
    authority: &ClientStableId,
    reuse: &ReuseIdentity,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let routes = guard.routes_by_authority.get(&authority.0);
    Ok(guard
        .reuse_index
        .get(&authority.0)
        .and_then(|index| index.get(reuse))
        .into_iter()
        .flat_map(|keys| keys.iter())
        .filter_map(|key| routes.and_then(|routes| routes.get(key)).cloned())
        .collect())
}

/// Returns true if a CAS(None→Some(route)) should be rejected because a version
/// floor exists that is >= the proposed route's version (anti-resurrection guard).
fn version_floor_blocks_insert(
    mesh: &ClusterRouteMesh,
    authority: &ClientStableId,
    key: &ObjectKey,
    expected: Option<RouteVersion>,
    next: Option<&ObjectRoute>,
) -> bool {
    if expected.is_some() {
        return false;
    }
    let Some(route) = next else {
        return false;
    };
    mesh.version_floors
        .get(&authority.0)
        .and_then(|floors| floors.get(&key.0))
        .is_some_and(|floor| route.version <= *floor)
}

pub(crate) fn authority_compare_and_swap(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    expected: Option<RouteVersion>,
    next: Option<&ObjectRoute>,
) -> Result<CasResult> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let current = guard
        .routes_by_authority
        .entry(authority.0.clone())
        .or_default()
        .get(&key.0)
        .cloned();
    let matches = match (expected, current.as_ref()) {
        (None, None) => true,
        (Some(version), Some(route)) => route.version == version,
        _ => false,
    };
    if !matches || version_floor_blocks_insert(&guard, authority, key, expected, next) {
        let result = CasResult {
            applied: false,
            current,
        };
        record_cas_outcome(&result, next, key);
        return Ok(result);
    }
    apply_route_update(&mut guard, authority, key, next);
    let result = CasResult {
        applied: true,
        current: next.cloned(),
    };
    record_cas_outcome(&result, next, key);
    Ok(result)
}

pub(crate) fn authority_compare_and_swap_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<Vec<CasResult>> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let current = guard
            .routes_by_authority
            .entry(authority.0.clone())
            .or_default()
            .get(&request.key.0)
            .cloned();
        let matches = match (request.expected, current.as_ref()) {
            (None, None) => true,
            (Some(version), Some(route)) => route.version == version,
            _ => false,
        };
        if !matches
            || version_floor_blocks_insert(
                &guard,
                authority,
                &request.key,
                request.expected,
                request.next.as_ref(),
            )
        {
            let result = CasResult {
                applied: false,
                current,
            };
            record_cas_outcome(&result, request.next.as_ref(), &request.key);
            results.push(result);
            continue;
        }
        apply_route_update(&mut guard, authority, &request.key, request.next.as_ref());
        let result = CasResult {
            applied: true,
            current: request.next.clone(),
        };
        record_cas_outcome(&result, request.next.as_ref(), &request.key);
        results.push(result);
    }
    Ok(results)
}

pub(crate) fn authority_replace(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    next: Option<&ObjectRoute>,
) -> Result<()> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    apply_route_update(&mut guard, authority, key, next);
    Ok(())
}

pub(crate) fn authority_replace_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<()> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    for request in requests {
        apply_route_update(&mut guard, authority, &request.key, request.next.as_ref());
    }
    Ok(())
}

fn apply_route_update(
    mesh: &mut ClusterRouteMesh,
    authority: &ClientStableId,
    key: &ObjectKey,
    next: Option<&ObjectRoute>,
) {
    let old = mesh
        .routes_by_authority
        .get(&authority.0)
        .and_then(|routes| routes.get(&key.0))
        .cloned();
    if let Some(route) = old.as_ref() {
        mesh.remove_route_indexes(authority, key, route);
    }
    {
        let routes = mesh
            .routes_by_authority
            .entry(authority.0.clone())
            .or_default();
        match next {
            Some(route) => {
                routes.insert(key.0.clone(), route.clone());
                if let Some(floors) = mesh.version_floors.get_mut(&authority.0) {
                    floors.remove(&key.0);
                }
            }
            None => {
                routes.remove(&key.0);
                if let Some(old) = old.as_ref() {
                    mesh.version_floors
                        .entry(authority.0.clone())
                        .or_default()
                        .insert(key.0.clone(), old.version);
                }
            }
        }
    }
    if let Some(route) = next {
        mesh.insert_route_indexes(authority, key, route);
    }
}

#[derive(Default)]
struct ClusterRouteMesh {
    attached_locals: BTreeMap<String, usize>,
    routes_by_authority: BTreeMap<String, BTreeMap<String, ObjectRoute>>,
    scope_index: BTreeMap<String, BTreeMap<NamespaceScope, BTreeSet<String>>>,
    reuse_index: BTreeMap<String, BTreeMap<ReuseIdentity, BTreeSet<String>>>,
    authority_services: BTreeMap<String, Arc<dyn RouteAuthorityService>>,
    version_floors: BTreeMap<String, BTreeMap<String, RouteVersion>>,
}

impl ClusterRouteMesh {
    fn register_local(&mut self, stable_id: &ClientStableId) {
        *self.attached_locals.entry(stable_id.0.clone()).or_default() += 1;
        self.routes_by_authority
            .entry(stable_id.0.clone())
            .or_default();
        self.scope_index.entry(stable_id.0.clone()).or_default();
        self.reuse_index.entry(stable_id.0.clone()).or_default();
    }

    fn bind_local_service(
        &mut self,
        stable_id: &ClientStableId,
        service: Arc<dyn RouteAuthorityService>,
    ) {
        self.routes_by_authority
            .entry(stable_id.0.clone())
            .or_default();
        self.scope_index.entry(stable_id.0.clone()).or_default();
        self.reuse_index.entry(stable_id.0.clone()).or_default();
        self.authority_services.insert(stable_id.0.clone(), service);
    }

    fn unregister_local(&mut self, stable_id: &ClientStableId) {
        let key = stable_id.0.clone();
        match self.attached_locals.get_mut(&key) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.attached_locals.remove(&key);
                self.routes_by_authority.remove(&key);
                self.scope_index.remove(&key);
                self.reuse_index.remove(&key);
                self.authority_services.remove(&key);
                self.version_floors.remove(&key);
            }
            None => {}
        }
    }

    fn is_local(&self, stable_id: &ClientStableId) -> bool {
        self.attached_locals
            .get(&stable_id.0)
            .is_some_and(|count| *count > 0)
    }

    fn authority_service(
        &self,
        stable_id: &ClientStableId,
    ) -> Option<Arc<dyn RouteAuthorityService>> {
        self.authority_services.get(&stable_id.0).cloned()
    }

    fn insert_route_indexes(
        &mut self,
        authority: &ClientStableId,
        key: &ObjectKey,
        route: &ObjectRoute,
    ) {
        if let Some(scope) = route.namespace.clone() {
            self.scope_index
                .entry(authority.0.clone())
                .or_default()
                .entry(scope)
                .or_default()
                .insert(key.0.clone());
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            self.reuse_index
                .entry(authority.0.clone())
                .or_default()
                .entry(reuse)
                .or_default()
                .insert(key.0.clone());
        }
    }

    fn remove_route_indexes(
        &mut self,
        authority: &ClientStableId,
        key: &ObjectKey,
        route: &ObjectRoute,
    ) {
        if let Some(scope) = route.namespace.as_ref() {
            remove_index_key(
                self.scope_index.get_mut(&authority.0),
                scope,
                key.0.as_str(),
            );
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            remove_index_key(
                self.reuse_index.get_mut(&authority.0),
                &reuse,
                key.0.as_str(),
            );
        }
    }
}

fn remove_index_key<K: Ord>(
    index: Option<&mut BTreeMap<K, BTreeSet<String>>>,
    identity: &K,
    key: &str,
) {
    let Some(index) = index else {
        return;
    };
    let Some(keys) = index.get_mut(identity) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        index.remove(identity);
    }
}

pub(crate) fn register_local_authority(namespace: &str, authority: &ClientStableId) {
    route_mesh(namespace).lock().register_local(authority);
}

pub(crate) fn unregister_local_authority(namespace: &str, authority: &ClientStableId) {
    route_mesh(namespace).lock().unregister_local(authority);
}

pub(crate) fn authority_is_local(namespace: &str, authority: &ClientStableId) -> bool {
    route_mesh(namespace).lock().is_local(authority)
}

pub(crate) fn local_authority_service(
    namespace: &str,
    authority: &ClientStableId,
) -> Option<Arc<dyn RouteAuthorityService>> {
    route_mesh(namespace).lock().authority_service(authority)
}

fn route_mesh(namespace: &str) -> Arc<Mutex<ClusterRouteMesh>> {
    static ROUTE_MESHES: OnceLock<Mutex<BTreeMap<String, Arc<Mutex<ClusterRouteMesh>>>>> =
        OnceLock::new();
    let meshes = ROUTE_MESHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = meshes.lock();
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(ClusterRouteMesh::default())))
        .clone()
}
