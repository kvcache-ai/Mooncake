use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use mooncake_store_core::{
    CasResult, ClientRuntimeId, ClientStableId, NamespaceScope, ObjectKey, ObjectRoute, Result,
    ReuseIdentity, RouteCasRequest, RouteVersion, StoreError,
};
use parking_lot::Mutex;

use crate::shim::RouteAuthorityService;
use crate::table::LocalRouteTable;

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
    Ok(guard.local_table(authority)?.get(key))
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
    Ok(guard.local_table(authority)?.get_many(keys))
}

pub(crate) fn authority_get_version_floor(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Option<RouteVersion> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    guard
        .local_table(authority)
        .ok()
        .and_then(|table| table.version_floor(key))
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
    Ok(guard.local_table(authority)?.list_by_replica_owner(owner))
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
    Ok(guard.local_table(authority)?.list_routes())
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
    Ok(guard.local_table(authority)?.list_in_scope(scope))
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
    Ok(guard.local_table(authority)?.list_reuse_candidates(reuse))
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
    Ok(guard
        .local_table_mut(authority)?
        .compare_and_swap(key, expected, next))
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
    Ok(guard
        .local_table_mut(authority)?
        .compare_and_swap_many(requests))
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
    guard.local_table_mut(authority)?.replace(key, next);
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
    guard.local_table_mut(authority)?.replace_many(requests);
    Ok(())
}

#[derive(Default)]
struct ClusterRouteMesh {
    attached_locals: BTreeMap<String, usize>,
    tables_by_authority: BTreeMap<String, LocalRouteTable>,
    authority_services: BTreeMap<String, Arc<dyn RouteAuthorityService>>,
}

impl ClusterRouteMesh {
    fn register_local(&mut self, stable_id: &ClientStableId) {
        *self.attached_locals.entry(stable_id.0.clone()).or_default() += 1;
        self.tables_by_authority
            .entry(stable_id.0.clone())
            .or_default();
    }

    fn bind_local_service(
        &mut self,
        stable_id: &ClientStableId,
        service: Arc<dyn RouteAuthorityService>,
    ) {
        self.tables_by_authority
            .entry(stable_id.0.clone())
            .or_default();
        self.authority_services.insert(stable_id.0.clone(), service);
    }

    fn unregister_local(&mut self, stable_id: &ClientStableId) {
        let key = stable_id.0.clone();
        match self.attached_locals.get_mut(&key) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.attached_locals.remove(&key);
                self.tables_by_authority.remove(&key);
                self.authority_services.remove(&key);
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

    fn local_table(&self, authority: &ClientStableId) -> Result<&LocalRouteTable> {
        self.tables_by_authority.get(&authority.0).ok_or_else(|| {
            StoreError::NotFound(format!("route authority {} has no local table", authority))
        })
    }

    fn local_table_mut(&mut self, authority: &ClientStableId) -> Result<&mut LocalRouteTable> {
        self.tables_by_authority
            .get_mut(&authority.0)
            .ok_or_else(|| {
                StoreError::NotFound(format!("route authority {} has no local table", authority))
            })
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
