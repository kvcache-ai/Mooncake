use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use mooncake_store_core::{
    CasResult, ClientRuntimeId, ClientStableId, NamespaceScope, ObjectKey, ObjectRoute, Result,
    ReuseIdentity, RouteCasRequest, RouteVersion, StoreError,
};
use parking_lot::Mutex as ParkingMutex;

use crate::shim::RouteAuthorityService;
use crate::table::LocalRouteTable;

pub(crate) fn bind_local_authority_service(
    namespace: &str,
    authority: &ClientStableId,
    service: Arc<dyn RouteAuthorityService>,
) {
    let mesh = route_mesh(namespace);
    let mut slot = mesh.authorities.get_mut(&authority.0);
    if let Some(ref mut entry) = slot {
        entry.service = Some(service);
    }
}

pub(crate) fn authority_get(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Result<Option<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.get(key))
}

pub(crate) fn authority_get_many(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<Option<ObjectRoute>>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.get_many(keys))
}

pub(crate) fn authority_contains_many(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<bool>> {
    let mesh = route_mesh(namespace);
    let filter = mesh.readable_filter.lock().clone();
    match filter {
        None => {
            let slot = mesh
                .authorities
                .get(&authority.0)
                .ok_or_else(|| not_attached(authority))?;
            if slot.ref_count == 0 {
                return Err(not_attached(authority));
            }
            Ok(slot.table.contains_many(keys))
        }
        Some(readable) => {
            let mut slot = mesh
                .authorities
                .get_mut(&authority.0)
                .ok_or_else(|| not_attached(authority))?;
            if slot.ref_count == 0 {
                return Err(not_attached(authority));
            }
            let results = slot.table.contains_readable_many(keys, &readable);
            if results.iter().any(|r| !r) {
                let evict_keys: Vec<_> = keys
                    .iter()
                    .zip(results.iter())
                    .filter(|(_, &r)| !r)
                    .map(|(k, _)| k.clone())
                    .collect();
                slot.table.evict_unreadable_routes(&evict_keys, &readable);
            }
            Ok(results)
        }
    }
}

pub(crate) fn authority_get_version_floor(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Option<RouteVersion> {
    let mesh = route_mesh(namespace);
    mesh.authorities
        .get(&authority.0)
        .and_then(|slot| slot.table.version_floor(key))
}

pub(crate) fn authority_get_version_floors(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Vec<Option<RouteVersion>> {
    let mesh = route_mesh(namespace);
    mesh.authorities
        .get(&authority.0)
        .map(|slot| slot.table.version_floors(keys))
        .unwrap_or_else(|| vec![None; keys.len()])
}

pub(crate) fn authority_list_routes_by_replica_owner(
    namespace: &str,
    authority: &ClientStableId,
    owner: &ClientRuntimeId,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.list_by_replica_owner(owner))
}

pub(crate) fn authority_list_routes(
    namespace: &str,
    authority: &ClientStableId,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.list_routes())
}

pub(crate) fn authority_list_routes_in_scope(
    namespace: &str,
    authority: &ClientStableId,
    scope: &NamespaceScope,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.list_in_scope(scope))
}

pub(crate) fn authority_list_reuse_candidates(
    namespace: &str,
    authority: &ClientStableId,
    reuse: &ReuseIdentity,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.list_reuse_candidates(reuse))
}

pub(crate) fn authority_compare_and_swap(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    expected: Option<RouteVersion>,
    next: Option<&ObjectRoute>,
) -> Result<CasResult> {
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .get_mut(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.compare_and_swap(key, expected, next))
}

pub(crate) fn authority_compare_and_swap_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<Vec<CasResult>> {
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .get_mut(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.compare_and_swap_many(requests))
}

pub(crate) fn authority_replace(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    next: Option<&ObjectRoute>,
) -> Result<()> {
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .get_mut(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    slot.table.replace(key, next);
    Ok(())
}

pub(crate) fn authority_replace_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<()> {
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .get_mut(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    slot.table.replace_many(requests);
    Ok(())
}

struct ClusterRouteMesh {
    authorities: DashMap<String, AuthoritySlot>,
    readable_filter: ParkingMutex<Option<Arc<BTreeSet<ClientRuntimeId>>>>,
}

impl ClusterRouteMesh {
    fn new() -> Self {
        Self {
            authorities: DashMap::new(),
            readable_filter: ParkingMutex::new(None),
        }
    }
}

pub fn update_readable_filter(namespace: &str, filter: Option<BTreeSet<ClientRuntimeId>>) {
    let mesh = route_mesh(namespace);
    *mesh.readable_filter.lock() = filter.map(Arc::new);
}

pub fn is_readable_filter_active(namespace: &str) -> bool {
    let mesh = route_mesh(namespace);
    let active = mesh.readable_filter.lock().is_some();
    active
}

pub fn route_has_readable_replicas(namespace: &str, route: &ObjectRoute) -> bool {
    let mesh = route_mesh(namespace);
    let filter = mesh.readable_filter.lock().clone();
    match filter {
        None => true,
        Some(readable) => route.replicas.iter().any(|r| readable.contains(&r.owner)),
    }
}

#[derive(Default)]
struct AuthoritySlot {
    ref_count: usize,
    table: LocalRouteTable,
    service: Option<Arc<dyn RouteAuthorityService>>,
}

pub(crate) fn register_local_authority(namespace: &str, authority: &ClientStableId) {
    let mesh = route_mesh(namespace);
    let mut slot = mesh.authorities.entry(authority.0.clone()).or_default();
    slot.ref_count += 1;
}

pub(crate) fn unregister_local_authority(namespace: &str, authority: &ClientStableId) {
    let mesh = route_mesh(namespace);
    match mesh.authorities.entry(authority.0.clone()) {
        dashmap::mapref::entry::Entry::Occupied(mut occ) => {
            let slot = occ.get_mut();
            if slot.ref_count > 1 {
                slot.ref_count -= 1;
            } else {
                occ.remove();
            }
        }
        dashmap::mapref::entry::Entry::Vacant(_) => {}
    };
}

pub(crate) fn authority_is_local(namespace: &str, authority: &ClientStableId) -> bool {
    let mesh = route_mesh(namespace);
    mesh.authorities
        .get(&authority.0)
        .is_some_and(|slot| slot.ref_count > 0)
}

pub(crate) fn local_authority_service(
    namespace: &str,
    authority: &ClientStableId,
) -> Option<Arc<dyn RouteAuthorityService>> {
    let mesh = route_mesh(namespace);
    mesh.authorities
        .get(&authority.0)
        .and_then(|slot| slot.service.clone())
}

fn route_mesh(namespace: &str) -> Arc<ClusterRouteMesh> {
    static ROUTE_MESHES: OnceLock<DashMap<String, Arc<ClusterRouteMesh>>> = OnceLock::new();
    let meshes = ROUTE_MESHES.get_or_init(DashMap::new);
    meshes
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(ClusterRouteMesh::new()))
        .value()
        .clone()
}

fn not_attached(authority: &ClientStableId) -> StoreError {
    StoreError::NotFound(format!(
        "route authority {} is not attached locally",
        authority
    ))
}
