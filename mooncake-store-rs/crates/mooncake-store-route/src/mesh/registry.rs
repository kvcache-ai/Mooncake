use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientRuntimeId, ClientStableId, ColdBackingState, NamespaceScope, ObjectKey, ObjectRoute,
    Result, ReuseIdentity, RouteCasRequest, RouteVersion, StoreError,
};
use parking_lot::Mutex as ParkingMutex;

use crate::metrics::record_delete_marker_state;
use crate::shim::RouteAuthorityService;
use crate::table::LocalRouteTable;
use crate::{RouteAuthorityRecord, RouteOwnerPage, DEFAULT_ROUTE_OWNER_PAGE_SIZE};

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

pub(crate) fn authority_get_record(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Result<RouteAuthorityRecord> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.get_record(key))
}

pub(crate) fn authority_get_records(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<RouteAuthorityRecord>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    Ok(slot.table.get_records(keys))
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

pub(crate) fn authority_get_delete_marker(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Option<RouteVersion> {
    let mesh = route_mesh(namespace);
    mesh.authorities
        .get(&authority.0)
        .and_then(|slot| slot.table.delete_marker(key))
}

pub(crate) fn authority_get_delete_markers(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Vec<Option<RouteVersion>> {
    let mesh = route_mesh(namespace);
    mesh.authorities
        .get(&authority.0)
        .map(|slot| slot.table.delete_markers(keys))
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
    let page = slot
        .table
        .list_by_replica_owner_page(owner, None, DEFAULT_ROUTE_OWNER_PAGE_SIZE);
    if page.next_cursor.is_some() {
        return Err(StoreError::Unsupported(
            "complete owner-route listing exceeds the bounded compatibility page".to_string(),
        ));
    }
    Ok(page.routes)
}

pub(crate) fn authority_get_allocation_route_references(
    namespace: &str,
    authority: &ClientStableId,
    identities: &[AllocationRouteIdentity],
) -> Result<Vec<AllocationRouteReferenceState>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    slot.table.allocation_route_references(identities)
}

pub(crate) fn authority_get_allocation_route_reference_keys(
    namespace: &str,
    authority: &ClientStableId,
    identities: &[AllocationRouteIdentity],
) -> Result<Vec<AllocationRouteReference>> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    slot.table.allocation_route_reference_keys(identities)
}

pub(crate) fn authority_list_routes_by_replica_owner_page(
    namespace: &str,
    authority: &ClientStableId,
    owner: &ClientRuntimeId,
    cursor: Option<&str>,
    snapshot_epoch: Option<u64>,
    limit: usize,
) -> Result<RouteOwnerPage> {
    let mesh = route_mesh(namespace);
    let slot = mesh
        .authorities
        .get(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    slot.table
        .list_by_replica_owner_page_at_epoch(owner, cursor, limit, snapshot_epoch)
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

pub(crate) fn authority_publication_control(
    namespace: &str,
    authority: &ClientStableId,
    operations: &[mooncake_store_core::PublicationControl],
) -> Result<Vec<Result<mooncake_store_core::PublicationControlReply>>> {
    if operations.len() > 64 {
        return Err(StoreError::InvalidState(
            "publication control batch exceeds 64 items".to_string(),
        ));
    }
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .get_mut(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    let runtime = slot
        .runtime
        .clone()
        .ok_or_else(|| not_attached(authority))?;
    Ok(operations
        .iter()
        .map(|operation| slot.table.publication_control(&runtime, operation))
        .collect())
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
    let runtime = slot
        .runtime
        .clone()
        .ok_or_else(|| not_attached(authority))?;
    slot.table.admit_publications(&runtime, key, next, false)?;
    Ok(slot.table.compare_and_swap(key, expected, next))
}

pub(crate) fn authority_compare_and_swap_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<Vec<Result<CasResult>>> {
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .get_mut(&authority.0)
        .ok_or_else(|| not_attached(authority))?;
    if slot.ref_count == 0 {
        return Err(not_attached(authority));
    }
    let runtime = slot
        .runtime
        .clone()
        .ok_or_else(|| not_attached(authority))?;
    Ok(slot.table.compare_and_swap_many(&runtime, requests))
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
    let runtime = slot
        .runtime
        .clone()
        .ok_or_else(|| not_attached(authority))?;
    slot.table.admit_publications(&runtime, key, next, true)?;
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
    let runtime = slot
        .runtime
        .clone()
        .ok_or_else(|| not_attached(authority))?;
    for request in requests {
        slot.table
            .admit_publications(&runtime, &request.key, request.next.as_ref(), true)?;
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
        Some(readable) => {
            route.replicas.iter().any(|r| readable.contains(&r.owner))
                || route
                    .cold_backing
                    .as_ref()
                    .is_some_and(|cb| cb.state == ColdBackingState::Materialized)
        }
    }
}

#[derive(Default)]
struct AuthoritySlot {
    runtime: Option<ClientRuntimeId>,
    ref_count: usize,
    table: LocalRouteTable,
    service: Option<Arc<dyn RouteAuthorityService>>,
}

pub(crate) fn register_local_authority(
    namespace: &str,
    runtime: &ClientRuntimeId,
    delete_marker_budget_bytes: usize,
) {
    let authority = &runtime.stable_id;
    let mesh = route_mesh(namespace);
    let mut slot = mesh
        .authorities
        .entry(authority.0.clone())
        .or_insert_with(|| AuthoritySlot {
            runtime: Some(runtime.clone()),
            table: LocalRouteTable::with_delete_marker_budget(delete_marker_budget_bytes),
            ..AuthoritySlot::default()
        });
    if slot.runtime.as_ref() != Some(runtime) {
        slot.table = LocalRouteTable::with_delete_marker_budget(delete_marker_budget_bytes);
        slot.runtime = Some(runtime.clone());
    }
    slot.ref_count += 1;
    let snapshot = slot.table.delete_marker_snapshot();
    record_delete_marker_state(
        snapshot.entries as u64,
        snapshot.accounted_bytes as u64,
        snapshot.budget_bytes as u64,
    );
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
                record_delete_marker_state(0, 0, 0);
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
