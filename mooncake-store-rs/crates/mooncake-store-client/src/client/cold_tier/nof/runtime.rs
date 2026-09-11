//! StoreClient runtime binding for configured NoF targets.
//!
//! This runtime is for provider-addressed NoF backings such as KVCS. Targets are selected for a
//! write and discovered again through the provider for reads and deletes; placement is therefore
//! request-local and is never published as route metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{atomic::Ordering, Arc};

use mooncake_store_core::{
    ClientRuntimeId, ColdBackingReplica, ColdBackingRoute, ColdBackingState, MetadataBackend,
    NamespaceScope, NofBackingRoute, ObjectRoute, Result, StoreError,
};

use crate::client::cold_tier::layout::{derive_physical_key, PhysicalKeyInput};
use crate::client::cold_tier::owner::{NofHeartbeatMonitor, NofOwnerState, NofRuntimeTarget};
use crate::client::cold_tier::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaWriteCandidate, DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};
use crate::client::{
    PersistentStorageBackend, PersistentStorageBackendHealth, SharedLiveClientCache,
};
use crate::control_plane::{ControlPlaneClient, ManagedNofRouteAction};

use super::backing::validate_payload;
use super::managed::NofManagedAllocationRequest;
use super::managed_backend::NofManagedStorageBackend;
use super::object::NofObjectState;
use super::physical_backend::NofPhysicalStorageBackend;
use super::{ensure_batch_len, NofBackend};

/// One NoF target registered on `StoreClientBuilder`.
#[derive(Clone)]
pub struct NofTargetConfig {
    target_id: String,
    backend: NofBackend,
    data_plane: ProviderDataPlane,
}

impl NofTargetConfig {
    pub fn new(target_id: impl Into<String>, backend: NofBackend) -> Result<Self> {
        let target_id = target_id.into();
        if target_id.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF target ID must not be empty".to_string(),
            ));
        }
        let data_plane = provider_data_plane(&backend)?;
        Ok(Self {
            target_id,
            backend,
            data_plane,
        })
    }
}

pub(in crate::client) fn target_set_fingerprint(
    configs: &[NofTargetConfig],
) -> Result<Option<String>> {
    if configs.is_empty() {
        return Ok(None);
    }
    let mut targets = configs
        .iter()
        .map(|config| (config.target_id.as_str(), config.data_plane))
        .collect::<Vec<_>>();
    targets.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut encoded_fields = Vec::<Vec<u8>>::with_capacity(targets.len() * 2);
    for (target_id, data_plane) in targets {
        encoded_fields.push(match data_plane {
            ProviderDataPlane::Object => b"object".to_vec(),
            ProviderDataPlane::Physical => b"physical".to_vec(),
            ProviderDataPlane::Managed => b"managed".to_vec(),
        });
        encoded_fields.push(target_id.as_bytes().to_vec());
    }
    let fields = encoded_fields.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let fingerprint = derive_physical_key(PhysicalKeyInput {
        domain: b"nof-target-set",
        fields: &fields,
        chunk_index: None,
    })?;
    Ok(Some(fingerprint.to_hex()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderDataPlane {
    Object,
    Physical,
    Managed,
}

pub(in crate::client) struct NofTargetManager {
    data_plane: Option<ProviderDataPlane>,
    replica_count: usize,
    state: Arc<NofOwnerState>,
    managed_targets: BTreeMap<String, NofBackend>,
    route_ops: Option<mooncake_store_route::RouteOperations>,
    control_client: Arc<ControlPlaneClient>,
    _heartbeat: NofHeartbeatMonitor,
}

pub(in crate::client) struct ManagedPendingBacking {
    pub(in crate::client) backing: ColdBackingRoute,
    pub(in crate::client) route: ObjectRoute,
}

impl NofTargetManager {
    pub(in crate::client) fn new(
        configs: Vec<NofTargetConfig>,
        replica_count: usize,
        local_runtime: ClientRuntimeId,
        metadata: Arc<dyn MetadataBackend>,
        live_clients: SharedLiveClientCache,
        target_set_fingerprint: String,
    ) -> Result<Self> {
        let mut data_plane = None;
        let mut targets = BTreeMap::new();
        let mut managed_targets = BTreeMap::new();
        for config in configs {
            let next_data_plane = config.data_plane;
            if data_plane.is_some_and(|current| current != next_data_plane) {
                return Err(StoreError::InvalidState(
                    "one StoreClient cannot mix NoF logical-object and physical-KV targets"
                        .to_string(),
                ));
            }
            data_plane = Some(next_data_plane);
            let target_backend = config.backend.clone();
            let backend: Arc<dyn PersistentStorageBackend> = match next_data_plane {
                ProviderDataPlane::Object => Arc::new(NofObjectAdapter::new(config.backend)),
                ProviderDataPlane::Physical => {
                    Arc::new(NofPhysicalStorageBackend::new(config.backend))
                }
                ProviderDataPlane::Managed => {
                    managed_targets.insert(config.target_id.clone(), target_backend);
                    Arc::new(NofManagedStorageBackend::new(config.backend))
                }
            };
            if targets
                .insert(
                    config.target_id.clone(),
                    Arc::new(NofRuntimeTarget::new(backend)),
                )
                .is_some()
            {
                return Err(StoreError::InvalidState(format!(
                    "duplicate NoF target ID {}",
                    config.target_id
                )));
            }
        }
        let state = Arc::new(NofOwnerState::new(
            local_runtime,
            metadata,
            live_clients,
            target_set_fingerprint,
            targets,
        ));
        // Establish the first owner-scoped health snapshot before this target can be selected.
        // Later requests only read the cached snapshot; the monitor refreshes it in the background.
        state.heartbeat_owned_targets();
        if data_plane == Some(ProviderDataPlane::Managed) {
            recover_managed_owned_targets(&state, &managed_targets);
        }
        let previous_owned = Arc::new(parking_lot::Mutex::new(
            state
                .locally_owned_target_ids()
                .into_iter()
                .collect::<BTreeSet<_>>(),
        ));
        let managed_state = state.clone();
        let managed_targets_for_hook = managed_targets.clone();
        let managed_hook = if data_plane == Some(ProviderDataPlane::Managed) {
            Some(Arc::new(move || {
                let owned = managed_state
                    .locally_owned_target_ids()
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                let changed = {
                    let mut previous = previous_owned.lock();
                    if *previous == owned {
                        false
                    } else {
                        *previous = owned;
                        true
                    }
                };
                if changed {
                    recover_managed_owned_targets(&managed_state, &managed_targets_for_hook);
                }
            }) as Arc<dyn Fn() + Send + Sync>)
        } else {
            None
        };
        let heartbeat = if state.targets.is_empty() {
            NofHeartbeatMonitor::disabled()
        } else {
            NofHeartbeatMonitor::start_with_hook(state.clone(), managed_hook)?
        };
        Ok(Self {
            data_plane,
            replica_count,
            state,
            managed_targets,
            route_ops: None,
            control_client: Arc::new(ControlPlaneClient::new()?),
            _heartbeat: heartbeat,
        })
    }

    pub(in crate::client) fn with_route_control(
        mut self,
        route_ops: mooncake_store_route::RouteOperations,
        control_client: Arc<ControlPlaneClient>,
    ) -> Self {
        self.route_ops = Some(route_ops);
        self.control_client = control_client;
        self
    }

    pub(in crate::client) fn is_empty(&self) -> bool {
        self.state.targets.is_empty()
    }

    pub(in crate::client) fn contains(&self, target_id: &str) -> bool {
        self.state.targets.contains_key(target_id)
    }

    pub(in crate::client) fn available_for_io(&self, target_id: &str) -> bool {
        self.state.health_snapshot(target_id).is_ok()
    }

    pub(in crate::client) fn is_managed(&self) -> bool {
        self.data_plane == Some(ProviderDataPlane::Managed)
    }

    pub(in crate::client) fn target_ids(&self) -> impl Iterator<Item = &str> {
        self.state.targets.keys().map(String::as_str)
    }

    pub(in crate::client) fn accept_nof_owner_snapshot(
        &self,
        target_id: String,
        from: ClientRuntimeId,
        to: ClientRuntimeId,
        routes: Vec<ObjectRoute>,
    ) -> Result<usize> {
        self.state.accept_handoff(target_id, from, to, routes)
    }

    pub(in crate::client) fn manage_managed_route(
        &self,
        route_ops: &mooncake_store_route::RouteOperations,
        target_id: &str,
        action: ManagedNofRouteAction,
        route: ObjectRoute,
        length: u64,
        checksum: Option<u64>,
    ) -> Result<ObjectRoute> {
        if !self.is_managed() {
            return Err(StoreError::Unsupported(
                "managed NoF route control requires a managed target".to_string(),
            ));
        }
        if !self.contains(target_id) {
            return Err(StoreError::NotFound(format!(
                "managed NoF target {target_id} is not registered"
            )));
        }
        if self.state.owner_for(target_id).as_ref() != Some(&self.state.local_runtime) {
            return Err(StoreError::Conflict(format!(
                "managed NoF target {target_id} is not owned by {}",
                self.state.local_runtime
            )));
        }
        match action {
            ManagedNofRouteAction::Prepare => {
                let allocator = self
                    .managed_targets
                    .get(target_id)
                    .and_then(|backend| backend.backing.managed_allocator())
                    .ok_or_else(|| {
                        StoreError::Unsupported(
                            "managed NoF target is missing allocator capability".to_string(),
                        )
                    })?;
                if let Some(backing) = route.nof_backing.as_ref() {
                    if backing.target_id == target_id
                        || backing
                            .replicas
                            .iter()
                            .any(|replica| replica.target_id == target_id)
                    {
                        return Ok(route);
                    }
                    if backing.state != ColdBackingState::PendingOffload {
                        return Err(StoreError::Conflict(
                            "cannot add a managed NoF replica after materialization".to_string(),
                        ));
                    }
                }
                let mut reservations = allocator.reserve_batch(&[NofManagedAllocationRequest {
                    key: route.key.0.clone(),
                    length,
                    checksum,
                }]);
                ensure_batch_len("managed reserve", 1, reservations.len())?;
                let locator = reservations
                    .pop()
                    .expect("managed reserve result count checked")?;
                let object_locator = locator.to_hex();
                let mut next = route.clone();
                next.version = next.version.next();
                match next.nof_backing.as_mut() {
                    None => {
                        next.nof_backing = Some(NofBackingRoute {
                            owner: self.state.local_runtime.clone(),
                            target_id: target_id.to_string(),
                            object_locator,
                            length,
                            checksum,
                            state: ColdBackingState::PendingOffload,
                            replicas: Vec::new(),
                        });
                    }
                    Some(backing) => {
                        backing
                            .replicas
                            .push(mooncake_store_core::NofBackingReplica {
                                owner: self.state.local_runtime.clone(),
                                target_id: target_id.to_string(),
                                object_locator,
                            })
                    }
                }
                let cas = route_ops.compare_and_swap_route(
                    &route.key,
                    Some(route.version),
                    Some(&next),
                )?;
                if cas.applied {
                    Ok(next)
                } else {
                    let _ = allocator.release_batch(&[super::managed::NofManagedReadRequest {
                        locator,
                        length,
                        checksum,
                    }]);
                    Err(StoreError::Conflict(format!(
                        "managed NoF route {} changed while preparing target {target_id}",
                        route.key.0
                    )))
                }
            }
            ManagedNofRouteAction::Publish => {
                let backing = route.nof_backing.as_ref().ok_or_else(|| {
                    StoreError::NotFound("managed NoF route is missing".to_string())
                })?;
                if backing.target_id != target_id {
                    return Err(StoreError::Conflict(
                        "only the primary managed NoF target owner may publish".to_string(),
                    ));
                }
                if backing.state == ColdBackingState::Materialized {
                    return Ok(route);
                }
                let mut next = route.clone();
                next.version = next.version.next();
                next.nof_backing
                    .as_mut()
                    .expect("managed backing was checked")
                    .state = ColdBackingState::Materialized;
                let cas = route_ops.compare_and_swap_route(
                    &route.key,
                    Some(route.version),
                    Some(&next),
                )?;
                if cas.applied {
                    Ok(next)
                } else {
                    Err(StoreError::Conflict(format!(
                        "managed NoF route {} changed while publishing",
                        route.key.0
                    )))
                }
            }
            ManagedNofRouteAction::Release => {
                let backing = route.nof_backing.as_ref().ok_or_else(|| {
                    StoreError::NotFound("managed NoF route is missing".to_string())
                })?;
                let (locator, target_length, target_checksum, is_primary) =
                    if backing.target_id == target_id {
                        (
                            backing.object_locator.clone(),
                            backing.length,
                            backing.checksum,
                            true,
                        )
                    } else if let Some(replica) = backing
                        .replicas
                        .iter()
                        .find(|replica| replica.target_id == target_id)
                    {
                        (
                            replica.object_locator.clone(),
                            backing.length,
                            backing.checksum,
                            false,
                        )
                    } else {
                        return Ok(route);
                    };
                let mut next = route.clone();
                next.version = next.version.next();
                if is_primary {
                    if let Some(promoted) = next
                        .nof_backing
                        .as_mut()
                        .expect("managed backing was checked")
                        .replicas
                        .first()
                        .cloned()
                    {
                        let next_backing = next.nof_backing.as_mut().unwrap();
                        next_backing.owner = promoted.owner;
                        next_backing.target_id = promoted.target_id;
                        next_backing.object_locator = promoted.object_locator;
                        next_backing.replicas.remove(0);
                    } else {
                        next.nof_backing = None;
                    }
                } else {
                    next.nof_backing
                        .as_mut()
                        .expect("managed backing was checked")
                        .replicas
                        .retain(|replica| replica.target_id != target_id);
                }
                let cas = route_ops.compare_and_swap_route(
                    &route.key,
                    Some(route.version),
                    Some(&next),
                )?;
                if !cas.applied {
                    return Err(StoreError::Conflict(format!(
                        "managed NoF route {} changed while releasing target {target_id}",
                        route.key.0
                    )));
                }
                let allocator = self
                    .managed_targets
                    .get(target_id)
                    .and_then(|backend| backend.backing.managed_allocator())
                    .expect("managed allocator capability was checked during construction");
                let results = allocator.release_batch(&[super::managed::NofManagedReadRequest {
                    locator: super::managed::NofManagedLocator::from_hex(&locator)?,
                    length: target_length,
                    checksum: target_checksum,
                }]);
                ensure_batch_len("managed release", 1, results.len())?;
                results.into_iter().next().expect("length checked above")?;
                Ok(next)
            }
        }
    }

    #[cfg(test)]
    pub(in crate::client) fn heartbeat_owner_for(
        &self,
        target_id: &str,
    ) -> Option<ClientRuntimeId> {
        self.state.owner_for(target_id)
    }

    pub(in crate::client) fn backend_for(
        &self,
        target_id: &str,
    ) -> Result<Arc<dyn PersistentStorageBackend>> {
        self.state
            .targets
            .get(target_id)
            .map(|target| target.backend.clone())
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "NoF target backend {target_id} is not registered"
                ))
            })
    }

    fn select_write_targets(
        &self,
        wanted: usize,
        require_full_set: bool,
    ) -> Option<(Vec<ReplicaWriteCandidate<'_>>, Vec<usize>)> {
        let candidates = self
            .state
            .targets
            .iter()
            .filter_map(|(target_id, target)| {
                let health = match self.state.health_snapshot(target_id) {
                    Ok(health) => health,
                    Err(error) => {
                        tracing::warn!(target_id, error = %error, "NoF target excluded from offload");
                        return None;
                    }
                };
                Some(ReplicaWriteCandidate {
                    target_id,
                    score: match (health.capacity_bytes, health.available_bytes) {
                        (Some(capacity), Some(available)) if capacity > 0 => {
                            available as f64 / capacity as f64
                        }
                        _ => 0.0,
                    },
                    accumulated_writes: target.accumulated_writes.load(Ordering::Relaxed),
                })
            })
            .collect::<Vec<_>>();
        let selected =
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, wanted);
        if require_full_set && selected.len() < wanted {
            tracing::warn!(
                available_targets = selected.len(),
                required_targets = wanted,
                "NoF physical offload retained its hot copy because redundancy is unavailable"
            );
            return None;
        }
        for index in &selected {
            self.state.targets[candidates[*index].target_id]
                .accumulated_writes
                .fetch_add(1, Ordering::Relaxed);
        }
        Some((candidates, selected))
    }

    pub(in crate::client) fn pending_backing(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
    ) -> Result<Option<ColdBackingRoute>> {
        let Some(data_plane) = self.data_plane else {
            return Ok(None);
        };
        let wanted = match data_plane {
            // A logical-object provider owns placement and replication behind this one target.
            ProviderDataPlane::Object => 1,
            ProviderDataPlane::Physical => self.replica_count,
            ProviderDataPlane::Managed => return Ok(None),
        };
        let require_full_set = data_plane == ProviderDataPlane::Physical;
        let Some((candidates, selected)) = self.select_write_targets(wanted, require_full_set)
        else {
            return Ok(None);
        };
        let Some(primary_index) = selected.first().copied() else {
            return Ok(None);
        };
        let target_locator =
            |_target_id: &str| -> Result<String> { Ok(self.object_locator(route)) };
        let primary_id = candidates[primary_index].target_id.to_string();
        let primary_locator = target_locator(&primary_id)?;
        let replicas = selected[1..]
            .iter()
            .map(|index| {
                let target_id = candidates[*index].target_id.to_string();
                Ok(ColdBackingReplica {
                    owner: self.state.local_runtime.clone(),
                    cold_tier_id: target_id.clone(),
                    object_locator: target_locator(&target_id)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(ColdBackingRoute {
            owner: self.state.local_runtime.clone(),
            cold_tier_id: primary_id,
            object_locator: primary_locator,
            length,
            checksum: Some(checksum),
            state: ColdBackingState::PendingOffload,
            replicas,
        }))
    }

    pub(in crate::client) fn pending_managed_backing(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
    ) -> Result<Option<ManagedPendingBacking>> {
        if self.data_plane != Some(ProviderDataPlane::Managed) {
            return Ok(None);
        }
        let Some((candidates, selected)) = self.select_write_targets(self.replica_count, true)
        else {
            return Ok(None);
        };
        self.managed_pending_backing(route, length, checksum, &candidates, &selected)
    }

    fn managed_pending_backing(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
        candidates: &[ReplicaWriteCandidate<'_>],
        selected: &[usize],
    ) -> Result<Option<ManagedPendingBacking>> {
        let mut managed_route = route.clone();
        for index in selected {
            let target_id = candidates[*index].target_id;
            managed_route = self.dispatch_managed_route_action(
                target_id,
                ManagedNofRouteAction::Prepare,
                managed_route,
                length,
                Some(checksum),
            )?;
        }
        let backing = managed_route
            .nof_backing
            .as_ref()
            .ok_or_else(|| StoreError::InvalidState("managed route was not published".to_string()))?
            .to_cold();
        Ok(Some(ManagedPendingBacking {
            backing,
            route: managed_route,
        }))
    }

    pub(in crate::client) fn publish_managed_route(
        &self,
        route: ObjectRoute,
    ) -> Result<ObjectRoute> {
        let backing = route
            .nof_backing
            .as_ref()
            .ok_or_else(|| StoreError::InvalidState("managed NoF route is missing".to_string()))?;
        self.dispatch_managed_route_action(
            &backing.target_id,
            ManagedNofRouteAction::Publish,
            route.clone(),
            backing.length,
            backing.checksum,
        )
    }

    pub(in crate::client) fn release_managed_target(
        &self,
        target_id: &str,
        route: ObjectRoute,
    ) -> Result<ObjectRoute> {
        let (length, checksum) = route
            .nof_backing
            .as_ref()
            .map(|backing| (backing.length, backing.checksum))
            .unwrap_or((0, None));
        self.dispatch_managed_route_action(
            target_id,
            ManagedNofRouteAction::Release,
            route,
            length,
            checksum,
        )
    }

    fn dispatch_managed_route_action(
        &self,
        target_id: &str,
        action: ManagedNofRouteAction,
        route: ObjectRoute,
        length: u64,
        checksum: Option<u64>,
    ) -> Result<ObjectRoute> {
        let owner = self.state.owner_for(target_id).ok_or_else(|| {
            StoreError::Transport(format!("NoF target {target_id} has no live owner"))
        })?;
        if owner == self.state.local_runtime {
            return self.manage_managed_route(
                self.route_ops.as_ref().ok_or_else(|| {
                    StoreError::InvalidState(
                        "managed NoF route control is missing route operations".to_string(),
                    )
                })?,
                target_id,
                action,
                route,
                length,
                checksum,
            );
        }
        let lease = self.state.owner_lease(target_id).ok_or_else(|| {
            StoreError::Transport(format!(
                "NoF target {target_id} owner {owner} has no live lease"
            ))
        })?;
        self.control_client
            .manage_nof_backing(&lease, target_id, action, &route, length, checksum)
    }

    fn transient_backing(&self, target_id: &str, object_locator: &str) -> ColdBackingRoute {
        ColdBackingRoute {
            owner: self.state.local_runtime.clone(),
            cold_tier_id: target_id.to_string(),
            object_locator: object_locator.to_string(),
            length: 0,
            checksum: None,
            state: ColdBackingState::Materialized,
            replicas: Vec::new(),
        }
    }

    pub(in crate::client) fn read_backing(
        &self,
        route: &ObjectRoute,
    ) -> Result<Option<ColdBackingRoute>> {
        if self.is_managed() {
            return Ok(None);
        }
        let locator = self.object_locator(route);
        let mut found = Vec::new();
        let mut first_error = None;
        for target_id in self.target_ids() {
            if !self.available_for_io(target_id) {
                continue;
            }
            let mut backing = self.transient_backing(target_id, &locator);
            match self.backend_for(target_id)?.query_object_length(&backing) {
                Ok(Some(length)) => {
                    backing.length = length;
                    found.push(backing);
                }
                Ok(None) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        let Some(mut primary) = found.first().cloned() else {
            return match first_error {
                Some(error) => Err(error),
                None => Ok(None),
            };
        };
        if found.iter().any(|backing| backing.length != primary.length) {
            return Err(StoreError::InvalidState(
                "NoF replicas reported different logical object lengths".to_string(),
            ));
        }
        primary.replicas = found
            .into_iter()
            .skip(1)
            .map(|backing| ColdBackingReplica {
                owner: backing.owner,
                cold_tier_id: backing.cold_tier_id,
                object_locator: backing.object_locator,
            })
            .collect();
        Ok(Some(primary))
    }

    pub(in crate::client) fn put_selected(
        &self,
        backing: &ColdBackingRoute,
        payload: &[u8],
    ) -> Result<()> {
        for target in crate::client::cold_tier::persistent_backing_targets(backing) {
            self.backend_for(&target.cold_tier_id)?
                .put_object(&target, payload)?;
        }
        Ok(())
    }

    /// Delete the logical object from all configured targets. No reclaim route is persisted.
    pub(in crate::client) fn delete_object(&self, route: &ObjectRoute) -> Result<bool> {
        if self.is_managed() {
            let Some(backing) = route.nof_backing.as_ref() else {
                return Ok(false);
            };
            let mut target_ids = Vec::with_capacity(1 + backing.replicas.len());
            target_ids.push(backing.target_id.clone());
            target_ids.extend(
                backing
                    .replicas
                    .iter()
                    .map(|replica| replica.target_id.clone()),
            );
            let mut current = route.clone();
            let mut deleted = false;
            let mut first_error = None;
            for target_id in target_ids {
                match self.release_managed_target(&target_id, current.clone()) {
                    Ok(next) => {
                        current = next;
                        deleted = true;
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            return match first_error {
                Some(error) => Err(error),
                None => Ok(deleted),
            };
        }
        let object_locator = self.object_locator(route);
        let mut deleted = false;
        let mut first_error = None;
        for target_id in self.target_ids() {
            let mut request = self.transient_backing(target_id, &object_locator);
            request.state = ColdBackingState::PendingDelete;
            match self.backend_for(target_id)?.delete_object(&request) {
                Ok(found) => deleted |= found,
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(deleted),
        }
    }

    fn object_locator(&self, route: &ObjectRoute) -> String {
        route.key.0.clone()
    }

    pub(in crate::client) fn release_ownership_on_shutdown(
        &self,
        control_client: &ControlPlaneClient,
    ) {
        if !self.is_empty() {
            if self.is_managed() {
                self.state.handoff_owned_targets(control_client);
            }
            self.state.release();
        }
    }
}

fn recover_managed_owned_targets(state: &NofOwnerState, targets: &BTreeMap<String, NofBackend>) {
    for target_id in state.locally_owned_target_ids() {
        let Some(target) = targets.get(&target_id) else {
            continue;
        };
        if let Err(error) = recover_managed_target(state, target, &target_id) {
            tracing::warn!(
                target_id,
                error = %error,
                "managed NoF route recovery failed"
            );
        }
    }
}

fn recover_managed_target(
    state: &NofOwnerState,
    target: &NofBackend,
    target_id: &str,
) -> Result<()> {
    let routes = match state.take_handoff(target_id) {
        Some(routes) => routes,
        None => state.list_managed_routes(target_id)?,
    };
    recover_managed_routes(target, target_id, routes)
}

fn recover_managed_routes(
    target: &NofBackend,
    target_id: &str,
    routes: Vec<ObjectRoute>,
) -> Result<()> {
    let mut records = Vec::new();
    for route in routes {
        let Some(backing) = route.nof_backing else {
            continue;
        };
        if backing.state != ColdBackingState::Materialized {
            continue;
        }
        if backing.target_id == target_id {
            records.push(managed_read_request(&backing)?);
        }
        for replica in backing.replicas {
            if replica.target_id != target_id {
                continue;
            }
            records.push(managed_read_request(&NofBackingRoute {
                owner: backing.owner.clone(),
                target_id: replica.target_id,
                object_locator: replica.object_locator,
                length: backing.length,
                checksum: backing.checksum,
                state: backing.state,
                replicas: Vec::new(),
            })?);
        }
    }
    target
        .backing
        .managed_allocator()
        .expect("managed allocator capability was checked during construction")
        .recover(&records)
}

fn managed_read_request(
    backing: &NofBackingRoute,
) -> Result<super::managed::NofManagedReadRequest> {
    Ok(super::managed::NofManagedReadRequest {
        locator: super::managed::NofManagedLocator::from_hex(&backing.object_locator)?,
        length: backing.length,
        checksum: backing.checksum,
    })
}

fn provider_data_plane(backend: &NofBackend) -> Result<ProviderDataPlane> {
    let object = backend.backing.object_write().is_some()
        && backend.backing.object_read().is_some()
        && backend.backing.object_query().is_some()
        && backend.backing.object_delete().is_some();
    let physical = backend.backing.physical_write().is_some()
        && backend.backing.physical_read().is_some()
        && backend.backing.physical_query().is_some()
        && backend.backing.physical_delete().is_some();
    let managed = backend.backing.managed_allocator().is_some()
        && backend.backing.managed_read().is_some()
        && backend.backing.managed_write().is_some();
    if managed {
        return Ok(ProviderDataPlane::Managed);
    }
    match (object, physical) {
        (true, false) => Ok(ProviderDataPlane::Object),
        (false, true) => Ok(ProviderDataPlane::Physical),
        (true, true) => Err(StoreError::InvalidState(
            "NoF runtime target must select one data plane through its advertised capabilities"
                .to_string(),
        )),
        (false, false) => Err(StoreError::InvalidState(
            "NoF runtime target requires complete read, write and delete capabilities".to_string(),
        )),
    }
}

struct NofObjectAdapter {
    target: NofBackend,
    namespace: NamespaceScope,
}

impl NofObjectAdapter {
    fn new(target: NofBackend) -> Self {
        Self {
            target,
            namespace: NamespaceScope::new("mooncake", "nof", "objects"),
        }
    }

    fn provider_key(object_id: &str) -> Result<String> {
        Ok(format!(
            "mooncake-{}",
            derive_physical_key(PhysicalKeyInput {
                domain: b"nof-provider-object",
                fields: &[object_id.as_bytes()],
                chunk_index: None,
            })?
            .to_hex()
        ))
    }
}

impl PersistentStorageBackend for NofObjectAdapter {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        self.target.health_snapshot()
    }

    fn put_object(&self, backing: &ColdBackingRoute, payload: &[u8]) -> Result<ColdBackingRoute> {
        validate_payload(backing, payload)?;
        let key = Self::provider_key(&backing.object_locator)?;
        self.target.init_namespace(&self.namespace)?;
        self.target.put_object(&self.namespace, &key, payload)?;
        Ok(ColdBackingRoute {
            state: ColdBackingState::Materialized,
            ..backing.clone()
        })
    }

    fn get_object(&self, backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let key = Self::provider_key(&backing.object_locator)?;
        match self
            .target
            .get_object_with_known_length(&self.namespace, &key, backing.length)?
        {
            NofObjectState::Found(value) => {
                if backing.length != 0 {
                    validate_payload(backing, &value)?;
                }
                Ok(Some(value))
            }
            NofObjectState::Missing => Ok(None),
            NofObjectState::Incomplete => Err(StoreError::Backpressure(
                "NoF provider object is incomplete".to_string(),
            )),
        }
    }

    fn query_object_length(&self, backing: &ColdBackingRoute) -> Result<Option<u64>> {
        let key = Self::provider_key(&backing.object_locator)?;
        match self.target.query_object(&self.namespace, &key)? {
            NofObjectState::Found(length) => Ok(Some(length)),
            NofObjectState::Missing => Ok(None),
            NofObjectState::Incomplete => Err(StoreError::Backpressure(
                "NoF provider object manifest is incomplete".to_string(),
            )),
        }
    }

    fn delete_object(&self, backing: &ColdBackingRoute) -> Result<bool> {
        let key = Self::provider_key(&backing.object_locator)?;
        match self.target.delete_object(&self.namespace, &key)? {
            NofObjectState::Found(()) => Ok(true),
            NofObjectState::Missing => Ok(false),
            NofObjectState::Incomplete => Err(StoreError::Backpressure(
                "NoF provider object deletion is incomplete".to_string(),
            )),
        }
    }

    fn put_pending_source(&self, _backing: &ColdBackingRoute, _payload: &[u8]) -> Result<()> {
        Ok(())
    }

    fn get_pending_source(&self, _backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn delete_pending_source(&self, _backing: &ColdBackingRoute) -> Result<bool> {
        Ok(false)
    }

    fn disable_pending_source(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientStableId,
        CompatibilityDescriptor, ObjectKey, RouteState, RouteVersion,
    };

    use super::*;
    use crate::client::cold_tier::owner::{NOF_TARGET_SET_LABEL, NOF_UNHEALTHY_TARGETS_LABEL};
    use crate::client::LiveClientCache;

    struct FakeBackend;

    impl PersistentStorageBackend for FakeBackend {
        fn health(&self) -> Result<PersistentStorageBackendHealth> {
            Ok(PersistentStorageBackendHealth {
                capacity_bytes: None,
                available_bytes: None,
            })
        }

        fn put_object(
            &self,
            _backing: &ColdBackingRoute,
            _payload: &[u8],
        ) -> Result<ColdBackingRoute> {
            unreachable!("heartbeat test does not write objects")
        }

        fn get_object(&self, _backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
            unreachable!("heartbeat test does not read objects")
        }

        fn delete_object(&self, _backing: &ColdBackingRoute) -> Result<bool> {
            unreachable!("heartbeat test does not delete objects")
        }

        fn put_pending_source(&self, _backing: &ColdBackingRoute, _payload: &[u8]) -> Result<()> {
            unreachable!("heartbeat test does not stage objects")
        }

        fn get_pending_source(&self, _backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
            unreachable!("heartbeat test does not read staged objects")
        }

        fn delete_pending_source(&self, _backing: &ColdBackingRoute) -> Result<bool> {
            unreachable!("heartbeat test does not delete staged objects")
        }
    }

    fn owner_lease(stable_id: &str, epoch: u64, fingerprint: &str) -> ClientLease {
        let mut endpoints = ClientEndpointSet::default();
        endpoints
            .labels
            .insert(NOF_TARGET_SET_LABEL.to_string(), fingerprint.to_string());
        endpoints
            .labels
            .insert(NOF_UNHEALTHY_TARGETS_LABEL.to_string(), "[]".to_string());
        ClientLease {
            runtime: ClientRuntimeId {
                stable_id: ClientStableId::new(stable_id),
                epoch: ClientEpoch(epoch),
            },
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints,
            expires_at_ms: u64::MAX,
        }
    }

    fn route(key: &str) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(7),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: None,
            nof_backing: None,
        }
    }

    #[test]
    fn pending_route_uses_the_calling_runtime_not_the_heartbeat_owner() {
        let local = owner_lease("client-a", 1, "same-target-set");
        let remote = owner_lease("client-b", 1, "same-target-set");
        let live_clients = Arc::new(parking_lot::Mutex::new(LiveClientCache::default()));
        live_clients
            .lock()
            .store(vec![local.clone(), remote.clone()]);
        let targets = (0..64)
            .map(|index| {
                let target_id = format!("nof-{index:04}");
                let backend = Arc::new(FakeBackend);
                (target_id, Arc::new(NofRuntimeTarget::new(backend)))
            })
            .collect::<BTreeMap<_, _>>();
        let state = Arc::new(NofOwnerState::new(
            local.runtime.clone(),
            Arc::new(InMemoryMetadataBackend::new()),
            live_clients,
            "same-target-set".to_string(),
            targets,
        ));
        state.heartbeat_owned_targets();
        let manager = NofTargetManager {
            data_plane: Some(ProviderDataPlane::Physical),
            replica_count: 8,
            state: state.clone(),
            managed_targets: BTreeMap::new(),
            route_ops: None,
            control_client: Arc::new(
                ControlPlaneClient::new().expect("test control client should build"),
            ),
            _heartbeat: NofHeartbeatMonitor::disabled(),
        };

        let backing = manager
            .pending_backing(&route("owned-route"), 4, 9)
            .expect("pending backing should build")
            .expect("pending backing should be selected");
        for target in backing.all_targets() {
            assert_eq!(target.owner, &local.runtime);
        }
        assert!(
            backing.all_targets().into_iter().any(|target| {
                manager.heartbeat_owner_for(target.cold_tier_id).as_ref() == Some(&remote.runtime)
            }),
            "test selection should include a target whose heartbeat owner is remote"
        );
    }
}
