//! StoreClient runtime binding for configured NoF targets.
//!
//! Provider-owned backings such as KVCS keep placement request-local. Mooncake-managed targets
//! publish their physical placement through the existing object-route authority.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{atomic::Ordering, Arc};

use mooncake_store_core::{
    ClientRuntimeId, ColdBackingReplica, ColdBackingRoute, ColdBackingState, MetadataBackend,
    NamespaceScope, NofBackingRoute, ObjectRoute, Result, RouteState, StoreError,
};

use crate::client::cold_tier::layout::{derive_physical_key, PhysicalKeyInput};
use crate::client::cold_tier::owner::{NofHeartbeatMonitor, NofOwnerState, NofRuntimeTarget};
use crate::client::cold_tier::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaWriteCandidate, DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};
use crate::client::cold_tier_storage_backend::reconcile_nof_managed_startup;
use crate::client::{
    PersistentStorageBackend, PersistentStorageBackendHealth, SharedLiveClientCache,
};
use crate::control_plane::{ControlPlaneClient, ManagedNofRouteAction};

use super::backing::validate_payload;
use super::managed::{NofManagedAllocationRequest, NofManagedRouteIdentity};
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
    replica_count: usize,
) -> Result<Option<String>> {
    if configs.is_empty() {
        return Ok(None);
    }
    let mut targets = configs
        .iter()
        .map(|config| (config.target_id.as_str(), config.data_plane))
        .collect::<Vec<_>>();
    targets.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut encoded_fields = Vec::<Vec<u8>>::with_capacity(targets.len() * 2 + 1);
    encoded_fields.push((replica_count as u64).to_le_bytes().to_vec());
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

pub(in crate::client) fn mirror_managed_route_index(
    metadata: &dyn MetadataBackend,
    route: &ObjectRoute,
) -> Result<()> {
    let current = metadata.get_object_route(&route.key)?;
    if current
        .as_ref()
        .is_some_and(|current| current.version > route.version)
    {
        return Ok(());
    }
    let expected = current.as_ref().map(|current| current.version);
    let next = route.nof_backing.as_ref().map(|_| route);
    let cas = metadata.compare_and_swap_object_route(&route.key, expected, next)?;
    if cas.applied {
        Ok(())
    } else {
        Err(StoreError::Conflict(format!(
            "managed NoF metadata mirror changed for {}",
            route.key.0
        )))
    }
}

impl NofTargetManager {
    fn mirror_managed_route(&self, target_id: &str, route: &ObjectRoute) -> Result<()> {
        self.state.mirror_managed_route(route, target_id)
    }

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
            data_plane == Some(ProviderDataPlane::Managed),
            targets,
        ));
        // Establish the first owner-scoped health snapshot before this target can be selected.
        // Later requests only read the cached snapshot; the monitor refreshes it in the background.
        state.heartbeat_owned_targets();
        let heartbeat = if state.targets.is_empty() {
            NofHeartbeatMonitor::disabled()
        } else {
            NofHeartbeatMonitor::start(state.clone())?
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
        if self.is_managed() {
            self.state.require_recovery_for_locally_owned();
            if let Err(error) = self.recover_owned_managed_targets() {
                tracing::warn!(
                    error = %error,
                    "managed NoF startup recovery is pending retry"
                );
            }
            self.state.heartbeat_owned_targets();
        }
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

    pub(in crate::client) fn locally_owned_managed_target_ids(&self) -> Vec<String> {
        if !self.is_managed() {
            return Vec::new();
        }
        self.state.locally_owned_target_ids()
    }

    pub(in crate::client) fn recover_owned_managed_targets(&self) -> Result<()> {
        let _management = self.state.management_gate.write();
        if self.state.is_released() {
            return Err(StoreError::InvalidState(
                "managed NoF owner is draining".to_string(),
            ));
        }
        if !self.is_managed() {
            return Ok(());
        }
        let route_ops = self.route_ops.as_ref().ok_or_else(|| {
            StoreError::InvalidState("managed NoF recovery requires route operations".to_string())
        })?;
        recover_managed_owned_targets(&self.state, &self.managed_targets, route_ops)
    }

    pub(in crate::client) fn managed_downline_ready_target_ids(&self) -> Vec<String> {
        if !self.is_managed() {
            return Vec::new();
        }
        self.state.locally_owned_downline_ready_target_ids()
    }

    pub(in crate::client) fn list_managed_routes(
        &self,
        target_id: &str,
    ) -> Result<Vec<ObjectRoute>> {
        self.state.list_managed_routes(target_id)
    }

    pub(in crate::client) fn managed_target_health(
        &self,
        target_id: &str,
    ) -> Result<Option<PersistentStorageBackendHealth>> {
        self.locally_owned_managed_target(target_id)?
            .map(NofRuntimeTarget::health_snapshot)
            .transpose()
    }

    pub(in crate::client) fn refresh_managed_target_health(
        &self,
        target_id: &str,
    ) -> Result<Option<PersistentStorageBackendHealth>> {
        let Some(target) = self.locally_owned_managed_target(target_id)? else {
            return Ok(None);
        };
        target.heartbeat(target_id);
        target.health_snapshot().map(Some)
    }

    fn locally_owned_managed_target(&self, target_id: &str) -> Result<Option<&NofRuntimeTarget>> {
        if !self.is_managed()
            || self.state.owner_for(target_id).as_ref() != Some(&self.state.local_runtime)
        {
            return Ok(None);
        }
        self.state
            .targets
            .get(target_id)
            .map(Arc::as_ref)
            .map(Some)
            .ok_or_else(|| {
                StoreError::NotFound(format!("managed NoF target {target_id} is not registered"))
            })
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
        target_id: &str,
        action: ManagedNofRouteAction,
        route: ObjectRoute,
        length: u64,
        checksum: Option<u64>,
    ) -> Result<ObjectRoute> {
        let _management = self.state.management_gate.read();
        if self.state.is_released() {
            return Err(StoreError::InvalidState(
                "managed NoF owner is draining".to_string(),
            ));
        }
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
        self.state.ensure_local_managed_owner(target_id)?;
        if self.state.targets[target_id].recovery_required() {
            return Err(StoreError::Backpressure(format!(
                "managed NoF target {target_id} is recovering"
            )));
        }
        let route_ops = self.route_ops.as_ref().ok_or_else(|| {
            StoreError::InvalidState(
                "managed NoF route control is missing route operations".to_string(),
            )
        })?;
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
                        self.mirror_managed_route(target_id, &route)?;
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
                    route_identity: Some(NofManagedRouteIdentity {
                        key: route.key.clone(),
                        namespace: route.namespace.clone(),
                        logical_key: route.logical_key.clone(),
                        canonical_key: route.canonical_key.clone(),
                        sharing_scope: route.sharing_scope.clone(),
                        qos_tier: route.qos_tier.clone(),
                        route_version: route.version.next(),
                        target_id: target_id.to_string(),
                    }),
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
                    if let Err(error) = self.mirror_managed_route(target_id, &next) {
                        let mut rollback = route.clone();
                        rollback.version = next.version.next();
                        let rollback_cas = route_ops.compare_and_swap_route(
                            &next.key,
                            Some(next.version),
                            Some(&rollback),
                        )?;
                        if rollback_cas.applied {
                            let _ =
                                allocator.release_batch(&[super::managed::NofManagedReadRequest {
                                    locator,
                                    length,
                                    checksum,
                                }]);
                            let _ = self.mirror_managed_route(target_id, &rollback);
                        }
                        return Err(error);
                    }
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
                let requested = route.nof_backing.as_ref().ok_or_else(|| {
                    StoreError::NotFound("managed NoF route is missing".to_string())
                })?;
                if requested.target_id != target_id {
                    return Err(StoreError::Conflict(
                        "only the primary managed NoF target owner may publish".to_string(),
                    ));
                }
                if requested
                    .all_targets()
                    .map(|(target_id, _, _)| target_id)
                    .collect::<BTreeSet<_>>()
                    .len()
                    != self.replica_count
                {
                    return Err(StoreError::Backpressure(format!(
                        "managed NoF route {} does not have all {} target reservations",
                        route.key.0, self.replica_count
                    )));
                }
                let current = route_ops.load_route(&route.key)?.ok_or_else(|| {
                    StoreError::Conflict(format!(
                        "managed NoF route {} disappeared while publishing",
                        route.key.0
                    ))
                })?;
                let current_backing = current.nof_backing.as_ref().ok_or_else(|| {
                    StoreError::Conflict(format!(
                        "managed NoF route {} no longer has its allocation",
                        route.key.0
                    ))
                })?;
                if !same_managed_allocation(requested, current_backing) {
                    return Err(StoreError::Conflict(format!(
                        "managed NoF route {} changed allocation while publishing",
                        route.key.0
                    )));
                }
                if current_backing.state == ColdBackingState::Materialized {
                    let _ = self.mirror_managed_route(target_id, &current);
                    return Ok(current);
                }
                if current_backing.state != ColdBackingState::PendingOffload {
                    return Err(StoreError::Conflict(format!(
                        "managed NoF route {} is not pending publication",
                        route.key.0
                    )));
                }
                let mut next = current.clone();
                next.version = current.version.next();
                next.nof_backing.as_mut().expect("checked above").state =
                    ColdBackingState::Materialized;
                let cas = route_ops.compare_and_swap_route(
                    &current.key,
                    Some(current.version),
                    Some(&next),
                )?;
                if cas.applied {
                    let _ = self.mirror_managed_route(target_id, &next);
                    Ok(next)
                } else {
                    Err(StoreError::Conflict(format!(
                        "managed NoF route {} changed while publishing",
                        route.key.0
                    )))
                }
            }
            ManagedNofRouteAction::Release => {
                if route
                    .nof_backing
                    .as_ref()
                    .is_some_and(|backing| backing.state == ColdBackingState::PendingOffload)
                {
                    return Err(StoreError::Backpressure(format!(
                        "managed NoF route {} is still being written",
                        route.key.0
                    )));
                }
                let Some((next, release)) = route_without_managed_target(&route, target_id)? else {
                    return Ok(route);
                };
                let keeps_payload = route_has_payload(&next);
                let cas = if keeps_payload {
                    route_ops.compare_and_swap_route(
                        &route.key,
                        Some(route.version),
                        Some(&next),
                    )?
                } else {
                    route_ops.delete_route_with_version_fence(&route)?
                };
                if !cas.applied {
                    return Err(StoreError::Conflict(format!(
                        "managed NoF route {} changed while completing target {target_id} release",
                        route.key.0
                    )));
                }
                let _ = self.mirror_managed_route(target_id, &next);
                let allocator = self
                    .managed_targets
                    .get(target_id)
                    .and_then(|backend| backend.backing.managed_allocator())
                    .expect("managed allocator capability was checked during construction");
                let release_result = (|| {
                    let results =
                        allocator.release_batch(&[super::managed::NofManagedReadRequest {
                            locator: super::managed::NofManagedLocator::from_hex(&release.locator)?,
                            length: release.length,
                            checksum: release.checksum,
                        }]);
                    ensure_batch_len("managed release", 1, results.len())?;
                    results.into_iter().next().expect("length checked above")
                })();
                if let Err(error) = release_result {
                    if keeps_payload {
                        let mut rollback = route.clone();
                        rollback.version = next.version.next();
                        if route_ops
                            .compare_and_swap_route(&next.key, Some(next.version), Some(&rollback))
                            .is_ok_and(|cas| cas.applied)
                        {
                            let _ = self.mirror_managed_route(target_id, &rollback);
                        }
                    }
                    self.state.targets[target_id].require_recovery();
                    return Err(error);
                }
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
        excluded: &BTreeSet<String>,
    ) -> Option<(Vec<ReplicaWriteCandidate<'_>>, Vec<usize>)> {
        let candidates = self
            .state
            .targets
            .iter()
            .filter(|(target_id, _)| !excluded.contains(*target_id))
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
                    // Managed target capacity is owner-local and is not published to every
                    // writer. Keep all healthy managed targets comparable and let the shared
                    // strategy balance them by accumulated writes and stable target ID.
                    score: if self.is_managed() {
                        0.0
                    } else {
                        match (health.capacity_bytes, health.available_bytes) {
                            (Some(capacity), Some(available)) if capacity > 0 => {
                                available as f64 / capacity as f64
                            }
                            _ => 0.0,
                        }
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
        let Some((candidates, selected)) =
            self.select_write_targets(wanted, require_full_set, &BTreeSet::new())
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
        self.managed_pending_backing(route, length, checksum)
    }

    fn managed_pending_backing(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
    ) -> Result<Option<ManagedPendingBacking>> {
        let mut managed_route = route.clone();
        let mut prepared_targets = BTreeSet::new();
        if let Some(backing) = route.nof_backing.as_ref() {
            if backing.state != ColdBackingState::PendingOffload
                || backing.length != length
                || backing.checksum != Some(checksum)
            {
                return Err(StoreError::Conflict(format!(
                    "managed NoF route {} has a different pending allocation",
                    route.key.0
                )));
            }
            prepared_targets.extend(
                backing
                    .all_targets()
                    .map(|(target_id, _, _)| target_id.to_string()),
            );
        }
        if prepared_targets.len() > self.replica_count {
            return Err(StoreError::InvalidState(format!(
                "managed NoF route {} has more target reservations than configured",
                route.key.0
            )));
        }
        let missing = self.replica_count.saturating_sub(prepared_targets.len());
        let (candidates, selected) = if missing == 0 {
            (Vec::new(), Vec::new())
        } else {
            let Some(selected) = self.select_write_targets(missing, true, &prepared_targets) else {
                return Err(StoreError::Backpressure(format!(
                    "managed NoF route {} cannot reach {} target replicas",
                    route.key.0, self.replica_count
                )));
            };
            selected
        };
        for index in selected {
            let target_id = candidates[index].target_id;
            managed_route = self.dispatch_managed_route_action(
                target_id,
                ManagedNofRouteAction::Prepare,
                managed_route,
                length,
                Some(checksum),
            )?;
            prepared_targets.insert(target_id.to_string());
        }
        if prepared_targets.len() != self.replica_count {
            return Err(StoreError::Backpressure(format!(
                "managed NoF route {} has {} of {} target reservations",
                route.key.0,
                prepared_targets.len(),
                self.replica_count
            )));
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

    pub(in crate::client) fn forget_managed_target_route(
        &self,
        target_id: &str,
        route: ObjectRoute,
    ) -> Result<ObjectRoute> {
        let _management = self.state.management_gate.read();
        if self.state.is_released() {
            return Err(StoreError::InvalidState(
                "managed NoF owner is draining".to_string(),
            ));
        }
        if !self.is_managed() {
            return Err(StoreError::Unsupported(
                "managed NoF route control requires a managed target".to_string(),
            ));
        }
        self.state.ensure_local_managed_owner(target_id)?;
        let deletion_pending = route.state == RouteState::Deleting;
        let Some((mut next, _release)) = route_without_managed_target(&route, target_id)? else {
            return Ok(route);
        };
        if deletion_pending && !route_has_payload(&next) {
            next.state = RouteState::Deleting;
        }
        let route_ops = self.route_ops.as_ref().ok_or_else(|| {
            StoreError::InvalidState(
                "managed NoF route control is missing route operations".to_string(),
            )
        })?;
        // Downline removes the failed target reference but keeps the logical route. If this was
        // the last payload copy, the empty route lets the shared manifest recovery path restore
        // the target without overriding a user-delete version fence. Existing delete/reclaim
        // intent remains Deleting so recovery can finish the physical release instead.
        let cas = route_ops.compare_and_swap_route(&route.key, Some(route.version), Some(&next))?;
        if cas.applied {
            let _ = self.mirror_managed_route(target_id, &next);
            Ok(next)
        } else {
            Err(StoreError::Conflict(format!(
                "managed NoF route {} changed while forgetting target {target_id}",
                route.key.0
            )))
        }
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
            return self.manage_managed_route(target_id, action, route, length, checksum);
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
            let target_ids = backing
                .all_targets()
                .map(|(target_id, _, _)| target_id.to_string())
                .collect::<Vec<_>>();
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
            // Drain in-flight owner operations before taking the handoff snapshot. New operations
            // remain blocked until release withdraws the owner capability.
            let _management = self.state.management_gate.write();
            if self.is_managed() {
                self.state.handoff_owned_targets(control_client);
            }
            self.state.release();
        }
    }
}

pub(in crate::client) struct ManagedTargetRelease {
    pub(in crate::client) locator: String,
    pub(in crate::client) length: u64,
    pub(in crate::client) checksum: Option<u64>,
}

pub(in crate::client) fn route_has_payload(route: &ObjectRoute) -> bool {
    !route.replicas.is_empty() || route.cold_backing.is_some() || route.nof_backing.is_some()
}

fn same_managed_allocation(left: &NofBackingRoute, right: &NofBackingRoute) -> bool {
    left.target_id == right.target_id
        && left.owner == right.owner
        && left.object_locator == right.object_locator
        && left.length == right.length
        && left.checksum == right.checksum
        && left.replicas == right.replicas
}

pub(in crate::client) fn route_without_managed_target(
    route: &ObjectRoute,
    target_id: &str,
) -> Result<Option<(ObjectRoute, ManagedTargetRelease)>> {
    let backing = route
        .nof_backing
        .as_ref()
        .ok_or_else(|| StoreError::NotFound("managed NoF route is missing".to_string()))?;
    let (locator, is_primary) = if backing.target_id == target_id {
        (backing.object_locator.clone(), true)
    } else if let Some(replica) = backing
        .replicas
        .iter()
        .find(|replica| replica.target_id == target_id)
    {
        (replica.object_locator.clone(), false)
    } else {
        return Ok(None);
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
    Ok(Some((
        next,
        ManagedTargetRelease {
            locator,
            length: backing.length,
            checksum: backing.checksum,
        },
    )))
}

pub(in crate::client) fn managed_target_cold_backing(
    route: &ObjectRoute,
    target_id: &str,
) -> Result<ColdBackingRoute> {
    let backing = route
        .nof_backing
        .as_ref()
        .ok_or_else(|| StoreError::NotFound("managed NoF route is missing".to_string()))?;
    if backing.target_id == target_id {
        return Ok(backing.to_cold());
    }
    let replica = backing
        .replicas
        .iter()
        .find(|replica| replica.target_id == target_id)
        .ok_or_else(|| {
            StoreError::NotFound(format!(
                "managed NoF route does not contain target {target_id}"
            ))
        })?;
    Ok(ColdBackingRoute {
        owner: replica.owner.clone(),
        cold_tier_id: replica.target_id.clone(),
        object_locator: replica.object_locator.clone(),
        length: backing.length,
        checksum: backing.checksum,
        state: backing.state,
        replicas: Vec::new(),
    })
}

fn recover_managed_owned_targets(
    state: &NofOwnerState,
    targets: &BTreeMap<String, NofBackend>,
    route_ops: &mooncake_store_route::RouteOperations,
) -> Result<()> {
    let mut first_error = None;
    for target_id in state.locally_owned_recovery_target_ids() {
        state.ensure_local_managed_owner(&target_id)?;
        let Some(target) = targets.get(&target_id) else {
            continue;
        };
        let handoff = state.take_handoff(&target_id);
        let result = if let Some(snapshot) = handoff.as_ref() {
            recover_managed_routes(
                target,
                &target_id,
                snapshot.routes.clone(),
                &state.local_runtime,
                route_ops,
            )
        } else if target.backing.managed_recovery().is_some() {
            reconcile_nof_managed_startup(route_ops, &target_id, target)
        } else {
            recover_managed_routes(
                target,
                &target_id,
                state.list_managed_routes(&target_id)?,
                &state.local_runtime,
                route_ops,
            )
        };
        match result {
            Ok(()) => {
                state.targets[&target_id].finish_recovery();
                state.clear_handoff_dirty(&target_id);
            }
            Err(error) => {
                if let Some(snapshot) = handoff {
                    state.restore_handoff(target_id.clone(), snapshot);
                }
                tracing::warn!(target_id, error = %error, "managed NoF target recovery failed");
                first_error.get_or_insert(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn recover_managed_routes(
    target: &NofBackend,
    target_id: &str,
    routes: Vec<ObjectRoute>,
    owner: &ClientRuntimeId,
    route_ops: &mooncake_store_route::RouteOperations,
) -> Result<()> {
    let mut records = Vec::new();
    for route in routes {
        let route = match adopt_managed_target_owner(route_ops, route, target_id, owner) {
            Ok(route) => route,
            Err(StoreError::NotFound(_)) => continue,
            Err(error) => return Err(error),
        };
        let Some(backing) = route.nof_backing else {
            continue;
        };
        for (_, _, object_locator) in backing
            .all_targets()
            .filter(|(backing_target_id, _, _)| *backing_target_id == target_id)
        {
            records.push(super::managed::NofManagedReadRequest {
                locator: super::managed::NofManagedLocator::from_hex(object_locator)?,
                length: backing.length,
                checksum: backing.checksum,
            });
        }
    }
    target
        .backing
        .managed_allocator()
        .expect("managed allocator capability was checked during construction")
        .recover(&records)
}

pub(in crate::client) fn adopt_managed_target_owner(
    route_ops: &mooncake_store_route::RouteOperations,
    route: ObjectRoute,
    target_id: &str,
    owner: &ClientRuntimeId,
) -> Result<ObjectRoute> {
    let Some(mut current) = route_ops.load_route(&route.key)? else {
        return Err(StoreError::NotFound(format!(
            "managed NoF route {} no longer exists",
            route.key.0
        )));
    };
    let Some(backing) = current.nof_backing.as_mut() else {
        return Ok(current);
    };
    let current_owner = if backing.target_id == target_id {
        &mut backing.owner
    } else if let Some(replica) = backing
        .replicas
        .iter_mut()
        .find(|replica| replica.target_id == target_id)
    {
        &mut replica.owner
    } else {
        return Ok(current);
    };
    if current_owner == owner {
        return Ok(current);
    }
    *current_owner = owner.clone();
    let expected = current.version;
    current.version = current.version.next();
    let cas = route_ops.compare_and_swap_route(&current.key, Some(expected), Some(&current))?;
    if !cas.applied {
        return Err(StoreError::Conflict(format!(
            "managed NoF route {} changed while adopting target {target_id}",
            current.key.0
        )));
    }
    mirror_managed_route_index(route_ops.metadata(), &current)?;
    Ok(current)
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
            false,
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

    #[test]
    fn managed_recovery_excludes_target_management() {
        let local = owner_lease("client-a", 1, "same-target-set");
        let state = Arc::new(NofOwnerState::new(
            local.runtime,
            Arc::new(InMemoryMetadataBackend::new()),
            Arc::new(parking_lot::Mutex::new(LiveClientCache::default())),
            "same-target-set".to_string(),
            true,
            BTreeMap::new(),
        ));
        let manager = Arc::new(NofTargetManager {
            data_plane: Some(ProviderDataPlane::Managed),
            replica_count: 1,
            state: state.clone(),
            managed_targets: BTreeMap::new(),
            route_ops: None,
            control_client: Arc::new(
                ControlPlaneClient::new().expect("test control client should build"),
            ),
            _heartbeat: NofHeartbeatMonitor::disabled(),
        });
        let management = state.management_gate.read();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            tx.send(manager.recover_owned_managed_targets()).unwrap();
        });
        assert!(rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err());
        drop(management);
        assert!(rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
            .is_err());
        worker.join().unwrap();
    }

    #[test]
    fn managed_route_target_removal_promotes_the_next_replica() {
        let primary = owner_lease("client-a", 1, "same-target-set").runtime;
        let replica = owner_lease("client-b", 1, "same-target-set").runtime;
        let mut route = route("managed-nof-route");
        route.nof_backing = Some(NofBackingRoute {
            owner: primary.clone(),
            target_id: "nof-a".to_string(),
            object_locator: "aaaa".to_string(),
            length: 128,
            checksum: Some(7),
            state: ColdBackingState::Materialized,
            replicas: vec![mooncake_store_core::NofBackingReplica {
                owner: replica.clone(),
                target_id: "nof-b".to_string(),
                object_locator: "bbbb".to_string(),
            }],
        });

        let (next, release) = route_without_managed_target(&route, "nof-a")
            .expect("route removal should succeed")
            .expect("primary target should exist");
        let backing = next.nof_backing.expect("replica should be promoted");
        assert_eq!(backing.owner, replica);
        assert_eq!(backing.target_id, "nof-b");
        assert_eq!(backing.object_locator, "bbbb");
        assert!(backing.replicas.is_empty());
        assert_eq!(release.locator, "aaaa");
        assert_eq!(release.length, 128);
        assert_eq!(release.checksum, Some(7));
    }
}
