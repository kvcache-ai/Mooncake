//! StoreClient runtime binding for configured NoF targets.
//!
//! The existing Cold Tier state machine still owns queueing, batching, route CAS and rollback.
//! These types only select a NoF target and adapt its advertised data-plane capabilities to the
//! internal I/O envelope. Persisted metadata remains `NofBackingRoute` throughout.

use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};

use mooncake_store_core::{
    route_logical_object_id, ClientRuntimeId, ColdBackingRoute, ColdBackingState, MetadataBackend,
    NamespaceScope, NofBackingReplica, NofBackingRoute, NofBackingRouteFilter, NofBackingState,
    ObjectRoute, Result, StoreError,
};

use crate::client::cold_tier::layout::{
    decode_hex, encode_hex, PhysicalKeyCodec, PhysicalKeyInput, Sha256PhysicalKeyCodec,
};
use crate::client::cold_tier::owner::{NofHeartbeatMonitor, NofOwnerState, NofRuntimeTarget};
use crate::client::cold_tier::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaWriteCandidate, DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};
use crate::client::{
    PersistentStorageBackend, PersistentStorageBackendHealth, PersistentStorageManagement,
    SharedLiveClientCache,
};

use super::backing::validate_payload;
use super::object::NofObjectState;
use super::physical_backend::NofPhysicalStorageBackend;
use super::NofBackend;

const OBJECT_LOCATOR_PREFIX: &str = "nof-object:v1:";

/// One NoF target registered on `StoreClientBuilder`.
#[derive(Clone)]
pub struct NofTargetConfig {
    target_id: String,
    backend: NofBackend,
}

impl NofTargetConfig {
    pub fn new(target_id: impl Into<String>, backend: NofBackend) -> Result<Self> {
        let target_id = target_id.into();
        if target_id.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF target ID must not be empty".to_string(),
            ));
        }
        Ok(Self { target_id, backend })
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
        .map(|config| {
            Ok((
                config.target_id.as_str(),
                runtime_data_plane(&config.backend)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    targets.sort_unstable_by(|left, right| left.0.cmp(right.0));
    let mut encoded_fields = Vec::<Vec<u8>>::with_capacity(targets.len() * 2);
    for (target_id, data_plane) in targets {
        encoded_fields.push(match data_plane {
            NofDataPlane::Object => b"object".to_vec(),
            NofDataPlane::Physical => b"physical".to_vec(),
        });
        encoded_fields.push(target_id.as_bytes().to_vec());
    }
    let fields = encoded_fields.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let fingerprint = Sha256PhysicalKeyCodec.encode(PhysicalKeyInput {
        domain: b"nof-target-set-v1",
        fields: &fields,
        chunk_index: None,
    })?;
    Ok(Some(fingerprint.to_hex()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NofDataPlane {
    Object,
    Physical,
}

pub(in crate::client) struct NofTargetManager {
    data_plane: Option<NofDataPlane>,
    replica_count: usize,
    state: Arc<NofOwnerState>,
    _heartbeat: NofHeartbeatMonitor,
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
        for config in configs {
            let next_data_plane = runtime_data_plane(&config.backend)?;
            if data_plane.is_some_and(|current| current != next_data_plane) {
                return Err(StoreError::InvalidState(
                    "one StoreClient cannot mix NoF logical-object and physical-KV targets"
                        .to_string(),
                ));
            }
            data_plane = Some(next_data_plane);
            let backend: Arc<dyn PersistentStorageBackend> = match next_data_plane {
                NofDataPlane::Object => Arc::new(NofObjectAdapter::new(config.backend)),
                NofDataPlane::Physical => {
                    let backend =
                        NofPhysicalStorageBackend::new(config.target_id.clone(), config.backend)?;
                    if backend.requires_recovery() {
                        let routes =
                            metadata.list_object_routes_by_nof_backing(&NofBackingRouteFilter {
                                target_id: Some(config.target_id.clone()),
                                state: Some(NofBackingState::Materialized),
                                ..NofBackingRouteFilter::default()
                            })?;
                        backend.recover_routes(&routes)?;
                    }
                    Arc::new(backend)
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
        let heartbeat = if state.targets.is_empty() {
            NofHeartbeatMonitor::disabled()
        } else {
            NofHeartbeatMonitor::start(state.clone())?
        };
        Ok(Self {
            data_plane,
            replica_count: replica_count.clamp(1, 8),
            state,
            _heartbeat: heartbeat,
        })
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

    pub(in crate::client) fn target_ids(&self) -> Vec<String> {
        self.state.targets.keys().cloned().collect()
    }

    pub(in crate::client) fn owner_for(&self, target_id: &str) -> Option<ClientRuntimeId> {
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

    pub(in crate::client) fn pending_backing(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
    ) -> Result<Option<NofBackingRoute>> {
        let Some(data_plane) = self.data_plane else {
            return Ok(None);
        };
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
        let wanted = match data_plane {
            // A logical-object provider owns placement and replication behind this one target.
            NofDataPlane::Object => 1,
            NofDataPlane::Physical => self.replica_count,
        };
        let selected =
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, wanted.max(1));
        let Some(primary_index) = selected.first().copied() else {
            return Ok(None);
        };
        for index in &selected {
            if let Some(target) = self.state.targets.get(candidates[*index].target_id) {
                target.accumulated_writes.fetch_add(1, Ordering::Relaxed);
            }
        }
        let locator = format!(
            "{}@v{}",
            route
                .canonical_key
                .clone()
                .unwrap_or_else(|| route.key.0.clone()),
            route.version.0
        );
        let primary_id = candidates[primary_index].target_id.to_string();
        let primary_owner = self.state.owner_for(&primary_id).ok_or_else(|| {
            StoreError::Transport(format!("NoF target {primary_id} has no live owner"))
        })?;
        let replicas = selected[1..]
            .iter()
            .map(|index| {
                let target_id = candidates[*index].target_id.to_string();
                self.state
                    .owner_for(&target_id)
                    .map(|owner| NofBackingReplica {
                        owner,
                        target_id,
                        object_locator: locator.clone(),
                    })
                    .ok_or_else(|| {
                        StoreError::Transport("NoF replica target has no live owner".to_string())
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(NofBackingRoute {
            owner: primary_owner,
            target_id: primary_id,
            object_locator: locator,
            length,
            checksum: Some(checksum),
            state: NofBackingState::PendingWrite,
            replicas,
        }))
    }

    pub(in crate::client) fn release_ownership_on_shutdown(&self) {
        if !self.is_empty() {
            self.state.release();
        }
    }
}

fn runtime_data_plane(backend: &NofBackend) -> Result<NofDataPlane> {
    let object = backend.backing.object_write().is_some()
        && backend.backing.object_read().is_some()
        && backend.backing.object_delete().is_some();
    let physical = backend.backing.physical_write().is_some()
        && backend.backing.physical_read().is_some()
        && backend.backing.physical_delete().is_some();
    match (object, physical) {
        (true, false) => Ok(NofDataPlane::Object),
        (false, true) => Ok(NofDataPlane::Physical),
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
}

impl NofObjectAdapter {
    fn new(target: NofBackend) -> Self {
        Self { target }
    }

    fn identity(locator: &str) -> Result<(NamespaceScope, String)> {
        let encoded = locator.strip_prefix(OBJECT_LOCATOR_PREFIX).ok_or_else(|| {
            StoreError::InvalidState(format!("unknown NoF object locator: {locator}"))
        })?;
        let bytes = decode_hex(encoded, "NoF object locator")?;
        let mut cursor = bytes.as_slice();
        let tenant = read_locator_field(&mut cursor)?;
        let domain = read_locator_field(&mut cursor)?;
        let object_set = read_locator_field(&mut cursor)?;
        let key = read_locator_field(&mut cursor)?;
        if !cursor.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF object locator has trailing bytes".to_string(),
            ));
        }
        Ok((NamespaceScope::new(tenant, domain, object_set), key))
    }

    fn provider_identity(route: &ObjectRoute) -> Result<(NamespaceScope, String)> {
        let object_id = route_logical_object_id(route)?;
        let route_version = route.version.0.to_le_bytes();
        let key = Sha256PhysicalKeyCodec.encode(PhysicalKeyInput {
            domain: b"nof-object",
            fields: &[
                object_id.scope.tenant.as_bytes(),
                object_id.scope.domain.as_bytes(),
                object_id.scope.object_set.as_bytes(),
                object_id.logical_key.as_bytes(),
                &route_version,
            ],
            chunk_index: None,
        })?;
        Ok((object_id.scope, format!("mooncake-{}", key.to_hex())))
    }

    fn locator(namespace: &NamespaceScope, key: &str) -> Result<String> {
        let mut bytes = Vec::new();
        for field in [
            namespace.tenant.as_str(),
            namespace.domain.as_str(),
            namespace.object_set.as_str(),
            key,
        ] {
            let length = u32::try_from(field.len()).map_err(|_| {
                StoreError::InvalidState("NoF object identity field is too long".to_string())
            })?;
            bytes.extend_from_slice(&length.to_le_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        Ok(format!("{OBJECT_LOCATOR_PREFIX}{}", encode_hex(&bytes)))
    }
}

impl PersistentStorageBackend for NofObjectAdapter {
    fn storage_management(&self) -> PersistentStorageManagement {
        self.target.management_mode()
    }

    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        self.target.health_snapshot()
    }

    fn put_object(&self, backing: &ColdBackingRoute, payload: &[u8]) -> Result<ColdBackingRoute> {
        let _ = (backing, payload);
        Err(StoreError::InvalidState(
            "NoF logical-object writes require the object route identity".to_string(),
        ))
    }

    fn put_object_with_route(
        &self,
        route: Option<&ObjectRoute>,
        backing: &ColdBackingRoute,
        payload: &[u8],
    ) -> Result<ColdBackingRoute> {
        validate_payload(backing, payload)?;
        let route = route.ok_or_else(|| {
            StoreError::InvalidState(
                "NoF logical-object writes require the object route identity".to_string(),
            )
        })?;
        let (namespace, key) = Self::provider_identity(route)?;
        self.target.init_namespace(&namespace)?;
        let metadata = self.target.put_object(&namespace, &key, payload)?;
        if metadata.length != backing.length {
            return Err(StoreError::InvalidState(
                "NoF provider returned an unexpected logical-object length".to_string(),
            ));
        }
        Ok(ColdBackingRoute {
            object_locator: Self::locator(&namespace, &key)?,
            state: ColdBackingState::Materialized,
            ..backing.clone()
        })
    }

    fn get_object(&self, backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let (namespace, key) = Self::identity(&backing.object_locator)?;
        match self.target.get_object(&namespace, &key)? {
            NofObjectState::Found(object) => {
                validate_payload(backing, &object.value)?;
                if object.metadata.length != backing.length {
                    return Err(StoreError::InvalidState(
                        "NoF provider object metadata length does not match route".to_string(),
                    ));
                }
                Ok(Some(object.value))
            }
            NofObjectState::Missing => Ok(None),
            NofObjectState::Incomplete => Err(StoreError::Backpressure(
                "NoF provider object is incomplete".to_string(),
            )),
        }
    }

    fn delete_object(&self, backing: &ColdBackingRoute) -> Result<bool> {
        let (namespace, key) = Self::identity(&backing.object_locator)?;
        match self.target.delete_object(&namespace, &key)? {
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

fn read_locator_field(cursor: &mut &[u8]) -> Result<String> {
    if cursor.len() < 4 {
        return Err(StoreError::InvalidState(
            "NoF object locator is truncated".to_string(),
        ));
    }
    let mut encoded_length = [0; 4];
    encoded_length.copy_from_slice(&cursor[..4]);
    *cursor = &cursor[4..];
    let length = u32::from_le_bytes(encoded_length) as usize;
    if cursor.len() < length {
        return Err(StoreError::InvalidState(
            "NoF object locator field is truncated".to_string(),
        ));
    }
    let value = std::str::from_utf8(&cursor[..length])
        .map_err(|_| StoreError::InvalidState("NoF object locator is not UTF-8".to_string()))?
        .to_string();
    *cursor = &cursor[length..];
    Ok(value)
}

pub(in crate::client) fn nof_as_cold(backing: &NofBackingRoute) -> ColdBackingRoute {
    ColdBackingRoute {
        owner: backing.owner.clone(),
        cold_tier_id: backing.target_id.clone(),
        object_locator: backing.object_locator.clone(),
        length: backing.length,
        checksum: backing.checksum,
        state: match backing.state {
            NofBackingState::PendingWrite => ColdBackingState::PendingOffload,
            NofBackingState::Materialized => ColdBackingState::Materialized,
            NofBackingState::PendingDelete => ColdBackingState::PendingDelete,
        },
        replicas: backing
            .replicas
            .iter()
            .map(|replica| mooncake_store_core::ColdBackingReplica {
                owner: replica.owner.clone(),
                cold_tier_id: replica.target_id.clone(),
                object_locator: replica.object_locator.clone(),
            })
            .collect(),
    }
}

pub(in crate::client) fn cold_as_nof(backing: &ColdBackingRoute) -> NofBackingRoute {
    NofBackingRoute {
        owner: backing.owner.clone(),
        target_id: backing.cold_tier_id.clone(),
        object_locator: backing.object_locator.clone(),
        length: backing.length,
        checksum: backing.checksum,
        state: match backing.state {
            ColdBackingState::PendingOffload => NofBackingState::PendingWrite,
            ColdBackingState::Materialized => NofBackingState::Materialized,
            ColdBackingState::PendingDelete => NofBackingState::PendingDelete,
        },
        replicas: backing
            .replicas
            .iter()
            .map(|replica| NofBackingReplica {
                owner: replica.owner.clone(),
                target_id: replica.cold_tier_id.clone(),
                object_locator: replica.object_locator.clone(),
            })
            .collect(),
    }
}

pub(in crate::client) fn route_backing_as_cold(route: &ObjectRoute) -> Option<ColdBackingRoute> {
    route
        .cold_backing
        .clone()
        .or_else(|| route.nof_backing.as_ref().map(nof_as_cold))
}

#[cfg(test)]
mod tests {
    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientStableId,
        CompatibilityDescriptor, ObjectKey, RouteState, RouteVersion,
    };

    use super::*;
    use crate::client::cold_tier::owner::NOF_TARGET_SET_LABEL;
    use crate::client::LiveClientCache;

    struct FakeBackend;

    impl PersistentStorageBackend for FakeBackend {
        fn health(&self) -> Result<PersistentStorageBackendHealth> {
            unreachable!("disabled owner worker does not probe the test backend")
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
    fn pending_route_records_each_targets_elected_owner() {
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
        state.refresh_ownership();
        let manager = NofTargetManager {
            data_plane: Some(NofDataPlane::Physical),
            replica_count: 8,
            state: state.clone(),
            _heartbeat: NofHeartbeatMonitor::disabled(),
        };

        let backing = manager
            .pending_backing(&route("owned-route"), 4, 9)
            .expect("pending backing should build")
            .expect("pending backing should be selected");
        let mut saw_remote_owner = false;
        for target in backing.all_targets() {
            let expected = manager
                .owner_for(target.target_id)
                .expect("selected target should have an owner");
            assert_eq!(*target.owner, expected);
            saw_remote_owner |= target.owner == &remote.runtime;
        }
        assert!(
            saw_remote_owner,
            "test selection should cover a remote owner"
        );
    }
}
