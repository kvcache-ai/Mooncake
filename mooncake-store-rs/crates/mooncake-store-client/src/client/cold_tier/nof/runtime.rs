//! StoreClient runtime binding for configured NoF targets.
//!
//! NoF placement is request-local. Targets are selected for a write and discovered again through
//! the provider for reads and deletes; provider placement is never published as route metadata.

use std::collections::BTreeMap;
use std::sync::{atomic::Ordering, Arc};

use mooncake_store_core::{
    ClientRuntimeId, ColdBackingReplica, ColdBackingRoute, ColdBackingState, MetadataBackend,
    NamespaceScope, ObjectRoute, Result, StoreError,
};

use crate::client::cold_tier::layout::{derive_physical_key, PhysicalKeyInput};
use crate::client::cold_tier::owner::{NofHeartbeatMonitor, NofOwnerState, NofRuntimeTarget};
use crate::client::cold_tier::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaWriteCandidate, DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};
use crate::client::{
    PersistentObjectProbe, PersistentStorageBackend, PersistentStorageBackendHealth,
    SharedLiveClientCache,
};

use super::backing::validate_payload;
use super::object::NofObjectState;
use super::physical_backend::NofPhysicalStorageBackend;
use super::NofBackend;

/// One NoF target registered on `StoreClientBuilder`.
#[derive(Clone)]
pub struct NofTargetConfig {
    target_id: String,
    backend: NofBackend,
    data_plane: NofDataPlane,
}

impl NofTargetConfig {
    pub fn new(target_id: impl Into<String>, backend: NofBackend) -> Result<Self> {
        let target_id = target_id.into();
        if target_id.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF target ID must not be empty".to_string(),
            ));
        }
        let data_plane = runtime_data_plane(&backend)?;
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
            NofDataPlane::Object => b"object".to_vec(),
            NofDataPlane::Physical => b"physical".to_vec(),
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
            let next_data_plane = config.data_plane;
            if data_plane.is_some_and(|current| current != next_data_plane) {
                return Err(StoreError::InvalidState(
                    "one StoreClient cannot mix NoF logical-object and physical-KV targets"
                        .to_string(),
                ));
            }
            data_plane = Some(next_data_plane);
            let backend: Arc<dyn PersistentStorageBackend> = match next_data_plane {
                NofDataPlane::Object => Arc::new(NofObjectAdapter::new(config.backend)),
                NofDataPlane::Physical => Arc::new(NofPhysicalStorageBackend::new(config.backend)),
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
            replica_count,
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

    pub(in crate::client) fn target_ids(&self) -> impl Iterator<Item = &str> {
        self.state.targets.keys().map(String::as_str)
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
    ) -> Result<Option<ColdBackingRoute>> {
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
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, wanted);
        if data_plane == NofDataPlane::Physical && selected.len() < wanted {
            tracing::warn!(
                available_targets = selected.len(),
                required_targets = wanted,
                "NoF physical offload retained its hot copy because redundancy is unavailable"
            );
            return Ok(None);
        }
        let Some(primary_index) = selected.first().copied() else {
            return Ok(None);
        };
        let primary_id = candidates[primary_index].target_id.to_string();
        let primary_target = self
            .state
            .targets
            .get(&primary_id)
            .expect("selected NoF target must remain registered");
        primary_target
            .accumulated_writes
            .fetch_add(1, Ordering::Relaxed);
        for index in selected.iter().skip(1) {
            self.state.targets[candidates[*index].target_id]
                .accumulated_writes
                .fetch_add(1, Ordering::Relaxed);
        }
        let object_locator = self.object_locator(route)?;
        let primary_owner = self.state.owner_for(&primary_id).ok_or_else(|| {
            StoreError::Transport(format!("NoF target {primary_id} has no live owner"))
        })?;
        let replicas = selected[1..]
            .iter()
            .map(|index| {
                let target_id = candidates[*index].target_id.to_string();
                self.state
                    .owner_for(&target_id)
                    .map(|owner| ColdBackingReplica {
                        owner,
                        cold_tier_id: target_id,
                        object_locator: object_locator.clone(),
                    })
                    .ok_or_else(|| {
                        StoreError::Transport("NoF replica target has no live owner".to_string())
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(ColdBackingRoute {
            owner: primary_owner,
            cold_tier_id: primary_id,
            object_locator,
            length,
            checksum: Some(checksum),
            state: ColdBackingState::PendingOffload,
            replicas,
        }))
    }

    /// Find a provider object using only the logical route identity. The returned backing is a
    /// one-request I/O descriptor and is never written to metadata.
    pub(in crate::client) fn discover_backing(
        &self,
        route: &ObjectRoute,
    ) -> Result<Option<ColdBackingRoute>> {
        let object_locator = self.object_locator(route)?;
        let mut first_error = None;
        let mut found = Vec::new();
        for target_id in self.target_ids() {
            if !self.available_for_io(target_id) {
                continue;
            }
            let owner = self
                .owner_for(target_id)
                .unwrap_or_else(|| self.state.local_runtime.clone());
            let probe = ColdBackingRoute {
                owner,
                cold_tier_id: target_id.to_string(),
                object_locator: object_locator.clone(),
                length: 0,
                checksum: None,
                state: ColdBackingState::Materialized,
                replicas: Vec::new(),
            };
            match self.backend_for(target_id)?.probe_object(&probe) {
                Ok(Some(metadata)) => found.push((probe, metadata.length)),
                Ok(None) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if found.is_empty() {
            return match first_error {
                Some(error) => Err(error),
                None => Ok(None),
            };
        }
        let mut primary_index = 0usize;
        let mut length = found.iter().find_map(|(_, length)| *length);
        let mut checksum = None;
        if length.is_none() {
            for (index, (probe, _)) in found.iter().enumerate() {
                match self.backend_for(&probe.cold_tier_id)?.get_object(probe) {
                    Ok(Some(value)) => {
                        primary_index = index;
                        length = Some(value.len() as u64);
                        checksum = Some(crate::client::payload_checksum(&value));
                        break;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        let Some(length) = length else {
            return match first_error {
                Some(error) => Err(error),
                None => Ok(None),
            };
        };
        let (mut primary, _) = found.swap_remove(primary_index);
        primary.length = length;
        primary.checksum = checksum;
        primary.replicas = found
            .into_iter()
            .map(|(probe, _)| ColdBackingReplica {
                owner: probe.owner,
                cold_tier_id: probe.cold_tier_id,
                object_locator: probe.object_locator,
            })
            .collect();
        Ok(Some(primary))
    }

    pub(in crate::client) fn contains_object(&self, route: &ObjectRoute) -> Result<bool> {
        let object_locator = self.object_locator(route)?;
        let mut first_error = None;
        for target_id in self.target_ids() {
            if !self.available_for_io(target_id) {
                continue;
            }
            let probe = ColdBackingRoute {
                owner: self
                    .owner_for(target_id)
                    .unwrap_or_else(|| self.state.local_runtime.clone()),
                cold_tier_id: target_id.to_string(),
                object_locator: object_locator.clone(),
                length: 0,
                checksum: None,
                state: ColdBackingState::Materialized,
                replicas: Vec::new(),
            };
            match self.backend_for(target_id)?.probe_object(&probe) {
                Ok(Some(_)) => return Ok(true),
                Ok(None) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(false),
        }
    }

    pub(in crate::client) fn has_required_copies(&self, backing: &ColdBackingRoute) -> bool {
        let required = match self.data_plane {
            Some(NofDataPlane::Object) => 1,
            Some(NofDataPlane::Physical) => self.replica_count,
            None => return false,
        };
        1 + backing.replicas.len() >= required
    }

    pub(in crate::client) fn verify_backing(
        &self,
        backing: &ColdBackingRoute,
        expected_length: u64,
        expected_checksum: u64,
    ) -> Result<bool> {
        if backing.length != expected_length || !self.has_required_copies(backing) {
            return Ok(false);
        }
        if let Some(checksum) = backing.checksum {
            return Ok(checksum == expected_checksum);
        }
        Ok(self
            .backend_for(&backing.cold_tier_id)?
            .get_object(backing)?
            .is_some_and(|value| {
                value.len() as u64 == expected_length
                    && crate::client::payload_checksum(&value) == expected_checksum
            }))
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

    /// Delete one content generation from all configured targets. No reclaim route is persisted.
    pub(in crate::client) fn delete_object(&self, route: &ObjectRoute) -> Result<bool> {
        let object_locator = self.object_locator(route)?;
        let mut deleted = false;
        let mut first_error = None;
        for target_id in self.target_ids() {
            let owner = self
                .owner_for(target_id)
                .unwrap_or_else(|| self.state.local_runtime.clone());
            let request = ColdBackingRoute {
                owner,
                cold_tier_id: target_id.to_string(),
                object_locator: object_locator.clone(),
                length: 0,
                checksum: None,
                state: ColdBackingState::PendingDelete,
                replicas: Vec::new(),
            };
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

    fn object_locator(&self, route: &ObjectRoute) -> Result<String> {
        let generation = route.content_generation.to_le_bytes();
        Ok(derive_physical_key(PhysicalKeyInput {
            domain: b"nof-object",
            fields: &[route.key.0.as_bytes(), &generation],
            chunk_index: None,
        })?
        .to_hex())
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
        let metadata = self.target.put_object(&self.namespace, &key, payload)?;
        if metadata.length != backing.length {
            return Err(StoreError::InvalidState(
                "NoF provider returned an unexpected logical-object length".to_string(),
            ));
        }
        Ok(ColdBackingRoute {
            state: ColdBackingState::Materialized,
            ..backing.clone()
        })
    }

    fn get_object(&self, backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let key = Self::provider_key(&backing.object_locator)?;
        match self.target.get_object(&self.namespace, &key)? {
            NofObjectState::Found(object) => {
                if backing.length != 0 {
                    validate_payload(backing, &object.value)?;
                }
                if backing.length != 0 && object.metadata.length != backing.length {
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

    fn probe_object(&self, backing: &ColdBackingRoute) -> Result<Option<PersistentObjectProbe>> {
        if self.target.backing.object_query().is_none() {
            return Ok(self
                .get_object(backing)?
                .map(|value| PersistentObjectProbe {
                    length: Some(value.len() as u64),
                }));
        }
        let key = Self::provider_key(&backing.object_locator)?;
        match self.target.query_object(&self.namespace, &key)? {
            NofObjectState::Found(metadata) => Ok(Some(PersistentObjectProbe {
                length: Some(metadata.length),
            })),
            NofObjectState::Missing => Ok(None),
            NofObjectState::Incomplete => Err(StoreError::Backpressure(
                "NoF provider object is incomplete".to_string(),
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
            content_generation: 0,
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: None,
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
        state.heartbeat_owned_targets();
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
                .owner_for(target.cold_tier_id)
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
