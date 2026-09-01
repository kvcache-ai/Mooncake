//! StoreClient runtime binding for configured NoF targets.
//!
//! The existing Cold Tier state machine still owns queueing, batching, route CAS and rollback.
//! These types only select a NoF target and adapt its advertised data-plane capabilities to the
//! internal I/O envelope. Persisted metadata remains `NofBackingRoute` throughout.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use mooncake_store_core::{
    route_logical_object_id, ClientRuntimeId, ColdBackingRoute, ColdBackingState, NamespaceScope,
    NofBackingReplica, NofBackingRoute, NofBackingState, ObjectRoute, Result, StoreError,
};

use crate::client::cold_tier::layout::{
    decode_hex, encode_hex, PhysicalKeyCodec, PhysicalKeyInput, Sha256PhysicalKeyCodec,
};
use crate::client::cold_tier::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaWriteCandidate, DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};
use crate::client::{
    PersistentStorageBackend, PersistentStorageBackendHealth, PersistentStorageManagement,
};

use super::object::NofObjectState;
use super::physical::NofStorageHealth;
use super::physical_adapter::NofPhysicalAdapter;
use super::NofBackend;

const OBJECT_LOCATOR_PREFIX: &str = "nof-object:v1:";
const TARGET_HEALTH_CACHE_TTL: Duration = Duration::from_secs(1);

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

    pub fn target_id(&self) -> &str {
        &self.target_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NofDataPlane {
    Object,
    Physical,
}

struct NofRuntimeTarget {
    backend: Arc<dyn PersistentStorageBackend>,
    accumulated_writes: AtomicU64,
    health: parking_lot::Mutex<Option<(Instant, Result<PersistentStorageBackendHealth>)>>,
}

impl NofRuntimeTarget {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        let mut cached = self.health.lock();
        if let Some((observed_at, health)) = cached.as_ref() {
            if observed_at.elapsed() < TARGET_HEALTH_CACHE_TTL {
                return health.clone();
            }
        }
        let health = self.backend.health();
        *cached = Some((Instant::now(), health.clone()));
        health
    }
}

pub(in crate::client) struct NofTargetManager {
    data_plane: Option<NofDataPlane>,
    replica_count: usize,
    targets: BTreeMap<String, NofRuntimeTarget>,
}

impl NofTargetManager {
    pub(in crate::client) fn new(
        configs: Vec<NofTargetConfig>,
        replica_count: usize,
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
                NofDataPlane::Physical => Arc::new(NofPhysicalAdapter::new(
                    config.target_id.clone(),
                    config.backend,
                )?),
            };
            if targets
                .insert(
                    config.target_id.clone(),
                    NofRuntimeTarget {
                        backend,
                        accumulated_writes: AtomicU64::new(0),
                        health: parking_lot::Mutex::new(None),
                    },
                )
                .is_some()
            {
                return Err(StoreError::InvalidState(format!(
                    "duplicate NoF target ID {}",
                    config.target_id
                )));
            }
        }
        Ok(Self {
            data_plane,
            replica_count: replica_count.clamp(1, 8),
            targets,
        })
    }

    pub(in crate::client) fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    pub(in crate::client) fn contains(&self, target_id: &str) -> bool {
        self.targets.contains_key(target_id)
    }

    pub(in crate::client) fn available_for_io(&self, target_id: &str) -> bool {
        self.targets
            .get(target_id)
            .is_some_and(|target| target.health().is_ok())
    }

    pub(in crate::client) fn target_ids(&self) -> Vec<String> {
        self.targets.keys().cloned().collect()
    }

    pub(in crate::client) fn backend_for(
        &self,
        target_id: &str,
    ) -> Result<Arc<dyn PersistentStorageBackend>> {
        self.targets
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
        owner: ClientRuntimeId,
        length: u64,
        checksum: u64,
    ) -> Result<Option<NofBackingRoute>> {
        let Some(data_plane) = self.data_plane else {
            return Ok(None);
        };
        let mut scored = Vec::with_capacity(self.targets.len());
        for (target_id, target) in &self.targets {
            let health = match target.health() {
                Ok(health) => health,
                Err(error) => {
                    tracing::warn!(target_id, error = %error, "NoF target excluded from offload");
                    continue;
                }
            };
            let score = match (health.capacity_bytes, health.available_bytes) {
                (Some(capacity), Some(available)) if capacity > 0 => {
                    Some(available as f64 / capacity as f64)
                }
                _ => None,
            }
            .unwrap_or(0.0);
            scored.push((
                target_id.as_str(),
                score,
                target.accumulated_writes.load(Ordering::Relaxed),
            ));
        }
        let candidates = scored
            .iter()
            .map(
                |(target_id, score, accumulated_writes)| ReplicaWriteCandidate {
                    target_id,
                    score: *score,
                    accumulated_writes: *accumulated_writes,
                },
            )
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
            if let Some(target) = self.targets.get(scored[*index].0) {
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
        let primary_id = scored[primary_index].0.to_string();
        let replicas = selected[1..]
            .iter()
            .map(|index| NofBackingReplica {
                owner: owner.clone(),
                target_id: scored[*index].0.to_string(),
                object_locator: locator.clone(),
            })
            .collect();
        Ok(Some(NofBackingRoute {
            owner,
            target_id: primary_id,
            object_locator: locator,
            length,
            checksum: Some(checksum),
            state: NofBackingState::PendingWrite,
            replicas,
        }))
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
        if self.target.backing.storage_management().is_some() {
            PersistentStorageManagement::MooncakeManaged
        } else {
            PersistentStorageManagement::BackendManaged
        }
    }

    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        let health = nof_health(&self.target)?;
        Ok(PersistentStorageBackendHealth {
            capacity_bytes: health.capacity_bytes,
            available_bytes: health.available_bytes,
        })
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
        validate_object_payload(backing, payload)?;
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
                validate_object_payload(backing, &object.value)?;
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

fn nof_health(target: &NofBackend) -> Result<NofStorageHealth> {
    if let Some(storage) = target.backing.storage_management() {
        storage.storage_health()?.validate(true)
    } else if let Some(health) = target.backing.health_capability() {
        health.health()?.validate(false)
    } else if let Some(devices) = target.backing.device_management() {
        devices.device_health()?.validate(false)
    } else {
        Ok(NofStorageHealth::default())
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

fn validate_object_payload(backing: &ColdBackingRoute, payload: &[u8]) -> Result<()> {
    if payload.len() as u64 != backing.length {
        return Err(StoreError::InvalidState(format!(
            "NoF payload length {} does not match route length {}",
            payload.len(),
            backing.length
        )));
    }
    if backing
        .checksum
        .is_some_and(|checksum| checksum != crate::client::payload_checksum(payload))
    {
        return Err(StoreError::InvalidState(
            "NoF payload checksum does not match route checksum".to_string(),
        ));
    }
    Ok(())
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
