//! Cold Tier target ownership and owner-scoped health maintenance.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::{
    ClientLease, ClientLifecycleState, ClientRuntimeId, MetadataBackend, Result, StoreError,
};

use crate::client::{
    ColdTierDeviceManager, PersistentStorageBackend, PersistentStorageBackendHealth,
    SharedLiveClientCache,
};
use crate::placement::stable_rendezvous_score;

const TARGET_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
pub(super) const TARGET_HEARTBEAT_FAILURE_THRESHOLD: u32 = 3;
pub(in crate::client) const NOF_TARGET_SET_LABEL: &str = "nof.target-set";
pub(in crate::client) const NOF_UNHEALTHY_TARGETS_LABEL: &str = "nof.unhealthy-targets";
const NOF_MANAGED_LABELS: [&str; 2] = [NOF_TARGET_SET_LABEL, NOF_UNHEALTHY_TARGETS_LABEL];

impl ColdTierDeviceManager {
    /// Resolve the authority for a local persistent target.
    ///
    /// NoF ownership only distributes heartbeat work and never gates data I/O.
    pub(in crate::client) fn current_target_owner(
        &self,
        target_id: &str,
        recorded_owner: Option<&ClientRuntimeId>,
    ) -> Result<ClientRuntimeId> {
        if let Some(owner) = recorded_owner {
            return Ok(owner.clone());
        }
        if self.has_local_backend(target_id) {
            return Ok(self.runtime.clone());
        }
        Err(StoreError::NotFound(format!(
            "persistent target {target_id} is not registered"
        )))
    }

    pub(in crate::client) fn release_nof_ownership_on_shutdown(&self) {
        self.nof_targets.release_ownership_on_shutdown();
    }
}

pub(super) struct NofRuntimeTarget {
    pub(super) backend: Arc<dyn PersistentStorageBackend>,
    pub(super) accumulated_writes: AtomicU64,
    health: parking_lot::RwLock<NofTargetHealthState>,
}

impl NofRuntimeTarget {
    pub(super) fn new(backend: Arc<dyn PersistentStorageBackend>) -> Self {
        Self {
            backend,
            accumulated_writes: AtomicU64::new(0),
            health: parking_lot::RwLock::new(NofTargetHealthState::default()),
        }
    }

    pub(super) fn health_snapshot(&self) -> Result<PersistentStorageBackendHealth> {
        self.health.read().snapshot()
    }

    fn heartbeat(&self, target_id: &str) -> bool {
        let result = self.backend.health();
        let mut health = self.health.write();
        let previous_failures = health.consecutive_failures;
        health.record(result);
        match (previous_failures, health.consecutive_failures) {
            (failures, 0) if failures > 0 => {
                tracing::info!(
                    target_id,
                    previous_failures = failures,
                    "NoF target heartbeat recovered"
                );
            }
            (_, TARGET_HEARTBEAT_FAILURE_THRESHOLD) => {
                tracing::warn!(
                    target_id,
                    failures = health.consecutive_failures,
                    error = ?health.last_error,
                    "NoF target excluded after consecutive heartbeat failures"
                );
            }
            _ => {}
        }
        health.snapshot().is_ok()
    }
}

#[derive(Default)]
struct NofTargetHealthState {
    last_success: Option<PersistentStorageBackendHealth>,
    last_error: Option<StoreError>,
    consecutive_failures: u32,
}

impl NofTargetHealthState {
    fn record(&mut self, result: Result<PersistentStorageBackendHealth>) {
        match result {
            Ok(health) => {
                self.last_success = Some(health);
                self.last_error = None;
                self.consecutive_failures = 0;
            }
            Err(error) => {
                self.last_error = Some(error);
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            }
        }
    }

    fn snapshot(&self) -> Result<PersistentStorageBackendHealth> {
        if self.consecutive_failures < TARGET_HEARTBEAT_FAILURE_THRESHOLD {
            if let Some(health) = &self.last_success {
                return Ok(health.clone());
            }
        }
        Err(self.last_error.clone().unwrap_or_else(|| {
            StoreError::Transport("NoF target has no successful heartbeat".to_string())
        }))
    }
}

pub(super) struct NofOwnerState {
    pub(super) local_runtime: ClientRuntimeId,
    metadata: Arc<dyn MetadataBackend>,
    live_clients: SharedLiveClientCache,
    target_set_fingerprint: String,
    pub(super) targets: BTreeMap<String, Arc<NofRuntimeTarget>>,
    owners: parking_lot::RwLock<BTreeMap<String, ClientRuntimeId>>,
    remote_unhealthy_targets: parking_lot::RwLock<BTreeSet<String>>,
    published_unhealthy_targets: parking_lot::Mutex<Option<BTreeSet<String>>>,
    released: AtomicBool,
}

impl NofOwnerState {
    pub(super) fn new(
        local_runtime: ClientRuntimeId,
        metadata: Arc<dyn MetadataBackend>,
        live_clients: SharedLiveClientCache,
        target_set_fingerprint: String,
        targets: BTreeMap<String, Arc<NofRuntimeTarget>>,
    ) -> Self {
        Self {
            local_runtime,
            metadata,
            live_clients,
            target_set_fingerprint,
            targets,
            owners: parking_lot::RwLock::new(BTreeMap::new()),
            remote_unhealthy_targets: parking_lot::RwLock::new(BTreeSet::new()),
            published_unhealthy_targets: parking_lot::Mutex::new(None),
            released: AtomicBool::new(false),
        }
    }

    pub(super) fn refresh_ownership(&self) {
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let leases = self.live_clients.lock().snapshot().unwrap_or_default();
        let next = assign_target_owners(
            self.targets.keys().map(String::as_str),
            &leases,
            &self.target_set_fingerprint,
        );
        let mut owners = self.owners.write();
        if *owners != next {
            let reassigned = next
                .iter()
                .filter(|(target_id, owner)| owners.get(*target_id) != Some(*owner))
                .count()
                + owners
                    .keys()
                    .filter(|target_id| !next.contains_key(*target_id))
                    .count();
            tracing::info!(
                runtime = %self.local_runtime,
                targets = next.len(),
                reassigned,
                "NoF target ownership updated"
            );
            *owners = next;
        }
        let remote_unhealthy = remote_unhealthy_targets(&owners, &leases, &self.local_runtime);
        *self.remote_unhealthy_targets.write() = remote_unhealthy;
    }

    pub(super) fn heartbeat_owned_targets(&self) {
        self.refresh_ownership();
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let unhealthy = self
            .locally_owned_target_ids()
            .into_iter()
            .filter(|target_id| {
                let target = self
                    .targets
                    .get(target_id)
                    .expect("ownership only contains registered NoF targets");
                !target.heartbeat(target_id)
            })
            .collect();
        self.publish_unhealthy_targets(unhealthy);
    }

    pub(super) fn owner_for(&self, target_id: &str) -> Option<ClientRuntimeId> {
        self.owners.read().get(target_id).cloned()
    }

    pub(super) fn locally_owned_target_ids(&self) -> Vec<String> {
        self.owners
            .read()
            .iter()
            .filter(|(_, owner)| owner == &&self.local_runtime)
            .map(|(target_id, _)| target_id.clone())
            .collect()
    }

    pub(super) fn health_snapshot(
        &self,
        target_id: &str,
    ) -> Result<PersistentStorageBackendHealth> {
        let owner = self.owner_for(target_id).ok_or_else(|| {
            StoreError::Transport(format!("NoF target {target_id} has no live owner"))
        })?;
        if owner != self.local_runtime {
            if self.remote_unhealthy_targets.read().contains(target_id) {
                return Err(StoreError::Transport(format!(
                    "NoF target {target_id} is unhealthy according to owner {owner}"
                )));
            }
            return Ok(PersistentStorageBackendHealth {
                capacity_bytes: None,
                available_bytes: None,
            });
        }
        self.targets
            .get(target_id)
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "NoF target backend {target_id} is not registered"
                ))
            })?
            .health_snapshot()
    }

    fn publish_unhealthy_targets(&self, unhealthy: BTreeSet<String>) {
        if self.published_unhealthy_targets.lock().as_ref() == Some(&unhealthy) {
            return;
        }
        let result = {
            let _guard = nof_managed_label_lock().lock();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            let Some(mut lease) = self
                .metadata
                .get_client_lease(&self.local_runtime)
                .ok()
                .flatten()
            else {
                return;
            };
            set_unhealthy_target_label(&mut lease, &unhealthy);
            self.metadata.upsert_client_lease(&lease).map(|()| lease)
        };
        match result {
            Ok(lease) => {
                replace_cached_lease(&self.live_clients, &lease);
                *self.published_unhealthy_targets.lock() = Some(unhealthy);
            }
            Err(error) => {
                tracing::warn!(
                    runtime = %self.local_runtime,
                    error = %error,
                    "failed to publish NoF target health snapshot"
                );
            }
        }
    }

    pub(super) fn release(&self) {
        if self.released.swap(true, Ordering::AcqRel) {
            return;
        }

        // Withdraw only the NoF-manager capability. The Client remains live
        // long enough to finish the rest of its normal shutdown cleanup.
        let released_lease = {
            let _guard = nof_managed_label_lock().lock();
            self.metadata
                .get_client_lease(&self.local_runtime)
                .ok()
                .flatten()
                .or_else(|| {
                    self.live_clients
                        .lock()
                        .snapshot()
                        .unwrap_or_default()
                        .into_iter()
                        .find(|lease| lease.runtime == self.local_runtime)
                })
                .map(|mut lease| {
                    for label in NOF_MANAGED_LABELS {
                        lease.endpoints.labels.remove(label);
                    }
                    let error = self.metadata.upsert_client_lease(&lease).err();
                    (lease, error)
                })
        };
        if let Some((lease, error)) = released_lease {
            replace_cached_lease(&self.live_clients, &lease);
            if let Some(error) = error {
                tracing::warn!(
                    runtime = %self.local_runtime,
                    error = %error,
                    "failed to withdraw NoF target ownership during shutdown"
                );
            }
        }
        self.owners.write().clear();
        self.remote_unhealthy_targets.write().clear();
    }
}

fn nof_managed_label_lock() -> &'static parking_lot::Mutex<()> {
    static LOCK: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| parking_lot::Mutex::new(()))
}

pub(in crate::client) fn upsert_client_lease_preserving_nof_labels(
    metadata: &dyn MetadataBackend,
    lease: &mut ClientLease,
) -> Result<()> {
    if NOF_MANAGED_LABELS
        .iter()
        .all(|label| !lease.endpoints.labels.contains_key(*label))
    {
        return metadata.upsert_client_lease(lease);
    }
    let _guard = nof_managed_label_lock().lock();
    preserve_nof_managed_labels(metadata, lease);
    metadata.upsert_client_lease(lease)
}

fn set_unhealthy_target_label(lease: &mut ClientLease, unhealthy: &BTreeSet<String>) {
    let encoded =
        serde_json::to_string(unhealthy).expect("serializing a set of NoF target IDs cannot fail");
    lease
        .endpoints
        .labels
        .insert(NOF_UNHEALTHY_TARGETS_LABEL.to_string(), encoded);
}

fn remote_unhealthy_targets(
    owners: &BTreeMap<String, ClientRuntimeId>,
    leases: &[ClientLease],
    local_runtime: &ClientRuntimeId,
) -> BTreeSet<String> {
    let unhealthy_by_owner = leases
        .iter()
        .map(|lease| {
            let unhealthy = lease
                .endpoints
                .labels
                .get(NOF_UNHEALTHY_TARGETS_LABEL)
                .and_then(|encoded| serde_json::from_str::<BTreeSet<String>>(encoded).ok());
            (&lease.runtime, unhealthy)
        })
        .collect::<BTreeMap<_, _>>();
    owners
        .iter()
        .filter_map(|(target_id, owner)| {
            if owner == local_runtime {
                return None;
            }
            match unhealthy_by_owner.get(owner) {
                Some(Some(unhealthy)) if !unhealthy.contains(target_id) => None,
                _ => Some(target_id.clone()),
            }
        })
        .collect()
}

fn replace_cached_lease(live_clients: &SharedLiveClientCache, updated: &ClientLease) {
    let mut cache = live_clients.lock();
    if let Some(mut leases) = cache.snapshot() {
        if let Some(lease) = leases
            .iter_mut()
            .find(|lease| lease.runtime == updated.runtime)
        {
            *lease = updated.clone();
        }
        cache.store(leases);
    }
}

fn preserve_nof_managed_labels(metadata: &dyn MetadataBackend, lease: &mut ClientLease) {
    let Ok(Some(current)) = metadata.get_client_lease(&lease.runtime) else {
        return;
    };
    for key in NOF_MANAGED_LABELS {
        match current.endpoints.labels.get(key) {
            Some(value) => {
                lease
                    .endpoints
                    .labels
                    .insert(key.to_string(), value.clone());
            }
            None => {
                lease.endpoints.labels.remove(key);
            }
        }
    }
}

fn assign_target_owners<'a>(
    target_ids: impl Iterator<Item = &'a str>,
    leases: &[ClientLease],
    target_set_fingerprint: &str,
) -> BTreeMap<String, ClientRuntimeId> {
    // LiveClientCache already removes expired leases and older active epochs.
    let candidates = leases
        .iter()
        .filter(|lease| {
            lease.state == ClientLifecycleState::Active
                && lease
                    .endpoints
                    .labels
                    .get(NOF_TARGET_SET_LABEL)
                    .is_some_and(|value| value == target_set_fingerprint)
        })
        .collect::<Vec<_>>();
    target_ids
        .filter_map(|target_id| {
            candidates
                .iter()
                .map(|owner| {
                    (
                        stable_rendezvous_score(
                            &["nof-target-owner", target_set_fingerprint, target_id],
                            &owner.runtime.stable_id,
                        ),
                        owner,
                    )
                })
                .max_by(|(left_score, left), (right_score, right)| {
                    left_score
                        .cmp(right_score)
                        .then_with(|| right.runtime.cmp(&left.runtime))
                })
                .map(|(_, owner)| owner)
                .map(|owner| (target_id.to_string(), owner.runtime.clone()))
        })
        .collect()
}

#[derive(Default)]
pub(super) struct NofHeartbeatMonitor {
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl NofHeartbeatMonitor {
    pub(super) fn disabled() -> Self {
        Self::default()
    }

    pub(super) fn start(state: Arc<NofOwnerState>) -> Result<Self> {
        let (shutdown, shutdown_rx) = std::sync::mpsc::channel();
        let stable_id = state.local_runtime.stable_id.0.clone();
        let initial_delay = Duration::from_millis(crate::client::stable_phase_spread_ms(
            &stable_id,
            TARGET_HEARTBEAT_INTERVAL.as_millis() as u64,
            "nof-owner-heartbeat",
        ));
        let thread = thread::Builder::new()
            .name(format!("nof-owner-{stable_id}"))
            .spawn(move || {
                if !matches!(
                    shutdown_rx.recv_timeout(initial_delay),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                ) {
                    return;
                }
                loop {
                    state.heartbeat_owned_targets();
                    match shutdown_rx.recv_timeout(TARGET_HEARTBEAT_INTERVAL) {
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!("failed to start NoF owner worker: {error}"))
            })?;
        Ok(Self {
            shutdown: Some(shutdown),
            thread: Some(thread),
        })
    }
}

impl Drop for NofHeartbeatMonitor {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicUsize;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientStableId, ColdBackingRoute, CompatibilityDescriptor,
    };

    use super::*;
    use crate::client::LiveClientCache;

    struct CountingHealthBackend {
        calls: AtomicUsize,
        fail: AtomicBool,
    }

    impl CountingHealthBackend {
        fn new(fail: bool) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                fail: AtomicBool::new(fail),
            }
        }
    }

    impl PersistentStorageBackend for CountingHealthBackend {
        fn health(&self) -> Result<PersistentStorageBackendHealth> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if self.fail.load(Ordering::Relaxed) {
                return Err(StoreError::Transport("target offline".to_string()));
            }
            Ok(PersistentStorageBackendHealth {
                capacity_bytes: Some(100),
                available_bytes: Some(75),
            })
        }

        fn put_object(
            &self,
            _backing: &ColdBackingRoute,
            _payload: &[u8],
        ) -> Result<ColdBackingRoute> {
            unreachable!("owner test does not write objects")
        }

        fn get_object(&self, _backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
            unreachable!("owner test does not read objects")
        }

        fn delete_object(&self, _backing: &ColdBackingRoute) -> Result<bool> {
            unreachable!("owner test does not delete objects")
        }

        fn put_pending_source(&self, _backing: &ColdBackingRoute, _payload: &[u8]) -> Result<()> {
            unreachable!("owner test does not stage objects")
        }

        fn get_pending_source(&self, _backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
            unreachable!("owner test does not read staged objects")
        }

        fn delete_pending_source(&self, _backing: &ColdBackingRoute) -> Result<bool> {
            unreachable!("owner test does not delete staged objects")
        }
    }

    fn runtime_target(backend: Arc<CountingHealthBackend>) -> NofRuntimeTarget {
        NofRuntimeTarget::new(backend)
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

    fn assigned(
        target_ids: &[String],
        leases: &[ClientLease],
    ) -> BTreeMap<String, ClientRuntimeId> {
        assign_target_owners(
            target_ids.iter().map(String::as_str),
            leases,
            "same-target-set",
        )
    }

    #[test]
    fn request_health_snapshot_never_probes_backend_and_starts_unknown() {
        let backend = Arc::new(CountingHealthBackend::new(false));
        let target = runtime_target(backend.clone());
        for _ in 0..100 {
            assert!(target.health_snapshot().is_err());
        }
        assert_eq!(backend.calls.load(Ordering::Relaxed), 0);
        target.heartbeat("nof-a");
        assert_eq!(backend.calls.load(Ordering::Relaxed), 1);
        assert_eq!(target.health_snapshot().unwrap().available_bytes, Some(75));
    }

    #[test]
    fn heartbeat_fences_after_threshold_and_recovers_on_success() {
        let backend = Arc::new(CountingHealthBackend::new(false));
        let target = runtime_target(backend.clone());
        target.heartbeat("nof-a");
        backend.fail.store(true, Ordering::Relaxed);
        for _ in 1..TARGET_HEARTBEAT_FAILURE_THRESHOLD {
            target.heartbeat("nof-a");
            assert!(target.health_snapshot().is_ok());
        }
        target.heartbeat("nof-a");
        assert!(target.health_snapshot().is_err());
        backend.fail.store(false, Ordering::Relaxed);
        target.heartbeat("nof-a");
        assert!(target.health_snapshot().is_ok());
    }

    #[test]
    fn owner_assignment_is_stable_and_only_uses_matching_active_clients() {
        let target_ids = (0..256)
            .map(|index| format!("nof-{index:04}"))
            .collect::<Vec<_>>();
        let a = owner_lease("client-a", 1, "same-target-set");
        let b = owner_lease("client-b", 1, "same-target-set");
        let c = owner_lease("client-c", 1, "same-target-set");
        let wrong_set = owner_lease("client-wrong", 1, "different-target-set");
        let mut standby = owner_lease("client-standby", 1, "same-target-set");
        standby.state = ClientLifecycleState::Standby;
        let forward = assigned(
            &target_ids,
            &[a.clone(), b.clone(), c.clone(), wrong_set, standby],
        );
        let reverse = assigned(&target_ids, &[c.clone(), b.clone(), a.clone()]);
        assert_eq!(forward, reverse);
        assert!(forward.values().all(|owner| {
            ["client-a", "client-b", "client-c"].contains(&owner.stable_id.0.as_str())
        }));
        let before = assigned(&target_ids, &[a.clone(), b.clone(), c.clone()]);
        let after = assigned(&target_ids, &[a, c]);
        for target_id in &target_ids {
            let previous = &before[target_id];
            if previous.stable_id.0 != "client-b" {
                assert_eq!(after[target_id], *previous);
            }
        }
    }

    #[test]
    fn rendezvous_owner_load_is_even_over_many_targets() {
        let target_ids = (0..4096)
            .map(|index| format!("nof-{index:04}"))
            .collect::<Vec<_>>();
        let leases = [
            owner_lease("client-a", 1, "same-target-set"),
            owner_lease("client-b", 1, "same-target-set"),
            owner_lease("client-c", 1, "same-target-set"),
            owner_lease("client-d", 1, "same-target-set"),
        ];
        let owners = assigned(&target_ids, &leases);
        let mut counts = BTreeMap::<String, usize>::new();
        for owner in owners.values() {
            *counts.entry(owner.stable_id.0.clone()).or_default() += 1;
        }
        assert_eq!(counts.len(), leases.len());
        assert!(counts.values().all(|count| (800..=1250).contains(count)));
    }

    #[test]
    fn heartbeat_probes_only_targets_owned_by_the_local_client() {
        let local = owner_lease("client-a", 1, "same-target-set");
        let remote = owner_lease("client-b", 1, "same-target-set");
        let live_clients = Arc::new(parking_lot::Mutex::new(LiveClientCache::default()));
        live_clients
            .lock()
            .store(vec![local.clone(), remote.clone()]);
        let mut targets = BTreeMap::new();
        let mut backends = BTreeMap::new();
        for index in 0..64 {
            let target_id = format!("nof-{index:04}");
            let backend = Arc::new(CountingHealthBackend::new(false));
            targets.insert(target_id.clone(), Arc::new(runtime_target(backend.clone())));
            backends.insert(target_id, backend);
        }
        let state = NofOwnerState::new(
            local.runtime.clone(),
            Arc::new(InMemoryMetadataBackend::new()),
            live_clients,
            "same-target-set".to_string(),
            targets,
        );
        state.heartbeat_owned_targets();
        let local_targets = state
            .locally_owned_target_ids()
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert!(!local_targets.is_empty());
        assert!(local_targets.len() < backends.len());
        for (target_id, backend) in backends {
            assert_eq!(
                backend.calls.load(Ordering::Relaxed),
                usize::from(local_targets.contains(&target_id))
            );
        }
    }

    #[test]
    fn graceful_release_withdraws_nof_manager_label() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let template = owner_lease("client-a", 0, "same-target-set");
        let runtime = metadata
            .allocate_client_lease(&template)
            .expect("lease allocation should succeed");
        let lease = metadata
            .get_client_lease(&runtime)
            .expect("lease lookup should succeed")
            .expect("allocated lease should exist");
        let mut stale_heartbeat = lease.clone();
        let live_clients = Arc::new(parking_lot::Mutex::new(LiveClientCache::default()));
        live_clients.lock().store(vec![lease]);
        let target_id = "nof-released".to_string();
        let backend = Arc::new(CountingHealthBackend::new(true));
        let state = NofOwnerState::new(
            runtime.clone(),
            metadata.clone(),
            live_clients,
            "same-target-set".to_string(),
            BTreeMap::from([(target_id, Arc::new(NofRuntimeTarget::new(backend)))]),
        );
        for _ in 0..TARGET_HEARTBEAT_FAILURE_THRESHOLD {
            state.heartbeat_owned_targets();
        }
        state.release();
        *state.published_unhealthy_targets.lock() = None;
        state.publish_unhealthy_targets(BTreeSet::new());
        let released = metadata
            .get_client_lease(&runtime)
            .expect("released lease lookup should succeed")
            .expect("released client remains live for normal cleanup");
        assert!(!released.endpoints.labels.contains_key(NOF_TARGET_SET_LABEL));
        assert!(!released
            .endpoints
            .labels
            .contains_key(NOF_UNHEALTHY_TARGETS_LABEL));
        assert_eq!(released.state, ClientLifecycleState::Active);

        upsert_client_lease_preserving_nof_labels(metadata.as_ref(), &mut stale_heartbeat)
            .expect("stale heartbeat should preserve the ownership withdrawal");
        assert!(!stale_heartbeat
            .endpoints
            .labels
            .contains_key(NOF_TARGET_SET_LABEL));
        let persisted = metadata.get_client_lease(&runtime).unwrap().unwrap();
        assert!(!persisted
            .endpoints
            .labels
            .contains_key(NOF_UNHEALTHY_TARGETS_LABEL));
        assert!(!persisted
            .endpoints
            .labels
            .contains_key(NOF_TARGET_SET_LABEL));
    }

    #[test]
    fn owner_publishes_failed_target_for_remote_request_filtering() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let template = owner_lease("client-a", 0, "same-target-set");
        let runtime = metadata
            .allocate_client_lease(&template)
            .expect("lease allocation should succeed");
        let lease = metadata
            .get_client_lease(&runtime)
            .expect("lease lookup should succeed")
            .expect("allocated lease should exist");
        let live_clients = Arc::new(parking_lot::Mutex::new(LiveClientCache::default()));
        live_clients.lock().store(vec![lease]);
        let target_id = "nof-failed".to_string();
        let backend = Arc::new(CountingHealthBackend::new(true));
        let state = NofOwnerState::new(
            runtime.clone(),
            metadata.clone(),
            live_clients,
            "same-target-set".to_string(),
            BTreeMap::from([(target_id.clone(), Arc::new(NofRuntimeTarget::new(backend)))]),
        );

        for _ in 0..TARGET_HEARTBEAT_FAILURE_THRESHOLD {
            state.heartbeat_owned_targets();
        }

        let published = metadata
            .get_client_lease(&runtime)
            .expect("published lease lookup should succeed")
            .expect("published lease should exist");
        let encoded = published
            .endpoints
            .labels
            .get(NOF_UNHEALTHY_TARGETS_LABEL)
            .expect("failed target should be published");
        let unhealthy = serde_json::from_str::<BTreeSet<String>>(encoded)
            .expect("health label should be valid JSON");
        assert_eq!(unhealthy, BTreeSet::from([target_id.clone()]));

        let owners = BTreeMap::from([(target_id.clone(), runtime)]);
        let remote_runtime = owner_lease("client-b", 1, "same-target-set").runtime;
        assert!(remote_unhealthy_targets(
            &owners,
            std::slice::from_ref(&published),
            &remote_runtime
        )
        .contains(&target_id));

        let mut unknown = published.clone();
        unknown.endpoints.labels.remove(NOF_UNHEALTHY_TARGETS_LABEL);
        assert!(
            remote_unhealthy_targets(&owners, &[unknown.clone()], &remote_runtime)
                .contains(&target_id)
        );
        unknown.endpoints.labels.insert(
            NOF_UNHEALTHY_TARGETS_LABEL.to_string(),
            "not-json".to_string(),
        );
        assert!(
            remote_unhealthy_targets(&owners, &[unknown], &remote_runtime).contains(&target_id)
        );
    }
}
