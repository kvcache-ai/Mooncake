use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_store_client::{
    record_heartbeat_health, stable_phase_spread_ms, GetRequest, HealthChannel, HealthUpdate,
    MooncakeCompatibilityFacade, MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef,
    OperationTracker, ReplicationPolicy, StoreClient,
};
use mooncake_store_core::{
    ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, HandoffKind, HandoffPlan,
    StoreError,
};
use parking_lot::Mutex;
use tokio::runtime::Runtime;
use tracing::{info, warn};

use crate::config::CompatTimeoutConfig;
use crate::dummy_service::pb;
use crate::hot_cache::{HotCacheHandle, HotCacheKey, LocalHotCache};
use crate::shm::{DummyClientId, OwnedMappedRegion};

type RegionKey = (u64, u64, u64);
type RegionMap = BTreeMap<RegionKey, OwnedMappedRegion>;
type TrackedKey = (String, String);
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 30_000;

pub(crate) struct DispatcherState {
    tracked_keys: BTreeSet<TrackedKey>,
}

#[derive(Clone, Copy, Debug, Default)]
struct HeartbeatHealthState {
    consecutive_failures: u64,
    last_success_ms: u64,
}

struct HeartbeatLoopHandle {
    stop: Arc<AtomicBool>,
    thread: JoinHandle<()>,
}

#[derive(Clone)]
struct HealthPublisher {
    client: Arc<StoreClient>,
    health: Arc<HealthChannel>,
    closed: Arc<AtomicBool>,
    health_inflight: Arc<AtomicBool>,
    health_timeout: Duration,
    runtime: String,
    executor: Arc<Runtime>,
    heartbeat_health: Arc<Mutex<HeartbeatHealthState>>,
}

pub struct StoreDispatcher {
    client: Arc<StoreClient>,
    health: Arc<HealthChannel>,
    regions: Arc<Mutex<RegionMap>>,
    state: Arc<Mutex<DispatcherState>>,
    closed: Arc<AtomicBool>,
    health_inflight: Arc<AtomicBool>,
    heartbeat_loop: Arc<Mutex<Option<HeartbeatLoopHandle>>>,
    request_timeout: Duration,
    registration_timeout_override: Option<Duration>,
    startup_timeout: Duration,
    health_timeout: Duration,
    runtime: String,
    executor: Arc<Runtime>,
    heartbeat_health: Arc<Mutex<HeartbeatHealthState>>,
    hot_cache: Option<Arc<LocalHotCache>>,
    #[cfg_attr(not(test), allow(dead_code))]
    compat_scope: String,
}

impl StoreDispatcher {
    pub fn spawn(client: StoreClient, _thread_name: impl Into<String>) -> Result<Self, StoreError> {
        Self::spawn_with_timeout_config(client, _thread_name, CompatTimeoutConfig::from_env())
    }

    pub fn spawn_with_heartbeat_timeout(
        client: StoreClient,
        _thread_name: impl Into<String>,
        heartbeat_timeout: Duration,
    ) -> Result<Self, StoreError> {
        let timeouts = CompatTimeoutConfig::from_env().with_heartbeat_timeout(heartbeat_timeout);
        Self::spawn_with_timeout_config(client, _thread_name, timeouts)
    }

    pub fn spawn_with_timeouts(
        client: StoreClient,
        _thread_name: impl Into<String>,
        request_timeout: Duration,
        heartbeat_timeout: Duration,
    ) -> Result<Self, StoreError> {
        let timeouts = CompatTimeoutConfig::from_env()
            .with_request_timeout(request_timeout)
            .with_heartbeat_timeout(heartbeat_timeout);
        Self::spawn_with_timeout_config(client, _thread_name, timeouts)
    }

    pub fn spawn_with_timeout_config(
        client: StoreClient,
        _thread_name: impl Into<String>,
        timeouts: CompatTimeoutConfig,
    ) -> Result<Self, StoreError> {
        Self::spawn_with_timeout_config_and_scope(client, _thread_name, timeouts, "default")
    }

    pub fn spawn_with_timeout_config_and_scope(
        client: StoreClient,
        _thread_name: impl Into<String>,
        timeouts: CompatTimeoutConfig,
        compat_scope: impl Into<String>,
    ) -> Result<Self, StoreError> {
        Self::new_with_shared_client(Arc::new(client), timeouts, compat_scope.into())
    }

    pub(crate) fn fork_with_scope(
        &self,
        compat_scope: impl Into<String>,
    ) -> Result<Self, StoreError> {
        Self::new_with_shared_client(
            self.client.clone(),
            CompatTimeoutConfig {
                request_timeout: self.request_timeout,
                startup_timeout_override: self.registration_timeout_override,
                heartbeat_timeout: self.health_timeout,
                transfer_stall_timeout: CompatTimeoutConfig::from_env().transfer_stall_timeout,
                dummy_rpc_timeout: self.request_timeout,
            },
            compat_scope.into(),
        )
    }

    fn new_with_shared_client(
        client: Arc<StoreClient>,
        timeouts: CompatTimeoutConfig,
        compat_scope: String,
    ) -> Result<Self, StoreError> {
        let startup_timeout =
            timeouts.registration_timeout_for_bytes(client.local_memory_registration_bytes());
        let runtime = client.runtime_id().to_string();
        let health = Arc::new(client.health_channel());
        let hot_cache = LocalHotCache::from_env()?.map(Arc::new);
        let executor = Arc::new(Runtime::new().map_err(|error| {
            StoreError::Transport(format!("dispatcher runtime should initialize: {error}"))
        })?);
        record_heartbeat_health(&runtime, 0, 0);
        Ok(Self {
            client,
            health,
            regions: Arc::new(Mutex::new(BTreeMap::new())),
            state: Arc::new(Mutex::new(DispatcherState {
                tracked_keys: BTreeSet::new(),
            })),
            closed: Arc::new(AtomicBool::new(false)),
            health_inflight: Arc::new(AtomicBool::new(false)),
            heartbeat_loop: Arc::new(Mutex::new(None)),
            request_timeout: timeouts.request_timeout.max(Duration::from_millis(1)),
            registration_timeout_override: timeouts.startup_timeout_override,
            startup_timeout: startup_timeout.max(Duration::from_millis(1)),
            health_timeout: timeouts.heartbeat_timeout.max(Duration::from_millis(1)),
            runtime,
            executor,
            heartbeat_health: Arc::new(Mutex::new(HeartbeatHealthState::default())),
            hot_cache,
            compat_scope,
        })
    }

    pub fn start_heartbeat_loop(
        &self,
        lease_ttl_ms: u64,
        requested_interval_ms: Option<u64>,
    ) -> Result<(), StoreError> {
        let mut loop_guard = self.heartbeat_loop.lock();
        if loop_guard.is_some() {
            return Ok(());
        }

        let lease_ttl_ms = lease_ttl_ms.max(1);
        let interval_ms = effective_heartbeat_interval(
            requested_interval_ms.unwrap_or(DEFAULT_HEARTBEAT_INTERVAL_MS),
            lease_ttl_ms,
        );
        let initial_delay_ms = initial_heartbeat_delay_ms(&self.runtime, interval_ms);
        let stop = Arc::new(AtomicBool::new(false));
        let publisher = self.health_publisher();
        let thread_stop = stop.clone();
        let thread_name = format!("{}-heartbeat", self.runtime.replace(':', "-"));
        let thread = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_heartbeat_loop(
                    publisher,
                    thread_stop,
                    lease_ttl_ms,
                    interval_ms,
                    initial_delay_ms,
                );
            })
            .map_err(|error| {
                StoreError::Transport(format!("store dispatcher heartbeat thread failed: {error}"))
            })?;

        *loop_guard = Some(HeartbeatLoopHandle { stop, thread });
        Ok(())
    }

    pub fn stop_heartbeat_loop(&self) {
        let Some(handle) = self.heartbeat_loop.lock().take() else {
            return;
        };
        handle.stop.store(true, Ordering::SeqCst);
        let _ = handle.thread.join();
    }

    pub fn register_local_memory(&self) -> Result<(), StoreError> {
        let result =
            self.run_with_timeout("startup registration", self.startup_timeout, |client| {
                client.register_local_memory()
            });
        if result.is_ok() {
            let state = self.run(|client| Ok::<_, StoreError>(client.lifecycle_state()))?;
            self.health.sync_state(state);
        }
        result
    }

    pub fn heartbeat(&self, expires_at_ms: u64) -> Result<(), StoreError> {
        self.health_publisher().heartbeat(expires_at_ms)
    }

    pub fn activate(&self) -> Result<(), StoreError> {
        let update = self.prepare_state_update(ClientLifecycleState::Active, "activate");
        self.run_health_update("state update publish", update)
    }

    pub fn enter_standby(&self) -> Result<(), StoreError> {
        let update = self.prepare_state_update(ClientLifecycleState::Standby, "enter_standby");
        self.run_health_update("state update publish", update)
    }

    pub fn enter_draining(&self) -> Result<(), StoreError> {
        let update = self.prepare_state_update(ClientLifecycleState::Draining, "enter_draining");
        self.run_health_update("state update publish", update)
    }

    pub fn enter_offline(&self) -> Result<(), StoreError> {
        let update = self.prepare_state_update(ClientLifecycleState::Offline, "enter_offline");
        self.run_health_update("state update publish", update)
    }

    pub fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan, StoreError> {
        self.run(move |client| {
            client.plan_handoff(
                successor_epoch,
                kind,
                barrier_version,
                created_at_ms,
                deadline_ms,
            )
        })
    }

    pub fn activate_if_targeted_handoff(&self) -> Result<Option<HandoffPlan>, StoreError> {
        let current_state = self.health.snapshot_lease().state;
        let Some(plan) =
            self.run(move |client| client.targeted_handoff_plan_for_state(current_state))?
        else {
            return Ok(None);
        };
        self.activate()?;
        Ok(Some(plan))
    }

    pub fn find_hot_upgrade_successor(&self) -> Result<Option<ClientLease>, StoreError> {
        self.run(|client| client.find_hot_upgrade_successor())
    }

    pub fn runtime_state(
        &self,
        runtime: ClientRuntimeId,
    ) -> Result<Option<ClientLifecycleState>, StoreError> {
        self.run(move |client| client.runtime_state(&runtime))
    }

    pub fn evacuate_owned_replicas(&self) -> Result<usize, StoreError> {
        if self.health.snapshot_lease().state != ClientLifecycleState::Draining {
            self.enter_draining()?;
        }
        self.run(|client| client.evacuate_owned_replicas_when_draining())
    }

    pub fn evacuate_owned_replicas_to_runtime(
        &self,
        runtime: ClientRuntimeId,
    ) -> Result<usize, StoreError> {
        if self.health.snapshot_lease().state != ClientLifecycleState::Draining {
            self.enter_draining()?;
        }
        self.run(move |client| client.evacuate_owned_replicas_to_runtime_when_draining(&runtime))
    }

    pub fn register_buffer(&self, base_ptr: usize, len: usize) -> Result<(), StoreError> {
        let timeout = self.registration_timeout_for_bytes(len);
        self.run_with_timeout("buffer registration", timeout, move |client| {
            client.register_buffer(base_ptr as *mut c_void, len)
        })
    }

    pub fn unregister_buffer(&self, base_ptr: usize, len: usize) -> Result<(), StoreError> {
        let timeout = self.registration_timeout_for_bytes(len);
        self.run_with_timeout("buffer unregistration", timeout, move |client| {
            client.unregister_buffer(base_ptr as *mut c_void, len)
        })
    }

    pub async fn unregister_buffer_async(
        &self,
        base_ptr: usize,
        len: usize,
    ) -> Result<(), StoreError> {
        let timeout = self.registration_timeout_for_bytes(len);
        self.run_async_with_timeout("buffer unregistration", timeout, move |client| {
            client.unregister_buffer(base_ptr as *mut c_void, len)
        })
        .await
    }

    pub(crate) fn registration_timeout_for_bytes(&self, registration_bytes: usize) -> Duration {
        self.registration_timeout_override.unwrap_or_else(|| {
            CompatTimeoutConfig::default_registration_timeout_for_bytes(registration_bytes as u64)
        })
    }

    pub(crate) fn run<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&StoreClient) -> Result<T, StoreError> + Send + 'static,
    {
        self.executor.block_on(self.run_async(f))
    }

    pub(crate) fn run_with_timeout<T, F>(
        &self,
        context: &'static str,
        timeout: Duration,
        f: F,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&StoreClient) -> Result<T, StoreError> + Send + 'static,
    {
        self.executor
            .block_on(self.run_async_with_timeout(context, timeout, f))
    }

    pub(crate) async fn run_async<T, F>(&self, f: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&StoreClient) -> Result<T, StoreError> + Send + 'static,
    {
        self.run_async_with_timeout("request execution", self.request_timeout, f)
            .await
    }

    pub(crate) async fn run_async_with_timeout<T, F>(
        &self,
        context: &'static str,
        timeout: Duration,
        f: F,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&StoreClient) -> Result<T, StoreError> + Send + 'static,
    {
        self.ensure_open()?;
        let closed = self.closed.clone();
        let client = self.client.clone();
        self.await_blocking_with_timeout(context, timeout, move || {
            if closed.load(Ordering::SeqCst) {
                return Err(dispatcher_stopped());
            }
            f(client.as_ref())
        })
        .await
    }

    pub(crate) fn install_region(
        &self,
        client_id: DummyClientId,
        region_id: u64,
        mapped: OwnedMappedRegion,
    ) -> Option<OwnedMappedRegion> {
        self.regions
            .lock()
            .insert(region_key(client_id, region_id), mapped)
    }

    pub(crate) fn take_region(
        &self,
        client_id: DummyClientId,
        region_id: u64,
    ) -> Option<OwnedMappedRegion> {
        self.regions
            .lock()
            .remove(&region_key(client_id, region_id))
    }

    pub(crate) fn hot_cache_fd(&self) -> Result<Option<(OwnedFd, usize)>, StoreError> {
        match self.hot_cache.as_ref() {
            Some(cache) => cache.duplicate_shm_fd(),
            None => Ok(None),
        }
    }

    pub(crate) fn acquire_hot_cache(
        &self,
        request: pb::HotCacheAcquireRequest,
    ) -> pb::HotCacheAcquireReply {
        acquire_hot_cache_reply(self.hot_cache.as_ref(), self.client.as_ref(), request)
    }

    pub(crate) fn batch_acquire_hot_cache(
        &self,
        request: pb::BatchHotCacheAcquireRequest,
    ) -> pb::BatchHotCacheAcquireReply {
        let items = request
            .objects
            .into_iter()
            .map(|object| {
                acquire_hot_cache_reply(
                    self.hot_cache.as_ref(),
                    self.client.as_ref(),
                    pb::HotCacheAcquireRequest {
                        key: object.key,
                        tenant: object.tenant,
                    },
                )
            })
            .collect();
        pb::BatchHotCacheAcquireReply { items }
    }

    pub(crate) fn release_hot_cache(&self, request: pb::HotCacheReleaseRequest) -> i32 {
        let Some(cache) = self.hot_cache.as_ref() else {
            return -1;
        };
        cache.release(HotCacheHandle {
            block_id: request.block_id,
            generation: request.generation,
            offset: 0,
            len: 0,
        });
        0
    }

    pub(crate) fn batch_release_hot_cache(&self, request: pb::BatchHotCacheReleaseRequest) -> i32 {
        let Some(cache) = self.hot_cache.as_ref() else {
            return -1;
        };
        for handle in request.handles {
            cache.release(HotCacheHandle {
                block_id: handle.block_id,
                generation: handle.generation,
                offset: 0,
                len: 0,
            });
        }
        0
    }

    pub fn get_value(&self, key: String, tenant: Option<String>) -> Result<Vec<u8>, StoreError> {
        let hot_cache = self.hot_cache.clone();
        self.run(move |client| execute_get_value(client, hot_cache.as_ref(), tenant, key))
    }

    pub fn batch_get_values(
        &self,
        keys: Vec<String>,
        tenant: Option<String>,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        let hot_cache = self.hot_cache.clone();
        self.run(move |client| execute_batch_get_values(client, hot_cache.as_ref(), tenant, keys))
    }

    pub fn get_into_buffer(
        &self,
        key: String,
        tenant: Option<String>,
        buffer_ptr: usize,
        size: usize,
    ) -> Result<usize, StoreError> {
        let hot_cache = self.hot_cache.clone();
        self.run(move |client| {
            execute_get_into_value(client, hot_cache.as_ref(), tenant, key, buffer_ptr, size)
        })
    }

    pub fn batch_get_into_buffers(
        &self,
        items: Vec<(String, usize, usize)>,
        tenant: Option<String>,
    ) -> Result<Vec<i64>, StoreError> {
        let hot_cache = self.hot_cache.clone();
        self.run(move |client| {
            execute_batch_get_values_into(client, hot_cache.as_ref(), tenant, items)
        })
    }

    pub fn batch_get_into_multi_buffers_raw(
        &self,
        keys: Vec<String>,
        all_buffer_ptrs: Vec<Vec<usize>>,
        all_sizes: Vec<Vec<usize>>,
        tenant: Option<String>,
    ) -> Result<Vec<i64>, StoreError> {
        let hot_cache = self.hot_cache.clone();
        self.run(move |client| {
            execute_batch_get_values_into_multi(
                client,
                hot_cache.as_ref(),
                tenant,
                keys,
                all_buffer_ptrs,
                all_sizes,
            )
        })
    }

    pub fn invalidate_key(&self, key: String, tenant: Option<String>) {
        let Some(cache) = self.hot_cache.as_ref() else {
            return;
        };
        let tenant = normalized_tenant(self.client.as_ref(), tenant.as_deref().unwrap_or_default());
        cache.invalidate(&HotCacheKey::new(tenant, key));
    }

    pub fn invalidate_keys(&self, keys: Vec<String>, tenant: Option<String>) {
        let Some(cache) = self.hot_cache.as_ref() else {
            return;
        };
        let tenant = normalized_tenant(self.client.as_ref(), tenant.as_deref().unwrap_or_default());
        cache.invalidate_many(
            keys.into_iter()
                .map(|key| HotCacheKey::new(tenant.clone(), key)),
        );
    }

    #[cfg(test)]
    pub(crate) fn hot_cache_contains(&self, tenant: &str, key: &str) -> bool {
        self.hot_cache
            .as_ref()
            .is_some_and(|cache| cache.contains(&HotCacheKey::new(tenant, key)))
    }

    #[cfg(test)]
    pub(crate) fn hot_cache_contains_in_scope(&self, scope: &str, tenant: &str, key: &str) -> bool {
        scope == self.compat_scope && self.hot_cache_contains(tenant, key)
    }

    pub async fn put(&self, request: pb::PutRequest) -> Result<i32, StoreError> {
        let state = self.state.clone();
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| Ok(execute_put(client, hot_cache.as_ref(), &state, request)))
            .await
    }

    pub async fn get(&self, request: pb::GetRequest) -> Result<pb::GetReply, StoreError> {
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| Ok(execute_get(client, hot_cache.as_ref(), request)))
            .await
    }

    pub async fn batch_is_exist(
        &self,
        request: pb::BatchIsExistRequest,
    ) -> Result<Vec<i32>, StoreError> {
        self.run_async(move |client| Ok(execute_batch_is_exist(client, request)))
            .await
    }

    pub async fn batch_put_from(
        &self,
        request: pb::BatchPutFromRequest,
    ) -> Result<Vec<i32>, StoreError> {
        let regions = self.regions.clone();
        let state = self.state.clone();
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| {
            Ok(execute_batch_put_from(
                client,
                hot_cache.as_ref(),
                &regions,
                &state,
                request,
            ))
        })
        .await
    }

    pub async fn batch_put_from_multi_buffers(
        &self,
        request: pb::BatchPutFromMultiBuffersRequest,
    ) -> Result<Vec<i32>, StoreError> {
        let regions = self.regions.clone();
        let state = self.state.clone();
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| {
            Ok(execute_batch_put_from_multi_buffers(
                client,
                hot_cache.as_ref(),
                &regions,
                &state,
                request,
            ))
        })
        .await
    }

    pub async fn batch_get_into(
        &self,
        request: pb::BatchGetIntoRequest,
    ) -> Result<Vec<i64>, StoreError> {
        let regions = self.regions.clone();
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| {
            Ok(execute_batch_get_into(
                client,
                hot_cache.as_ref(),
                &regions,
                request,
            ))
        })
        .await
    }

    pub async fn batch_get_into_multi_buffers(
        &self,
        request: pb::BatchGetIntoMultiBuffersRequest,
    ) -> Result<Vec<i64>, StoreError> {
        let regions = self.regions.clone();
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| {
            Ok(execute_batch_get_into_multi_buffers(
                client,
                hot_cache.as_ref(),
                &regions,
                request,
            ))
        })
        .await
    }

    pub async fn remove_all(&self, force: bool) -> Result<(i32, i64), StoreError> {
        let state = self.state.clone();
        let hot_cache = self.hot_cache.clone();
        self.run_async(move |client| {
            Ok(execute_remove_all(
                client,
                hot_cache.as_ref(),
                &state,
                force,
            ))
        })
        .await
    }

    pub fn shutdown(&self) {
        self.stop_heartbeat_loop();
        self.closed.store(true, Ordering::SeqCst);
    }

    pub fn startup_timeout(&self) -> Duration {
        self.startup_timeout
    }

    async fn await_blocking_with_timeout<T, F>(
        &self,
        context: &'static str,
        timeout: Duration,
        f: F,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, StoreError> + Send + 'static,
    {
        /*
         * Async callers already run inside a Tokio runtime. Schedule blocking work on
         * that current runtime instead of bouncing through the dispatcher's private
         * runtime first.
         */
        let tracker = OperationTracker::new("compat_dispatcher_bridge")
            .scope(dispatcher_metric_scope(context));
        let result = match tokio::time::timeout(timeout, tokio::task::spawn_blocking(f)).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(StoreError::Transport(
                "store dispatcher worker failed".to_string(),
            )),
            Err(_) => Err(dispatcher_timeout(context, timeout)),
        };
        tracker.finish(&result, 0);
        result
    }

    fn ensure_open(&self) -> Result<(), StoreError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(dispatcher_stopped());
        }
        Ok(())
    }

    fn prepare_state_update(
        &self,
        next_state: ClientLifecycleState,
        operation: &'static str,
    ) -> HealthUpdate {
        self.client.sync_lifecycle_state(next_state);
        self.health.prepare_state_update(next_state, operation)
    }

    fn run_health_update(
        &self,
        context: &'static str,
        update: HealthUpdate,
    ) -> Result<(), StoreError> {
        run_health_update(
            self.closed.as_ref(),
            self.health_inflight.clone(),
            self.health_timeout,
            context,
            update,
            self.executor.clone(),
        )
    }

    fn health_publisher(&self) -> HealthPublisher {
        HealthPublisher {
            client: self.client.clone(),
            health: self.health.clone(),
            closed: self.closed.clone(),
            health_inflight: self.health_inflight.clone(),
            health_timeout: self.health_timeout,
            runtime: self.runtime.clone(),
            executor: self.executor.clone(),
            heartbeat_health: self.heartbeat_health.clone(),
        }
    }
}

impl HealthPublisher {
    fn heartbeat(&self, expires_at_ms: u64) -> Result<(), StoreError> {
        let heartbeat = self.health.prepare_heartbeat(expires_at_ms);
        let recovered_from_failures = {
            let heartbeat = self.heartbeat_health.lock();
            heartbeat.consecutive_failures > 0
        };
        let result = run_health_update(
            self.closed.as_ref(),
            self.health_inflight.clone(),
            self.health_timeout,
            "heartbeat publish",
            heartbeat,
            self.executor.clone(),
        );
        match result {
            Ok(()) => {
                if recovered_from_failures {
                    if let Err(error) = self.client.repair_local_metadata_after_heartbeat_recovery()
                    {
                        self.record_heartbeat_failure();
                        return Err(error);
                    }
                }
                self.record_heartbeat_success();
                Ok(())
            }
            Err(error) => {
                self.record_heartbeat_failure();
                Err(error)
            }
        }
    }

    fn record_heartbeat_success(&self) {
        let now_ms = current_time_ms();
        let mut heartbeat = self.heartbeat_health.lock();
        heartbeat.consecutive_failures = 0;
        heartbeat.last_success_ms = now_ms;
        record_heartbeat_health(&self.runtime, 0, now_ms);
    }

    fn record_heartbeat_failure(&self) {
        let mut heartbeat = self.heartbeat_health.lock();
        heartbeat.consecutive_failures = heartbeat.consecutive_failures.saturating_add(1);
        record_heartbeat_health(
            &self.runtime,
            heartbeat.consecutive_failures,
            heartbeat.last_success_ms,
        );
    }
}

fn run_health_update(
    closed: &AtomicBool,
    inflight: Arc<AtomicBool>,
    timeout: Duration,
    context: &'static str,
    update: HealthUpdate,
    executor: Arc<Runtime>,
) -> Result<(), StoreError> {
    if closed.load(Ordering::SeqCst) {
        return Err(dispatcher_stopped());
    }
    if inflight.swap(true, Ordering::SeqCst) {
        return Err(StoreError::Transport(format!(
            "store dispatcher {context} is still in flight"
        )));
    }
    let executor_for_task = executor.clone();
    executor.block_on(async move {
        match tokio::time::timeout(
            timeout,
            executor_for_task.spawn_blocking(move || {
                let _guard = InflightGuard(inflight);
                update.publish()
            }),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(StoreError::Transport(format!(
                "store dispatcher worker failed: {error}"
            ))),
            Err(_) => Err(dispatcher_timeout(context, timeout)),
        }
    })
}

struct InflightGuard(Arc<AtomicBool>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl Drop for StoreDispatcher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn execute_put(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    state: &Arc<Mutex<DispatcherState>>,
    request: pb::PutRequest,
) -> i32 {
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let result = match policy.as_ref() {
        Some(policy) => {
            client.put_in_tenant_with_policy(&tenant, &request.key, &request.value, policy)
        }
        None => client.put_in_tenant(&tenant, &request.key, &request.value),
    };
    if result.is_ok() {
        track_key(state, tenant.clone(), request.key.clone());
        invalidate_hot_cache(hot_cache, &tenant, &request.key);
        0
    } else {
        -1
    }
}

fn execute_get(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    request: pb::GetRequest,
) -> pb::GetReply {
    match execute_get_value(client, hot_cache, Some(request.tenant), request.key) {
        Ok(value) => pb::GetReply { status: 0, value },
        Err(_) => pb::GetReply {
            status: -1,
            value: Vec::new(),
        },
    }
}

fn execute_get_value(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    tenant_hint: Option<String>,
    key: String,
) -> Result<Vec<u8>, StoreError> {
    let tenant = normalized_tenant(client, tenant_hint.as_deref().unwrap_or_default());
    let cache_key = HotCacheKey::new(tenant.clone(), key.clone());
    if let Some(cache) = hot_cache {
        if let Some(value) = cache.get(&cache_key) {
            return Ok(value);
        }
    }
    let value = client.get_in_tenant(&tenant, &key)?;
    insert_hot_cache(hot_cache, cache_key, &value);
    Ok(value)
}

fn execute_batch_get_values(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    tenant_hint: Option<String>,
    keys: Vec<String>,
) -> Result<Vec<Vec<u8>>, StoreError> {
    let tenant = normalized_tenant(client, tenant_hint.as_deref().unwrap_or_default());
    let mut results = vec![Vec::new(); keys.len()];
    let mut misses = Vec::new();
    let mut objects = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let cache_key = HotCacheKey::new(tenant.clone(), key.clone());
        if let Some(cache) = hot_cache {
            if let Some(value) = cache.get(&cache_key) {
                results[index] = value;
                continue;
            }
        }
        objects.push(ObjectRef::new(key.as_str()).tenant(tenant.as_str()));
        misses.push((index, key.clone()));
    }
    if !objects.is_empty() {
        for ((index, key), value) in misses.into_iter().zip(client.batch_get(&objects)?) {
            insert_hot_cache(
                hot_cache,
                HotCacheKey::new(tenant.clone(), key.clone()),
                &value,
            );
            results[index] = value;
        }
    }
    Ok(results)
}

fn execute_get_into_value(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    tenant_hint: Option<String>,
    key: String,
    buffer_ptr: usize,
    size: usize,
) -> Result<usize, StoreError> {
    let tenant = normalized_tenant(client, tenant_hint.as_deref().unwrap_or_default());
    let target = unsafe { std::slice::from_raw_parts_mut(buffer_ptr as *mut u8, size) };
    let cache_key = HotCacheKey::new(tenant.clone(), key.clone());
    if let Some(cache) = hot_cache {
        if let Some(hit) = cache.copy_into(&cache_key, target)? {
            return Ok(hit);
        }
    }
    let copied = client.get_into_in_tenant(&tenant, &key, target)?;
    insert_hot_cache(hot_cache, cache_key, &target[..copied]);
    Ok(copied)
}

fn execute_get_into_multi_value(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    tenant: &str,
    key: String,
    buffer_ptrs: Vec<usize>,
    sizes: Vec<usize>,
) -> Result<usize, StoreError> {
    let cache_key = HotCacheKey::new(tenant.to_string(), key.clone());
    let mut buffers = buffer_ptrs
        .iter()
        .zip(sizes.iter())
        .map(|(buffer_ptr, size)| unsafe {
            std::slice::from_raw_parts_mut(*buffer_ptr as *mut u8, *size)
        })
        .collect::<Vec<_>>();
    if let Some(cache) = hot_cache {
        if let Some(hit) = cache.copy_into_multi(&cache_key, buffers.as_mut_slice())? {
            return Ok(hit);
        }
    }
    let source_ptrs = buffers
        .iter()
        .map(|buffer| (buffer.as_ptr() as usize, buffer.len()))
        .collect::<Vec<_>>();
    let copied = {
        let mut request = MultiBufferGetRequest::new(key.as_str(), buffers.as_mut_slice());
        request = request.tenant(tenant);
        let mut requests = vec![request];
        client
            .batch_get_into_multi_buffers(requests.as_mut_slice())?
            .into_iter()
            .next()
            .unwrap_or_default()
    };
    let sources = source_ptrs
        .iter()
        .map(|(ptr, len)| unsafe { std::slice::from_raw_parts(*ptr as *const u8, *len) })
        .collect::<Vec<_>>();
    insert_hot_cache_from_slices(hot_cache, cache_key, &sources, copied);
    Ok(copied)
}

fn execute_batch_is_exist(client: &StoreClient, request: pb::BatchIsExistRequest) -> Vec<i32> {
    let tenants = request
        .objects
        .iter()
        .map(|object| normalized_tenant(client, &object.tenant))
        .collect::<Vec<_>>();
    let refs = request
        .objects
        .iter()
        .zip(tenants.iter())
        .map(|(object, tenant)| ObjectRef::new(object.key.as_str()).tenant(tenant.as_str()))
        .collect::<Vec<_>>();
    client
        .batch_is_exist(&refs)
        .map(|items| items.into_iter().map(i32::from).collect())
        .unwrap_or_else(|_| vec![-1; refs.len()])
}

fn execute_batch_put_from(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    regions: &Arc<Mutex<RegionMap>>,
    state: &Arc<Mutex<DispatcherState>>,
    request: pb::BatchPutFromRequest,
) -> Vec<i32> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let mut statuses = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let status = match item.buffer.as_ref() {
            Some(buffer) => {
                let regions = regions.lock();
                match resolve_shared_slice(&regions, client_id, buffer) {
                    Ok(payload) => {
                        let result = match policy.as_ref() {
                            Some(policy) => client
                                .put_in_tenant_with_policy(&tenant, &item.key, payload, policy),
                            None => client.put_in_tenant(&tenant, &item.key, payload),
                        };
                        if result.is_ok() {
                            0
                        } else {
                            -1
                        }
                    }
                    Err(_) => -1,
                }
            }
            None => -1,
        };
        if status == 0 {
            track_key(state, tenant.clone(), item.key.clone());
            invalidate_hot_cache(hot_cache, &tenant, &item.key);
        }
        statuses.push(status);
    }
    statuses
}

fn execute_batch_put_from_multi_buffers(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    regions: &Arc<Mutex<RegionMap>>,
    state: &Arc<Mutex<DispatcherState>>,
    request: pb::BatchPutFromMultiBuffersRequest,
) -> Vec<i32> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let policy = proto_policy(&request.replication);
    let mut statuses = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let status = match item.buffers.as_ref() {
            Some(group) => {
                let regions = regions.lock();
                let mut buffers = Vec::with_capacity(group.buffers.len());
                let mut invalid = false;
                for buffer in &group.buffers {
                    match resolve_shared_slice(&regions, client_id, buffer) {
                        Ok(slice) => buffers.push(slice),
                        Err(_) => {
                            invalid = true;
                            break;
                        }
                    }
                }
                if invalid {
                    -1
                } else {
                    let mut request =
                        MultiBufferPutRequest::new(item.key.as_str(), buffers.as_slice());
                    request = request.tenant(tenant.as_str());
                    if let Some(policy) = policy.clone() {
                        request = request.replication(policy);
                    }
                    match client.batch_put_from_multi_buffers(std::slice::from_ref(&request)) {
                        Ok(_) => 0,
                        Err(_) => -1,
                    }
                }
            }
            None => -1,
        };
        if status == 0 {
            track_key(state, tenant.clone(), item.key.clone());
            invalidate_hot_cache(hot_cache, &tenant, &item.key);
        }
        statuses.push(status);
    }
    statuses
}

fn execute_batch_get_into(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    regions: &Arc<Mutex<RegionMap>>,
    request: pb::BatchGetIntoRequest,
) -> Vec<i64> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let mut lengths = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let length = match item.buffer.as_ref() {
            Some(buffer) => {
                let regions = regions.lock();
                match resolve_shared_slice_mut(&regions, client_id, buffer) {
                    Ok(target) => {
                        let cache_key = HotCacheKey::new(tenant.clone(), item.key.clone());
                        if let Some(cache) = hot_cache {
                            match cache.copy_into(&cache_key, target) {
                                Ok(Some(size)) => {
                                    lengths.push(size as i64);
                                    continue;
                                }
                                Ok(None) => {}
                                Err(_) => {
                                    lengths.push(-1);
                                    continue;
                                }
                            }
                        }
                        match client.get_into_in_tenant(&tenant, &item.key, target) {
                            Ok(size) => {
                                insert_hot_cache(hot_cache, cache_key, &target[..size]);
                                size as i64
                            }
                            Err(_) => -1,
                        }
                    }
                    Err(_) => -1,
                }
            }
            None => -1,
        };
        lengths.push(length);
    }
    lengths
}

fn execute_batch_get_into_multi_buffers(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    regions: &Arc<Mutex<RegionMap>>,
    request: pb::BatchGetIntoMultiBuffersRequest,
) -> Vec<i64> {
    let client_id = request_client_id(request.client_id_hi, request.client_id_lo);
    let tenant = normalized_tenant(client, &request.tenant);
    let mut lengths = Vec::with_capacity(request.items.len());
    for item in &request.items {
        let length = match item.buffers.as_ref() {
            Some(group) => {
                let regions = regions.lock();
                let mut buffers = Vec::with_capacity(group.buffers.len());
                let mut invalid = false;
                for buffer in &group.buffers {
                    match resolve_shared_slice_mut(&regions, client_id, buffer) {
                        Ok(slice) => buffers.push(slice),
                        Err(_) => {
                            invalid = true;
                            break;
                        }
                    }
                }
                if invalid {
                    -1
                } else {
                    let cache_key = HotCacheKey::new(tenant.clone(), item.key.clone());
                    if let Some(cache) = hot_cache {
                        match cache.copy_into_multi(&cache_key, buffers.as_mut_slice()) {
                            Ok(Some(size)) => {
                                lengths.push(size as i64);
                                continue;
                            }
                            Ok(None) => {}
                            Err(_) => {
                                lengths.push(-1);
                                continue;
                            }
                        }
                    }
                    let source_ptrs = buffers
                        .iter()
                        .map(|buffer| (buffer.as_ptr() as usize, buffer.len()))
                        .collect::<Vec<_>>();
                    let copied = {
                        let mut request =
                            MultiBufferGetRequest::new(item.key.as_str(), buffers.as_mut_slice());
                        request = request.tenant(tenant.as_str());
                        match client
                            .batch_get_into_multi_buffers(std::slice::from_mut(&mut request))
                        {
                            Ok(mut sizes) => Some(sizes.pop().unwrap_or_default()),
                            Err(_) => None,
                        }
                    };
                    match copied {
                        Some(copied) => {
                            let sources = source_ptrs
                                .iter()
                                .map(|(ptr, len)| unsafe {
                                    std::slice::from_raw_parts(*ptr as *const u8, *len)
                                })
                                .collect::<Vec<_>>();
                            insert_hot_cache_from_slices(hot_cache, cache_key, &sources, copied);
                            copied as i64
                        }
                        None => -1,
                    }
                }
            }
            None => -1,
        };
        lengths.push(length);
    }
    lengths
}

fn execute_batch_get_values_into(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    tenant_hint: Option<String>,
    items: Vec<(String, usize, usize)>,
) -> Result<Vec<i64>, StoreError> {
    let tenant = normalized_tenant(client, tenant_hint.as_deref().unwrap_or_default());
    let mut lengths = vec![-1; items.len()];
    let mut misses = Vec::new();
    for (index, (key, buffer_ptr, size)) in items.iter().enumerate() {
        let target = unsafe { std::slice::from_raw_parts_mut(*buffer_ptr as *mut u8, *size) };
        let cache_key = HotCacheKey::new(tenant.clone(), key.clone());
        if let Some(cache) = hot_cache {
            if let Some(hit) = cache.copy_into(&cache_key, target)? {
                lengths[index] = hit as i64;
                continue;
            }
        }
        misses.push((index, key.clone(), *buffer_ptr, *size));
    }
    if misses.is_empty() {
        return Ok(lengths);
    }
    let mut buffers = misses
        .iter()
        .map(|(_, _, buffer_ptr, size)| unsafe {
            std::slice::from_raw_parts_mut(*buffer_ptr as *mut u8, *size)
        })
        .collect::<Vec<_>>();
    let mut requests = misses
        .iter()
        .zip(buffers.iter_mut())
        .map(|((_, key, _, _), buffer)| {
            GetRequest::new(key.as_str(), buffer).tenant(tenant.as_str())
        })
        .collect::<Vec<_>>();
    let sizes = client.batch_get_into(requests.as_mut_slice())?;
    for (((index, key, _, _), buffer), copied) in misses.into_iter().zip(buffers.iter()).zip(sizes)
    {
        insert_hot_cache(
            hot_cache,
            HotCacheKey::new(tenant.clone(), key),
            &buffer[..copied],
        );
        lengths[index] = copied as i64;
    }
    Ok(lengths)
}

fn execute_batch_get_values_into_multi(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    tenant_hint: Option<String>,
    keys: Vec<String>,
    all_buffer_ptrs: Vec<Vec<usize>>,
    all_sizes: Vec<Vec<usize>>,
) -> Result<Vec<i64>, StoreError> {
    let tenant = normalized_tenant(client, tenant_hint.as_deref().unwrap_or_default());
    let mut lengths = Vec::with_capacity(keys.len());
    for ((key, buffer_ptrs), sizes) in keys.into_iter().zip(all_buffer_ptrs).zip(all_sizes) {
        lengths.push(execute_get_into_multi_value(
            client,
            hot_cache,
            &tenant,
            key,
            buffer_ptrs,
            sizes,
        )? as i64);
    }
    Ok(lengths)
}

fn execute_remove_all(
    client: &StoreClient,
    hot_cache: Option<&Arc<LocalHotCache>>,
    state: &Arc<Mutex<DispatcherState>>,
    force: bool,
) -> (i32, i64) {
    let keys = tracked_keys_snapshot(state);
    let mut removed = 0i64;
    for (tenant, key) in keys {
        if client.remove_in_tenant(&tenant, &key, force).is_ok() {
            invalidate_hot_cache(hot_cache, &tenant, &key);
            untrack_key(state, &tenant, &key);
            removed += 1;
        }
    }
    (0, removed)
}

fn acquire_hot_cache_reply(
    hot_cache: Option<&Arc<LocalHotCache>>,
    client: &StoreClient,
    request: pb::HotCacheAcquireRequest,
) -> pb::HotCacheAcquireReply {
    let Some(cache) = hot_cache else {
        return hot_cache_miss_reply();
    };
    let tenant = normalized_tenant(client, &request.tenant);
    let key = HotCacheKey::new(tenant, request.key);
    match cache.acquire(&key) {
        Some(handle) => pb::HotCacheAcquireReply {
            status: 0,
            offset: handle.offset as u64,
            length: handle.len as u64,
            block_id: handle.block_id,
            generation: handle.generation,
        },
        None => hot_cache_miss_reply(),
    }
}

fn hot_cache_miss_reply() -> pb::HotCacheAcquireReply {
    pb::HotCacheAcquireReply {
        status: -1,
        ..Default::default()
    }
}

fn insert_hot_cache(hot_cache: Option<&Arc<LocalHotCache>>, key: HotCacheKey, value: &[u8]) {
    if let Some(cache) = hot_cache {
        let _ = cache.insert(key, value);
    }
}

fn insert_hot_cache_from_slices(
    hot_cache: Option<&Arc<LocalHotCache>>,
    key: HotCacheKey,
    sources: &[&[u8]],
    len: usize,
) {
    if let Some(cache) = hot_cache {
        let _ = cache.insert_from_slices(key, sources, len);
    }
}

fn invalidate_hot_cache(hot_cache: Option<&Arc<LocalHotCache>>, tenant: &str, key: &str) {
    if let Some(cache) = hot_cache {
        cache.invalidate(&HotCacheKey::new(tenant.to_string(), key.to_string()));
    }
}

fn resolve_shared_slice<'a>(
    regions: &'a RegionMap,
    client_id: DummyClientId,
    buffer: &pb::SharedBufferRef,
) -> Result<&'a [u8], StoreError> {
    let offset = usize::try_from(buffer.offset)
        .map_err(|_| StoreError::Allocator("shared buffer offset overflow".to_string()))?;
    let len = usize::try_from(buffer.length)
        .map_err(|_| StoreError::Allocator("shared buffer length overflow".to_string()))?;
    let region = regions
        .get(&region_key(client_id, buffer.region_id))
        .ok_or_else(|| StoreError::NotFound("shared region not found".to_string()))?;
    region.slice(offset, len)
}

fn resolve_shared_slice_mut<'a>(
    regions: &'a RegionMap,
    client_id: DummyClientId,
    buffer: &pb::SharedBufferRef,
) -> Result<&'a mut [u8], StoreError> {
    let offset = usize::try_from(buffer.offset)
        .map_err(|_| StoreError::Allocator("shared buffer offset overflow".to_string()))?;
    let len = usize::try_from(buffer.length)
        .map_err(|_| StoreError::Allocator("shared buffer length overflow".to_string()))?;
    let region = regions
        .get(&region_key(client_id, buffer.region_id))
        .ok_or_else(|| StoreError::NotFound("shared region not found".to_string()))?;
    region.slice_mut(offset, len)
}

fn request_client_id(high: u64, low: u64) -> DummyClientId {
    DummyClientId { high, low }
}

fn region_key(client_id: DummyClientId, region_id: u64) -> RegionKey {
    (client_id.high, client_id.low, region_id)
}

fn dispatcher_stopped() -> StoreError {
    StoreError::Transport("store dispatcher stopped".to_string())
}

fn dispatcher_timeout(context: &'static str, timeout: Duration) -> StoreError {
    StoreError::Transport(format!(
        "store dispatcher {context} timed out after {}ms",
        timeout.as_millis(),
    ))
}

fn dispatcher_metric_scope(context: &'static str) -> &'static str {
    match context {
        "request execution" => "request",
        "startup registration" => "startup",
        _ => "other",
    }
}

fn effective_heartbeat_interval(requested_ms: u64, lease_ttl_ms: u64) -> u64 {
    let interval_ms = if requested_ms == 0 || requested_ms >= lease_ttl_ms {
        (lease_ttl_ms / 3).max(1_000)
    } else {
        requested_ms
    };
    interval_ms.min(lease_ttl_ms.saturating_sub(1).max(1))
}

fn initial_heartbeat_delay_ms(runtime: &str, heartbeat_interval_ms: u64) -> u64 {
    stable_phase_spread_ms(runtime, heartbeat_interval_ms, "heartbeat")
}

fn heartbeat_retry_delay_ms(heartbeat_interval_ms: u64) -> u64 {
    heartbeat_interval_ms.clamp(500, 1_000)
}

fn run_heartbeat_loop(
    publisher: HealthPublisher,
    stop: Arc<AtomicBool>,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
    initial_delay_ms: u64,
) {
    let runtime = publisher.runtime.clone();
    let mut next_heartbeat = current_time_ms().saturating_add(initial_delay_ms);
    while !stop.load(Ordering::SeqCst) && !publisher.closed.load(Ordering::SeqCst) {
        let now_ms = current_time_ms();
        if now_ms >= next_heartbeat {
            match publisher.heartbeat(now_ms.saturating_add(lease_ttl_ms)) {
                Ok(()) => {
                    next_heartbeat = now_ms.saturating_add(heartbeat_interval_ms);
                }
                Err(error) => {
                    let retry_after_ms = heartbeat_retry_delay_ms(heartbeat_interval_ms);
                    warn!(
                        runtime,
                        retry_after_ms,
                        error = %error,
                        "store dispatcher heartbeat failed"
                    );
                    next_heartbeat = now_ms.saturating_add(retry_after_ms);
                }
            }
            continue;
        }
        let sleep_ms = next_heartbeat.saturating_sub(now_ms).clamp(1, 100);
        thread::sleep(Duration::from_millis(sleep_ms));
    }
    info!(runtime, "store dispatcher heartbeat loop stopped");
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}

fn track_key(state: &Arc<Mutex<DispatcherState>>, tenant: String, key: String) {
    state.lock().tracked_keys.insert((tenant, key));
}

fn tracked_keys_snapshot(state: &Arc<Mutex<DispatcherState>>) -> Vec<TrackedKey> {
    state.lock().tracked_keys.iter().cloned().collect()
}

fn untrack_key(state: &Arc<Mutex<DispatcherState>>, tenant: &str, key: &str) {
    state
        .lock()
        .tracked_keys
        .remove(&(tenant.to_string(), key.to_string()));
}

fn normalized_tenant(client: &StoreClient, tenant: &str) -> String {
    if tenant.is_empty() {
        client.default_tenant().to_string()
    } else {
        tenant.to_string()
    }
}

fn proto_policy(policy: &Option<pb::ReplicationPolicy>) -> Option<ReplicationPolicy> {
    let policy = policy.as_ref()?;
    let mut resolved = ReplicationPolicy::new()
        .prefer_local(policy.prefer_local)
        .prefer_alloc_in_same_node(policy.prefer_alloc_in_same_node)
        .with_soft_pin(policy.with_soft_pin)
        .replica_count(policy.replica_count.max(1) as usize);
    if !policy.preferred_segments.is_empty() {
        resolved = resolved.preferred_segments(policy.preferred_segments.clone());
    }
    if !policy.preferred_storage_owners.is_empty() {
        resolved = resolved.preferred_storage_owners(policy.preferred_storage_owners.clone());
    }
    Some(resolved)
}

#[cfg(test)]
mod tests {
    use mooncake_store_client::StoreClient;

    use super::StoreDispatcher;

    #[test]
    fn dispatcher_handle_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StoreDispatcher>();
    }

    #[test]
    fn store_client_is_send() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StoreClient>();
    }
}
