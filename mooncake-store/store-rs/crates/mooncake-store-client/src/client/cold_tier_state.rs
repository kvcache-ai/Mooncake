// Cold tier state types and admission control.
// Included via `include!()` at module level in state_core.rs.

/// Controls what happens to cold-tier routes and SSD data during `StoreClient::drop`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ColdTierShutdownMode {
    /// Preserve cold-only routes and SSD data for restart recovery.
    /// On Drop: remove our DRAM replicas from routes, mark devices Unregistered.
    /// Cold-only routes remain for the next incarnation to adopt.
    #[default]
    Restart,
    /// Full cleanup: delete all owned routes and SSD data.
    /// On Drop: remove our DRAM replicas, delete cold-only routes, delete SSD files,
    /// mark devices Unregistered.
    Decommission,
}

pub(crate) type SharedColdTierDeviceCache = Arc<Mutex<ColdTierDeviceCache>>;

#[derive(Default)]
pub(crate) struct ColdTierDeviceCache {
    refreshed_at: Option<Instant>,
    devices: Vec<ColdTierDeviceRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ColdTierDeviceSnapshot {
    cache_initialized: bool,
    total_devices: usize,
    schedulable_devices: usize,
    total_used_bytes: u64,
    total_reserved_bytes: u64,
    total_capacity_bytes: Option<u64>,
    by_state: BTreeMap<mooncake_store_core::ColdTierDeviceState, usize>,
}

pub(crate) fn cold_tier_device_visible_in_cache(device: &ColdTierDeviceRecord) -> bool {
    device.state != mooncake_store_core::ColdTierDeviceState::Unregistered
        && device.epoch.is_some()
        && device.root_dir.is_some()
}

impl ColdTierDeviceCache {
    fn observability_snapshot(&self) -> ColdTierDeviceSnapshot {
        let visible_devices = self
            .devices
            .iter()
            .filter(|device| cold_tier_device_visible_in_cache(device))
            .collect::<Vec<_>>();
        let mut snapshot = ColdTierDeviceSnapshot {
            cache_initialized: self.refreshed_at.is_some(),
            total_devices: visible_devices.len(),
            ..ColdTierDeviceSnapshot::default()
        };
        let mut total_capacity = 0u64;
        let mut has_unknown_capacity = false;
        for device in visible_devices {
            if device.schedulable() {
                snapshot.schedulable_devices = snapshot.schedulable_devices.saturating_add(1);
            }
            snapshot.total_used_bytes = snapshot.total_used_bytes.saturating_add(device.used_bytes);
            snapshot.total_reserved_bytes = snapshot
                .total_reserved_bytes
                .saturating_add(device.reserved_bytes);
            match device.capacity_bytes {
                Some(capacity) => total_capacity = total_capacity.saturating_add(capacity),
                None => has_unknown_capacity = true,
            }
            *snapshot.by_state.entry(device.state).or_default() += 1;
        }
        snapshot.total_capacity_bytes = (!has_unknown_capacity).then_some(total_capacity);
        snapshot
    }

    pub(crate) fn snapshot(&self) -> Option<Vec<ColdTierDeviceRecord>> {
        self.refreshed_at?;
        Some(self.devices.clone())
    }

    pub(crate) fn store(&mut self, devices: Vec<ColdTierDeviceRecord>) {
        self.refreshed_at = Some(Instant::now());
        // Merge: if a locally-upserted record is newer (by updated_at_ms) than the
        // incoming refresh snapshot, keep the local version to avoid a stale
        // background refresh overwriting a recent apply_cold_tier_usage_delta.
        let merged = devices
            .into_iter()
            .map(|incoming| {
                match self
                    .devices
                    .iter()
                    .find(|d| d.device_id == incoming.device_id)
                {
                    Some(existing) if existing.updated_at_ms > incoming.updated_at_ms => {
                        existing.clone()
                    }
                    _ => incoming,
                }
            })
            .collect();
        self.devices = merged;
    }

    pub(crate) fn upsert(&mut self, device: ColdTierDeviceRecord) {
        self.refreshed_at = Some(Instant::now());
        match self
            .devices
            .iter_mut()
            .find(|current| current.device_id == device.device_id)
        {
            Some(current) => *current = device,
            None => self.devices.push(device),
        }
    }
}

pub(crate) fn shared_cold_tier_device_cache(namespace: &str) -> SharedColdTierDeviceCache {
    static COLD_TIER_DEVICE_CACHES: OnceLock<Mutex<BTreeMap<String, SharedColdTierDeviceCache>>> =
        OnceLock::new();
    let caches = COLD_TIER_DEVICE_CACHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = caches.lock();
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(ColdTierDeviceCache::default())))
        .clone()
}

struct StorageOwnerColdTierConfig {
    resolver: ColdTierBackendResolver,
    nof_targets: cold_tier::nof::NofTargetManager,
    devices: SharedColdTierDeviceCache,
    watermarks: ColdTierWatermarkConfig,
    rate_limits: ColdTierRateLimitConfig,
    offload_mode: ColdTierOffloadMode,
    offload_priority: ColdTierOffloadPriorityConfig,
    runtime: ClientRuntimeId,
}

struct ColdTierDeviceManager {
    resolver: Mutex<ColdTierBackendResolver>,
    nof_targets: cold_tier::nof::NofTargetManager,
    devices: SharedColdTierDeviceCache,
    watermarks: ColdTierWatermarkConfig,
    admission: Arc<ColdTierAdmission>,
    runtime: ClientRuntimeId,
}

struct ColdTierOffloadManager {
    queue: Mutex<PendingOffloadQueue>,
    materializing: AtomicBool,
}

#[derive(Default)]
struct ColdTierCleanupManager {
    scheduler: Arc<Mutex<FreeSchedulerState>>,
}

struct ColdTierCleanupGuard {
    scheduler: Arc<Mutex<FreeSchedulerState>>,
    device_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PendingOffloadQueueSnapshot {
    total_pending: usize,
    ready: usize,
    delayed: usize,
    max_attempts: u32,
    total_attempts: u64,
}

struct PendingOffloadQueue {
    entries: VecDeque<PendingOffloadEntry>,
    keys: BTreeSet<PendingOffloadKey>,
    in_flight: BTreeSet<PendingOffloadKey>,
    priority: ColdTierOffloadPriorityConfig,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PendingOffloadKey {
    route_key: ObjectKey,
}

#[derive(Clone, Debug)]
struct PendingOffloadEntry {
    key: PendingOffloadKey,
    route_version: RouteVersion,
    attempts: u32,
    not_before: Instant,
    enqueued_at: Instant,
    length_bytes: Option<u64>,
}

struct ColdTierOffloadRunGuard<'a> {
    manager: &'a ColdTierOffloadManager,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColdTierAdmissionOp {
    Offload,
    Restore,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColdTierAdmissionRejection {
    RuntimeLimit,
    DeviceLimit,
    DevicePaused,
}

/// Tracks offload queue pressure to enable adaptive restore throttling.
/// When the pending offload queue grows (indicating SSD write starvation),
/// the effective restore concurrency limit is reduced proportionally,
/// freeing SSD bandwidth for offload to catch up.
///
/// Inspired by Linux balance_dirty_pages (throttle producers when consumer
/// falls behind) and RocksDB write stall (linear slowdown based on backlog).
pub(super) struct OffloadPressureTracker {
    /// Current pending offload queue depth.
    pending_depth: AtomicUsize,
    /// Timestamp (ms since process start) of last successful offload completion.
    last_completion_ms: AtomicU64,
    /// Monotonic reference point for timestamps.
    epoch: Instant,
    /// Configuration thresholds.
    soft_threshold: usize,
    hard_threshold: usize,
    restore_floor: usize,
    restore_ceiling: usize,
    offload_boost: usize,
    stall_timeout_ms: u64,
}

impl OffloadPressureTracker {
    fn new(config: &ColdTierRateLimitConfig) -> Self {
        Self {
            pending_depth: AtomicUsize::new(0),
            last_completion_ms: AtomicU64::new(0),
            epoch: Instant::now(),
            soft_threshold: config.pressure_soft_threshold.max(1),
            hard_threshold: config.pressure_hard_threshold.max(config.pressure_soft_threshold + 1),
            restore_floor: config.pressure_restore_floor.max(1),
            restore_ceiling: config.restore_device_max_in_flight.max(1),
            offload_boost: config.pressure_offload_boost.max(config.offload_device_max_in_flight),
            stall_timeout_ms: config.pressure_stall_timeout_ms.max(100),
        }
    }

    /// Compute effective restore device concurrency limit.
    /// Linear ramp-down between soft and hard thresholds.
    /// Also drops to floor immediately if offload is stalled (SSD write starvation).
    ///
    /// NOTE: No longer used in the admission path — restore admission is now
    /// unconditional (track-only).  Kept for diagnostics / metrics export.
    #[allow(dead_code)]
    pub(super) fn effective_restore_limit(&self) -> usize {
        let depth = self.pending_depth.load(Ordering::Relaxed);
        // If offload is stalled AND there's any pending work, SSD reads are
        // starving writes. Drop to floor immediately — don't wait for depth
        // to grow past soft threshold. This catches the burst scenario where
        // many concurrent reads saturate the disk before depth accumulates.
        if depth > 0 && self.is_offload_stalled() {
            return self.restore_floor;
        }
        if depth <= self.soft_threshold {
            return self.restore_ceiling;
        }
        if depth >= self.hard_threshold {
            return self.restore_floor;
        }
        // Linear interpolation between ceiling and floor
        let range = self.hard_threshold - self.soft_threshold;
        let excess = depth - self.soft_threshold;
        let scale = (range - excess) as f64 / range as f64;
        let effective = self.restore_floor as f64
            + (self.restore_ceiling - self.restore_floor) as f64 * scale;
        (effective as usize).max(self.restore_floor).min(self.restore_ceiling)
    }

    /// Compute effective offload device concurrency limit.
    /// Under pressure: boost to allow faster backlog drain.
    pub(super) fn effective_offload_limit(&self, configured: usize) -> usize {
        if self.is_under_pressure() {
            self.offload_boost
        } else {
            configured
        }
    }

    /// True if pending_depth > soft_threshold OR offload appears stalled.
    pub(super) fn is_under_pressure(&self) -> bool {
        self.pending_depth.load(Ordering::Relaxed) > self.soft_threshold
            || self.is_offload_stalled()
    }

    fn is_offload_stalled(&self) -> bool {
        let last = self.last_completion_ms.load(Ordering::Relaxed);
        if last == 0 {
            // No completions yet; don't trigger stall on startup.
            return false;
        }
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        now_ms.saturating_sub(last) > self.stall_timeout_ms
    }

    /// Called when a new entry is enqueued for offload.
    pub(super) fn increment_pending(&self) {
        self.pending_depth.fetch_add(1, Ordering::Relaxed);
    }

    /// Called when an offload completes (success or permanent skip).
    pub(super) fn decrement_pending(&self) {
        self.pending_depth.fetch_sub(1, Ordering::Relaxed);
        let now_ms = self.epoch.elapsed().as_millis() as u64;
        self.last_completion_ms.store(now_ms, Ordering::Relaxed);
    }

    /// Sync with actual queue length (drift protection).
    pub(super) fn refresh_depth(&self, actual: usize) {
        self.pending_depth.store(actual, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct HotReplicaTracker {
    clock: Mutex<StorageClockState>,
}

struct ColdTierAdmission {
    config: ColdTierRateLimitConfig,
    runtime_offload: Mutex<ColdTierAdmissionBucket>,
    runtime_restore: Mutex<ColdTierAdmissionBucket>,
    devices: Mutex<BTreeMap<String, ColdTierDeviceAdmissionState>>,
    pressure: OffloadPressureTracker,
}

struct ColdTierDeviceAdmissionState {
    offload: ColdTierAdmissionBucket,
    restore: ColdTierAdmissionBucket,
    consecutive_errors: u32,
    paused_until: Option<Instant>,
    probe_in_flight: usize,
}

struct ColdTierAdmissionBucket {
    max_in_flight: usize,
    ops_per_sec: u32,
    tokens: f64,
    last_refill: Instant,
    in_flight: usize,
}

struct ColdTierAdmissionPermit {
    admission: Arc<ColdTierAdmission>,
    device_id: String,
    op: ColdTierAdmissionOp,
    probe: bool,
    released: AtomicBool,
}

#[derive(Default)]
struct FreeSchedulerState {
    active_devices: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ColdTierCleanupResult {
    scanned_devices: usize,
    attempted_victims: usize,
    freed_backings: usize,
    skipped_backings: usize,
    restored_devices: usize,
    reached_low_watermark: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColdTierCompactionResult {
    pub(crate) scanned_devices: usize,
    pub(crate) candidate_devices: usize,
    pub(crate) skipped_devices: usize,
    pub(crate) unsupported_devices: usize,
    pub(crate) backend_dead_bytes: u64,
    pub(crate) compacted_candidate_bytes: u64,
    pub(crate) reclaimed_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PendingDeleteGcResult {
    scanned_routes: usize,
    cas_removed_routes: usize,
    backend_deleted: usize,
    backend_missing: usize,
    pending_source_deleted: usize,
    cas_conflicts: usize,
    backend_errors: usize,
    used_bytes_released: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColdTierMaintenanceStats {
    pub(crate) devices: BTreeMap<String, BackendMaintenanceStats>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReclaimQueueSnapshot {
    total_pending: usize,
    due: usize,
    cold_backing_reclaims: usize,
    hot_segment_reclaims: usize,
    by_qos_tier: BTreeMap<String, usize>,
    by_policy_rank: BTreeMap<u8, usize>,
}

type SharedRestorePromotionQueue = Arc<RestorePromotionQueue>;
type SharedColdRestoreSingleflight = Arc<ColdRestoreSingleflight>;

const DEFAULT_RESTORE_PROMOTION_QUEUE_LIMIT: usize = 1024;

struct RestorePromotionQueue {
    state: Mutex<RestorePromotionQueueState>,
    idle: parking_lot::Condvar,
    limit: usize,
    batch_limit: usize,
    max_in_flight: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestorePromotionPushOutcome {
    Accepted,
    Shutdown,
    Duplicate,
    InFlight,
    Full,
}

#[derive(Default)]
struct RestorePromotionQueueState {
    entries: VecDeque<RestorePromotionTask>,
    keys: BTreeSet<RestorePromotionKey>,
    in_flight: BTreeSet<RestorePromotionKey>,
    worker_active: bool,
    shutdown: bool,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RestorePromotionKey {
    route_key: ObjectKey,
    route_version: RouteVersion,
}

#[derive(Clone, Debug)]
struct RestorePromotionTask {
    key: RestorePromotionKey,
    tenant: String,
    object_id: LogicalObjectId,
    qos_tier: Option<String>,
    current: ObjectRoute,
    payload: Arc<Vec<u8>>,
    policy: ReplicationPolicy,
    target_runtime: Option<ClientRuntimeId>,
    target_segment: Option<SegmentName>,
}

enum RestorePromotionPayload<'a> {
    Borrowed(&'a [u8]),
    Shared(Arc<Vec<u8>>),
}

impl RestorePromotionPayload<'_> {
    fn into_arc(self) -> Arc<Vec<u8>> {
        match self {
            Self::Borrowed(payload) => Arc::new(payload.to_vec()),
            Self::Shared(payload) => payload,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ColdRestoreFlightKey {
    route_key: ObjectKey,
    route_version: RouteVersion,
    cold_tier_id: String,
    object_locator: String,
    length: u64,
    checksum: Option<u64>,
}

struct ColdRestoreSingleflight {
    state: StdMutex<ColdRestoreSingleflightState>,
    max_distinct_flights: usize,
}

#[derive(Default)]
struct ColdRestoreSingleflightState {
    flights: BTreeMap<ColdRestoreFlightKey, Arc<ColdRestoreFlight>>,
}

struct ColdRestoreFlight {
    state: StdMutex<ColdRestoreFlightState>,
    completed: StdCondvar,
    waiters: AtomicUsize,
}

#[derive(Default)]
struct ColdRestoreFlightState {
    result: Option<Result<Arc<Vec<u8>>>>,
}

enum ColdRestoreFlightRegistration {
    Leader(ColdRestoreFlightLeader),
    Waiter(Arc<ColdRestoreFlight>),
    Rejected,
}

struct ColdRestoreFlightLeader {
    key: ColdRestoreFlightKey,
    flight: Arc<ColdRestoreFlight>,
    singleflight: SharedColdRestoreSingleflight,
    completed: bool,
}

struct PendingOffloadMaterialization {
    entry: PendingOffloadEntry,
    route: ObjectRoute,
    cold_backing: mooncake_store_core::ColdBackingRoute,
    nof_backing: bool,
    payload: PendingOffloadPayload,
    device: Option<ColdTierDeviceRecord>,
    permit: Option<ColdTierAdmissionPermit>,
}

enum PendingBackingRoute {
    Cold(mooncake_store_core::ColdBackingRoute),
    Nof(mooncake_store_core::NofBackingRoute),
}

impl PendingBackingRoute {
    fn publish(self, route: &mut ObjectRoute) {
        match self {
            Self::Cold(backing) => route.cold_backing = Some(backing),
            Self::Nof(backing) => route.nof_backing = Some(backing),
        }
    }
}

struct PendingOffloadPayload(Vec<u8>);

impl PendingOffloadPayload {
    fn new(payload: Vec<u8>) -> Self {
        Self(payload)
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

struct PendingOffloadReadyRoute {
    index: usize,
    materialized_cold_backing: mooncake_store_core::ColdBackingRoute,
    next_route: ObjectRoute,
}

struct PendingOffloadCasOutcome {
    materialized: usize,
    used_add_bytes: i64,
    reserved_release_bytes: i64,
    cleanup: Vec<mooncake_store_core::ColdBackingRoute>,
}

struct PendingOffloadCasContext<'a> {
    backend: &'a dyn PersistentStorageBackend,
    pending: &'a [PendingOffloadMaterialization],
    first_error: &'a mut Option<StoreError>,
    outcome: &'a mut PendingOffloadCasOutcome,
}

enum PendingOffloadPrepareOutcome {
    Ready(Box<PendingOffloadMaterialization>),
    Retry(PendingOffloadEntry),
    RetryAfter(PendingOffloadEntry, Duration),
    Refreshed(PendingOffloadEntry),
    Skipped,
}

// ---------------------------------------------------------------------------
// Owner-side cold restore singleflight (dedup across prefetch / sync cold read)
// ---------------------------------------------------------------------------

/// RDMA coordinates for a cold restore that has data ready in segment memory.
/// Published as soon as SSD read completes — remote readers can RDMA from these
/// coordinates immediately, without waiting for route CAS.
#[derive(Clone, Debug)]
struct OwnerColdRestoreOutcome {
    segment_name: String,
    segment_offset: u64,
    length: u64,
    checksum: Option<u64>,
    target_chunks: Vec<SegmentTargetChunk>,
    transport_endpoint: Option<String>,
    transport_segment_descriptor: Option<String>,
    timing: OwnerColdRestoreTiming,
}

#[derive(Clone, Debug, Default)]
struct OwnerColdRestoreTiming {
    admission_wait_us: u64,
    ssd_read_us: u64,
    memcpy_us: u64,
    route_update_us: u64,
    total_us: u64,
}

/// Owner-side cold restore deduplication map.
///
/// All paths that promote a cold-only key to DRAM on the owner go through this:
/// - `read_from_cold_one_shot` (sync gRPC handler for remote readers)
/// - `batch_is_exist` async prefetch
/// - `promote_owned_materialized_route_by_key` (migration tasks)
///
/// Two-stage design:
/// 1. Leader: admission → reserve segment → SSD read into segment → **publish outcome**
///    (Waiters are unblocked here — data is RDMA-readable from segment)
/// 2. Leader: route CAS (async, not on waiter's critical path)
///
/// Keyed by `ObjectKey` so concurrent requests for the same key share one SSD read.
struct OwnerColdRestoreFlightMap {
    state: StdMutex<BTreeMap<ObjectKey, Arc<OwnerColdRestoreFlight>>>,
    max_flights: usize,
}

struct OwnerColdRestoreFlight {
    /// Set once SSD read is done and data is in segment (RDMA-ready).
    /// Waiters poll/wait on this.
    data_ready: StdMutex<Option<Result<OwnerColdRestoreOutcome>>>,
    data_ready_cv: StdCondvar,
}

enum OwnerColdRestoreFlightRole {
    /// This caller is responsible for executing the promote.
    Leader(OwnerColdRestoreFlightLeader),
    /// Another caller is already promoting this key; wait for their result.
    Waiter(Arc<OwnerColdRestoreFlight>),
    /// Too many concurrent flights; caller should proceed without dedup.
    Rejected,
}

struct OwnerColdRestoreFlightLeader {
    key: ObjectKey,
    flight: Arc<OwnerColdRestoreFlight>,
    map: *const OwnerColdRestoreFlightMap,
    published: bool,
}

// SAFETY: OwnerColdRestoreFlightLeader is only accessed on a single thread
// (the leader that performs the promote). The raw pointer is used to remove
// the flight from the map on drop; the map outlives all leaders because it
// lives in StorageOwnerState.
unsafe impl Send for OwnerColdRestoreFlightLeader {}

impl OwnerColdRestoreFlightMap {
    fn new(max_flights: usize) -> Self {
        Self {
            state: StdMutex::new(BTreeMap::new()),
            max_flights: max_flights.max(1),
        }
    }

    /// Attempt to register as leader for `key`, or join an existing flight.
    fn acquire(&self, key: &ObjectKey) -> OwnerColdRestoreFlightRole {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(flight) = state.get(key) {
            return OwnerColdRestoreFlightRole::Waiter(flight.clone());
        }
        if state.len() >= self.max_flights {
            return OwnerColdRestoreFlightRole::Rejected;
        }
        let flight = Arc::new(OwnerColdRestoreFlight {
            data_ready: StdMutex::new(None),
            data_ready_cv: StdCondvar::new(),
        });
        state.insert(key.clone(), flight.clone());
        OwnerColdRestoreFlightRole::Leader(OwnerColdRestoreFlightLeader {
            key: key.clone(),
            flight,
            map: self as *const Self,
            published: false,
        })
    }

    /// Wait for data to become RDMA-ready (SSD read done), with timeout.
    /// Does NOT wait for route CAS — that happens asynchronously.
    fn wait(
        &self,
        flight: &OwnerColdRestoreFlight,
        timeout: Duration,
    ) -> Result<OwnerColdRestoreOutcome> {
        let deadline = Instant::now() + timeout;
        let mut state = flight.data_ready.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(result) = state.as_ref() {
                return result.clone();
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(StoreError::Allocator(
                    "owner cold restore flight wait timed out".to_string(),
                ));
            }
            let (new_state, wait_result) = flight
                .data_ready_cv
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner());
            state = new_state;
            if wait_result.timed_out() {
                if let Some(result) = state.as_ref() {
                    return result.clone();
                }
                return Err(StoreError::Allocator(
                    "owner cold restore flight wait timed out".to_string(),
                ));
            }
        }
    }

    /// Non-blocking check: returns the leader's published result if available,
    /// `None` if the leader is still in-flight. Used by batch coordinator to
    /// avoid blocking the coordinator thread on external singleflight leaders.
    fn try_wait(
        &self,
        flight: &OwnerColdRestoreFlight,
    ) -> Option<Result<OwnerColdRestoreOutcome>> {
        let state = flight.data_ready.lock().unwrap_or_else(|e| e.into_inner());
        state.clone()
    }

    /// Remove a flight from the map (called after route CAS or on error).
    fn remove(&self, key: &ObjectKey) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
    }
}

impl OwnerColdRestoreFlightLeader {
    /// Publish RDMA coordinates to waiters — called as soon as SSD read is done.
    /// Waiters are unblocked immediately. Route CAS happens after this returns.
    /// The flight is removed from the map when this leader is dropped.
    fn publish_data_ready(&mut self, result: Result<OwnerColdRestoreOutcome>) -> Result<OwnerColdRestoreOutcome> {
        {
            let mut state = self.flight.data_ready.lock().unwrap_or_else(|e| e.into_inner());
            *state = Some(result.clone());
        }
        self.flight.data_ready_cv.notify_all();
        self.published = true;
        result
    }
}

impl Drop for OwnerColdRestoreFlightLeader {
    fn drop(&mut self) {
        if !self.published {
            // Leader died before publishing — notify waiters with error.
            let mut state = self.flight.data_ready.lock().unwrap_or_else(|e| e.into_inner());
            *state = Some(Err(StoreError::InvalidState(
                "owner cold restore leader dropped before publishing result".to_string(),
            )));
            self.flight.data_ready_cv.notify_all();
        }
        // Always remove from map on drop (whether published or not).
        // SAFETY: same as above.
        let map = unsafe { &*self.map };
        map.remove(&self.key);
    }
}

impl Default for OwnerColdRestoreFlightMap {
    fn default() -> Self {
        // Default: allow up to 64 concurrent distinct flights (matches typical
        // workload where 40 sessions can each have one cold restore in-flight).
        Self::new(64)
    }
}

// ---------------------------------------------------------------------------
// ColdRestoreIoTracker: per-object concurrent SSD I/O counting.
//
// Tracks how many I/O workers are simultaneously reading the same object
// from SSD. If singleflight works correctly, this should always be 1.
// A value >1 means singleflight failed to dedup or was bypassed.
//
// Used at the execute_owner_cold_restore_ssd_phase_staging layer, which
// is the actual SSD read entry point for both singleflight-managed and
// rejected (bypass) paths.
// ---------------------------------------------------------------------------

pub(super) struct ColdRestoreIoTracker {
    /// Per-object concurrent I/O count.
    active: StdMutex<HashMap<ObjectKey, usize>>,
    /// High-water mark: max concurrent I/O workers seen for any single object.
    max_concurrent: AtomicUsize,
}

impl Default for ColdRestoreIoTracker {
    fn default() -> Self {
        Self {
            active: StdMutex::new(HashMap::new()),
            max_concurrent: AtomicUsize::new(1),
        }
    }
}

impl ColdRestoreIoTracker {
    /// Record the start of an SSD I/O for `key`. Returns an RAII guard that
    /// decrements the count on drop.
    pub(super) fn begin_io(&self, key: &ObjectKey) -> ColdRestoreIoGuard<'_> {
        let current = {
            let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
            let count = active.entry(key.clone()).or_insert(0);
            *count += 1;
            *count
        };
        // Update high-water mark (monotonic — never decreases).
        // Only touch the global metrics registry when singleflight is violated
        // (current > 1), keeping the normal path lock-free.
        if current > 1 {
            self.max_concurrent.fetch_max(current, Ordering::Relaxed);
            crate::client::cold_tier::set_cold_restore_max_concurrent_io_per_object(current);
        }
        ColdRestoreIoGuard {
            tracker: self,
            key: key.clone(),
        }
    }

    fn end_io(&self, key: &ObjectKey) {
        let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(count) = active.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                active.remove(key);
            }
        }
    }

    /// Returns the high-water mark: max concurrent I/O workers observed for
    /// any single object since this tracker was created.
    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) fn max_concurrent_io(&self) -> usize {
        self.max_concurrent.load(Ordering::Relaxed)
    }
}

pub(super) struct ColdRestoreIoGuard<'a> {
    tracker: &'a ColdRestoreIoTracker,
    key: ObjectKey,
}

impl Drop for ColdRestoreIoGuard<'_> {
    fn drop(&mut self) {
        self.tracker.end_io(&self.key);
    }
}

// ---------------------------------------------------------------------------
// ReadPinRegistry: protects DRAM slots from eviction while readers are
// actively performing RDMA reads on them.
//
// Each cold restore promote pins the (segment_name, offset) of the slot it
// wrote into. Eviction checks `is_pinned` and skips pinned slots. The reader
// ACKs after RDMA completes, which calls `unpin`. Refcount handles
// singleflight (multiple readers sharing one promote).
// ---------------------------------------------------------------------------

pub(super) struct ReadPinRegistry {
    inner: Mutex<ReadPinInner>,
}

#[derive(Default)]
struct ReadPinInner {
    /// (segment_name, offset) → refcount.
    pins: BTreeMap<(SegmentName, u64), u32>,
    /// Number of distinct slots currently pinned (pins with refcount > 0).
    pinned_count: u32,
}

impl Default for ReadPinRegistry {
    fn default() -> Self {
        Self {
            inner: Mutex::new(ReadPinInner::default()),
        }
    }
}

impl ReadPinRegistry {
    /// Pin a slot (increment refcount). Called after promote writes data.
    pub(super) fn pin(&self, segment: &SegmentName, offset: u64) {
        let mut inner = self.inner.lock();
        let key = (segment.clone(), offset);
        let prev = inner.pins.get(&key).copied().unwrap_or(0);
        inner.pins.insert(key, prev + 1);
        if prev == 0 {
            inner.pinned_count += 1;
        }
    }

    /// Unpin a slot (decrement refcount). Called when reader ACKs RDMA complete.
    #[allow(dead_code)]
    pub(super) fn unpin(&self, segment: &SegmentName, offset: u64) {
        let mut inner = self.inner.lock();
        let key = (segment.clone(), offset);
        if let Some(count) = inner.pins.get_mut(&key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                inner.pins.remove(&key);
                inner.pinned_count = inner.pinned_count.saturating_sub(1);
            }
        }
    }

    /// Batch unpin multiple slots at once (for ACK RPCs).
    pub(super) fn unpin_batch(&self, slots: &[(SegmentName, u64)]) {
        let mut inner = self.inner.lock();
        for (segment, offset) in slots {
            let key = (segment.clone(), *offset);
            if let Some(count) = inner.pins.get_mut(&key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    inner.pins.remove(&key);
                    inner.pinned_count = inner.pinned_count.saturating_sub(1);
                }
            }
        }
    }

    /// Check if a slot is currently pinned by any reader.
    pub(super) fn is_pinned(&self, segment: &SegmentName, offset: u64) -> bool {
        let inner = self.inner.lock();
        inner
            .pins
            .get(&(segment.clone(), offset))
            .is_some_and(|count| *count > 0)
    }

    /// Number of distinct slots currently pinned.
    #[allow(dead_code)]
    pub(super) fn pinned_count(&self) -> u32 {
        self.inner.lock().pinned_count
    }

    /// Remove a pin entry entirely regardless of refcount.
    /// Used by TTL sweep to reclaim leaked staging slots whose readers
    /// will never send an ACK (crash, network partition).
    #[allow(dead_code)]
    pub(super) fn force_unpin(&self, segment: &SegmentName, offset: u64) {
        let mut inner = self.inner.lock();
        let key = (segment.clone(), offset);
        if inner.pins.remove(&key).is_some() {
            inner.pinned_count = inner.pinned_count.saturating_sub(1);
        }
    }

}
