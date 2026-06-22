// Cold tier state types and admission control.
// Included via `include!()` at module level in state_core.rs.
//
// Component hierarchy:
//
//   StorageOwnerState
//     +-- ColdTierDeviceManager        device selection, backend resolution
//     |     +-- ColdTierBackendResolver   backend I/O dispatch
//     |     +-- SharedColdTierDeviceCache  process-global device state
//     |     +-- ColdTierAdmission         two-tier rate limiting
//     |           +-- OffloadPressureTracker  adaptive throttle
//     +-- HotReplicaTracker             clock-based eviction tracking
//
// Types below are grouped into:
//   1. Active — fully implemented, consumed by device.rs / builder.rs
//   2. Forward-declared for offload pipeline
//   3. Forward-declared for restore
//   4. Forward-declared for cleanup / GC / compaction

const DEFAULT_RESTORE_PROMOTION_QUEUE_LIMIT: usize = 1024;

// ---------------------------------------------------------------------------
// Active: device cache, device manager, admission control
// ---------------------------------------------------------------------------

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

/// Aggregated observability summary of cached cold tier devices.
/// Used by metrics export and diagnostic endpoints.
#[allow(dead_code)] // consumed by observability/metrics export
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ColdTierDeviceCacheSummary {
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

#[allow(dead_code)] // observability_snapshot called by metrics export
impl ColdTierDeviceCache {
    fn observability_snapshot(&self) -> ColdTierDeviceCacheSummary {
        let visible_devices = self
            .devices
            .iter()
            .filter(|device| cold_tier_device_visible_in_cache(device))
            .collect::<Vec<_>>();
        let mut snapshot = ColdTierDeviceCacheSummary {
            cache_initialized: self.refreshed_at.is_some(),
            total_devices: visible_devices.len(),
            ..ColdTierDeviceCacheSummary::default()
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
        // Split-field merge: lifecycle fields (state, epoch, root_dir) always
        // come from the incoming backend snapshot — the metadata backend is the
        // authoritative source for device lifecycle.  Usage fields (used_bytes,
        // reserved_bytes) are kept from the local version when the local
        // timestamp is newer, because apply_cold_tier_usage_delta updates them
        // in real-time and the background refresh may lag behind.
        let merged = devices
            .into_iter()
            .map(|mut incoming| {
                match self
                    .devices
                    .iter()
                    .find(|d| d.device_id == incoming.device_id)
                {
                    Some(existing) if existing.updated_at_ms > incoming.updated_at_ms => {
                        // Local usage is fresher — graft it onto the incoming
                        // record so lifecycle fields stay authoritative.
                        incoming.used_bytes = existing.used_bytes;
                        incoming.reserved_bytes = existing.reserved_bytes;
                        incoming.updated_at_ms = existing.updated_at_ms;
                        incoming
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

/// Returns a process-global device cache for the given namespace.
///
/// Shared across all StoreClient instances in the same namespace so that
/// background membership sync refreshes are visible to all clients without
/// per-client polling.  Keyed by route namespace to isolate multi-tenant
/// deployments within a single process.
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

/// Builder-to-runtime parameter bag for cold tier configuration.
/// Consumed once by `StorageOwnerState::new()` to construct the device manager.
#[allow(dead_code)] // fields consumed in StorageOwnerState::new (cold_tier/device.rs)
struct StorageOwnerColdTierConfig {
    resolver: ColdTierBackendResolver,
    devices: SharedColdTierDeviceCache,
    watermarks: ColdTierWatermarkConfig,
    rate_limits: ColdTierRateLimitConfig,
    offload_mode: ColdTierOffloadMode,
    offload_priority: ColdTierOffloadPriorityConfig,
    runtime: ClientRuntimeId,
}

/// Manages cold tier device selection, backend resolution, and watermark enforcement.
#[allow(dead_code)] // StorageOwnerState field, consumed by offload/restore paths
struct ColdTierDeviceManager {
    resolver: Mutex<ColdTierBackendResolver>,
    devices: SharedColdTierDeviceCache,
    watermarks: ColdTierWatermarkConfig,
    admission: Arc<ColdTierAdmission>,
    runtime: ClientRuntimeId,
}

/// Two-tier admission controller for cold tier I/O operations.
///
/// Architecture:
/// - Offload (write): full token-bucket rate limiting at both runtime and
///   device level, with health-based pause/probe after consecutive errors.
/// - Restore (read): tracking only, no rejection.  Natural backpressure comes
///   from the fixed-size staging pool and io_uring queue depth, which are
///   more effective than algorithmic limits for read workloads.
///
/// When the offload queue grows, OffloadPressureTracker boosts offload
/// concurrency to drain the backlog faster.
#[allow(dead_code)] // constructed in device.rs, used by offload/restore admission
struct ColdTierAdmission {
    config: ColdTierRateLimitConfig,
    runtime_offload: Mutex<ColdTierAdmissionBucket>,
    runtime_restore: Mutex<ColdTierAdmissionBucket>,
    devices: Mutex<BTreeMap<String, ColdTierDeviceAdmissionState>>,
    pressure: OffloadPressureTracker,
}

#[allow(dead_code)] // per-device admission state inside ColdTierAdmission
struct ColdTierDeviceAdmissionState {
    offload: ColdTierAdmissionBucket,
    restore: ColdTierAdmissionBucket,
    consecutive_errors: u32,
    paused_until: Option<Instant>,
    probe_in_flight: usize,
}

/// Token-bucket rate limiter for a single resource (runtime or device).
#[allow(dead_code)] // constructed in device.rs admission control
struct ColdTierAdmissionBucket {
    max_in_flight: usize,
    ops_per_sec: u32,
    tokens: f64,
    last_refill: Instant,
    in_flight: usize,
}

/// RAII permit returned by ColdTierAdmission::try_acquire.
/// Decrements in-flight counters on drop.
#[allow(dead_code)] // returned by try_acquire, consumed by offload/restore callers
struct ColdTierAdmissionPermit {
    admission: Arc<ColdTierAdmission>,
    device_id: String,
    op: ColdTierAdmissionOp,
    probe: bool,
    released: AtomicBool,
}

#[allow(dead_code)] // discriminant for offload vs restore admission
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColdTierAdmissionOp {
    Offload,
    Restore,
}

#[allow(dead_code)] // rejection reason from try_acquire
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColdTierAdmissionRejection {
    RuntimeLimit,
    DeviceLimit,
    DevicePaused,
}

/// Tracks offload queue pressure to enable adaptive offload throttling.
/// When the pending offload queue grows (indicating SSD write starvation),
/// offload concurrency is boosted to drain the backlog faster.
///
/// Inspired by Linux balance_dirty_pages (throttle producers when consumer
/// falls behind) and RocksDB write stall (linear slowdown based on backlog).
#[allow(dead_code)] // constructed in ColdTierAdmission, queried by offload scheduler
pub(super) struct OffloadPressureTracker {
    /// Current pending offload queue depth.
    pending_depth: AtomicUsize,
    /// Timestamp (ms since process start) of last successful offload completion.
    last_completion_ms: AtomicU64,
    /// Monotonic reference point for timestamps.
    epoch: Instant,
    /// Configuration thresholds.
    soft_threshold: usize,
    offload_boost: usize,
    stall_timeout_ms: u64,
}

#[allow(dead_code)] // impl methods called by offload scheduler and admission
impl OffloadPressureTracker {
    fn new(config: &ColdTierRateLimitConfig) -> Self {
        Self {
            pending_depth: AtomicUsize::new(0),
            last_completion_ms: AtomicU64::new(0),
            epoch: Instant::now(),
            soft_threshold: config.pressure_soft_threshold.max(1),
            offload_boost: config.pressure_offload_boost.max(config.offload_device_max_in_flight),
            stall_timeout_ms: config.pressure_stall_timeout_ms.max(100),
        }
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

/// Tracks hot replica access patterns for clock-based eviction.
/// Wraps StorageClockState; eviction priority decisions use the
/// access recency tracked here.
#[derive(Default)]
struct HotReplicaTracker {
    clock: Mutex<StorageClockState>,
}

/// Serializes route mutations during cold-tier state transitions.
/// Coordinates offload CAS operations with concurrent route updates.
#[derive(Default)]
struct RouteWriteGate {
    state: Mutex<()>,
}

impl RouteWriteGate {
    fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.state.lock()
    }
}

/// RAII guard from RouteWriteGate::lock().
#[allow(dead_code)] // held during offload route CAS
struct RouteWritePermit<'a> {
    guard: Option<parking_lot::MutexGuard<'a, ()>>,
}

// ---------------------------------------------------------------------------
// Forward-declared for offload pipeline
// ---------------------------------------------------------------------------

/// Orchestrates background offload writes from DRAM to SSD.
/// Type definition only; impl and integration added with the offload pipeline.
#[allow(dead_code)] // offload pipeline
struct ColdTierOffloadManager {
    queue: Mutex<PendingOffloadQueue>,
    materializing: AtomicBool,
}

/// Exclusion guard for a single offload materialization run.
#[allow(dead_code)] // offload pipeline
struct ColdTierOffloadRunGuard<'a> {
    manager: &'a ColdTierOffloadManager,
}

/// Queue of objects awaiting offload to SSD.
#[allow(dead_code)] // offload pipeline
struct PendingOffloadQueue {
    entries: VecDeque<PendingOffloadEntry>,
    keys: BTreeSet<PendingOffloadKey>,
    in_flight: BTreeSet<PendingOffloadKey>,
    priority: ColdTierOffloadPriorityConfig,
}

#[allow(dead_code)] // offload pipeline
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PendingOffloadKey {
    route_key: ObjectKey,
}

#[allow(dead_code)] // offload pipeline
#[derive(Clone, Debug)]
struct PendingOffloadEntry {
    key: PendingOffloadKey,
    route_version: RouteVersion,
    attempts: u32,
    not_before: Instant,
    enqueued_at: Instant,
    length_bytes: Option<u64>,
}

/// Observability snapshot of the pending offload queue.
#[allow(dead_code)] // offload metrics
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PendingOffloadQueueSnapshot {
    total_pending: usize,
    ready: usize,
    delayed: usize,
    max_attempts: u32,
    total_attempts: u64,
}

// ---------------------------------------------------------------------------
// Forward-declared for restore
// ---------------------------------------------------------------------------

/// Observability snapshot of the pending reclaim queue.
#[allow(dead_code)] // reclaim queue diagnostics
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReclaimQueueSnapshot {
    total_pending: usize,
    due: usize,
    cold_backing_reclaims: usize,
    hot_segment_reclaims: usize,
    by_qos_tier: BTreeMap<String, usize>,
    by_policy_rank: BTreeMap<u8, usize>,
}

// ---------------------------------------------------------------------------
// Forward-declared for cleanup / GC / compaction
// ---------------------------------------------------------------------------

/// Manages background cleanup of cold tier devices (eviction, GC, compaction).
/// Type definition only; impl added with the cleanup module.
#[allow(dead_code)] // cleanup lifecycle
#[derive(Default)]
struct ColdTierCleanupManager {
    scheduler: Arc<Mutex<FreeSchedulerState>>,
}

/// RAII guard preventing concurrent cleanup on the same device.
#[allow(dead_code)] // cleanup lifecycle
struct ColdTierCleanupGuard {
    scheduler: Arc<Mutex<FreeSchedulerState>>,
    device_id: String,
}

/// Tracks which devices are actively running cleanup operations.
#[allow(dead_code)] // cleanup scheduling
#[derive(Default)]
struct FreeSchedulerState {
    active_devices: BTreeSet<String>,
}

/// Result of a single cold tier cleanup pass.
#[allow(dead_code)] // cleanup result
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ColdTierCleanupResult {
    scanned_devices: usize,
    attempted_victims: usize,
    freed_backings: usize,
    skipped_backings: usize,
    restored_devices: usize,
    reached_low_watermark: bool,
}

/// Result of backend compaction (dead byte reclamation).
#[allow(dead_code)] // compaction result
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

/// Result of pending-delete route garbage collection.
#[allow(dead_code)] // GC result
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

/// Aggregated per-device maintenance statistics.
#[allow(dead_code)] // maintenance tick stats
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColdTierMaintenanceStats {
    pub(crate) devices: BTreeMap<String, BackendMaintenanceStats>,
}

// ---------------------------------------------------------------------------
// Eviction readiness signal
// ---------------------------------------------------------------------------

#[derive(Default)]
#[allow(dead_code)]
struct EvictionReadySignal {
    ready: AtomicBool,
}

#[allow(dead_code)]
impl EvictionReadySignal {
    fn signal(&self) {
        self.ready.store(true, Ordering::Release);
    }

    fn take(&self) -> bool {
        self.ready.swap(false, Ordering::AcqRel)
    }
}

// ---------------------------------------------------------------------------
// Offload pipeline: materialization, CAS, prepare
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct PendingOffloadMaterialization {
    entry: PendingOffloadEntry,
    route: ObjectRoute,
    cold_backing: mooncake_store_core::ColdBackingRoute,
    payload: PendingOffloadPayload,
    device: ColdTierDeviceRecord,
    permit: ColdTierAdmissionPermit,
}

#[allow(dead_code)]
enum PendingOffloadPayload {
    Owned(Vec<u8>),
    /// Raw pointer into registered shared-memory segment.
    ///
    /// # Safety
    ///
    /// The pointer is valid for the lifetime of the `PendingOffloadMaterialization` because:
    /// 1. The source segment is pinned in the memory registry while any route references it.
    /// 2. The offload CAS atomically transitions the route away from the segment before the
    ///    segment can be freed — if the CAS fails, the pointer is never dereferenced again.
    /// 3. The `PendingOffloadMaterialization` is consumed (and the pointer discarded) within a
    ///    single offload batch iteration; it never escapes to another thread or outlives the
    ///    segment pin.
    LocalHot { addr: *const u8, len: usize },
}

// SAFETY: The raw pointer in LocalHot points into a pinned shared-memory segment that
// remains valid for the lifetime of the containing PendingOffloadMaterialization (see
// safety invariants on the LocalHot variant). The offload worker is the sole consumer.
unsafe impl Send for PendingOffloadPayload {}

#[allow(dead_code)]
impl PendingOffloadPayload {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(payload) => payload,
            // SAFETY: See invariants documented on `LocalHot` variant.
            Self::LocalHot { addr, len } => unsafe { slice::from_raw_parts(*addr, *len) },
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Owned(payload) => payload.len(),
            Self::LocalHot { len, .. } => *len,
        }
    }
}

#[allow(dead_code)]
struct PendingOffloadReadyRoute {
    index: usize,
    materialized_cold_backing: mooncake_store_core::ColdBackingRoute,
    next_route: ObjectRoute,
}

#[allow(dead_code)]
struct PendingOffloadCasOutcome {
    materialized: usize,
    used_add_bytes: i64,
    reserved_release_bytes: i64,
    cleanup: Vec<mooncake_store_core::ColdBackingRoute>,
}

#[allow(dead_code)]
struct PendingOffloadCasContext<'a> {
    backend: &'a dyn PersistentStorageBackend,
    pending: &'a [PendingOffloadMaterialization],
    first_error: &'a mut Option<StoreError>,
    outcome: &'a mut PendingOffloadCasOutcome,
}

#[allow(dead_code)]
enum PendingOffloadPrepareOutcome {
    Ready(Box<PendingOffloadMaterialization>),
    Retry(PendingOffloadEntry),
    RetryAfter(PendingOffloadEntry, Duration),
    Refreshed(PendingOffloadEntry),
    Skipped,
}

// ---------------------------------------------------------------------------
// Restore promotion queue
// ---------------------------------------------------------------------------

type SharedRestorePromotionQueue = Arc<RestorePromotionQueue>;

#[allow(dead_code)]
struct RestorePromotionQueue {
    state: Mutex<RestorePromotionQueueState>,
    limit: usize,
    batch_limit: usize,
    max_in_flight: usize,
}

#[derive(Default)]
#[allow(dead_code)]
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
#[allow(dead_code)]
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

impl RestorePromotionQueue {
    fn new(limit: usize, batch_limit: usize, max_in_flight: usize) -> Self {
        Self {
            state: Mutex::new(RestorePromotionQueueState::default()),
            limit,
            batch_limit,
            max_in_flight,
        }
    }

    fn shutdown(&self) {
        self.state.lock().shutdown = true;
    }
}
