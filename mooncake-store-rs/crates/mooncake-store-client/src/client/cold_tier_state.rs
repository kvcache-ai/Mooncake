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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
struct StorageOwnerColdTierConfig {
    resolver: ColdTierBackendResolver,
    devices: SharedColdTierDeviceCache,
    watermarks: ColdTierWatermarkConfig,
    rate_limits: ColdTierRateLimitConfig,
    offload_mode: ColdTierOffloadMode,
    offload_priority: ColdTierOffloadPriorityConfig,
    runtime: ClientRuntimeId,
}

#[allow(dead_code)]
struct ColdTierDeviceManager {
    resolver: Mutex<ColdTierBackendResolver>,
    devices: SharedColdTierDeviceCache,
    watermarks: ColdTierWatermarkConfig,
    admission: Arc<ColdTierAdmission>,
    runtime: ClientRuntimeId,
}

#[allow(dead_code)]
struct ColdTierOffloadManager {
    queue: Mutex<PendingOffloadQueue>,
    materializing: AtomicBool,
}

#[allow(dead_code)]
#[derive(Default)]
struct ColdTierCleanupManager {
    scheduler: Arc<Mutex<FreeSchedulerState>>,
}

#[allow(dead_code)]
struct ColdTierCleanupGuard {
    scheduler: Arc<Mutex<FreeSchedulerState>>,
    device_id: String,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PendingOffloadQueueSnapshot {
    total_pending: usize,
    ready: usize,
    delayed: usize,
    max_attempts: u32,
    total_attempts: u64,
}

#[allow(dead_code)]
struct PendingOffloadQueue {
    entries: VecDeque<PendingOffloadEntry>,
    keys: BTreeSet<PendingOffloadKey>,
    in_flight: BTreeSet<PendingOffloadKey>,
    priority: ColdTierOffloadPriorityConfig,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PendingOffloadKey {
    route_key: ObjectKey,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
struct PendingOffloadEntry {
    key: PendingOffloadKey,
    route_version: RouteVersion,
    attempts: u32,
    not_before: Instant,
    enqueued_at: Instant,
    length_bytes: Option<u64>,
}

#[allow(dead_code)]
struct ColdTierOffloadRunGuard<'a> {
    manager: &'a ColdTierOffloadManager,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColdTierAdmissionOp {
    Offload,
    Restore,
}

#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
struct ColdTierAdmission {
    config: ColdTierRateLimitConfig,
    runtime_offload: Mutex<ColdTierAdmissionBucket>,
    runtime_restore: Mutex<ColdTierAdmissionBucket>,
    devices: Mutex<BTreeMap<String, ColdTierDeviceAdmissionState>>,
    pressure: OffloadPressureTracker,
}

#[allow(dead_code)]
struct ColdTierDeviceAdmissionState {
    offload: ColdTierAdmissionBucket,
    restore: ColdTierAdmissionBucket,
    consecutive_errors: u32,
    paused_until: Option<Instant>,
    probe_in_flight: usize,
}

#[allow(dead_code)]
struct ColdTierAdmissionBucket {
    max_in_flight: usize,
    ops_per_sec: u32,
    tokens: f64,
    last_refill: Instant,
    in_flight: usize,
}

#[allow(dead_code)]
struct ColdTierAdmissionPermit {
    admission: Arc<ColdTierAdmission>,
    device_id: String,
    op: ColdTierAdmissionOp,
    probe: bool,
    released: AtomicBool,
}

#[allow(dead_code)]
#[derive(Default)]
struct FreeSchedulerState {
    active_devices: BTreeSet<String>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ColdTierCleanupResult {
    scanned_devices: usize,
    attempted_victims: usize,
    freed_backings: usize,
    skipped_backings: usize,
    restored_devices: usize,
    reached_low_watermark: bool,
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ColdTierMaintenanceStats {
    pub(crate) devices: BTreeMap<String, BackendMaintenanceStats>,
}

