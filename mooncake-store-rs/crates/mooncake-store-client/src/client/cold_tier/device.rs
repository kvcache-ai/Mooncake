use super::super::{
    cold_tier_device_visible_in_cache, current_time_ms, pending_publish_deadline_ms,
    refresh_cold_tier_device_cache, ClientRuntimeId, ColdTierAdmission, ColdTierAdmissionBucket,
    ColdTierAdmissionOp, ColdTierAdmissionPermit, ColdTierAdmissionRejection,
    ColdTierDeviceAdmissionState, ColdTierDeviceManager, ColdTierRateLimitConfig, MetadataBackend,
    ObjectRoute, PersistentStorageBackend, Result, StorageOwnerColdTierConfig, StorageOwnerState,
    StoreError, DEFAULT_TRANSFER_STALL_TIMEOUT,
};
use super::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaWriteCandidate, DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};
use mooncake_store_core::{ColdTierDeviceRecord, ColdTierUsageDelta};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

/// Returns true if offload writes should be deferred when restore reads are
/// active on the same device. This prevents write bandwidth from competing
/// with latency-sensitive cold reads on the same SSD.
/// Number of cold-tier replicas to write per object.  Default 1 (no extra
/// replicas).  Set to 2 or 3 to spread read load across multiple devices.
fn cold_tier_replica_count() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("MC_STORE_RS_COLD_TIER_REPLICAS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(1)
            .clamp(1, 8)
    })
}

/// Returns the IO-score weight used in `device_offload_score()`.
///
/// When `MC_STORE_RS_COLD_TIER_THROTTLE_OFFLOAD_ON_RESTORE` is set (non-zero,
/// non-"false"), the weight is raised from the default 0.7 to 0.9 so offload
/// more aggressively avoids devices with concurrent restore reads. The
/// selection never hard-filters devices — even when all devices have active
/// restores, the least-busy one is still chosen to prevent offload starvation.
fn io_score_weight() -> f64 {
    static CACHED: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        let throttle = std::env::var("MC_STORE_RS_COLD_TIER_THROTTLE_OFFLOAD_ON_RESTORE")
            .map(|v| v != "0" && v != "false")
            .unwrap_or(false);
        if throttle {
            0.9
        } else {
            0.7
        }
    })
}

impl StorageOwnerState {
    pub(in super::super) fn new(
        runtime: ClientRuntimeId,
        route_ops: mooncake_store_route::RouteOperations,
        metadata: Arc<dyn MetadataBackend>,
        allocator: Arc<parking_lot::Mutex<super::super::LocalAllocatorState>>,
        state: Arc<parking_lot::Mutex<super::super::StoreState>>,
        cold_tier: StorageOwnerColdTierConfig,
    ) -> Self {
        let offload_mode = cold_tier.offload_mode;
        let offload_priority = cold_tier.offload_priority;
        let staging_pool_bytes = std::env::var("MC_STORE_RS_STAGING_POOL_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(cold_tier.rate_limits.staging_pool_bytes);
        let staging_slot_ttl =
            Duration::from_millis(cold_tier.rate_limits.staging_slot_ttl_ms.max(1));
        let restore_max_distinct_flights = cold_tier.rate_limits.restore_max_distinct_flights;
        Self {
            runtime,
            route_ops,
            metadata,
            allocator: allocator.clone(),
            state,
            cold_tier_devices: ColdTierDeviceManager::new(cold_tier),
            hot_replicas: super::super::HotReplicaTracker::default(),
            pending_offloads: super::super::ColdTierOffloadManager::new(offload_priority),
            cold_tier_cleanup: super::super::ColdTierCleanupManager::default(),
            initial_cold_backing_repair_at_ms: AtomicU64::new(0),
            stale_reclaim_scan_completed_at_ms: AtomicU64::new(0),
            offload_mode,
            offload_priority,
            owner_cold_restore_flights: super::super::OwnerColdRestoreFlightMap::new(
                restore_max_distinct_flights,
            ),
            cold_restore_io_tracker: super::super::ColdRestoreIoTracker::default(),
            eviction_ready_signal: super::super::EvictionReadySignal::default(),
            read_pin_registry: super::super::ReadPinRegistry::default(),
            staging_pool: parking_lot::Mutex::new(None),
            pending_staging_slots: parking_lot::Mutex::new(std::collections::HashMap::new()),
            staging_pool_bytes,
            staging_slot_ttl,
        }
    }

    fn select_cold_tier_device(&self, length: u64) -> Result<Option<ColdTierDeviceRecord>> {
        self.cold_tier_devices
            .select_for_offload(self.route_ops.directory().metadata(), length)
    }

    /// Select up to `n` distinct devices for multi-replica offload.
    fn select_cold_tier_devices(&self, length: u64, n: usize) -> Result<Vec<ColdTierDeviceRecord>> {
        self.cold_tier_devices.select_n_for_offload(
            self.route_ops.directory().metadata(),
            length,
            n,
        )
    }

    pub(in super::super) fn cold_tier_device_crosses_critical(
        &self,
        device: &ColdTierDeviceRecord,
        length: u64,
    ) -> bool {
        self.cold_tier_devices.crosses_critical(device, length)
    }

    pub(in super::super) fn backing_for_route(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
        allow_nof: bool,
    ) -> Result<Option<super::super::PendingBackingRoute>> {
        match self.offload_mode {
            super::super::ColdTierOffloadMode::Passthrough => {
                self.pending_backing_for_route(route, length, checksum, allow_nof)
            }
            super::super::ColdTierOffloadMode::EvictTriggered => Ok(None),
        }
    }

    pub(in super::super) fn pending_backing_for_route(
        &self,
        route: &ObjectRoute,
        length: u64,
        checksum: u64,
        allow_nof: bool,
    ) -> Result<Option<super::super::PendingBackingRoute>> {
        let replica_count = cold_tier_replica_count();
        if !self.cold_tier_devices.has_any_local_backend() {
            if !allow_nof {
                return Ok(None);
            }
            let backing = self
                .cold_tier_devices
                .nof_targets
                .pending_backing(route, length, checksum)?;
            return Ok(backing.map(super::super::PendingBackingRoute::Nof));
        }
        let select_devices = |s: &Self| -> Result<Vec<ColdTierDeviceRecord>> {
            if replica_count > 1 {
                s.select_cold_tier_devices(length, replica_count)
            } else {
                Ok(s.select_cold_tier_device(length)?.into_iter().collect())
            }
        };
        let mut devices = select_devices(self)?;
        if devices.is_empty() {
            // Inline GC: try to free cold tier space, then retry selection.
            let freed = self.try_inline_gc_for_offload(length)?;
            if freed > 0 {
                devices = select_devices(self)?;
            }
        }
        let Some(primary) = devices.first() else {
            if allow_nof {
                if let Some(backing) = self
                    .cold_tier_devices
                    .nof_targets
                    .pending_backing(route, length, checksum)?
                {
                    return Ok(Some(super::super::PendingBackingRoute::Nof(backing)));
                }
            }
            tracing::debug!(
                runtime = %self.runtime,
                key = %route.key.0,
                length,
                "cold_backing_device_unavailable: route remains hot-only"
            );
            return Ok(None);
        };
        let object_locator = format!(
            "{}@v{}",
            route
                .canonical_key
                .clone()
                .unwrap_or_else(|| route.key.0.clone()),
            route.version.0,
        );
        let owner = self
            .cold_tier_devices
            .current_target_owner(&primary.device_id, None)?;
        let replicas = devices[1..]
            .iter()
            .map(|device| {
                self.cold_tier_devices
                    .current_target_owner(&device.device_id, None)
                    .map(|owner| mooncake_store_core::ColdBackingReplica {
                        owner,
                        cold_tier_id: device.device_id.clone(),
                        object_locator: object_locator.clone(),
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if !replicas.is_empty() {
            tracing::debug!(
                runtime = %self.runtime,
                key = %route.key.0,
                primary_device = %primary.device_id,
                replica_devices = ?replicas.iter().map(|r| &r.cold_tier_id).collect::<Vec<_>>(),
                "cold_backing_multi_replica_selected"
            );
        }
        // Signal that a new entry needs offload — pressure tracker uses this
        // to throttle restore concurrency when the backlog grows.
        Ok(Some(super::super::PendingBackingRoute::Cold(
            mooncake_store_core::ColdBackingRoute {
                cold_tier_id: primary.device_id.clone(),
                owner,
                object_locator,
                length,
                checksum: Some(checksum),
                state: mooncake_store_core::ColdBackingState::PendingOffload,
                replicas,
            },
        )))
    }

    pub(in super::super) fn reserve_owner_restore_space(
        &self,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        // Fully synchronous eviction strategy — every loop iteration does actual
        // work (SSD write via inline flush), never idle-waits.
        //
        // 1. Fast path: allocate from free pool
        // 2. Clean evict-and-reserve: Materialized entry, zero I/O, direct slot reuse
        // 3. Pinned check: if all entries pinned by RDMA readers, return Backpressure
        //    (only ACK can free pins; waiting here deadlocks the reader's thread::scope)
        // 4. Inline flush: evict_one_blocking flushes a PendingOffload entry to SSD
        //    (~10ms actual I/O work), making it Materialized, then evicts it to free pool.
        //    Loop back to step 1 to grab the freed slot.
        for _ in 0..32usize {
            // 1. Fast path: free pool
            {
                let mut allocator = self.allocator.lock();
                match allocator.reserve_any(&self.runtime, length_bytes) {
                    Ok(reservation) => {
                        allocator.mark_pending_reservation(
                            &reservation,
                            pending_publish_deadline_ms(
                                reservation.length_bytes,
                                DEFAULT_TRANSFER_STALL_TIMEOUT,
                                None,
                            ),
                        );
                        return Ok(reservation);
                    }
                    Err(StoreError::Allocator(_)) => {}
                    Err(error) => return Err(error),
                }
            }

            // 2. Clean evict-and-reserve (Materialized, zero I/O, direct slot reuse)
            if let Some(reservation) =
                self.evict_one_clean_and_reserve_bounded(length_bytes, None)?
            {
                self.allocator.lock().mark_pending_allocation(
                    &reservation.segment_name,
                    reservation.offset_bytes,
                    reservation.length_bytes,
                    pending_publish_deadline_ms(
                        reservation.length_bytes,
                        DEFAULT_TRANSFER_STALL_TIMEOUT,
                        None,
                    ),
                );
                return Ok(reservation);
            }

            // 3. Pinned entries block progress — return Backpressure immediately.
            // Reader-side wave retry handles sequencing: RDMA+ACK frees pins
            // between waves, so the next wave finds free slots.
            if self.read_pin_registry.pinned_count() > 0 {
                return Err(StoreError::Backpressure(
                    "restore: eviction candidates pinned by active readers".into(),
                ));
            }

            // 4. Inline flush via existing evict_one_blocking:
            //    PendingOffload → SSD write → Materialized → evict → free to allocator.
            //    This is ~10ms of actual SSD work per iteration (not idle waiting).
            if !self.evict_one_blocking(None)? {
                break;
            }
            // Slot freed to allocator pool — loop back to step 1 to grab it.
        }

        Err(StoreError::Allocator(format!(
            "segment capacity exhausted for cold restore: requested={length_bytes}"
        )))
    }

    /// Reclaim staging slots whose readers never sent ACK (crash, network
    /// partition).  Called opportunistically when the staging pool is exhausted.
    /// Returns the number of slots reclaimed.
    pub(in super::super) fn sweep_expired_staging_slots(&self, ttl: Duration) -> usize {
        let now = Instant::now();
        let mut pending = self.pending_staging_slots.lock();
        let before = pending.len();
        pending.retain(|key, (_slot, created_at)| {
            if now.duration_since(*created_at) > ttl {
                // Force-unpin: the reader will never ACK, so clear the orphaned pin.
                self.read_pin_registry.force_unpin(&key.0, key.1);
                tracing::warn!(
                    segment = key.0 .0.as_str(),
                    offset = key.1,
                    age_s = now.duration_since(*created_at).as_secs(),
                    "staging slot TTL expired, force-releasing"
                );
                false // StagingSlot::drop → pool.release → notify waiters
            } else {
                true
            }
        });
        let swept = before - pending.len();
        if swept > 0 {
            super::super::registry::record_cold_restore_batch_items(
                "staging_slots_swept",
                swept as u64,
            );
            tracing::info!(
                runtime = %self.runtime,
                swept,
                "swept expired cold restore staging slots"
            );
        }
        swept
    }

    pub(in super::super) fn apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        used_bytes: i64,
        reserved_bytes: i64,
    ) -> Result<()> {
        let device = self.route_ops.metadata().apply_cold_tier_usage_delta(
            device_id,
            ColdTierUsageDelta {
                used_bytes,
                reserved_bytes,
            },
            current_time_ms(),
        )?;
        self.cold_tier_devices.upsert(device);
        Ok(())
    }
}

impl ColdTierAdmissionBucket {
    pub(in super::super) fn new(max_in_flight: usize, ops_per_sec: u32) -> Self {
        let ops_per_sec = ops_per_sec.max(1);
        Self {
            max_in_flight: max_in_flight.max(1),
            ops_per_sec,
            tokens: ops_per_sec as f64,
            last_refill: Instant::now(),
            in_flight: 0,
        }
    }

    pub(in super::super) fn try_acquire(&mut self, now: Instant) -> bool {
        self.try_acquire_with_limit(now, self.max_in_flight)
    }

    /// Like try_acquire but uses a dynamic effective limit instead of the
    /// configured max_in_flight. Used by the pressure-adaptive throttle.
    pub(in super::super) fn try_acquire_with_limit(
        &mut self,
        now: Instant,
        effective_limit: usize,
    ) -> bool {
        self.refill(now);
        if self.in_flight >= effective_limit || self.tokens < 1.0 {
            return false;
        }
        self.in_flight += 1;
        self.tokens -= 1.0;
        true
    }

    /// Track admission for observability without enforcing rate or concurrency
    /// limits.  Used by restore, which relies on staging-pool capacity as the
    /// natural backpressure mechanism instead of algorithmic limits.
    pub(in super::super) fn admit_unconditionally(&mut self) {
        self.in_flight += 1;
    }

    pub(in super::super) fn release(&mut self) {
        self.in_flight = self.in_flight.saturating_sub(1);
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        let capacity = self.ops_per_sec as f64;
        self.tokens = (self.tokens + elapsed * self.ops_per_sec as f64).min(capacity);
        self.last_refill = now;
    }
}

impl ColdTierDeviceAdmissionState {
    pub(in super::super) fn new(config: ColdTierRateLimitConfig) -> Self {
        Self {
            offload: ColdTierAdmissionBucket::new(
                config.offload_device_max_in_flight,
                config.offload_device_ops_per_sec,
            ),
            restore: ColdTierAdmissionBucket::new(
                config.restore_device_max_in_flight,
                config.restore_device_ops_per_sec,
            ),
            consecutive_errors: 0,
            paused_until: None,
            probe_in_flight: 0,
        }
    }
}

impl ColdTierAdmission {
    pub(in super::super) fn new(config: ColdTierRateLimitConfig) -> Self {
        let pressure = super::super::OffloadPressureTracker::new(&config);
        Self {
            config,
            runtime_offload: parking_lot::Mutex::new(ColdTierAdmissionBucket::new(
                config.offload_runtime_max_in_flight,
                config.offload_runtime_ops_per_sec,
            )),
            runtime_restore: parking_lot::Mutex::new(ColdTierAdmissionBucket::new(
                config.restore_runtime_max_in_flight,
                config.restore_runtime_ops_per_sec,
            )),
            devices: parking_lot::Mutex::new(BTreeMap::new()),
            pressure,
        }
    }

    pub(in super::super) fn try_acquire(
        self: &Arc<Self>,
        device_id: &str,
        op: ColdTierAdmissionOp,
    ) -> std::result::Result<ColdTierAdmissionPermit, ColdTierAdmissionRejection> {
        let now = Instant::now();
        let is_restore = matches!(op, ColdTierAdmissionOp::Restore);

        // ---------------------------------------------------------------
        // Runtime-level gate
        // ---------------------------------------------------------------
        // Restore: track in-flight for observability only — never reject.
        //   Natural backpressure comes from the staging pool (256 MiB)
        //   and io_uring queue depth, not algorithmic limits.
        // Offload: full token-bucket + in-flight admission (unchanged).
        // ---------------------------------------------------------------
        let mut runtime = match op {
            ColdTierAdmissionOp::Offload => self.runtime_offload.lock(),
            ColdTierAdmissionOp::Restore => self.runtime_restore.lock(),
        };
        if is_restore {
            runtime.admit_unconditionally();
        } else if !runtime.try_acquire(now) {
            return Err(ColdTierAdmissionRejection::RuntimeLimit);
        }
        drop(runtime);

        let mut devices = self.devices.lock();
        let device = devices
            .entry(device_id.to_string())
            .or_insert_with(|| ColdTierDeviceAdmissionState::new(self.config));

        // ---------------------------------------------------------------
        // Device pause (health protection after consecutive errors)
        // ---------------------------------------------------------------
        // Restore: skip — the SSD read will surface the error naturally,
        //   and rejecting here was the root cause of the 24% fail-fast
        //   rejection storm in previous E2E tests.
        // Offload: enforce pause + probe logic (unchanged).
        // ---------------------------------------------------------------
        let mut probe = false;
        if !is_restore {
            if let Some(paused_until) = device.paused_until {
                if now < paused_until {
                    self.runtime_offload.lock().release();
                    return Err(ColdTierAdmissionRejection::DevicePaused);
                }
                if device.probe_in_flight >= self.config.device_probe_batch.max(1) {
                    self.runtime_offload.lock().release();
                    return Err(ColdTierAdmissionRejection::DevicePaused);
                }
                device.probe_in_flight += 1;
                probe = true;
            }
        }

        // ---------------------------------------------------------------
        // Device-level gate
        // ---------------------------------------------------------------
        // Restore: track only.
        // Offload: token-bucket + pressure-boosted limit.
        // ---------------------------------------------------------------
        let bucket = match op {
            ColdTierAdmissionOp::Offload => &mut device.offload,
            ColdTierAdmissionOp::Restore => &mut device.restore,
        };
        if is_restore {
            bucket.admit_unconditionally();
        } else {
            let effective_limit = self.pressure.effective_offload_limit(bucket.max_in_flight);
            if !bucket.try_acquire_with_limit(now, effective_limit) {
                if device.paused_until.is_some() {
                    device.probe_in_flight = device.probe_in_flight.saturating_sub(1);
                }
                drop(devices);
                self.runtime_offload.lock().release();
                return Err(ColdTierAdmissionRejection::DeviceLimit);
            }
        }

        Ok(ColdTierAdmissionPermit {
            admission: self.clone(),
            device_id: device_id.to_string(),
            op,
            probe,
            released: AtomicBool::new(false),
        })
    }

    fn release(&self, permit: &ColdTierAdmissionPermit) {
        if permit.released.swap(true, Ordering::AcqRel) {
            return;
        }
        match permit.op {
            ColdTierAdmissionOp::Offload => self.runtime_offload.lock().release(),
            ColdTierAdmissionOp::Restore => self.runtime_restore.lock().release(),
        }
        let mut devices = self.devices.lock();
        if let Some(device) = devices.get_mut(&permit.device_id) {
            match permit.op {
                ColdTierAdmissionOp::Offload => device.offload.release(),
                ColdTierAdmissionOp::Restore => device.restore.release(),
            }
            if permit.probe {
                device.probe_in_flight = device.probe_in_flight.saturating_sub(1);
            }
        }
    }

    pub(in super::super) fn complete_ok(&self, permit: &ColdTierAdmissionPermit) {
        let mut devices = self.devices.lock();
        if let Some(device) = devices.get_mut(&permit.device_id) {
            if device.paused_until.is_some() {
                tracing::info!(
                    device_id = %permit.device_id,
                    operation = permit.op.operation_name(),
                    "cold tier device resumed after probe success"
                );
                super::super::registry::record_cold_tier_operation(
                    permit.op.operation_name(),
                    "device_resumed",
                    "probe_success",
                );
            }
            device.consecutive_errors = 0;
            device.paused_until = None;
        }
        drop(devices);
        self.release(permit);
    }

    pub(in super::super) fn complete_error(&self, permit: &ColdTierAdmissionPermit) {
        let mut devices = self.devices.lock();
        if let Some(device) = devices.get_mut(&permit.device_id) {
            device.consecutive_errors = device.consecutive_errors.saturating_add(1);
            if device.consecutive_errors >= self.config.device_pause_after_errors.max(1) {
                device.paused_until = Some(
                    Instant::now() + Duration::from_millis(self.config.device_pause_ms.max(1)),
                );
                tracing::warn!(
                    device_id = %permit.device_id,
                    operation = permit.op.operation_name(),
                    consecutive_errors = device.consecutive_errors,
                    pause_ms = self.config.device_pause_ms,
                    "cold tier device paused after backend errors"
                );
                super::super::registry::record_cold_tier_operation(
                    permit.op.operation_name(),
                    "device_paused",
                    "backend_error",
                );
            }
        }
        drop(devices);
        self.release(permit);
    }
}

impl ColdTierAdmissionRejection {
    pub(in super::super) fn metric_label(self) -> &'static str {
        match self {
            ColdTierAdmissionRejection::RuntimeLimit => "runtime_limit",
            ColdTierAdmissionRejection::DeviceLimit => "device_limit",
            ColdTierAdmissionRejection::DevicePaused => "device_paused",
        }
    }
}

impl ColdTierAdmission {
    /// Returns the number of in-flight restore reads on a specific device.
    /// Used by the device selector to avoid sending offload writes to SSDs
    /// that are currently busy serving restore reads.
    pub(in super::super) fn device_restore_in_flight(&self, device_id: &str) -> usize {
        self.devices
            .lock()
            .get(device_id)
            .map(|state| state.restore.in_flight)
            .unwrap_or(0)
    }

    /// Non-blocking snapshot of per-device inflight restore counts.
    /// Returns `None` if the lock is contended — callers should degrade
    /// gracefully to other balancing signals.
    pub(in super::super) fn try_snapshot_device_inflight(&self) -> Option<Vec<(String, u64)>> {
        let guard = self.devices.try_lock()?;
        Some(
            guard
                .iter()
                .map(|(id, state)| (id.clone(), state.restore.in_flight as u64))
                .collect(),
        )
    }
}

impl ColdTierAdmissionOp {
    fn operation_name(self) -> &'static str {
        match self {
            ColdTierAdmissionOp::Offload => "offload",
            ColdTierAdmissionOp::Restore => "restore",
        }
    }
}

impl ColdTierAdmissionPermit {
    pub(in super::super) fn complete_ok(&self) {
        self.admission.complete_ok(self);
    }

    pub(in super::super) fn complete_error(&self) {
        self.admission.complete_error(self);
    }
}

impl Drop for ColdTierAdmissionPermit {
    fn drop(&mut self) {
        let admission = self.admission.clone();
        admission.release(self);
    }
}

/// Free percentage for device selection.  Returns 0.0–100.0 when capacity is
/// known, or -1.0 for unknown/zero capacity so those devices sort last.
pub(crate) fn cold_tier_free_percentage(device: &ColdTierDeviceRecord) -> f64 {
    match device.capacity_bytes {
        Some(cap) if cap > 0 => {
            let used = device.used_bytes.saturating_add(device.reserved_bytes);
            cap.saturating_sub(used) as f64 / cap as f64 * 100.0
        }
        _ => -1.0,
    }
}

impl ColdTierDeviceManager {
    pub(in super::super) fn new(config: StorageOwnerColdTierConfig) -> Self {
        Self {
            resolver: parking_lot::Mutex::new(config.resolver),
            nof_targets: config.nof_targets,
            devices: config.devices,
            watermarks: config.watermarks,
            admission: Arc::new(ColdTierAdmission::new(config.rate_limits)),
            runtime: config.runtime,
        }
    }

    /// Expose admission for non-blocking inflight snapshot in target selection.
    pub(in super::super) fn admission(&self) -> &ColdTierAdmission {
        &self.admission
    }

    /// Returns true if the local resolver has a backend registered for the
    /// given `cold_tier_id`. Used to determine data locality without relying
    /// on owner identity comparison.
    pub(in super::super) fn has_local_backend(&self, cold_tier_id: &str) -> bool {
        self.resolver.lock().has_backend(cold_tier_id)
    }

    /// Returns true if this runtime has at least one local cold-tier backend.
    pub(in super::super) fn has_any_local_backend(&self) -> bool {
        self.resolver.lock().has_any_backend()
    }

    pub(in super::super) fn has_any_persistent_backend(&self) -> bool {
        self.has_any_local_backend() || !self.nof_targets.is_empty()
    }

    pub(in super::super) fn has_nof_backend(&self, target_id: &str) -> bool {
        self.nof_targets.contains(target_id)
    }

    pub(in super::super) fn has_runtime_backend(&self, target_id: &str) -> bool {
        self.has_local_backend(target_id) || self.has_nof_backend(target_id)
    }

    pub(in super::super) fn runtime_backend_available(&self, target_id: &str) -> bool {
        self.has_local_backend(target_id) || self.nof_targets.available_for_io(target_id)
    }

    /// Returns the set of cold_tier_ids that have local backends registered.
    pub(in super::super) fn local_device_ids(&self) -> Vec<String> {
        self.resolver.lock().backend_ids()
    }

    /// Returns the cached device state for the given cold_tier_id, if known.
    /// Checks both `device_id` and `cold_tier_id` fields in cached records.
    pub(in super::super) fn device_state(
        &self,
        cold_tier_id: &str,
    ) -> Option<mooncake_store_core::ColdTierDeviceState> {
        self.devices
            .lock()
            .devices
            .iter()
            .find(|d| d.device_id == cold_tier_id || d.cold_tier_id == cold_tier_id)
            .map(|d| d.state)
    }

    fn select_for_offload(
        &self,
        metadata: &dyn MetadataBackend,
        length: u64,
    ) -> Result<Option<ColdTierDeviceRecord>> {
        let snapshot = match self.devices.lock().snapshot() {
            Some(devices) => devices,
            None => refresh_cold_tier_device_cache(
                metadata,
                &self.devices,
                "cold_tier_device_snapshot_admission",
            )?,
        };
        match self.select_from(snapshot, length) {
            Some(device) => Ok(Some(device)),
            None => {
                let refreshed = refresh_cold_tier_device_cache(
                    metadata,
                    &self.devices,
                    "cold_tier_device_snapshot_admission_retry",
                )?;
                Ok(self.select_from(refreshed, length))
            }
        }
    }

    fn select_n_for_offload(
        &self,
        metadata: &dyn MetadataBackend,
        length: u64,
        n: usize,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        let snapshot = match self.devices.lock().snapshot() {
            Some(devices) => devices,
            None => refresh_cold_tier_device_cache(
                metadata,
                &self.devices,
                "cold_tier_device_snapshot_admission_n",
            )?,
        };
        let selected = self.select_n_from(snapshot, length, n);
        if !selected.is_empty() {
            return Ok(selected);
        }
        let refreshed = refresh_cold_tier_device_cache(
            metadata,
            &self.devices,
            "cold_tier_device_snapshot_admission_n_retry",
        )?;
        Ok(self.select_n_from(refreshed, length, n))
    }

    pub(in super::super) fn has_usable_device(
        &self,
        metadata: &dyn MetadataBackend,
    ) -> Result<bool> {
        let snapshot = match self.devices.lock().snapshot() {
            Some(devices) => devices,
            None => refresh_cold_tier_device_cache(
                metadata,
                &self.devices,
                "cold_tier_device_snapshot_eviction",
            )?,
        };
        Ok(snapshot
            .into_iter()
            .any(|device| self.usable_device(&device)))
    }

    fn select_from(
        &self,
        devices: Vec<ColdTierDeviceRecord>,
        length: u64,
    ) -> Option<ColdTierDeviceRecord> {
        self.select_n_from(devices, length, 1).into_iter().next()
    }

    /// Select up to `n` distinct devices for multi-replica offload, ordered by
    /// descending offload score.  Returns at least 1 device if any are eligible.
    fn select_n_from(
        &self,
        devices: Vec<ColdTierDeviceRecord>,
        length: u64,
        n: usize,
    ) -> Vec<ColdTierDeviceRecord> {
        if devices.is_empty() || n == 0 {
            return Vec::new();
        }
        let eligible: Vec<_> = devices
            .into_iter()
            .filter(|device| self.offload_rejection_reason(device, length).is_none())
            .collect();
        let candidates = eligible
            .iter()
            .map(|device| ReplicaWriteCandidate {
                target_id: &device.device_id,
                score: self.device_offload_score(device),
                accumulated_writes: 0,
            })
            .collect::<Vec<_>>();
        DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY
            .select_write_targets(&candidates, n)
            .into_iter()
            .map(|index| eligible[index].clone())
            .collect()
    }

    /// Compute a composite score for how suitable a device is for offload writes.
    /// Higher score = better candidate.
    fn device_offload_score(&self, device: &ColdTierDeviceRecord) -> f64 {
        let free_pct = cold_tier_free_percentage(device);
        if free_pct < 0.0 {
            // Unknown capacity — sort last.
            return -1.0;
        }
        let capacity_score = free_pct / 100.0; // 0.0 – 1.0
        let restore_in_flight = self.admission.device_restore_in_flight(&device.device_id);
        let io_score = 1.0 / (1.0 + restore_in_flight as f64); // 0.0 – 1.0
        let weight = io_score_weight();
        (1.0 - weight) * capacity_score + weight * io_score
    }

    fn usable_device(&self, device: &ColdTierDeviceRecord) -> bool {
        self.offload_rejection_reason(device, 0).is_none()
    }

    pub(in super::super) fn accepts_offload(
        &self,
        device: &ColdTierDeviceRecord,
        length: u64,
    ) -> bool {
        self.offload_rejection_reason(device, length).is_none()
    }

    pub(in super::super) fn offload_rejection_reason(
        &self,
        device: &ColdTierDeviceRecord,
        length: u64,
    ) -> Option<&'static str> {
        if !cold_tier_device_visible_in_cache(device) {
            return Some("not_visible_in_cache");
        }
        if device.stable_id != self.runtime.stable_id.0
            || device.epoch != Some(self.runtime.epoch.0)
        {
            return Some("not_current_runtime_epoch");
        }
        if !device.schedulable() {
            return Some("not_schedulable");
        }
        if device.root_dir.is_none() {
            return Some("missing_root_dir");
        }
        if !self.resolver.lock().has_backend(&device.device_id) {
            return Some("backend_not_registered");
        }
        let projected = device
            .used_bytes
            .saturating_add(device.reserved_bytes)
            .saturating_add(length)
            .saturating_add(self.watermarks.reserve_bytes);
        if let Some(capacity) = device.capacity_bytes {
            if projected > capacity {
                return Some("insufficient_capacity");
            }
        }
        None
    }

    fn crosses_critical(&self, device: &ColdTierDeviceRecord, length: u64) -> bool {
        let Some(critical) = self.watermarks.critical_bytes else {
            return false;
        };
        device
            .used_bytes
            .saturating_add(device.reserved_bytes)
            .saturating_add(length)
            .saturating_add(self.watermarks.reserve_bytes)
            >= critical
    }

    pub(in super::super) fn low_bytes(&self) -> Option<u64> {
        self.watermarks.low_bytes
    }

    pub(in super::super) fn high_bytes(&self) -> Option<u64> {
        self.watermarks.high_bytes
    }

    pub(in super::super) fn try_acquire_offload(
        &self,
        device_id: &str,
    ) -> std::result::Result<ColdTierAdmissionPermit, ColdTierAdmissionRejection> {
        self.admission
            .try_acquire(device_id, ColdTierAdmissionOp::Offload)
    }

    pub(in super::super) fn try_acquire_restore(
        &self,
        device_id: &str,
    ) -> std::result::Result<ColdTierAdmissionPermit, ColdTierAdmissionRejection> {
        self.admission
            .try_acquire(device_id, ColdTierAdmissionOp::Restore)
    }

    pub(in super::super) fn foreground_offload_kick_batch(&self) -> usize {
        self.admission.config.foreground_offload_kick_batch.max(1)
    }

    pub(in super::super) fn pressure_increment_pending(&self) {
        self.admission.pressure.increment_pending();
    }

    pub(in super::super) fn pressure_decrement_pending(&self) {
        self.admission.pressure.decrement_pending();
    }

    pub(in super::super) fn pressure_refresh_depth(&self, actual: usize) {
        self.admission.pressure.refresh_depth(actual);
    }

    pub(in super::super) fn backend_for(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Arc<dyn PersistentStorageBackend>> {
        if self.nof_targets.contains(&cold_backing.cold_tier_id) {
            return self.nof_targets.backend_for(&cold_backing.cold_tier_id);
        }
        self.backend_for_from_snapshot(cold_backing, self.devices.lock().snapshot())
    }

    pub(in super::super) fn backend_for_with_refresh(
        &self,
        metadata: &dyn MetadataBackend,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        operation: &'static str,
    ) -> Result<Arc<dyn PersistentStorageBackend>> {
        if self.nof_targets.contains(&cold_backing.cold_tier_id) {
            return self.nof_targets.backend_for(&cold_backing.cold_tier_id);
        }
        match self.backend_for(cold_backing) {
            Ok(backend) => Ok(backend),
            Err(first_error) => {
                let refreshed = refresh_cold_tier_device_cache(metadata, &self.devices, operation)?;
                self.backend_for_from_snapshot(cold_backing, Some(refreshed))
                    .map_err(|_| first_error)
            }
        }
    }

    fn backend_for_from_snapshot(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        snapshot: Option<Vec<ColdTierDeviceRecord>>,
    ) -> Result<Arc<dyn PersistentStorageBackend>> {
        {
            let resolver = self.resolver.lock();
            if resolver.has_backend(&cold_backing.cold_tier_id) {
                return resolver.backend_for(cold_backing);
            }
        }

        let device = snapshot
            .and_then(|devices| {
                devices.into_iter().find(|device| {
                    device.device_id == cold_backing.cold_tier_id
                        || device.cold_tier_id == cold_backing.cold_tier_id
                })
            })
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "cold tier backend {} is not registered",
                    cold_backing.cold_tier_id
                ))
            })?;
        let backend = super::super::cold_tier_backend_from_device_record(&device)?;
        let mut resolver = self.resolver.lock();
        if !resolver.has_backend(&device.device_id) {
            resolver.insert_backend(device.device_id.clone(), backend.clone());
        }
        if device.cold_tier_id != device.device_id && !resolver.has_backend(&device.cold_tier_id) {
            resolver.insert_backend(device.cold_tier_id.clone(), backend.clone());
        }
        resolver.backend_for(cold_backing)
    }

    pub(in super::super) fn upsert(&self, device: ColdTierDeviceRecord) {
        self.devices.lock().upsert(device);
    }

    pub(in super::super) fn observability_snapshot(&self) -> super::super::ColdTierDeviceSnapshot {
        self.devices.lock().observability_snapshot()
    }

    #[cfg(test)]
    pub(in super::super) fn cache(&self) -> &super::super::SharedColdTierDeviceCache {
        &self.devices
    }
}
