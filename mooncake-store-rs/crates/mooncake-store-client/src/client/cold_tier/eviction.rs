use super::super::{
    is_missing_live_allocation_error, refresh_cold_tier_device_cache, ClockEntryId,
    DebugEvictAllResult, ObjectRoute, OperationTracker, PendingOffloadPrepareOutcome,
    RestorePromotionQueue, Result, RouteState, SegmentName, StorageOwnerState,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar as StdCondvar, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

#[derive(Clone, Copy, Debug, Default)]
struct DebugEvictOneResult {
    evicted: bool,
    dropped_without_cold: bool,
}

impl StorageOwnerState {
    pub(in super::super) fn evict_all(
        &self,
        restore_promotions: &RestorePromotionQueue,
    ) -> Result<DebugEvictAllResult> {
        const MAX_ERROR_RETRIES: usize = 3;
        const MAX_NO_PROGRESS_RETRIES: usize = 100;
        const NO_PROGRESS_RETRY_DELAY: Duration = Duration::from_millis(10);
        const DEBUG_EVICT_ALL_DEADLINE: Duration = Duration::from_secs(25);

        let started = Instant::now();
        let mut result = DebugEvictAllResult::default();
        let mut error_retries = 0usize;
        let mut no_progress_retries = 0usize;
        self.rebuild_clock()?;
        loop {
            if started.elapsed() >= DEBUG_EVICT_ALL_DEADLINE {
                result.timed_out = true;
                result.stopped_reason = Some("timeout".to_string());
                break;
            }
            match self.evict_one_blocking_for_debug_evict_all(None, false) {
                Ok(outcome) if outcome.evicted => {
                    result.evicted = result.evicted.saturating_add(1);
                    if outcome.dropped_without_cold {
                        result.dropped_without_cold = result.dropped_without_cold.saturating_add(1);
                    }
                    error_retries = 0;
                    no_progress_retries = 0;
                }
                Ok(_) if !self.has_hot_replicas_for_debug_evict_all()? => break,
                Ok(_) if no_progress_retries < MAX_NO_PROGRESS_RETRIES => {
                    no_progress_retries += 1;
                    restore_promotions.wait_for_worker_idle();
                    let repaired = self.repair_stuck_pending_offloads_for_debug_evict_all()?;
                    result.repaired = result.repaired.saturating_add(repaired);
                    warn!(
                        runtime = %self.runtime,
                        evicted = result.evicted,
                        repaired,
                        attempt = no_progress_retries,
                        max_attempts = MAX_NO_PROGRESS_RETRIES,
                        "debug evict_all retrying after no-progress scan with hot replicas remaining"
                    );
                    std::thread::sleep(NO_PROGRESS_RETRY_DELAY);
                    self.rebuild_clock()?;
                }
                Ok(_) => {
                    let forced = self.evict_one_blocking_for_debug_evict_all(None, true)?;
                    if forced.evicted {
                        result.evicted = result.evicted.saturating_add(1);
                        if forced.dropped_without_cold {
                            result.dropped_without_cold =
                                result.dropped_without_cold.saturating_add(1);
                        }
                        error_retries = 0;
                        no_progress_retries = 0;
                        self.rebuild_clock()?;
                        continue;
                    }
                    result.stopped_reason = Some("no_progress".to_string());
                    break;
                }
                Err(error) if error_retries < MAX_ERROR_RETRIES => {
                    error_retries += 1;
                    result.last_error = Some(error.to_string());
                    warn!(
                        runtime = %self.runtime,
                        evicted = result.evicted,
                        attempt = error_retries,
                        max_attempts = MAX_ERROR_RETRIES,
                        error = %error,
                        "debug evict_all retrying after eviction error"
                    );
                    self.rebuild_clock()?;
                }
                Err(error) => {
                    result.last_error = Some(error.to_string());
                    let forced = self.evict_one_blocking_for_debug_evict_all(None, true)?;
                    if forced.evicted {
                        result.evicted = result.evicted.saturating_add(1);
                        if forced.dropped_without_cold {
                            result.dropped_without_cold =
                                result.dropped_without_cold.saturating_add(1);
                        }
                        error_retries = 0;
                        no_progress_retries = 0;
                        self.rebuild_clock()?;
                        continue;
                    }
                    result.stopped_reason = Some("eviction_error".to_string());
                    break;
                }
            }
        }
        result.remaining_hot_replicas = self.hot_replica_count_for_debug_evict_all()?;
        result.completed = result.remaining_hot_replicas == 0;
        if result.completed {
            result.stopped_reason = None;
        }
        info!(
            runtime = %self.runtime,
            evicted = result.evicted,
            completed = result.completed,
            remaining_hot_replicas = result.remaining_hot_replicas,
            dropped_without_cold = result.dropped_without_cold,
            timed_out = result.timed_out,
            "debug evict_all completed"
        );
        Ok(result)
    }

    fn evict_one_blocking_for_debug_evict_all(
        &self,
        preferred_segment: Option<&SegmentName>,
        force_drop_without_cold: bool,
    ) -> Result<DebugEvictOneResult> {
        let tracker = OperationTracker::new("storage_owner_debug_evict_one_force_drop");
        let result = (|| {
            if self.reclaim_stale_local_allocations_before_clock_eviction(preferred_segment)? > 0 {
                return Ok(DebugEvictOneResult {
                    evicted: true,
                    dropped_without_cold: false,
                });
            }
            for rebuild in 0..=1usize {
                let budget = {
                    let clock = self.hot_replicas.clock.lock();
                    clock.eviction_budget()
                };
                for _ in 0..budget.max(1) {
                    let victim = {
                        let mut clock = self.hot_replicas.clock.lock();
                        clock.pick_victim(
                            preferred_segment,
                            self.offload_priority.eviction_policy,
                            self.offload_priority.eviction_scan_limit,
                        )
                    };
                    let Some(victim) = victim else {
                        break;
                    };
                    let outcome =
                        self.evict_candidate_for_debug_evict_all(&victim, force_drop_without_cold)?;
                    if outcome.evicted {
                        return Ok(outcome);
                    }
                }
                if rebuild == 0 {
                    self.rebuild_clock()?;
                }
            }
            Ok(DebugEvictOneResult::default())
        })();
        tracker.finish(
            &result,
            u64::from(result.as_ref().map(|r| r.evicted).unwrap_or(false)),
        );
        result
    }

    fn evict_candidate_for_debug_evict_all(
        &self,
        victim: &ClockEntryId,
        force_drop_without_cold: bool,
    ) -> Result<DebugEvictOneResult> {
        let Some(route) = self.load_victim_route_bounded(&victim.route_key)? else {
            self.hot_replicas.clock.lock().remove_id(victim);
            return Ok(DebugEvictOneResult::default());
        };
        if route.state != RouteState::Active {
            if self.reclaim_stale_local_allocations_for_key(&victim.route_key, Some(&route))? > 0 {
                return Ok(DebugEvictOneResult {
                    evicted: true,
                    dropped_without_cold: false,
                });
            }
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(&victim.route_key));
            return Ok(DebugEvictOneResult::default());
        }
        let Some(replica_index) = self.replica_index_for_victim(&route, victim) else {
            if self.reclaim_stale_local_allocations_for_key(&victim.route_key, Some(&route))? > 0 {
                return Ok(DebugEvictOneResult {
                    evicted: true,
                    dropped_without_cold: false,
                });
            }
            self.sync_route(&route);
            return Ok(DebugEvictOneResult::default());
        };
        let replica = &route.replicas[replica_index];
        if self
            .read_pin_registry
            .is_pinned(&replica.segment_name, replica.segment_offset)
        {
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(&victim.route_key));
            return Ok(DebugEvictOneResult::default());
        }

        let has_usable_cold_tier_device = self.has_usable_cold_tier_device()?;
        let cold_backing_not_on_disk = route.cold_backing.is_none()
            || route.cold_backing.as_ref().is_some_and(|backing| {
                backing.state == mooncake_store_core::ColdBackingState::PendingOffload
            });
        if cold_backing_not_on_disk && (!has_usable_cold_tier_device || force_drop_without_cold) {
            return self.evict_route_replica_for_debug_evict_all(victim, &route, replica_index);
        }

        let Some(route) = self.ensure_materialized_cold_backing_for_eviction(&route)? else {
            return Ok(DebugEvictOneResult::default());
        };
        let Some(replica_index) = self.replica_index_for_victim(&route, victim) else {
            if self.reclaim_stale_local_allocations_for_key(&victim.route_key, Some(&route))? > 0 {
                return Ok(DebugEvictOneResult {
                    evicted: true,
                    dropped_without_cold: false,
                });
            }
            self.sync_route(&route);
            return Ok(DebugEvictOneResult::default());
        };
        Ok(DebugEvictOneResult {
            evicted: self.evict_route_replica(victim, &route, replica_index, false)?,
            dropped_without_cold: false,
        })
    }

    fn evict_route_replica_for_debug_evict_all(
        &self,
        victim: &ClockEntryId,
        route: &ObjectRoute,
        replica_index: usize,
    ) -> Result<DebugEvictOneResult> {
        let evicted_replica = route.replicas[replica_index].clone();
        let (next, dropped_without_cold) = debug_route_after_replica_eviction(route, replica_index);
        let cas = match next.as_ref() {
            Some(next_route) => {
                self.route_ops
                    .prune_route(&route.key, Some(route.version), next_route)?
            }
            None => self
                .route_ops
                .delete_route(&route.key, Some(route.version))?,
        };
        if !cas.applied {
            match cas.current.as_ref() {
                Some(current) => self.sync_route(current),
                None => self.hot_replicas.clock.lock().remove_id(victim),
            }
            return Ok(DebugEvictOneResult::default());
        }

        if dropped_without_cold {
            self.pending_offloads
                .discard_for_debug_evict_all(&route.key);
        }
        self.hot_replicas.clock.lock().remove_id(victim);
        if let Err(error) = self.allocator.lock().release(
            &self.runtime,
            &evicted_replica.segment_name,
            evicted_replica.segment_offset,
            evicted_replica.length,
        ) {
            if is_missing_live_allocation_error(&error) {
                warn!(
                    runtime = %self.runtime,
                    key = %route.key.0,
                    segment = %evicted_replica.segment_name.0,
                    offset_bytes = evicted_replica.segment_offset,
                    length_bytes = evicted_replica.length,
                    error = %error,
                    "debug_dram_eviction_allocator_release_already_absent"
                );
            } else {
                return Err(error);
            }
        }
        if let Some(next_route) = &next {
            self.sync_route(next_route);
        }
        info!(
            runtime = %self.runtime,
            key = %route.key.0,
            segment = %evicted_replica.segment_name.0,
            offset_bytes = evicted_replica.segment_offset,
            length_bytes = evicted_replica.length,
            dropped_without_cold,
            route_deleted = next.is_none(),
            "debug_dram_eviction_complete"
        );
        Ok(DebugEvictOneResult {
            evicted: true,
            dropped_without_cold,
        })
    }

    fn has_hot_replicas_for_debug_evict_all(&self) -> Result<bool> {
        Ok(self.hot_replica_count_for_debug_evict_all()? > 0)
    }

    fn hot_replica_count_for_debug_evict_all(&self) -> Result<usize> {
        let mut count = 0usize;
        self.route_ops
            .visit_routes_by_replica_owner(&self.runtime, &mut |_| {
                count = count.saturating_add(1);
                Ok(())
            })?;
        Ok(count)
    }

    fn repair_stuck_pending_offloads_for_debug_evict_all(&self) -> Result<usize> {
        let mut repaired = 0usize;
        let offload_worker_idle = !self.pending_offloads.is_materializing();
        self.route_ops
            .visit_routes_by_replica_owner(&self.runtime, &mut |route| {
                if route.state != RouteState::Active {
                    self.sync_route(&route);
                    return Ok(());
                }
                if !route
                    .replicas
                    .iter()
                    .any(|replica| replica.owner == self.runtime)
                {
                    self.sync_route(&route);
                    return Ok(());
                }
                let Some(cold_backing) = route.cold_backing.clone() else {
                    return Ok(());
                };
                if cold_backing.owner != self.runtime
                    || cold_backing.state != mooncake_store_core::ColdBackingState::PendingOffload
                {
                    return Ok(());
                }
                if super::cold_tier_disabled() {
                    super::offload::clear_unavailable_pending_cold_backing(
                        self,
                        &route,
                        &cold_backing,
                        "debug_evict_all_cold_tier_disabled",
                        None,
                    )?;
                    repaired = repaired.saturating_add(1);
                    return Ok(());
                }
                if let Err(error) = self.cold_tier_devices.backend_for(&cold_backing) {
                    super::offload::clear_unavailable_pending_cold_backing(
                        self,
                        &route,
                        &cold_backing,
                        "debug_evict_all_backend_unavailable",
                        Some(&error),
                    )?;
                    repaired = repaired.saturating_add(1);
                    return Ok(());
                }
                if offload_worker_idle
                    && self.pending_offloads.requeue_for_debug_evict_all(
                        route.key.clone(),
                        route.version,
                        Some(cold_backing.length),
                    )
                {
                    repaired = repaired.saturating_add(1);
                    warn!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        route_version = route.version.0,
                        "debug evict_all requeued stuck pending offload"
                    );
                }
                Ok(())
            })?;
        Ok(repaired)
    }
}

/// Notification channel: offload thread signals cold restore waiters
/// when entries become Materialized (evictable without I/O).
pub(in super::super) struct EvictionReadySignal {
    pub(in super::super) generation: AtomicU64,
    condvar: StdCondvar,
    mutex: StdMutex<()>,
}

impl Default for EvictionReadySignal {
    fn default() -> Self {
        Self {
            generation: AtomicU64::new(0),
            condvar: StdCondvar::new(),
            mutex: StdMutex::new(()),
        }
    }
}

impl EvictionReadySignal {
    /// Called by offload thread after an entry becomes Materialized.
    pub(in super::super) fn notify_materialized(&self) {
        self.generation.fetch_add(1, Ordering::Release);
        self.condvar.notify_all();
    }

    pub(in super::super) fn signal(&self) {
        self.notify_materialized();
    }

    /// Wait until a new Materialized entry is available, or timeout.
    #[allow(dead_code)]
    pub(in super::super) fn wait_for_materialized(&self, timeout: Duration) -> bool {
        let guard = self.mutex.lock().unwrap_or_else(|e| e.into_inner());
        let gen_before = self.generation.load(Ordering::Acquire);
        let (_guard, result) = self
            .condvar
            .wait_timeout(guard, timeout)
            .unwrap_or_else(|e| e.into_inner());
        if result.timed_out() {
            // Check if generation advanced during our wait (spurious wakeup protection)
            self.generation.load(Ordering::Acquire) != gen_before
        } else {
            true
        }
    }
}

fn debug_route_after_replica_eviction(
    route: &ObjectRoute,
    replica_index: usize,
) -> (Option<ObjectRoute>, bool) {
    let keep_cold_backing = route.cold_backing.as_ref().is_some_and(|backing| {
        backing.state == mooncake_store_core::ColdBackingState::Materialized
    });
    let dropped_without_cold = !keep_cold_backing;
    if route.replicas.len() == 1 && dropped_without_cold {
        return (None, true);
    }

    let mut next = route.clone();
    next.version = next.version.next();
    if route.replicas.len() == 1 {
        next.replicas = Vec::new();
        return (Some(next), false);
    }

    next.replicas.remove(replica_index);
    next.replicas.sort_by_key(|replica| replica.priority);
    for (priority, replica) in next.replicas.iter_mut().enumerate() {
        replica.priority = priority as u16;
    }
    if dropped_without_cold {
        next.cold_backing = None;
    }
    (Some(next), dropped_without_cold)
}

pub(in super::super) fn should_delete_route_after_last_replica_eviction(
    route: &ObjectRoute,
) -> bool {
    !super::has_materialized_cold_backing(route)
}

pub(in super::super) fn route_after_replica_eviction(
    route: &ObjectRoute,
    replica_index: usize,
    delete_empty_route: bool,
) -> Option<ObjectRoute> {
    if route.replicas.len() == 1 && delete_empty_route {
        return None;
    }

    let mut next = route.clone();
    next.version = next.version.next();

    if route.replicas.len() == 1 {
        next.replicas = Vec::new();
        return Some(next);
    }

    next.replicas.remove(replica_index);
    next.replicas.sort_by_key(|replica| replica.priority);
    for (priority, replica) in next.replicas.iter_mut().enumerate() {
        replica.priority = priority as u16;
    }
    Some(next)
}

impl StorageOwnerState {
    pub(in super::super) fn ensure_materialized_cold_backing_for_eviction(
        &self,
        route: &ObjectRoute,
    ) -> Result<Option<ObjectRoute>> {
        let cold_state = super::nof::route_backing_as_cold(route).map(|backing| backing.state);
        match cold_state {
            Some(mooncake_store_core::ColdBackingState::Materialized) => Ok(Some(route.clone())),
            Some(mooncake_store_core::ColdBackingState::PendingOffload) => {
                if !self.materialize_pending_offload_route_for_eviction(route)? {
                    warn!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        "eviction_inline_offload_failed: PendingOffload materialization failed, eviction skipped"
                    );
                    return Ok(None);
                }
                self.current_materialized_route(&route.key)
            }
            Some(mooncake_store_core::ColdBackingState::PendingDelete) => {
                self.sync_route(route);
                Ok(None)
            }
            None => {
                let Some(route) = self.publish_pending_cold_backing_for_eviction(route)? else {
                    warn!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        "eviction_publish_cold_backing_failed: cannot create PendingOffload, eviction skipped"
                    );
                    return Ok(None);
                };
                if !self.materialize_pending_offload_route_for_eviction(&route)? {
                    warn!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        "eviction_inline_offload_failed_after_publish: eviction skipped"
                    );
                    return Ok(None);
                }
                self.current_materialized_route(&route.key)
            }
        }
    }

    fn publish_pending_cold_backing_for_eviction(
        &self,
        route: &ObjectRoute,
    ) -> Result<Option<ObjectRoute>> {
        super::publish_pending_cold_backing_for_eviction(self, route)
    }

    fn materialize_pending_offload_route_for_eviction(&self, route: &ObjectRoute) -> Result<bool> {
        let tracker = OperationTracker::new("storage_owner_eviction_forced_offload");
        let result = (|| {
            let devices = match self.cold_tier_devices.devices.lock().snapshot() {
                Some(devices) => devices,
                None => refresh_cold_tier_device_cache(
                    self.metadata.as_ref(),
                    &self.cold_tier_devices.devices,
                    "cold_tier_device_snapshot_eviction_offload_prepare",
                )?,
            };
            let devices = devices
                .into_iter()
                .map(|device| (device.device_id.clone(), device))
                .collect::<BTreeMap<_, _>>();
            let Some(entry) = self.pending_offloads.claim(&route.key, route.version) else {
                return Ok(false);
            };
            let key = entry.key.clone();
            let prepare_result =
                self.prepare_pending_offload_entry(entry.clone(), Some(route.clone()), &devices);
            let materialization = match prepare_result {
                Ok(PendingOffloadPrepareOutcome::Ready(materialization)) => *materialization,
                Ok(PendingOffloadPrepareOutcome::Retry(entry)) => {
                    self.pending_offloads.retry(entry);
                    return Ok(false);
                }
                Ok(PendingOffloadPrepareOutcome::RetryAfter(entry, delay)) => {
                    self.pending_offloads.retry_after(entry, delay);
                    return Ok(false);
                }
                Ok(PendingOffloadPrepareOutcome::Refreshed(entry)) => {
                    self.pending_offloads.retry_after(entry, Duration::ZERO);
                    return Ok(false);
                }
                Ok(PendingOffloadPrepareOutcome::Skipped) => {
                    self.pending_offloads.complete(&key);
                    return Ok(false);
                }
                Err(error) => {
                    self.pending_offloads.retry(entry);
                    return Err(error);
                }
            };
            let mut first_error = None;
            let materialized = self.materialize_prepared_pending_offload_batch(
                std::slice::from_ref(&materialization),
                &mut first_error,
            )?;
            if materialized == 0 {
                if let Some(error) = first_error {
                    return Err(error);
                }
            }
            Ok(materialized > 0)
        })();
        tracker.finish(
            &result,
            u64::from(result.as_ref().copied().unwrap_or(false)),
        );
        result
    }
}
