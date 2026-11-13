use super::super::{
    refresh_cold_tier_device_cache, ClockEntryId, DebugEvictAllResult, ObjectRoute,
    OperationTracker, PendingOffloadPrepareOutcome, RestorePromotionQueue, Result,
    StorageOwnerState,
};
use super::{payload_checksum, pin_local_hot_replica_payload};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar as StdCondvar, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

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
        loop {
            if started.elapsed() >= DEBUG_EVICT_ALL_DEADLINE {
                result.timed_out = true;
                result.stopped_reason = Some("timeout".to_string());
                break;
            }
            match self.evict_one_blocking_with_outcome(None) {
                Ok(outcome) if outcome.made_progress => {
                    result.evicted = result.evicted.saturating_add(1);
                    if outcome.dropped_without_cold {
                        result.dropped_without_cold = result.dropped_without_cold.saturating_add(1);
                    }
                    error_retries = 0;
                    no_progress_retries = 0;
                }
                Ok(_) if self.evictable_hot_replica_count() == 0 => break,
                Ok(_) if no_progress_retries < MAX_NO_PROGRESS_RETRIES => {
                    no_progress_retries += 1;
                    restore_promotions.wait_for_worker_idle();
                    warn!(
                        runtime = %self.runtime,
                        evicted = result.evicted,
                        attempt = no_progress_retries,
                        max_attempts = MAX_NO_PROGRESS_RETRIES,
                        "debug evict_all retrying after bounded eviction made no progress"
                    );
                    std::thread::sleep(NO_PROGRESS_RETRY_DELAY);
                }
                Ok(_) => {
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
                    restore_promotions.wait_for_worker_idle();
                    std::thread::sleep(NO_PROGRESS_RETRY_DELAY);
                }
                Err(error) => {
                    result.last_error = Some(error.to_string());
                    result.stopped_reason = Some("eviction_error".to_string());
                    break;
                }
            }
        }
        result.remaining_hot_replicas = self.evictable_hot_replica_count();
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

    fn evictable_hot_replica_count(&self) -> usize {
        let pending_delete_ids = self
            .pending_deletes
            .lock()
            .entries
            .values()
            .map(|entry| ClockEntryId {
                route_key: entry.route_key.clone(),
                segment_name: entry.identity.segment_name.clone(),
                segment_offset: entry.identity.offset_bytes,
                allocation_id: Some(entry.identity.allocation_id),
            })
            .collect::<BTreeSet<_>>();
        self.hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&pending_delete_ids)
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
        if route.cold_backing.is_none()
            && !self.cold_tier_devices.nof_targets.is_empty()
            && !self.cold_tier_devices.nof_targets.is_managed()
        {
            return self.ensure_request_local_nof_copy(route);
        }
        let cold_state = route
            .cold_backing
            .as_ref()
            .map(|backing| backing.state)
            .or_else(|| route.nof_backing.as_ref().map(|backing| backing.state));
        match cold_state {
            Some(mooncake_store_core::ColdBackingState::Materialized) => Ok(Some(route.clone())),
            Some(mooncake_store_core::ColdBackingState::PendingOffload) => {
                if !self.materialize_pending_offload_route_for_eviction(route)? {
                    debug!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        "eviction_inline_offload_deferred: PendingOffload is busy or not ready, eviction skipped"
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
                    debug!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        "eviction_inline_offload_deferred_after_publish: eviction skipped"
                    );
                    return Ok(None);
                }
                self.current_materialized_route(&route.key)
            }
        }
    }

    fn ensure_request_local_nof_copy(&self, route: &ObjectRoute) -> Result<Option<ObjectRoute>> {
        let Some(replica) = route
            .replicas
            .iter()
            .filter(|replica| replica.owner == self.runtime)
            .min_by_key(|replica| replica.priority)
        else {
            return Ok(None);
        };
        let payload = pin_local_hot_replica_payload(&self.allocator, &self.state, replica)?;
        let checksum = replica
            .checksum
            .unwrap_or_else(|| payload_checksum(payload.as_slice()));
        let Some(target) =
            self.cold_tier_devices
                .nof_targets
                .pending_backing(route, replica.length, checksum)?
        else {
            return Ok(None);
        };
        self.cold_tier_devices
            .nof_targets
            .put_selected(&target, payload.as_slice())?;
        Ok(Some(route.clone()))
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
            let devices = if self.cold_tier_devices.has_any_local_backend() {
                let devices = match self.cold_tier_devices.devices.lock().snapshot() {
                    Some(devices) => devices,
                    None => refresh_cold_tier_device_cache(
                        self.metadata.as_ref(),
                        &self.cold_tier_devices.devices,
                        "cold_tier_device_snapshot_eviction_offload_prepare",
                    )?,
                };
                devices
                    .into_iter()
                    .map(|device| (device.device_id.clone(), device))
                    .collect::<BTreeMap<_, _>>()
            } else {
                BTreeMap::new()
            };
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
