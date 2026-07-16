use super::super::{
    refresh_cold_tier_device_cache, ObjectRoute, OperationTracker, PendingOffloadPrepareOutcome,
    RestorePromotionQueue, Result, RouteState, StorageOwnerState,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar as StdCondvar, Mutex as StdMutex};
use std::time::Duration;
use tracing::{info, warn};

impl StorageOwnerState {
    pub(in super::super) fn evict_all(
        &self,
        restore_promotions: &RestorePromotionQueue,
    ) -> Result<usize> {
        const MAX_ERROR_RETRIES: usize = 3;
        const MAX_NO_PROGRESS_RETRIES: usize = 100;
        const NO_PROGRESS_RETRY_DELAY: Duration = Duration::from_millis(10);

        let mut evicted = 0usize;
        let mut error_retries = 0usize;
        let mut no_progress_retries = 0usize;
        self.rebuild_clock()?;
        loop {
            match self.evict_one_blocking(None) {
                Ok(true) => {
                    evicted += 1;
                    error_retries = 0;
                    no_progress_retries = 0;
                }
                Ok(false) if !self.has_hot_replicas_for_debug_evict_all()? => break,
                Ok(false) if no_progress_retries < MAX_NO_PROGRESS_RETRIES => {
                    no_progress_retries += 1;
                    restore_promotions.wait_for_worker_idle();
                    warn!(
                        runtime = %self.runtime,
                        evicted,
                        attempt = no_progress_retries,
                        max_attempts = MAX_NO_PROGRESS_RETRIES,
                        "debug evict_all retrying after no-progress scan with hot replicas remaining"
                    );
                    std::thread::sleep(NO_PROGRESS_RETRY_DELAY);
                    self.rebuild_clock()?;
                }
                Ok(false) => {
                    let repaired = self.repair_stuck_pending_offloads_for_debug_evict_all()?;
                    if repaired > 0 {
                        warn!(
                            runtime = %self.runtime,
                            evicted,
                            repaired,
                            "debug evict_all repaired stuck pending offloads after no-progress scans"
                        );
                        no_progress_retries = 0;
                        self.rebuild_clock()?;
                        continue;
                    }
                    let remaining = self.hot_replica_count_for_debug_evict_all()?;
                    warn!(
                        runtime = %self.runtime,
                        evicted,
                        remaining,
                        max_attempts = MAX_NO_PROGRESS_RETRIES,
                        "debug evict_all stopped after best-effort no-progress scans"
                    );
                    break;
                }
                Err(error) if error_retries < MAX_ERROR_RETRIES => {
                    error_retries += 1;
                    warn!(
                        runtime = %self.runtime,
                        evicted,
                        attempt = error_retries,
                        max_attempts = MAX_ERROR_RETRIES,
                        error = %error,
                        "debug evict_all retrying after eviction error"
                    );
                    self.rebuild_clock()?;
                }
                Err(error) => {
                    let repaired = self.repair_stuck_pending_offloads_for_debug_evict_all()?;
                    if repaired > 0 {
                        warn!(
                            runtime = %self.runtime,
                            evicted,
                            repaired,
                            error = %error,
                            "debug evict_all repaired stuck pending offloads after eviction error"
                        );
                        error_retries = 0;
                        self.rebuild_clock()?;
                        continue;
                    }
                    let remaining = self.hot_replica_count_for_debug_evict_all()?;
                    warn!(
                        runtime = %self.runtime,
                        evicted,
                        remaining,
                        error = %error,
                        "debug evict_all stopped after best-effort eviction error handling"
                    );
                    break;
                }
            }
        }
        info!(
            runtime = %self.runtime,
            evicted,
            "debug evict_all completed"
        );
        Ok(evicted)
    }

    fn has_hot_replicas_for_debug_evict_all(&self) -> Result<bool> {
        Ok(self.hot_replica_count_for_debug_evict_all()? > 0)
    }

    fn hot_replica_count_for_debug_evict_all(&self) -> Result<usize> {
        Ok(self.collect_routes_by_replica_owner(&self.runtime)?.len())
    }

    fn repair_stuck_pending_offloads_for_debug_evict_all(&self) -> Result<usize> {
        let routes = self.collect_routes_by_replica_owner(&self.runtime)?;
        let mut repaired = 0usize;
        let offload_worker_idle = !self.pending_offloads.is_materializing();
        for route in routes {
            if route.state != RouteState::Active {
                self.sync_route(&route);
                continue;
            }
            if !route
                .replicas
                .iter()
                .any(|replica| replica.owner == self.runtime)
            {
                self.sync_route(&route);
                continue;
            }
            let Some(cold_backing) = route.cold_backing.clone() else {
                continue;
            };
            if cold_backing.owner != self.runtime
                || cold_backing.state != mooncake_store_core::ColdBackingState::PendingOffload
            {
                continue;
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
                continue;
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
                continue;
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
        }
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

pub(in super::super) fn should_delete_route_after_last_replica_eviction(
    route: &ObjectRoute,
) -> bool {
    !route
        .cold_backing
        .as_ref()
        .is_some_and(|backing| backing.state == mooncake_store_core::ColdBackingState::Materialized)
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
        let cold_state = route.cold_backing.as_ref().map(|backing| backing.state);
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
