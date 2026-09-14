use super::super::{
    cold_tier_device_id, current_time_ms, now_ms, registry, ClockEntryId, ColdTierCleanupResult,
    ColdTierCompactionResult, ColdTierDeviceState, ColdTierDeviceUpdate, ColdTierMaintenanceStats,
    ObjectRoute, OperationTracker, PendingDeleteGcResult, PendingOffloadEntry,
    PendingOffloadMaterialization, PendingOffloadPrepareOutcome, Result, RouteState,
    StorageOwnerState, StoreError, DEFAULT_COLD_TIER_CLEANUP_DEVICE_BATCH,
    DEFAULT_COLD_TIER_CLEANUP_VICTIM_BATCH, DEFAULT_OFFLOAD_MATERIALIZE_BATCH,
    DEFAULT_PENDING_DELETE_GC_BATCH,
};
use super::{
    backend_remove_cold_payload, backend_remove_pending_source, persistent_backing_contains_target,
};
use mooncake_store_core::{ClientRuntimeId, ColdTierDeviceRecord};
use std::collections::BTreeMap;
use std::time::Instant;
use tracing::info;

impl StorageOwnerState {
    pub(crate) fn accept_nof_owner_snapshot(
        &self,
        target_id: String,
        from: ClientRuntimeId,
        to: ClientRuntimeId,
        routes: Vec<ObjectRoute>,
    ) -> Result<usize> {
        self.cold_tier_devices
            .accept_nof_owner_snapshot(target_id, from, to, routes)
    }

    pub(in super::super) fn enqueue_pending_offload(&self, route: &ObjectRoute) {
        crate::client::cold_tier::enqueue_pending_offload(self, route)
    }

    pub(in super::super) fn garbage_collect_pending_delete_backings(&self) -> Result<usize> {
        Ok(self
            .garbage_collect_pending_delete_backings_bounded(DEFAULT_PENDING_DELETE_GC_BATCH)?
            .cas_removed_routes)
    }

    pub(crate) fn garbage_collect_pending_delete_backings_for_device_bounded(
        &self,
        device_id: &str,
        max_backings: usize,
    ) -> Result<usize> {
        self.ensure_cold_tier_device_loaded(device_id)?;
        Ok(self
            .garbage_collect_pending_delete_backings_bounded_for_device(
                Some(device_id),
                max_backings,
            )?
            .cas_removed_routes)
    }

    fn garbage_collect_pending_delete_backings_bounded(
        &self,
        max_backings: usize,
    ) -> Result<PendingDeleteGcResult> {
        self.garbage_collect_pending_delete_backings_bounded_for_device(None, max_backings)
    }

    pub(crate) fn maintain_cold_tier_backends(&self) -> ColdTierMaintenanceStats {
        let mut stats = ColdTierMaintenanceStats::default();
        let Some(devices) = self.cold_tier_devices.devices.lock().snapshot() else {
            return stats;
        };
        for device in devices {
            let cold_backing = mooncake_store_core::ColdBackingRoute {
                owner: self.runtime.clone(),
                cold_tier_id: device.device_id.clone(),
                object_locator: String::new(),
                length: 0,
                checksum: None,
                state: mooncake_store_core::ColdBackingState::Materialized,
                replicas: vec![],
            };
            let Ok(backend) = self.cold_tier_devices.backend_for(&cold_backing) else {
                continue;
            };
            if let Ok(device_stats) = backend.maintenance_stats() {
                stats.devices.insert(device.device_id, device_stats);
            }
        }
        stats
    }

    pub(in super::super) fn garbage_collect_pending_delete_backings_bounded_for_device(
        &self,
        device_id: Option<&str>,
        max_backings: usize,
    ) -> Result<PendingDeleteGcResult> {
        let tracker = OperationTracker::new("storage_owner_pending_delete_gc");
        let result = (|| {
            let limit = max_backings.max(1);
            let routes = self.metadata.as_ref().list_object_routes_by_cold_backing(
                &mooncake_store_core::ColdBackingRouteFilter {
                    device_id: device_id.map(str::to_string),
                    state: Some(mooncake_store_core::ColdBackingState::PendingDelete),
                    limit: Some(limit),
                    ..mooncake_store_core::ColdBackingRouteFilter::default()
                },
            )?;
            let mut result = PendingDeleteGcResult::default();
            for route in routes {
                if result.scanned_routes >= limit {
                    break;
                }
                result.scanned_routes = result.scanned_routes.saturating_add(1);
                let Some(cold_backing) = route.cold_backing.as_ref() else {
                    continue;
                };
                if cold_backing.state != mooncake_store_core::ColdBackingState::PendingDelete {
                    continue;
                }

                let backend = match self.cold_tier_devices.backend_for(cold_backing) {
                    Ok(backend) => backend,
                    Err(error) => {
                        result.backend_errors = result.backend_errors.saturating_add(1);
                        self.record_cold_tier_cleanup_error(
                            cold_tier_device_id(cold_backing),
                            &error,
                        );
                        continue;
                    }
                };
                let removed_cold_payload =
                    match backend_remove_cold_payload(backend.as_ref(), cold_backing) {
                        Ok(removed) => removed,
                        Err(error) => {
                            result.backend_errors = result.backend_errors.saturating_add(1);
                            self.record_cold_tier_cleanup_error(
                                cold_tier_device_id(cold_backing),
                                &error,
                            );
                            continue;
                        }
                    };
                let removed_pending_source =
                    match backend_remove_pending_source(backend.as_ref(), cold_backing) {
                        Ok(removed) => removed,
                        Err(error) => {
                            result.backend_errors = result.backend_errors.saturating_add(1);
                            self.record_cold_tier_cleanup_error(
                                cold_tier_device_id(cold_backing),
                                &error,
                            );
                            false
                        }
                    };

                // Remove replicas (best-effort).
                for replica in &cold_backing.replicas {
                    let replica_backing = mooncake_store_core::ColdBackingRoute {
                        owner: replica.owner.clone(),
                        cold_tier_id: replica.cold_tier_id.clone(),
                        object_locator: replica.object_locator.clone(),
                        ..cold_backing.clone()
                    };
                    if let Ok(replica_backend) =
                        self.cold_tier_devices.backend_for(&replica_backing)
                    {
                        let _ =
                            backend_remove_cold_payload(replica_backend.as_ref(), &replica_backing);
                        let _ = backend_remove_pending_source(
                            replica_backend.as_ref(),
                            &replica_backing,
                        );
                    }
                }

                let next = if route.replicas.is_empty() {
                    None
                } else {
                    let mut next = route.clone();
                    next.version = next.version.next();
                    next.cold_backing = None;
                    Some(next)
                };
                let cas = self.route_ops.compare_and_swap_route(
                    &route.key,
                    Some(route.version),
                    next.as_ref(),
                )?;
                if cas.applied {
                    info!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        cold_tier_id = %cold_tier_device_id(cold_backing),
                        object_locator = %cold_backing.object_locator,
                        length = cold_backing.length,
                        removed_cold_payload,
                        removed_pending_source,
                        route_deleted = next.is_none(),
                        "cold_tier_gc_backing_removed"
                    );
                    result.cas_removed_routes = result.cas_removed_routes.saturating_add(1);
                    if removed_cold_payload {
                        result.backend_deleted = result.backend_deleted.saturating_add(1);
                    } else {
                        result.backend_missing = result.backend_missing.saturating_add(1);
                    }
                    if removed_pending_source {
                        result.pending_source_deleted =
                            result.pending_source_deleted.saturating_add(1);
                    }
                    self.apply_cold_tier_usage_delta(
                        cold_tier_device_id(cold_backing),
                        -(cold_backing.length as i64),
                        0,
                    )?;
                    result.used_bytes_released = result
                        .used_bytes_released
                        .saturating_add(cold_backing.length);
                    if let Some(next) = next.as_ref() {
                        self.sync_route(next);
                    } else {
                        self.hot_replicas.remove_key(&route.key);
                    }
                } else if let Some(current) = cas.current.as_ref() {
                    result.cas_conflicts = result.cas_conflicts.saturating_add(1);
                    self.sync_route(current);
                } else {
                    result.cas_conflicts = result.cas_conflicts.saturating_add(1);
                    self.hot_replicas.remove_key(&route.key);
                }
            }
            Ok(result)
        })();
        tracker.finish(
            &result,
            result
                .as_ref()
                .map(|result| result.cas_removed_routes as u64)
                .unwrap_or_default(),
        );
        result
    }

    pub(in super::super) fn free_cold_tier_until_low_watermark(&self) -> Result<usize> {
        Ok(self
            .free_cold_tier_until_low_watermark_bounded(
                DEFAULT_COLD_TIER_CLEANUP_DEVICE_BATCH,
                DEFAULT_COLD_TIER_CLEANUP_VICTIM_BATCH,
            )?
            .freed_backings)
    }

    pub(crate) fn compact_cold_tier_backends(&self) -> ColdTierCompactionResult {
        let maintenance = self.maintain_cold_tier_backends();
        let mut result = ColdTierCompactionResult {
            scanned_devices: maintenance.devices.len(),
            ..ColdTierCompactionResult::default()
        };
        for (device_id, stats) in maintenance.devices {
            if stats.extent_dead_bytes == 0 {
                continue;
            }
            let Some(_guard) = self.cold_tier_cleanup.try_start_device(&device_id) else {
                result.skipped_devices = result.skipped_devices.saturating_add(1);
                continue;
            };
            result.candidate_devices = result.candidate_devices.saturating_add(1);
            result.backend_dead_bytes = result
                .backend_dead_bytes
                .saturating_add(stats.extent_dead_bytes);
            let cold_backing = mooncake_store_core::ColdBackingRoute {
                owner: self.runtime.clone(),
                cold_tier_id: device_id.clone(),
                object_locator: String::new(),
                length: 0,
                checksum: None,
                state: mooncake_store_core::ColdBackingState::Materialized,
                replicas: vec![],
            };
            if let Ok(backend) = self.cold_tier_devices.backend_for(&cold_backing) {
                if let Ok(compaction) = backend.compact(stats.extent_dead_bytes) {
                    if compaction.supported {
                        result.compacted_candidate_bytes = result
                            .compacted_candidate_bytes
                            .saturating_add(compaction.candidate_bytes);
                        result.reclaimed_bytes = result
                            .reclaimed_bytes
                            .saturating_add(compaction.reclaimed_bytes);
                    } else {
                        result.unsupported_devices = result.unsupported_devices.saturating_add(1);
                    }
                }
            }
        }
        result
    }

    pub(in super::super) fn free_cold_tier_until_low_watermark_bounded(
        &self,
        max_devices: usize,
        max_victims_per_device: usize,
    ) -> Result<ColdTierCleanupResult> {
        let low_bytes = self.cold_tier_devices.low_bytes();
        let restored_before = self.restore_full_cold_tier_devices_below_low_watermark(low_bytes)?;
        let mut nof_downline =
            self.downline_unhealthy_managed_nof_targets(max_devices, max_victims_per_device)?;
        let Some(high_bytes) = self.cold_tier_devices.high_bytes() else {
            nof_downline.restored_devices = nof_downline
                .restored_devices
                .saturating_add(restored_before);
            nof_downline.reached_low_watermark = true;
            return Ok(nof_downline);
        };
        let low_bytes = low_bytes.unwrap_or(high_bytes);
        let nof_watermark = self.free_managed_nof_targets_until_low_watermark(
            high_bytes,
            low_bytes,
            max_devices,
            max_victims_per_device,
        )?;
        Self::merge_cleanup_result(&mut nof_downline, nof_watermark);
        let nof_result = nof_downline;
        let devices = self
            .metadata
            .as_ref()
            .list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())?;
        let mut devices = devices
            .into_iter()
            .filter(|device| device.stable_id == self.runtime.stable_id.0)
            .filter(|device| device.used_bytes.saturating_add(device.reserved_bytes) >= high_bytes)
            .collect::<Vec<_>>();
        devices.sort_by_key(|device| {
            std::cmp::Reverse(device.used_bytes.saturating_add(device.reserved_bytes))
        });
        let mut result = ColdTierCleanupResult {
            scanned_devices: devices.len(),
            restored_devices: restored_before,
            reached_low_watermark: devices.is_empty(),
            ..ColdTierCleanupResult::default()
        };
        Self::merge_cleanup_result(&mut result, nof_result);
        for device in devices.into_iter().take(max_devices.max(1)) {
            if device.used_bytes.saturating_add(device.reserved_bytes) <= low_bytes {
                result.reached_low_watermark = true;
                continue;
            }
            let Some(_guard) = self.cold_tier_cleanup.try_start_device(&device.device_id) else {
                result.skipped_backings = result.skipped_backings.saturating_add(1);
                continue;
            };
            let device_result = self.free_cold_tier_device_until_low_watermark(
                &device.device_id,
                low_bytes,
                max_victims_per_device,
            )?;
            Self::merge_cleanup_result(&mut result, device_result);
        }
        result.restored_devices = result.restored_devices.saturating_add(
            self.restore_full_cold_tier_devices_below_low_watermark(Some(low_bytes))?,
        );
        Ok(result)
    }

    fn merge_cleanup_result(into: &mut ColdTierCleanupResult, add: ColdTierCleanupResult) {
        into.scanned_devices = into.scanned_devices.saturating_add(add.scanned_devices);
        into.attempted_victims = into.attempted_victims.saturating_add(add.attempted_victims);
        into.freed_backings = into.freed_backings.saturating_add(add.freed_backings);
        into.skipped_backings = into.skipped_backings.saturating_add(add.skipped_backings);
        into.restored_devices = into.restored_devices.saturating_add(add.restored_devices);
        into.reached_low_watermark |= add.reached_low_watermark;
    }

    fn free_managed_nof_targets_until_low_watermark(
        &self,
        high_bytes: u64,
        low_bytes: u64,
        max_targets: usize,
        max_victims_per_target: usize,
    ) -> Result<ColdTierCleanupResult> {
        let mut targets = self
            .cold_tier_devices
            .nof_targets
            .locally_owned_managed_target_ids()
            .into_iter()
            .filter_map(|target_id| match self.managed_nof_used_bytes(&target_id) {
                Ok(Some(used_bytes)) if used_bytes >= high_bytes => {
                    Some(Ok((target_id, used_bytes)))
                }
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
        targets.sort_by_key(|(_, used_bytes)| std::cmp::Reverse(*used_bytes));
        let mut result = ColdTierCleanupResult {
            scanned_devices: targets.len(),
            reached_low_watermark: targets.is_empty(),
            ..ColdTierCleanupResult::default()
        };
        for (target_id, _) in targets.into_iter().take(max_targets.max(1)) {
            let Some(_guard) = self.cold_tier_cleanup.try_start_device(&target_id) else {
                result.skipped_backings = result.skipped_backings.saturating_add(1);
                continue;
            };
            let target_result = self.free_managed_nof_target_until_low_watermark(
                &target_id,
                low_bytes,
                max_victims_per_target,
            )?;
            Self::merge_cleanup_result(&mut result, target_result);
        }
        Ok(result)
    }

    fn free_managed_nof_target_until_low_watermark(
        &self,
        target_id: &str,
        low_bytes: u64,
        max_victims: usize,
    ) -> Result<ColdTierCleanupResult> {
        let mut result = ColdTierCleanupResult::default();
        for _ in 0..max_victims.max(1) {
            let Some(used_bytes) = self.managed_nof_used_bytes(target_id)? else {
                result.reached_low_watermark = true;
                break;
            };
            if used_bytes <= low_bytes {
                result.reached_low_watermark = true;
                break;
            }
            result.attempted_victims = result.attempted_victims.saturating_add(1);
            if self.free_one_managed_nof_backing_lru(target_id)? {
                result.freed_backings = result.freed_backings.saturating_add(1);
            } else {
                result.skipped_backings = result.skipped_backings.saturating_add(1);
                break;
            }
        }
        Ok(result)
    }

    fn managed_nof_used_bytes(&self, target_id: &str) -> Result<Option<u64>> {
        let Some(health) = self
            .cold_tier_devices
            .nof_targets
            .managed_target_health(target_id)?
        else {
            return Ok(None);
        };
        Ok(match (health.capacity_bytes, health.available_bytes) {
            (Some(capacity), Some(available)) => Some(capacity.saturating_sub(available)),
            _ => None,
        })
    }

    pub(crate) fn manual_free_cold_tier_device_bounded(
        &self,
        device_id: &str,
        max_victims: usize,
    ) -> Result<ColdTierCleanupResult> {
        let Some(device) = self.ensure_cold_tier_device_loaded(device_id)? else {
            return Ok(ColdTierCleanupResult {
                reached_low_watermark: true,
                ..ColdTierCleanupResult::default()
            });
        };
        let low_bytes = self.cold_tier_devices.low_bytes().unwrap_or(0);
        let Some(_guard) = self.cold_tier_cleanup.try_start_device(device_id) else {
            return Ok(ColdTierCleanupResult {
                scanned_devices: 1,
                skipped_backings: 1,
                reached_low_watermark: device.used_bytes.saturating_add(device.reserved_bytes)
                    <= low_bytes,
                ..ColdTierCleanupResult::default()
            });
        };
        let mut result =
            self.free_cold_tier_device_until_low_watermark(device_id, low_bytes, max_victims)?;
        result.scanned_devices = 1;
        Ok(result)
    }

    fn free_cold_tier_device_until_low_watermark(
        &self,
        device_id: &str,
        low_bytes: u64,
        max_victims: usize,
    ) -> Result<ColdTierCleanupResult> {
        let mut result = ColdTierCleanupResult::default();
        for _ in 0..max_victims.max(1) {
            let Some(device) = self.metadata.as_ref().get_cold_tier_device(device_id)? else {
                result.reached_low_watermark = true;
                break;
            };
            if device.used_bytes.saturating_add(device.reserved_bytes) <= low_bytes {
                result.reached_low_watermark = true;
                break;
            }
            result.attempted_victims = result.attempted_victims.saturating_add(1);
            if self.free_one_cold_tier_backing_lru(device_id)? {
                result.freed_backings = result.freed_backings.saturating_add(1);
            } else {
                result.skipped_backings = result.skipped_backings.saturating_add(1);
                break;
            }
        }
        Ok(result)
    }

    /// Inline GC triggered by offload device selection failure.
    ///
    /// When offload cannot find a device with enough capacity for `needed_length`
    /// bytes, this function tries to free cold backings on local devices to make
    /// room.  It marks Materialized cold backings as PendingDelete and immediately
    /// garbage-collects them (deleting disk data and updating `used_bytes`).
    ///
    /// Uses the same per-device `try_start_device` guard as background GC,
    /// ensuring at most one GC operation per device at any time.  If the
    /// background GC already holds the guard for a device, that device is
    /// skipped (and vice versa).
    ///
    /// Returns the total bytes freed across all devices.
    pub(in super::super) fn try_inline_gc_for_offload(&self, needed_length: u64) -> Result<u64> {
        const MAX_INLINE_GC_VICTIMS: usize = 8;

        let snapshot = match self.cold_tier_devices.devices.lock().snapshot() {
            Some(s) => s,
            None => return Ok(0),
        };

        let mut total_freed = 0u64;
        for device in &snapshot {
            // Only GC on devices owned by this runtime.
            if device.stable_id != self.runtime.stable_id.0
                || device.epoch != Some(self.runtime.epoch.0)
            {
                continue;
            }
            // GC both Healthy (capacity tight) and Full (need to reclaim) devices.
            if !matches!(
                device.state,
                ColdTierDeviceState::Healthy | ColdTierDeviceState::Full
            ) {
                continue;
            }
            let Some(capacity) = device.capacity_bytes else {
                continue;
            };
            let projected = device
                .used_bytes
                .saturating_add(device.reserved_bytes)
                .saturating_add(needed_length)
                .saturating_add(self.cold_tier_devices.watermarks.reserve_bytes);
            if projected <= capacity {
                continue; // Already has room — no GC needed.
            }

            // Per-device mutual exclusion with background GC.
            let Some(_guard) = self.cold_tier_cleanup.try_start_device(&device.device_id) else {
                continue; // Another GC in progress on this device — skip.
            };

            // Step 1: Mark Materialized cold backings as PendingDelete.
            let mut marked = 0usize;
            for _ in 0..MAX_INLINE_GC_VICTIMS {
                if self.free_one_cold_tier_backing_lru(&device.device_id)? {
                    marked += 1;
                } else {
                    break;
                }
            }

            // Step 2: Garbage-collect PendingDelete backings to free disk space.
            if marked > 0 {
                let gc = self.garbage_collect_pending_delete_backings_bounded_for_device(
                    Some(&device.device_id),
                    marked,
                )?;
                total_freed = total_freed.saturating_add(gc.used_bytes_released);
                info!(
                    runtime = %self.runtime,
                    device_id = %device.device_id,
                    marked,
                    freed_bytes = gc.used_bytes_released,
                    needed_length,
                    "inline_gc_for_offload"
                );
            }
        }

        Ok(total_freed)
    }

    pub(in super::super) fn reclaim_cold_backing_for_route_delete(
        &self,
        route_key: &mooncake_store_core::ObjectKey,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<crate::control_plane::ColdReclaimResult> {
        let expected_owner = self
            .cold_tier_devices
            .current_target_owner(cold_tier_device_id(cold_backing), Some(&cold_backing.owner))?;
        if expected_owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "cold backing {} belongs to runtime {}, not {}",
                cold_tier_device_id(cold_backing),
                expected_owner,
                self.runtime
            )));
        }
        if self
            .ensure_cold_tier_device_loaded(cold_tier_device_id(cold_backing))?
            .is_none()
        {
            return Err(StoreError::NotFound(format!(
                "cold tier device {} not found",
                cold_tier_device_id(cold_backing)
            )));
        }
        if self.persistent_backing_still_referenced_by_active_route(route_key, cold_backing)? {
            return Ok(crate::control_plane::ColdReclaimResult {
                skipped_still_referenced: true,
                ..crate::control_plane::ColdReclaimResult::default()
            });
        }
        let backend = self.cold_tier_devices.backend_for(cold_backing)?;
        let removed_cold_payload = backend_remove_cold_payload(backend.as_ref(), cold_backing)?;
        let removed_pending_source =
            match backend_remove_pending_source(backend.as_ref(), cold_backing) {
                Ok(removed) => removed,
                Err(error) => {
                    self.record_cold_tier_cleanup_error(cold_tier_device_id(cold_backing), &error);
                    tracing::warn!(
                        error = %error,
                        cold_tier_id = %cold_tier_device_id(cold_backing),
                        object_locator = %cold_backing.object_locator,
                        "failed to remove pending source during owner reclaim"
                    );
                    false
                }
            };
        if removed_cold_payload
            && cold_backing.state == mooncake_store_core::ColdBackingState::Materialized
        {
            self.apply_cold_tier_usage_delta(
                cold_tier_device_id(cold_backing),
                -(cold_backing.length as i64),
                0,
            )?;
        }
        info!(
            runtime = %self.runtime,
            cold_tier_id = %cold_tier_device_id(cold_backing),
            object_locator = %cold_backing.object_locator,
            length = cold_backing.length,
            removed_cold_payload,
            removed_pending_source,
            "cold_reclaim_payload_deleted_by_owner"
        );
        Ok(crate::control_plane::ColdReclaimResult {
            removed_cold_payload,
            removed_pending_source,
            skipped_still_referenced: false,
        })
    }

    fn persistent_backing_still_referenced_by_active_route(
        &self,
        route_key: &mooncake_store_core::ObjectKey,
        backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<bool> {
        let Some(route) = self.metadata.get_object_route(route_key)? else {
            return Ok(false);
        };
        Ok(route.state == RouteState::Active
            && route
                .cold_backing
                .as_ref()
                .is_some_and(|current| persistent_backing_contains_target(current, backing)))
    }

    pub(crate) fn probe_cold_tier_device(&self, device_id: &str) -> Result<ColdTierDeviceRecord> {
        let Some(device) = self.ensure_cold_tier_device_loaded(device_id)? else {
            return Err(StoreError::NotFound(format!(
                "cold tier device {device_id} not found"
            )));
        };
        let cold_backing = mooncake_store_core::ColdBackingRoute {
            owner: self.runtime.clone(),
            cold_tier_id: device.device_id.clone(),
            object_locator: String::new(),
            length: 0,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: vec![],
        };
        let mut update = ColdTierDeviceUpdate::new(current_time_ms());
        update.expected_updated_at_ms = Some(device.updated_at_ms);
        match self
            .cold_tier_devices
            .backend_for(&cold_backing)
            .and_then(|backend| backend.health())
        {
            Ok(health) => {
                // Prefer the user-configured capacity (from --cold-tier-capacity-bytes / bootstrap)
                // over the filesystem-reported capacity.  Only fall back to the disk probe
                // when no explicit capacity was ever set.
                update.capacity_bytes = Some(device.capacity_bytes.or(health.capacity_bytes));
                if device.state == ColdTierDeviceState::Failed {
                    update.state = Some(ColdTierDeviceState::Healthy);
                }
                update.last_error = Some(None);
            }
            Err(error) => {
                update.state = Some(ColdTierDeviceState::Failed);
                update.failure_count = Some(device.failure_count.saturating_add(1));
                update.last_error = Some(Some(error.to_string()));
            }
        }
        let updated = self
            .metadata
            .as_ref()
            .update_cold_tier_device(device_id, update)?;
        self.cold_tier_devices.upsert(updated.clone());
        Ok(updated)
    }

    fn ensure_cold_tier_device_loaded(
        &self,
        device_id: &str,
    ) -> Result<Option<mooncake_store_core::ColdTierDeviceRecord>> {
        let device = self.metadata.as_ref().get_cold_tier_device(device_id)?;
        if let Some(device) = device.as_ref() {
            self.ensure_cold_tier_device_owned_by_runtime(device)?;
            self.cold_tier_devices.upsert(device.clone());
        }
        Ok(device)
    }

    fn ensure_cold_tier_device_owned_by_runtime(
        &self,
        device: &mooncake_store_core::ColdTierDeviceRecord,
    ) -> Result<()> {
        if device.stable_id != self.runtime.stable_id.0 {
            return Err(StoreError::InvalidState(format!(
                "cold tier device {} belongs to runtime {}, not {}",
                device.device_id, device.stable_id, self.runtime.stable_id.0
            )));
        }
        if let Some(epoch) = device.epoch {
            if epoch != self.runtime.epoch.0 {
                return Err(StoreError::StaleEpoch(format!(
                    "cold tier device {} belongs to epoch {}, not {}",
                    device.device_id, epoch, self.runtime.epoch.0
                )));
            }
        }
        Ok(())
    }

    fn restore_full_cold_tier_devices_below_low_watermark(
        &self,
        low_bytes: Option<u64>,
    ) -> Result<usize> {
        let Some(low_bytes) = low_bytes else {
            return Ok(0);
        };
        let devices = self
            .metadata
            .as_ref()
            .list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())?;
        let mut restored = 0usize;
        for device in devices
            .into_iter()
            .filter(|device| device.stable_id == self.runtime.stable_id.0)
            .filter(|device| device.state == mooncake_store_core::ColdTierDeviceState::Full)
            .filter(|device| device.used_bytes.saturating_add(device.reserved_bytes) <= low_bytes)
        {
            let mut update = mooncake_store_core::ColdTierDeviceUpdate::new(current_time_ms());
            update.expected_updated_at_ms = Some(device.updated_at_ms);
            update.state = Some(mooncake_store_core::ColdTierDeviceState::Healthy);
            update.last_error = Some(None);
            let updated = self
                .metadata
                .as_ref()
                .update_cold_tier_device(&device.device_id, update)?;
            self.cold_tier_devices.upsert(updated);
            restored = restored.saturating_add(1);
        }
        Ok(restored)
    }

    fn free_one_cold_tier_backing_lru(&self, device_id: &str) -> Result<bool> {
        for rebuild in 0..=1usize {
            let budget = self.hot_replicas.eviction_budget().max(1);
            for _ in 0..budget {
                let victim = self.hot_replicas.pick_victim(
                    None,
                    self.offload_priority.eviction_policy,
                    self.offload_priority.eviction_scan_limit,
                );
                let Some(victim) = victim else {
                    break;
                };
                if self.free_cold_tier_candidate(&victim, device_id)? {
                    return Ok(true);
                }
            }
            if rebuild == 0 {
                self.rebuild_clock()?;
            }
        }
        Ok(false)
    }

    fn free_cold_tier_candidate(&self, victim: &ClockEntryId, device_id: &str) -> Result<bool> {
        let Some(route) = self.route_ops.load_route(&victim.route_key)? else {
            self.hot_replicas.remove_id(victim);
            return Ok(false);
        };
        if route.state != RouteState::Active {
            self.hot_replicas.remove_id(victim);
            return Ok(false);
        }
        let Some(cold_backing) = route.cold_backing.as_ref() else {
            self.sync_route(&route);
            return Ok(false);
        };
        if cold_backing.cold_tier_id != device_id
            || cold_backing.state != mooncake_store_core::ColdBackingState::Materialized
            || route.replicas.is_empty()
        {
            self.sync_route(&route);
            return Ok(false);
        }
        let mut next = route.clone();
        next.version = next.version.next();
        if let Some(next_backing) = next.cold_backing.as_mut() {
            next_backing.state = mooncake_store_core::ColdBackingState::PendingDelete;
        }
        let cas =
            self.route_ops
                .compare_and_swap_route(&route.key, Some(route.version), Some(&next))?;
        if cas.applied {
            info!(
                runtime = %self.runtime,
                key = %route.key.0,
                device_id,
                cold_backing_length = cold_backing.length,
                "cold_tier_backing_marked_pending_delete"
            );
            self.sync_route(&next);
            return Ok(true);
        }
        match cas.current.as_ref() {
            Some(current) => self.sync_route(current),
            None => self.hot_replicas.remove_id(victim),
        }
        Ok(false)
    }

    fn free_one_managed_nof_backing_lru(&self, target_id: &str) -> Result<bool> {
        for rebuild in 0..=1usize {
            let budget = self.hot_replicas.eviction_budget().max(1);
            for _ in 0..budget {
                let victim = self.hot_replicas.pick_victim(
                    None,
                    self.offload_priority.eviction_policy,
                    self.offload_priority.eviction_scan_limit,
                );
                let Some(victim) = victim else {
                    break;
                };
                if self.free_managed_nof_candidate(&victim, target_id)? {
                    return Ok(true);
                }
            }
            if rebuild == 0 {
                self.rebuild_clock()?;
            }
        }
        Ok(false)
    }

    fn free_managed_nof_candidate(&self, victim: &ClockEntryId, target_id: &str) -> Result<bool> {
        let Some(route) = self.route_ops.load_route(&victim.route_key)? else {
            self.hot_replicas.remove_id(victim);
            return Ok(false);
        };
        if route.state != RouteState::Active {
            self.hot_replicas.remove_id(victim);
            return Ok(false);
        }
        let Some(backing) = route.nof_backing.as_ref() else {
            self.sync_route(&route);
            return Ok(false);
        };
        if backing.state != mooncake_store_core::ColdBackingState::Materialized
            || !managed_nof_backing_contains_target(backing, target_id)
            || !managed_nof_target_removal_is_safe(&route, backing)
        {
            self.sync_route(&route);
            return Ok(false);
        }
        match self
            .cold_tier_devices
            .nof_targets
            .release_managed_target(target_id, route.clone())
        {
            Ok(next) => {
                info!(
                    runtime = %self.runtime,
                    key = %route.key.0,
                    target_id,
                    nof_backing_length = backing.length,
                    "managed_nof_backing_released_for_watermark"
                );
                self.sync_route(&next);
                Ok(true)
            }
            Err(StoreError::Conflict(_)) => {
                if let Some(current) = self.route_ops.load_route(&route.key)? {
                    self.sync_route(&current);
                } else {
                    self.hot_replicas.remove_key(&route.key);
                }
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    fn downline_unhealthy_managed_nof_targets(
        &self,
        max_targets: usize,
        max_routes_per_target: usize,
    ) -> Result<ColdTierCleanupResult> {
        let targets = self
            .cold_tier_devices
            .nof_targets
            .managed_downline_ready_target_ids();
        let mut result = ColdTierCleanupResult {
            scanned_devices: targets.len(),
            reached_low_watermark: targets.is_empty(),
            ..ColdTierCleanupResult::default()
        };
        for target_id in targets.into_iter().take(max_targets.max(1)) {
            let Some(_guard) = self.cold_tier_cleanup.try_start_device(&target_id) else {
                result.skipped_backings = result.skipped_backings.saturating_add(1);
                continue;
            };
            let target_result =
                self.downline_managed_nof_target(&target_id, max_routes_per_target)?;
            Self::merge_cleanup_result(&mut result, target_result);
        }
        Ok(result)
    }

    fn downline_managed_nof_target(
        &self,
        target_id: &str,
        max_routes: usize,
    ) -> Result<ColdTierCleanupResult> {
        let routes = self.metadata.as_ref().list_object_routes_by_nof_backing(
            &mooncake_store_core::NofBackingRouteFilter {
                target_id: Some(target_id.to_string()),
                state: Some(mooncake_store_core::ColdBackingState::Materialized),
                limit: Some(max_routes.max(1)),
            },
        )?;
        let mut result = ColdTierCleanupResult::default();
        for route in routes {
            result.attempted_victims = result.attempted_victims.saturating_add(1);
            let Some(backing) = route.nof_backing.as_ref() else {
                continue;
            };
            if !managed_nof_target_removal_is_safe(&route, backing) {
                result.skipped_backings = result.skipped_backings.saturating_add(1);
                self.sync_route(&route);
                continue;
            }
            match self
                .cold_tier_devices
                .nof_targets
                .forget_managed_target_route(target_id, route.clone())
            {
                Ok(next) => {
                    result.freed_backings = result.freed_backings.saturating_add(1);
                    self.sync_route(&next);
                    info!(
                        runtime = %self.runtime,
                        key = %route.key.0,
                        target_id,
                        "managed_nof_target_downlined"
                    );
                }
                Err(StoreError::Conflict(_)) => {
                    result.skipped_backings = result.skipped_backings.saturating_add(1);
                    if let Some(current) = self.route_ops.load_route(&route.key)? {
                        self.sync_route(&current);
                    } else {
                        self.hot_replicas.remove_key(&route.key);
                    }
                }
                Err(error) => {
                    result.skipped_backings = result.skipped_backings.saturating_add(1);
                    tracing::warn!(
                        target_id,
                        key = %route.key.0,
                        error = %error,
                        "managed NoF target downline failed"
                    );
                }
            }
        }
        Ok(result)
    }

    pub(in super::super) fn materialize_pending_offloads(&self) -> Result<usize> {
        self.materialize_pending_offloads_bounded(DEFAULT_OFFLOAD_MATERIALIZE_BATCH)
    }

    pub(in super::super) fn materialize_pending_offloads_foreground_kick(&self) -> Result<usize> {
        self.materialize_pending_offloads_bounded(
            self.cold_tier_devices.foreground_offload_kick_batch(),
        )
    }

    pub(in super::super) fn record_cold_tier_observability_snapshots(&self) {
        let device_snapshot = self.cold_tier_devices.observability_snapshot();
        registry::record_cold_tier_device_metrics(registry::ColdTierDeviceMetrics {
            runtime: &self.runtime.stable_id.0,
            total_devices: device_snapshot.total_devices,
            schedulable_devices: device_snapshot.schedulable_devices,
            total_used_bytes: device_snapshot.total_used_bytes,
            total_reserved_bytes: device_snapshot.total_reserved_bytes,
            total_capacity_bytes: device_snapshot.total_capacity_bytes,
            by_state: device_snapshot.by_state.into_iter().collect(),
        });
        let offload_snapshot = self.pending_offloads.observability_snapshot(Instant::now());
        registry::record_cold_tier_pending_offload_metrics(
            registry::ColdTierPendingOffloadMetrics {
                runtime: &self.runtime.stable_id.0,
                total_pending: offload_snapshot.total_pending,
                ready: offload_snapshot.ready,
                delayed: offload_snapshot.delayed,
                max_attempts: offload_snapshot.max_attempts,
                total_attempts: offload_snapshot.total_attempts,
            },
        );
        let reclaim_snapshot = self.state.lock().reclaim_queue_snapshot(now_ms());
        registry::record_cold_tier_reclaim_metrics(registry::ColdTierReclaimMetrics {
            runtime: &self.runtime.stable_id.0,
            total_pending: reclaim_snapshot.total_pending,
            due: reclaim_snapshot.due,
            cold_backing_reclaims: reclaim_snapshot.cold_backing_reclaims,
            hot_segment_reclaims: reclaim_snapshot.hot_segment_reclaims,
            by_qos_tier: reclaim_snapshot
                .by_qos_tier
                .iter()
                .map(|(qos_tier, count)| (qos_tier.as_str(), *count))
                .collect(),
            by_policy_rank: reclaim_snapshot.by_policy_rank.into_iter().collect(),
        });
    }

    pub(crate) fn materialize_pending_offloads_bounded(&self, max_tasks: usize) -> Result<usize> {
        crate::client::cold_tier::materialize_pending_offloads_bounded(self, max_tasks)
    }

    pub(in super::super) fn prepare_pending_offload_entries(
        &self,
        entries: Vec<PendingOffloadEntry>,
    ) -> Result<Vec<PendingOffloadMaterialization>> {
        crate::client::cold_tier::prepare_pending_offload_entries(self, entries)
    }

    pub(in super::super) fn prepare_pending_offload_entry(
        &self,
        entry: PendingOffloadEntry,
        route: Option<super::super::ObjectRoute>,
        devices: &BTreeMap<String, ColdTierDeviceRecord>,
    ) -> Result<PendingOffloadPrepareOutcome> {
        crate::client::cold_tier::prepare_pending_offload_entry(self, entry, route, devices)
    }

    pub(in super::super) fn materialize_prepared_pending_offload_batch(
        &self,
        pending: &[PendingOffloadMaterialization],
        first_error: &mut Option<StoreError>,
    ) -> Result<usize> {
        crate::client::cold_tier::materialize_prepared_pending_offload_batch(
            self,
            pending,
            first_error,
        )
    }

    fn record_cold_tier_cleanup_error(&self, device_id: &str, error: &StoreError) {
        let Ok(Some(device)) = self.metadata.as_ref().get_cold_tier_device(device_id) else {
            return;
        };
        let mut update = mooncake_store_core::ColdTierDeviceUpdate::new(current_time_ms());
        update.expected_updated_at_ms = Some(device.updated_at_ms);
        update.failure_count = Some(device.failure_count.saturating_add(1));
        update.last_error = Some(Some(error.to_string()));
        if let Ok(updated) = self
            .metadata
            .as_ref()
            .update_cold_tier_device(device_id, update)
        {
            self.cold_tier_devices.upsert(updated);
        }
    }

    pub(crate) fn unregister_cold_tier_devices_on_shutdown(&self) {
        let devices = match self.metadata.as_ref().list_cold_tier_devices(
            &mooncake_store_core::ColdTierDeviceFilter {
                stable_id: Some(self.runtime.stable_id.0.clone()),
                ..mooncake_store_core::ColdTierDeviceFilter::default()
            },
        ) {
            Ok(devices) => devices,
            Err(error) => {
                tracing::warn!(error = %error, runtime = %self.runtime, "cold-tier device shutdown unregister failed to list devices");
                return;
            }
        };
        let mut unregistered = 0usize;
        for device in devices.into_iter().filter(|device| {
            device.epoch == Some(self.runtime.epoch.0)
                && device.state != mooncake_store_core::ColdTierDeviceState::Unregistered
        }) {
            let mut update = mooncake_store_core::ColdTierDeviceUpdate::new(current_time_ms());
            update.expected_updated_at_ms = Some(device.updated_at_ms);
            update.epoch = Some(None);
            update.root_dir = Some(None);
            update.state = Some(mooncake_store_core::ColdTierDeviceState::Unregistered);
            update.reserved_bytes = Some(0);
            update.last_error = Some(None);
            match self
                .metadata
                .as_ref()
                .update_cold_tier_device(&device.device_id, update)
            {
                Ok(updated) => {
                    self.cold_tier_devices.upsert(updated);
                    unregistered = unregistered.saturating_add(1);
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        runtime = %self.runtime,
                        device_id = %device.device_id,
                        "cold-tier device shutdown unregister failed"
                    );
                }
            }
        }
    }

    pub(crate) fn release_nof_ownership_on_shutdown(
        &self,
        control_client: &crate::control_plane::ControlPlaneClient,
    ) {
        self.cold_tier_devices
            .release_nof_ownership_on_shutdown(control_client);
    }
}

fn managed_nof_backing_contains_target(
    backing: &mooncake_store_core::NofBackingRoute,
    target_id: &str,
) -> bool {
    backing.target_id == target_id
        || backing
            .replicas
            .iter()
            .any(|replica| replica.target_id == target_id)
}

fn managed_nof_target_removal_is_safe(
    route: &ObjectRoute,
    backing: &mooncake_store_core::NofBackingRoute,
) -> bool {
    !route.replicas.is_empty() || managed_nof_target_count(backing) > 1
}

fn managed_nof_target_count(backing: &mooncake_store_core::NofBackingRoute) -> usize {
    1 + backing.replicas.len()
}
