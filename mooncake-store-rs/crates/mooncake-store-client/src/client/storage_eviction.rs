// Generic storage-owner eviction and reclaim tracking state.
// Included via `include!()` at module level in mod.rs.

#[derive(Default)]
struct HotReplicaTracker {
    clock: Mutex<StorageClockState>,
}

#[derive(Default)]
struct StorageClockState {
    entries: Vec<Option<ClockEntry>>,
    by_id: BTreeMap<ClockEntryId, usize>,
    by_key: BTreeMap<ObjectKey, Vec<usize>>,
    pending_hot_keys: BTreeSet<ObjectKey>,
    hand: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ClockEntryId {
    route_key: ObjectKey,
    segment_name: SegmentName,
    segment_offset: u64,
}

#[derive(Clone, Debug)]
struct ClockEntry {
    id: ClockEntryId,
    length_bytes: u64,
    hot: bool,
    hot_credit: u8,
    fresh_write: bool,
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

impl StorageClockState {
    const READ_HOT_CLOCK_CREDIT: u8 = 2;

    fn eviction_budget(&self) -> usize {
        self.entries.len().saturating_mul(3)
    }

    fn track_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.track_route_with_fresh_write(route, runtime, false);
    }

    fn track_fresh_route(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
    ) {
        self.track_route_with_fresh_write(route, runtime, true);
    }

    fn track_route_with_fresh_write(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
        fresh_write: bool,
    ) {
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner == *runtime)
        {
            self.upsert_replica(route, replica, fresh_write);
        }
    }

    fn untrack_route(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
    ) {
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner == *runtime)
        {
            self.remove_id(&ClockEntryId {
                route_key: route.key.clone(),
                segment_name: replica.segment_name.clone(),
                segment_offset: replica.segment_offset,
            });
        }
    }

    fn sync_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.remove_key(&route.key);
        self.track_route(route, runtime);
    }

    fn sync_fresh_route(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
    ) {
        self.remove_key(&route.key);
        self.track_fresh_route(route, runtime);
    }

    fn mark_hot_keys(&mut self, keys: &[ObjectKey]) -> RouteTrafficReport {
        let mut slots = Vec::new();
        let mut bytes = 0u64;
        for key in keys {
            self.pending_hot_keys.insert(key.clone());
            if let Some(indices) = self.by_key.get(key) {
                for index in indices {
                    if let Some(entry) = self.entries.get(*index).and_then(Option::as_ref) {
                        bytes = bytes.saturating_add(entry.length_bytes);
                        slots.push(*index);
                    }
                }
            }
        }
        for index in slots {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.hot = true;
                entry.hot_credit = Self::READ_HOT_CLOCK_CREDIT;
                entry.fresh_write = false;
            }
        }
        RouteTrafficReport::new(keys.len(), bytes)
    }

    fn pick_victim(
        &mut self,
        preferred_segment: Option<&SegmentName>,
        policy: ColdTierEvictionPriorityPolicy,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        match policy {
            ColdTierEvictionPriorityPolicy::Clock => self.pick_victim_clock(preferred_segment),
            ColdTierEvictionPriorityPolicy::ColdestLargestFirst => {
                self.pick_victim_coldest_largest_first(preferred_segment, scan_limit)
            }
        }
    }

    fn pick_victim_clock(
        &mut self,
        preferred_segment: Option<&SegmentName>,
    ) -> Option<ClockEntryId> {
        if self.entries.is_empty() {
            return None;
        }
        let limit = self.eviction_budget().max(1);
        for _ in 0..limit {
            let index = self.hand % self.entries.len();
            self.hand = (self.hand + 1) % self.entries.len();
            let Some(entry) = self.entries[index].as_mut() else {
                continue;
            };
            if preferred_segment.is_some_and(|segment| entry.id.segment_name != *segment) {
                continue;
            }
            if entry.hot_credit > 0 {
                entry.hot_credit -= 1;
                entry.hot = entry.hot_credit > 0;
                continue;
            }
            if entry.fresh_write {
                entry.fresh_write = false;
                continue;
            }
            return Some(entry.id.clone());
        }
        None
    }

    fn upsert_replica(
        &mut self,
        route: &ObjectRoute,
        replica: &ReplicaRoute,
        fresh_write: bool,
    ) {
        let id = ClockEntryId {
            route_key: route.key.clone(),
            segment_name: replica.segment_name.clone(),
            segment_offset: replica.segment_offset,
        };
        if let Some(index) = self.by_id.get(&id).copied() {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.length_bytes = replica.length;
                if fresh_write && entry.hot_credit == 0 {
                    entry.fresh_write = true;
                }
            }
            return;
        }
        let hot_credit = if self.pending_hot_keys.contains(&route.key) {
            Self::READ_HOT_CLOCK_CREDIT
        } else {
            0
        };
        let entry = ClockEntry {
            id: id.clone(),
            length_bytes: replica.length,
            hot: hot_credit > 0,
            hot_credit,
            fresh_write: fresh_write && hot_credit == 0,
        };
        let index = self
            .entries
            .iter()
            .position(Option::is_none)
            .unwrap_or(self.entries.len());
        if index == self.entries.len() {
            self.entries.push(Some(entry));
        } else {
            self.entries[index] = Some(entry);
        }
        self.by_id.insert(id.clone(), index);
        self.by_key.entry(id.route_key).or_default().push(index);
    }

    fn remove_key(&mut self, key: &ObjectKey) {
        let indices = self.by_key.remove(key).unwrap_or_default();
        for index in indices {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::take) {
                self.by_id.remove(&entry.id);
            }
        }
    }

    fn remove_id(&mut self, id: &ClockEntryId) {
        let Some(index) = self.by_id.remove(id) else {
            return;
        };
        self.entries[index] = None;
        if let Some(indices) = self.by_key.get_mut(&id.route_key) {
            indices.retain(|candidate| *candidate != index);
            if indices.is_empty() {
                self.by_key.remove(&id.route_key);
            }
        }
    }

    fn pick_victim_coldest_largest_first(
        &mut self,
        preferred_segment: Option<&SegmentName>,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        if self.entries.is_empty() {
            return None;
        }
        let limit = scan_limit.max(1).min(self.eviction_budget().max(1));
        let mut selected: Option<(ClockEntryId, u64)> = None;
        for _ in 0..limit {
            let index = self.hand % self.entries.len();
            self.hand = (self.hand + 1) % self.entries.len();
            let Some(entry) = self.entries[index].as_mut() else {
                continue;
            };
            if preferred_segment.is_some_and(|segment| entry.id.segment_name != *segment) {
                continue;
            }
            if entry.hot {
                entry.hot = false;
                continue;
            }
            match selected.as_ref() {
                Some((_, length)) if *length >= entry.length_bytes => {}
                _ => selected = Some((entry.id.clone(), entry.length_bytes)),
            }
        }
        selected.map(|(id, _)| id)
    }
}

#[allow(dead_code)]
impl HotReplicaTracker {
    fn eviction_budget(&self) -> usize {
        self.clock.lock().eviction_budget()
    }

    fn track_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().track_route(route, runtime);
    }

    fn untrack_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().untrack_route(route, runtime);
    }

    fn sync_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().sync_route(route, runtime);
    }

    fn sync_routes(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = self.clock.lock();
        for route in routes {
            clock.sync_route(route, runtime);
        }
    }

    fn mark_hot_keys(&self, keys: &[ObjectKey]) {
        self.clock.lock().mark_hot_keys(keys);
    }

    fn pick_victim(
        &self,
        preferred_segment: Option<&SegmentName>,
        policy: ColdTierEvictionPriorityPolicy,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        self.clock
            .lock()
            .pick_victim(preferred_segment, policy, scan_limit)
    }

    fn remove_id(&self, id: &ClockEntryId) {
        self.clock.lock().remove_id(id);
    }

    fn remove_key(&self, key: &ObjectKey) {
        self.clock.lock().remove_key(key);
    }

    fn rebuild(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = StorageClockState::default();
        for route in routes {
            clock.track_route(route, runtime);
        }
        *self.clock.lock() = clock;
    }
}


#[allow(dead_code)]
impl StoreState {
    fn reclaim_queue_snapshot(&self, now_ms: u64) -> ReclaimQueueSnapshot {
        let mut snapshot = ReclaimQueueSnapshot {
            total_pending: self.pending_reclaims.len(),
            ..ReclaimQueueSnapshot::default()
        };
        for reclaim in &self.pending_reclaims {
            if reclaim.due_at_ms <= now_ms {
                snapshot.due = snapshot.due.saturating_add(1);
            }
            if reclaim.cold_backing.is_some() {
                snapshot.cold_backing_reclaims = snapshot.cold_backing_reclaims.saturating_add(1);
            } else {
                snapshot.hot_segment_reclaims = snapshot.hot_segment_reclaims.saturating_add(1);
            }
            *snapshot
                .by_qos_tier
                .entry(reclaim.qos_tier.clone())
                .or_default() += 1;
            *snapshot
                .by_policy_rank
                .entry(reclaim.policy_rank)
                .or_default() += 1;
        }
        snapshot
    }
}
