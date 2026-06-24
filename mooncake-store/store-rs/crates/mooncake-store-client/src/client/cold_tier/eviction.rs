use super::super::{
    ClientRuntimeId, ColdTierEvictionPriorityPolicy, ObjectKey, ObjectRoute, ReplicaRoute,
    RouteTrafficReport, SegmentName,
};
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar as StdCondvar, Mutex as StdMutex};
use std::time::Duration;

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

#[derive(Default)]
pub(in super::super) struct HotReplicaTracker {
    pub(in super::super) clock: Mutex<StorageClockState>,
}

#[derive(Default)]
pub(in super::super) struct StorageClockState {
    pub(in super::super) entries: Vec<Option<ClockEntry>>,
    pub(in super::super) by_id: BTreeMap<ClockEntryId, usize>,
    pub(in super::super) by_key: BTreeMap<ObjectKey, Vec<usize>>,
    pub(in super::super) pending_hot_keys: BTreeSet<ObjectKey>,
    pub(in super::super) hand: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(in super::super) struct ClockEntryId {
    pub(in super::super) route_key: ObjectKey,
    pub(in super::super) segment_name: SegmentName,
    pub(in super::super) segment_offset: u64,
}

#[derive(Clone, Debug)]
pub(in super::super) struct ClockEntry {
    pub(in super::super) id: ClockEntryId,
    pub(in super::super) length_bytes: u64,
    pub(in super::super) hot: bool,
    pub(in super::super) hot_credit: u8,
    pub(in super::super) fresh_write: bool,
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

impl StorageClockState {
    const READ_HOT_CLOCK_CREDIT: u8 = 2;

    pub(in super::super) fn eviction_budget(&self) -> usize {
        self.entries.len().saturating_mul(3)
    }

    pub(in super::super) fn track_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.track_route_with_fresh_write(route, runtime, false);
    }

    pub(in super::super) fn track_fresh_route(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
    ) {
        self.track_route_with_fresh_write(route, runtime, true);
    }

    pub(in super::super) fn track_route_with_fresh_write(
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

    pub(in super::super) fn untrack_route(
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

    pub(in super::super) fn sync_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.remove_key(&route.key);
        self.track_route(route, runtime);
    }

    pub(in super::super) fn sync_fresh_route(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
    ) {
        self.remove_key(&route.key);
        self.track_fresh_route(route, runtime);
    }

    pub(in super::super) fn mark_hot_keys(&mut self, keys: &[ObjectKey]) -> RouteTrafficReport {
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

    pub(in super::super) fn pick_victim(
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

    pub(in super::super) fn pick_victim_clock(
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

    pub(in super::super) fn upsert_replica(
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

    pub(in super::super) fn remove_key(&mut self, key: &ObjectKey) {
        let indices = self.by_key.remove(key).unwrap_or_default();
        for index in indices {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::take) {
                self.by_id.remove(&entry.id);
            }
        }
    }

    pub(in super::super) fn remove_id(&mut self, id: &ClockEntryId) {
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

    pub(in super::super) fn pick_victim_coldest_largest_first(
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
    pub(in super::super) fn eviction_budget(&self) -> usize {
        self.clock.lock().eviction_budget()
    }

    pub(in super::super) fn track_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().track_route(route, runtime);
    }

    pub(in super::super) fn untrack_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().untrack_route(route, runtime);
    }

    pub(in super::super) fn sync_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().sync_route(route, runtime);
    }

    fn sync_routes(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = self.clock.lock();
        for route in routes {
            clock.sync_route(route, runtime);
        }
    }

    pub(in super::super) fn mark_hot_keys(&self, keys: &[ObjectKey]) {
        self.clock.lock().mark_hot_keys(keys);
    }

    pub(in super::super) fn pick_victim(
        &self,
        preferred_segment: Option<&SegmentName>,
        policy: ColdTierEvictionPriorityPolicy,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        self.clock
            .lock()
            .pick_victim(preferred_segment, policy, scan_limit)
    }

    pub(in super::super) fn remove_id(&self, id: &ClockEntryId) {
        self.clock.lock().remove_id(id);
    }

    pub(in super::super) fn remove_key(&self, key: &ObjectKey) {
        self.clock.lock().remove_key(key);
    }

    pub(in super::super) fn rebuild(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = StorageClockState::default();
        for route in routes {
            clock.track_route(route, runtime);
        }
        *self.clock.lock() = clock;
    }
}
