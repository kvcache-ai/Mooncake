use super::super::{ObjectKey, ObjectRoute, SegmentName};
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
