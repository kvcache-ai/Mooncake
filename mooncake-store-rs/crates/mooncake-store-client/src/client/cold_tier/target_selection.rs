use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
};

use mooncake_store_core::ClientRuntimeId;

/// Resolve-stage target selection for cold-backed objects with multiple replicas.
///
/// Priority:
///   1. Local target — any target whose `cold_tier_id` has a local backend gets
///      absolute preference.
///   2. Remote targets — if no local target exists, keep the primary unchanged.
///
/// When a non-primary target is selected, rewrites `cold_backing` primary fields
/// (`owner`, `cold_tier_id`, `object_locator`) so that the cold placeholder points
/// at the selected local/remote target.
pub(in super::super) fn select_cold_backing_target(
    cold_backing: &mut mooncake_store_core::ColdBackingRoute,
    is_local: impl Fn(&str) -> bool,
) {
    if cold_backing.replicas.is_empty() {
        return;
    }
    if is_local(&cold_backing.cold_tier_id) {
        return;
    }
    let local_idx = cold_backing
        .replicas
        .iter()
        .position(|r| is_local(&r.cold_tier_id));
    let Some(idx) = local_idx else {
        return;
    };
    let local_replica = cold_backing.replicas.swap_remove(idx);
    let old_primary = mooncake_store_core::ColdBackingReplica {
        owner: std::mem::replace(&mut cold_backing.owner, local_replica.owner),
        cold_tier_id: std::mem::replace(&mut cold_backing.cold_tier_id, local_replica.cold_tier_id),
        object_locator: std::mem::replace(
            &mut cold_backing.object_locator,
            local_replica.object_locator,
        ),
    };
    cold_backing.replicas.push(old_primary);
}

/// Per-batch target selector for multi-replica cold-tier reads.
///
/// Three-layer load balancing (inspired by 3FS `LoadBalanceStrategy`):
///
///   1. `inflight_snapshot` — real-time device restore load (try_lock, non-blocking)
///   2. `batch_ios`         — how many IOs this batch has routed to each target
///   3. `GLOBAL_ACCUM_IOS`  — cumulative per-target IO count across all batches
///
/// No exclusive locks on the hot path: `batch_ios` is a small Vec with borrowed lookup,
/// `GLOBAL_ACCUM_IOS` uses `RwLock<HashMap>` + `AtomicU64` (shared read lock +
/// atomic increment), and `inflight_snapshot` is taken once at construction via
/// `try_lock`.  `select_target` avoids exclusive locks on the common path.
pub(in super::super) struct ColdTierTargetSelector {
    /// Per-target IO count within this batch.  Vec with linear scan — device
    /// count is tens at most, and contiguous memory beats HashMap key
    /// construction + hashing.  No clones needed for lookup (borrowed compare).
    batch_ios: Vec<BatchIoEntry>,
    /// Snapshot of per-device inflight restore counts, taken once at
    /// construction via try_lock (non-blocking).
    inflight_snapshot: Vec<(String, u64)>,
}

struct BatchIoEntry {
    owner: ClientRuntimeId,
    device_id: String,
    count: u64,
}

/// Result of target selection — full routing info for the chosen replica.
pub(in super::super) struct ColdTierTargetResult {
    pub owner: ClientRuntimeId,
    pub cold_tier_id: String,
    pub object_locator: String,
}

// ---------------------------------------------------------------------------
// Global per-(owner, device) cumulative IO counter.
//
// HashMap with AtomicU64 counts:
//   - Reads:  RwLock::read() (shared) + O(1) lookup + AtomicU64::load(Relaxed)
//   - Writes: common path is read() + fetch_add (entry exists); only the first
//     occurrence of a new (owner, device) pair takes write() to insert.
//
// The map is bounded to avoid stale runtime/device pairs accumulating forever
// across restarts. When the bound is hit, counters reset; this only affects
// long-term tie-breaking fairness, not correctness.
// ---------------------------------------------------------------------------

const GLOBAL_ACCUM_MAX_ENTRIES: usize = 4096;
type GlobalAccumKey = (ClientRuntimeId, String);
type GlobalAccumMap = HashMap<GlobalAccumKey, AtomicU64>;

static GLOBAL_ACCUM_IOS: OnceLock<parking_lot::RwLock<GlobalAccumMap>> = OnceLock::new();

fn global_accum_ios() -> &'static parking_lot::RwLock<GlobalAccumMap> {
    GLOBAL_ACCUM_IOS.get_or_init(|| parking_lot::RwLock::new(HashMap::new()))
}

fn global_accum_count(entries: &GlobalAccumMap, owner: &ClientRuntimeId, device_id: &str) -> u64 {
    entries
        .get(&(owner.clone(), device_id.to_string()))
        .map(|count| count.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Increment global accum for (owner, device). Fast path (read lock +
/// fetch_add) when the entry exists; slow path (write lock + insert) only for
/// the first occurrence of each (owner, device) pair.
fn increment_global_accum(owner: &ClientRuntimeId, device_id: &str) {
    let key = (owner.clone(), device_id.to_string());
    {
        let entries = global_accum_ios().read();
        if let Some(count) = entries.get(&key) {
            count.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }
    let mut entries = global_accum_ios().write();
    if let Some(count) = entries.get(&key) {
        count.fetch_add(1, Ordering::Relaxed);
        return;
    }
    if entries.len() >= GLOBAL_ACCUM_MAX_ENTRIES {
        entries.clear();
    }
    entries.insert(key, AtomicU64::new(1));
}

impl ColdTierTargetSelector {
    /// Create a new selector, snapshotting inflight restore counts from
    /// the admission controller (non-blocking: returns empty on contention).
    pub fn new(admission: &super::super::ColdTierAdmission) -> Self {
        let inflight_snapshot = match admission.try_snapshot_device_inflight() {
            Some(map) => map.into_iter().collect(),
            None => Vec::new(),
        };
        Self {
            batch_ios: Vec::new(),
            inflight_snapshot,
        }
    }

    /// Lookup batch IO count by borrowed references — zero clones.
    fn batch_count(&self, owner: &ClientRuntimeId, device_id: &str) -> u64 {
        self.batch_ios
            .iter()
            .find(|e| &e.owner == owner && e.device_id == device_id)
            .map(|e| e.count)
            .unwrap_or(0)
    }

    /// Increment batch IO count.  Appends a new entry if the (owner, device)
    /// pair is seen for the first time — only clones then.
    fn batch_increment(&mut self, owner: &ClientRuntimeId, device_id: &str) {
        if let Some(entry) = self
            .batch_ios
            .iter_mut()
            .find(|e| &e.owner == owner && e.device_id == device_id)
        {
            entry.count += 1;
        } else {
            self.batch_ios.push(BatchIoEntry {
                owner: owner.clone(),
                device_id: device_id.to_string(),
                count: 1,
            });
        }
    }

    /// Lookup inflight count for a device — zero clones.
    fn inflight_count(&self, device_id: &str) -> u64 {
        self.inflight_snapshot
            .iter()
            .find(|(id, _)| id == device_id)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    }

    /// Select the best cold-tier target for reading this object.
    ///
    /// For single-replica objects, returns the primary target immediately.
    ///
    /// For multi-replica objects, picks the target with the lowest
    /// `(inflight_io, batch_ios, global_accum_ios)` tuple — three-layer
    /// tiebreaking for real-time load, intra-batch balance, and long-term
    /// fairness.
    ///
    /// Hot path: zero exclusive locks; only lookup keys and the final result are cloned.
    pub fn select_target(
        &mut self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> ColdTierTargetResult {
        let targets = cold_backing.all_targets();
        if targets.len() <= 1 {
            return ColdTierTargetResult {
                owner: cold_backing.owner.clone(),
                cold_tier_id: cold_backing.cold_tier_id.clone(),
                object_locator: cold_backing.object_locator.clone(),
            };
        }

        // Shared read lock — never blocks other readers.
        let global = global_accum_ios().read();

        let selected_idx = targets
            .iter()
            .enumerate()
            .min_by_key(|(_, t)| {
                let inflight = self.inflight_count(t.cold_tier_id);
                let batch = self.batch_count(t.owner, t.cold_tier_id);
                let global_count = global_accum_count(&global, t.owner, t.cold_tier_id);
                (inflight, batch, global_count)
            })
            .map(|(idx, _)| idx)
            .expect("targets is non-empty");

        let selected = &targets[selected_idx];

        // Update batch-local counter (Vec scan, clone only on first occurrence).
        self.batch_increment(selected.owner, selected.cold_tier_id);

        // Update global accum (atomic under shared read lock — common path).
        let global_key = (selected.owner.clone(), selected.cold_tier_id.to_string());
        if let Some(count) = global.get(&global_key) {
            count.fetch_add(1, Ordering::Relaxed);
        } else {
            drop(global);
            increment_global_accum(selected.owner, selected.cold_tier_id);
        }

        ColdTierTargetResult {
            owner: selected.owner.clone(),
            cold_tier_id: selected.cold_tier_id.to_string(),
            object_locator: selected.object_locator.to_string(),
        }
    }
}
