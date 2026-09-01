use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
};

use mooncake_store_core::ClientRuntimeId;

use super::replica_policy::{
    ReplicaLoadBalanceStrategy, ReplicaReadCandidate, ReplicaTargetSet,
    DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY,
};

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
    let local_replica = &cold_backing.replicas[idx];
    promote_cold_backing_target(
        cold_backing,
        &ReplicaTargetResult {
            owner: local_replica.owner.clone(),
            target_id: local_replica.cold_tier_id.clone(),
            object_locator: local_replica.object_locator.clone(),
        },
    );
}

pub(in super::super) fn promote_cold_backing_target(
    cold_backing: &mut mooncake_store_core::ColdBackingRoute,
    selected: &ReplicaTargetResult,
) {
    if selected.owner == cold_backing.owner
        && selected.target_id == cold_backing.cold_tier_id
        && selected.object_locator == cold_backing.object_locator
    {
        return;
    }
    let Some(index) = cold_backing.replicas.iter().position(|replica| {
        replica.owner == selected.owner
            && replica.cold_tier_id == selected.target_id
            && replica.object_locator == selected.object_locator
    }) else {
        return;
    };
    let local_replica = cold_backing.replicas.swap_remove(index);
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

/// Per-batch target selector shared by local Cold Tier and NoF reads.
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
pub(in super::super) struct ReplicaTargetSelector {
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
    target_id: String,
    count: u64,
}

/// Result of target selection — full routing info for the chosen replica.
pub(in super::super) struct ReplicaTargetResult {
    pub owner: ClientRuntimeId,
    pub target_id: String,
    pub object_locator: String,
}

// ---------------------------------------------------------------------------
// Global per-(owner, target) cumulative IO counter.
//
// HashMap with AtomicU64 counts:
//   - Reads:  RwLock::read() (shared) + O(1) lookup + AtomicU64::load(Relaxed)
//   - Writes: common path is read() + fetch_add (entry exists); only the first
//     occurrence of a new (owner, target) pair takes write() to insert.
//
// The map is bounded to avoid stale runtime/target pairs accumulating forever
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

fn global_accum_count(entries: &GlobalAccumMap, owner: &ClientRuntimeId, target_id: &str) -> u64 {
    entries
        .get(&(owner.clone(), target_id.to_string()))
        .map(|count| count.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Increment global accum for (owner, target). Fast path (read lock +
/// fetch_add) when the entry exists; slow path (write lock + insert) only for
/// the first occurrence of each (owner, target) pair.
fn increment_global_accum(owner: &ClientRuntimeId, target_id: &str) {
    let key = (owner.clone(), target_id.to_string());
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

impl ReplicaTargetSelector {
    /// Create a new selector, snapshotting inflight restore counts from
    /// the admission controller (non-blocking: returns empty on contention).
    pub fn new(admission: &super::super::ColdTierAdmission) -> Self {
        let inflight_snapshot = match admission.try_snapshot_device_inflight() {
            Some(map) => map.into_iter().collect(),
            None => Vec::new(),
        };
        Self::from_inflight_snapshot(inflight_snapshot)
    }

    /// Construct a selector from a provider-neutral target load snapshot.
    ///
    /// NoF uses this entry point because its targets are not local Cold Tier devices.
    pub fn from_inflight_snapshot(inflight_snapshot: Vec<(String, u64)>) -> Self {
        Self {
            batch_ios: Vec::new(),
            inflight_snapshot,
        }
    }

    /// Lookup batch IO count by borrowed references — zero clones.
    fn batch_count(&self, owner: &ClientRuntimeId, target_id: &str) -> u64 {
        self.batch_ios
            .iter()
            .find(|entry| &entry.owner == owner && entry.target_id == target_id)
            .map(|e| e.count)
            .unwrap_or(0)
    }

    /// Increment batch IO count. Appends a new entry if the (owner, target)
    /// pair is seen for the first time — only clones then.
    fn batch_increment(&mut self, owner: &ClientRuntimeId, target_id: &str) {
        if let Some(entry) = self
            .batch_ios
            .iter_mut()
            .find(|entry| &entry.owner == owner && entry.target_id == target_id)
        {
            entry.count += 1;
        } else {
            self.batch_ios.push(BatchIoEntry {
                owner: owner.clone(),
                target_id: target_id.to_string(),
                count: 1,
            });
        }
    }

    /// Lookup inflight count for a target — zero clones.
    fn inflight_count(&self, target_id: &str) -> u64 {
        self.inflight_snapshot
            .iter()
            .find(|(id, _)| id == target_id)
            .map(|(_, c)| *c)
            .unwrap_or(0)
    }

    /// Select the best persistent target for reading this object.
    ///
    /// For single-replica objects, returns the primary target immediately.
    ///
    /// For multi-replica objects, picks the target with the lowest
    /// `(inflight_io, batch_ios, global_accum_ios)` tuple — three-layer
    /// tiebreaking for real-time load, intra-batch balance, and long-term
    /// fairness.
    ///
    /// Hot path: zero exclusive locks; only lookup keys and the final result are cloned.
    pub fn select_target<T: ReplicaTargetSet + ?Sized>(
        &mut self,
        backing: &T,
    ) -> ReplicaTargetResult {
        let targets = backing.replica_targets();
        if targets.len() <= 1 {
            let target = targets
                .first()
                .expect("a persistent backing has at least one target");
            return ReplicaTargetResult {
                owner: target.owner.clone(),
                target_id: target.target_id.to_string(),
                object_locator: target.object_locator.to_string(),
            };
        }

        // Shared read lock — never blocks other readers.
        let global = global_accum_ios().read();
        let candidates = targets
            .iter()
            .map(|target| ReplicaReadCandidate {
                inflight_io: self.inflight_count(target.target_id),
                batch_ios: self.batch_count(target.owner, target.target_id),
                global_accum_ios: global_accum_count(&global, target.owner, target.target_id),
            })
            .collect::<Vec<_>>();
        let selected_idx = DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY
            .select_read_target(&candidates)
            .expect("targets is non-empty");

        let selected = &targets[selected_idx];

        // Update batch-local counter (Vec scan, clone only on first occurrence).
        self.batch_increment(selected.owner, selected.target_id);

        // Update global accum (atomic under shared read lock — common path).
        let global_key = (selected.owner.clone(), selected.target_id.to_string());
        if let Some(count) = global.get(&global_key) {
            count.fetch_add(1, Ordering::Relaxed);
        } else {
            drop(global);
            increment_global_accum(selected.owner, selected.target_id);
        }

        ReplicaTargetResult {
            owner: selected.owner.clone(),
            target_id: selected.target_id.to_string(),
            object_locator: selected.object_locator.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{
        ClientEpoch, ClientStableId, ColdBackingReplica, ColdBackingRoute, ColdBackingState,
        NofBackingReplica, NofBackingRoute, NofBackingState,
    };

    fn owner(name: &str) -> ClientRuntimeId {
        ClientRuntimeId {
            stable_id: ClientStableId::new(name),
            epoch: ClientEpoch(1),
        }
    }

    #[test]
    fn selector_balances_local_and_nof_routes_with_the_same_policy() {
        let owner = owner("shared-replica-selector");
        let cold = ColdBackingRoute {
            owner: owner.clone(),
            cold_tier_id: "shared-cold-busy".to_string(),
            object_locator: "cold-a".to_string(),
            length: 1,
            checksum: None,
            state: ColdBackingState::Materialized,
            replicas: vec![ColdBackingReplica {
                owner: owner.clone(),
                cold_tier_id: "shared-cold-idle".to_string(),
                object_locator: "cold-b".to_string(),
            }],
        };
        let nof = NofBackingRoute {
            owner: owner.clone(),
            target_id: "shared-nof-busy".to_string(),
            object_locator: "nof-a".to_string(),
            length: 1,
            checksum: None,
            state: NofBackingState::Materialized,
            replicas: vec![NofBackingReplica {
                owner,
                target_id: "shared-nof-idle".to_string(),
                object_locator: "nof-b".to_string(),
            }],
        };
        let mut selector = ReplicaTargetSelector::from_inflight_snapshot(vec![
            ("shared-cold-busy".to_string(), 2),
            ("shared-cold-idle".to_string(), 0),
            ("shared-nof-busy".to_string(), 2),
            ("shared-nof-idle".to_string(), 0),
        ]);

        assert_eq!(selector.select_target(&cold).target_id, "shared-cold-idle");
        assert_eq!(selector.select_target(&nof).target_id, "shared-nof-idle");
    }
}
