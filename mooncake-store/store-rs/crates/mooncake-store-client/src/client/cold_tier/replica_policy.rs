//! Provider-neutral replica placement and read load-balancing policy.

use std::cmp::Ordering;

use mooncake_store_core::{ClientRuntimeId, ColdBackingRoute, NofBackingRoute};

/// A persistent replica target independent of its backing implementation.
#[derive(Clone, Copy)]
pub(in crate::client) struct ReplicaTarget<'a> {
    pub owner: &'a ClientRuntimeId,
    pub target_id: &'a str,
    pub object_locator: &'a str,
}

/// Exposes the primary and replica targets of a persistent backing route.
pub(in crate::client) trait ReplicaTargetSet {
    fn replica_targets(&self) -> Vec<ReplicaTarget<'_>>;
}

impl ReplicaTargetSet for ColdBackingRoute {
    fn replica_targets(&self) -> Vec<ReplicaTarget<'_>> {
        self.all_targets()
            .into_iter()
            .map(|target| ReplicaTarget {
                owner: target.owner,
                target_id: target.cold_tier_id,
                object_locator: target.object_locator,
            })
            .collect()
    }
}

impl ReplicaTargetSet for NofBackingRoute {
    fn replica_targets(&self) -> Vec<ReplicaTarget<'_>> {
        self.all_targets()
            .into_iter()
            .map(|target| ReplicaTarget {
                owner: target.owner,
                target_id: target.target_id,
                object_locator: target.object_locator,
            })
            .collect()
    }
}

/// Provider-computed write suitability for one replica target.
///
/// The policy only orders candidates. Local Cold Tier computes this score from physical free
/// capacity and restore load; NoF can supply a score based on configured logical capacity and
/// target load without pretending that the provider exposes physical disk capacity.
#[derive(Clone, Copy, Debug)]
pub(in crate::client) struct ReplicaWriteCandidate<'a> {
    pub target_id: &'a str,
    pub score: f64,
    pub accumulated_writes: u64,
}

/// Load snapshot for one readable replica target.
#[derive(Clone, Copy, Debug)]
pub(in crate::client) struct ReplicaReadCandidate {
    pub inflight_io: u64,
    pub batch_ios: u64,
    pub global_accum_ios: u64,
}

/// Shared multi-replica placement and read load-balancing strategy.
///
/// Backings remain responsible for eligibility and provider-specific scoring. The strategy is
/// deliberately limited to ordering eligible targets so local disks and NoF can share it without
/// sharing storage-management policy.
pub(in crate::client) trait ReplicaLoadBalanceStrategy: Send + Sync {
    /// Select up to `replica_count` write targets, in preferred order.
    fn select_write_targets(
        &self,
        candidates: &[ReplicaWriteCandidate<'_>],
        replica_count: usize,
    ) -> Vec<usize>;

    /// Select one readable target from an already filtered candidate set.
    fn select_read_target(&self, candidates: &[ReplicaReadCandidate]) -> Option<usize>;
}

/// Existing Mooncake policy expressed independently of a storage provider.
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::client) struct DefaultReplicaLoadBalanceStrategy;

impl ReplicaLoadBalanceStrategy for DefaultReplicaLoadBalanceStrategy {
    fn select_write_targets(
        &self,
        candidates: &[ReplicaWriteCandidate<'_>],
        replica_count: usize,
    ) -> Vec<usize> {
        let mut selected = (0..candidates.len()).collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            let left = &candidates[*left];
            let right = &candidates[*right];
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.accumulated_writes.cmp(&right.accumulated_writes))
                .then_with(|| left.target_id.cmp(right.target_id))
        });
        selected.truncate(replica_count);
        selected
    }

    fn select_read_target(&self, candidates: &[ReplicaReadCandidate]) -> Option<usize> {
        candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, candidate)| {
                (
                    candidate.inflight_io,
                    candidate.batch_ios,
                    candidate.global_accum_ios,
                )
            })
            .map(|(index, _)| index)
    }
}

pub(in crate::client) static DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY:
    DefaultReplicaLoadBalanceStrategy = DefaultReplicaLoadBalanceStrategy;

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{
        ClientEpoch, ClientStableId, ColdBackingReplica, ColdBackingState, NofBackingReplica,
        NofBackingState,
    };

    fn owner(name: &str) -> ClientRuntimeId {
        ClientRuntimeId {
            stable_id: ClientStableId::new(name),
            epoch: ClientEpoch(1),
        }
    }

    #[test]
    fn write_selection_preserves_score_and_target_id_ordering() {
        let candidates = [
            ReplicaWriteCandidate {
                target_id: "target-c",
                score: 0.5,
                accumulated_writes: 0,
            },
            ReplicaWriteCandidate {
                target_id: "target-b",
                score: 0.8,
                accumulated_writes: 0,
            },
            ReplicaWriteCandidate {
                target_id: "target-a",
                score: 0.8,
                accumulated_writes: 0,
            },
        ];

        assert_eq!(
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, 2),
            vec![2, 1]
        );
    }

    #[test]
    fn write_selection_uses_accumulated_writes_when_scores_are_equal() {
        let candidates = [
            ReplicaWriteCandidate {
                target_id: "target-a",
                score: 0.0,
                accumulated_writes: 4,
            },
            ReplicaWriteCandidate {
                target_id: "target-b",
                score: 0.0,
                accumulated_writes: 1,
            },
        ];

        assert_eq!(
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, 1),
            vec![1]
        );
    }

    #[test]
    fn read_selection_preserves_three_layer_load_ordering() {
        let candidates = [
            ReplicaReadCandidate {
                inflight_io: 1,
                batch_ios: 0,
                global_accum_ios: 0,
            },
            ReplicaReadCandidate {
                inflight_io: 0,
                batch_ios: 10,
                global_accum_ios: 10,
            },
        ];

        assert_eq!(
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_read_target(&candidates),
            Some(1)
        );
    }

    #[test]
    fn local_and_nof_routes_expose_the_same_target_shape() {
        let primary = owner("primary");
        let replica = owner("replica");
        let cold = ColdBackingRoute {
            owner: primary.clone(),
            cold_tier_id: "cold-a".to_string(),
            object_locator: "cold-locator-a".to_string(),
            length: 1,
            checksum: None,
            state: ColdBackingState::Materialized,
            replicas: vec![ColdBackingReplica {
                owner: replica.clone(),
                cold_tier_id: "cold-b".to_string(),
                object_locator: "cold-locator-b".to_string(),
            }],
        };
        let nof = NofBackingRoute {
            owner: primary,
            target_id: "nof-a".to_string(),
            object_locator: "nof-locator-a".to_string(),
            length: 1,
            checksum: None,
            state: NofBackingState::Materialized,
            replicas: vec![NofBackingReplica {
                owner: replica,
                target_id: "nof-b".to_string(),
                object_locator: "nof-locator-b".to_string(),
            }],
        };

        let cold_targets = cold.replica_targets();
        let nof_targets = nof.replica_targets();
        assert_eq!(cold_targets.len(), 2);
        assert_eq!(nof_targets.len(), 2);
        assert_eq!(cold_targets[1].target_id, "cold-b");
        assert_eq!(nof_targets[1].target_id, "nof-b");
    }
}
