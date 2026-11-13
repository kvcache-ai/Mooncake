//! Provider-neutral replica placement and read load-balancing policy.

// Capacity snapshots are updated asynchronously. Treat sub-per-mille score drift as the same
// tier so the process-local byte counter, rather than a stale capacity sample, keeps equal-class
// devices balanced. Material score and restore-load differences still win before this tie-breaker.
const WRITE_SCORE_BUCKET_WIDTH: f64 = 1e-3;

fn write_score_bucket(score: f64) -> i64 {
    if score.is_finite() {
        (score / WRITE_SCORE_BUCKET_WIDTH).round() as i64
    } else {
        0
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
    pub accumulated_write_bytes: u64,
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
            write_score_bucket(right.score)
                .cmp(&write_score_bucket(left.score))
                .then_with(|| {
                    left.accumulated_write_bytes
                        .cmp(&right.accumulated_write_bytes)
                })
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

    #[test]
    fn write_selection_preserves_score_and_target_id_ordering() {
        let candidates = [
            ReplicaWriteCandidate {
                target_id: "target-c",
                score: 0.5,
                accumulated_write_bytes: 0,
            },
            ReplicaWriteCandidate {
                target_id: "target-b",
                score: 0.8,
                accumulated_write_bytes: 0,
            },
            ReplicaWriteCandidate {
                target_id: "target-a",
                score: 0.8,
                accumulated_write_bytes: 0,
            },
        ];

        assert_eq!(
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, 2),
            vec![2, 1]
        );
    }

    #[test]
    fn write_selection_uses_accumulated_bytes_when_scores_are_equal() {
        let candidates = [
            ReplicaWriteCandidate {
                target_id: "target-a",
                score: 0.0,
                accumulated_write_bytes: 4096,
            },
            ReplicaWriteCandidate {
                target_id: "target-b",
                score: 0.0,
                accumulated_write_bytes: 1024,
            },
        ];

        assert_eq!(
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, 1),
            vec![1]
        );
    }

    #[test]
    fn write_selection_uses_accumulated_bytes_when_scores_are_nearly_equal() {
        let candidates = [
            ReplicaWriteCandidate {
                target_id: "target-a",
                score: 0.800_003,
                accumulated_write_bytes: 4096,
            },
            ReplicaWriteCandidate {
                target_id: "target-b",
                score: 0.8,
                accumulated_write_bytes: 1024,
            },
        ];

        assert_eq!(
            DEFAULT_REPLICA_LOAD_BALANCE_STRATEGY.select_write_targets(&candidates, 1),
            vec![1]
        );
    }

    #[test]
    fn write_selection_ignores_stale_sub_per_mille_capacity_drift() {
        let candidates = [
            ReplicaWriteCandidate {
                target_id: "target-a",
                score: 0.800_3,
                accumulated_write_bytes: 54 * 1024,
            },
            ReplicaWriteCandidate {
                target_id: "target-b",
                score: 0.8,
                accumulated_write_bytes: 0,
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
}
