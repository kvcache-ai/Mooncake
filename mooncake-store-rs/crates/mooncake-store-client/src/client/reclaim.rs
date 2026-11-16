use std::collections::{BTreeMap, BTreeSet};

use mooncake_store_core::{ObjectKey, RouteVersion};

/// A backend-independent owner-local reclaim token.
///
/// The queue owns only protocol state.  The caller supplies the exact target
/// key and performs authority confirmation and physical release.  This keeps
/// allocator release and persistent cold-backing deletion on one retry/
/// backpressure path without pretending that their physical resources are the
/// same.
#[derive(Clone, Debug)]
pub(crate) struct ReclaimToken<K> {
    pub(crate) route_key: ObjectKey,
    pub(crate) expected_route_version: Option<RouteVersion>,
    pub(crate) prepare_only: bool,
    pub(crate) identity: K,
    pub(crate) grace_ms: u64,
    pub(crate) prepared_at_ms: u64,
    pub(crate) confirmed_absent_at_ms: Option<u64>,
    pub(crate) confirmation_attempts: u32,
    pub(crate) next_attempt_at_ms: u64,
    pub(crate) confirmation_in_progress: bool,
    pub(crate) deferred: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ReclaimReady<K> {
    pub(crate) next_attempt_at_ms: u64,
    pub(crate) identity: K,
}

pub(crate) struct ReclaimQueue<K> {
    pub(crate) entries: BTreeMap<K, ReclaimToken<K>>,
    pub(crate) ready: BTreeSet<ReclaimReady<K>>,
}

impl<K> Default for ReclaimQueue<K> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            ready: BTreeSet::new(),
        }
    }
}

impl<K: Clone + Ord> ReclaimQueue<K> {
    pub(crate) fn insert(&mut self, token: ReclaimToken<K>) -> bool {
        if let Some(existing) = self.entries.get_mut(&token.identity) {
            // A replay may only make a token eligible sooner.  It must not
            // reset a confirmed absence or retry history.
            if token.route_key != existing.route_key
                || token.expected_route_version != existing.expected_route_version
            {
                return false;
            }
            if !token.prepare_only {
                existing.prepare_only = false;
            }
            if !token.prepare_only {
                // A post-delete/shutdown replay carries the authoritative grace
                // policy. In particular, shutdown uses zero to force confirmation.
                existing.grace_ms = token.grace_ms;
            } else {
                existing.grace_ms = existing.grace_ms.max(token.grace_ms);
            }
            if token.prepared_at_ms < existing.prepared_at_ms {
                existing.prepared_at_ms = token.prepared_at_ms;
            }
            if token.next_attempt_at_ms < existing.next_attempt_at_ms {
                self.ready.remove(&ReclaimReady {
                    next_attempt_at_ms: existing.next_attempt_at_ms,
                    identity: token.identity.clone(),
                });
                existing.next_attempt_at_ms = token.next_attempt_at_ms;
                self.ready.insert(ReclaimReady {
                    next_attempt_at_ms: existing.next_attempt_at_ms,
                    identity: token.identity.clone(),
                });
            }
            return true;
        }
        self.ready.insert(ReclaimReady {
            next_attempt_at_ms: token.next_attempt_at_ms,
            identity: token.identity.clone(),
        });
        self.entries.insert(token.identity.clone(), token);
        true
    }

    pub(crate) fn claim_due(&mut self, now_ms: u64, limit: usize) -> Vec<ReclaimToken<K>> {
        let mut claimed = Vec::with_capacity(limit.max(1));
        while claimed.len() < limit.max(1) {
            let Some(ready) = self.ready.first().cloned() else {
                break;
            };
            if ready.next_attempt_at_ms > now_ms {
                break;
            }
            self.ready.pop_first();
            let Some(token) = self.entries.get_mut(&ready.identity) else {
                continue;
            };
            if token.next_attempt_at_ms != ready.next_attempt_at_ms
                || token.confirmation_in_progress
            {
                continue;
            }
            token.confirmation_in_progress = true;
            claimed.push(token.clone());
        }
        claimed
    }

    pub(crate) fn complete(&mut self, identity: &K) -> Option<ReclaimToken<K>> {
        let token = self.entries.remove(identity)?;
        self.ready.remove(&ReclaimReady {
            next_attempt_at_ms: token.next_attempt_at_ms,
            identity: identity.clone(),
        });
        Some(token)
    }

    pub(crate) fn defer(
        &mut self,
        identity: &K,
        now_ms: u64,
        base_delay_ms: u64,
        max_delay_ms: u64,
        warn_after_attempts: u32,
    ) -> Option<(u64, u32, bool)> {
        let previous = self.entries.get(identity)?.next_attempt_at_ms;
        self.ready.remove(&ReclaimReady {
            next_attempt_at_ms: previous,
            identity: identity.clone(),
        });
        let token = self.entries.get_mut(identity)?;
        token.confirmation_in_progress = false;
        token.confirmation_attempts = token.confirmation_attempts.saturating_add(1);
        let exponent = token.confirmation_attempts.saturating_sub(1).min(9);
        let delay = base_delay_ms
            .saturating_mul(1u64 << exponent)
            .min(max_delay_ms);
        token.next_attempt_at_ms = now_ms.saturating_add(delay);
        let became_deferred = token.confirmation_attempts >= warn_after_attempts && !token.deferred;
        token.deferred |= became_deferred;
        self.ready.insert(ReclaimReady {
            next_attempt_at_ms: token.next_attempt_at_ms,
            identity: identity.clone(),
        });
        Some((
            token.next_attempt_at_ms,
            token.confirmation_attempts,
            became_deferred,
        ))
    }

    pub(crate) fn reschedule(&mut self, identity: &K, next_attempt_at_ms: u64) -> bool {
        let Some(token) = self.entries.get_mut(identity) else {
            return false;
        };
        self.ready.remove(&ReclaimReady {
            next_attempt_at_ms: token.next_attempt_at_ms,
            identity: identity.clone(),
        });
        token.confirmation_in_progress = false;
        token.next_attempt_at_ms = next_attempt_at_ms;
        self.ready.insert(ReclaimReady {
            next_attempt_at_ms,
            identity: identity.clone(),
        });
        true
    }

    pub(crate) fn mark_absent(&mut self, identity: &K, now_ms: u64, grace_ms: u64) -> Option<u64> {
        let token = self.entries.get_mut(identity)?;
        if !token.prepare_only {
            return Some(now_ms);
        }
        let confirmed = *token.confirmed_absent_at_ms.get_or_insert(now_ms);
        Some(confirmed.saturating_add(grace_ms.max(token.grace_ms)))
    }

    pub(crate) fn clear_absent(&mut self, identity: &K) {
        if let Some(token) = self.entries.get_mut(identity) {
            token.confirmed_absent_at_ms = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(identity: u64, due: u64) -> ReclaimToken<u64> {
        ReclaimToken {
            route_key: ObjectKey::new(format!("key-{identity}")),
            expected_route_version: Some(RouteVersion(1)),
            prepare_only: false,
            identity,
            grace_ms: 0,
            prepared_at_ms: 0,
            confirmed_absent_at_ms: None,
            confirmation_attempts: 0,
            next_attempt_at_ms: due,
            confirmation_in_progress: false,
            deferred: false,
        }
    }

    #[test]
    fn queue_claims_in_deadline_order_and_replays_idempotently() {
        let mut queue = ReclaimQueue::default();
        assert!(queue.insert(token(2, 20)));
        assert!(queue.insert(token(1, 10)));
        assert!(queue.insert(token(1, 5)));
        assert_eq!(
            queue
                .claim_due(10, 8)
                .into_iter()
                .map(|t| t.identity)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert!(queue.complete(&1).is_some());
        assert_eq!(
            queue
                .claim_due(20, 8)
                .into_iter()
                .map(|t| t.identity)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn queue_backoff_is_bounded_and_marks_blocked() {
        let mut queue = ReclaimQueue::default();
        queue.insert(token(1, 0));
        queue.claim_due(0, 1);
        let (_, attempts, blocked) = queue.defer(&1, 0, 100, 250, 2).unwrap();
        assert_eq!(attempts, 1);
        assert!(!blocked);
        queue.claim_due(100, 1);
        let (next, attempts, blocked) = queue.defer(&1, 100, 100, 250, 2).unwrap();
        assert_eq!(attempts, 2);
        assert!(blocked);
        assert_eq!(next, 300);
    }
}
