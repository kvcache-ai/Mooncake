use std::cmp::Ordering;
use std::collections::BTreeMap;

use mooncake_store_core::{ClientLease, ClientLifecycleState, ObjectKey, Result, RouteCasRequest};

use crate::shim::RouteMembershipProvider;
use crate::util::{
    compatibility_matches, route_authority_lifetime, route_capable, route_weight,
    weighted_rendezvous_score, RouteAuthorityLifetime,
};

const ROUTE_SCOPE_LABEL: &str = "route_scope";

#[derive(Clone)]
pub(crate) struct RouteAuthoritySelection {
    pub(crate) authorities: Vec<ClientLease>,
}

fn authority_rank_cmp(
    left_lifetime: RouteAuthorityLifetime,
    left_score: f64,
    left_lease: &ClientLease,
    right_lifetime: RouteAuthorityLifetime,
    right_score: f64,
    right_lease: &ClientLease,
) -> Ordering {
    left_lifetime.cmp(&right_lifetime).then_with(|| {
        left_score.total_cmp(&right_score).then_with(|| {
            left_lease
                .runtime
                .stable_id
                .cmp(&right_lease.runtime.stable_id)
        })
    })
}

pub(crate) fn active_route_leases(
    membership: &dyn RouteMembershipProvider,
    force_refresh: bool,
) -> Result<Vec<ClientLease>> {
    let operation = if force_refresh {
        "live_client_snapshot_force_refresh"
    } else {
        "live_client_snapshot"
    };
    let leases = membership.live_clients(force_refresh, operation)?;
    membership.reconcile_suspects(&leases);
    Ok(leases
        .into_iter()
        .filter(|lease| lease.state == ClientLifecycleState::Active && route_capable(lease))
        .filter(|lease| !membership.is_suspect(&lease.runtime))
        .collect())
}

pub(crate) fn authority_candidates(
    membership: &dyn RouteMembershipProvider,
    observer: &ClientLease,
    force_refresh: bool,
) -> Result<Vec<ClientLease>> {
    let route_scope = observer.endpoints.labels.get(ROUTE_SCOPE_LABEL).cloned();
    Ok(deduplicate_authority_candidates(
        active_route_leases(membership, force_refresh)?
            .into_iter()
            .filter(|lease| compatibility_matches(observer, lease))
            .filter(|lease| {
                route_scope.as_ref().is_none_or(|scope| {
                    lease
                        .endpoints
                        .labels
                        .get(ROUTE_SCOPE_LABEL)
                        .is_some_and(|candidate| candidate == scope)
                })
            }),
    ))
}

pub(crate) fn retained_authority_candidates(
    membership: &dyn RouteMembershipProvider,
    observer: &ClientLease,
) -> Result<Vec<ClientLease>> {
    let route_scope = observer.endpoints.labels.get(ROUTE_SCOPE_LABEL).cloned();
    Ok(deduplicate_authority_candidates(
        membership
            .retained_live_clients("retained_live_client_snapshot")?
            .into_iter()
            .filter(|lease| {
                lease.state == ClientLifecycleState::Active
                    && route_capable(lease)
                    && compatibility_matches(observer, lease)
            })
            .filter(|lease| {
                route_scope.as_ref().is_none_or(|scope| {
                    lease
                        .endpoints
                        .labels
                        .get(ROUTE_SCOPE_LABEL)
                        .is_some_and(|candidate| candidate == scope)
                })
            }),
    ))
}

fn deduplicate_authority_candidates(
    leases: impl IntoIterator<Item = ClientLease>,
) -> Vec<ClientLease> {
    let mut candidates = BTreeMap::<String, ClientLease>::new();
    for lease in leases {
        let stable_id = lease.runtime.stable_id.0.clone();
        match candidates.get(&stable_id) {
            Some(current) if current.runtime.epoch >= lease.runtime.epoch => {}
            _ => {
                candidates.insert(stable_id, lease);
            }
        }
    }
    candidates.into_values().collect()
}

pub(crate) fn merge_authority_candidates(
    current: &[ClientLease],
    retained: &[ClientLease],
) -> Vec<ClientLease> {
    deduplicate_authority_candidates(current.iter().chain(retained).cloned())
}

pub(crate) fn ranked_authorities(
    namespace: &str,
    candidates: &[ClientLease],
    key: &ObjectKey,
) -> Vec<ClientLease> {
    let mut ranked = candidates
        .iter()
        .map(|lease| {
            (
                route_authority_lifetime(lease),
                weighted_rendezvous_score(
                    namespace,
                    &key.0,
                    &lease.runtime.stable_id.0,
                    route_weight(lease),
                ),
                lease.clone(),
            )
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        authority_rank_cmp(left.0, left.1, &left.2, right.0, right.1, &right.2)
    });
    ranked.into_iter().map(|(_, _, lease)| lease).collect()
}

pub(crate) fn ranked_top_authorities(
    namespace: &str,
    candidates: &[ClientLease],
    key: &ObjectKey,
    limit: usize,
) -> Vec<ClientLease> {
    let mut top = Vec::<(RouteAuthorityLifetime, f64, ClientLease)>::with_capacity(
        limit.min(candidates.len()),
    );
    for lease in candidates {
        let lifetime = route_authority_lifetime(lease);
        let score = weighted_rendezvous_score(
            namespace,
            &key.0,
            &lease.runtime.stable_id.0,
            route_weight(lease),
        );
        let insert_at = top.partition_point(|(current_lifetime, current_score, current_lease)| {
            authority_rank_cmp(
                *current_lifetime,
                *current_score,
                current_lease,
                lifetime,
                score,
                lease,
            )
            .is_lt()
        });
        if insert_at < limit {
            top.insert(insert_at, (lifetime, score, lease.clone()));
            top.truncate(limit);
        }
    }
    top.into_iter().map(|(_, _, lease)| lease).collect()
}

pub(crate) fn ranked_merged_authorities(
    namespace: &str,
    current: &[ClientLease],
    retained: &[ClientLease],
    key: &ObjectKey,
    limit: usize,
) -> Vec<ClientLease> {
    ranked_top_authorities(
        namespace,
        &merge_authority_candidates(current, retained),
        key,
        limit,
    )
}

pub(crate) fn select_authorities_for_requests(
    namespace: &str,
    candidates: &[ClientLease],
    requests: &[RouteCasRequest],
    route_topk: usize,
) -> Vec<RouteAuthoritySelection> {
    requests
        .iter()
        .map(|request| {
            let ranked = ranked_top_authorities(namespace, candidates, &request.key, route_topk);
            RouteAuthoritySelection {
                authorities: ranked,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientRuntimeId, CompatibilityDescriptor,
    };
    use std::collections::BTreeMap;
    use std::time::Instant;

    struct TestMembership {
        leases: Vec<ClientLease>,
        suspects: Vec<ClientRuntimeId>,
    }

    impl RouteMembershipProvider for TestMembership {
        fn live_clients(
            &self,
            _force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            Ok(self.leases.clone())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn is_suspect(&self, runtime: &ClientRuntimeId) -> bool {
            self.suspects.contains(runtime)
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    fn make_lease(id: &str) -> ClientLease {
        make_lease_with_labels(id, &[("route", "true")])
    }

    fn make_ephemeral_lease(id: &str) -> ClientLease {
        make_lease_with_labels(id, &[("route", "true"), ("route_ephemeral", "true")])
    }

    fn make_lease_with_labels(id: &str, labels: &[(&str, &str)]) -> ClientLease {
        let mut endpoint_labels = BTreeMap::new();
        for (k, v) in labels {
            endpoint_labels.insert(k.to_string(), v.to_string());
        }
        ClientLease {
            runtime: ClientRuntimeId::new(id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            expires_at_ms: u64::MAX,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            endpoints: ClientEndpointSet {
                labels: endpoint_labels,
                ..ClientEndpointSet::default()
            },
        }
    }

    fn make_lease_epoch(id: &str, epoch: u64) -> ClientLease {
        let mut lease = make_lease(id);
        lease.runtime.epoch = ClientEpoch(epoch);
        lease
    }

    #[test]
    fn ranked_authorities_stable_ordering() {
        let candidates = vec![make_lease("a"), make_lease("b"), make_lease("c")];
        let key = ObjectKey::new("test-key".to_string());
        let first = ranked_authorities("ns", &candidates, &key);
        let second = ranked_authorities("ns", &candidates, &key);
        assert_eq!(
            first
                .iter()
                .map(|l| &l.runtime.stable_id)
                .collect::<Vec<_>>(),
            second
                .iter()
                .map(|l| &l.runtime.stable_id)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn ranked_top_truncates_to_limit() {
        let candidates: Vec<_> = (0..5).map(|i| make_lease(&format!("node-{i}"))).collect();
        let key = ObjectKey::new("key".to_string());
        let top = ranked_top_authorities("ns", &candidates, &key, 2);
        assert_eq!(top.len(), 2);
    }

    #[test]
    fn merged_candidates_keep_the_highest_epoch_per_stable_id() {
        let current = vec![make_lease_epoch("shared", 1), make_lease("current")];
        let retained = vec![make_lease_epoch("shared", 3), make_lease("retained")];

        let merged = merge_authority_candidates(&current, &retained);

        assert_eq!(merged.len(), 3);
        assert_eq!(
            merged
                .iter()
                .find(|lease| lease.runtime.stable_id.0 == "shared")
                .expect("shared authority should remain")
                .runtime
                .epoch,
            ClientEpoch(3)
        );
    }

    #[test]
    fn merged_authorities_are_ranked_once_before_truncation() {
        let current = vec![make_lease("current-a"), make_lease("current-b")];
        let retained = vec![make_lease("retained-a"), make_lease("retained-b")];
        let key = ObjectKey::new("merged-ranking-key".to_string());

        let actual = ranked_merged_authorities("ns", &current, &retained, &key, 2);
        let expected = ranked_top_authorities(
            "ns",
            &merge_authority_candidates(&current, &retained),
            &key,
            2,
        );

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2);
    }

    #[test]
    fn candidates_filter_inactive_leases() {
        let mut inactive = make_lease("inactive");
        inactive.state = ClientLifecycleState::Draining;
        let active = make_lease("active");
        let membership = TestMembership {
            leases: vec![inactive, active.clone()],
            suspects: Vec::new(),
        };
        let observer = make_lease("observer");
        let result = authority_candidates(&membership, &observer, false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].runtime.stable_id.0, "active");
    }

    #[test]
    fn candidates_filter_suspect_authorities() {
        let suspect = make_lease("suspect-node");
        let healthy = make_lease("healthy-node");
        let membership = TestMembership {
            leases: vec![suspect.clone(), healthy.clone()],
            suspects: vec![suspect.runtime.clone()],
        };
        let observer = make_lease("observer");
        let result = authority_candidates(&membership, &observer, false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].runtime.stable_id.0, "healthy-node");
    }

    #[test]
    fn candidates_filter_non_route_capable() {
        let no_route = make_lease_with_labels("no-route", &[]);
        let with_route = make_lease("with-route");
        let membership = TestMembership {
            leases: vec![no_route, with_route],
            suspects: Vec::new(),
        };
        let observer = make_lease("observer");
        let result = authority_candidates(&membership, &observer, false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].runtime.stable_id.0, "with-route");
    }

    #[test]
    fn candidates_filter_route_scope_mismatch() {
        let scope_a =
            make_lease_with_labels("scope-a", &[("route", "true"), ("route_scope", "gpu")]);
        let scope_b =
            make_lease_with_labels("scope-b", &[("route", "true"), ("route_scope", "cpu")]);
        let no_scope = make_lease("no-scope");
        let membership = TestMembership {
            leases: vec![scope_a, scope_b, no_scope],
            suspects: Vec::new(),
        };
        let observer =
            make_lease_with_labels("observer", &[("route", "true"), ("route_scope", "gpu")]);
        let result = authority_candidates(&membership, &observer, false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].runtime.stable_id.0, "scope-a");
    }

    #[test]
    fn candidates_deduplicate_by_stable_id_keeps_highest_epoch() {
        let old = make_lease_epoch("node-x", 1);
        let new = make_lease_epoch("node-x", 5);
        let membership = TestMembership {
            leases: vec![old, new],
            suspects: Vec::new(),
        };
        let observer = make_lease("observer");
        let result = authority_candidates(&membership, &observer, false).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].runtime.epoch, ClientEpoch(5));
    }

    #[test]
    fn weight_affects_ranking() {
        let heavy = make_lease_with_labels("heavy", &[("route", "true"), ("route_weight", "10.0")]);
        let light = make_lease_with_labels("light", &[("route", "true"), ("route_weight", "0.1")]);
        let key = ObjectKey::new("test-key".to_string());
        let ranked = ranked_authorities("ns", &[heavy.clone(), light.clone()], &key);
        assert_eq!(ranked[0].runtime.stable_id.0, "heavy");
    }

    #[test]
    fn durable_authorities_rank_before_ephemeral_authorities() {
        let durable = make_lease_with_labels(
            "durable",
            &[
                ("route", "true"),
                ("route_ephemeral", "false"),
                ("route_weight", "0.001"),
            ],
        );
        let ephemeral = make_lease_with_labels(
            "ephemeral",
            &[
                ("route", "true"),
                ("route_ephemeral", "true"),
                ("route_weight", "1000"),
            ],
        );
        let key = ObjectKey::new("test-key".to_string());

        let ranked = ranked_authorities("ns", &[ephemeral, durable], &key);

        assert_eq!(ranked[0].runtime.stable_id.0, "durable");
    }

    #[test]
    fn explicit_durable_then_legacy_then_ephemeral_for_rolling_upgrade() {
        let durable = make_lease_with_labels(
            "durable",
            &[("route", "true"), ("route_ephemeral", "false")],
        );
        let legacy = make_lease("legacy");
        let ephemeral = make_ephemeral_lease("ephemeral");
        let key = ObjectKey::new("test-key".to_string());

        let ranked = ranked_authorities("ns", &[ephemeral, legacy, durable], &key);

        assert_eq!(
            ranked
                .iter()
                .map(|lease| lease.runtime.stable_id.0.as_str())
                .collect::<Vec<_>>(),
            vec!["durable", "legacy", "ephemeral"]
        );
    }

    #[test]
    fn ephemeral_authorities_fill_topk_after_durable_authorities() {
        let durable = make_lease_with_labels(
            "durable",
            &[("route", "true"), ("route_ephemeral", "false")],
        );
        let ephemeral_a = make_ephemeral_lease("ephemeral-a");
        let ephemeral_b = make_ephemeral_lease("ephemeral-b");
        let key = ObjectKey::new("test-key".to_string());

        let top = ranked_top_authorities("ns", &[ephemeral_a, durable, ephemeral_b], &key, 2);

        assert_eq!(top.len(), 2);
        assert_eq!(top[0].runtime.stable_id.0, "durable");
        assert!(top[1]
            .endpoints
            .labels
            .get("route_ephemeral")
            .is_some_and(|value| value == "true"));
    }

    #[test]
    fn ephemeral_authorities_remain_eligible_without_durable_authorities() {
        let ephemeral_a = make_ephemeral_lease("ephemeral-a");
        let ephemeral_b = make_ephemeral_lease("ephemeral-b");
        let key = ObjectKey::new("test-key".to_string());

        let top = ranked_top_authorities("ns", &[ephemeral_a, ephemeral_b], &key, 2);

        assert_eq!(top.len(), 2);
        assert!(top
            .iter()
            .all(|lease| route_authority_lifetime(lease) == RouteAuthorityLifetime::Ephemeral));
    }

    #[test]
    fn select_authorities_for_requests_matches_topk() {
        let candidates: Vec<_> = (0..5).map(|i| make_lease(&format!("n-{i}"))).collect();
        let requests = vec![
            RouteCasRequest {
                key: ObjectKey::new("key-a".to_string()),
                expected: None,
                next: None,
            },
            RouteCasRequest {
                key: ObjectKey::new("key-b".to_string()),
                expected: None,
                next: None,
            },
        ];
        let selections = select_authorities_for_requests("ns", &candidates, &requests, 3);
        assert_eq!(selections.len(), 2);
        assert_eq!(selections[0].authorities.len(), 3);
        assert_eq!(selections[1].authorities.len(), 3);
    }
}
