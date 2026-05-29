use mooncake_store_core::{ClientLease, ClientLifecycleState, ObjectKey, Result, RouteCasRequest};

use crate::shim::RouteMembershipProvider;
use crate::util::{compatibility_matches, route_capable, route_weight, weighted_rendezvous_score};

const ROUTE_SCOPE_LABEL: &str = "route_scope";

#[derive(Clone)]
pub(crate) struct RouteAuthoritySelection {
    pub(crate) authorities: Vec<ClientLease>,
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
    use std::collections::BTreeMap;

    let route_scope = observer.endpoints.labels.get(ROUTE_SCOPE_LABEL).cloned();
    let mut candidates = BTreeMap::<String, ClientLease>::new();
    for lease in active_route_leases(membership, force_refresh)? {
        if !compatibility_matches(observer, &lease) {
            continue;
        }
        if route_scope.as_ref().is_some_and(|scope| {
            lease
                .endpoints
                .labels
                .get(ROUTE_SCOPE_LABEL)
                .is_none_or(|candidate| candidate != scope)
        }) {
            continue;
        }
        let stable_id = lease.runtime.stable_id.0.clone();
        match candidates.get(&stable_id) {
            Some(current) if current.runtime.epoch >= lease.runtime.epoch => {}
            _ => {
                candidates.insert(stable_id, lease);
            }
        }
    }
    Ok(candidates.into_values().collect())
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
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.runtime.stable_id.cmp(&right.1.runtime.stable_id))
    });
    ranked.into_iter().map(|(_, lease)| lease).collect()
}

pub(crate) fn ranked_top_authorities(
    namespace: &str,
    candidates: &[ClientLease],
    key: &ObjectKey,
    limit: usize,
) -> Vec<ClientLease> {
    let mut top = Vec::<(f64, ClientLease)>::with_capacity(limit.min(candidates.len()));
    for lease in candidates {
        let score = weighted_rendezvous_score(
            namespace,
            &key.0,
            &lease.runtime.stable_id.0,
            route_weight(lease),
        );
        let insert_at = top.partition_point(|(current_score, current_lease)| {
            current_score
                .total_cmp(&score)
                .then_with(|| {
                    current_lease
                        .runtime
                        .stable_id
                        .cmp(&lease.runtime.stable_id)
                })
                .is_lt()
        });
        if insert_at < limit {
            top.insert(insert_at, (score, lease.clone()));
            top.truncate(limit);
        }
    }
    top.into_iter().map(|(_, lease)| lease).collect()
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
