use std::collections::BTreeMap;

use mooncake_store_core::{ClientLease, ObjectKey, ObjectRoute, RouteCasRequest};
use tracing::warn;

use crate::mesh::selection::RouteAuthoritySelection;
use crate::metrics::record_route_repair_metric;
use crate::util::canonical_route_key;

const ROUTE_REPAIR_MISSING_AUTHORITY: &str = "route_repair_missing_authority";
const ROUTE_REPAIR_STALE_AUTHORITY: &str = "route_repair_stale_authority";
const ROUTE_REPAIR_DIVERGENT_AUTHORITY: &str = "route_repair_divergent_authority";

pub(crate) type RouteReadRepairState = Vec<RouteAuthorityObservation>;

#[derive(Clone, Default)]
pub(crate) enum RouteAuthorityObservation {
    #[default]
    Unobserved,
    Missing,
    Present(Box<ObjectRoute>),
}

pub(crate) fn set_repair_observation(
    state: &mut RouteReadRepairState,
    rank: usize,
    observation: RouteAuthorityObservation,
) {
    if let Some(slot) = state.get_mut(rank) {
        *slot = observation;
    }
}

pub(crate) fn repair_request(
    key: &ObjectKey,
    best: &ObjectRoute,
    observed: &RouteAuthorityObservation,
) -> Option<RouteCasRequest> {
    match observed {
        RouteAuthorityObservation::Missing => {
            record_route_repair_metric(ROUTE_REPAIR_MISSING_AUTHORITY);
            Some(RouteCasRequest {
                key: key.clone(),
                expected: None,
                next: Some(best.clone()),
            })
        }
        RouteAuthorityObservation::Present(current) if current.version < best.version => {
            record_route_repair_metric(ROUTE_REPAIR_STALE_AUTHORITY);
            Some(RouteCasRequest {
                key: key.clone(),
                expected: Some(current.version),
                next: Some(best.clone()),
            })
        }
        RouteAuthorityObservation::Present(current)
            if current.version == best.version && current.as_ref() != best =>
        {
            record_route_repair_metric(ROUTE_REPAIR_DIVERGENT_AUTHORITY);
            Some(RouteCasRequest {
                key: key.clone(),
                expected: Some(current.version),
                next: Some(best.clone()),
            })
        }
        _ => None,
    }
}

pub(crate) fn merge_fresher_route(
    current: &mut Option<ObjectRoute>,
    candidate: ObjectRoute,
    authority: &str,
    key: &ObjectKey,
) {
    match current {
        None => *current = Some(candidate),
        Some(existing) if candidate.version > existing.version => *current = Some(candidate),
        Some(existing) if candidate.version == existing.version && *existing != candidate => {
            let choose_candidate = canonical_route_key(&candidate) < canonical_route_key(existing);
            let version = candidate.version.0;
            warn!(
                key = %key.0,
                authority = %authority,
                version,
                canonical = if choose_candidate { "candidate" } else { "existing" },
                "route authorities disagree on the same route version; choosing canonical route"
            );
            if choose_candidate {
                *existing = candidate;
            }
        }
        Some(_) => {}
    }
}

pub(crate) fn merge_route_listing(
    routes: &mut BTreeMap<String, ObjectRoute>,
    candidate: ObjectRoute,
    authority: &str,
) {
    match routes.get(&candidate.key.0) {
        None => {
            routes.insert(candidate.key.0.clone(), candidate);
        }
        Some(current) if candidate.version > current.version => {
            routes.insert(candidate.key.0.clone(), candidate);
        }
        Some(current) if candidate.version == current.version && *current != candidate => {
            let choose_candidate = canonical_route_key(&candidate) < canonical_route_key(current);
            let key_str = candidate.key.0.clone();
            let version = candidate.version.0;
            warn!(
                key = %key_str,
                authority,
                version,
                canonical = if choose_candidate { "candidate" } else { "existing" },
                "route authorities disagree on the same route version; choosing canonical route"
            );
            if choose_candidate {
                routes.insert(key_str, candidate);
            }
        }
        Some(_) => {}
    }
}

pub(crate) fn compute_backfill_requests(
    keys: &[ObjectKey],
    selections: &[RouteAuthoritySelection],
    resolved: &[Option<ObjectRoute>],
    repairs: &[RouteReadRepairState],
) -> BTreeMap<String, (ClientLease, Vec<RouteCasRequest>)> {
    debug_assert_eq!(keys.len(), resolved.len());
    debug_assert_eq!(keys.len(), selections.len());
    debug_assert_eq!(keys.len(), repairs.len());
    let mut backfills = BTreeMap::<String, (ClientLease, Vec<RouteCasRequest>)>::new();
    for (index, route) in resolved.iter().enumerate() {
        let Some(route) = route.as_ref() else {
            continue;
        };
        for (rank, authority) in selections[index].authorities.iter().cloned().enumerate() {
            if let Some(request) = repairs[index]
                .get(rank)
                .and_then(|observed| repair_request(&keys[index], route, observed))
            {
                backfills
                    .entry(authority.runtime.stable_id.0.clone())
                    .or_insert_with(|| (authority, Vec::new()))
                    .1
                    .push(request);
            }
        }
    }
    backfills
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor, ReplicaRoute, ReplicaTier, RouteState, RouteVersion, SegmentName,
    };

    fn test_lease(id: &str) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            expires_at_ms: u64::MAX,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            endpoints: ClientEndpointSet::default(),
        }
    }

    fn test_route(key: &str, version: u64) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key.to_string()),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(version),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("owner", ClientEpoch(1)),
                segment_name: SegmentName::new("seg".to_string()),
                segment_offset: 0,
                length: 100,
                checksum: Some(42),
                offset: Some(0),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        }
    }

    fn test_route_different_replica(key: &str, version: u64) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key.to_string()),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(version),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("other-owner", ClientEpoch(1)),
                segment_name: SegmentName::new("other-seg".to_string()),
                segment_offset: 0,
                length: 200,
                checksum: Some(99),
                offset: Some(0),
                tier: ReplicaTier::Nvme,
                priority: 1,
            }],
        }
    }

    #[test]
    fn repair_request_missing_generates_insert() {
        let best = test_route("key-a", 3);
        let result = repair_request(
            &ObjectKey::new("key-a".to_string()),
            &best,
            &RouteAuthorityObservation::Missing,
        );
        let req = result.expect("should generate repair");
        assert_eq!(req.expected, None);
        assert_eq!(req.next.as_ref().unwrap().version, RouteVersion(3));
    }

    #[test]
    fn repair_request_stale_generates_update() {
        let stale = test_route("key-a", 1);
        let best = test_route("key-a", 3);
        let result = repair_request(
            &ObjectKey::new("key-a".to_string()),
            &best,
            &RouteAuthorityObservation::Present(Box::new(stale)),
        );
        let req = result.expect("should generate repair");
        assert_eq!(req.expected, Some(RouteVersion(1)));
        assert_eq!(req.next.as_ref().unwrap().version, RouteVersion(3));
    }

    #[test]
    fn repair_request_divergent_same_version() {
        let divergent = test_route_different_replica("key-a", 3);
        let best = test_route("key-a", 3);
        let result = repair_request(
            &ObjectKey::new("key-a".to_string()),
            &best,
            &RouteAuthorityObservation::Present(Box::new(divergent)),
        );
        let req = result.expect("should generate repair for divergent");
        assert_eq!(req.expected, Some(RouteVersion(3)));
    }

    #[test]
    fn repair_request_up_to_date_returns_none() {
        let best = test_route("key-a", 3);
        let result = repair_request(
            &ObjectKey::new("key-a".to_string()),
            &best,
            &RouteAuthorityObservation::Present(Box::new(best.clone())),
        );
        assert!(result.is_none());
    }

    #[test]
    fn merge_fresher_route_higher_version_wins() {
        let mut current = Some(test_route("key", 1));
        let candidate = test_route("key", 5);
        merge_fresher_route(
            &mut current,
            candidate.clone(),
            "auth-a",
            &ObjectKey::new("key".to_string()),
        );
        assert_eq!(current.unwrap().version, RouteVersion(5));
    }

    #[test]
    fn merge_fresher_route_lower_version_loses() {
        let mut current = Some(test_route("key", 5));
        let candidate = test_route("key", 2);
        merge_fresher_route(
            &mut current,
            candidate,
            "auth-a",
            &ObjectKey::new("key".to_string()),
        );
        assert_eq!(current.unwrap().version, RouteVersion(5));
    }

    #[test]
    fn merge_fresher_route_none_gets_filled() {
        let mut current: Option<ObjectRoute> = None;
        let candidate = test_route("key", 1);
        merge_fresher_route(
            &mut current,
            candidate.clone(),
            "auth-a",
            &ObjectKey::new("key".to_string()),
        );
        assert_eq!(current.unwrap().version, RouteVersion(1));
    }

    #[test]
    fn merge_route_listing_dedup_by_key() {
        let mut routes = BTreeMap::new();
        let old = test_route("key-x", 1);
        let new = test_route("key-x", 3);
        merge_route_listing(&mut routes, old, "auth-a");
        merge_route_listing(&mut routes, new, "auth-b");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes["key-x"].version, RouteVersion(3));
    }

    #[test]
    fn compute_backfill_groups_by_authority() {
        let auth_a = test_lease("auth-a");
        let auth_b = test_lease("auth-b");
        let keys = vec![
            ObjectKey::new("k1".to_string()),
            ObjectKey::new("k2".to_string()),
        ];
        let selections = vec![
            RouteAuthoritySelection {
                authorities: vec![auth_a.clone(), auth_b.clone()],
            },
            RouteAuthoritySelection {
                authorities: vec![auth_a.clone(), auth_b.clone()],
            },
        ];
        let resolved = vec![Some(test_route("k1", 3)), Some(test_route("k2", 2))];
        let repairs = vec![
            vec![
                RouteAuthorityObservation::Present(Box::new(test_route("k1", 3))),
                RouteAuthorityObservation::Missing,
            ],
            vec![
                RouteAuthorityObservation::Present(Box::new(test_route("k2", 2))),
                RouteAuthorityObservation::Missing,
            ],
        ];
        let backfills = compute_backfill_requests(&keys, &selections, &resolved, &repairs);
        assert_eq!(backfills.len(), 1);
        let (_, (lease, reqs)) = backfills.into_iter().next().unwrap();
        assert_eq!(lease.runtime.stable_id.0, "auth-b");
        assert_eq!(reqs.len(), 2);
    }
}
