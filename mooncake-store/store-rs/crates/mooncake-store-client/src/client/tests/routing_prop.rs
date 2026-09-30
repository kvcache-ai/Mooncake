use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    CasResult, ClientEpoch, ClientRuntimeId, CompatibilityDescriptor, MetadataBackend, ObjectKey,
    ObjectRoute, ReplicaRoute, ReplicaTier, RouteControlMode, RoutePolicy, RoutePolicyDomain,
    RouteState, RouteVersion, SegmentName, StoreError,
};
use mooncake_store_test_utils::metadata::{CountingMetadataBackend, FaultyMetadataBackend};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_object_key(ns: &str, key: &str) -> ObjectKey {
    ObjectKey::new(format!("{ns}::{key}"))
}

fn make_creator() -> ClientRuntimeId {
    ClientRuntimeId::new("test-creator", ClientEpoch(1))
}

fn make_policy() -> RoutePolicy {
    RoutePolicy {
        route_topk: 2,
        route_control: RouteControlMode::MetadataOnly,
        created_by: make_creator(),
        created_at_ms: 0,
    }
}

fn make_route(key: &ObjectKey, segment: &str) -> ObjectRoute {
    ObjectRoute {
        key: key.clone(),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: None,
        version: RouteVersion(1),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::mooncake_v1(),
        replicas: vec![ReplicaRoute {
            segment_name: SegmentName::new(segment),
            owner: make_creator(),
            offset: Some(0),
            segment_offset: 0,
            length: 64,
            checksum: None,
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
        cold_backing: None,
        nof_backing: None,
    }
}

// ---------------------------------------------------------------------------
// Property: object route CAS semantics
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn prop_get_route_returns_none_for_absent_key(key in "[a-z]{4,12}") {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let obj_key = make_object_key("ns", &key);
        let result = meta.get_object_route(&obj_key).expect("get should not error");
        prop_assert!(result.is_none(), "absent key must yield None");
    }

    #[test]
    fn prop_cas_insert_then_get_round_trip(key in "[a-z]{4,12}", segment in "[a-z]{4,8}") {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let obj_key = make_object_key("ns", &key);
        let route = make_route(&obj_key, &segment);

        let result = meta
            .compare_and_swap_object_route(&obj_key, None, Some(&route))
            .expect("CAS insert should not error");
        prop_assert!(result.applied, "first CAS insert must be applied");

        let fetched = meta.get_object_route(&obj_key).expect("get").expect("must exist");
        prop_assert_eq!(fetched.version, route.version, "version must match");
    }

    #[test]
    fn prop_cas_stale_version_is_rejected(key in "[a-z]{4,12}") {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let obj_key = make_object_key("ns", &key);
        let route = make_route(&obj_key, "seg");

        meta.compare_and_swap_object_route(&obj_key, None, Some(&route))
            .expect("insert");

        let wrong = RouteVersion(999);
        let result = meta
            .compare_and_swap_object_route(&obj_key, Some(wrong), Some(&route))
            .expect("stale CAS should not error");
        prop_assert!(!result.applied, "stale CAS must not be applied");
    }

    #[test]
    fn prop_route_policy_put_if_absent_is_idempotent(tenant in "[a-z]{4,10}") {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let domain = RoutePolicyDomain::Tenant(tenant.clone());
        let policy = make_policy();

        let first = meta
            .put_route_policy_if_absent(&domain, &policy)
            .expect("first put_if_absent");
        prop_assert!(first, "first put_if_absent must succeed");

        let second = meta
            .put_route_policy_if_absent(&domain, &policy)
            .expect("second put_if_absent");
        prop_assert!(!second, "second put_if_absent must be rejected (already present)");
    }

    #[test]
    fn prop_counting_backend_get_object_route_counter(n in 1u64..=15u64) {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(InMemoryMetadataBackend::new());
        let (backend, counts) = CountingMetadataBackend::wrap(inner);

        let key = make_object_key("ns", "counted-key");
        for i in 1..=n {
            let _ = backend.get_object_route(&key);
            prop_assert_eq!(
                counts.get_object_route.load(Ordering::Relaxed),
                i
            );
        }
        prop_assert_eq!(counts.compare_and_swap_object_route.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn prop_faulty_backend_cas_fails_after_threshold(fail_after in 0u64..=5u64) {
        let inner = Arc::new(InMemoryMetadataBackend::new());
        let faulty = FaultyMetadataBackend::wrap(inner, fail_after);
        let obj_key = make_object_key("ns", "faulty-key");
        let route = make_route(&obj_key, "seg");

        let mut success = 0u64;
        let mut failures = 0u64;
        for _ in 0..=fail_after + 2 {
            match faulty.compare_and_swap_object_route(&obj_key, None, Some(&route)) {
                Ok(_) => success += 1,
                Err(StoreError::Metadata(_)) => failures += 1,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        prop_assert_eq!(success, fail_after);
        prop_assert_eq!(failures, 3);
    }
}

// ---------------------------------------------------------------------------
// Deterministic route policy scenario tests
// ---------------------------------------------------------------------------

#[test]
fn route_policy_get_absent_returns_none() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let result = meta
        .get_route_policy(&RoutePolicyDomain::Tenant("absent".to_string()))
        .expect("get should not error");
    assert!(result.is_none());
}

#[test]
fn route_policy_put_and_get_round_trip() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let domain = RoutePolicyDomain::Tenant("ns-put-get".to_string());
    let policy = make_policy();
    meta.put_route_policy(&domain, &policy).expect("put");

    let fetched = meta
        .get_route_policy(&domain)
        .expect("get")
        .expect("must exist");
    assert_eq!(fetched, policy);
}

#[test]
fn route_policy_default_domain_put_and_get() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let policy = make_policy();
    meta.put_route_policy(&RoutePolicyDomain::Default, &policy)
        .expect("put");
    let fetched = meta
        .get_route_policy(&RoutePolicyDomain::Default)
        .expect("get")
        .expect("must exist after put");
    assert_eq!(fetched, policy);
}

#[test]
fn route_policy_delete_existing_returns_true() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let domain = RoutePolicyDomain::Tenant("ns-delete".to_string());
    meta.put_route_policy(&domain, &make_policy()).expect("put");
    let deleted = meta.delete_route_policy(&domain).expect("delete");
    assert!(deleted);
    assert!(meta.get_route_policy(&domain).expect("get").is_none());
}

#[test]
fn route_policy_delete_absent_returns_false() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let domain = RoutePolicyDomain::Tenant("ns-absent".to_string());
    let deleted = meta.delete_route_policy(&domain).expect("delete absent");
    assert!(!deleted);
}

#[test]
fn route_policy_list_returns_all() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let domains = [
        RoutePolicyDomain::Tenant("ns-a".to_string()),
        RoutePolicyDomain::Tenant("ns-b".to_string()),
        RoutePolicyDomain::Default,
    ];
    for domain in &domains {
        meta.put_route_policy(domain, &make_policy()).expect("put");
    }
    let all = meta.list_route_policies().expect("list");
    assert_eq!(all.len(), domains.len());
}

#[test]
fn object_route_cas_delete_existing() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let obj_key = make_object_key("ns", "deletable-key");
    let route = make_route(&obj_key, "seg");

    meta.compare_and_swap_object_route(&obj_key, None, Some(&route))
        .expect("insert");
    let fetched = meta
        .get_object_route(&obj_key)
        .expect("get")
        .expect("must exist");

    let result = meta
        .compare_and_swap_object_route(&obj_key, Some(fetched.version), None)
        .expect("delete CAS");
    assert!(result.applied, "delete CAS must apply");
    assert!(
        meta.get_object_route(&obj_key)
            .expect("get after delete")
            .is_none(),
        "route must not exist after delete"
    );
}

#[test]
fn object_routes_listed_after_insert() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let keys = ["key-x", "key-y", "key-z"];
    for k in &keys {
        let obj_key = make_object_key("ns", k);
        let route = make_route(&obj_key, "seg");
        meta.compare_and_swap_object_route(&obj_key, None, Some(&route))
            .expect("insert");
    }
    let all = meta.list_object_routes().expect("list");
    assert_eq!(all.len(), keys.len());
}

#[test]
fn counting_backend_cas_route_increments_counter() {
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let obj_key = make_object_key("ns", "counted-cas");
    let route = make_route(&obj_key, "seg");

    backend
        .compare_and_swap_object_route(&obj_key, None, Some(&route))
        .expect("insert");
    backend
        .compare_and_swap_object_route(&obj_key, None, Some(&route))
        .expect("second cas (conflict)");

    assert_eq!(
        counts.compare_and_swap_object_route.load(Ordering::Relaxed),
        2
    );
}

#[test]
fn counting_backend_route_policy_operations_tracked() {
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);
    let domain = RoutePolicyDomain::Tenant("tracked-ns".to_string());

    backend
        .put_route_policy(&domain, &make_policy())
        .expect("put");
    let _ = backend.get_route_policy(&domain);
    let _ = backend.list_route_policies();
    backend.delete_route_policy(&domain).expect("delete");

    assert_eq!(counts.put_route_policy.load(Ordering::Relaxed), 1);
    assert_eq!(counts.get_route_policy.load(Ordering::Relaxed), 1);
    assert_eq!(counts.list_route_policies.load(Ordering::Relaxed), 1);
    assert_eq!(counts.delete_route_policy.load(Ordering::Relaxed), 1);
}

#[test]
fn faulty_backend_get_object_route_fails_at_threshold() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 1);

    let key = make_object_key("ns", "faulty-get");
    faulty.get_object_route(&key).expect("first call ok");
    let err = faulty
        .get_object_route(&key)
        .expect_err("second call must fail");
    assert!(matches!(err, StoreError::Metadata(_)));
}

#[test]
fn faulty_backend_put_route_policy_fails_at_threshold() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 0);
    let err = faulty
        .put_route_policy(&RoutePolicyDomain::Default, &make_policy())
        .expect_err("must fail immediately");
    assert!(matches!(err, StoreError::Metadata(_)));
}

#[test]
fn counting_backend_put_if_absent_counter() {
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);
    let domain = RoutePolicyDomain::Tenant("if-absent-ns".to_string());

    backend
        .put_route_policy_if_absent(&domain, &make_policy())
        .expect("first");
    backend
        .put_route_policy_if_absent(&domain, &make_policy())
        .expect("second (conflict)");

    assert_eq!(counts.put_route_policy_if_absent.load(Ordering::Relaxed), 2);
}

#[test]
fn cas_result_exposes_current_route_on_conflict() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let obj_key = make_object_key("ns", "cas-current");
    let route = make_route(&obj_key, "seg");

    meta.compare_and_swap_object_route(&obj_key, None, Some(&route))
        .expect("insert");

    let CasResult {
        applied, current, ..
    } = meta
        .compare_and_swap_object_route(&obj_key, None, Some(&route))
        .expect("conflict CAS");
    assert!(!applied, "duplicate insert must conflict");
    assert!(current.is_some(), "conflict must include current value");
}
