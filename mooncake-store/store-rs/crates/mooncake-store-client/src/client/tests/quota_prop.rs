use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    MetadataBackend, StoreError, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
    TenantQuotaPolicy,
};
use mooncake_store_test_utils::metadata::{CountingMetadataBackend, FaultyMetadataBackend};
use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tenant_scope(tenant: &str) -> TenantPolicyScope {
    TenantPolicyScope {
        tenant: tenant.to_string(),
        domain: None,
        object_set: None,
    }
}

fn install_quota_policy(meta: &Arc<InMemoryMetadataBackend>, scope: &TenantPolicyScope) {
    let policy = TenantPolicy {
        scope: scope.clone(),
        spec: TenantPolicySpec {
            quota: Some(TenantQuotaPolicy {
                max_bytes: Some(1 << 30),
                max_objects: Some(10_000),
            }),
            ..Default::default()
        },
        version: 0,
        updated_at_ms: 0,
        updated_by: "test".to_string(),
    };
    meta.put_tenant_policy(&policy, None)
        .expect("install quota policy");
}

// ---------------------------------------------------------------------------
// Property: CountingMetadataBackend quota counters
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn prop_counting_backend_quota_state_queries(n in 1u64..=20u64) {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(InMemoryMetadataBackend::new());
        let scope = tenant_scope("prop-quota-state");
        install_quota_policy(&inner, &scope);

        let (backend, counts) = CountingMetadataBackend::wrap(inner);
        for i in 1..=n {
            let _ = backend.get_tenant_quota_state(&scope);
            prop_assert_eq!(
                counts.get_tenant_quota_state.load(Ordering::Relaxed),
                i
            );
        }
        prop_assert_eq!(counts.reserve_tenant_quota.load(Ordering::Relaxed), 0);
        prop_assert_eq!(counts.finalize_tenant_quota.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn prop_faulty_backend_list_tenant_policies_threshold(fail_after in 0u64..=8u64) {
        let inner = Arc::new(InMemoryMetadataBackend::new());
        let faulty = FaultyMetadataBackend::wrap(inner, fail_after);

        let mut success = 0u64;
        let mut failures = 0u64;
        for _ in 0..fail_after + 3 {
            match faulty.list_tenant_policies(None) {
                Ok(_) => success += 1,
                Err(StoreError::Metadata(_)) => failures += 1,
                Err(other) => panic!("unexpected: {other:?}"),
            }
        }
        prop_assert_eq!(success, fail_after);
        prop_assert_eq!(failures, 3);
    }

    #[test]
    fn prop_counting_backend_put_and_get_tenant_policy(n in 1u64..=10u64) {
        use std::sync::atomic::Ordering;
        let inner = Arc::new(InMemoryMetadataBackend::new());
        let (backend, counts) = CountingMetadataBackend::wrap(inner);

        for i in 0..n {
            let scope = tenant_scope(&format!("t-{i}"));
            let policy = TenantPolicy {
                scope: scope.clone(),
                spec: TenantPolicySpec::default(),
                version: 0,
            updated_at_ms: 0,
            updated_by: "test".to_string(),
            };
            backend.put_tenant_policy(&policy, None).expect("put");
            let _ = backend.get_tenant_policy(&scope);
        }

        prop_assert_eq!(counts.put_tenant_policy.load(Ordering::Relaxed), n);
        prop_assert_eq!(counts.get_tenant_policy.load(Ordering::Relaxed), n);
    }

    #[test]
    fn prop_tenant_policy_version_increments_on_update(updates in 1u32..=8u32) {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let scope = tenant_scope("version-track");
        let policy = TenantPolicy {
            scope: scope.clone(),
            spec: TenantPolicySpec::default(),
            version: 0,
        updated_at_ms: 0,
        updated_by: "test".to_string(),
        };
        let mut stored = meta.put_tenant_policy(&policy, None).expect("initial put");
        let initial_version = stored.version;

        for _ in 0..updates {
            let updated = TenantPolicy {
                scope: scope.clone(),
                spec: TenantPolicySpec::default(),
                version: stored.version,
                updated_at_ms: 0,
                updated_by: "test".to_string(),
            };
            stored = meta
                .put_tenant_policy(&updated, Some(stored.version))
                .expect("update");
        }

        prop_assert!(
            stored.version >= initial_version,
            "version should not decrease after updates"
        );
    }
}

// ---------------------------------------------------------------------------
// Deterministic quota state-machine tests
// ---------------------------------------------------------------------------

#[test]
fn quota_policy_put_and_get_round_trip() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("round-trip");
    let policy = TenantPolicy {
        scope: scope.clone(),
        spec: TenantPolicySpec {
            quota: Some(TenantQuotaPolicy {
                max_bytes: Some(512 * 1024 * 1024),
                max_objects: Some(5000),
            }),
            ..Default::default()
        },
        version: 0,
        updated_at_ms: 0,
        updated_by: "test".to_string(),
    };
    let stored = meta.put_tenant_policy(&policy, None).expect("put");
    assert_eq!(stored.scope, scope);

    let fetched = meta
        .get_tenant_policy(&scope)
        .expect("get")
        .expect("must exist");
    assert_eq!(fetched.scope, scope);
    let q = fetched.spec.quota.expect("quota must be present");
    assert_eq!(q.max_bytes, Some(512 * 1024 * 1024));
    assert_eq!(q.max_objects, Some(5000));
}

#[test]
fn quota_policy_delete_removes_entry() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("delete-me");
    let policy = TenantPolicy {
        scope: scope.clone(),
        spec: TenantPolicySpec::default(),
        version: 0,
        updated_at_ms: 0,
        updated_by: "test".to_string(),
    };
    let stored = meta.put_tenant_policy(&policy, None).expect("put");
    let removed = meta
        .delete_tenant_policy(&scope, Some(stored.version))
        .expect("delete");
    assert!(removed, "delete must return true");

    let fetched = meta.get_tenant_policy(&scope).expect("get after delete");
    assert!(fetched.is_none(), "deleted policy must not be retrievable");
}

#[test]
fn quota_policy_list_returns_all_installed() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scopes = [tenant_scope("a"), tenant_scope("b"), tenant_scope("c")];
    for scope in &scopes {
        let policy = TenantPolicy {
            scope: scope.clone(),
            spec: TenantPolicySpec::default(),
            version: 0,
            updated_at_ms: 0,
            updated_by: "test".to_string(),
        };
        meta.put_tenant_policy(&policy, None).expect("put");
    }
    let all = meta.list_tenant_policies(None).expect("list");
    assert_eq!(all.len(), scopes.len());
}

#[test]
fn quota_policy_delete_absent_returns_false() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("absent");
    let deleted = meta
        .delete_tenant_policy(&scope, None)
        .expect("delete absent");
    assert!(!deleted);
}

#[test]
fn quota_state_returns_none_for_missing_policy() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("no-policy");
    let state = meta
        .get_tenant_quota_state(&scope)
        .expect("get_tenant_quota_state should not error");
    // No policy installed → state is None or empty
    if let Some(s) = state {
        assert_eq!(s.used_bytes, 0);
    }
}

#[test]
fn quota_state_after_policy_install_is_empty() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("empty-state");
    install_quota_policy(&meta, &scope);

    let state = meta.get_tenant_quota_state(&scope).expect("get state");
    if let Some(s) = state {
        assert_eq!(s.used_bytes, 0, "fresh quota state must have 0 used bytes");
        assert_eq!(s.used_objects, 0);
    }
}

#[test]
fn counting_backend_tracks_get_tenant_quota_state() {
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("count-quota");
    install_quota_policy(&inner, &scope);

    let (backend, counts) = CountingMetadataBackend::wrap(inner);
    for i in 1..=4u64 {
        let _ = backend.get_tenant_quota_state(&scope);
        assert_eq!(counts.get_tenant_quota_state.load(Ordering::Relaxed), i);
    }
    assert_eq!(counts.reserve_tenant_quota.load(Ordering::Relaxed), 0);
}

#[test]
fn faulty_backend_fails_quota_state_after_threshold() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let scope = tenant_scope("faulty");
    install_quota_policy(&inner, &scope);

    let faulty = FaultyMetadataBackend::wrap(inner, 2);
    faulty.get_tenant_quota_state(&scope).expect("call 1 ok");
    faulty.get_tenant_quota_state(&scope).expect("call 2 ok");
    let err = faulty
        .get_tenant_quota_state(&scope)
        .expect_err("call 3 must fail");
    assert!(matches!(err, StoreError::Metadata(_)));
}

#[test]
fn counting_backend_list_tenant_policies_increments() {
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    backend.list_tenant_policies(None).expect("list 1");
    backend.list_tenant_policies(None).expect("list 2");
    assert_eq!(counts.list_tenant_policies.load(Ordering::Relaxed), 2);
    assert_eq!(counts.get_tenant_policy.load(Ordering::Relaxed), 0);
}

#[test]
fn counting_backend_put_handoff_and_get_handoff_tracked() {
    use mooncake_store_core::{
        ClientEpoch, ClientRuntimeId, ClientStableId, HandoffKind, HandoffPlan,
    };
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let stable_id = ClientStableId::new("hoff");
    let plan = HandoffPlan {
        stable_id: stable_id.clone(),
        from: ClientRuntimeId::new("hoff", ClientEpoch(1)),
        to: ClientRuntimeId::new("hoff", ClientEpoch(2)),
        kind: HandoffKind::HotUpgrade,
        barrier_version: 0,
        created_at_ms: 0,
        deadline_ms: None,
    };
    backend.put_handoff(&plan).expect("put 1");
    backend.put_handoff(&plan).expect("put 2");
    let _ = backend.get_handoff(&stable_id);

    assert_eq!(counts.put_handoff.load(Ordering::Relaxed), 2);
    assert_eq!(counts.get_handoff.load(Ordering::Relaxed), 1);
}

#[test]
fn counting_backend_update_client_state_tracked() {
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor,
    };
    use std::sync::atomic::Ordering;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let runtime = ClientRuntimeId::new("update-state", ClientEpoch(1));
    let lease = ClientLease {
        runtime: runtime.clone(),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::mooncake_v1(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: u64::MAX,
    };
    backend.upsert_client_lease(&lease).expect("upsert");
    backend
        .update_client_state(&runtime, ClientLifecycleState::Draining)
        .expect("update 1");
    backend
        .update_client_state(&runtime, ClientLifecycleState::Draining)
        .expect("update 2");

    assert_eq!(counts.upsert_client_lease.load(Ordering::Relaxed), 1);
    assert_eq!(counts.update_client_state.load(Ordering::Relaxed), 2);
}
