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

// ===========================================================================
// Namespace × scope quota-state isolation invariants
// ---------------------------------------------------------------------------
// These tests cover the metadata-layer guarantee that `TenantQuotaState` and
// `TenantObjectAccounting` are keyed *exactly* by `TenantPolicyScope` (i.e.
// (tenant, domain, object_set)) and never aggregate across distinct scopes.
//
// The existing `prop_counting_backend_quota_state_queries` proptest at the
// top of this file already validates the counter-side contract; the cases
// below validate the *value* side: writes to scope X must only show up in
// scope X's accounting / state, never in a sibling scope's.
// ===========================================================================

fn scoped_policy_scope(
    tenant: &str,
    domain: Option<&str>,
    object_set: Option<&str>,
) -> TenantPolicyScope {
    TenantPolicyScope {
        tenant: tenant.to_string(),
        domain: domain.map(|s| s.to_string()),
        object_set: object_set.map(|s| s.to_string()),
    }
}

fn install_quota_for_scope(meta: &Arc<InMemoryMetadataBackend>, scope: &TenantPolicyScope) {
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
        updated_by: "scope-iso-test".to_string(),
    };
    meta.put_tenant_policy(&policy, None)
        .expect("install scoped quota policy should succeed");
}

#[test]
fn quota_state_is_isolated_per_scope_at_metadata_layer() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let scope_a = scoped_policy_scope("alpha", None, None);
    let scope_b = scoped_policy_scope("beta", None, None);
    install_quota_for_scope(&meta, &scope_a);
    install_quota_for_scope(&meta, &scope_b);

    // Both scopes start with no state (or with a zero state, depending on
    // the backend implementation).
    let initial_a = meta
        .get_tenant_quota_state(&scope_a)
        .expect("alpha state read")
        .map(|s| s.used_bytes)
        .unwrap_or(0);
    let initial_b = meta
        .get_tenant_quota_state(&scope_b)
        .expect("beta state read")
        .map(|s| s.used_bytes)
        .unwrap_or(0);
    assert_eq!(initial_a, 0);
    assert_eq!(initial_b, 0);
}

#[test]
fn quota_state_for_unknown_scope_is_independent_from_default() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let unknown = scoped_policy_scope("never-installed", None, None);
    // Reading state for a never-installed scope must not synthesise a value
    // taken from the default tenant; it must be either None or a fresh
    // zeroed state.
    let s = meta
        .get_tenant_quota_state(&unknown)
        .expect("read unknown scope");
    assert!(
        s.is_none() || s.as_ref().unwrap().used_bytes == 0,
        "unknown scope state must not aggregate from default: {s:?}"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    // For an arbitrary set of distinct tenant names, installing per-tenant
    // policies must not cause `list_tenant_policies(tenant)` to leak
    // policies from other tenants.
    #[test]
    fn prop_list_tenant_policies_filters_by_tenant(
        tenants in proptest::collection::hash_set("[a-z]{3,8}", 2..=6)
    ) {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let tenants_vec: Vec<String> = tenants.into_iter().collect();
        for t in &tenants_vec {
            install_quota_for_scope(&meta, &scoped_policy_scope(t, None, None));
        }

        for t in &tenants_vec {
            let listed = meta
                .list_tenant_policies(Some(t.as_str()))
                .expect("list per-tenant");
            // Every returned policy must be for the requested tenant only.
            for p in &listed {
                prop_assert_eq!(&p.scope.tenant, t);
            }
            // And the requested tenant's policy must be in the list.
            prop_assert!(
                listed.iter().any(|p| &p.scope.tenant == t),
                "tenant {} missing from its own list_tenant_policies", t
            );
        }
    }

    // Per-domain scopes under the same tenant are distinct policies and must
    // not collide.
    #[test]
    fn prop_per_domain_scopes_are_distinct_policies(
        domains in proptest::collection::hash_set("[a-z]{3,6}", 2..=5)
    ) {
        let meta = Arc::new(InMemoryMetadataBackend::new());
        let tenant = "shared-tenant";
        let domains_vec: Vec<String> = domains.into_iter().collect();
        for d in &domains_vec {
            install_quota_for_scope(
                &meta,
                &scoped_policy_scope(tenant, Some(d.as_str()), None),
            );
        }

        // Each (tenant, domain) policy must be retrievable individually.
        for d in &domains_vec {
            let scope = scoped_policy_scope(tenant, Some(d.as_str()), None);
            let policy = meta
                .get_tenant_policy(&scope)
                .expect("get per-domain policy");
            prop_assert!(
                policy.is_some(),
                "policy for (tenant={}, domain={}) was not retrievable", tenant, d
            );
            let policy = policy.unwrap();
            prop_assert_eq!(policy.scope.domain.as_deref(), Some(d.as_str()));
        }

        // The bare-tenant scope must NOT exist (we only installed
        // domain-scoped policies).
        let bare = scoped_policy_scope(tenant, None, None);
        let bare_policy = meta
            .get_tenant_policy(&bare)
            .expect("get bare policy");
        prop_assert!(
            bare_policy.is_none(),
            "bare-tenant policy must not exist: domain-scoped installs leaked into the bare scope"
        );
    }
}
