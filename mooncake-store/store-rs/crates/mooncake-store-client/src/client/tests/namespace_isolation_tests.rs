// ---------------------------------------------------------------------------
// namespace_isolation_tests.rs — End-to-end correctness gates for the
// three-layer namespace model (tenant / domain / object_set).
//
// Existing `routing_tests.rs` covers tenant-vs-tenant *route key* divergence
// for `query_route_in_tenant`; this module is intentionally additive and
// covers everything that was missing:
//
//   * Cross-tenant **read** isolation across the full read API surface
//     (`get`, `batch_get`, `is_exist`, `batch_is_exist`, `query_route_in_scope`,
//     `list_routes_in_scope`).
//   * `domain` and `object_set` isolation (the routing tests exercise only
//     `tenant`).
//   * `remove_in_tenant` / `batch_remove` cross-tenant isolation.
//   * Strict-quota **per-tenant** isolation (A exhausting its quota does not
//     block B; B's writes do not appear in A's `TenantQuotaState`).
//   * Property-style invariants over a small grid of scopes and clients.
//
// The fixtures live in `isolation_test_harness.rs`.  All tests run with the
// in-process `TestTransport` so cross-client visibility is bounded only by
// metadata convergence, which the harness waits for at construction time.
// ---------------------------------------------------------------------------

use mooncake_store_core::{
    MetadataBackend, NamespaceScope, ObjectKey, StoreError, TenantQuotaState,
};

use crate::{MooncakeCompatibilityFacade, ObjectRef, PutRequest};

mod harness {
    include!("isolation_test_harness.rs");
}

use harness::{install_generous_quota_policy, install_tenant_quota_policy, IsolationCluster};

// ===========================================================================
// 1. Cross-tenant write/read isolation across the full client API
// ===========================================================================

#[test]
fn cross_tenant_get_returns_not_found_for_other_tenants_key() {
    let cluster = IsolationCluster::with_clients("xt-get", 1);
    let client = cluster.client(0);

    client
        .put_in_tenant("alpha", "shared-key", b"alpha-data")
        .expect("alpha put should succeed");

    // Same logical key under tenant=beta must not resolve to alpha's value.
    let err = client
        .get_in_tenant("beta", "shared-key")
        .expect_err("beta get should fail");
    assert!(
        matches!(err, StoreError::NotFound(_)),
        "expected NotFound, got {err:?}"
    );

    // Sanity: alpha's own get still works.
    let v = client
        .get_in_tenant("alpha", "shared-key")
        .expect("alpha get should succeed");
    assert_eq!(v, b"alpha-data");
}

#[test]
fn cross_tenant_is_exist_and_batch_is_exist_are_isolated() {
    let cluster = IsolationCluster::with_clients("xt-exist", 1);
    let client = cluster.client(0);

    client
        .put_in_tenant("alpha", "k1", b"v1")
        .expect("alpha put should succeed");

    assert!(client
        .is_exist_in_tenant("alpha", "k1")
        .expect("is_exist alpha"));
    assert!(!client
        .is_exist_in_tenant("beta", "k1")
        .expect("is_exist beta"));

    let refs = [
        ObjectRef::new("k1").tenant("alpha"),
        ObjectRef::new("k1").tenant("beta"),
        ObjectRef::new("missing").tenant("alpha"),
    ];
    let exists = client
        .batch_is_exist(&refs)
        .expect("batch_is_exist should succeed");
    assert_eq!(exists, vec![true, false, false]);
}

#[test]
fn cross_tenant_batch_get_yields_per_tenant_results() {
    let cluster = IsolationCluster::with_clients("xt-batch-get", 1);
    let client = cluster.client(0);

    client
        .put_in_tenant("alpha", "k", b"alpha-v")
        .expect("alpha put");
    client
        .put_in_tenant("beta", "k", b"beta-v")
        .expect("beta put");

    let refs = [
        ObjectRef::new("k").tenant("alpha"),
        ObjectRef::new("k").tenant("beta"),
    ];
    let values = client.batch_get(&refs).expect("batch_get should succeed");
    assert_eq!(values, vec![b"alpha-v".to_vec(), b"beta-v".to_vec()]);
}

#[test]
fn cross_tenant_remove_does_not_affect_other_tenant() {
    let cluster = IsolationCluster::with_clients("xt-remove", 1);
    let client = cluster.client(0);

    client.put_in_tenant("alpha", "k", b"a").expect("alpha put");
    client.put_in_tenant("beta", "k", b"b").expect("beta put");

    client
        .remove_in_tenant("alpha", "k", false)
        .expect("alpha remove should succeed");

    // beta untouched.
    let v = client
        .get_in_tenant("beta", "k")
        .expect("beta get should still succeed");
    assert_eq!(v, b"b");

    // alpha gone.
    let err = client
        .get_in_tenant("alpha", "k")
        .expect_err("alpha get should now fail");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[test]
fn cross_tenant_batch_remove_only_removes_matching_scope() {
    let cluster = IsolationCluster::with_clients("xt-batch-remove", 1);
    let client = cluster.client(0);

    client.put_in_tenant("alpha", "k", b"a").expect("alpha put");
    client.put_in_tenant("beta", "k", b"b").expect("beta put");

    client
        .batch_remove(&[ObjectRef::new("k").tenant("alpha")], false)
        .expect("batch_remove alpha should succeed");

    let v = client
        .get_in_tenant("beta", "k")
        .expect("beta get should still succeed");
    assert_eq!(v, b"b");
}

// ===========================================================================
// 2. Domain isolation (same tenant, different domain)
// ===========================================================================

#[test]
fn cross_domain_keys_are_independent_objects() {
    let cluster = IsolationCluster::with_clients("xd", 1);
    let client = cluster.client(0);

    client
        .batch_put(&[
            PutRequest::new("k", b"a").tenant("t1").domain("d1"),
            PutRequest::new("k", b"b").tenant("t1").domain("d2"),
        ])
        .expect("cross-domain batch_put should succeed");

    let r1 = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t1"), Some("d1"), None),
            "k",
        )
        .expect("d1 query")
        .expect("d1 must exist");
    let r2 = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t1"), Some("d2"), None),
            "k",
        )
        .expect("d2 query")
        .expect("d2 must exist");

    assert_ne!(
        r1.key.0, r2.key.0,
        "different domains must produce different ObjectKey"
    );
    assert_eq!(r1.canonical_key.as_deref(), Some("t1/d1/default/k"));
    assert_eq!(r2.canonical_key.as_deref(), Some("t1/d2/default/k"));
}

// ===========================================================================
// 3. object_set isolation (same tenant + domain, different object_set)
// ===========================================================================

#[test]
fn cross_object_set_keys_are_independent_objects() {
    let cluster = IsolationCluster::with_clients("xs", 1);
    let client = cluster.client(0);

    client
        .batch_put(&[
            PutRequest::new("k", b"a")
                .tenant("t1")
                .domain("d1")
                .object_set("s1"),
            PutRequest::new("k", b"b")
                .tenant("t1")
                .domain("d1")
                .object_set("s2"),
        ])
        .expect("cross-object-set batch_put should succeed");

    let r1 = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t1"), Some("d1"), Some("s1")),
            "k",
        )
        .expect("s1 query")
        .expect("s1 must exist");
    let r2 = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t1"), Some("d1"), Some("s2")),
            "k",
        )
        .expect("s2 query")
        .expect("s2 must exist");

    assert_ne!(r1.key.0, r2.key.0);
    assert_eq!(r1.canonical_key.as_deref(), Some("t1/d1/s1/k"));
    assert_eq!(r2.canonical_key.as_deref(), Some("t1/d1/s2/k"));
}

// ===========================================================================
// 4. Default scope key encoding: legacy `tenant::logical_key` vs full canonical
// ===========================================================================

#[test]
fn default_scope_uses_legacy_tenant_double_colon_key_encoding() {
    let cluster = IsolationCluster::with_clients("default-encoding", 1);
    let client = cluster.client(0);

    client
        .put_in_tenant("t-legacy", "k", b"v")
        .expect("default-scope put should succeed");

    let route = client
        .query_route_in_tenant("t-legacy", "k")
        .expect("query")
        .expect("must exist");
    // Default scope (default domain + default object_set) uses the compact
    // `tenant::logical_key` ObjectKey form; canonical_key remains the
    // explicit `tenant/default/default/logical_key` for traceability.
    assert_eq!(route.key, ObjectKey::new("t-legacy::k"));
    assert_eq!(
        route.canonical_key.as_deref(),
        Some("t-legacy/default/default/k")
    );
}

#[test]
fn non_default_domain_uses_legacy_namespace_object_key() {
    let cluster = IsolationCluster::with_clients("non-default-encoding", 1);
    let client = cluster.client(0);

    client
        .batch_put(&[PutRequest::new("k", b"v").tenant("t1").domain("d1")])
        .expect("non-default domain put should succeed");

    let route = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t1"), Some("d1"), None),
            "k",
        )
        .expect("query")
        .expect("must exist");
    // With tenant="t1", domain="d1", object_set=default, the client uses the
    // legacy namespace ObjectKey form, which yields "t1::ns/d1/default/k" rather
    // than the pure canonical "t1/d1/default/k".  This is intentional for
    // back-compat with the existing wire format; the test pins the actual
    // contract so any future change becomes visible.
    assert_eq!(route.key, ObjectKey::new("t1::ns/d1/default/k"));
}

// ===========================================================================
// 5. list_routes_in_scope must not leak across scopes
// ===========================================================================

#[test]
fn list_routes_in_scope_does_not_leak_across_tenants() {
    let cluster = IsolationCluster::with_clients("xt-list", 1);
    let client = cluster.client(0);

    client.put_in_tenant("alpha", "k1", b"a1").expect("a1");
    client.put_in_tenant("alpha", "k2", b"a2").expect("a2");
    client.put_in_tenant("beta", "k1", b"b1").expect("b1");

    let alpha_scope = NamespaceScope::with_defaults(Some("alpha"), None, None);
    let beta_scope = NamespaceScope::with_defaults(Some("beta"), None, None);

    let alpha_routes = client
        .list_routes_in_scope(&alpha_scope)
        .expect("list alpha");
    let beta_routes = client.list_routes_in_scope(&beta_scope).expect("list beta");

    assert_eq!(alpha_routes.len(), 2, "alpha must see 2 routes");
    assert_eq!(beta_routes.len(), 1, "beta must see 1 route");

    // Cross-check: every alpha route key must contain "alpha::" and never "beta::".
    for r in &alpha_routes {
        let k = r.key.0.as_str();
        assert!(
            k.starts_with("alpha::") || k.starts_with("alpha/"),
            "alpha listing leaked non-alpha key: {k}"
        );
    }
    for r in &beta_routes {
        let k = r.key.0.as_str();
        assert!(
            k.starts_with("beta::") || k.starts_with("beta/"),
            "beta listing leaked non-beta key: {k}"
        );
    }
}

#[test]
fn list_routes_in_scope_does_not_leak_across_domains() {
    let cluster = IsolationCluster::with_clients("xd-list", 1);
    let client = cluster.client(0);

    client
        .batch_put(&[
            PutRequest::new("k1", b"v").tenant("t").domain("d1"),
            PutRequest::new("k2", b"v").tenant("t").domain("d1"),
            PutRequest::new("k1", b"v").tenant("t").domain("d2"),
        ])
        .expect("cross-domain put");

    let d1 = client
        .list_routes_in_scope(&NamespaceScope::with_defaults(Some("t"), Some("d1"), None))
        .expect("list d1");
    let d2 = client
        .list_routes_in_scope(&NamespaceScope::with_defaults(Some("t"), Some("d2"), None))
        .expect("list d2");

    assert_eq!(d1.len(), 2);
    assert_eq!(d2.len(), 1);
}

// ===========================================================================
// 6. Cross-tenant strict-quota isolation
// ===========================================================================

#[test]
fn tenant_quota_state_increments_only_for_owning_tenant() {
    let cluster = IsolationCluster::with_clients_setup("quota-iso-state", 1, |metadata| {
        install_generous_quota_policy(metadata, "alpha");
        install_generous_quota_policy(metadata, "beta");
    });
    let client = cluster.client(0);

    client
        .put_in_tenant("alpha", "k", b"hello")
        .expect("alpha put");
    client
        .put_in_tenant("beta", "k", b"world!!")
        .expect("beta put");

    let alpha_state = cluster
        .metadata
        .get_tenant_quota_state(&policy_scope("alpha"))
        .expect("alpha state should be readable");
    let beta_state = cluster
        .metadata
        .get_tenant_quota_state(&policy_scope("beta"))
        .expect("beta state should be readable");

    let alpha_used = used_bytes(alpha_state.as_ref());
    let beta_used = used_bytes(beta_state.as_ref());
    assert_eq!(alpha_used, b"hello".len() as u64);
    assert_eq!(beta_used, b"world!!".len() as u64);
}

#[test]
fn tenant_quota_exhaustion_does_not_block_other_tenant() {
    // Production behaviour: when a tenant's per-tenant byte/object quota is
    // exceeded, the client first attempts a *tenant-local eviction* (see
    // `runtime_io::reserve_tenant_quota_for_put` and
    // `try_evict_one_object_in_tenant`).  So a single small overflow is NOT
    // a deterministic rejection signal -- the runtime may make room by
    // evicting one of the tenant's own earlier objects.
    //
    // To get a deterministic "hard rejection" we issue a second put whose
    // single-object size already exceeds `max_bytes`; in that case eviction
    // cannot help (the new object alone does not fit) and the request MUST
    // be rejected with QuotaExceeded.
    //
    // The client warms its tenant-policy hot cache during `build()`, so the
    // policy is installed via `with_clients_setup`'s pre-build hook.
    let cluster = IsolationCluster::with_clients_setup("quota-iso-exhaust", 1, |metadata| {
        install_tenant_quota_policy(metadata, "alpha", Some(8u64), Some(1024usize));
        install_generous_quota_policy(metadata, "beta");
    });
    let client = cluster.client(0);

    // First small write fits within max_bytes=8.
    client.put_in_tenant("alpha", "k1", b"abc").expect("a1");

    // Second write is intentionally larger than max_bytes=8: even if the
    // runtime evicts every alpha object, the new object alone does not fit
    // and the put MUST be rejected.
    let oversize = vec![0xCDu8; 16];
    let denied = client.put_in_tenant("alpha", "k2", &oversize);
    assert!(
        denied.is_err(),
        "alpha second put (16B > max_bytes=8) should be quota-rejected, got Ok"
    );

    // beta is unaffected by alpha's exhausted quota.
    for i in 0..16 {
        let key = format!("k-{i}");
        client
            .put_in_tenant("beta", &key, b"some-payload")
            .expect("beta put should not be blocked by alpha quota");
    }
}

// ===========================================================================
// 7. Two-client visibility: writer / reader isolation
// ===========================================================================

#[test]
fn writer_and_reader_clients_observe_per_tenant_writes_consistently() {
    let cluster = IsolationCluster::with_clients("two-clients", 2);
    let writer = cluster.client(0);
    let reader = cluster.client(1);

    writer
        .put_in_tenant("alpha", "k", b"alpha-data")
        .expect("w-a");
    writer
        .put_in_tenant("beta", "k", b"beta-data")
        .expect("w-b");

    // reader must see both, scoped correctly.
    let va = reader
        .get_in_tenant("alpha", "k")
        .expect("reader gets alpha");
    let vb = reader.get_in_tenant("beta", "k").expect("reader gets beta");
    assert_eq!(va, b"alpha-data");
    assert_eq!(vb, b"beta-data");

    // reader's cross-tenant query under a *third* tenant must be NotFound,
    // ruling out a "first observed value wins" leak in the reader's caches.
    let err = reader
        .get_in_tenant("gamma", "k")
        .expect_err("reader gamma must NotFound");
    assert!(matches!(err, StoreError::NotFound(_)));
}

// ===========================================================================
// 8. Property: independent (tenant, domain, object_set, key) tuples never
//    collide on the routing layer.
// ===========================================================================

#[test]
fn routes_for_distinct_scope_tuples_have_distinct_object_keys() {
    let cluster = IsolationCluster::with_clients("scope-grid", 1);
    let client = cluster.client(0);

    let tenants = ["t1", "t2"];
    let domains = ["d1", "d2"];
    let sets = ["s1", "s2"];
    let keys = ["alpha", "beta"];

    let mut requests: Vec<PutRequest<'_>> = Vec::new();
    for t in &tenants {
        for d in &domains {
            for s in &sets {
                for k in &keys {
                    requests.push(PutRequest::new(k, b"v").tenant(t).domain(d).object_set(s));
                }
            }
        }
    }
    let routes = client.batch_put(&requests).expect("grid batch_put");
    assert_eq!(routes.len(), requests.len());

    // Every route's ObjectKey must be unique because every (t,d,s,k) is.
    let mut seen = std::collections::HashSet::new();
    for r in &routes {
        let inserted = seen.insert(r.key.0.clone());
        assert!(
            inserted,
            "scope grid produced duplicate ObjectKey: {}",
            r.key.0
        );
    }
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

fn policy_scope(tenant: &str) -> mooncake_store_core::TenantPolicyScope {
    mooncake_store_core::TenantPolicyScope {
        tenant: tenant.to_string(),
        domain: None,
        object_set: None,
    }
}

fn used_bytes(state: Option<&TenantQuotaState>) -> u64 {
    state.map(|s| s.used_bytes).unwrap_or(0)
}
