// ---------------------------------------------------------------------------
// namespace_adversarial.rs — Adversarial / boundary tests for the
// namespace-scope encoding and tenant identity surface.
//
// These tests document the current boundary behavior for "interesting" tenant /
// domain / object_set strings:
//   * Known ambiguous separator cases are kept as ignored regression baselines;
//   * Unicode look-alikes must not collide due to accidental normalisation;
//   * Empty or extremely long inputs must not crash the encoder;
//   * Routing-layer key isolation should not be bypassed by control characters.
//
// We test the lower-level `NamespaceScope` / `LogicalObjectId` surface
// directly when possible (those are pure functions), and fall back to driving
// the full client when the assertion is about end-to-end behaviour.
// ---------------------------------------------------------------------------

use std::collections::HashSet;
use std::sync::Arc;

use mooncake_store_core::{
    LogicalObjectId, NamespaceScope, ObjectKey, DEFAULT_DOMAIN, DEFAULT_OBJECT_SET, DEFAULT_TENANT,
};
use mooncake_store_test_utils::transport::TestTransport;

use crate::{MooncakeCompatibilityFacade, PutRequest};

mod harness {
    include!("isolation_test_harness.rs");
}

use harness::IsolationCluster;

// ===========================================================================
// 1. canonical_prefix / canonical_key — pure adversarial input fuzzing
// ===========================================================================

// FIXME(known-limitation, P3-security): namespace scope encoding is currently
// slash-delimited without escaping or length-prefixing.  This is ambiguous when
// a tenant/domain/object_set/logical_key component itself contains '/'.  In the
// current threat model (trusted tenants; the isolation goal is anti-mistake, not
// anti-malice) this is accepted.  When adversarial SDK consumers are in scope,
// the encoding must escape '/' or switch to a length-prefixed form.
#[test]
#[ignore = "known limitation: slash-delimited namespace encoding can collide; see FIXME above"]
fn canonical_key_with_slash_components_can_collide_across_tuples() {
    let left = LogicalObjectId::new(NamespaceScope::new("a/b", "c", "d"), "e");
    let right = LogicalObjectId::new(NamespaceScope::new("a", "b", "c"), "d/e");

    assert_ne!(
        left.canonical_key(),
        right.canonical_key(),
        "slash-delimited canonical_key must be made injective before this test is enabled"
    );
}

#[test]
fn canonical_prefix_with_empty_strings_is_treated_as_explicit_value() {
    // An empty tenant string is *not* the same as the default tenant; this
    // guards against accidental promotion when callers forget to pass a
    // tenant name.
    let empty = NamespaceScope::new("", "", "");
    let default = NamespaceScope::default();
    assert_ne!(
        empty.canonical_prefix(),
        default.canonical_prefix(),
        "explicit empty tenant must not collide with the default tenant"
    );
}

#[test]
fn canonical_prefix_unicode_lookalikes_are_distinct() {
    // 'A' (U+0041) vs 'Α' (Greek capital alpha, U+0391) — visually identical
    // in many fonts.  They must canonicalise differently because we do not
    // perform Unicode normalisation.
    let latin = NamespaceScope::with_defaults(Some("A"), None, None);
    let greek = NamespaceScope::with_defaults(Some("\u{0391}"), None, None);
    assert_ne!(latin.canonical_prefix(), greek.canonical_prefix());
}

#[test]
fn canonical_key_with_extremely_long_tenant_does_not_panic() {
    let long_tenant: String = std::iter::repeat('t').take(8 * 1024).collect();
    let scope = NamespaceScope::with_defaults(Some(&long_tenant), None, None);
    let oid = LogicalObjectId::new(scope, "k");
    let _ = oid.canonical_key(); // must not panic / overflow
}

#[test]
fn canonical_key_with_null_byte_in_logical_key_is_preserved_not_truncated() {
    // The key encoding is a Rust `String`, not a C string, so an embedded
    // NUL must round-trip through `canonical_key()` without truncation.
    let scope = NamespaceScope::with_defaults(Some("t"), None, None);
    let oid = LogicalObjectId::new(scope, "ab\0cd");
    let key = oid.canonical_key();
    assert!(
        key.contains('\0'),
        "canonical_key must preserve embedded NUL"
    );
    assert!(key.ends_with("/ab\0cd"));
}

#[test]
fn canonical_prefix_default_scope_matches_default_constants() {
    // Sanity-check: the public default constants are what we advertise to
    // operators in the user guide; tests depend on that contract.
    let scope = NamespaceScope::default();
    assert_eq!(scope.tenant, DEFAULT_TENANT);
    assert_eq!(scope.domain, DEFAULT_DOMAIN);
    assert_eq!(scope.object_set, DEFAULT_OBJECT_SET);
    assert_eq!(
        scope.canonical_prefix(),
        format!("{DEFAULT_TENANT}/{DEFAULT_DOMAIN}/{DEFAULT_OBJECT_SET}")
    );
}

#[test]
fn canonical_keys_form_an_injective_map_when_components_do_not_contain_separator() {
    // Property-style assertion over the supported subset: components that do
    // not contain '/' must yield distinct canonical_keys.
    let tenants = ["a", "a\u{0041}", "a\u{0391}", "", "long"];
    let domains = ["d1", "d2", "d3"];
    let sets = ["s1", "s2"];
    let keys = ["k", "kk"];

    let mut seen = HashSet::new();
    for t in &tenants {
        for d in &domains {
            for s in &sets {
                for k in &keys {
                    let scope = NamespaceScope::new(*t, *d, *s);
                    let oid = LogicalObjectId::new(scope, *k);
                    let canonical = oid.canonical_key();
                    let inserted = seen.insert(canonical.clone());
                    assert!(
                        inserted,
                        "canonical_key collision for distinct tuple: {canonical}"
                    );
                }
            }
        }
    }
}

// ===========================================================================
// 2. End-to-end: forging a scope must not let one tenant read another's data
// ===========================================================================

#[test]
fn tenant_with_embedded_slash_cannot_read_two_level_scope_data() {
    let cluster = IsolationCluster::with_clients("adv-slash", 1);
    let client = cluster.client(0);

    // Real two-level write: tenant="t", domain="x"
    client
        .batch_put(&[PutRequest::new("k", b"two-level-data")
            .tenant("t")
            .domain("x")])
        .expect("two-level put");

    // Adversary: a single-level tenant whose name embeds the slash
    // (`tenant="t/x"`).  It must NOT see the two-level write.
    let _put_attempt = client
        .put_in_tenant("t/x", "probe", b"adv")
        .expect("forged-tenant put should still succeed (it's just another tenant)");

    let two_level_route = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t"), Some("x"), None),
            "k",
        )
        .expect("real two-level query")
        .expect("two-level data must still resolve");

    let forged_route = client
        .query_route_in_tenant("t/x", "k")
        .expect("forged-tenant query");
    assert!(
        forged_route.is_none(),
        "tenant with embedded slash leaked into two-level scope: {forged_route:?}"
    );
    // The two-level data is still uniquely owned by the real path.
    assert_eq!(
        two_level_route.canonical_key.as_deref(),
        Some("t/x/default/k")
    );
}

#[test]
fn empty_tenant_is_a_distinct_namespace_from_default_tenant() {
    let cluster = IsolationCluster::with_clients("adv-empty-tenant", 1);
    let client = cluster.client(0);

    // Default tenant write.
    client
        .put_in_tenant("default", "k", b"default-data")
        .expect("d");

    // Empty tenant write attempts to put under tenant="".  This is a
    // suspicious request and the client may either reject it outright or
    // route it to a fresh, isolated namespace.  Either outcome is acceptable
    // for security; the only forbidden behaviour is "" silently aliasing to
    // "default".
    let result = client.put_in_tenant("", "k", b"empty-data");

    if result.is_ok() {
        // If the client accepted it, the resulting tenant must NOT be the
        // default tenant.
        let v_default = client
            .get_in_tenant("default", "k")
            .expect("default still readable");
        assert_eq!(
            v_default, b"default-data",
            "empty tenant write must not overwrite the default tenant"
        );
    }
    // If the client rejected it (Err), that's also a valid security stance.
}

// ===========================================================================
// 3. Special characters in keys must not break ObjectKey encoding
// ===========================================================================

// FIXME(known-limitation, P3-security): the default-scope encoding
// `tenant::logical_key` is ambiguous when either side contains the literal
// substring "::".  In the current threat model (trusted tenants; the
// isolation goal is anti-mistake, not anti-malice) this is accepted.
// When the threat model changes to include adversarial SDK consumers,
// the ObjectKey encoding must escape "::" or switch to a length-prefixed
// form.  This test is kept (with `#[ignore]`) so it acts as a regression
// baseline the moment the encoder is hardened.
#[test]
#[ignore = "known limitation: tenant/key with literal '::' can collide; see FIXME above"]
fn keys_with_double_colon_substring_do_not_collide_across_tenants() {
    // The default-scope encoding uses "::" as a separator.  A key that
    // itself contains "::" must not be mis-parsed in a way that lets it
    // collide with another tenant's key.
    let cluster = IsolationCluster::with_clients("adv-colon-key", 1);
    let client = cluster.client(0);

    client
        .put_in_tenant("alpha", "x::y", b"alpha-xy")
        .expect("alpha put");
    client
        .put_in_tenant("alpha::x", "y", b"forged")
        .expect("forged-tenant put");

    let v1 = client
        .get_in_tenant("alpha", "x::y")
        .expect("alpha::x::y get");
    let v2 = client
        .get_in_tenant("alpha::x", "y")
        .expect("alpha::x::y get via forged tenant");

    // Both reads succeed, and they MUST resolve to different values (or, at
    // minimum, the routes must differ).  If the encoder collapsed them, v1
    // == v2 and the property fails.
    let r1 = client
        .query_route_in_tenant("alpha", "x::y")
        .expect("q1")
        .expect("r1");
    let r2 = client
        .query_route_in_tenant("alpha::x", "y")
        .expect("q2")
        .expect("r2");
    assert_ne!(
        r1.key.0, r2.key.0,
        "ObjectKey collision between (alpha, 'x::y') and ('alpha::x', 'y')"
    );
    // Defensive: even if the routes differ, also assert the values differ
    // because the underlying storage might have been shared.
    assert_ne!(v1, v2, "values collided: {:?} vs {:?}", v1, v2);
}

#[test]
#[ignore = "known limitation: slash in logical_key can collide with object_set boundary"]
fn keys_with_path_separator_do_not_collide_across_scopes() {
    // Once any scope component is non-default, the canonical encoding uses
    // '/'.  A key containing '/' is currently ambiguous with a deeper scope.
    let cluster = IsolationCluster::with_clients("adv-slash-key", 1);
    let client = cluster.client(0);

    client
        .batch_put(&[
            PutRequest::new("k/sub", b"v-sub").tenant("t").domain("d"),
            // Forged: try to land in a "deeper" scope by smuggling '/' in
            // the key.  A hardened encoder must keep this in (t, d, default,
            // "k/sub"), NOT in (t, d, "k", "sub").
        ])
        .expect("put with '/' in key");

    let r = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t"), Some("d"), None),
            "k/sub",
        )
        .expect("q")
        .expect("must exist");
    assert_eq!(r.canonical_key.as_deref(), Some("t/d/default/k/sub"));

    let probe = client
        .query_route_in_scope(
            &NamespaceScope::with_defaults(Some("t"), Some("d"), Some("k")),
            "sub",
        )
        .expect("q probe");
    assert!(
        probe.is_none(),
        "forged scope (t,d,k) leaked from key 'k/sub': {probe:?}"
    );
}

// ===========================================================================
// 4. ObjectKey constructor on raw strings must not accept ambiguous forms
// ===========================================================================

#[test]
fn object_key_round_trip_on_canonical_form_is_stable() {
    // We do not advertise an ObjectKey-string parser back to LogicalObjectId,
    // so we just check that constructing an ObjectKey and reading its
    // string form is a stable identity.
    let raw = "tenant/domain/set/key";
    let key = ObjectKey::new(raw);
    assert_eq!(key.0, raw);
}

// ---------------------------------------------------------------------------
// Suppress unused-import warning for harness types we only conditionally
// reach via the `mod harness { include! ... }` shim.
// ---------------------------------------------------------------------------
#[allow(dead_code)]
fn _adversarial_harness_marker() -> Arc<TestTransport> {
    Arc::new(TestTransport::new("unused"))
}
