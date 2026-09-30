// ---------------------------------------------------------------------------
// perf_invariant_tests.rs — Hot-path metadata-call invariants for the
// multi-tenant feature stack.
//
// These tests do NOT measure wall-clock time (Criterion benches in
// `benches/multi_tenant_perf.rs` do that).  Instead they assert *structural*
// performance invariants:
//
//   * After warmup, the put/get hot path must NOT call expensive metadata
//     APIs such as `list_tenant_policies` or repeatedly re-resolve a tenant
//     policy that already lives in the per-client hot cache.
//   * Default-scope puts must take the legacy `tenant::logical_key` form
//     (no extra metadata round-trip for scope canonicalisation).
//   * Admin-installed strict-quota policy must trigger exactly one
//     reserve+finalize pair per put, with no leaked reservations.
//
// Together these invariants act as a regression gate: if a future change
// accidentally adds a metadata round-trip to the hot path the tests will
// fail well before any latency-bench regression appears.
//
// Implementation notes:
//   * Tests run with the in-process `TestTransport`, so transport cost is
//     constant; counter deltas reflect only metadata-side cost.
//   * Each test uses a fresh `CountingMetadataBackend` wrapping a fresh
//     `InMemoryMetadataBackend`, isolating it from other tests.
//   * `super::*` is intentionally NOT imported — we deliberately avoid
//     coupling to `mod.rs`'s ~12k-line fixture surface so this file stays
//     auditable as a self-contained perf gate.
// ---------------------------------------------------------------------------

use std::sync::atomic::Ordering;
use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    ClientLifecycleState, MetadataBackend, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
    TenantQuotaPolicy,
};
use mooncake_store_test_utils::fixtures::test_future_expiry_ms;
use mooncake_store_test_utils::metadata::{CountingMetadataBackend, OperationCounts};
use mooncake_store_test_utils::transport::TestTransport;

use crate::{MooncakeCompatibilityFacade, PlacementPlanner, ReplicationPolicy, StoreClientBuilder};

mod helpers {
    include!("perf_invariant_helpers.rs");
}

use helpers::build_perf_client;

// ===========================================================================
// Invariant 1: default-scope put hot path is hot-cache friendly
// ===========================================================================

#[test]
fn default_scope_put_does_not_call_list_tenant_policies_after_warmup() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let transport = Arc::new(TestTransport::new("perf-inv-default-scope"));
    let client = build_perf_client(
        backend.clone() as Arc<dyn MetadataBackend>,
        transport,
        "perf-inv-default",
        "tenant-default",
    );

    // Warmup: prime any per-tenant hot caches.
    client
        .put_in_tenant("tenant-default", "warm", b"v")
        .expect("warmup put should succeed");
    let warm_list_calls = counts.list_tenant_policies.load(Ordering::Relaxed);

    // Steady state: 32 puts in a row.
    for i in 0..32u64 {
        let key = format!("k-{i}");
        client
            .put_in_tenant("tenant-default", &key, b"v")
            .expect("steady-state put should succeed");
    }

    let steady_list_calls = counts.list_tenant_policies.load(Ordering::Relaxed);
    assert_eq!(
        steady_list_calls,
        warm_list_calls,
        "list_tenant_policies must not be called by the put hot path after warmup; \
         saw {} additional calls over 32 puts",
        steady_list_calls - warm_list_calls
    );
}

#[test]
fn default_scope_put_does_not_re_resolve_tenant_policy_after_warmup() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let transport = Arc::new(TestTransport::new("perf-inv-resolve"));
    let client = build_perf_client(
        backend.clone() as Arc<dyn MetadataBackend>,
        transport,
        "perf-inv-resolve",
        "tenant-resolve",
    );

    client
        .put_in_tenant("tenant-resolve", "warm", b"v")
        .expect("warmup put should succeed");
    let warm_get_policy_calls = counts.get_tenant_policy.load(Ordering::Relaxed);

    for i in 0..32u64 {
        let key = format!("k-{i}");
        client
            .put_in_tenant("tenant-resolve", &key, b"v")
            .expect("steady-state put should succeed");
    }

    let steady_get_policy_calls = counts.get_tenant_policy.load(Ordering::Relaxed);
    let delta = steady_get_policy_calls.saturating_sub(warm_get_policy_calls);
    // Allow a small constant ceiling for opportunistic policy refresh — but
    // hard-cap it well below "1 per put" so a hot-cache regression fails
    // loudly.  Current expected steady-state is 0; we accept up to 4 to leave
    // headroom for future opportunistic refresh logic.
    assert!(
        delta <= 4,
        "get_tenant_policy hot-path budget exceeded: {delta} extra calls over 32 puts"
    );
}

// ===========================================================================
// Invariant 2: get hot path stays out of quota / policy metadata APIs
// ===========================================================================

#[test]
fn get_hot_path_does_not_touch_quota_or_policy_metadata() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let transport = Arc::new(TestTransport::new("perf-inv-get"));
    let client = build_perf_client(
        backend.clone() as Arc<dyn MetadataBackend>,
        transport,
        "perf-inv-get",
        "tenant-get",
    );

    // Seed one object so subsequent gets all hit the same fixed route.
    client
        .put_in_tenant("tenant-get", "fixed", b"v")
        .expect("seed put should succeed");
    let baseline = snapshot_quota_and_policy_counts(&counts);

    for _ in 0..32 {
        let _ = client
            .get_in_tenant("tenant-get", "fixed")
            .expect("get should succeed");
    }

    let after = snapshot_quota_and_policy_counts(&counts);
    assert_eq!(
        after.reserve_tenant_quota, baseline.reserve_tenant_quota,
        "get hot path must not call reserve_tenant_quota"
    );
    assert_eq!(
        after.finalize_tenant_quota, baseline.finalize_tenant_quota,
        "get hot path must not call finalize_tenant_quota"
    );
    assert_eq!(
        after.list_tenant_policies, baseline.list_tenant_policies,
        "get hot path must not call list_tenant_policies"
    );
}

#[test]
fn routed_put_reuses_prewarmed_segment_target_chunks() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);
    let metadata = backend.clone() as Arc<dyn MetadataBackend>;

    let storage_transport = Arc::new(TestTransport::new("perf-target-storage-segment"));
    let storage = StoreClientBuilder::new(metadata.clone(), "perf-target-storage")
        .state(ClientLifecycleState::Active)
        .tenant("tenant-target")
        .label("pool", "perf-target-pool")
        .label("storage", "true")
        .transport(storage_transport.clone())
        .local_memory(helpers::perf_storage_config())
        .build(test_future_expiry_ms())
        .expect("storage build should succeed");
    storage
        .register_local_memory()
        .expect("storage memory should register");

    let writer_transport = Arc::new(storage_transport.peer("perf-target-writer-segment"));
    let writer = StoreClientBuilder::new(metadata.clone(), "perf-target-writer")
        .state(ClientLifecycleState::Active)
        .tenant("tenant-target")
        .label("pool", "perf-target-pool")
        .label("storage", "false")
        .transport(writer_transport)
        .local_memory(helpers::perf_storage_config())
        .routed_writes(
            PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer memory should register");

    let baseline_get_segment = counts.get_segment.load(Ordering::Relaxed);
    let baseline_list_segments = counts.list_segments.load(Ordering::Relaxed);
    let policy = ReplicationPolicy::new().prefer_local(false);
    for i in 0..32u64 {
        let key = format!("target-k-{i}");
        writer
            .put_in_tenant_with_policy("tenant-target", &key, b"v", &policy)
            .expect("routed put should succeed");
    }

    assert_eq!(
        counts.get_segment.load(Ordering::Relaxed),
        baseline_get_segment,
        "routed put hot path must not fetch segment announcements after build prewarm"
    );
    assert_eq!(
        counts.list_segments.load(Ordering::Relaxed),
        baseline_list_segments,
        "routed put hot path must not fall back to segment listing"
    );
}

// ===========================================================================
// Invariant 3: strict-quota path performs exactly one reserve+finalize per put
// ===========================================================================

#[test]
fn strict_quota_put_uses_exactly_one_reserve_and_finalize() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    install_generous_quota_policy(&inner, "tenant-strict");
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let transport = Arc::new(TestTransport::new("perf-inv-strict-quota"));
    let client = build_perf_client(
        backend.clone() as Arc<dyn MetadataBackend>,
        transport,
        "perf-inv-strict-quota",
        "tenant-strict",
    );

    client
        .put_in_tenant("tenant-strict", "warm", b"v")
        .expect("warmup put should succeed");
    let baseline_reserve = counts.reserve_tenant_quota.load(Ordering::Relaxed);
    let baseline_finalize = counts.finalize_tenant_quota.load(Ordering::Relaxed);
    let baseline_abort = counts.abort_tenant_quota.load(Ordering::Relaxed);

    const N: u64 = 16;
    for i in 0..N {
        let key = format!("k-{i}");
        client
            .put_in_tenant("tenant-strict", &key, b"v")
            .expect("strict-quota put should succeed");
    }

    let reserve_delta = counts.reserve_tenant_quota.load(Ordering::Relaxed) - baseline_reserve;
    let finalize_delta = counts.finalize_tenant_quota.load(Ordering::Relaxed) - baseline_finalize;
    let abort_delta = counts.abort_tenant_quota.load(Ordering::Relaxed) - baseline_abort;

    assert_eq!(
        reserve_delta, N,
        "strict-quota put should reserve exactly once per call"
    );
    assert_eq!(
        finalize_delta, N,
        "strict-quota put should finalize exactly once per successful call"
    );
    assert_eq!(
        abort_delta, 0,
        "happy-path strict-quota put must not abort any reservation"
    );
}

// ===========================================================================
// Invariant 4: many tenants share the metadata backend without scaling its
// hot-path call count by tenant count
// ===========================================================================

#[test]
fn multi_tenant_round_robin_put_does_not_amplify_metadata_calls() {
    const TENANTS: usize = 8;
    let inner = Arc::new(InMemoryMetadataBackend::new());
    for i in 0..TENANTS {
        install_generous_quota_policy(&inner, &format!("tenant-{i}"));
    }
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let transport = Arc::new(TestTransport::new("perf-inv-multi-tenant"));
    let client = build_perf_client(
        backend.clone() as Arc<dyn MetadataBackend>,
        transport,
        "perf-inv-multi-tenant",
        "tenant-0",
    );
    // Warm every tenant's hot cache before measuring.
    for i in 0..TENANTS {
        let tenant = format!("tenant-{i}");
        client
            .put_in_tenant(&tenant, "warm", b"v")
            .expect("warmup put should succeed");
    }
    let baseline_get_policy = counts.get_tenant_policy.load(Ordering::Relaxed);
    let baseline_list_policy = counts.list_tenant_policies.load(Ordering::Relaxed);

    const PUTS: usize = 64;
    for i in 0..PUTS {
        let tenant = format!("tenant-{}", i % TENANTS);
        let key = format!("k-{i}");
        client
            .put_in_tenant(&tenant, &key, b"v")
            .expect("steady-state put should succeed");
    }

    let delta_get_policy = counts.get_tenant_policy.load(Ordering::Relaxed) - baseline_get_policy;
    let delta_list_policy =
        counts.list_tenant_policies.load(Ordering::Relaxed) - baseline_list_policy;

    // Per-tenant hot cache should absorb policy lookups; allow a small
    // constant ceiling proportional to tenant count, well under "1 per put".
    let cap = TENANTS as u64;
    assert!(
        delta_get_policy <= cap,
        "get_tenant_policy delta {delta_get_policy} exceeded per-tenant cap {cap}"
    );
    assert_eq!(
        delta_list_policy, 0,
        "list_tenant_policies must not be called by steady-state hot path"
    );
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct QuotaPolicySnapshot {
    reserve_tenant_quota: u64,
    finalize_tenant_quota: u64,
    list_tenant_policies: u64,
}

fn snapshot_quota_and_policy_counts(counts: &Arc<OperationCounts>) -> QuotaPolicySnapshot {
    QuotaPolicySnapshot {
        reserve_tenant_quota: counts.reserve_tenant_quota.load(Ordering::Relaxed),
        finalize_tenant_quota: counts.finalize_tenant_quota.load(Ordering::Relaxed),
        list_tenant_policies: counts.list_tenant_policies.load(Ordering::Relaxed),
    }
}

fn install_generous_quota_policy(metadata: &Arc<InMemoryMetadataBackend>, tenant: &str) {
    let policy = TenantPolicy {
        scope: TenantPolicyScope {
            tenant: tenant.to_string(),
            domain: None,
            object_set: None,
        },
        spec: TenantPolicySpec {
            quota: Some(TenantQuotaPolicy {
                max_bytes: Some(1 << 30),
                max_objects: Some(1_000_000),
            }),
            ..Default::default()
        },
        version: 0,
        updated_at_ms: 0,
        updated_by: "perf-test".to_string(),
    };
    metadata
        .put_tenant_policy(&policy, None)
        .expect("install generous quota policy should succeed");
}
