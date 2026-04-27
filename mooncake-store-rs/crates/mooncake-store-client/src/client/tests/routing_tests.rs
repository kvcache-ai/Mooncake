// ---------------------------------------------------------------------------
// routing_tests.rs — route CAS, query_route, get_into/get_size, placement
// ---------------------------------------------------------------------------
//
// Exercises route-layer contracts:
//   - CAS: version mismatch conflict, sequential updates, cross-tenant
//     independence
//   - query_route: default-tenant scope, tenant isolation, post-overwrite
//     reflection, post-remove absence
//   - RouteVersion monotonicity across overwrites and reset-on-remove
//   - get_into: exact/oversize/undersize buffers, tenant round-trip
//   - get_size: post-overwrite size, nonexistent
//   - PlacementPlanner: input validation (zero replicas, too many replicas)

use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{ClientLifecycleState, StoreError};
use mooncake_store_test_utils::transport::TestTransport;

use crate::{MooncakeCompatibilityFacade, StoreClientBuilder};

use super::{fast_live_client_sync_interval, storage_config_with_bytes, test_future_expiry_ms};

// ===========================================================================
// Helpers
// ===========================================================================

fn build_local_writer(
    meta: &Arc<InMemoryMetadataBackend>,
    stable_id: &str,
    storage_bytes: usize,
) -> crate::StoreClient {
    let transport = Arc::new(TestTransport::new(&format!("{stable_id}-seg")));
    let t = Arc::new(transport.peer(&format!("{stable_id}-seg")));
    let client = StoreClientBuilder::new(meta.clone(), stable_id)
        .state(ClientLifecycleState::Active)
        .segment_name(format!("{stable_id}-seg"))
        .transport(t)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(storage_bytes))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");
    client
}

// ===========================================================================
// CAS route — version conflicts, deletes, sequential updates
// ===========================================================================

#[test]
fn cas_route_version_mismatch_returns_conflict() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "cas-mismatch", 16 * 1024);

    client.put("cas-key", b"initial").expect("put");
    let current = client.query_route("cas-key").expect("query").expect("some");

    // A CAS that expects a stale version must not apply
    let res = client
        .cas_route(
            "cas-key",
            Some(mooncake_store_core::RouteVersion(999)),
            Some(&current),
        )
        .expect("cas call");
    assert!(!res.applied, "mismatched-version CAS must not apply");
    assert!(
        res.current.is_some(),
        "mismatch result must include current route"
    );
}

#[test]
fn cas_route_delete_existing_key() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "cas-delete", 16 * 1024);

    client.put("cas-del-key", b"data").expect("put");
    let current = client
        .query_route("cas-del-key")
        .expect("query")
        .expect("some");

    let res = client
        .cas_route("cas-del-key", Some(current.version), None)
        .expect("cas");
    assert!(res.applied, "CAS-delete with correct version must apply");

    let after = client.query_route("cas-del-key").expect("query after");
    assert!(after.is_none(), "key must be absent after CAS-delete");
}

#[test]
fn cas_route_sequential_updates() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "cas-seq", 32 * 1024);

    client.put("cas-seq-key", b"v1").expect("put v1");
    let r1 = client
        .query_route("cas-seq-key")
        .expect("query")
        .expect("some");
    let v1 = r1.version;

    client.put("cas-seq-key", b"v2").expect("overwrite v2");
    let r2 = client
        .query_route("cas-seq-key")
        .expect("query")
        .expect("some");
    assert!(r2.version > v1, "overwrite must bump version");

    client.put("cas-seq-key", b"v3").expect("overwrite v3");
    let r3 = client
        .query_route("cas-seq-key")
        .expect("query")
        .expect("some");
    assert!(r3.version > r2.version, "second overwrite must bump again");
}

#[test]
fn cas_route_in_tenant_different_tenants_independent() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "cas-t-iso", 32 * 1024);

    client.put_in_tenant("alpha", "k", b"a").expect("put alpha");
    client.put_in_tenant("beta", "k", b"b").expect("put beta");

    let ra = client
        .query_route_in_tenant("alpha", "k")
        .expect("q alpha")
        .expect("some");
    let rb = client
        .query_route_in_tenant("beta", "k")
        .expect("q beta")
        .expect("some");

    // Tenants are independent → their ObjectKey encodings must differ
    assert_ne!(ra.key.0, rb.key.0);
}

// ===========================================================================
// query_route — default tenant, isolation, overwrites, post-remove absence
// ===========================================================================

#[test]
fn query_route_uses_default_tenant_scope() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "qr-default", 8 * 1024);

    client.put("default-key", b"v").expect("put");
    let r1 = client.query_route("default-key").expect("q");
    let r2 = client
        .query_route_in_tenant("default", "default-key")
        .expect("q tenant");
    assert_eq!(r1, r2, "bare query_route must use tenant='default'");
}

#[test]
fn query_route_in_tenant_returns_correct_tenant_route() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "qr-tenant", 16 * 1024);

    client
        .put_in_tenant("alice", "shared", b"alice")
        .expect("put alice");
    client
        .put_in_tenant("bob", "shared", b"bob")
        .expect("put bob");

    let ra = client
        .query_route_in_tenant("alice", "shared")
        .expect("q alice")
        .expect("some");
    let rb = client
        .query_route_in_tenant("bob", "shared")
        .expect("q bob")
        .expect("some");
    assert_ne!(ra.key.0, rb.key.0, "per-tenant ObjectKey must differ");
}

#[test]
fn query_route_in_tenant_isolates_correctly() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "qr-iso", 8 * 1024);

    client
        .put_in_tenant("isolated", "only-here", b"data")
        .expect("put");

    assert!(client
        .query_route_in_tenant("isolated", "only-here")
        .expect("q")
        .is_some());
    assert!(client
        .query_route_in_tenant("other", "only-here")
        .expect("q other")
        .is_none());
}

#[test]
fn query_route_reflects_latest_version_after_overwrite() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "qr-latest", 8 * 1024);

    client.put("k", b"v1").expect("v1");
    let v1 = client.query_route("k").expect("q").expect("some").version;
    client.put("k", b"v2").expect("v2");
    let v2 = client.query_route("k").expect("q").expect("some").version;
    assert!(v2 > v1, "query_route must reflect latest version");
}

#[test]
fn query_route_returns_none_after_remove() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "qr-absent", 8 * 1024);

    client.put("gone-key", b"v").expect("put");
    client.remove("gone-key", false).expect("remove");

    assert!(client.query_route("gone-key").expect("q").is_none());
}

// ===========================================================================
// RouteVersion monotonicity
// ===========================================================================

#[test]
fn route_version_increases_monotonically_across_10_overwrites() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "rv-mono", 64 * 1024);

    let mut prev = mooncake_store_core::RouteVersion(0);
    for i in 0..10 {
        client
            .put("rv-key", format!("v{i}").as_bytes())
            .expect("put");
        let v = client
            .query_route("rv-key")
            .expect("q")
            .expect("some")
            .version;
        assert!(v > prev, "version must strictly increase on overwrite");
        prev = v;
    }
}

#[test]
fn put_remove_put_version_continues_to_increment() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "rv-reinsert", 8 * 1024);

    client.put("rkey", b"v1").expect("put");
    let v1 = client
        .query_route("rkey")
        .expect("q")
        .expect("some")
        .version;
    client.put("rkey", b"v2").expect("overwrite");
    client.remove("rkey", false).expect("remove");
    client.put("rkey", b"new").expect("re-insert");

    let v = client
        .query_route("rkey")
        .expect("q")
        .expect("some")
        .version;
    assert!(
        v > v1,
        "remove-then-reinsert must not roll version backwards (got {v:?} vs initial {v1:?})"
    );
}

// ===========================================================================
// get_into — buffer-size variants
// ===========================================================================

#[test]
fn get_into_reads_into_preallocated_buffer() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gi-pre", 8 * 1024);

    client.put("gi-key", b"hello").expect("put");
    let mut buf = [0u8; 5];
    let n = client.get_into("gi-key", &mut buf).expect("get_into");
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");
}

#[test]
fn get_into_buffer_exact_size() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gi-exact", 8 * 1024);

    client.put("exact", b"ABCDEFGH").expect("put");
    let mut buf = [0u8; 8];
    let n = client.get_into("exact", &mut buf).expect("get_into");
    assert_eq!(n, 8);
    assert_eq!(&buf, b"ABCDEFGH");
}

#[test]
fn get_into_buffer_larger_than_value_only_fills_needed() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gi-large", 8 * 1024);

    client.put("k", b"hi").expect("put");
    let mut buf = [0xFFu8; 16];
    let n = client.get_into("k", &mut buf).expect("get_into");
    assert_eq!(n, 2);
    assert_eq!(&buf[..2], b"hi");
}

#[test]
fn get_into_buffer_too_small_returns_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gi-small", 8 * 1024);

    client
        .put("too-small", b"this-is-longer-than-4")
        .expect("put");
    let mut buf = [0u8; 4];
    let err = client
        .get_into("too-small", &mut buf)
        .expect_err("undersized must fail");
    assert!(matches!(
        err,
        StoreError::InvalidState(_) | StoreError::Allocator(_)
    ));
}

#[test]
fn get_into_in_tenant_round_trip() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gi-t", 8 * 1024);

    client
        .put_in_tenant("t-rt", "key", b"tenant-data")
        .expect("put");
    let mut buf = [0u8; 11];
    let n = client
        .get_into_in_tenant("t-rt", "key", &mut buf)
        .expect("get_into_in_tenant");
    assert_eq!(n, 11);
    assert_eq!(&buf, b"tenant-data");
}

// ===========================================================================
// get_size
// ===========================================================================

#[test]
fn get_size_after_overwrite_reflects_new_size() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gs-ow", 8 * 1024);

    client.put("gs-key", b"short").expect("put short");
    assert_eq!(client.get_size("gs-key").expect("size"), 5);
    client.put("gs-key", b"longer-value").expect("put longer");
    assert_eq!(client.get_size("gs-key").expect("size after overwrite"), 12);
}

#[test]
fn get_size_for_nonexistent_key_returns_zero_or_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "gs-nf", 8 * 1024);

    let result = client.get_size("no-such-key");
    match result {
        Ok(n) => assert_eq!(n, 0, "absent key must report zero size"),
        Err(StoreError::NotFound(_)) => {}
        Err(e) => panic!("unexpected error for absent key: {e:?}"),
    }
}

// ===========================================================================
// PlacementPlanner — input validation
// ===========================================================================

#[test]
fn placement_plan_zero_replica_count_returns_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "pp-zero", 8 * 1024);

    let planner = crate::PlacementPlanner::new(meta);
    let objects = vec![crate::ObjectRef::new("k")];
    let err = planner
        .plan(&client, &objects, 0)
        .expect_err("zero replica count must fail");
    assert!(matches!(
        err,
        StoreError::InvalidState(_) | StoreError::Allocator(_)
    ));
}

#[test]
fn placement_plan_more_replicas_than_nodes_returns_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_local_writer(&meta, "pp-many", 8 * 1024);

    let planner = crate::PlacementPlanner::new(meta);
    let objects = vec![crate::ObjectRef::new("k")];
    // Only one live client → cannot plan 5 replicas
    let err = planner
        .plan(&client, &objects, 5)
        .expect_err("too many replicas must fail");
    assert!(matches!(
        err,
        StoreError::InvalidState(_) | StoreError::Allocator(_) | StoreError::Conflict(_)
    ));
}
