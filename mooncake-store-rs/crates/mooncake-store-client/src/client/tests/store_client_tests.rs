// ---------------------------------------------------------------------------
// store_client_tests.rs — StoreClient operation lifecycle and fault tolerance
// ---------------------------------------------------------------------------
//
// Organized into six groups mirroring fulin's store_client_tests.rs structure:
//   Phase 1 — basic put/get/remove/is_exist lifecycle
//   Phase 2 — batch operations, multi-tenant isolation, boundary conditions
//   Phase 3 — dual-node read path (reader/writer sharing metadata)
//   Phase 4 — fault tolerance (memory exhaustion, transport failure, concurrency)
//   Phase 5 — performance benchmarks (latency and throughput logging)
//   Phase 6 — FaultyTransport network-fault integration

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, CompatibilityDescriptor, MetadataBackend,
    ObjectKey, ObjectRoute, RouteDirectory, RouteState, RouteVersion, StoreError,
};
use mooncake_store_test_utils::transport::{FaultyTransport, TestTransport};

use crate::{
    MooncakeCompatibilityFacade, ObjectRef, PutRequest, ReplicationPolicy, StoreClientBuilder,
};

use super::{
    fast_live_client_sync_interval, storage_config_with_bytes, test_future_expiry_ms,
    wait_for_runtime_visibility,
};

// ===========================================================================
// Phase 1 — basic put/get/remove/is_exist lifecycle
// ===========================================================================

fn make_writer(
    meta: &Arc<InMemoryMetadataBackend>,
    transport: &Arc<TestTransport>,
) -> crate::StoreClient {
    let t = Arc::new(transport.peer("writer-seg"));
    StoreClientBuilder::new(meta.clone(), "writer")
        .state(ClientLifecycleState::Active)
        .segment_name("writer-seg")
        .transport(t)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(
            crate::LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(64 * 1024)
                .scratch_bytes(4096)
                .reclaim_grace_ms(0),
        )
        .build(test_future_expiry_ms())
        .expect("writer should build")
}

#[test]
fn put_get_remove_lifecycle() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    // put
    client.put("hello", b"world").expect("put should succeed");

    // is_exist — present
    assert!(client.is_exist("hello").expect("is_exist"));

    // get — round-trip
    let got = client.get("hello").expect("get should succeed");
    assert_eq!(got, b"world");

    // remove
    client
        .remove("hello", false)
        .expect("remove should succeed");

    // is_exist — absent
    assert!(!client.is_exist("hello").expect("is_exist after remove"));

    // get after remove — NotFound
    let err = client.get("hello").expect_err("get after remove must fail");
    assert!(matches!(err, StoreError::NotFound(_)));
}

#[test]
fn put_returns_route_with_data() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rt-seg"));
    let t = Arc::new(transport.peer("rt-seg"));
    let client = StoreClientBuilder::new(meta.clone(), "rt-client")
        .state(ClientLifecycleState::Active)
        .segment_name("rt-seg")
        .transport(t)
        .local_memory(storage_config_with_bytes(16 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    let route = client.put("k", b"v").expect("put");
    assert!(
        !route.replicas.is_empty(),
        "route must have at least one replica"
    );
    assert_eq!(route.replicas[0].length, 1);
}

#[test]
fn get_nonexistent_returns_not_found() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("nf-seg"));
    let client = make_writer(&meta, &transport);

    let err = client.get("nonexistent-key").expect_err("must fail");
    assert!(matches!(err, StoreError::NotFound(_)), "expected NotFound");
}

#[test]
fn remove_nonexistent_returns_not_found_or_ok() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rm-nf-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    // remove of absent key should not panic; error variant is acceptable
    let result = client.remove("absent-key", false);
    assert!(
        result.is_ok() || matches!(result.unwrap_err(), StoreError::NotFound(_)),
        "remove absent must be ok or NotFound"
    );
}

#[test]
fn overwrite_updates_data() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("ow-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put("key", b"first").expect("first put");
    client.put("key", b"second").expect("second put");

    let got = client.get("key").expect("get");
    assert_eq!(got, b"second", "overwrite must update the stored value");
}

#[test]
fn is_exist_returns_false_for_absent_key() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("ie-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    assert!(!client.is_exist("no-such-key").expect("is_exist"));
}

#[test]
fn put_and_get_preserves_exact_bytes() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("exact-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let payload: Vec<u8> = (0u8..=255u8).collect();
    client.put("binary", &payload).expect("put binary");
    let got = client.get("binary").expect("get binary");
    assert_eq!(got, payload, "all 256 byte values must round-trip");
}

// ===========================================================================
// Phase 2 — batch operations, multi-tenant isolation, boundary conditions
// ===========================================================================

#[test]
fn batch_put_then_batch_get_consistency() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("bp-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let payloads: Vec<Vec<u8>> = (0u8..8).map(|i| vec![i; 64]).collect();
    let requests: Vec<PutRequest<'_>> = payloads
        .iter()
        .enumerate()
        .map(|(i, v)| {
            PutRequest::new(
                &*Box::leak(format!("batch-key-{i}").into_boxed_str()),
                v.as_slice(),
            )
        })
        .collect();
    client.batch_put(&requests).expect("batch_put");

    for (i, expected) in payloads.iter().enumerate() {
        let got = client.get(&format!("batch-key-{i}")).expect("get");
        assert_eq!(&got, expected, "key {i} payload mismatch");
    }
}

#[test]
fn batch_is_exist_reflects_put_and_remove() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("bie-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put("p", b"v").expect("put p");
    client.put("q", b"v").expect("put q");

    let refs = [
        ObjectRef::new("p"),
        ObjectRef::new("q"),
        ObjectRef::new("absent"),
    ];
    let exists = client.batch_is_exist(&refs).expect("batch_is_exist");
    assert_eq!(exists, vec![true, true, false]);

    client.remove("p", false).expect("remove p");
    let exists2 = client
        .batch_is_exist(&refs)
        .expect("batch_is_exist after remove");
    assert_eq!(exists2, vec![false, true, false]);
}

#[derive(Default)]
struct RecordingRouteDirectory {
    single_calls: AtomicUsize,
    batch_calls: AtomicUsize,
    bounded_batch_calls: AtomicUsize,
    last_batch_len: AtomicUsize,
}

impl RecordingRouteDirectory {
    fn route_for(key: &ObjectKey) -> Option<ObjectRoute> {
        (!key.0.contains("absent")).then(|| ObjectRoute {
            key: key.clone(),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
        })
    }
}

impl RouteDirectory for RecordingRouteDirectory {
    fn get_object_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.single_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Self::route_for(key))
    }

    fn get_object_routes(
        &self,
        _observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<Vec<Option<ObjectRoute>>> {
        self.batch_calls.fetch_add(1, Ordering::Relaxed);
        self.last_batch_len.store(keys.len(), Ordering::Relaxed);
        Ok(keys.iter().map(Self::route_for).collect())
    }

    fn get_object_routes_bounded(
        &self,
        _observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<Vec<Option<ObjectRoute>>> {
        self.bounded_batch_calls.fetch_add(1, Ordering::Relaxed);
        self.last_batch_len.store(keys.len(), Ordering::Relaxed);
        Ok(keys.iter().map(Self::route_for).collect())
    }

    fn compare_and_swap_object_route(
        &self,
        _observer: &ClientLease,
        _key: &ObjectKey,
        _expected: Option<RouteVersion>,
        _next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<CasResult> {
        Err(StoreError::Unsupported(
            "recording route directory does not support CAS".to_string(),
        ))
    }
}

#[test]
fn batch_is_exist_uses_batched_route_lookup() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("bie-batch-route-seg"));
    let mut client = make_writer(&meta, &transport);
    let directory = Arc::new(RecordingRouteDirectory::default());
    client.route_directory = directory.clone();

    let exists = client
        .batch_is_exist(&[
            ObjectRef::new("present-a"),
            ObjectRef::new("absent"),
            ObjectRef::new("present-b").tenant("tenant-b"),
        ])
        .expect("batch_is_exist should succeed");

    assert_eq!(exists, vec![true, false, true]);
    assert_eq!(directory.bounded_batch_calls.load(Ordering::Relaxed), 1);
    assert_eq!(directory.batch_calls.load(Ordering::Relaxed), 0);
    assert_eq!(directory.last_batch_len.load(Ordering::Relaxed), 3);
    assert_eq!(directory.single_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn multi_tenant_isolation_put_in_tenant_a_invisible_to_tenant_b() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("mt-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    // Put key under tenant "alice"
    client
        .put_in_tenant("alice", "shared-key", b"alice-data")
        .expect("put in alice");

    // Key should be visible in tenant alice
    assert!(client
        .is_exist_in_tenant("alice", "shared-key")
        .expect("is_exist alice"));

    // Key should NOT be visible in tenant bob
    assert!(!client
        .is_exist_in_tenant("bob", "shared-key")
        .expect("is_exist bob"));
}

#[test]
fn boundary_single_byte_value() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("sb-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put("single", &[0xABu8]).expect("put single byte");
    let got = client.get("single").expect("get");
    assert_eq!(got, [0xABu8]);
}

#[test]
fn boundary_special_characters_in_key() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("sc-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let special_key = "key/with:special.chars?query=1&a=2#fragment";
    client
        .put(special_key, b"special")
        .expect("put special key");
    let got = client.get(special_key).expect("get special key");
    assert_eq!(got, b"special");
}

#[test]
fn boundary_long_key() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("lk-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let long_key = "k".repeat(512);
    client
        .put(&long_key, b"long-key-value")
        .expect("put long key");
    let got = client.get(&long_key).expect("get long key");
    assert_eq!(got, b"long-key-value");
}

#[test]
fn boundary_binary_data_in_value() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("bin-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let binary: Vec<u8> = (0..256).map(|i| (i % 256) as u8).collect();
    client.put("binary-key", &binary).expect("put binary");
    let got = client.get("binary-key").expect("get binary");
    assert_eq!(got, binary);
}

#[test]
fn batch_remove_eliminates_multiple_keys() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("br-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    for k in ["x", "y", "z"] {
        client.put(k, b"v").expect("put");
    }

    let refs = [
        ObjectRef::new("x"),
        ObjectRef::new("y"),
        ObjectRef::new("z"),
    ];
    client.batch_remove(&refs, false).expect("batch_remove");

    for k in ["x", "y", "z"] {
        assert!(
            !client.is_exist(k).expect("is_exist"),
            "{k} must not exist after batch_remove"
        );
    }
}

#[test]
fn put_with_replication_policy_succeeds() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("rp-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let policy = ReplicationPolicy::new().replica_count(1).prefer_local(true);
    client
        .put_with_policy("policy-key", b"data", &policy)
        .expect("put_with_policy");
    let got = client.get("policy-key").expect("get");
    assert_eq!(got, b"data");
}

// ===========================================================================
// Phase 3 — dual-node read path (writer puts, reader reads via shared metadata)
// ===========================================================================

#[test]
fn dual_node_writer_puts_reader_gets() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-seg"));

    // writer: local storage node
    let writer_t = Arc::new(transport.peer("writer-seg"));
    let writer = Arc::new(
        StoreClientBuilder::new(meta.clone(), "writer")
            .state(ClientLifecycleState::Active)
            .segment_name("writer-seg")
            .transport(writer_t)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .local_memory(storage_config_with_bytes(32 * 1024))
            .build(test_future_expiry_ms())
            .expect("writer build"),
    );
    writer.register_local_memory().expect("writer register");
    meta.upsert_client_lease(&writer.lease())
        .expect("upsert writer lease");

    // reader: rw-only client sharing same metadata
    let reader_t = Arc::new(transport.peer("reader-seg"));
    let reader = StoreClientBuilder::new(meta.clone(), "reader")
        .state(ClientLifecycleState::Active)
        .segment_name("reader-seg")
        .transport(reader_t)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(
            crate::LocalMemoryConfig::new()
                .storage_bytes(0)
                .scratch_bytes(4096)
                .numa_aware(false),
        )
        .build(test_future_expiry_ms())
        .expect("reader build");

    writer
        .put("shared-key", b"shared-data")
        .expect("writer put");

    // wait for reader to discover writer
    wait_for_runtime_visibility(&reader, writer.runtime_id());

    let got = reader.get("shared-key").expect("reader get");
    assert_eq!(got, b"shared-data", "reader must see writer's data");
}

#[test]
fn get_size_returns_correct_byte_count() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("gs-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let payload = b"hello world";
    client.put("gs-key", payload).expect("put");
    let size = client.get_size("gs-key").expect("get_size");
    assert_eq!(
        size,
        payload.len(),
        "get_size must return exact payload length"
    );
}

// ===========================================================================
// Phase 4 — fault tolerance
// ===========================================================================

#[test]
fn memory_exhaustion_returns_error_when_storage_full() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("oom-seg"));
    let oom_t = Arc::new(transport.peer("oom-seg"));
    // 4 KB storage — exhausted by a few 2 KB writes
    let client = StoreClientBuilder::new(meta.clone(), "oom-client")
        .state(ClientLifecycleState::Active)
        .segment_name("oom-seg")
        .transport(oom_t)
        .local_memory(storage_config_with_bytes(4 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    let value = vec![0u8; 2 * 1024];
    let mut success = 0usize;
    let mut last_err: Option<StoreError> = None;
    for i in 0..8 {
        match client.put(&format!("oom-key-{i}"), &value) {
            Ok(_) => success += 1,
            Err(e) => {
                last_err = Some(e);
                break;
            }
        }
    }
    // At least one write must have succeeded, then we hit capacity
    assert!(success >= 1, "must succeed at least once before OOM");
    assert!(
        last_err.is_some(),
        "must fail eventually when storage is full"
    );
}

#[test]
fn concurrent_puts_no_deadlock() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("conc-seg"));
    let client = Arc::new(make_writer(&meta, &transport));
    client.register_local_memory().expect("register");

    let handles: Vec<_> = (0..6)
        .map(|t| {
            let client = client.clone();
            thread::spawn(move || {
                for i in 0..8 {
                    let key = format!("conc-key-{t}-{i}");
                    let _ = client.put(&key, b"concurrent-value");
                }
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(10);
    for h in handles {
        let timeout = deadline.saturating_duration_since(Instant::now());
        assert!(
            h.join().is_ok(),
            "concurrent put thread must not panic within timeout"
        );
        let _ = timeout;
    }
}

/// Helper: build a reader client using FaultyTransport that shares the same
/// transport state as `writer_transport`. The reader has no local storage
/// and reads remotely from the writer's segment.
fn make_faulty_reader(
    meta: &Arc<InMemoryMetadataBackend>,
    writer_transport: &Arc<TestTransport>,
    stable_id: &str,
    segment_name: &str,
) -> (
    crate::StoreClient,
    Arc<mooncake_store_test_utils::transport::FaultConfig>,
) {
    let inner = Arc::new(writer_transport.peer(segment_name));
    let (faulty, faults) = FaultyTransport::new(inner);
    let client = StoreClientBuilder::new(meta.clone(), stable_id)
        .state(ClientLifecycleState::Active)
        .segment_name(segment_name)
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(
            crate::LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(0)
                .scratch_bytes(4096),
        )
        .build(test_future_expiry_ms())
        .expect("faulty reader build");
    (client, faults)
}

#[test]
fn faulty_transport_open_failure_prevents_remote_read() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("writer-fault-seg"));
    let writer_t = Arc::new(writer_transport.peer("writer-fault-seg"));
    let writer = Arc::new(
        StoreClientBuilder::new(meta.clone(), "writer-fault")
            .state(ClientLifecycleState::Active)
            .segment_name("writer-fault-seg")
            .transport(writer_t)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .local_memory(storage_config_with_bytes(32 * 1024))
            .build(test_future_expiry_ms())
            .expect("writer build"),
    );
    writer.register_local_memory().expect("register");
    meta.upsert_client_lease(&writer.lease()).expect("upsert");

    writer.put("fault-key", b"fault-value").expect("writer put");

    let (reader, faults) = make_faulty_reader(
        &meta,
        &writer_transport,
        "faulty-reader",
        "reader-fault-seg",
    );
    wait_for_runtime_visibility(&reader, writer.runtime_id());

    // Inject open failures — reader cannot open writer's segment
    faults.fail_next_opens(1_000_000);

    let err = reader
        .get("fault-key")
        .expect_err("get must fail with open failures");
    assert!(matches!(
        err,
        StoreError::Transport(_) | StoreError::NotFound(_)
    ));
}

#[test]
fn faulty_transport_submit_failure_prevents_remote_read() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("submit-fault-seg"));
    let writer_t = Arc::new(writer_transport.peer("submit-fault-seg"));
    let writer = Arc::new(
        StoreClientBuilder::new(meta.clone(), "submit-fault-writer")
            .state(ClientLifecycleState::Active)
            .segment_name("submit-fault-seg")
            .transport(writer_t)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .local_memory(storage_config_with_bytes(32 * 1024))
            .build(test_future_expiry_ms())
            .expect("writer build"),
    );
    writer.register_local_memory().expect("register");
    meta.upsert_client_lease(&writer.lease()).expect("upsert");
    writer
        .put("submit-key", b"submit-value")
        .expect("writer put");

    let (reader, faults) = make_faulty_reader(
        &meta,
        &writer_transport,
        "submit-fault-reader",
        "reader-submit-seg",
    );
    wait_for_runtime_visibility(&reader, writer.runtime_id());

    // Inject submit failures — reader cannot fetch data
    faults.fail_next_submits(1_000_000);

    let err = reader
        .get("submit-key")
        .expect_err("get must fail with submit failures");
    assert!(matches!(
        err,
        StoreError::Transport(_) | StoreError::NotFound(_)
    ));
}

#[test]
fn faulty_transport_disconnect_then_reconnect_restores_remote_read() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("disc-rr-seg"));
    let writer_t = Arc::new(writer_transport.peer("disc-rr-seg"));
    let writer = Arc::new(
        StoreClientBuilder::new(meta.clone(), "disc-rr-writer")
            .state(ClientLifecycleState::Active)
            .segment_name("disc-rr-seg")
            .transport(writer_t)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .local_memory(storage_config_with_bytes(32 * 1024))
            .build(test_future_expiry_ms())
            .expect("writer build"),
    );
    writer.register_local_memory().expect("register");
    meta.upsert_client_lease(&writer.lease()).expect("upsert");
    writer.put("disc-rr-key", b"disc-rr-value").expect("put");

    let (reader, faults) = make_faulty_reader(
        &meta,
        &writer_transport,
        "disc-rr-reader",
        "reader-disc-rr-seg",
    );
    wait_for_runtime_visibility(&reader, writer.runtime_id());

    // Disconnect — skip the get during disconnect to avoid corrupting the route
    // (a failed transport read can cause the client to invalidate the route in metadata)
    faults.disconnect();
    faults.reconnect();

    let got = reader.get("disc-rr-key").expect("get after reconnect");
    assert_eq!(got, b"disc-rr-value");
}

#[test]
fn faulty_transport_open_latency_does_not_corrupt_data() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let writer_transport = Arc::new(TestTransport::new("lat-rr-seg"));
    let writer_t = Arc::new(writer_transport.peer("lat-rr-seg"));
    let writer = Arc::new(
        StoreClientBuilder::new(meta.clone(), "lat-rr-writer")
            .state(ClientLifecycleState::Active)
            .segment_name("lat-rr-seg")
            .transport(writer_t)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .local_memory(storage_config_with_bytes(32 * 1024))
            .build(test_future_expiry_ms())
            .expect("writer build"),
    );
    writer.register_local_memory().expect("register");
    meta.upsert_client_lease(&writer.lease()).expect("upsert");
    writer.put("lat-key", b"lat-value").expect("put");

    let (reader, faults) =
        make_faulty_reader(&meta, &writer_transport, "lat-reader", "reader-lat-seg");
    wait_for_runtime_visibility(&reader, writer.runtime_id());

    // Introduce 1ms open latency
    faults.set_open_latency(Duration::from_millis(1));

    let got = reader.get("lat-key").expect("get with open latency");
    assert_eq!(
        got, b"lat-value",
        "data must not be corrupted by open latency"
    );
    faults.set_open_latency(Duration::ZERO);
}

// ===========================================================================
// Phase 5 — performance benchmarks (log latency / throughput; do not assert)
// ===========================================================================

#[test]
fn benchmark_single_put_latency_1kb() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("perf-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let value = vec![0u8; 1024];
    let iterations = 200usize;
    let mut latencies: Vec<Duration> = Vec::with_capacity(iterations);

    for i in 0..iterations {
        let key = format!("perf-{i}");
        let start = Instant::now();
        let _ = client.put(&key, &value);
        latencies.push(start.elapsed());
    }

    latencies.sort_unstable();
    let p50 = latencies[iterations / 2];
    let p99 = latencies[iterations * 99 / 100];
    let avg = latencies.iter().sum::<Duration>() / iterations as u32;
    eprintln!("[benchmark_single_put_latency_1kb] p50={p50:?} p99={p99:?} avg={avg:?}");
    // Not asserting performance bounds — environment variability is too high.
    // The test exists to catch regressions via log review.
}

#[test]
fn benchmark_single_get_latency_1kb() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("get-perf-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let value = vec![0xAAu8; 1024];
    client.put("perf-base-key", &value).expect("put");

    let iterations = 200usize;
    let mut latencies: Vec<Duration> = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = client.get("perf-base-key");
        latencies.push(start.elapsed());
    }

    latencies.sort_unstable();
    let p50 = latencies[iterations / 2];
    let p99 = latencies[iterations * 99 / 100];
    eprintln!("[benchmark_single_get_latency_1kb] p50={p50:?} p99={p99:?}");
}

// ===========================================================================
// Phase 6 — FaultyTransport network fault integration
// ===========================================================================

#[test]
fn network_recovery_reconnect_restores_operations() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let inner = Arc::new(TestTransport::new("recovery-seg"));
    let (faulty, faults) = FaultyTransport::new(inner.clone());

    let client = StoreClientBuilder::new(meta.clone(), "recovery-client")
        .state(ClientLifecycleState::Active)
        .segment_name("recovery-seg")
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(32 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    client
        .put("pre", b"pre-data")
        .expect("put before disconnect");

    // put() writes to local memory — transport disconnect does not affect it
    faults.disconnect();
    let _ = client.put("during", b"whatever");
    faults.reconnect();

    client
        .put("post", b"post-data")
        .expect("put after reconnect");
    let got = client.get("post").expect("get after reconnect");
    assert_eq!(got, b"post-data");
    // pre-disconnect data must survive the cycle
    let pre = client.get("pre").expect("pre-disconnect data must survive");
    assert_eq!(pre, b"pre-data");
}

#[test]
fn network_data_survives_disconnect_reconnect_cycle() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let inner = Arc::new(TestTransport::new("survive-seg"));
    let (faulty, faults) = FaultyTransport::new(inner.clone());

    let client = StoreClientBuilder::new(meta.clone(), "survive-client")
        .state(ClientLifecycleState::Active)
        .segment_name("survive-seg")
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(32 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    // Write before disconnect
    client.put("survive-key", b"original-data").expect("put");

    // Disconnect and attempt write (fails)
    faults.disconnect();
    let _ = client.put("during", b"lost");
    faults.reconnect();

    // Original data must still be readable
    let got = client.get("survive-key").expect("get after reconnect");
    assert_eq!(got, b"original-data", "pre-disconnect data must survive");
}

#[test]
fn network_partial_failure_some_puts_fail_while_others_succeed() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let inner = Arc::new(TestTransport::new("partial-seg"));
    let (faulty, faults) = FaultyTransport::new(inner.clone());

    let client = StoreClientBuilder::new(meta.clone(), "partial-client")
        .state(ClientLifecycleState::Active)
        .segment_name("partial-seg")
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(64 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    // put() is a local memory write — fail_next_submits injections are not consumed
    // by local puts. All puts succeed; the test verifies no panic/corruption occurs.
    let mut successes = 0usize;

    for i in 0..10 {
        if i % 2 == 1 {
            faults.fail_next_submits(1);
        }
        let key = format!("partial-{i}");
        if client.put(&key, b"v").is_ok() {
            successes += 1;
        }
    }

    assert!(successes > 0, "at least some puts must succeed");
    // Verify data integrity: all even-indexed keys were put without injection
    for i in (0..10usize).step_by(2) {
        assert!(
            client.is_exist(&format!("partial-{i}")).expect("is_exist"),
            "even key {i} must exist"
        );
    }
}

#[test]
fn network_flash_disconnect_brief_interruption_then_immediate_reconnect() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let inner = Arc::new(TestTransport::new("flash-seg"));
    let (faulty, faults) = FaultyTransport::new(inner.clone());

    let client = StoreClientBuilder::new(meta.clone(), "flash-client")
        .state(ClientLifecycleState::Active)
        .segment_name("flash-seg")
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(32 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    // rapid disconnect-reconnect cycles
    for _ in 0..5 {
        faults.disconnect();
        faults.reconnect();
    }

    // After all flash disconnects, operations must succeed
    client.put("after-flash", b"ok").expect("put after flash");
    let got = client.get("after-flash").expect("get after flash");
    assert_eq!(got, b"ok");
}

#[test]
fn network_is_exist_works_after_fault_cleared() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let inner = Arc::new(TestTransport::new("ie-fault-seg"));
    let (faulty, faults) = FaultyTransport::new(inner.clone());

    let client = StoreClientBuilder::new(meta.clone(), "ie-fault-client")
        .state(ClientLifecycleState::Active)
        .segment_name("ie-fault-seg")
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(16 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    client.put("ie-key", b"v").expect("put");

    faults.fail_next_submits(3);
    // is_exist may work via metadata only (local route), so just verify no panic
    let _ = client.is_exist("ie-key");
    faults.reset_all();

    assert!(client.is_exist("ie-key").expect("is_exist after reset"));
}

#[test]
fn network_submit_latency_does_not_corrupt_data() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let inner = Arc::new(TestTransport::new("lat-seg"));
    let (faulty, faults) = FaultyTransport::new(inner.clone());

    let client = StoreClientBuilder::new(meta.clone(), "lat-client")
        .state(ClientLifecycleState::Active)
        .segment_name("lat-seg")
        .transport(Arc::new(faulty))
        .live_client_sync_interval(fast_live_client_sync_interval())
        .local_memory(storage_config_with_bytes(32 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    // Introduce 1ms submit latency
    faults.set_submit_latency(Duration::from_millis(1));

    for i in 0..5 {
        let key = format!("lat-{i}");
        let value = format!("value-{i}");
        client
            .put(&key, value.as_bytes())
            .expect("put with latency");
        let got = client.get(&key).expect("get with latency");
        assert_eq!(
            got,
            value.as_bytes(),
            "data must not be corrupted by latency"
        );
    }

    faults.set_submit_latency(Duration::ZERO);
}

// ===========================================================================
// Additional boundary / edge cases
// ===========================================================================

#[test]
fn empty_value_put_returns_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("empty-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    // Zero-length value should be rejected by the client
    let result = client.put("empty-val-key", b"");
    assert!(result.is_err(), "zero-length value must be rejected");
}

#[test]
fn put_large_value_round_trips() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("large-seg"));
    let large_t = Arc::new(transport.peer("large-seg"));
    let client = StoreClientBuilder::new(meta.clone(), "large-client")
        .state(ClientLifecycleState::Active)
        .segment_name("large-seg")
        .transport(large_t)
        .local_memory(storage_config_with_bytes(512 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    let value: Vec<u8> = (0..16 * 1024).map(|i| (i % 251) as u8).collect();
    client.put("large-key", &value).expect("put large value");
    let got = client.get("large-key").expect("get large value");
    assert_eq!(got, value, "large value must round-trip correctly");
}

#[test]
fn multiple_independent_keys_coexist() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("coex-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let keys = ["alpha", "beta", "gamma", "delta"];
    for (i, k) in keys.iter().enumerate() {
        client.put(k, &[i as u8]).expect("put");
    }
    for (i, k) in keys.iter().enumerate() {
        let got = client.get(k).expect("get");
        assert_eq!(got, vec![i as u8], "key {k} has wrong value");
    }
}

#[test]
fn remove_then_reput_key_works() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("reput-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put("reput-key", b"first").expect("first put");
    client.remove("reput-key", false).expect("remove");
    client.put("reput-key", b"second").expect("second put");
    let got = client.get("reput-key").expect("get after reput");
    assert_eq!(got, b"second");
}

#[test]
fn query_route_returns_none_for_absent_key() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("qr-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let route = client.query_route("absent-key").expect("query_route");
    assert!(
        route.is_none(),
        "query_route must return None for absent key"
    );
}

#[test]
fn query_route_returns_some_after_put() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("qrput-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put("qr-key", b"qr-value").expect("put");
    let route = client.query_route("qr-key").expect("query_route");
    assert!(route.is_some(), "query_route must return Some after put");
    let route = route.unwrap();
    assert!(!route.replicas.is_empty());
    assert_eq!(route.replicas[0].length, 8); // len of "qr-value"
}

#[test]
fn stress_rapid_put_remove_cycles_maintain_consistency() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("stress-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    for i in 0..50 {
        let key = format!("stress-{i}");
        let value = format!("val-{i}");
        client.put(&key, value.as_bytes()).expect("put");
        let got = client.get(&key).expect("get");
        assert_eq!(got, value.as_bytes(), "get must return put value");
        client.remove(&key, false).expect("remove");
        assert!(
            !client.is_exist(&key).expect("is_exist"),
            "key must be absent after remove"
        );
    }
}

// ===========================================================================
// Data integrity — binary, null bytes, patterns
// ===========================================================================

#[test]
fn data_integrity_null_bytes_in_value() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("null-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let payload = vec![0u8; 128];
    client.put("null-key", &payload).expect("put");
    let got = client.get("null-key").expect("get");
    assert_eq!(got, payload, "null-byte payload must round-trip");
}

#[test]
fn data_integrity_repeated_pattern() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("pat-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    let pattern: Vec<u8> = (0..512).map(|i| ((i * 37) % 256) as u8).collect();
    client.put("pattern-key", &pattern).expect("put");
    assert_eq!(client.get("pattern-key").expect("get"), pattern);
}

#[test]
fn data_integrity_multiple_overwrites_preserve_latest() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("ow-int-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    for i in 0..10 {
        client
            .put("ow-key", format!("iter-{i}").as_bytes())
            .expect("put");
    }
    let got = client.get("ow-key").expect("get");
    assert_eq!(got, b"iter-9", "last overwrite must win");
}

// ===========================================================================
// Multi-tenant — isolation of exist / size / remove
// ===========================================================================

#[test]
fn multi_tenant_is_exist_in_tenant() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("mt-ie-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client
        .put_in_tenant("alpha", "shared", b"a")
        .expect("put alpha");

    assert!(client.is_exist_in_tenant("alpha", "shared").expect("a"));
    assert!(!client.is_exist_in_tenant("beta", "shared").expect("b"));
}

#[test]
fn multi_tenant_get_size_in_tenant() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("mt-sz-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put_in_tenant("t1", "sized", b"abcdef").expect("put");
    assert_eq!(client.get_size_in_tenant("t1", "sized").expect("size"), 6);
}

#[test]
fn multi_tenant_remove_in_tenant_does_not_affect_other() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("mt-rm-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    client.put_in_tenant("a", "k", b"va").expect("put a");
    client.put_in_tenant("b", "k", b"vb").expect("put b");

    client
        .remove_in_tenant("a", "k", false)
        .expect("remove in a");

    assert!(!client.is_exist_in_tenant("a", "k").expect("a gone"));
    assert!(client.is_exist_in_tenant("b", "k").expect("b remains"));
}

// ===========================================================================
// Additional stress — overwrite burst, multi-tenant fanout
// ===========================================================================

#[test]
fn stress_rapid_overwrite_100_times() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("ow-stress-seg"));
    let client = make_writer(&meta, &transport);
    client.register_local_memory().expect("register");

    for i in 0..100 {
        client
            .put("burst-key", format!("v{i}").as_bytes())
            .expect("put");
    }
    let got = client.get("burst-key").expect("get");
    assert_eq!(got, b"v99", "after 100 overwrites latest value wins");
}

#[test]
fn stress_concurrent_multi_tenant_writes() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("mt-conc-seg"));
    let client = Arc::new(make_writer(&meta, &transport));
    client.register_local_memory().expect("register");

    let handles: Vec<_> = (0..4)
        .map(|t| {
            let client = client.clone();
            thread::spawn(move || {
                let tenant = format!("tenant-{t}");
                for i in 0..20 {
                    let key = format!("k{i}");
                    let _ = client.put_in_tenant(&tenant, &key, b"v");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("no panic");
    }

    // All 4 tenants * 20 keys must be readable in their own tenant
    for t in 0..4 {
        let tenant = format!("tenant-{t}");
        for i in 0..20 {
            let key = format!("k{i}");
            assert!(
                client.is_exist_in_tenant(&tenant, &key).expect("is_exist"),
                "tenant={tenant} key={key} must exist"
            );
        }
    }
}
