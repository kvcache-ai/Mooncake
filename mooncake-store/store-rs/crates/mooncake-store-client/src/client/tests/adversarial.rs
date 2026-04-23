use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, StoreError,
};
use mooncake_store_test_utils::{
    fixtures::{now_ms, test_future_expiry_ms},
    metadata::{CountingMetadataBackend, FaultyMetadataBackend},
    transport::{TestTransport, TestTransportFactory, TestTransportState},
};
use parking_lot::Mutex;
use proptest::prelude::*;

use crate::transport::{StoreTransport, StoreTransportFactory};

use super::super::align_up_u64;

// ---------------------------------------------------------------------------
// align_up_u64 property tests
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn prop_align_up_result_is_multiple_of_alignment(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let result = align_up_u64(value, alignment);
        prop_assert_eq!(result % alignment, 0u64);
    }

    #[test]
    fn prop_align_up_result_not_less_than_input(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        prop_assert!(align_up_u64(value, alignment) >= value);
    }

    #[test]
    fn prop_align_up_overshoot_less_than_one_alignment_unit(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let result = align_up_u64(value, alignment);
        prop_assert!(result - value < alignment);
    }

    #[test]
    fn prop_align_up_is_idempotent(
        value in 0u64..=1_000_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let once = align_up_u64(value, alignment);
        prop_assert_eq!(align_up_u64(once, alignment), once);
    }

    #[test]
    fn prop_align_up_zero_input_is_always_zero(log_align in 0u32..=20u32) {
        let alignment = 1u64 << log_align;
        prop_assert_eq!(align_up_u64(0, alignment), 0u64);
    }

    #[test]
    fn prop_align_up_of_exact_multiple_is_unchanged(
        multiplier in 0u64..=10_000u64,
        log_align in 0u32..=20u32,
    ) {
        let alignment = 1u64 << log_align;
        let value = multiplier.saturating_mul(alignment);
        prop_assert_eq!(align_up_u64(value, alignment), value);
    }
}

// ---------------------------------------------------------------------------
// Epoch allocation properties (public MetadataBackend API)
// ---------------------------------------------------------------------------

fn make_template(stable_id: &str) -> ClientLease {
    ClientLease {
        runtime: ClientRuntimeId::new(stable_id, ClientEpoch(0)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: now_ms() + 30_000,
    }
}

#[test]
fn epoch_alloc_strictly_monotone_for_same_stable_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let template = make_template("adv-epoch-mono");
    let mut prev = 0u64;
    for _ in 0..10u32 {
        let rid = meta
            .allocate_client_lease(&template)
            .expect("allocate_client_lease should succeed");
        assert!(
            rid.epoch.0 > prev,
            "epoch {} not strictly greater than prev {}",
            rid.epoch.0,
            prev
        );
        prev = rid.epoch.0;
    }
}

#[test]
fn epoch_alloc_independent_per_stable_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let rid_a = meta
        .allocate_client_lease(&make_template("adv-stable-a"))
        .unwrap();
    let rid_b = meta
        .allocate_client_lease(&make_template("adv-stable-b"))
        .unwrap();
    assert_eq!(
        rid_a.epoch.0, 1,
        "first alloc for stable-a should be epoch 1"
    );
    assert_eq!(
        rid_b.epoch.0, 1,
        "first alloc for stable-b should be epoch 1"
    );
}

#[test]
fn counting_backend_tracks_allocate_calls() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);
    let template = make_template("adv-counting");

    for i in 1..=5u64 {
        backend
            .allocate_client_lease(&template)
            .expect("allocate should succeed");
        assert_eq!(
            counts.allocate_client_lease.load(Ordering::Relaxed),
            i,
            "allocate counter should be {i} after {i} calls"
        );
    }
}

#[test]
fn faulty_backend_fails_after_threshold() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 3);
    let template = make_template("adv-faulty");

    for _ in 0..3 {
        assert!(
            faulty.allocate_client_lease(&template).is_ok(),
            "calls before threshold should succeed"
        );
    }
    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "call at threshold should fail"
    );
    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "calls after threshold should fail"
    );
}

// ---------------------------------------------------------------------------
// Time fixture sanity
// ---------------------------------------------------------------------------

#[test]
fn now_ms_returns_plausible_unix_timestamp() {
    let ms = now_ms();
    // After 2024-01-01 (1704067200000 ms)
    assert!(ms > 1_704_067_200_000, "now_ms {ms} is suspiciously small");
    // Before year 2100 (4102444800000 ms)
    assert!(
        ms < 4_102_444_800_000,
        "now_ms {ms} is unreasonably far in the future"
    );
}

#[test]
fn test_future_expiry_ms_is_strictly_after_now() {
    let before = now_ms();
    let expiry = test_future_expiry_ms();
    assert!(
        expiry > before,
        "expiry {expiry} should be after now {before}"
    );
}

// ---------------------------------------------------------------------------
// CountingMetadataBackend — per-method counter isolation
// ---------------------------------------------------------------------------

#[test]
fn counting_backend_tracks_list_live_clients_independent_of_other_counters() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    for i in 1..=3u64 {
        let _ = backend.list_live_clients();
        assert_eq!(counts.list_live_clients.load(Ordering::Relaxed), i);
    }
    assert_eq!(counts.publish_segment.load(Ordering::Relaxed), 0);
    assert_eq!(counts.get_object_route.load(Ordering::Relaxed), 0);
    assert_eq!(counts.allocate_client_lease.load(Ordering::Relaxed), 0);
}

#[test]
fn counting_backend_tracks_upsert_client_lease() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner.clone());
    let template = make_template("count-upsert");
    let rid = inner
        .allocate_client_lease(&template)
        .expect("pre-allocate should succeed");
    let lease = ClientLease {
        runtime: rid,
        ..template
    };

    backend
        .upsert_client_lease(&lease)
        .expect("first upsert should succeed");
    assert_eq!(counts.upsert_client_lease.load(Ordering::Relaxed), 1);
    backend
        .upsert_client_lease(&lease)
        .expect("second upsert should succeed");
    assert_eq!(counts.upsert_client_lease.load(Ordering::Relaxed), 2);
    assert_eq!(counts.list_live_clients.load(Ordering::Relaxed), 0);
}

#[test]
fn counting_backend_tracks_update_client_state() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner.clone());
    let template = make_template("count-state");
    let rid = inner
        .allocate_client_lease(&template)
        .expect("pre-allocate should succeed");

    backend
        .update_client_state(&rid, ClientLifecycleState::Draining)
        .expect("state update should succeed");
    assert_eq!(counts.update_client_state.load(Ordering::Relaxed), 1);
    assert_eq!(counts.upsert_client_lease.load(Ordering::Relaxed), 0);
}

#[test]
fn counting_backend_tracks_put_handoff_and_get_handoff() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner.clone());

    let stable = ClientStableId::new("handoff-stable");
    let rid_pred = ClientRuntimeId::new("handoff-stable", ClientEpoch(1));
    let rid_succ = ClientRuntimeId::new("handoff-stable", ClientEpoch(2));
    let plan = HandoffPlan {
        stable_id: stable.clone(),
        from: rid_pred,
        to: rid_succ,
        kind: HandoffKind::HotUpgrade,
        barrier_version: 0,
        created_at_ms: now_ms(),
        deadline_ms: None,
    };

    backend
        .put_handoff(&plan)
        .expect("put_handoff should succeed");
    assert_eq!(counts.put_handoff.load(Ordering::Relaxed), 1);

    let _ = backend.get_handoff(&stable);
    assert_eq!(counts.get_handoff.load(Ordering::Relaxed), 1);
    assert_eq!(counts.list_live_clients.load(Ordering::Relaxed), 0);
}

#[test]
fn counting_backend_all_unexercised_counters_stay_at_zero() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (backend, counts) = CountingMetadataBackend::wrap(inner);

    let _ = backend.list_live_clients();

    // Every counter except list_live_clients must remain zero.
    assert_eq!(counts.upsert_client_lease.load(Ordering::Relaxed), 0);
    assert_eq!(counts.allocate_client_lease.load(Ordering::Relaxed), 0);
    assert_eq!(counts.update_client_state.load(Ordering::Relaxed), 0);
    assert_eq!(counts.publish_segment.load(Ordering::Relaxed), 0);
    assert_eq!(counts.unpublish_segment.load(Ordering::Relaxed), 0);
    assert_eq!(counts.list_segments.load(Ordering::Relaxed), 0);
    assert_eq!(counts.get_object_route.load(Ordering::Relaxed), 0);
    assert_eq!(
        counts.compare_and_swap_object_route.load(Ordering::Relaxed),
        0
    );
    assert_eq!(counts.put_handoff.load(Ordering::Relaxed), 0);
    assert_eq!(counts.get_handoff.load(Ordering::Relaxed), 0);
}

// ---------------------------------------------------------------------------
// FaultyMetadataBackend — deeper scenarios
// ---------------------------------------------------------------------------

#[test]
fn faulty_backend_threshold_zero_fails_every_call_immediately() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 0);
    let template = make_template("faulty-zero");

    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "call 1 should fail immediately"
    );
    assert!(
        faulty.list_live_clients().is_err(),
        "call 2 should also fail"
    );
    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "call 3 should still fail"
    );
}

#[test]
fn faulty_backend_error_is_metadata_variant_with_injected_fault_message() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 0);
    let err = faulty.list_live_clients().unwrap_err();

    assert!(
        matches!(err, StoreError::Metadata(_)),
        "expected Metadata variant, got: {err:?}"
    );
    assert!(
        err.to_string().contains("injected fault"),
        "error message should contain 'injected fault', got: {err}"
    );
}

#[test]
fn faulty_backend_global_counter_spans_multiple_method_types() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 3);
    let template = make_template("faulty-multi");

    // Calls 0, 1, 2 across different methods — all succeed.
    assert!(faulty.allocate_client_lease(&template).is_ok()); // 0
    assert!(faulty.list_live_clients().is_ok()); // 1
    assert!(faulty.allocate_client_lease(&template).is_ok()); // 2
                                                              // Call 3 meets the threshold — must fail.
    assert!(faulty.list_live_clients().is_err()); // 3
}

#[test]
fn faulty_backend_wrapping_counting_backend_stops_inner_calls_after_fault() {
    use std::sync::atomic::Ordering;

    let inner = Arc::new(InMemoryMetadataBackend::new());
    let (counting, counts) = CountingMetadataBackend::wrap(inner);
    let faulty = FaultyMetadataBackend::wrap(counting, 2);
    let template = make_template("faulty-wraps-counting");

    // First 2 calls reach the inner CountingMetadataBackend.
    let _ = faulty.allocate_client_lease(&template);
    let _ = faulty.list_live_clients();
    assert_eq!(counts.allocate_client_lease.load(Ordering::Relaxed), 1);
    assert_eq!(counts.list_live_clients.load(Ordering::Relaxed), 1);

    // Third call is blocked by faulty — counting backend never sees it.
    let _ = faulty.list_live_clients();
    assert_eq!(
        counts.list_live_clients.load(Ordering::Relaxed),
        1,
        "counting backend should not be reached after fault threshold"
    );
}

#[test]
fn faulty_backend_high_threshold_allows_many_calls() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let faulty = FaultyMetadataBackend::wrap(inner, 100);
    let template = make_template("faulty-high");

    for i in 0..100u32 {
        assert!(
            faulty.allocate_client_lease(&template).is_ok(),
            "call {i} should succeed before threshold 100"
        );
    }
    assert!(
        faulty.allocate_client_lease(&template).is_err(),
        "call at threshold should fail"
    );
}

// ---------------------------------------------------------------------------
// TestTransport unit behavior (accessible via super:: as child module)
// ---------------------------------------------------------------------------

#[test]
fn test_transport_segment_name_returns_constructor_argument() {
    let transport = TestTransport::new("my-segment");
    let name = transport
        .segment_name()
        .expect("segment_name should succeed");
    assert_eq!(name, "my-segment");
}

#[test]
fn test_transport_rpc_server_address_is_loopback() {
    let transport = TestTransport::new("seg");
    let (host, _port) = transport
        .rpc_server_address()
        .expect("rpc_server_address should succeed");
    assert_eq!(host, "127.0.0.1");
}

#[test]
fn test_transport_open_nonexistent_segment_returns_not_found() {
    let transport = TestTransport::new("local");
    let err = transport.open_segment("nonexistent").unwrap_err();
    assert!(
        matches!(err, StoreError::NotFound(_)),
        "expected NotFound, got: {err:?}"
    );
}

#[test]
fn test_transport_open_segment_after_memory_allocation_succeeds() {
    let transport = TestTransport::new("local");
    let addr = transport
        .allocate_memory(128, "cpu:0")
        .expect("allocate_memory should succeed");
    let handle = transport
        .open_segment("local")
        .expect("open_segment should succeed after allocate_memory");
    let info = transport
        .get_segment_info(handle)
        .expect("get_segment_info should succeed");
    assert!(
        !info.buffers.is_empty(),
        "segment should report at least one buffer"
    );
    transport
        .close_segment(handle)
        .expect("close_segment should succeed");
    transport
        .free_memory(addr)
        .expect("free_memory should succeed");
}

#[test]
fn test_transport_allocate_and_free_batch_roundtrip() {
    let transport = TestTransport::new("seg");
    let batch_id = transport
        .allocate_batch(4)
        .expect("allocate_batch should succeed");
    transport
        .free_batch(batch_id)
        .expect("free_batch should succeed");
}

#[test]
fn test_transport_free_nonexistent_batch_returns_not_found() {
    let transport = TestTransport::new("seg");
    let err = transport.free_batch(9999).unwrap_err();
    assert!(
        matches!(err, StoreError::NotFound(_)),
        "expected NotFound for missing batch, got: {err:?}"
    );
}

#[test]
fn test_transport_allocate_batch_zero_size_is_rejected() {
    let transport = TestTransport::new("seg");
    assert!(
        transport.allocate_batch(0).is_err(),
        "zero batch size should be rejected"
    );
}

#[test]
fn test_transport_free_batch_twice_fails() {
    let transport = TestTransport::new("seg");
    let batch_id = transport.allocate_batch(4).expect("should allocate");
    transport
        .free_batch(batch_id)
        .expect("first free should succeed");
    assert!(
        transport.free_batch(batch_id).is_err(),
        "double-free should return an error"
    );
}

#[test]
fn test_transport_memory_register_unregister_roundtrip() {
    let transport = TestTransport::new("seg");
    let addr = transport
        .allocate_memory(256, "cpu:0")
        .expect("allocate_memory should succeed");
    transport
        .register_memory(addr, 256)
        .expect("register_memory should succeed");
    transport
        .unregister_memory(addr, 256)
        .expect("unregister_memory should succeed");
    transport
        .free_memory(addr)
        .expect("free_memory should succeed after unregister");
}

#[test]
fn test_transport_republish_local_metadata_increments_counter() {
    let transport = TestTransport::new("seg");
    assert_eq!(transport.republish_local_metadata_calls(), 0);
    transport
        .republish_local_metadata()
        .expect("first republish should succeed");
    transport
        .republish_local_metadata()
        .expect("second republish should succeed");
    assert_eq!(transport.republish_local_metadata_calls(), 2);
}

#[test]
fn test_transport_factory_creates_instances_sharing_segment_state() {
    let state = Arc::new(Mutex::new(TestTransportState {
        next_handle: 1,
        next_batch: 1,
        allocations: BTreeMap::new(),
        segments_by_name: BTreeMap::new(),
        segments_by_handle: BTreeMap::new(),
        live_batches: BTreeSet::new(),
        registered_memory: BTreeMap::new(),
        republish_local_metadata_calls: 0,
        fail_next_submit_segments: BTreeSet::new(),
        max_registration_bytes: None,
        supports_parallel_startup_registration: false,
        submitted_batch_sizes: Vec::new(),
        submitted_batch_bytes: Vec::new(),
        submitted_batch_hints: Vec::new(),
        submitted_request_sources: Vec::new(),
        submitted_request_opcodes: Vec::new(),
    }));

    let factory = TestTransportFactory { state };
    let t1 = factory.create("seg-a").expect("create t1 should succeed");
    let t2 = factory.create("seg-b").expect("create t2 should succeed");

    // Allocate memory through t1; t2 shares the same state (segment table).
    let _addr = t1
        .allocate_memory(64, "cpu:0")
        .expect("allocate via t1 should succeed");
    // t2 can open the segment t1 allocated because they share state.
    let handle = t2
        .open_segment("seg-a")
        .expect("t2 should see seg-a allocated by t1");
    t2.close_segment(handle)
        .expect("t2 can close t1's segment handle");
}

#[test]
fn test_transport_fail_next_submit_injects_error_for_named_segment() {
    use mooncake_transport::{Opcode, TransferRequest};

    let transport = TestTransport::new("seg-fail");
    let _addr = transport
        .allocate_memory(128, "cpu:0")
        .expect("allocate should succeed");
    let handle = transport
        .open_segment("seg-fail")
        .expect("open should succeed");
    let batch_id = transport
        .allocate_batch(1)
        .expect("batch alloc should succeed");

    transport.fail_next_submit_for_segment("seg-fail");

    let request = TransferRequest {
        opcode: Opcode::Write,
        source: _addr,
        target_id: handle,
        target_offset: _addr as u64,
        length: 8,
    };
    assert!(
        transport.submit(batch_id, &[request]).is_err(),
        "submit should fail for the injected-fault segment"
    );
}

#[test]
fn test_transport_submit_batch_size_is_tracked() {
    use mooncake_transport::{Opcode, TransferBatchHints, TransferRequest};

    let transport = TestTransport::new("seg-track");
    let addr = transport
        .allocate_memory(64, "cpu:0")
        .expect("allocate should succeed");
    let handle = transport
        .open_segment("seg-track")
        .expect("open should succeed");

    let batch = transport.allocate_batch(2).expect("batch should allocate");
    // source and target must not overlap; use a stack buffer as the write source
    let mut write_buf = [0u8; 8];
    let write_ptr = write_buf.as_mut_ptr() as *mut std::ffi::c_void;
    let base = addr as u64;
    let reqs = [
        TransferRequest {
            opcode: Opcode::Write,
            source: write_ptr,
            target_id: handle,
            target_offset: base,
            length: 4,
        },
        TransferRequest {
            opcode: Opcode::Write,
            source: write_ptr,
            target_id: handle,
            target_offset: base + 4,
            length: 4,
        },
    ];
    transport
        .submit_with_hints(batch, &reqs, &TransferBatchHints::default())
        .expect("submit should succeed");

    let sizes = transport.submitted_batch_sizes();
    assert_eq!(sizes, vec![2], "batch of 2 requests should be recorded");
    let bytes = transport.submitted_batch_bytes();
    assert_eq!(bytes, vec![8], "total 8 bytes across 2 requests");
}
