// ---------------------------------------------------------------------------
// unit_tests.rs — internal helper functions and public type contract tests
// ---------------------------------------------------------------------------
//
// These tests exercise purely local, synchronous logic:
//   - Public request/ref builder types (ObjectRef, PutRequest, ReplicationPolicy, …)
//   - Internal utility functions  (flatten_slices, scatter_into_buffers,
//     control_bind_host, align_up_u64)
//   - Lightweight in-process state types (LiveClientCache, StoreState)
//   - StoreClientBuilder rejection / validation paths

use std::sync::Arc;

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId};

use crate::{
    BandwidthShaping, ExecutionFairness, GetRequest, MultiBufferGetRequest, MultiBufferPutRequest,
    NamespaceQuota, ObjectRef, PutRequest, ReplicationPolicy, StoreClientBuilder,
};

use super::super::{
    align_up_u64, control_bind_host, flatten_slices, payload_checksum, scatter_into_buffers,
    LiveClientCache, StoreState,
};
use super::{storage_config, test_future_expiry_ms};

// ===========================================================================
// ObjectRef
// ===========================================================================

#[test]
fn object_ref_new_sets_key_and_no_tenant() {
    let r = ObjectRef::new("my-key");
    assert_eq!(r.key, "my-key");
    assert!(r.tenant.is_none());
    assert!(r.domain.is_none());
    assert!(r.object_set.is_none());
    assert!(r.qos_tier.is_none());
}

#[test]
fn object_ref_tenant_sets_tenant_field() {
    let r = ObjectRef::new("k").tenant("ns-a");
    assert_eq!(r.tenant, Some("ns-a"));
    assert_eq!(r.key, "k");
}

#[test]
fn object_ref_domain_chain() {
    let r = ObjectRef::new("k").domain("dom-1");
    assert_eq!(r.domain, Some("dom-1"));
}

#[test]
fn object_ref_object_set_chain() {
    let r = ObjectRef::new("k").object_set("set-x");
    assert_eq!(r.object_set, Some("set-x"));
}

#[test]
fn object_ref_qos_tier_chain() {
    let r = ObjectRef::new("k").qos_tier("high");
    assert_eq!(r.qos_tier, Some("high"));
}

#[test]
fn object_ref_is_copy() {
    let r1 = ObjectRef::new("k").tenant("ns");
    let r2 = r1; // Copy — original still usable
    assert_eq!(r1.key, r2.key);
    assert_eq!(r1.tenant, r2.tenant);
}

#[test]
fn object_ref_full_chain() {
    let r = ObjectRef::new("full-key")
        .tenant("t")
        .domain("d")
        .object_set("o")
        .qos_tier("q");
    assert_eq!(r.key, "full-key");
    assert_eq!(r.tenant, Some("t"));
    assert_eq!(r.domain, Some("d"));
    assert_eq!(r.object_set, Some("o"));
    assert_eq!(r.qos_tier, Some("q"));
}

// ===========================================================================
// ReplicationPolicy
// ===========================================================================

#[test]
fn replication_policy_defaults_are_none() {
    let p = ReplicationPolicy::default();
    assert_eq!(p.replica_count, None);
}

#[test]
fn replication_policy_builder_chain() {
    let p = ReplicationPolicy::new()
        .replica_count(3)
        .with_soft_pin(true)
        .prefer_local(true)
        .prefer_alloc_in_same_node(false);
    assert_eq!(p.replica_count, Some(3));
}

#[test]
fn replication_policy_preferred_segments_replaces_existing() {
    let p = ReplicationPolicy::new()
        .preferred_segment("seg-a")
        .preferred_segments(["seg-b", "seg-c"]);
    // preferred_segments replaces — only seg-b and seg-c should be present
    assert!(!p
        .preferred_segments
        .contains(&mooncake_store_core::SegmentName::new("seg-a")));
    assert!(p
        .preferred_segments
        .contains(&mooncake_store_core::SegmentName::new("seg-b")));
    assert!(p
        .preferred_segments
        .contains(&mooncake_store_core::SegmentName::new("seg-c")));
}

#[test]
fn replication_policy_preferred_storage_owners_replaces_existing() {
    let p = ReplicationPolicy::new()
        .preferred_storage_owner("owner-a")
        .preferred_storage_owners(["owner-b", "owner-c"]);
    assert!(!p.preferred_storage_owners.contains(&"owner-a".to_string()));
    assert!(p.preferred_storage_owners.contains(&"owner-b".to_string()));
}

#[test]
fn replication_policy_clone_and_equality() {
    let p1 = ReplicationPolicy::new().replica_count(2).prefer_local(true);
    let p2 = p1.clone();
    assert_eq!(p1, p2);
}

// ===========================================================================
// PutRequest
// ===========================================================================

#[test]
fn put_request_new_sets_key_and_value() {
    let data = b"hello";
    let r = PutRequest::new("key1", data);
    assert_eq!(r.key, "key1");
    assert_eq!(r.value, data);
    assert!(r.tenant.is_none());
    assert!(r.policy.is_none());
}

#[test]
fn put_request_tenant_chain() {
    let r = PutRequest::new("k", b"v").tenant("ns-b");
    assert_eq!(r.tenant, Some("ns-b"));
}

#[test]
fn put_request_replication_chain() {
    let policy = ReplicationPolicy::new().replica_count(2);
    let r = PutRequest::new("k", b"v").replication(policy.clone());
    assert_eq!(r.policy, Some(policy));
}

#[test]
fn put_request_full_chain() {
    let data = b"payload";
    let policy = ReplicationPolicy::new().replica_count(1);
    let r = PutRequest::new("key-full", data)
        .tenant("t")
        .domain("d")
        .object_set("o")
        .qos_tier("q")
        .replication(policy);
    assert_eq!(r.key, "key-full");
    assert_eq!(r.tenant, Some("t"));
    assert_eq!(r.domain, Some("d"));
    assert_eq!(r.object_set, Some("o"));
    assert_eq!(r.qos_tier, Some("q"));
}

// ===========================================================================
// GetRequest
// ===========================================================================

#[test]
fn get_request_new_sets_key_and_buffer() {
    let mut buf = vec![0u8; 32];
    let r = GetRequest::new("k", &mut buf);
    assert_eq!(r.key, "k");
    assert!(r.tenant.is_none());
}

#[test]
fn get_request_tenant_chain() {
    let mut buf = vec![0u8; 8];
    let r = GetRequest::new("k", &mut buf).tenant("ns");
    assert_eq!(r.tenant, Some("ns"));
}

// ===========================================================================
// MultiBufferPutRequest
// ===========================================================================

#[test]
fn multi_buffer_put_request_new_has_no_tenant() {
    let bufs: Vec<&[u8]> = vec![b"a", b"bb"];
    let r = MultiBufferPutRequest::new("mkey", &bufs);
    assert_eq!(r.key, "mkey");
    assert!(r.tenant.is_none());
    assert!(r.policy.is_none());
    assert_eq!(r.buffers.len(), 2);
}

#[test]
fn multi_buffer_put_request_tenant_chain() {
    let bufs: Vec<&[u8]> = vec![b"x"];
    let r = MultiBufferPutRequest::new("k", &bufs).tenant("ns-m");
    assert_eq!(r.tenant, Some("ns-m"));
}

// ===========================================================================
// MultiBufferGetRequest
// ===========================================================================

#[test]
fn multi_buffer_get_request_new_has_no_tenant() {
    let mut b1 = vec![0u8; 4];
    let mut b2 = vec![0u8; 4];
    let mut bufs: Vec<&mut [u8]> = vec![&mut b1, &mut b2];
    let r = MultiBufferGetRequest::new("gk", &mut bufs);
    assert_eq!(r.key, "gk");
    assert!(r.tenant.is_none());
}

// ===========================================================================
// flatten_slices
// ===========================================================================

#[test]
fn flatten_slices_single_buffer_returns_same_content() {
    let a = b"hello";
    let result = flatten_slices(&[a.as_slice()]);
    assert_eq!(result, b"hello");
}

#[test]
fn flatten_slices_multiple_buffers_are_concatenated() {
    let result = flatten_slices(&[b"foo".as_slice(), b"bar".as_slice(), b"baz".as_slice()]);
    assert_eq!(result, b"foobarbaz");
}

#[test]
fn flatten_slices_empty_slice_list_returns_empty() {
    let result = flatten_slices(&[]);
    assert!(result.is_empty());
}

#[test]
fn flatten_slices_includes_empty_buffers() {
    let result = flatten_slices(&[b"a".as_slice(), b"".as_slice(), b"b".as_slice()]);
    assert_eq!(result, b"ab");
}

#[test]
fn flatten_slices_single_empty_buffer() {
    let result = flatten_slices(&[b"".as_slice()]);
    assert!(result.is_empty());
}

#[test]
fn flatten_slices_preserves_binary_content() {
    let a = [0x00u8, 0xFF, 0x80, 0x01];
    let b = [0xABu8, 0xCD];
    let result = flatten_slices(&[a.as_slice(), b.as_slice()]);
    assert_eq!(result, [0x00, 0xFF, 0x80, 0x01, 0xAB, 0xCD]);
}

// ===========================================================================
// scatter_into_buffers
// ===========================================================================

#[test]
fn scatter_into_buffers_exact_fit_one_buffer() {
    let payload = b"abcde";
    let mut buf = [0u8; 5];
    scatter_into_buffers(payload, &mut [buf.as_mut_slice()]);
    assert_eq!(&buf, b"abcde");
}

#[test]
fn scatter_into_buffers_payload_shorter_than_buffer_zero_pads_remainder() {
    let payload = b"abc";
    let mut buf = [0xFFu8; 6];
    scatter_into_buffers(payload, &mut [buf.as_mut_slice()]);
    assert_eq!(&buf, &[b'a', b'b', b'c', 0x00, 0x00, 0x00]);
}

#[test]
fn scatter_into_buffers_payload_longer_truncates_at_buffer_boundary() {
    let payload = b"abcdefgh";
    let mut buf = [0u8; 4];
    scatter_into_buffers(payload, &mut [buf.as_mut_slice()]);
    assert_eq!(&buf, b"abcd");
}

#[test]
fn scatter_into_buffers_multiple_buffers_fills_sequentially() {
    let payload = b"abcdef";
    let mut b1 = [0u8; 3];
    let mut b2 = [0u8; 3];
    scatter_into_buffers(payload, &mut [b1.as_mut_slice(), b2.as_mut_slice()]);
    assert_eq!(&b1, b"abc");
    assert_eq!(&b2, b"def");
}

#[test]
fn scatter_into_buffers_empty_payload_zero_pads_all_buffers() {
    let payload = b"";
    let mut buf = [0xFFu8; 4];
    scatter_into_buffers(payload, &mut [buf.as_mut_slice()]);
    assert_eq!(&buf, &[0x00, 0x00, 0x00, 0x00]);
}

#[test]
fn scatter_into_buffers_no_buffers_is_noop() {
    let payload = b"ignored";
    scatter_into_buffers(payload, &mut []);
    // no panic, no output
}

#[test]
fn scatter_into_buffers_excess_payload_trailing_buffer_zero_padded() {
    // payload is 2 bytes but two 3-byte buffers → second gets zeros
    let payload = b"xy";
    let mut b1 = [0u8; 3];
    let mut b2 = [0xFFu8; 3];
    scatter_into_buffers(payload, &mut [b1.as_mut_slice(), b2.as_mut_slice()]);
    assert_eq!(&b1, &[b'x', b'y', 0x00]);
    assert_eq!(&b2, &[0x00, 0x00, 0x00]);
}

// ===========================================================================
// control_bind_host
// ===========================================================================

#[test]
fn control_bind_host_empty_address_returns_loopback() {
    assert_eq!(control_bind_host(""), "127.0.0.1");
}

#[test]
fn control_bind_host_wildcard_ipv4_returns_loopback() {
    assert_eq!(control_bind_host("0.0.0.0:7001"), "127.0.0.1");
}

#[test]
fn control_bind_host_wildcard_ipv6_returns_loopback() {
    assert_eq!(control_bind_host(":::7001"), "127.0.0.1");
}

#[test]
fn control_bind_host_no_port_returns_loopback() {
    // rsplit_once(':') fails → fall through to default
    assert_eq!(control_bind_host("localhost"), "127.0.0.1");
}

#[test]
fn control_bind_host_localhost_colon_port() {
    assert_eq!(control_bind_host("localhost:7001"), "localhost");
}

#[test]
fn control_bind_host_explicit_ip_colon_port() {
    assert_eq!(control_bind_host("192.168.1.100:8080"), "192.168.1.100");
}

#[test]
fn control_bind_host_hostname_colon_port() {
    assert_eq!(
        control_bind_host("myhost.example.com:9000"),
        "myhost.example.com"
    );
}

// ===========================================================================
// align_up_u64
// ===========================================================================

#[test]
fn align_up_already_aligned_is_unchanged() {
    assert_eq!(align_up_u64(4096, 4096), 4096);
}

#[test]
fn align_up_zero_is_always_zero() {
    assert_eq!(align_up_u64(0, 4096), 0);
    assert_eq!(align_up_u64(0, 1), 0);
    assert_eq!(align_up_u64(0, 512), 0);
}

#[test]
fn align_up_one_alignment_is_identity() {
    assert_eq!(align_up_u64(12345, 1), 12345);
}

#[test]
fn align_up_needs_rounding() {
    assert_eq!(align_up_u64(1, 4096), 4096);
    assert_eq!(align_up_u64(4097, 4096), 8192);
}

#[test]
fn align_up_exact_multiple_unchanged() {
    assert_eq!(align_up_u64(8192, 4096), 8192);
}

#[test]
fn align_up_small_alignment() {
    assert_eq!(align_up_u64(7, 4), 8);
    assert_eq!(align_up_u64(8, 4), 8);
    assert_eq!(align_up_u64(9, 4), 12);
}

#[test]
fn align_up_power_of_two_roundtrip() {
    for log in 0..=20u32 {
        let alignment = 1u64 << log;
        for delta in 0u64..alignment.min(16) {
            let value = alignment * 3 + delta;
            let result = align_up_u64(value, alignment);
            assert!(result >= value, "result {result} < value {value}");
            assert_eq!(
                result % alignment,
                0,
                "result {result} not aligned to {alignment}"
            );
            assert!(result - value < alignment, "overshot by >= alignment");
        }
    }
}

// ===========================================================================
// LiveClientCache
// ===========================================================================

#[test]
fn live_client_cache_default_snapshot_is_none() {
    let cache = LiveClientCache::default();
    assert!(
        cache.snapshot().is_none(),
        "fresh cache must have None snapshot"
    );
}

#[test]
fn live_client_cache_store_then_snapshot_returns_list() {
    let mut cache = LiveClientCache::default();
    let lease = ClientLease {
        runtime: ClientRuntimeId::new("s", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: mooncake_store_core::CompatibilityDescriptor::default(),
        endpoints: mooncake_store_core::ClientEndpointSet::default(),
        expires_at_ms: u64::MAX,
    };
    cache.store(vec![lease.clone()]);
    let snap = cache.snapshot().expect("snapshot must be Some after store");
    assert!(!snap.is_empty(), "snapshot should not be empty");
}

#[test]
fn live_client_cache_store_overwrites_previous() {
    let mut cache = LiveClientCache::default();
    let lease_a = ClientLease {
        runtime: ClientRuntimeId::new("a", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: mooncake_store_core::CompatibilityDescriptor::default(),
        endpoints: mooncake_store_core::ClientEndpointSet::default(),
        expires_at_ms: u64::MAX,
    };
    let lease_b = ClientLease {
        runtime: ClientRuntimeId::new("b", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: mooncake_store_core::CompatibilityDescriptor::default(),
        endpoints: mooncake_store_core::ClientEndpointSet::default(),
        expires_at_ms: u64::MAX,
    };
    cache.store(vec![lease_a]);
    cache.store(vec![lease_b.clone()]);
    let snap = cache.snapshot().expect("must be Some");
    // second store wins; the second lease must be present
    assert!(
        snap.iter().any(|l| l.runtime.stable_id.0 == "b"),
        "second store should overwrite first"
    );
}

#[test]
fn live_client_cache_store_empty_list_produces_some_empty_snapshot() {
    let mut cache = LiveClientCache::default();
    cache.store(vec![]);
    let snap = cache.snapshot();
    assert!(
        snap.is_some(),
        "snapshot must be Some even after storing empty list"
    );
}

// ===========================================================================
// StoreState
// ===========================================================================

#[test]
fn store_state_default_has_no_memory() {
    let state = StoreState::default();
    assert!(
        state.memory.is_none(),
        "default StoreState must have no local memory"
    );
}

#[test]
fn store_state_default_buffer_is_registered_returns_false() {
    let state = StoreState::default();
    let addr = 0x1234_5678usize as *mut std::ffi::c_void;
    assert!(!state.buffer_is_registered(addr, 64));
}

// ===========================================================================
// StoreClientBuilder validation
// ===========================================================================

#[test]
fn store_client_builder_rejects_empty_default_tenant() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let result = StoreClientBuilder::new(meta, "valid-stable-id")
        .tenant("")
        .local_memory(storage_config())
        .build(test_future_expiry_ms());
    assert!(result.is_err(), "empty default_tenant must be rejected");
    if let Err(e) = result {
        assert!(
            matches!(e, mooncake_store_core::StoreError::InvalidState(_)),
            "expected InvalidState, got a different variant"
        );
    }
}

#[test]
fn store_client_builder_basic_build_succeeds() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(meta, "test-client")
        .local_memory(storage_config())
        .build(test_future_expiry_ms());
    assert!(client.is_ok(), "basic build should succeed");
}

#[test]
fn store_client_builder_segment_name_is_stored_in_runtime_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(meta, "seg-test")
        .segment_name("my-segment")
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("build should succeed");
    // runtime_id should reference the stable_id we provided
    assert_eq!(client.runtime_id().stable_id.0, "seg-test");
}

#[test]
fn store_client_builder_rpc_address_label_is_stored() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(meta, "rpc-test")
        .rpc_address("127.0.0.1:9999")
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("build should succeed");
    let lease = client.lease();
    assert!(
        lease.endpoints.rpc_address.contains("9999"),
        "rpc address should be stored in lease endpoints"
    );
}

#[test]
fn store_client_builder_state_is_reflected_in_lease() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(meta, "state-test")
        .state(ClientLifecycleState::Standby)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("build should succeed");
    let lease = client.lease();
    assert_eq!(lease.state, ClientLifecycleState::Standby);
}

// ===========================================================================
// StoreError — Display and static bounds
// ===========================================================================

#[test]
fn error_not_found_display() {
    let err = mooncake_store_core::StoreError::NotFound("the-key".to_string());
    let s = format!("{err}");
    assert!(s.contains("not found"), "Display must mention 'not found'");
    assert!(
        s.contains("the-key"),
        "Display must include the key payload"
    );
}

#[test]
fn error_conflict_display() {
    let err = mooncake_store_core::StoreError::Conflict("version mismatch".to_string());
    let s = format!("{err}");
    assert!(s.contains("conflict"));
    assert!(s.contains("version mismatch"));
}

#[test]
fn error_stale_epoch_display() {
    let err = mooncake_store_core::StoreError::StaleEpoch("epoch=1".to_string());
    let s = format!("{err}");
    assert!(s.contains("stale epoch"));
    assert!(s.contains("epoch=1"));
}

#[test]
fn error_transport_display() {
    let err = mooncake_store_core::StoreError::Transport("broken pipe".to_string());
    let s = format!("{err}");
    assert!(s.contains("transport"));
    assert!(s.contains("broken pipe"));
}

#[test]
fn error_metadata_display() {
    let err = mooncake_store_core::StoreError::Metadata("etcd unavailable".to_string());
    let s = format!("{err}");
    assert!(s.contains("metadata"));
    assert!(s.contains("etcd unavailable"));
}

#[test]
fn error_allocator_display() {
    let err = mooncake_store_core::StoreError::Allocator("out of space".to_string());
    let s = format!("{err}");
    assert!(s.contains("allocator"));
    assert!(s.contains("out of space"));
}

#[test]
fn error_invalid_state_display() {
    let err = mooncake_store_core::StoreError::InvalidState("draining".to_string());
    let s = format!("{err}");
    assert!(s.contains("invalid state"));
    assert!(s.contains("draining"));
}

#[test]
fn error_unsupported_display() {
    let err = mooncake_store_core::StoreError::Unsupported("evacuate".to_string());
    let s = format!("{err}");
    assert!(s.contains("unsupported"));
    assert!(s.contains("evacuate"));
}

#[test]
fn error_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<mooncake_store_core::StoreError>();
}

#[test]
fn error_propagates_through_question_mark() {
    fn inner() -> Result<(), mooncake_store_core::StoreError> {
        Err(mooncake_store_core::StoreError::NotFound("k".to_string()))
    }
    fn outer() -> Result<(), mooncake_store_core::StoreError> {
        inner()?;
        Ok(())
    }
    match outer() {
        Err(mooncake_store_core::StoreError::NotFound(k)) => assert_eq!(k, "k"),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

// ===========================================================================
// Core types — construction, ordering, defaults
// ===========================================================================

#[test]
fn core_client_epoch_ordering() {
    assert!(ClientEpoch(1) < ClientEpoch(2));
    assert!(ClientEpoch(10) > ClientEpoch(5));
    assert_eq!(ClientEpoch(7), ClientEpoch(7));
}

#[test]
fn core_route_version_next_increments() {
    use mooncake_store_core::RouteVersion;
    assert_eq!(RouteVersion(0).next(), RouteVersion(1));
    assert_eq!(RouteVersion(99).next(), RouteVersion(100));
}

#[test]
fn core_route_version_next_saturates() {
    use mooncake_store_core::RouteVersion;
    let maxed = RouteVersion(u64::MAX);
    assert_eq!(maxed.next(), maxed, "next() must saturate at u64::MAX");
}

#[test]
fn core_route_version_default_is_zero() {
    use mooncake_store_core::RouteVersion;
    assert_eq!(RouteVersion::default(), RouteVersion(0));
}

#[test]
fn core_segment_name_construction() {
    use mooncake_store_core::SegmentName;
    let n = SegmentName("my-seg".to_string());
    assert_eq!(n.0, "my-seg");
    let m = SegmentName("my-seg".to_string());
    assert_eq!(n, m);
}

#[test]
fn core_client_runtime_id_construction() {
    let r = ClientRuntimeId::new("stable-1", ClientEpoch(3));
    assert_eq!(r.stable_id.0, "stable-1");
    assert_eq!(r.epoch, ClientEpoch(3));
}

#[test]
fn core_client_stable_id_construction() {
    use mooncake_store_core::ClientStableId;
    let s = ClientStableId::new("foo");
    assert_eq!(s.0, "foo");
    let t = ClientStableId("foo".to_string());
    assert_eq!(s, t);
}

#[test]
fn core_handoff_kind_variants_distinct() {
    use mooncake_store_core::HandoffKind;
    assert_ne!(HandoffKind::HotUpgrade, HandoffKind::HotStandbyPromotion);
    assert_ne!(HandoffKind::HotUpgrade, HandoffKind::GracefulDrain);
    assert_ne!(HandoffKind::HotStandbyPromotion, HandoffKind::GracefulDrain);
}

#[test]
fn core_client_lifecycle_state_variants_distinct() {
    assert_ne!(ClientLifecycleState::Active, ClientLifecycleState::Standby);
    assert_ne!(ClientLifecycleState::Active, ClientLifecycleState::Draining);
    assert_ne!(
        ClientLifecycleState::Standby,
        ClientLifecycleState::Draining
    );
}

#[test]
fn core_segment_lifecycle_state_variants_distinct() {
    use mooncake_store_core::SegmentLifecycleState;
    assert_ne!(
        SegmentLifecycleState::Active,
        SegmentLifecycleState::Draining
    );
    assert_ne!(
        SegmentLifecycleState::Draining,
        SegmentLifecycleState::Retired
    );
    assert_ne!(
        SegmentLifecycleState::Active,
        SegmentLifecycleState::Retired
    );
}

// ===========================================================================
// Copy/Clone round-trip (cheap structural equality)
// ===========================================================================

#[test]
fn copy_roundtrip_route_version() {
    use mooncake_store_core::RouteVersion;
    let v = RouteVersion(42);
    let cloned = v;
    assert_eq!(v, cloned);
}

#[test]
fn clone_roundtrip_segment_name() {
    use mooncake_store_core::SegmentName;
    let n = SegmentName("alpha".to_string());
    let c = n.clone();
    assert_eq!(n, c);
}

#[test]
fn copy_roundtrip_handoff_kind() {
    use mooncake_store_core::HandoffKind;
    let k = HandoffKind::HotUpgrade;
    let c = k;
    assert_eq!(k, c);
}

#[test]
fn copy_roundtrip_client_lifecycle_state() {
    let s = ClientLifecycleState::Standby;
    let c = s;
    assert_eq!(s, c);
}

// ===========================================================================
// Additional helper-function edge cases
// ===========================================================================

#[test]
fn helper_now_ms_returns_reasonable_timestamp() {
    // now_ms should be a Unix-millis value — strictly greater than year 2000.
    let t = super::super::now_ms();
    assert!(
        t > 946_684_800_000u64,
        "now_ms must be a real Unix timestamp"
    );
}

#[test]
fn helper_now_ms_is_monotonically_non_decreasing() {
    let a = super::super::now_ms();
    let b = super::super::now_ms();
    assert!(b >= a, "second call must not go backwards");
}

// ===========================================================================
// payload_checksum invariants
// ===========================================================================

#[test]
fn payload_checksum_empty_payload_is_deterministic() {
    assert_eq!(payload_checksum(&[]), payload_checksum(&[]));
}

#[test]
fn payload_checksum_distinguishes_empty_and_non_empty() {
    assert_ne!(payload_checksum(&[]), payload_checksum(&[0u8]));
}

#[test]
fn payload_checksum_single_byte_is_deterministic() {
    let a = payload_checksum(&[0u8]);
    let b = payload_checksum(&[0u8]);
    assert_eq!(a, b, "same input must yield same checksum");
    let different = payload_checksum(&[1u8]);
    assert_ne!(
        a, different,
        "different input must yield different checksum"
    );
}

#[test]
fn payload_checksum_all_0xff_bytes_is_deterministic() {
    let payload = vec![0xFFu8; 64];
    let first = payload_checksum(&payload);
    let second = payload_checksum(&payload);
    assert_eq!(first, second);
}

#[test]
fn payload_checksum_is_order_sensitive() {
    let ab = payload_checksum(&[0x01, 0x02]);
    let ba = payload_checksum(&[0x02, 0x01]);
    assert_ne!(ab, ba, "checksum must be sensitive to byte order");
}

#[test]
fn payload_checksum_length_matters() {
    let one_byte = payload_checksum(&[0xAAu8]);
    let two_byte = payload_checksum(&[0xAAu8, 0xAAu8]);
    assert_ne!(
        one_byte, two_byte,
        "longer payload must produce distinct checksum"
    );
}

// ===========================================================================
// Type zero-clamp: BandwidthShaping / ExecutionFairness / NamespaceQuota
// ===========================================================================

#[test]
fn bandwidth_shaping_max_remote_batch_bytes_clamps_zero_to_one() {
    let bs = BandwidthShaping::new().max_remote_batch_bytes(0);
    assert_eq!(bs.max_remote_batch_bytes, Some(1));
}

#[test]
fn bandwidth_shaping_max_remote_batch_burst_items_clamps_zero_to_one() {
    let bs = BandwidthShaping::new().max_remote_batch_burst_items(0);
    assert_eq!(bs.max_remote_batch_burst_items, Some(1));
}

#[test]
fn bandwidth_shaping_max_inflight_bytes_per_batch_clamps_zero_to_one() {
    let bs = BandwidthShaping::new().max_inflight_bytes_per_batch(0);
    assert_eq!(bs.max_inflight_bytes_per_batch, Some(1));
}

#[test]
fn bandwidth_shaping_preserves_nonzero_values() {
    let bs = BandwidthShaping::new()
        .max_remote_batch_bytes(1024)
        .max_remote_batch_burst_items(8)
        .max_inflight_bytes_per_batch(1_048_576);
    assert_eq!(bs.max_remote_batch_bytes, Some(1024));
    assert_eq!(bs.max_remote_batch_burst_items, Some(8));
    assert_eq!(bs.max_inflight_bytes_per_batch, Some(1_048_576));
}

#[test]
fn execution_fairness_max_remote_batch_items_clamps_zero_to_one() {
    let ef = ExecutionFairness::new().max_remote_batch_items_per_tenant(0);
    assert_eq!(ef.max_remote_batch_items_per_tenant, Some(1));
}

#[test]
fn namespace_quota_max_bytes_clamps_zero_to_one() {
    let nq = NamespaceQuota::new().max_bytes(0);
    assert_eq!(nq.max_bytes, Some(1));
}

#[test]
fn namespace_quota_max_objects_clamps_zero_to_one() {
    let nq = NamespaceQuota::new().max_objects(0);
    assert_eq!(nq.max_objects, Some(1));
}

#[test]
fn namespace_quota_preserves_nonzero_values() {
    let nq = NamespaceQuota::new().max_bytes(1_000_000).max_objects(42);
    assert_eq!(nq.max_bytes, Some(1_000_000));
    assert_eq!(nq.max_objects, Some(42));
}

// ===========================================================================
// control_bind_host — IPv6 and injection edge cases
// ===========================================================================

#[test]
fn control_bind_host_ipv6_loopback_preserves_bracketed_host() {
    assert_eq!(control_bind_host("[::1]:8080"), "[::1]");
}

#[test]
fn control_bind_host_port_only_falls_back_to_loopback() {
    // An empty host before the colon is rejected; host collapses to loopback.
    assert_eq!(control_bind_host(":8080"), "127.0.0.1");
}

#[test]
fn control_bind_host_rejects_wildcard_ipv4_in_favor_of_loopback() {
    assert_eq!(control_bind_host("0.0.0.0:8080"), "127.0.0.1");
}
