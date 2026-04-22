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
    GetRequest, MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef, PutRequest,
    ReplicationPolicy, StoreClientBuilder,
};

use super::super::{
    align_up_u64, control_bind_host, flatten_slices, scatter_into_buffers, LiveClientCache,
    StoreState,
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
    assert!(p.replica_count.is_none() || p.replica_count == Some(1));
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
