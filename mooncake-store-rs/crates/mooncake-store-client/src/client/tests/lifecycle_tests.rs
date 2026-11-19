// ---------------------------------------------------------------------------
// lifecycle_tests.rs — client and segment lifecycle state machines
// ---------------------------------------------------------------------------
//
// Covers:
//   - Hot-upgrade handoff (plan, discover, activate) — targeting, expired
//     deadline, wrong state/target rejection
//   - Client lifecycle transitions (Active / Standby / Draining)
//   - Segment lifecycle (mount, expand, drain, retire)
//   - Evacuation entry-point for a single-node empty store

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, HandoffKind, MetadataBackend, SegmentLifecycleState,
    SegmentName, StoreError,
};
use mooncake_store_rs_metadata::InMemoryMetadataBackend;
use mooncake_store_test_utils::transport::{FaultConfig, FaultyTransport, TestTransport};

use crate::{
    memory::{with_test_numa_locations, LocalMemoryConfig},
    MooncakeCompatibilityFacade, StoreClient, StoreClientBuilder,
};

use super::{
    storage_config, storage_config_with_bytes, test_future_expiry_ms, RecoverableMetadataBackend,
};

// ===========================================================================
// Helpers
// ===========================================================================

/// Build a minimal client with no local storage — enough for lifecycle
/// state-machine tests that do not touch segments or data.
fn build_metadata_only_client(
    meta: &Arc<InMemoryMetadataBackend>,
    stable_id: &str,
    state: ClientLifecycleState,
    segment_name: &str,
) -> StoreClient {
    StoreClientBuilder::new(meta.clone(), stable_id)
        .state(state)
        .rpc_address("127.0.0.1:0")
        .segment_name(segment_name)
        .build(test_future_expiry_ms())
        .expect("client build")
}

/// Build a client with local memory so that segment operations are testable.
fn build_storage_client(
    meta: &Arc<InMemoryMetadataBackend>,
    stable_id: &str,
    storage_bytes: usize,
) -> StoreClient {
    build_storage_client_with_config(meta, stable_id, storage_config_with_bytes(storage_bytes))
}

fn build_storage_client_with_config(
    meta: &Arc<InMemoryMetadataBackend>,
    stable_id: &str,
    local_memory: LocalMemoryConfig,
) -> StoreClient {
    let seg = format!("{stable_id}-seg");
    let transport = Arc::new(TestTransport::new(&seg));
    build_storage_client_with_transport(meta, stable_id, local_memory, transport)
}

fn build_storage_client_with_transport(
    meta: &Arc<InMemoryMetadataBackend>,
    stable_id: &str,
    local_memory: LocalMemoryConfig,
    transport: Arc<TestTransport>,
) -> StoreClient {
    let seg = format!("{stable_id}-seg");
    let factory = transport.factory();
    let t = Arc::new(transport.peer(&seg));
    let c = StoreClientBuilder::new(meta.clone(), stable_id)
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:0")
        .segment_name(&seg)
        .transport(t)
        .transport_factory(factory)
        .local_memory(local_memory)
        .build(test_future_expiry_ms())
        .expect("build");
    c.register_local_memory().expect("register");
    c
}

fn build_retirement_client(meta: Arc<RecoverableMetadataBackend>, stable_id: &str) -> StoreClient {
    let segment = format!("{stable_id}-seg");
    let transport = Arc::new(TestTransport::new(&segment));
    let factory = transport.factory();
    let client = StoreClientBuilder::new(meta, stable_id)
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:0")
        .segment_name(&segment)
        .transport(Arc::new(transport.peer(&segment)))
        .transport_factory(factory)
        .local_memory(storage_config_with_bytes(8 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");
    client
}

fn build_faulty_retirement_client(
    meta: Arc<dyn MetadataBackend>,
    stable_id: &str,
) -> (StoreClient, Arc<FaultConfig>, Arc<TestTransport>) {
    let segment = format!("{stable_id}-seg");
    let transport = Arc::new(TestTransport::new(&segment));
    let factory = transport.factory();
    let (faulty_transport, faults) = FaultyTransport::new(transport.clone());
    let client = StoreClientBuilder::new(meta, stable_id)
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:0")
        .segment_name(&segment)
        .transport(Arc::new(faulty_transport))
        .transport_factory(factory)
        .local_memory(storage_config_with_bytes(8 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");
    (client, faults, transport)
}

fn hold_region(
    client: &StoreClient,
    segment: &SegmentName,
) -> Arc<crate::memory::RegisteredRegion> {
    let state = client.state.lock();
    state
        .memory_ref()
        .expect("memory should be registered")
        .storage_region(segment)
        .expect("segment should exist")
}

// ===========================================================================
// Hot upgrade — plan_handoff / find_hot_upgrade_successor / activate_if_targeted_handoff
// ===========================================================================

#[test]
fn plan_handoff_stores_all_fields_correctly() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client = build_metadata_only_client(
        &meta,
        "handoff-fields",
        ClientLifecycleState::Active,
        "handoff-fields-seg",
    );

    client.enter_draining().expect("drain");

    let plan = client
        .plan_handoff(
            ClientEpoch(4),
            HandoffKind::HotStandbyPromotion,
            42,
            12_345,
            Some(99_999),
        )
        .expect("plan_handoff");

    assert_eq!(plan.stable_id.0, "handoff-fields");
    assert_eq!(plan.from.epoch, ClientEpoch(1));
    assert_eq!(plan.to.epoch, ClientEpoch(4));
    assert_eq!(plan.kind, HandoffKind::HotStandbyPromotion);
    assert_eq!(plan.barrier_version, 42);
    assert_eq!(plan.created_at_ms, 12_345);
    assert_eq!(plan.deadline_ms, Some(99_999));
}

#[test]
fn handoff_plan_fields_are_preserved_correctly() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client = build_metadata_only_client(
        &meta,
        "handoff-preserve",
        ClientLifecycleState::Active,
        "handoff-preserve-seg",
    );
    client.enter_draining().expect("drain");

    let plan = client
        .plan_handoff(ClientEpoch(2), HandoffKind::HotUpgrade, 7, 100, Some(1_000))
        .expect("plan_handoff");

    let stored = meta
        .get_handoff(&plan.stable_id)
        .expect("get_handoff")
        .expect("exists");
    assert_eq!(stored, plan, "stored plan must equal returned plan");
}

#[test]
fn find_hot_upgrade_successor_picks_highest_epoch() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let predecessor = build_metadata_only_client(
        &meta,
        "hu-highest",
        ClientLifecycleState::Active,
        "hu-highest-v1",
    );
    let _succ1 = build_metadata_only_client(
        &meta,
        "hu-highest",
        ClientLifecycleState::Standby,
        "hu-highest-v2",
    );
    let succ2 = build_metadata_only_client(
        &meta,
        "hu-highest",
        ClientLifecycleState::Standby,
        "hu-highest-v3",
    );

    let found = predecessor
        .find_hot_upgrade_successor()
        .expect("find")
        .expect("some");
    assert_eq!(
        found.runtime,
        *succ2.runtime_id(),
        "must select highest-epoch standby"
    );
}

#[test]
fn find_hot_upgrade_successor_ignores_different_stable_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let predecessor = build_metadata_only_client(
        &meta,
        "hu-diff-a",
        ClientLifecycleState::Active,
        "hu-diff-a-seg",
    );
    let _other = build_metadata_only_client(
        &meta,
        "hu-diff-b",
        ClientLifecycleState::Standby,
        "hu-diff-b-seg",
    );

    let found = predecessor.find_hot_upgrade_successor().expect("find");
    assert!(found.is_none(), "different stable_id must not match");
}

#[test]
fn find_hot_upgrade_successor_returns_none_when_no_successor() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let predecessor = build_metadata_only_client(
        &meta,
        "hu-none",
        ClientLifecycleState::Active,
        "hu-none-seg",
    );

    let found = predecessor.find_hot_upgrade_successor().expect("find");
    assert!(found.is_none());
}

#[test]
fn activate_if_targeted_handoff_ignores_expired_deadline() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut predecessor = build_metadata_only_client(
        &meta,
        "hu-expired",
        ClientLifecycleState::Active,
        "hu-expired-v1",
    );
    let mut successor = build_metadata_only_client(
        &meta,
        "hu-expired",
        ClientLifecycleState::Standby,
        "hu-expired-v2",
    );

    predecessor.enter_draining().expect("drain");
    // deadline_ms = 0 → already in the past
    predecessor
        .plan_handoff(
            successor.runtime_id().epoch,
            HandoffKind::HotUpgrade,
            1,
            0,
            Some(0),
        )
        .expect("plan");

    let result = successor.activate_if_targeted_handoff().expect("check");
    assert!(
        result.is_none(),
        "expired-deadline handoff must not activate"
    );
    assert_eq!(successor.lease().state, ClientLifecycleState::Standby);
}

#[test]
fn activate_if_targeted_handoff_ignores_non_standby_state() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut predecessor = build_metadata_only_client(
        &meta,
        "hu-nonstandby",
        ClientLifecycleState::Active,
        "hu-nonstandby-v1",
    );
    // Successor starts Active (not Standby)
    let mut successor = build_metadata_only_client(
        &meta,
        "hu-nonstandby",
        ClientLifecycleState::Active,
        "hu-nonstandby-v2",
    );

    predecessor.enter_draining().expect("drain");
    predecessor
        .plan_handoff(
            successor.runtime_id().epoch,
            HandoffKind::HotUpgrade,
            1,
            0,
            None,
        )
        .expect("plan");

    let result = successor.activate_if_targeted_handoff().expect("check");
    assert!(result.is_none(), "non-Standby successor must not activate");
}

#[test]
fn activate_if_targeted_handoff_ignores_wrong_target() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut predecessor = build_metadata_only_client(
        &meta,
        "hu-wrong",
        ClientLifecycleState::Active,
        "hu-wrong-v1",
    );
    let targeted = build_metadata_only_client(
        &meta,
        "hu-wrong",
        ClientLifecycleState::Standby,
        "hu-wrong-v2",
    );
    let mut non_targeted = build_metadata_only_client(
        &meta,
        "hu-wrong",
        ClientLifecycleState::Standby,
        "hu-wrong-v3",
    );

    predecessor.enter_draining().expect("drain");
    predecessor
        .plan_handoff(
            targeted.runtime_id().epoch,
            HandoffKind::HotUpgrade,
            1,
            0,
            None,
        )
        .expect("plan");

    let result = non_targeted.activate_if_targeted_handoff().expect("check");
    assert!(
        result.is_none(),
        "wrong-epoch successor must not consume handoff"
    );
    assert_eq!(non_targeted.lease().state, ClientLifecycleState::Standby);
}

// ===========================================================================
// Lifecycle state machine transitions
// ===========================================================================

#[test]
fn lifecycle_standby_client_can_be_built() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_metadata_only_client(
        &meta,
        "lc-standby",
        ClientLifecycleState::Standby,
        "lc-standby-seg",
    );
    assert_eq!(client.lease().state, ClientLifecycleState::Standby);
}

#[test]
fn lifecycle_active_to_draining() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client =
        build_metadata_only_client(&meta, "lc-a2d", ClientLifecycleState::Active, "lc-a2d-seg");

    client.enter_draining().expect("drain");
    assert_eq!(client.lease().state, ClientLifecycleState::Draining);
}

#[test]
fn lifecycle_active_to_standby() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client =
        build_metadata_only_client(&meta, "lc-a2s", ClientLifecycleState::Active, "lc-a2s-seg");

    client.enter_standby().expect("standby");
    assert_eq!(client.lease().state, ClientLifecycleState::Standby);
}

#[test]
fn lifecycle_standby_to_active() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client =
        build_metadata_only_client(&meta, "lc-s2a", ClientLifecycleState::Standby, "lc-s2a-seg");

    client.activate().expect("activate");
    assert_eq!(client.lease().state, ClientLifecycleState::Active);
}

#[test]
fn lifecycle_standby_to_draining() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client =
        build_metadata_only_client(&meta, "lc-s2d", ClientLifecycleState::Standby, "lc-s2d-seg");

    client.enter_draining().expect("drain");
    assert_eq!(client.lease().state, ClientLifecycleState::Draining);
}

#[test]
fn lifecycle_full_state_cycle() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client = build_metadata_only_client(
        &meta,
        "lc-cycle",
        ClientLifecycleState::Active,
        "lc-cycle-seg",
    );

    client.enter_standby().expect("to standby");
    assert_eq!(client.lease().state, ClientLifecycleState::Standby);
    client.activate().expect("back to active");
    assert_eq!(client.lease().state, ClientLifecycleState::Active);
    client.enter_draining().expect("to draining");
    assert_eq!(client.lease().state, ClientLifecycleState::Draining);
}

#[test]
fn lifecycle_multiple_epochs_for_same_stable_id() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let c1 = build_metadata_only_client(
        &meta,
        "lc-multi",
        ClientLifecycleState::Active,
        "lc-multi-v1",
    );
    let c2 = build_metadata_only_client(
        &meta,
        "lc-multi",
        ClientLifecycleState::Standby,
        "lc-multi-v2",
    );
    let c3 = build_metadata_only_client(
        &meta,
        "lc-multi",
        ClientLifecycleState::Standby,
        "lc-multi-v3",
    );

    // Backend auto-allocates strictly-increasing epochs
    assert_eq!(c1.runtime_id().epoch, ClientEpoch(1));
    assert_eq!(c2.runtime_id().epoch, ClientEpoch(2));
    assert_eq!(c3.runtime_id().epoch, ClientEpoch(3));
}

#[test]
fn lifecycle_heartbeat_preserves_state() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client =
        build_metadata_only_client(&meta, "lc-hb", ClientLifecycleState::Active, "lc-hb-seg");

    client.heartbeat(test_future_expiry_ms()).expect("hb");
    assert_eq!(client.lease().state, ClientLifecycleState::Active);
}

#[test]
fn enter_draining_is_idempotent() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client = build_metadata_only_client(
        &meta,
        "lc-idem",
        ClientLifecycleState::Active,
        "lc-idem-seg",
    );

    client.enter_draining().expect("first drain");
    client.enter_draining().expect("second drain must succeed");
    assert_eq!(client.lease().state, ClientLifecycleState::Draining);
}

#[test]
fn draining_predecessor_rejects_new_writes() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("drain-reject-seg"));
    let t = Arc::new(transport.peer("drain-reject-seg"));
    let mut client = StoreClientBuilder::new(meta.clone(), "drain-reject")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:0")
        .segment_name("drain-reject-seg")
        .transport(t)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    client.put("pre-drain", b"ok").expect("put before drain");
    client.enter_draining().expect("drain");

    let err = client
        .put("post-drain", b"late")
        .expect_err("writes during drain must be rejected");
    assert!(matches!(err, StoreError::InvalidState(_)));
}

#[test]
fn data_readable_after_entering_draining_state() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("drain-read-seg"));
    let t = Arc::new(transport.peer("drain-read-seg"));
    let mut client = StoreClientBuilder::new(meta.clone(), "drain-read")
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:0")
        .segment_name("drain-read-seg")
        .transport(t)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("build");
    client.register_local_memory().expect("register");

    client.put("read-me", b"hello").expect("put");
    client.enter_draining().expect("drain");

    let got = client.get("read-me").expect("get during drain");
    assert_eq!(got, b"hello");
}

#[test]
fn runtime_state_returns_own_state() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client =
        build_metadata_only_client(&meta, "rs-own", ClientLifecycleState::Active, "rs-own-seg");

    let state = client
        .runtime_state(client.runtime_id())
        .expect("runtime_state");
    assert_eq!(state, Some(ClientLifecycleState::Active));
}

#[test]
fn runtime_state_returns_none_for_unknown_runtime() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_metadata_only_client(
        &meta,
        "rs-unknown",
        ClientLifecycleState::Active,
        "rs-unknown-seg",
    );

    let phantom = mooncake_store_core::ClientRuntimeId::new("no-such-id", ClientEpoch(42));
    let state = client.runtime_state(&phantom).expect("runtime_state");
    assert!(state.is_none());
}

// ===========================================================================
// Segment lifecycle — list / expand / drain / retire
// ===========================================================================

#[test]
fn list_segments_returns_local_segments() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-list", 8 * 1024);

    let segments = client.list_segments().expect("list");
    assert!(
        !segments.is_empty(),
        "primary segment must be listed after register"
    );
}

#[test]
fn expand_local_memory_creates_unique_segment_names() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-expand-unique", 8 * 1024);

    let primary = client.list_segments().expect("list")[0]
        .segment_name
        .clone();
    let second = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;

    assert_ne!(
        primary, second,
        "expanded segment must have a distinct name"
    );
}

#[test]
fn startup_multi_segment_registration_preserves_future_segment_names() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    with_test_numa_locations(&["cpu:0", "cpu:1"], || {
        let transport = Arc::new(TestTransport::new("seg-startup-pipeline-seg"));
        transport.set_supports_parallel_startup_registration(true);
        let client = build_storage_client_with_transport(
            &meta,
            "seg-startup-pipeline",
            LocalMemoryConfig::new()
                .storage_bytes(8 * 1024)
                .scratch_bytes(4 * 1024)
                .location("cpu:0")
                .alignment(1)
                .numa_aware(true)
                .reclaim_grace_ms(0),
            transport,
        );

        let mut segment_names = client
            .list_segments()
            .expect("list")
            .into_iter()
            .map(|segment| segment.segment_name.0)
            .collect::<Vec<_>>();
        segment_names.sort();
        assert_eq!(
            segment_names,
            vec![
                "seg-startup-pipeline-seg".to_string(),
                "seg-startup-pipeline-seg-ext-1".to_string(),
            ]
        );

        let expanded = client
            .expand_local_memory(4 * 1024)
            .expect("expand after startup pipeline");
        assert_eq!(
            expanded.segment_name,
            SegmentName::new("seg-startup-pipeline-seg-ext-2")
        );
    });
}

#[test]
fn multi_segment_publish_uses_each_segments_transport_descriptor() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    with_test_numa_locations(&["cpu:0", "cpu:1"], || {
        let primary_segment = "seg-startup-descriptor-seg";
        let extra_segment = "seg-startup-descriptor-seg-ext-1";
        let expanded_segment = "seg-startup-descriptor-seg-ext-2";
        let transport = Arc::new(TestTransport::new(primary_segment));
        transport.set_supports_parallel_startup_registration(true);
        transport.set_local_segment_descriptor_for_segment(primary_segment, "primary-descriptor");
        transport.set_local_segment_descriptor_for_segment(extra_segment, "extra-descriptor");
        transport.set_local_segment_descriptor_for_segment(expanded_segment, "expanded-descriptor");

        let client = build_storage_client_with_transport(
            &meta,
            "seg-startup-descriptor",
            LocalMemoryConfig::new()
                .storage_bytes(8 * 1024)
                .scratch_bytes(4 * 1024)
                .location("cpu:0")
                .alignment(1)
                .numa_aware(true)
                .reclaim_grace_ms(0),
            transport,
        );

        let primary = meta
            .get_segment(client.runtime_id(), &SegmentName::new(primary_segment))
            .expect("primary segment lookup")
            .expect("primary segment should be published");
        assert_eq!(
            primary.transport_segment_descriptor.as_deref(),
            Some("primary-descriptor")
        );

        let extra = meta
            .get_segment(client.runtime_id(), &SegmentName::new(extra_segment))
            .expect("extra segment lookup")
            .expect("extra segment should be published");
        assert_eq!(
            extra.transport_segment_descriptor.as_deref(),
            Some("extra-descriptor")
        );

        let expanded = client
            .expand_local_memory(4 * 1024)
            .expect("expand after startup registration");
        assert_eq!(expanded.segment_name, SegmentName::new(expanded_segment));
        assert_eq!(
            expanded.transport_segment_descriptor.as_deref(),
            Some("expanded-descriptor")
        );
    });
}

#[test]
fn expand_local_memory_zero_bytes_fails() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-expand-zero", 8 * 1024);

    let err = client.expand_local_memory(0).expect_err("zero must fail");
    assert!(matches!(
        err,
        StoreError::InvalidState(_) | StoreError::Allocator(_)
    ));
}

#[test]
fn expand_increases_segment_count() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-expand-count", 8 * 1024);

    let before = client.list_segments().expect("list").len();
    let _ = client.expand_local_memory(4 * 1024).expect("expand");
    let after = client.list_segments().expect("list").len();
    assert_eq!(after, before + 1);
}

#[test]
fn drain_nonexistent_segment_returns_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-drain-nf", 8 * 1024);

    let phantom = SegmentName("no-such-segment".to_string());
    let result = client.drain_segment(&phantom);
    assert!(result.is_err(), "draining unknown segment must fail");
}

#[test]
fn drain_last_active_segment_returns_error() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-drain-last", 8 * 1024);

    let primary = client.list_segments().expect("list")[0]
        .segment_name
        .clone();
    let result = client.drain_segment(&primary);
    assert!(
        result.is_err(),
        "must refuse to drain the only active segment"
    );
}

#[test]
fn drain_expanded_segment_succeeds_when_primary_remains() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-drain-ok", 8 * 1024);

    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain expanded");

    let listed = client.list_segments().expect("list");
    let exp = listed
        .iter()
        .find(|s| s.segment_name == expanded)
        .expect("expanded segment still listed");
    assert_eq!(exp.state, SegmentLifecycleState::Draining);
}

#[test]
fn drain_already_draining_segment_is_idempotent() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-drain-idem", 8 * 1024);

    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("first drain");
    client
        .drain_segment(&expanded)
        .expect("second drain must succeed");
}

#[test]
fn retire_segment_that_is_not_draining_fails() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-retire-active", 8 * 1024);

    let primary = client.list_segments().expect("list")[0]
        .segment_name
        .clone();
    let result = client.retire_segment(&primary);
    assert!(result.is_err(), "cannot retire a non-draining segment");
}

#[test]
fn retire_empty_draining_segment_succeeds() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-retire-ok", 8 * 1024);

    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain");
    let retired = client.retire_segment(&expanded).expect("retire");
    assert!(retired, "retire of empty draining segment must succeed");
}

#[test]
fn retire_segment_retries_after_unpublish_failure() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let meta = Arc::new(RecoverableMetadataBackend::new(inner.clone()));
    let stable_id = "seg-retire-retry";
    let segment = format!("{stable_id}-seg");
    let transport = Arc::new(TestTransport::new(&segment));
    let factory = transport.factory();
    let client = StoreClientBuilder::new(meta.clone(), stable_id)
        .state(ClientLifecycleState::Active)
        .rpc_address("127.0.0.1:0")
        .segment_name(&segment)
        .transport(Arc::new(transport.peer(&segment)))
        .transport_factory(factory)
        .local_memory(storage_config_with_bytes(8 * 1024))
        .build(test_future_expiry_ms())
        .expect("build");
    let owner = client.runtime_id().clone();
    client.register_local_memory().expect("register");
    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain");
    meta.fail_next_segment_unpublishes(1);

    let error = client
        .retire_segment(&expanded)
        .expect_err("injected unpublish failure must be returned");
    assert!(matches!(error, StoreError::Metadata(_)));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_some());
    let local = client.list_segments().expect("local segment listing");
    let expanded_local = local
        .iter()
        .find(|segment| segment.segment_name == expanded)
        .expect("failed retire must preserve the local segment");
    assert_eq!(expanded_local.state, SegmentLifecycleState::Draining);
    assert!(client
        .state
        .lock()
        .memory_ref()
        .expect("memory should be registered")
        .has_storage_segment(&expanded));

    assert!(client
        .retire_segment(&expanded)
        .expect("retire should retry after unpublish failure"));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(!client
        .list_segments()
        .expect("local segment listing should succeed")
        .iter()
        .any(|segment| segment.segment_name == expanded));
}

#[test]
fn retire_segment_recovers_after_inflight_copy() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let meta = Arc::new(RecoverableMetadataBackend::new(inner.clone()));
    let client = build_retirement_client(meta, "seg-retire-copy");
    let owner = client.runtime_id().clone();
    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain");
    let region = hold_region(&client, &expanded);

    let error = client
        .retire_segment(&expanded)
        .expect_err("copy reference must block retirement");
    assert!(matches!(error, StoreError::InvalidState(_)));
    assert!(
        inner
            .get_segment(&owner, &expanded)
            .expect("metadata segment lookup should succeed")
            .is_some(),
        "a copy-pinned detach failure must preserve the announcement"
    );
    assert!(!client
        .state
        .lock()
        .pending_retirements
        .contains_key(&expanded));
    let local = client.list_segments().expect("local segment listing");
    assert!(local.iter().any(|segment| {
        segment.segment_name == expanded && segment.state == SegmentLifecycleState::Draining
    }));
    assert!(client
        .state
        .lock()
        .local_transports
        .contains_key(&expanded.0));

    drop(region);
    assert!(client.retire_segment(&expanded).expect("retry"));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(!client
        .list_segments()
        .expect("local segment listing")
        .iter()
        .any(|segment| segment.segment_name == expanded));
}

#[test]
fn failed_retirement_is_not_republished_by_heartbeat_recovery() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let meta = Arc::new(RecoverableMetadataBackend::new(inner.clone()));
    let client = build_retirement_client(meta.clone(), "seg-retire-recovery-retry");
    let owner = client.runtime_id().clone();
    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain");
    meta.fail_next_unpublish_response();

    let error = client
        .retire_segment(&expanded)
        .expect_err("lost unpublish response must be returned");
    assert!(matches!(error, StoreError::Metadata(_)));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    {
        let state = client.state.lock();
        assert!(state
            .memory_ref()
            .expect("memory should be registered")
            .has_storage_segment(&expanded));
        assert!(state
            .pending_retirements
            .get(&expanded)
            .is_some_and(|pending| pending.region.is_none() && !pending.unpublished));
    }

    client
        .repair_local_metadata_after_heartbeat_recovery()
        .expect("heartbeat recovery should succeed");
    assert!(
        inner
            .get_segment(&owner, &expanded)
            .expect("metadata segment lookup should succeed")
            .is_none(),
        "heartbeat recovery must not republish a pending retirement"
    );

    assert!(client.retire_segment(&expanded).expect("retry"));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(!client
        .list_segments()
        .expect("local segment listing")
        .iter()
        .any(|segment| segment.segment_name == expanded));
}

#[test]
fn retire_segment_lost_response_keeps_pending_and_blocks_name_reuse() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let meta = Arc::new(RecoverableMetadataBackend::new(inner.clone()));
    let client = build_retirement_client(meta.clone(), "seg-retire-lost-response");
    let owner = client.runtime_id().clone();
    let primary = client.segment_name().expect("primary segment name");
    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain");
    meta.fail_next_unpublish_response();

    let error = client
        .retire_segment(&expanded)
        .expect_err("lost unpublish response must be returned");
    assert!(matches!(error, StoreError::Metadata(_)));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    {
        let state = client.state.lock();
        assert!(state
            .memory_ref()
            .expect("memory should be registered")
            .has_storage_segment(&expanded));
        assert!(state
            .pending_retirements
            .get(&expanded)
            .is_some_and(|pending| pending.region.is_none() && !pending.unpublished));
    }
    client.state.lock().next_local_segment_id = 1;
    let next_name = client.state.lock().next_segment_name(&primary);
    assert_eq!(next_name.0, format!("{}-ext-2", primary.0));
    assert!(client
        .state
        .lock()
        .pending_retirements
        .contains_key(&expanded));

    assert!(client.retire_segment(&expanded).expect("retry"));
    assert!(client.state.lock().pending_retirements.is_empty());
    client.state.lock().next_local_segment_id = 1;
    assert_eq!(client.state.lock().next_segment_name(&primary), expanded);
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(!client
        .list_segments()
        .expect("local segment listing")
        .iter()
        .any(|segment| segment.segment_name == expanded));
}

#[test]
fn retire_segment_retries_after_unregister_failure() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let (client, faults, transport) =
        build_faulty_retirement_client(meta.clone(), "seg-retire-unregister-retry");
    let owner = client.runtime_id().clone();
    let primary = client.segment_name().expect("primary segment name");
    client.expand_local_memory(4 * 1024).expect("expand");
    client.drain_segment(&primary).expect("drain");
    let (registered_before, allocations_before) = {
        let state = transport.state.lock();
        (state.registered_memory.len(), state.allocations.len())
    };
    faults.fail_next_unregisters(1);

    let error = client
        .retire_segment(&primary)
        .expect_err("injected unregister failure must be returned");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(meta
        .get_segment(&owner, &primary)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(client
        .state
        .lock()
        .pending_retirements
        .get(&primary)
        .is_some_and(|pending| pending.region.is_some()));
    assert_eq!(faults.unregister_call_count.load(Ordering::SeqCst), 1);
    assert_eq!(faults.free_call_count.load(Ordering::SeqCst), 0);
    {
        let state = transport.state.lock();
        assert_eq!(state.registered_memory.len(), registered_before);
        assert_eq!(state.allocations.len(), allocations_before);
    }

    assert!(client.retire_segment(&primary).expect("retry"));
    assert_eq!(faults.unregister_call_count.load(Ordering::SeqCst), 2);
    assert_eq!(faults.free_call_count.load(Ordering::SeqCst), 1);
    {
        let state = transport.state.lock();
        assert_eq!(state.registered_memory.len(), registered_before - 1);
        assert_eq!(state.allocations.len(), allocations_before - 1);
    }
    assert!(client.state.lock().pending_retirements.is_empty());
    assert!(!client
        .list_segments()
        .expect("local segment listing")
        .iter()
        .any(|segment| segment.segment_name == primary));
}

#[test]
fn retire_segment_retries_free_without_repeating_unregister() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let (client, faults, transport) =
        build_faulty_retirement_client(meta.clone(), "seg-retire-free-retry");
    let owner = client.runtime_id().clone();
    let primary = client.segment_name().expect("primary segment name");
    client.expand_local_memory(4 * 1024).expect("expand");
    client.drain_segment(&primary).expect("drain");
    let (registered_before, allocations_before) = {
        let state = transport.state.lock();
        (state.registered_memory.len(), state.allocations.len())
    };
    faults.fail_next_frees(1);

    let error = client
        .retire_segment(&primary)
        .expect_err("injected free failure must be returned");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(meta
        .get_segment(&owner, &primary)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(client
        .state
        .lock()
        .pending_retirements
        .get(&primary)
        .is_some_and(|pending| pending.region.is_some()));
    assert_eq!(faults.unregister_call_count.load(Ordering::SeqCst), 1);
    assert_eq!(faults.free_call_count.load(Ordering::SeqCst), 1);
    {
        let state = transport.state.lock();
        assert_eq!(state.registered_memory.len(), registered_before - 1);
        assert_eq!(state.allocations.len(), allocations_before);
    }

    assert!(client.retire_segment(&primary).expect("retry"));
    assert_eq!(
        faults.unregister_call_count.load(Ordering::SeqCst),
        1,
        "retry must not unregister an already unregistered region"
    );
    assert_eq!(faults.free_call_count.load(Ordering::SeqCst), 2);
    {
        let state = transport.state.lock();
        assert_eq!(state.registered_memory.len(), registered_before - 1);
        assert_eq!(state.allocations.len(), allocations_before - 1);
    }
    assert!(client.state.lock().pending_retirements.is_empty());
    assert!(!client
        .list_segments()
        .expect("local segment listing")
        .iter()
        .any(|segment| segment.segment_name == primary));
}

#[test]
fn release_retry_does_not_reopen_metadata_withdrawal() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let meta = Arc::new(RecoverableMetadataBackend::new(inner));
    let (client, faults, _transport) =
        build_faulty_retirement_client(meta.clone(), "seg-retire-release-metadata-error");
    let primary = client.segment_name().expect("primary segment name");
    client
        .drain_segment_internal(&primary, true)
        .expect("drain primary");
    faults.fail_next_frees(1);

    let error = client
        .retire_segment(&primary)
        .expect_err("injected free failure must be returned");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(client
        .state
        .lock()
        .pending_retirements
        .get(&primary)
        .is_some_and(|pending| pending.unpublished && pending.region.is_some()));

    meta.fail_next_segment_unpublishes(1);
    assert!(client
        .retire_segment(&primary)
        .expect("release retry must not unpublish or reattach the region"));
    assert_eq!(
        meta.state
            .lock()
            .expect("recoverable metadata state lock should succeed")
            .unpublish_failures_remaining,
        1,
        "release retry must not call unpublish again"
    );
    assert!(client.state.lock().pending_retirements.is_empty());
    assert!(!client
        .state
        .lock()
        .memory_ref()
        .expect("memory should be registered")
        .has_storage_segment(&primary));
}

#[test]
fn pending_primary_retirement_blocks_replacement_and_mount() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let (client, faults, _transport) =
        build_faulty_retirement_client(meta.clone(), "seg-retire-primary-replacement");
    let owner = client.runtime_id().clone();
    let primary = client.segment_name().expect("primary segment name");
    client
        .drain_segment_internal(&primary, true)
        .expect("drain primary");
    faults.fail_next_frees(1);

    let error = client
        .retire_segment(&primary)
        .expect_err("injected free failure must be returned");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(meta
        .get_segment(&owner, &primary)
        .expect("metadata segment lookup should succeed")
        .is_none());

    let expand_error = client
        .expand_local_memory(4 * 1024)
        .expect_err("pending primary must not be replaced");
    assert!(matches!(expand_error, StoreError::InvalidState(_)));
    let mount_error = client
        .mount_segment(4 * 1024, 0, Vec::new())
        .expect_err("pending primary must not be republished");
    assert!(matches!(mount_error, StoreError::InvalidState(_)));
    assert!(meta
        .get_segment(&owner, &primary)
        .expect("metadata segment lookup should succeed")
        .is_none());

    assert!(client.retire_segment(&primary).expect("retry"));
    let replacement = client
        .expand_local_memory(4 * 1024)
        .expect("primary name can be reused after retirement completes");
    assert_eq!(replacement.segment_name, primary);
    assert_eq!(replacement.state, SegmentLifecycleState::Active);
    let published = meta
        .get_segment(&owner, &primary)
        .expect("metadata segment lookup should succeed")
        .expect("replacement should be published");
    assert_eq!(published.state, SegmentLifecycleState::Active);
}

#[test]
fn client_drop_releases_detached_pending_region() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let (client, faults, transport) =
        build_faulty_retirement_client(meta, "seg-retire-drop-pending");
    let primary = client.segment_name().expect("primary segment name");
    let primary_addr = client
        .allocator
        .lock()
        .announcement(&primary)
        .and_then(|announcement| {
            announcement
                .target_chunks
                .first()
                .map(|chunk| chunk.target_offset as usize)
        })
        .expect("primary allocation should exist");
    client
        .drain_segment_internal(&primary, true)
        .expect("drain primary");
    faults.fail_next_frees(1);

    let error = client
        .retire_segment(&primary)
        .expect_err("injected free failure must be returned");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(transport
        .state
        .lock()
        .allocations
        .contains_key(&primary_addr));

    let free_calls_before_drop = faults.free_call_count.load(Ordering::SeqCst);
    drop(client);
    assert!(faults.free_call_count.load(Ordering::SeqCst) > free_calls_before_drop);
    let state = transport.state.lock();
    assert!(!state.allocations.contains_key(&primary_addr));
    assert!(!state.registered_memory.contains_key(&primary_addr));
}

#[test]
fn retirement_waits_for_heartbeat_recovery_publication() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let meta = Arc::new(RecoverableMetadataBackend::new(inner.clone()));
    let client = Arc::new(build_retirement_client(meta.clone(), "seg-retire-recovery"));
    let owner = client.runtime_id().clone();
    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;
    client.drain_segment(&expanded).expect("drain");
    meta.arm_publish_gate(&expanded);

    let recovery_client = client.clone();
    let recovery = std::thread::spawn(move || {
        recovery_client.repair_local_metadata_after_heartbeat_recovery()
    });
    std::thread::sleep(Duration::from_millis(50));
    meta.wait_publish_gate_entered();
    let retire_client = client.clone();
    let expanded_for_retire = expanded.clone();
    let retire = std::thread::spawn(move || retire_client.retire_segment(&expanded_for_retire));
    std::thread::sleep(Duration::from_millis(20));
    assert!(!retire.is_finished());

    meta.release_publish_gate();
    recovery
        .join()
        .expect("recovery thread should not panic")
        .expect("recovery should succeed");
    assert!(retire
        .join()
        .expect("retire thread should not panic")
        .expect("retire should succeed"));
    assert!(inner
        .get_segment(&owner, &expanded)
        .expect("metadata segment lookup should succeed")
        .is_none());
    assert!(!client
        .list_segments()
        .expect("local segment listing")
        .iter()
        .any(|segment| segment.segment_name == expanded));
}

#[test]
fn expand_write_drain_retire_full_lifecycle() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let client = build_storage_client(&meta, "seg-full", 16 * 1024);

    // Expand
    let expanded = client
        .expand_local_memory(4 * 1024)
        .expect("expand")
        .segment_name;

    // Write data into the store (primary will hold it; expanded remains empty)
    client.put("full-key", b"full-value").expect("put");
    assert_eq!(client.get("full-key").expect("get"), b"full-value");

    // Drain the expanded (empty) segment
    client.drain_segment(&expanded).expect("drain");

    // Retire the drained empty segment
    let retired = client.retire_segment(&expanded).expect("retire");
    assert!(retired);

    // Primary still holds data
    assert_eq!(client.get("full-key").expect("get"), b"full-value");
}

// ===========================================================================
// Evacuation
// ===========================================================================

#[test]
fn evacuate_owned_replicas_on_empty_store() {
    let meta = Arc::new(InMemoryMetadataBackend::new());
    let mut client = build_storage_client(&meta, "evac-empty", 8 * 1024);

    // No data stored — evacuation is a no-op but must not panic or return Err.
    let moved = client
        .evacuate_owned_replicas()
        .expect("evacuate on empty store");
    assert_eq!(moved, 0, "empty store has no replicas to evacuate");
}
