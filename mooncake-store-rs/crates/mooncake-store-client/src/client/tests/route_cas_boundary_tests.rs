use super::*;

fn build_cas_boundary_client(stable_id: &str) -> StoreClient {
    let client = StoreClientBuilder::new(Arc::new(InMemoryMetadataBackend::new()), stable_id)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(Arc::new(TestTransport::new(&format!(
            "{stable_id}-segment"
        ))))
        .local_memory(storage_config_with_bytes(128))
        .build(test_future_expiry_ms())
        .expect("CAS boundary client should build");
    client
        .register_local_memory()
        .expect("CAS boundary memory should register");
    client
}

#[test]
fn mesh_cas_active_version_conflict_returns_current_route() {
    let client = build_cas_boundary_client("cas-boundary-active");
    client.put("key", b"payload").expect("put should succeed");
    let current = client.query_route("key").unwrap().unwrap();
    let conflict = client
        .cas_route("key", Some(current.version.next()), Some(&current))
        .expect("version conflict should be a CAS response");

    assert!(!conflict.applied);
    let returned = conflict
        .current
        .expect("live authority must return its current route");
    assert_eq!(returned.version, current.version);
    assert_eq!(returned.key, current.key);
    assert!(conflict.delete_version.is_none());
    assert_eq!(
        client.get("key").expect("conflict must preserve payload"),
        b"payload"
    );
}

#[test]
fn mesh_cas_deleted_route_conflict_returns_none_and_delete_marker() {
    let client = build_cas_boundary_client("cas-boundary-deleted");
    client.put("key", b"payload").expect("put should succeed");
    let current = client.query_route("key").unwrap().unwrap();
    let deleted = client
        .cas_route("key", Some(current.version), None)
        .expect("route deletion should succeed");
    assert!(deleted.applied);
    assert!(client.query_route("key").unwrap().is_none());

    let conflict = client
        .cas_route("key", Some(current.version.next()), Some(&current))
        .expect("stale insertion should return a CAS conflict");
    assert!(!conflict.applied);
    assert!(conflict.current.is_none());
    assert_eq!(conflict.delete_version, Some(current.version));
    assert!(client.query_route("key").unwrap().is_none());
}

#[test]
fn offload_deleted_route_cas_reply_preserves_republished_allocation() {
    assert_offload_cas_reply_preserves_republished_allocation(true);
}

#[test]
fn offload_live_route_cas_reply_preserves_republished_allocation() {
    assert_offload_cas_reply_preserves_republished_allocation(false);
}

fn assert_offload_cas_reply_preserves_republished_allocation(deleted_reply: bool) {
    use crate::control_plane::ColdTierControlService;

    let _cold_tier_env = enable_cold_tier_for_test();
    let mut client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "cas-boundary-offload-republish",
    )
    .state(ClientLifecycleState::Active)
    .label("storage", "true")
    .route_control(RouteControlMode::EmbeddedWrh)
    .transport(Arc::new(TestTransport::new("cas-boundary-offload-segment")))
    .local_memory(
        LocalMemoryConfig::new()
            .numa_aware(false)
            .storage_bytes(48)
            .scratch_bytes(4096)
            .alignment(1)
            .eviction_poll_interval(Duration::ZERO),
    )
    .build(test_future_expiry_ms())
    .expect("client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    let directory = Arc::new(CountingRouteDirectory::new(client.route_directory.clone()));
    let owner = test_storage_owner_state_with_route_directory(
        &client,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    client.storage_owner = owner.clone();
    let client = Arc::new(client);
    let old_payload = [1_u8; 16];
    let new_payload = [2_u8; 32];
    client
        .put("key", &old_payload)
        .expect("initial put should succeed");
    let original = client.query_route("key").unwrap().unwrap();
    let old_allocation = original.replicas[0].allocation_id;
    let mut pending = original.clone();
    pending.version = original.version.next();
    pending.cold_backing = Some(mooncake_store_core::ColdBackingRoute {
        owner: client.runtime_id().clone(),
        cold_tier_id: "cas-boundary-unregistered-device".to_string(),
        object_locator: "cas-boundary-pending-object".to_string(),
        length: 16,
        checksum: original.replicas[0].checksum,
        state: mooncake_store_core::ColdBackingState::PendingOffload,
        replicas: vec![],
    });
    assert!(
        client
            .cas_route("key", Some(original.version), Some(&pending))
            .expect("pending backing publication should succeed")
            .applied
    );
    owner.track_route(&pending);
    let now = Instant::now();
    owner.pending_offloads.retry_after(
        PendingOffloadEntry {
            key: PendingOffloadKey {
                route_key: pending.key.clone(),
            },
            route_version: pending.version,
            attempts: 0,
            not_before: now,
            enqueued_at: now,
            length_bytes: Some(16),
        },
        Duration::ZERO,
    );

    let before_client = Arc::downgrade(&client);
    *directory.before_single_cas_hook.lock() = Some(Box::new(move || {
        let client = before_client.upgrade().expect("client should remain live");
        let mut hot_only = client.query_route("key").unwrap().unwrap();
        let expected = hot_only.version;
        hot_only.version = expected.next();
        hot_only.cold_backing = None;
        assert!(
            client
                .cas_route("key", Some(expected), Some(&hot_only))
                .unwrap()
                .applied
        );
        if deleted_reply {
            client
                .remove("key", true)
                .expect("concurrent delete should succeed");
            assert!(client.query_route("key").unwrap().is_none());
        }
    }));
    let after_client = Arc::downgrade(&client);
    *directory.after_single_cas_hook.lock() = Some(Box::new(move |cas| {
        assert!(!cas.applied);
        assert_eq!(cas.current.is_none(), deleted_reply);
        assert_eq!(cas.delete_version.is_some(), deleted_reply);
        let client = after_client.upgrade().expect("client should remain live");
        if !deleted_reply {
            client
                .remove("key", true)
                .expect("delete after live CAS reply");
        }
        client
            .put("key", &new_payload)
            .expect("concurrent republication should succeed");
        let republished = client.query_route("key").unwrap().unwrap();
        assert_ne!(republished.replicas[0].allocation_id, old_allocation);
    }));
    let adapter = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: owner.clone(),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
        allocation_lifecycle: AllocationLifecycleConfig::default(),
    };
    adapter
        .trigger_offload(1)
        .expect("missing device should skip offload");
    assert!(directory.before_single_cas_hook.lock().is_none());
    assert!(directory.after_single_cas_hook.lock().is_none());
    assert_eq!(
        client
            .get("key")
            .expect("republished payload should remain readable"),
        new_payload
    );
    let mut recovered = false;
    for _ in 0..4 {
        if client.put("next", &[3_u8; 32]).is_ok() {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(120));
    }
    assert!(
        recovered,
        "public put should reclaim the republished allocation"
    );
}

fn assert_cold_publication_cas_reply_preserves_republished_allocation(initial_write: bool) {
    let _cold_tier_env = enable_cold_tier_for_test();
    let stable_id = if initial_write {
        "cas-initial-backing-republish"
    } else {
        "cas-eviction-backing-republish"
    };
    let cold_root = cold_tier_test_root(stable_id);
    let mut client = StoreClientBuilder::new(Arc::new(InMemoryMetadataBackend::new()), stable_id)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(Arc::new(TestTransport::new(&format!(
            "{stable_id}-segment"
        ))))
        .cold_tier_target(cold_tier_test_config_with_root(
            stable_id,
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::EvictTriggered)
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(48)
                .scratch_bytes(4096)
                .alignment(1)
                .eviction_poll_interval(Duration::ZERO),
        )
        .build(test_future_expiry_ms())
        .expect("cold publication client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    client
        .put("key", &[1_u8; 16])
        .expect("initial put should succeed");
    let original = client.query_route("key").unwrap().unwrap();
    assert!(original.cold_backing.is_none());
    let old_allocation = original.replicas[0].allocation_id;
    let directory = Arc::new(CountingRouteDirectory::new(client.route_directory.clone()));
    let owner = test_storage_owner_state_with_route_directory(
        &client,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    *owner.cold_tier_devices.resolver.lock() = ColdTierBackendResolver::from_handles(
        stable_id.to_string(),
        BTreeMap::from([(
            stable_id.to_string(),
            Arc::new(LocalDirPersistentStorageBackend::new_with_root(cold_root))
                as Arc<dyn PersistentStorageBackend>,
        )]),
    );
    for device in client
        .metadata
        .list_cold_tier_devices(&Default::default())
        .unwrap()
    {
        owner.cold_tier_devices.upsert(device);
    }
    owner.track_route(&original);
    client.storage_owner = owner.clone();
    let client = Arc::new(client);

    let before_client = Arc::downgrade(&client);
    *directory.before_single_cas_hook.lock() = Some(Box::new(move || {
        let client = before_client.upgrade().expect("client should remain live");
        client
            .remove("key", true)
            .expect("concurrent delete should succeed");
        assert!(client.query_route("key").unwrap().is_none());
    }));
    let after_client = Arc::downgrade(&client);
    *directory.after_single_cas_hook.lock() = Some(Box::new(move |cas| {
        assert!(!cas.applied);
        assert!(cas.current.is_none());
        assert!(cas.delete_version.is_some());
        let client = after_client.upgrade().expect("client should remain live");
        client
            .put("key", &[2_u8; 32])
            .expect("concurrent republication should succeed");
        let republished = client.query_route("key").unwrap().unwrap();
        assert_ne!(republished.replicas[0].allocation_id, old_allocation);
    }));

    let published = if initial_write {
        super::super::cold_tier::publish_initial_write_cold_backing(&owner, &original)
    } else {
        super::super::cold_tier::publish_pending_cold_backing_for_eviction(&owner, &original)
    }
    .expect("concurrent deletion should yield a normal CAS conflict");
    assert!(published.is_none());
    assert!(directory.before_single_cas_hook.lock().is_none());
    assert!(directory.after_single_cas_hook.lock().is_none());
    assert_eq!(
        client
            .get("key")
            .expect("republished payload should remain readable"),
        [2_u8; 32]
    );
    let mut recovered = false;
    for _ in 0..4 {
        if client.put("next", &[3_u8; 32]).is_ok() {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(120));
    }
    assert!(
        recovered,
        "public put should reclaim the republished allocation"
    );
}

#[test]
fn initial_cold_publication_cas_reply_preserves_republished_allocation() {
    assert_cold_publication_cas_reply_preserves_republished_allocation(true);
}

#[test]
fn eviction_cold_publication_cas_reply_preserves_republished_allocation() {
    assert_cold_publication_cas_reply_preserves_republished_allocation(false);
}
