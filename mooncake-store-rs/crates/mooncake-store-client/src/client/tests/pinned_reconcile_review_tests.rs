use super::*;

#[test]
fn orphan_reconcile_retains_offload_pin_until_borrower_drops() {
    let _cold_guard = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("pinned-orphan-segment"));
    let client = StoreClientBuilder::new(metadata, "pinned-orphan")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(
            ColdTierTargetConfig::directory(
                "pinned-orphan",
                ColdTierKind::Ssd,
                cold_tier_test_root("pinned-orphan"),
            )
            .extent_store_engine(),
        )
        .build(test_future_expiry_ms())
        .expect("client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    let payload = vec![0x5au8; 128];
    client
        .put("pinned-orphan", &payload)
        .expect("put should publish");
    let route = client
        .query_route("pinned-orphan")
        .expect("route should query")
        .expect("route should exist");
    client.storage_owner.track_route(&route);
    wait_for_storage_clock_route(&client, &route, false);
    let replica = route
        .replicas
        .iter()
        .find(|replica| replica.owner == *client.runtime_id())
        .expect("route should contain the local allocation");
    let identity = AllocationIdentity {
        segment_name: replica.segment_name.clone(),
        offset_bytes: replica.segment_offset,
        length_bytes: replica.length,
        allocation_id: replica
            .allocation_id
            .expect("allocation should have a generation"),
    };
    let entry = client
        .storage_owner
        .pending_offloads
        .claim(&route.key, route.version)
        .expect("offload should be claimable");
    let prepared = client
        .storage_owner
        .prepare_pending_offload_entries(vec![entry])
        .expect("offload should prepare");
    assert_eq!(prepared.len(), 1);
    assert!(client.allocator.lock().allocation_is_pinned(&identity));

    // Model an authoritative delete whose owner notification has not arrived.
    let cas = client
        .route_directory
        .compare_and_swap_object_route(&client.lease, &route.key, Some(route.version), None)
        .expect("authoritative delete should succeed");
    assert!(cas.applied);
    client.storage_owner.hot_replicas.remove_key(&route.key);
    let deadline = Instant::now() + Duration::from_secs(2);
    let reconciled = loop {
        let reconciled = client
            .storage_owner
            .reconcile_stale_clock_candidates(None, true)
            .expect("reconciliation while pinned should succeed");
        if reconciled != 0 || Instant::now() >= deadline {
            break reconciled;
        }
        // Honor the production reconciliation cadence while the borrower remains pinned.
        sleep(Duration::from_millis(20));
    };
    assert_eq!(reconciled, 1);
    assert_eq!(
        client.allocator.lock().allocation_pins[&identity]
            .deferred_release_owner
            .as_ref(),
        Some(client.runtime_id())
    );
    assert_eq!(client.allocator.lock().usage_bytes().0, 128);
    assert!(client
        .allocator
        .lock()
        .reserve_any(client.runtime_id(), 128)
        .is_err());
    assert_eq!(prepared[0].payload.as_slice(), payload);

    drop(prepared);
    assert!(!client.allocator.lock().allocation_is_pinned(&identity));
    assert_eq!(client.allocator.lock().usage_bytes().0, 0);
    assert!(client
        .query_route("pinned-orphan")
        .expect("deleted route should remain queryable as absent")
        .is_none());
    assert_eq!(client.allocator.lock().usage_bytes().0, 0);
    let reused = client
        .allocator
        .lock()
        .reserve_any(client.runtime_id(), 128)
        .expect("unborrowed orphan should be reusable");
    assert_eq!(reused.offset_bytes, identity.offset_bytes);
    assert_ne!(reused.allocation_id, identity.allocation_id);
}
