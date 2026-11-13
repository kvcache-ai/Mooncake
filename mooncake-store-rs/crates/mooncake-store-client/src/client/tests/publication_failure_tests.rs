use super::*;

#[test]
fn failed_batch_transfer_releases_publication_reservations() {
    check_failed_batch_transfer_releases_reservations(false);
}

#[test]
fn failed_batch_transfer_releases_enabled_publication_ledger() {
    check_failed_batch_transfer_releases_reservations(true);
}

fn check_failed_batch_transfer_releases_reservations(lifecycle: bool) {
    let route_mode = if lifecycle {
        RouteControlMode::EmbeddedWrh
    } else {
        RouteControlMode::MetadataOnly
    };
    let mut compatibility = CompatibilityDescriptor::default();
    compatibility
        .capabilities
        .insert(mooncake_store_core::ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY.to_string());
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("batch-put-retry-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("batch-put-retry-b-segment"));
    let writer_transport = Arc::new(store_a_transport.peer("batch-put-retry-writer-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("batch-put-retry-reader-segment"));
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");

    let store_a = StoreClientBuilder::new(metadata.clone(), "batch-put-retry-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(route_mode)
        .compatibility(compatibility.clone())
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "batch-put-retry-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .route_control(route_mode)
        .compatibility(compatibility.clone())
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let writer = StoreClientBuilder::new(metadata.clone(), "batch-put-retry-writer")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(route_mode)
        .compatibility(compatibility.clone())
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport)
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    let reader = StoreClientBuilder::new(metadata, "batch-put-retry-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .route_control(route_mode)
        .compatibility(compatibility.clone())
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    writer
        .register_local_memory()
        .expect("writer memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &writer, &reader]);

    for client in [&store_a, &store_b, &writer, &reader] {
        assert_eq!(
            client
                .lease()
                .compatibility
                .supports(mooncake_store_core::ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY),
            lifecycle,
            "test must exercise the advertised lifecycle mode"
        );
    }

    let source_a = b"batch-put-retry-payload-a".to_vec();
    let source_b = b"batch-put-retry-payload-b".to_vec();
    writer
        .register_buffer(source_a.as_ptr() as *mut c_void, source_a.len())
        .expect("source-a buffer should register");
    writer
        .register_buffer(source_b.as_ptr() as *mut c_void, source_b.len())
        .expect("source-b buffer should register");

    let store_a_segment = store_a
        .segment_name()
        .expect("store-a segment should exist");
    store_a_transport.fail_next_submit_for_segment(&store_a_segment.0);

    let policy = ReplicationPolicy::new()
        .replica_count(1)
        .prefer_local(false)
        .preferred_storage_owners([
            store_a.runtime_id().storage_key(),
            store_b.runtime_id().storage_key(),
        ]);
    let routes = writer
        .batch_put_from(&[
            PutFromRequest::new(
                "batch-put-retry-key-a",
                source_a.as_ptr().cast(),
                source_a.len(),
            )
            .replication(policy.clone()),
            PutFromRequest::new(
                "batch-put-retry-key-b",
                source_b.as_ptr().cast(),
                source_b.len(),
            )
            .replication(policy),
        ])
        .expect("batch_put_from should retry after the first target fails mid-transfer");

    assert!(
        store_a
            .allocator
            .lock()
            .pending_allocations(now_ms())
            .is_empty(),
        "failed remote writes must empty the owner's pending-publication ledger"
    );
    assert_eq!(routes.len(), 2);
    for route in &routes {
        if lifecycle {
            assert!(
                route.replicas[0].allocation_id.is_some(),
                "successful retry must use generation-aware publication in lifecycle mode"
            );
        }
        assert_eq!(route.replicas.len(), 1);
        assert_eq!(route.replicas[0].owner, *store_b.runtime_id());
    }
    assert!(
        writer
            .suspect_runtime_cache
            .lock()
            .contains(store_a.runtime_id()),
        "failed batch write target should be quarantined before retrying placement"
    );
    assert_eq!(
        store_a.allocator.lock().usage_bytes().0,
        0,
        "failed writes release reservations"
    );
    assert_eq!(
        reader
            .get("batch-put-retry-key-a")
            .expect("reader should see first retried write"),
        source_a
    );
    assert_eq!(
        reader
            .get("batch-put-retry-key-b")
            .expect("reader should see second retried write"),
        source_b
    );
}
