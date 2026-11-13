use super::*;
use crate::control_plane::ColdTierControlService;

#[test]
fn cold_read_route_miss_preserves_public_write_recovery() {
    check_cold_read_public_write_recovery(true);
}

#[test]
fn healthy_cold_read_preserves_public_write_recovery() {
    check_cold_read_public_write_recovery(false);
}

fn check_cold_read_public_write_recovery(inject_miss: bool) {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "clock-audit-cold-read")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(Arc::new(TestTransport::new(
            "clock-audit-cold-read-segment",
        )))
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(64)
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
    for index in 0..4 {
        let route = client
            .put(&format!("live-{index}"), b"0123456789abcdef")
            .expect("public put should fill capacity");
        owner.track_route(&route);
    }
    client.storage_owner = owner.clone();
    let adapter = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: owner.clone(),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
        allocation_lifecycle: AllocationLifecycleConfig::default(),
    };
    directory
        .miss_bounded_reads
        .store(inject_miss, Ordering::Relaxed);
    for index in 0..4 {
        let response = adapter.read_from_cold(
            &owner.metadata.route_namespace(),
            &client.runtime_id().stable_id.0,
            "default",
            &format!("live-{index}"),
            "",
            "",
        );
        assert!(matches!(response.result, Err(StoreError::NotFound(_))));
    }
    directory.miss_bounded_reads.store(false, Ordering::Relaxed);
    assert_eq!(
        owner
            .hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&BTreeSet::new()),
        4
    );
    sleep(Duration::from_millis(120));
    eprintln!(
        "after cold read misses: usage={} clock={} pending={}",
        owner.allocator.lock().usage_bytes().0,
        owner
            .hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&BTreeSet::new()),
        owner.pending_evictions.lock().len()
    );
    client
        .put("after-cold-miss", b"fedcba9876543210")
        .expect("public put must recover after a transient cold-read route miss");
    assert!(owner.pending_evictions.lock().is_empty());
}
