use super::*;
use crate::control_plane::ColdTierControlService;

#[test]
fn offload_route_miss_preserves_public_write_recovery() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let mut client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "clock-audit-offload",
    )
    .state(ClientLifecycleState::Active)
    .label("storage", "true")
    .route_control(RouteControlMode::EmbeddedWrh)
    .cold_tier_target(cold_tier_test_config_with_root(
        "clock-audit-offload",
        cold_tier_test_root("clock-route-offload-recovery"),
    ))
    .transport(Arc::new(TestTransport::new("clock-audit-offload-segment")))
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
        client
            .put(&format!("live-{index}"), b"0123456789abcdef")
            .expect("public put should fill capacity");
        client.flush_pending_cold_tier_offloads();
        wait_for_materialized_cold_backing(&client, &format!("live-{index}"));
        let route = client
            .query_route(&format!("live-{index}"))
            .unwrap()
            .unwrap();
        owner.track_route(&route);
        let now = Instant::now();
        assert!(route.cold_backing.is_some());
        owner.pending_offloads.retry_after(
            PendingOffloadEntry {
                key: PendingOffloadKey {
                    route_key: route.key.clone(),
                },
                route_version: route.version,
                attempts: 0,
                not_before: now,
                enqueued_at: now,
                length_bytes: Some(16),
            },
            Duration::ZERO,
        );
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
    sleep(Duration::from_millis(120));
    directory.miss_full_reads.store(true, Ordering::Relaxed);
    adapter
        .trigger_offload(4)
        .expect("transient route misses should be handled");
    eprintln!(
        "after offload misses: usage={} clock={} pending={}",
        owner.allocator.lock().usage_bytes().0,
        owner
            .hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&BTreeSet::new()),
        owner.pending_evictions.lock().len()
    );
    directory.miss_full_reads.store(false, Ordering::Relaxed);
    assert_eq!(
        owner
            .hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&BTreeSet::new()),
        4
    );
    sleep(Duration::from_millis(120));
    client
        .put("after-offload-miss", b"fedcba9876543210")
        .expect("public put must recover after transient offload route misses");
    assert!(owner.pending_evictions.lock().is_empty());
}
