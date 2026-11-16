use super::*;

#[test]
#[ignore = "shared baseline: authority-local filter eviction does not fence a delayed first mirror"]
fn readable_filter_eviction_fences_a_delayed_first_mirror() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let dead_transport = Arc::new(TestTransport::new("is-exist-filter-dead-segment"));
    let reader_transport = Arc::new(dead_transport.peer("is-exist-filter-reader-seg"));

    let dead_store = StoreClientBuilder::new(metadata.clone(), "is-exist-filter-dead")
        .label("route_ephemeral", "false")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::ZERO)
        .transport(dead_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("dead store build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "is-exist-filter-reader")
        .label("route_ephemeral", "true")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .live_client_sync_interval(Duration::ZERO)
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    dead_store
        .register_local_memory()
        .expect("dead store memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&dead_store, &reader]);

    let data = b"is-exist-filter-test-data-pad!";
    let policy = ReplicationPolicy::new()
        .prefer_local(false)
        .preferred_storage_owner(dead_store.runtime_id().storage_key());
    let mirror_gate = reader.route_write_gate.state.lock();
    dead_store
        .put_with_policy("is-exist-filter-key", data, &policy)
        .expect("initial put should succeed");

    assert!(
        reader
            .is_exist("is-exist-filter-key")
            .expect("is_exist should succeed"),
        "route should be visible before readable filter activation"
    );

    let namespace = metadata.route_namespace();
    mooncake_store_route::update_readable_filter(
        &namespace,
        Some(BTreeSet::from([reader.runtime_id().clone()])),
    );

    assert!(
        !reader
            .is_exist("is-exist-filter-key")
            .expect("is_exist should succeed"),
        "route should be invisible when readable filter excludes all replica owners"
    );

    mooncake_store_route::update_readable_filter(&namespace, None);
    drop(mirror_gate);
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if reader
            .is_exist("is-exist-filter-key")
            .expect("probe should succeed")
        {
            break;
        }
        std::thread::yield_now();
    }

    assert!(
        !reader
            .is_exist("is-exist-filter-key")
            .expect("is_exist should succeed"),
        "route should remain absent after eviction even when readable filter is deactivated"
    );
}
