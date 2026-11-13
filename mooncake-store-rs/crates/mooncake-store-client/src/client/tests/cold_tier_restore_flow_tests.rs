#[allow(unused_imports)]
use super::*;

fn prometheus_counter_value(metrics: &str, name: &str, labels: &[&str]) -> u64 {
    metrics
        .lines()
        .find(|line| line.starts_with(name) && labels.iter().all(|label| line.contains(label)))
        .and_then(|line| line.split_whitespace().last())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or_default()
}

#[test]
fn embedded_client_restores_cold_only_route_without_configured_cold_tier_target() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("embedded-cold-restore");
    let writer_transport = Arc::new(TestTransport::new("embedded-cold-restore-writer-segment"));
    let writer = StoreClientBuilder::new(metadata.clone(), "embedded-cold-restore-writer")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport.clone())
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(cold_tier_test_config_with_root(
            "embedded-cold-restore",
            cold_root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("writer build should succeed");
    writer
        .register_local_memory()
        .expect("writer local memory should register");

    let payload = b"embedded-cold-payload";
    writer
        .put("embedded-cold-key", payload)
        .expect("writer put should succeed");
    wait_for_materialized_cold_backing(&writer, "embedded-cold-key");
    force_cold_only_route(&writer, "embedded-cold-key");

    let reader_transport = Arc::new(writer_transport.peer("embedded-cold-restore-reader-segment"));
    let reader = StoreClientBuilder::new(metadata, "embedded-cold-restore-reader")
        .state(ClientLifecycleState::Active)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config_with_bytes(128))
        .build(test_future_expiry_ms())
        .expect("reader build should succeed without cold tier target");
    reader
        .register_local_memory()
        .expect("reader local memory should register");

    assert_eq!(
        reader
            .get("embedded-cold-key")
            .expect("embedded reader should restore via metadata cold tier record"),
        payload
    );
    assert_eq!(
        reader
            .batch_get(&[ObjectRef::new("embedded-cold-key")])
            .expect("embedded reader batch_get should restore via metadata cold tier record"),
        vec![payload.to_vec()]
    );

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn env_only_cold_tier_does_not_register_fallback_backend_for_eviction() {
    let _metrics_guard = metrics_test_lock().lock();
    reset_metrics();
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("env-only-no-fallback");
    let client = StoreClientBuilder::new(metadata.clone(), "env-only-no-fallback")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(Arc::new(TestTransport::new("env-only-no-fallback-segment")))
        .local_memory(storage_config_with_bytes(128))
        .build(test_future_expiry_ms())
        .expect("client build should succeed without cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");

    let scoped = metadata
        .for_tenant("default")
        .expect("for_tenant should return Some");
    let device = mooncake_store_core::ColdTierDeviceRecord {
        device_id: client.runtime_id().stable_id.0.clone(),
        stable_id: client.runtime_id().stable_id.0.clone(),
        epoch: Some(client.runtime_id().epoch.0),
        cold_tier_id: client.runtime_id().stable_id.0.clone(),
        kind: "ssd".to_string(),
        target: mooncake_store_core::ColdTierTargetSpec::Directory {
            path: cold_root.display().to_string(),
        },
        root_dir: Some(cold_root.display().to_string()),
        state: mooncake_store_core::ColdTierDeviceState::Healthy,
        capacity_bytes: Some(1024 * 1024),
        used_bytes: 0,
        reserved_bytes: 0,
        failure_count: 0,
        last_error: None,
        tags: Vec::new(),
        updated_at_ms: now_ms(),
    };
    assert!(matches!(
        scoped
            .put_cold_tier_device_if_absent(&device)
            .expect("device publish should succeed"),
        mooncake_store_core::ColdTierPutDeviceResult::Created(_)
    ));
    refresh_cold_tier_device_cache(
        scoped.as_ref(),
        client.storage_owner.cold_tier_devices.cache(),
        "test_env_only_no_fallback",
    )
    .expect("cache refresh should succeed");

    assert!(
        client
            .storage_owner
            .cold_tier_devices
            .local_device_ids()
            .is_empty(),
        "env-only cold tier startup must not register a fallback backend"
    );
    assert!(
        !client
            .storage_owner
            .cold_tier_devices
            .has_usable_device(scoped.as_ref())
            .expect("usable-device check should succeed"),
        "metadata-only device must not make eviction use cold offload without a local backend"
    );

    client
        .put("env-only-evict", b"env-only-payload")
        .expect("put should succeed");
    let route_before_evict = client
        .query_route("env-only-evict")
        .expect("route query should succeed")
        .expect("hot route should exist before eviction");
    assert_eq!(route_before_evict.replicas.len(), 1);
    assert!(
        route_before_evict.cold_backing.is_none(),
        "env-only/no-backend put must publish a hot-only route before eviction"
    );

    assert!(
        client
            .storage_owner
            .evict_one_blocking(None)
            .expect("manual eviction should succeed"),
        "manual eviction should find the hot victim"
    );
    assert!(
        client
            .query_route("env-only-evict")
            .expect("route query should succeed")
            .is_none(),
        "without a local cold backend, evicting the last replica should delete the route"
    );

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"storage_owner_evict_one\",status=\"ok\""));
    assert!(
        !metrics.contains("operation=\"storage_owner_eviction_forced_offload\""),
        "env-only/no-backend eviction must not force cold offload"
    );
    assert!(
        !metrics.contains("operation=\"storage_owner_background_offload\""),
        "env-only/no-backend eviction must not enqueue background offload"
    );

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn configured_cold_tier_eviction_materializes_cold_backing() {
    let _metrics_guard = metrics_test_lock().lock();
    reset_metrics();
    let _cold_tier_env = enable_cold_tier_for_test();
    let cold_root = cold_tier_test_root("configured-eviction-materializes");
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(metadata, "configured-eviction-materializes")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "configured-eviction-materializes-segment",
        )))
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(cold_tier_test_config_with_root(
            "configured-eviction-materializes",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::EvictTriggered)
        .build(test_future_expiry_ms())
        .expect("client build should succeed with cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");

    client
        .put("configured-evict", b"configured-payload")
        .expect("put should succeed");
    let route_before_evict = client
        .query_route("configured-evict")
        .expect("route query should succeed")
        .expect("hot route should exist before eviction");
    assert_eq!(route_before_evict.replicas.len(), 1);
    assert!(
        route_before_evict.cold_backing.is_none(),
        "evict-triggered mode starts as hot-only; eviction is what creates cold_backing"
    );

    assert!(
        client
            .storage_owner
            .evict_one_blocking(None)
            .expect("manual eviction should succeed"),
        "manual eviction should find the hot victim"
    );

    let route = client
        .query_route("configured-evict")
        .expect("route query should succeed")
        .expect("cold-backed route should remain after evicting last DRAM replica");
    assert!(route.replicas.is_empty());
    assert!(route.cold_backing.as_ref().is_some_and(|backing| {
        backing.state == mooncake_store_core::ColdBackingState::Materialized
    }));

    let metrics = render_prometheus_metrics();
    assert!(metrics.contains("operation=\"storage_owner_evict_one\",status=\"ok\""));
    assert!(
        metrics.contains("operation=\"storage_owner_eviction_forced_offload\",status=\"ok\""),
        "configured cold-tier eviction should force materialization before deleting the last replica"
    );

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn allocator_pressure_batches_pending_cold_victims() {
    let _metrics_guard = metrics_test_lock().lock();
    reset_metrics();
    let _cold_tier_env = enable_cold_tier_for_test();
    let cold_root = cold_tier_test_root("background-eviction-batch-offload");
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "background-eviction-batch-offload")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "background-eviction-batch-offload-segment",
        )))
        .local_memory(
            storage_config_with_bytes(128).eviction_poll_interval(Duration::ZERO),
        )
        .cold_tier_target(cold_tier_test_config_with_root(
            "background-eviction-batch-offload",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed with cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");
    client.cold_tier.shutdown();

    let keys = (0..4)
        .map(|index| {
            let key = format!("background-eviction-batch-{index}");
            client
                .put(&key, &[index as u8; 20])
                .expect("put should succeed");
            key
        })
        .collect::<Vec<_>>();

    let _generic_materializer_guard = client
        .storage_owner
        .pending_offloads
        .try_begin_materialization()
        .expect("test should reserve the generic materializer guard");

    assert!(
        client
            .storage_owner
            .evict_one_for_reservation_blocking(None)
            .expect("allocator-pressure eviction should succeed")
    );
    for key in &keys {
        let route = client
            .query_route(key)
            .expect("route query should succeed")
            .expect("cold-backed route should remain after eviction");
        assert!(route.replicas.is_empty());
        assert!(route.cold_backing.as_ref().is_some_and(|backing| {
            backing.state == mooncake_store_core::ColdBackingState::Materialized
        }));
    }

    let metrics = render_prometheus_metrics();
    let backend_batches = prometheus_counter_value(
        &metrics,
        "mooncake_store_operation_total{",
        &[
            "operation=\"cold_offload_backend_write_batch\"",
            "status=\"ok\"",
        ],
    );
    assert_eq!(backend_batches, 1, "pending victims should share one backend write batch");

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn allocator_pressure_batches_hot_only_cold_victims() {
    let _metrics_guard = metrics_test_lock().lock();
    reset_metrics();
    let _cold_tier_env = enable_cold_tier_for_test();
    let cold_root = cold_tier_test_root("background-eviction-hot-only-batch");
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "background-eviction-hot-only-batch")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "background-eviction-hot-only-batch-segment",
        )))
        .local_memory(
            storage_config_with_bytes(256).eviction_poll_interval(Duration::ZERO),
        )
        .cold_tier_target(cold_tier_test_config_with_root(
            "background-eviction-hot-only-batch",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::EvictTriggered)
        .build(test_future_expiry_ms())
        .expect("client build should succeed with cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");
    client.cold_tier.shutdown();

    let keys = (0..8)
        .map(|index| {
            let key = format!("background-eviction-hot-only-{index}");
            client
                .put(&key, &[index as u8; 20])
                .expect("put should succeed");
            let route = client
                .query_route(&key)
                .expect("route query should succeed")
                .expect("hot route should exist");
            assert!(route.cold_backing.is_none());
            key
        })
        .collect::<Vec<_>>();

    assert!(
        client
            .storage_owner
            .evict_one_for_reservation_blocking(None)
            .expect("allocator-pressure eviction should succeed")
    );
    for key in &keys {
        let route = client
            .query_route(key)
            .expect("route query should succeed")
            .expect("cold-backed route should remain after eviction");
        assert!(route.replicas.is_empty());
        assert!(route.cold_backing.as_ref().is_some_and(|backing| {
            backing.state == mooncake_store_core::ColdBackingState::Materialized
        }));
    }

    let metrics = render_prometheus_metrics();
    let backend_batches = prometheus_counter_value(
        &metrics,
        "mooncake_store_operation_total{",
        &[
            "operation=\"cold_offload_backend_write_batch\"",
            "status=\"ok\"",
        ],
    );
    assert_eq!(
        backend_batches, 2,
        "hot-only victims should drain consecutive device-sized waves without admission backoff"
    );

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn background_materializer_drains_single_target_admission_waves() {
    let _metrics_guard = metrics_test_lock().lock();
    reset_metrics();
    let _cold_tier_env = enable_cold_tier_for_test();
    let cold_root = cold_tier_test_root("background-single-target-admission-waves");
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "background-single-target-admission-waves")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "background-single-target-admission-waves-segment",
        )))
        .local_memory(storage_config_with_bytes(256).eviction_poll_interval(Duration::ZERO))
        .cold_tier_target(cold_tier_test_config_with_root(
            "background-single-target-admission-waves",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed with cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");
    client.cold_tier.shutdown();

    for index in 0..8 {
        client
            .put(
                &format!("background-single-target-admission-wave-{index}"),
                &[index as u8; 20],
            )
            .expect("put should succeed");
    }
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(8)
            .expect("bounded background materialization should succeed"),
        8,
        "one background pass should consume every requested single-target wave"
    );

    let metrics = render_prometheus_metrics();
    let backend_batches = prometheus_counter_value(
        &metrics,
        "mooncake_store_operation_total{",
        &[
            "operation=\"cold_offload_backend_write_batch\"",
            "status=\"ok\"",
        ],
    );
    assert_eq!(backend_batches, 2, "eight writes should use two four-entry waves");
    let admission_retries = prometheus_counter_value(
        &metrics,
        "mooncake_store_cold_tier_operation_total{",
        &[
            "operation=\"offload\"",
            "result=\"retry\"",
            "error_kind=\"admission\"",
        ],
    );
    assert_eq!(
        admission_retries, 1,
        "a full device window should defer the remaining entries as one wave"
    );

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn stale_pending_entry_does_not_reoffload_materialized_route() {
    let _metrics_guard = metrics_test_lock().lock();
    reset_metrics();
    let _cold_tier_env = enable_cold_tier_for_test();
    let cold_root = cold_tier_test_root("stale-pending-materialized-route");
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "stale-pending-materialized-route")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "stale-pending-materialized-route-segment",
        )))
        .local_memory(
            storage_config_with_bytes(128).eviction_poll_interval(Duration::ZERO),
        )
        .cold_tier_target(cold_tier_test_config_with_root(
            "stale-pending-materialized-route",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed with cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");
    client.cold_tier.shutdown();

    client
        .put("stale-pending-materialized-route", b"payload")
        .expect("put should succeed");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads()
            .expect("initial materialization should succeed"),
        1
    );
    let materialized = client
        .query_route("stale-pending-materialized-route")
        .expect("route query should succeed")
        .expect("materialized route should exist");
    let backing_before = materialized
        .cold_backing
        .as_ref()
        .expect("materialized route should have cold backing")
        .clone();
    let length = materialized
        .replicas
        .first()
        .expect("hot replica should remain")
        .length;
    assert!(client.storage_owner.pending_offloads.push(
        materialized.key.clone(),
        materialized.version,
        Some(length),
    ));

    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads()
            .expect("stale materialization pass should succeed"),
        0,
        "a stale queue entry must not write a new backing for an already materialized route"
    );
    let after = client
        .query_route("stale-pending-materialized-route")
        .expect("route query should succeed")
        .expect("materialized route should remain");
    assert_eq!(after.version, materialized.version);
    assert_eq!(after.cold_backing.as_ref(), Some(&backing_before));
    assert_eq!(
        client.storage_owner.pending_offloads.len(),
        0,
        "the stale queue entry should be retired after observing materialized backing"
    );

    let metrics = render_prometheus_metrics();
    let backend_outputs = prometheus_counter_value(
        &metrics,
        "mooncake_store_operation_bytes_out_total{",
        &[
            "operation=\"cold_offload_backend_write_batch\"",
            "status=\"ok\"",
        ],
    );
    assert_eq!(backend_outputs, 1, "the stale entry must not write again");

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn forced_eviction_preserves_in_flight_offload_owner() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let cold_root = cold_tier_test_root("forced-eviction-in-flight-owner");
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "forced-eviction-in-flight-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "forced-eviction-in-flight-owner-segment",
        )))
        .local_memory(
            storage_config_with_bytes(128).eviction_poll_interval(Duration::ZERO),
        )
        .cold_tier_target(cold_tier_test_config_with_root(
            "forced-eviction-in-flight-owner",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed with cold tier target");
    client
        .register_local_memory()
        .expect("local memory should register");
    client.cold_tier.shutdown();

    client
        .put("forced-eviction-in-flight-owner", b"payload")
        .expect("put should succeed");
    let route = client
        .query_route("forced-eviction-in-flight-owner")
        .expect("route query should succeed")
        .expect("pending route should exist");
    let claimed = client
        .storage_owner
        .pending_offloads
        .claim(&route.key, route.version)
        .expect("test should own the pending offload");

    assert!(
        !client
            .storage_owner
            .evict_one_blocking(None)
            .expect("eviction should safely skip an in-flight offload"),
        "forced eviction must not steal another materializer's in-flight key"
    );
    let pending = client
        .query_route("forced-eviction-in-flight-owner")
        .expect("route query should succeed")
        .expect("pending route should remain hot");
    assert_eq!(pending.replicas.len(), 1);
    assert!(pending.cold_backing.as_ref().is_some_and(|backing| {
        backing.state == mooncake_store_core::ColdBackingState::PendingOffload
    }));

    client.storage_owner.pending_offloads.retry(claimed);
    assert!(
        client
            .storage_owner
            .evict_one_blocking(None)
            .expect("eviction should proceed after the owner releases the key")
    );
    let materialized = client
        .query_route("forced-eviction-in-flight-owner")
        .expect("route query should succeed")
        .expect("cold-only route should remain");
    assert!(materialized.replicas.is_empty());
    assert!(materialized.cold_backing.as_ref().is_some_and(|backing| {
        backing.state == mooncake_store_core::ColdBackingState::Materialized
    }));

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn embedded_client_remote_cold_only_read_promotes_on_owner() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("embedded-remote-cold-restore");
    let transport = Arc::new(TestTransport::new("embedded-remote-cold-owner-segment"));
    let owner_transport_factory = transport.factory();
    let owner = StoreClientBuilder::new(metadata.clone(), "embedded-remote-cold-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(transport.clone())
        .transport_factory(owner_transport_factory)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(cold_tier_test_config_with_root(
            "embedded-remote-cold-restore",
            cold_root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("owner build should succeed");
    owner
        .register_local_memory()
        .expect("owner local memory should register");

    let payload = b"embedded-remote-cold-payload";
    let batch_payload = b"embedded-remote-cold-batch";
    owner
        .put("embedded-remote-cold-key", payload)
        .expect("owner put should succeed");
    owner
        .put("embedded-remote-cold-batch-key", batch_payload)
        .expect("owner batch-target put should succeed");
    wait_for_materialized_cold_backing(&owner, "embedded-remote-cold-key");
    wait_for_materialized_cold_backing(&owner, "embedded-remote-cold-batch-key");
    force_cold_only_route(&owner, "embedded-remote-cold-key");
    force_cold_only_route(&owner, "embedded-remote-cold-batch-key");

    let reader_transport = Arc::new(transport.peer("embedded-remote-cold-reader-segment"));
    let reader = StoreClientBuilder::new(metadata, "embedded-remote-cold-reader")
        .state(ClientLifecycleState::Active)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config_with_bytes(128))
        .build(test_future_expiry_ms())
        .expect("reader build should succeed without cold tier target");
    reader
        .register_local_memory()
        .expect("reader local memory should register");
    assert_eq!(
        reader
            .get("embedded-remote-cold-key")
            .expect("reader should trigger owner cold restore and read the promoted replica"),
        payload
    );
    assert_eq!(
        reader
            .batch_get(&[ObjectRef::new("embedded-remote-cold-batch-key")])
            .expect("batch get should trigger owner cold restore and read the promoted replica"),
        vec![batch_payload.to_vec()]
    );
    let route =
        wait_for_route_replica_on_owner(&reader, "embedded-remote-cold-key", &owner.lease.runtime);
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, owner.lease.runtime);

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn put_and_batch_put_survive_cold_offload_and_memory_eviction() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("put-batch-evict-restore");
    let transport = Arc::new(TestTransport::new("put-batch-evict-restore-segment"));
    let mut client = StoreClientBuilder::new(metadata, "put-batch-evict-restore")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(96))
        .cold_tier_target(cold_tier_test_config_with_root(
            "put-batch-evict-restore",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");
    // This test explicitly drives the existing offload path after stopping
    // the periodic worker. Background scheduling remains covered by the
    // Other cold-tier integration tests retain background-worker coverage,
    // while this flow stays independent of polling
    // and queue-claim races.
    client.cold_tier.shutdown();

    let single_payload = vec![1u8; 24];
    let batch_payload_a = vec![2u8; 24];
    let batch_payload_b = vec![3u8; 24];

    client
        .put("evict-restore-single", &single_payload)
        .expect("single put should succeed");
    client
        .batch_put(&[
            PutRequest::new("evict-restore-batch-a", &batch_payload_a),
            PutRequest::new("evict-restore-batch-b", &batch_payload_b),
        ])
        .expect("batch put should succeed");

    let initial_keys = [
        "evict-restore-single",
        "evict-restore-batch-a",
        "evict-restore-batch-b",
    ];
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads()
            .expect("synchronous cold offload should succeed"),
        initial_keys.len()
    );
    for key in initial_keys {
        let route = client
            .query_route(key)
            .expect("route query should succeed")
            .expect("route should exist");
        let cold_backing = route.cold_backing.as_ref().unwrap_or_else(|| {
            panic!(
                "route should have a cold backing after synchronous offload: key={key} route={route:?}"
            )
        });
        assert_eq!(
            cold_backing.state,
            mooncake_store_core::ColdBackingState::Materialized,
            "cold backing did not materialize: key={key} backing={cold_backing:?} route={route:?}"
        );
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let evicted = initial_keys.iter().all(|key| {
            client
                .query_route(key)
                .expect("route query should succeed")
                .expect("route should exist")
                .replicas
                .is_empty()
        });
        if evicted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "initial objects should be evicted from hot memory"
        );
        assert!(
            client
                .storage_owner
                .evict_one_blocking(None)
                .expect("manual eviction should succeed"),
            "manual eviction should find a hot victim"
        );
    }

    let mut single_buffer = vec![0u8; single_payload.len()];
    assert_eq!(
        client
            .get_into("evict-restore-single", &mut single_buffer)
            .expect("single get should restore cold-only payload"),
        single_payload.len()
    );
    assert_eq!(single_buffer, single_payload);
    let restored = client
        .batch_get(&[
            ObjectRef::new("evict-restore-batch-a"),
            ObjectRef::new("evict-restore-batch-b"),
        ])
        .expect("batch get should restore cold-only payloads");
    assert_eq!(restored, vec![batch_payload_a, batch_payload_b]);

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn batch_is_readable_uses_bounded_lookup_for_evicted_cold_only_entry() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("batch-readable-evict-range");
    let transport = Arc::new(TestTransport::new("batch-readable-evict-range-segment"));
    let mut client = StoreClientBuilder::new(metadata, "batch-readable-evict-range")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(96))
        .cold_tier_target(cold_tier_test_config_with_root(
            "batch-readable-evict-range",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let bounded_reads = Arc::new(AtomicUsize::new(0));
    let full_reads = Arc::new(AtomicUsize::new(0));
    client.route_directory = Arc::new(CountingRouteDirectory {
        inner: client.route_directory.clone(),
        miss_bounded_reads: AtomicBool::new(false),
        miss_full_reads: AtomicBool::new(false),
        bounded_read_hook: Mutex::new(None),
        owner_visit_hook: Mutex::new(None),
        before_single_cas_hook: Mutex::new(None),
        after_single_cas_hook: Mutex::new(None),
        injected_routes: Mutex::new(BTreeMap::new()),
        get_object_routes_calls: AtomicUsize::new(0),
        bounded_reads: bounded_reads.clone(),
        full_reads: full_reads.clone(),
        single_cas_calls: AtomicUsize::new(0),
        batch_cas_calls: AtomicUsize::new(0),
        max_cas_batch_size: AtomicUsize::new(0),
        owner_list_scans: AtomicUsize::new(0),
        allocation_reference_key_queries: AtomicUsize::new(0),
        fail_allocation_reference_key_queries: AtomicBool::new(false),
    });

    let payload = vec![7u8; 24];
    client
        .put("batch-readable-evict-target", &payload)
        .expect("target put should succeed");
    let cold_backing = wait_for_materialized_cold_backing(&client, "batch-readable-evict-target");
    assert_eq!(
        cold_backing.state,
        mooncake_store_core::ColdBackingState::Materialized
    );

    force_cold_only_route(&client, "batch-readable-evict-target");
    bounded_reads.store(0, Ordering::Relaxed);
    full_reads.store(0, Ordering::Relaxed);

    assert_eq!(
        client
            .batch_is_readable(&[ObjectRef::new("batch-readable-evict-target")])
            .expect("batch_is_readable should succeed for evicted cold-only entry"),
        vec![true]
    );
    assert_eq!(bounded_reads.load(Ordering::Relaxed), 1);
    assert_eq!(full_reads.load(Ordering::Relaxed), 0);

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn batch_is_exist_reports_true_for_evicted_cold_only_entry() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("batch-exist-evict-range");
    let transport = Arc::new(TestTransport::new("batch-exist-evict-range-segment"));
    let client = StoreClientBuilder::new(metadata, "batch-exist-evict-range")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(96))
        .cold_tier_target(cold_tier_test_config_with_root(
            "batch-exist-evict-range",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = vec![7u8; 24];
    client
        .put("batch-exist-evict-target", &payload)
        .expect("target put should succeed");
    let cold_backing = wait_for_materialized_cold_backing(&client, "batch-exist-evict-target");
    assert_eq!(
        cold_backing.state,
        mooncake_store_core::ColdBackingState::Materialized
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let route = client
            .query_route("batch-exist-evict-target")
            .expect("route query should succeed")
            .expect("route should exist");
        let evicted = route.replicas.is_empty()
            && route.cold_backing.as_ref().is_some_and(|backing| {
                backing.state == mooncake_store_core::ColdBackingState::Materialized
            });
        if evicted {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "target object should be evicted from hot memory"
        );
        assert!(
            client
                .storage_owner
                .evict_one_blocking(None)
                .expect("manual eviction should succeed"),
            "manual eviction should find a hot victim"
        );
    }

    assert_eq!(
        client
            .batch_is_exist(&[ObjectRef::new("batch-exist-evict-target")])
            .expect("batch_is_exist should succeed for evicted cold-only entry"),
        vec![true]
    );

    let _ = std::fs::remove_dir_all(cold_root);
}

#[test]
fn batch_get_restores_cold_only_entry_mixed_with_hot_entry() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("mixed-batch-cold-restore-segment"));
    let client = StoreClientBuilder::new(metadata, "mixed-batch-cold-restore")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(cold_tier_test_config("mixed-batch-cold-restore"))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let hot_payload = b"hot-payload";
    let cold_payload = b"cold-payload";
    client
        .put("mixed-hot", hot_payload)
        .expect("hot put should succeed");
    client
        .put("mixed-cold", cold_payload)
        .expect("cold put should succeed");
    wait_for_materialized_cold_backing(&client, "mixed-cold");
    force_cold_only_route(&client, "mixed-cold");

    assert!(client
        .is_exist("mixed-cold")
        .expect("cold-only is_exist should succeed"));
    assert_eq!(
        client
            .get_size("mixed-cold")
            .expect("cold-only get_size should succeed"),
        cold_payload.len()
    );
    assert_eq!(
        client
            .batch_is_exist(&[ObjectRef::new("mixed-hot"), ObjectRef::new("mixed-cold")])
            .expect("batch_is_exist should succeed"),
        vec![true, true]
    );
    assert_eq!(
        client
            .batch_is_readable(&[ObjectRef::new("mixed-hot"), ObjectRef::new("mixed-cold")])
            .expect("batch_is_readable should succeed"),
        vec![true, true]
    );

    let payloads = client
        .batch_get(&[ObjectRef::new("mixed-hot"), ObjectRef::new("mixed-cold")])
        .expect("mixed batch_get should restore cold-only entry");
    assert_eq!(payloads[0], hot_payload);
    assert_eq!(payloads[1], cold_payload);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_put_conflict_removes_unpublished_pending_source() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let metadata: Arc<dyn MetadataBackend> =
        Arc::new(BlockingCasMetadataBackend::with_empty_conflict(
            inner,
            "default::ns/default/default/pending-conflict-key",
        ));
    let root = cold_tier_test_root("pending-conflict-cleanup");
    let transport = Arc::new(TestTransport::new("pending-conflict-segment"));
    let client = StoreClientBuilder::new(metadata, "pending-conflict-cleanup")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "pending-conflict-cleanup",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let error = client
        .put("pending-conflict-key", b"pending-conflict-payload")
        .expect_err("injected route conflict should fail put");
    assert!(error.to_string().contains("route update lost race"));

    let pending_dir = root
        .join("__pending__")
        .join(encode_backend_component("pending-conflict-cleanup"));
    assert!(
        !pending_dir.exists()
            || fs::read_dir(&pending_dir)
                .expect("pending dir should list")
                .next()
                .is_none(),
        "unpublished pending source should be removed after CAS conflict"
    );
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_put_cas_error_removes_unpublished_pending_source() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let metadata: Arc<dyn MetadataBackend> =
        Arc::new(BlockingCasMetadataBackend::with_route_error(
            inner,
            "default::ns/default/default/pending-error-key",
        ));
    let root = cold_tier_test_root("pending-error-cleanup");
    let transport = Arc::new(TestTransport::new("pending-error-segment"));
    let client = StoreClientBuilder::new(metadata, "pending-error-cleanup")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "pending-error-cleanup",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let error = client
        .put("pending-error-key", b"pending-error-payload")
        .expect_err("injected route CAS error should fail put");
    assert!(error.to_string().contains("injected CAS error"));

    let pending_dir = root
        .join("__pending__")
        .join(encode_backend_component("pending-error-cleanup"));
    assert!(
        !pending_dir.exists()
            || fs::read_dir(&pending_dir)
                .expect("pending dir should list")
                .next()
                .is_none(),
        "unpublished pending source should be removed after CAS error"
    );
}

/// E2E: cold-only data must remain visible and readable with the readable filter active.
///
/// This exercises the full lifecycle:
///   1. put → offload → materialized cold backing
///   2. evict hot replicas → cold-only route (no hot replicas)
///   3. activate readable filter (simulating membership sync)
///   4. assert: is_exist, batch_is_exist, get_size, get, batch_get all succeed
///
/// Regression test for: readable filter treated cold-only routes as "unreadable" because
/// `contains_readable_many` only checked hot replicas, not materialized cold backing.
#[test]
fn cold_only_data_visible_and_readable_with_active_readable_filter() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let cold_root = cold_tier_test_root("cold-only-readable-filter-e2e");
    let transport = Arc::new(TestTransport::new("cold-only-readable-filter-e2e-seg"));
    let client = StoreClientBuilder::new(metadata.clone(), "cold-only-readable-filter-e2e")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(96))
        .cold_tier_target(cold_tier_test_config_with_root(
            "cold-only-readable-filter-e2e",
            cold_root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::Passthrough)
        .live_client_sync_interval(Duration::from_secs(60))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    // --- Phase 1: put data and wait for cold materialization ---
    let payload_a = b"cold-only-visible-alpha";
    let payload_b = b"cold-only-visible-bravo";
    client
        .put("cold-vis-a", payload_a)
        .expect("put a should succeed");
    client
        .put("cold-vis-b", payload_b)
        .expect("put b should succeed");

    for key in ["cold-vis-a", "cold-vis-b"] {
        let cold_backing = wait_for_materialized_cold_backing(&client, key);
        assert_eq!(
            cold_backing.state,
            mooncake_store_core::ColdBackingState::Materialized,
            "{key} should be materialized on cold tier"
        );
    }

    // --- Phase 2: evict hot replicas to create cold-only routes ---
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let all_cold_only = ["cold-vis-a", "cold-vis-b"].iter().all(|key| {
            client
                .query_route(key)
                .expect("route query should succeed")
                .expect("route should exist")
                .replicas
                .is_empty()
        });
        if all_cold_only {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "objects should be evicted from hot memory within deadline"
        );
        assert!(
            client
                .storage_owner
                .evict_one_blocking(None)
                .expect("manual eviction should succeed"),
            "manual eviction should find a hot victim"
        );
    }

    // Verify cold-only route shape: no replicas, materialized cold backing.
    for key in ["cold-vis-a", "cold-vis-b"] {
        let route = client
            .query_route(key)
            .expect("route query should succeed")
            .expect("route should exist");
        assert!(
            route.replicas.is_empty(),
            "{key} should have no hot replicas"
        );
        assert!(
            route
                .cold_backing
                .as_ref()
                .is_some_and(|cb| cb.state == mooncake_store_core::ColdBackingState::Materialized),
            "{key} should have materialized cold backing"
        );
    }

    // --- Phase 3: activate the readable filter ---
    // This simulates what the membership sync does: only the current runtime is "readable".
    let namespace = metadata.route_namespace();
    mooncake_store_route::update_readable_filter(
        &namespace,
        Some(BTreeSet::from([client.runtime_id().clone()])),
    );
    assert!(
        mooncake_store_route::is_readable_filter_active(&namespace),
        "readable filter should be active"
    );

    // --- Phase 4: assert cold-only data is visible and readable ---

    // 4a. is_exist (single)
    assert!(
        client
            .is_exist("cold-vis-a")
            .expect("is_exist should succeed"),
        "cold-only route must be visible via is_exist with readable filter active"
    );

    // 4b. batch_is_exist
    assert_eq!(
        client
            .batch_is_exist(&[ObjectRef::new("cold-vis-a"), ObjectRef::new("cold-vis-b")])
            .expect("batch_is_exist should succeed"),
        vec![true, true],
        "cold-only routes must be visible via batch_is_exist with readable filter active"
    );

    // 4c. get_size
    assert_eq!(
        client
            .get_size("cold-vis-a")
            .expect("get_size should succeed for cold-only route"),
        payload_a.len(),
        "cold-only get_size must return correct payload length"
    );

    // 4d. get (single — triggers cold restore)
    let mut buffer = vec![0u8; payload_a.len()];
    assert_eq!(
        client
            .get_into("cold-vis-a", &mut buffer)
            .expect("get_into should succeed for cold-only route"),
        payload_a.len()
    );
    assert_eq!(buffer, payload_a, "restored cold-only payload must match");

    // 4e. batch_get (triggers cold restore for remaining cold-only entry)
    let payloads = client
        .batch_get(&[ObjectRef::new("cold-vis-b")])
        .expect("batch_get should restore cold-only payload");
    assert_eq!(
        payloads[0], payload_b,
        "batch_get restored payload must match"
    );

    // --- Cleanup ---
    mooncake_store_route::update_readable_filter(&namespace, None);
    let _ = std::fs::remove_dir_all(cold_root);
}
