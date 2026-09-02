// Cold tier admin service tests.
// Included via `include!()` inside the `#[cfg(test)] mod tests` block in service.rs.

fn sample_cold_tier_device(device_id: &str) -> mooncake_store_core::ColdTierDeviceRecord {
    mooncake_store_core::ColdTierDeviceRecord {
        device_id: device_id.to_string(),
        stable_id: "storage-a".to_string(),
        epoch: Some(1),
        cold_tier_id: device_id.to_string(),
        kind: "ssd".to_string(),
        target: mooncake_store_core::ColdTierTargetSpec::Directory {
            path: format!("/tmp/{device_id}"),
        },
        root_dir: Some(format!("/tmp/{device_id}")),
        state: ColdTierDeviceState::Healthy,
        capacity_bytes: Some(1024),
        used_bytes: 12,
        reserved_bytes: 0,
        failure_count: 0,
        last_error: None,
        tags: Vec::new(),
        updated_at_ms: now_ms(),
    }
}

fn sample_cold_backing(
    device_id: &str,
    state: ColdBackingState,
) -> mooncake_store_core::ColdBackingRoute {
    mooncake_store_core::ColdBackingRoute {
        owner: ClientRuntimeId::new("storage-a", ClientEpoch(1)),
        cold_tier_id: device_id.to_string(),
        object_locator: "cold-object".to_string(),
        length: 12,
        checksum: None,
        state,
        replicas: Vec::new(),
    }
}

#[test]
fn cold_tier_offload_trigger_runs_as_async_task() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("storage-a", 1))
        .expect("old lease should store");
    backend
        .upsert_client_lease(&live_lease("storage-a", 3))
        .expect("new lease should store");
    let rpc = Arc::new(FakeMigrationRpc {
        offload_results: Arc::new(Mutex::new(vec![Ok(2), Ok(0)].into())),
        ..FakeMigrationRpc::default()
    });
    let service =
        test_migration_service_with_rpc(backend, rpc.clone(), MigrationQueueConfig::default());

    let response = service
        .trigger_cold_tier_offload(TriggerColdTierOffloadRequest {
            stable_id: "storage-a".to_string(),
            max_tasks: Some(8),
        })
        .expect("trigger should submit task");

    assert_eq!(response.task_id, "cold-tier-offload-1");
    assert_eq!(response.stable_id, "storage-a");
    assert_eq!(response.epoch, None);
    assert_eq!(response.max_tasks, 8);
    assert_eq!(response.state, ColdTierOffloadTaskState::Pending);

    let started = now_ms();
    let final_status = loop {
        let status = service
            .get_cold_tier_offload_task("cold-tier-offload-1")
            .expect("task should exist");
        if status.state == ColdTierOffloadTaskState::Succeeded {
            break status;
        }
        assert!(
            now_ms().saturating_sub(started) < 5_000,
            "offload task should finish, current={:?}",
            status.state
        );
        sleep(Duration::from_millis(10));
    };
    assert_eq!(final_status.epoch, Some(3));
    assert_eq!(final_status.materialized, 2);
    assert_eq!(
        *rpc.offload_calls.lock(),
        vec![("storage-a".to_string(), 8), ("storage-a".to_string(), 6)]
    );
}

#[test]
fn cold_tier_offload_trigger_validates_request() {
    let service = test_service();
    let missing = service
        .trigger_cold_tier_offload(TriggerColdTierOffloadRequest {
            stable_id: "".to_string(),
            max_tasks: Some(1),
        })
        .expect_err("missing stable id should fail");
    assert!(missing.to_string().contains("missing stable_id"));
    let zero = service
        .trigger_cold_tier_offload(TriggerColdTierOffloadRequest {
            stable_id: "storage-a".to_string(),
            max_tasks: Some(0),
        })
        .expect_err("zero max tasks should fail");
    assert!(zero.to_string().contains("max_tasks"));
}

#[test]
fn cold_tier_offload_finished_tasks_are_pruned_after_retention_window() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("storage-a", 1))
        .expect("lease should store");
    let rpc = Arc::new(FakeMigrationRpc {
        offload_results: Arc::new(Mutex::new(vec![Ok(1), Ok(0)].into())),
        ..FakeMigrationRpc::default()
    });
    let service = test_migration_service_with_rpc(backend, rpc, MigrationQueueConfig::default());

    service
        .trigger_cold_tier_offload(TriggerColdTierOffloadRequest {
            stable_id: "storage-a".to_string(),
            max_tasks: Some(2),
        })
        .expect("trigger should submit task");

    let started = now_ms();
    loop {
        let status = service
            .get_cold_tier_offload_task("cold-tier-offload-1")
            .expect("task should exist before pruning");
        if status.state == ColdTierOffloadTaskState::Succeeded {
            break;
        }
        assert!(
            now_ms().saturating_sub(started) < 5_000,
            "offload task should finish before pruning"
        );
        sleep(Duration::from_millis(10));
    }

    {
        let mut tasks = service.cold_tier_offloads.state.tasks.lock();
        let record = tasks
            .get_mut("cold-tier-offload-1")
            .expect("finished task should remain until retention expires");
        record.updated_at_ms = record
            .updated_at_ms
            .saturating_sub(COLD_TIER_OFFLOAD_TASK_RETAIN_MS + 1);
    }

    let listed = service.list_cold_tier_offload_tasks();
    assert_eq!(listed.count, 0);
    assert!(listed.tasks.is_empty());
    let error = service
        .get_cold_tier_offload_task("cold-tier-offload-1")
        .expect_err("expired finished task should be pruned on lookup");
    assert!(matches!(error, StoreError::NotFound(_)));
}

#[test]
fn cold_tier_unregister_drains_then_force_marks_pending_delete() {
    let service = test_service();
    let device = sample_cold_tier_device("ssd-drain");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");
    let mut route = sample_active_route("cold-drain-object", "segment-a", &[]);
    route.cold_backing = Some(sample_cold_backing(
        "ssd-drain",
        ColdBackingState::Materialized,
    ));
    service
        .backend()
        .compare_and_swap_object_route(&route.key, None, Some(&route))
        .expect("route should seed");

    let draining = service
        .unregister_cold_tier_device(None, "ssd-drain", ColdTierUnregisterRequest::default())
        .expect("non-force unregister should enter draining");
    assert_eq!(draining.state, ColdTierDeviceState::Draining);
    assert_eq!(draining.blocked_objects, 1);
    let stored = service
        .backend()
        .get_cold_tier_device("ssd-drain")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Draining);

    let forced = service
        .unregister_cold_tier_device(
            None,
            "ssd-drain",
            ColdTierUnregisterRequest {
                dry_run: false,
                force: true,
            },
        )
        .expect("force unregister should mark materialized hot-backed object pending delete");
    assert_eq!(forced.state, ColdTierDeviceState::Unregistered);
    assert_eq!(forced.blocked_objects, 1);
    let forced_device = service
        .backend()
        .get_cold_tier_device("ssd-drain")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(forced_device.used_bytes, 12);
    let updated = service
        .backend()
        .get_object_route(&route.key)
        .expect("route should load")
        .expect("route should remain");
    assert_eq!(
        updated
            .cold_backing
            .as_ref()
            .expect("cold backing should remain for GC")
            .state,
        ColdBackingState::PendingDelete
    );
}

#[test]
fn cold_tier_unregister_rejects_inflight_reservations() {
    let service = test_service();
    let mut device = sample_cold_tier_device("ssd-inflight");
    device.reserved_bytes = 4;
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    let error = service
        .unregister_cold_tier_device(None, "ssd-inflight", ColdTierUnregisterRequest::default())
        .expect_err("unregister should reject inflight reservations");
    assert!(error
        .to_string()
        .contains("reserved by inflight operations"));
    let stored = service
        .backend()
        .get_cold_tier_device("ssd-inflight")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Healthy);
    assert_eq!(stored.reserved_bytes, 4);
}

#[test]
fn cold_tier_force_unregister_rejects_cold_only_backing() {
    let service = test_service();
    let device = sample_cold_tier_device("ssd-cold-only");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");
    let mut route = sample_active_route("cold-only-object", "", &[]);
    route.cold_backing = Some(sample_cold_backing(
        "ssd-cold-only",
        ColdBackingState::Materialized,
    ));
    service
        .backend()
        .compare_and_swap_object_route(&route.key, None, Some(&route))
        .expect("route should seed");

    let error = service
        .unregister_cold_tier_device(
            None,
            "ssd-cold-only",
            ColdTierUnregisterRequest {
                dry_run: false,
                force: true,
            },
        )
        .expect_err("force unregister must not strand cold-only objects");
    assert!(error.to_string().contains("reclaimable"));
    let stored = service
        .backend()
        .get_cold_tier_device("ssd-cold-only")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Healthy);
}

#[test]
fn cold_tier_drain_and_blockers_are_explicit_operations() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("storage-a", 1))
        .expect("lease should seed");
    let rpc = Arc::new(FakeMigrationRpc::default());
    rpc.manual_gc_results.lock().push_back(Ok(1));
    let service = test_migration_service_with_rpc(
        backend.clone(),
        rpc.clone(),
        MigrationQueueConfig::default(),
    );
    let device = sample_cold_tier_device("ssd-explicit-drain");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");
    let mut route = sample_active_route("explicit-drain-object", "segment-a", &[]);
    route.cold_backing = Some(sample_cold_backing(
        "ssd-explicit-drain",
        ColdBackingState::Materialized,
    ));
    service
        .backend()
        .compare_and_swap_object_route(&route.key, None, Some(&route))
        .expect("route should seed");

    let blockers = service
        .get_cold_tier_blockers(None, "ssd-explicit-drain")
        .expect("blockers should load");
    assert_eq!(blockers.blocked_objects, 1);
    assert_eq!(blockers.reclaimable_objects, 1);
    assert_eq!(blockers.pending_delete_objects, 0);

    let drained = service
        .drain_cold_tier_device(
            None,
            "ssd-explicit-drain",
            ColdTierDrainRequest {
                dry_run: false,
                ..ColdTierDrainRequest::default()
            },
        )
        .expect("drain should mark reclaimable backing pending delete");
    assert_eq!(drained.state, ColdTierDeviceState::Draining);
    assert_eq!(drained.marked_pending_delete, 1);
    assert_eq!(drained.collected_pending_delete, 1);
    assert_eq!(
        rpc.manual_gc_calls.lock().as_slice(),
        &[("storage-a".to_string(), "ssd-explicit-drain".to_string(), 1)]
    );

    let after = service
        .get_cold_tier_blockers(None, "ssd-explicit-drain")
        .expect("blockers should reload");
    assert_eq!(after.blocked_objects, 0);
    assert_eq!(after.pending_delete_objects, 1);
}

#[test]
fn cold_tier_drain_with_migration_tasks_stays_async() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("storage-a", 1))
        .expect("lease should seed");
    let rpc = Arc::new(FakeMigrationRpc::default());
    let service =
        test_migration_service_with_rpc(backend.clone(), rpc, MigrationQueueConfig::default());
    let device = sample_cold_tier_device("ssd-drain-migrate");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");
    let mut route = sample_active_route("drain-migrate-object", "", &[]);
    route.cold_backing = Some(sample_cold_backing(
        "ssd-drain-migrate",
        ColdBackingState::Materialized,
    ));
    service
        .backend()
        .compare_and_swap_object_route(&route.key, None, Some(&route))
        .expect("route should seed");

    let drained = service
        .drain_cold_tier_device(
            None,
            "ssd-drain-migrate",
            ColdTierDrainRequest {
                dry_run: false,
                migration_task_executor: Some("executor-a".to_string()),
                migration_target_segments: vec!["segment-b".to_string()],
                migration_max_retries: Some(3),
            },
        )
        .expect("drain should accept async migration-backed progress");
    assert_eq!(drained.state, ColdTierDeviceState::Draining);
    assert_eq!(drained.blocked_objects, 1);
    assert_eq!(drained.reclaimable_objects, 0);
    assert_eq!(drained.marked_pending_delete, 0);
    assert_eq!(drained.collected_pending_delete, 0);
    assert_eq!(drained.migration_tasks_submitted, 1);
    assert!(drained.message.contains("waiting for route migration"));

    let stored = service
        .backend()
        .get_cold_tier_device("ssd-drain-migrate")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Draining);

    let tasks = service.list_route_migration_tasks();
    assert_eq!(tasks.count, 1);
}

#[test]
fn cold_tier_manual_gc_and_free_call_storage_runtime() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("runtime-a", 7))
        .expect("lease should seed");
    let rpc = Arc::new(FakeMigrationRpc::default());
    rpc.manual_gc_results.lock().push_back(Ok(1));
    rpc.manual_free_results
        .lock()
        .push_back(Ok(control_plane_pb::ManualColdTierFreeReply {
            attempted_victims: 2,
            freed_backings: 1,
            skipped_backings: 1,
            reached_low_watermark: true,
            error: None,
            collected_backings: 1,
        }));
    let service = test_migration_service_with_rpc(
        backend.clone(),
        rpc.clone(),
        MigrationQueueConfig::default(),
    );
    let mut device = sample_cold_tier_device("ssd-manual");
    device.stable_id = "runtime-a".to_string();
    device.reserved_bytes = 9;
    backend
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");
    let mut route = sample_active_route("manual-gc-object", "segment-a", &[]);
    route.cold_backing = Some(sample_cold_backing(
        "ssd-manual",
        ColdBackingState::PendingDelete,
    ));
    backend
        .compare_and_swap_object_route(&route.key, None, Some(&route))
        .expect("route should seed");

    let gc = service
        .manual_gc_cold_tier_device(None, "ssd-manual")
        .expect("manual gc should reach runtime");
    assert_eq!(gc.pending_delete_objects, 1);
    assert_eq!(gc.collected_objects, 1);
    assert_eq!(
        rpc.manual_gc_calls.lock().as_slice(),
        &[("runtime-a".to_string(), "ssd-manual".to_string(), 1)]
    );
    let free = service
        .manual_free_cold_tier_device(None, "ssd-manual")
        .expect("manual free should reach runtime");
    assert_eq!(free.inflight_operations, 9);
    assert_eq!(free.attempted_victims, 2);
    assert_eq!(free.freed_backings, 1);
    assert_eq!(free.collected_backings, 1);
    assert_eq!(free.skipped_backings, 1);
    assert!(free.reached_low_watermark);
    assert_eq!(
        rpc.manual_free_calls.lock().as_slice(),
        &[("runtime-a".to_string(), "ssd-manual".to_string(), 64)]
    );
}

#[test]
fn admin_get_cold_tier_device_does_not_persist_probe_capacity() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("storage-a", 1))
        .expect("lease should store");
    let mut device = sample_cold_tier_device("ssd-capacity");
    device.capacity_bytes = Some(64 * 1024 * 1024);
    backend
        .put_cold_tier_device_if_absent(&device)
        .expect("device should store");
    let rpc = Arc::new(FakeMigrationRpc {
        probe_results: Arc::new(Mutex::new(
            vec![Ok(control_plane_pb::ProbeColdTierDeviceReply {
                device_id: "ssd-capacity".to_string(),
                capacity_bytes: 4096 * 1024 * 1024,
                used_bytes: 123,
                reserved_bytes: 0,
                schedulable: true,
                state: "Healthy".to_string(),
                last_error: String::new(),
                error: None,
            })]
            .into(),
        )),
        ..FakeMigrationRpc::default()
    });
    let service = test_migration_service_with_rpc(
        backend.clone(),
        rpc.clone(),
        MigrationQueueConfig::default(),
    );

    let response = service
        .get_cold_tier_device(None, "ssd-capacity")
        .expect("admin get should succeed");
    assert_eq!(response.capacity_bytes, Some(4096 * 1024 * 1024));
    assert_eq!(response.used_bytes, 123);
    assert_eq!(
        rpc.probe_calls.lock().as_slice(),
        &[("storage-a".to_string(), "ssd-capacity".to_string())]
    );

    let stored = backend
        .get_cold_tier_device("ssd-capacity")
        .expect("device lookup should succeed")
        .expect("device should exist");
    assert_eq!(stored.capacity_bytes, Some(64 * 1024 * 1024));
    assert_eq!(stored.used_bytes, 12);
}

#[test]
fn admin_service_uses_shared_in_memory_backend_for_cold_tier_queries() {
    let shared = Arc::new(InMemoryMetadataBackend::new_hard_isolated());
    let tenant_backend = shared.for_tenant("tenant-a").expect("tenant view");
    tenant_backend
        .upsert_client_lease(&ClientLease {
            runtime: ClientRuntimeId::new("store-a", ClientEpoch(7)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: now_ms() + 60_000,
        })
        .expect("lease should store");
    tenant_backend
        .put_cold_tier_device_if_absent(&sample_cold_tier_device("ssd-shared"))
        .expect("device should store");

    let service = test_service_with_backend(tenant_backend);
    let device = service
        .get_cold_tier_device(None, "ssd-shared")
        .expect("admin should read shared tenant device");
    assert_eq!(device.device_id, "ssd-shared");
    assert_eq!(device.stable_id, "storage-a");

    let listed = service
        .list_cold_tier_devices(None, Some("storage-a"), None, None, None)
        .expect("device list should succeed");
    assert_eq!(listed.devices.len(), 1);
    assert_eq!(listed.devices[0].device_id, "ssd-shared");
}

#[test]
fn admin_service_cold_tier_object_lookup_uses_tenant_scoped_backend() {
    let shared: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new_hard_isolated());
    let tenant_a = shared
        .for_tenant("tenant-a")
        .expect("tenant-a view should exist");
    let tenant_b = shared
        .for_tenant("tenant-b")
        .expect("tenant-b view should exist");
    tenant_a
        .put_cold_tier_device_if_absent(&sample_cold_tier_device("device-a"))
        .expect("tenant-a device should store");
    tenant_b
        .put_cold_tier_device_if_absent(&sample_cold_tier_device("device-b"))
        .expect("tenant-b device should store");
    tenant_a
        .compare_and_swap_object_route(
            &ObjectKey::new("shared-key"),
            None,
            Some(&ObjectRoute {
                key: ObjectKey::new("shared-key"),
                namespace: None,
                logical_key: None,
                canonical_key: None,
                sharing_scope: None,
                qos_tier: None,
                version: RouteVersion(1),
                content_generation: 0,
                state: RouteState::Active,
                compatibility: CompatibilityDescriptor::default(),
                replicas: Vec::new(),
                cold_backing: Some(sample_cold_backing(
                    "device-a",
                    ColdBackingState::Materialized,
                )),
            }),
        )
        .expect("tenant-a route should store");
    tenant_b
        .compare_and_swap_object_route(
            &ObjectKey::new("shared-key"),
            None,
            Some(&ObjectRoute {
                key: ObjectKey::new("shared-key"),
                namespace: None,
                logical_key: None,
                canonical_key: None,
                sharing_scope: None,
                qos_tier: None,
                version: RouteVersion(1),
                content_generation: 0,
                state: RouteState::Active,
                compatibility: CompatibilityDescriptor::default(),
                replicas: Vec::new(),
                cold_backing: Some(sample_cold_backing(
                    "device-b",
                    ColdBackingState::PendingOffload,
                )),
            }),
        )
        .expect("tenant-b route should store");

    let service = test_service_with_backend(shared);
    let tenant_a_lookup = service
        .get_cold_tier_object(Some("tenant-a"), "shared-key")
        .expect("tenant-a lookup should succeed");
    assert_eq!(tenant_a_lookup.key, "shared-key");
    assert_eq!(
        tenant_a_lookup
            .cold_backing
            .as_ref()
            .expect("tenant-a cold backing should exist")
            .device_id,
        "device-a"
    );
    assert_eq!(
        tenant_a_lookup
            .cold_backing
            .as_ref()
            .expect("tenant-a cold backing should exist")
            .state,
        "materialized"
    );

    let tenant_b_lookup = service
        .get_cold_tier_object(Some("tenant-b"), "shared-key")
        .expect("tenant-b lookup should succeed");
    assert_eq!(tenant_b_lookup.key, "shared-key");
    assert_eq!(
        tenant_b_lookup
            .cold_backing
            .as_ref()
            .expect("tenant-b cold backing should exist")
            .device_id,
        "device-b"
    );
    assert_eq!(
        tenant_b_lookup
            .cold_backing
            .as_ref()
            .expect("tenant-b cold backing should exist")
            .state,
        "pending_offload"
    );
}

#[test]
fn cold_tier_create_device_validates_missing_fields() {
    let service = test_service();
    let missing_stable = service.create_cold_tier_device(
        None,
        CreateColdTierDeviceRequest {
            stable_id: "".to_string(),
            cold_tier_id: "device-a".to_string(),
            kind: "ssd".to_string(),
            target: ColdTierTargetSpec::Directory {
                path: "/tmp/device-a".to_string(),
            },
            capacity_override_bytes: None,
            tags: Vec::new(),
        },
    );
    assert!(missing_stable
        .expect_err("empty stable_id should fail")
        .to_string()
        .contains("missing stable_id"));

    let missing_kind = service.create_cold_tier_device(
        None,
        CreateColdTierDeviceRequest {
            stable_id: "runtime-a".to_string(),
            cold_tier_id: "device-a".to_string(),
            kind: "  ".to_string(),
            target: ColdTierTargetSpec::Directory {
                path: "/tmp/device-a".to_string(),
            },
            capacity_override_bytes: None,
            tags: Vec::new(),
        },
    );
    assert!(missing_kind
        .expect_err("blank kind should fail")
        .to_string()
        .contains("missing kind"));

    let missing_path = service.create_cold_tier_device(
        None,
        CreateColdTierDeviceRequest {
            stable_id: "runtime-a".to_string(),
            cold_tier_id: "device-a".to_string(),
            kind: "ssd".to_string(),
            target: ColdTierTargetSpec::Directory {
                path: "".to_string(),
            },
            capacity_override_bytes: None,
            tags: Vec::new(),
        },
    );
    assert!(missing_path
        .expect_err("empty directory path should fail")
        .to_string()
        .contains("missing path"));
}

#[test]
fn cold_tier_create_device_succeeds_and_is_idempotent() {
    let service = test_service();
    let request = CreateColdTierDeviceRequest {
        stable_id: "runtime-a".to_string(),
        cold_tier_id: "device-create".to_string(),
        kind: "ssd".to_string(),
        target: ColdTierTargetSpec::Directory {
            path: "/tmp/device-create".to_string(),
        },
        capacity_override_bytes: Some(4096),
        tags: vec!["tier:hot".to_string()],
    };

    let first = service
        .create_cold_tier_device(None, request.clone())
        .expect("first create should succeed");
    assert_eq!(first.device.device_id, "device-create");
    assert_eq!(first.device.stable_id, "runtime-a");
    assert_eq!(first.device.state, ColdTierDeviceState::Unregistered);
    assert!(first.message.contains("created"));

    let second = service
        .create_cold_tier_device(None, request.clone())
        .expect("idempotent re-create should succeed");
    assert!(second.message.contains("already exists"));
    assert_eq!(second.device.device_id, first.device.device_id);
}

#[test]
fn cold_tier_create_device_rejects_conflicting_target() {
    let service = test_service();
    let request = CreateColdTierDeviceRequest {
        stable_id: "runtime-a".to_string(),
        cold_tier_id: "device-conflict".to_string(),
        kind: "ssd".to_string(),
        target: ColdTierTargetSpec::Directory {
            path: "/tmp/device-conflict".to_string(),
        },
        capacity_override_bytes: None,
        tags: Vec::new(),
    };
    service
        .create_cold_tier_device(None, request)
        .expect("first create should succeed");

    let conflicting = CreateColdTierDeviceRequest {
        stable_id: "runtime-a".to_string(),
        cold_tier_id: "device-conflict".to_string(),
        kind: "hdd".to_string(),
        target: ColdTierTargetSpec::Directory {
            path: "/tmp/device-conflict-other".to_string(),
        },
        capacity_override_bytes: None,
        tags: Vec::new(),
    };
    let error = service
        .create_cold_tier_device(None, conflicting)
        .expect_err("conflicting target should fail");
    assert!(error.to_string().contains("different target definition"));
}

#[test]
fn cold_tier_register_device_activates_and_dry_run_skips_mutation() {
    let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    backend
        .upsert_client_lease(&live_lease("runtime-reg", 5))
        .expect("lease should store");
    let rpc = Arc::new(FakeMigrationRpc::default());
    let service =
        test_migration_service_with_rpc(backend.clone(), rpc, MigrationQueueConfig::default());

    let mut device = sample_cold_tier_device("ssd-register");
    device.stable_id = "runtime-reg".to_string();
    device.state = ColdTierDeviceState::Unregistered;
    device.epoch = None;
    backend
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    // dry_run should not mutate
    let dry = service
        .register_cold_tier_device(
            None,
            "ssd-register",
            ColdTierRegisterRequest {
                scan_existing: false,
                dry_run: true,
            },
        )
        .expect("dry run register should succeed");
    assert!(dry.dry_run);
    assert_eq!(dry.epoch, Some(5));
    let stored = backend
        .get_cold_tier_device("ssd-register")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Unregistered);

    // real register should mutate
    let real = service
        .register_cold_tier_device(
            None,
            "ssd-register",
            ColdTierRegisterRequest {
                scan_existing: false,
                dry_run: false,
            },
        )
        .expect("register should succeed");
    assert!(!real.dry_run);
    assert_eq!(real.epoch, Some(5));
    assert_eq!(real.state, ColdTierDeviceState::Healthy);
    let stored = backend
        .get_cold_tier_device("ssd-register")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Healthy);
    assert_eq!(stored.epoch, Some(5));
}

#[test]
fn cold_tier_disable_device_transitions_healthy_to_disabled() {
    let service = test_service();
    let device = sample_cold_tier_device("ssd-disable");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    let disabled = service
        .disable_cold_tier_device(None, "ssd-disable", ColdTierDisableRequest { reason: None })
        .expect("disable should succeed");
    assert_eq!(disabled.state, ColdTierDeviceState::DisabledByAdmin);
    assert!(!disabled.schedulable);
    assert!(disabled.message.contains("disabled"));

    // already disabled is idempotent
    let again = service
        .disable_cold_tier_device(None, "ssd-disable", ColdTierDisableRequest { reason: None })
        .expect("re-disable should succeed");
    assert_eq!(again.state, ColdTierDeviceState::DisabledByAdmin);
    assert!(again.message.contains("already disabled"));
}

#[test]
fn cold_tier_disable_rejects_unregistered_device() {
    let service = test_service();
    let mut device = sample_cold_tier_device("ssd-disable-unreg");
    device.state = ColdTierDeviceState::Unregistered;
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    let error = service
        .disable_cold_tier_device(
            None,
            "ssd-disable-unreg",
            ColdTierDisableRequest { reason: None },
        )
        .expect_err("disable from unregistered should fail");
    assert!(error.to_string().contains("cannot disable"));
}

#[test]
fn cold_tier_enable_device_transitions_disabled_to_healthy() {
    let service = test_service();
    let mut device = sample_cold_tier_device("ssd-enable");
    device.state = ColdTierDeviceState::DisabledByAdmin;
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    let enabled = service
        .enable_cold_tier_device(None, "ssd-enable", ColdTierEnableRequest {})
        .expect("enable should succeed");
    assert_eq!(enabled.state, ColdTierDeviceState::Healthy);
    assert!(enabled.schedulable);
    assert!(enabled.message.contains("enabled"));

    // already enabled is idempotent
    let again = service
        .enable_cold_tier_device(None, "ssd-enable", ColdTierEnableRequest {})
        .expect("re-enable should succeed");
    assert_eq!(again.state, ColdTierDeviceState::Healthy);
    assert!(again.message.contains("already enabled"));
}

#[test]
fn cold_tier_enable_rejects_draining_device() {
    let service = test_service();
    let mut device = sample_cold_tier_device("ssd-enable-drain");
    device.state = ColdTierDeviceState::Draining;
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    let error = service
        .enable_cold_tier_device(None, "ssd-enable-drain", ColdTierEnableRequest {})
        .expect_err("enable from draining should fail");
    assert!(error.to_string().contains("cannot enable"));
}

#[test]
fn parse_cold_tier_device_state_covers_all_variants_and_invalid() {
    assert_eq!(
        parse_cold_tier_device_state("unregistered").unwrap(),
        ColdTierDeviceState::Unregistered
    );
    assert_eq!(
        parse_cold_tier_device_state("healthy").unwrap(),
        ColdTierDeviceState::Healthy
    );
    assert_eq!(
        parse_cold_tier_device_state("full").unwrap(),
        ColdTierDeviceState::Full
    );
    assert_eq!(
        parse_cold_tier_device_state("disabled_by_admin").unwrap(),
        ColdTierDeviceState::DisabledByAdmin
    );
    assert_eq!(
        parse_cold_tier_device_state("draining").unwrap(),
        ColdTierDeviceState::Draining
    );
    assert_eq!(
        parse_cold_tier_device_state("failed").unwrap(),
        ColdTierDeviceState::Failed
    );
    assert!(parse_cold_tier_device_state("unknown").is_err());
    assert!(parse_cold_tier_device_state("").is_err());
}

#[test]
fn cold_tier_unregister_dry_run_reports_state_without_mutation() {
    let service = test_service();
    let device = sample_cold_tier_device("ssd-dry-unreg");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device)
        .expect("device should seed");

    let dry = service
        .unregister_cold_tier_device(
            None,
            "ssd-dry-unreg",
            ColdTierUnregisterRequest {
                dry_run: true,
                force: false,
            },
        )
        .expect("dry run should succeed");
    assert!(dry.dry_run);
    assert_eq!(dry.blocked_objects, 0);
    assert!(dry.message.contains("can be unregistered"));

    let stored = service
        .backend()
        .get_cold_tier_device("ssd-dry-unreg")
        .expect("device should load")
        .expect("device should exist");
    assert_eq!(stored.state, ColdTierDeviceState::Healthy);
}

#[test]
fn cold_tier_list_devices_filters_by_state_and_stable_id() {
    let service = test_service();
    let mut device_a = sample_cold_tier_device("ssd-list-a");
    device_a.stable_id = "runtime-x".to_string();
    device_a.state = ColdTierDeviceState::Healthy;
    let mut device_b = sample_cold_tier_device("ssd-list-b");
    device_b.stable_id = "runtime-y".to_string();
    device_b.state = ColdTierDeviceState::DisabledByAdmin;
    service
        .backend()
        .put_cold_tier_device_if_absent(&device_a)
        .expect("device a should seed");
    service
        .backend()
        .put_cold_tier_device_if_absent(&device_b)
        .expect("device b should seed");

    let all = service
        .list_cold_tier_devices(None, None, None, None, None)
        .expect("list all should succeed");
    assert!(all.devices.len() >= 2);

    let by_stable = service
        .list_cold_tier_devices(None, Some("runtime-x"), None, None, None)
        .expect("list by stable_id should succeed");
    assert_eq!(by_stable.devices.len(), 1);
    assert_eq!(by_stable.devices[0].device_id, "ssd-list-a");

    let by_state = service
        .list_cold_tier_devices(None, None, Some(ColdTierDeviceState::DisabledByAdmin), None, None)
        .expect("list by state should succeed");
    assert!(by_state
        .devices
        .iter()
        .all(|d| d.state == ColdTierDeviceState::DisabledByAdmin));
    assert!(by_state
        .devices
        .iter()
        .any(|d| d.device_id == "ssd-list-b"));
}
