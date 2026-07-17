#![allow(dead_code, unused_imports)]

use super::*;

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_only_route_restores_on_read_miss() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("cold-restore-segment"));
    let client = StoreClientBuilder::new(metadata, "cold-restore")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_background_eviction(96, 60, 20))
        .cold_tier_target(cold_tier_test_config("cold-restore"))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let hot = b"0123456789abcdef";
    let cold = b"fedcba9876543210";
    let fresh = b"abcdefghijklmnop";

    client
        .put("restore-hot", hot)
        .expect("hot put should succeed");
    client
        .put("restore-cold", cold)
        .expect("cold put should succeed");
    client
        .get("restore-hot")
        .expect("hot get should establish clock heat");
    client
        .put("restore-fresh", fresh)
        .expect("fresh put should trigger eviction");

    let cold_route = client
        .query_route("restore-cold")
        .expect("cold route query should succeed")
        .expect("cold route should exist");
    assert!(
        cold_route.replicas.is_empty() || cold_route.cold_backing.is_some(),
        "evicted route should still expose cold backing"
    );
    assert_eq!(
        client
            .get("restore-cold")
            .expect("cold-only route should restore successfully"),
        cold
    );
    assert_eq!(
        client
            .get("restore-cold")
            .expect("restored payload should stay readable"),
        cold
    );
}

#[test]
#[ignore = "too slow for default CI gate"]
fn legacy_backend_object_route_is_not_restored_on_read_miss() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("legacy-restore-segment"));
    let client = StoreClientBuilder::new(metadata, "legacy-restore")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(cold_tier_test_config("legacy-restore"))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"legacy-cold-payload";
    let route = client
        .put("legacy-cold", payload)
        .expect("seed put should succeed");
    let cold_backing = route
        .cold_backing
        .clone()
        .expect("seed route should publish cold backing");
    let legacy_cold_backing = mooncake_store_core::ColdBackingRoute {
        owner: cold_backing.owner.clone(),
        cold_tier_id: cold_backing.cold_tier_id.clone(),
        object_locator: route
            .canonical_key
            .clone()
            .unwrap_or_else(|| route.key.0.clone()),
        length: cold_backing.length,
        checksum: cold_backing.checksum,
        state: mooncake_store_core::ColdBackingState::Materialized,
        replicas: Vec::new(),
    };
    store_test_cold_payload(&legacy_cold_backing, payload);

    let legacy_route = ObjectRoute {
        key: route.key.clone(),
        namespace: route.namespace.clone(),
        logical_key: route.logical_key.clone(),
        canonical_key: route.canonical_key.clone(),
        sharing_scope: route.sharing_scope.clone(),
        qos_tier: route.qos_tier.clone(),
        version: route.version.next(),
        state: route.state,
        compatibility: route.compatibility.clone(),
        replicas: vec![ReplicaRoute {
            owner: cold_backing.owner.clone(),
            segment_name: SegmentName::new("__legacy_backend_object__"),
            offset: None,
            segment_offset: 0,
            length: cold_backing.length,
            checksum: cold_backing.checksum,
            tier: mooncake_store_core::ReplicaTier::File,
            priority: 0,
        }],
        cold_backing: None,
    };
    loop {
        let cas = client
            .cas_route("legacy-cold", Some(route.version), Some(&legacy_route))
            .expect("legacy route CAS should succeed");
        if cas.applied {
            break;
        }
    }

    assert!(matches!(
        client.get("legacy-cold"),
        Err(StoreError::NotFound(_))
    ));
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_rejects_pending_delete_backing() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("pending-delete-restore");
    let transport = Arc::new(TestTransport::new("pending-delete-restore-segment"));
    let client = StoreClientBuilder::new(metadata, "pending-delete-restore")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "pending-delete-restore",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"pending-delete-payload";
    let route = client
        .put("pending-delete-restore-key", payload)
        .expect("seed put should succeed");
    let mut next = route.clone();
    next.version = next.version.next();
    next.replicas.clear();
    if let Some(cold_backing) = next.cold_backing.as_mut() {
        cold_backing.state = mooncake_store_core::ColdBackingState::PendingDelete;
    }
    assert!(
        client
            .cas_route(
                "pending-delete-restore-key",
                Some(route.version),
                Some(&next)
            )
            .expect("pending delete route CAS should succeed")
            .applied
    );

    assert!(matches!(
        client.get("pending-delete-restore-key"),
        Err(StoreError::NotFound(_))
    ));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_checksum_mismatch_does_not_promote() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("checksum-restore");
    let transport = Arc::new(TestTransport::new("checksum-restore-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "checksum-restore")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "checksum-restore",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"checksum-good";
    let corrupt = b"checksum-bad!";
    assert_eq!(payload.len(), corrupt.len());
    let key = ObjectKey::from_scope(
        &NamespaceScope::with_defaults(Some("default"), None, None),
        "checksum-restore-key",
    );
    let cold_backing = test_cold_backing(&client, "checksum-restore", &key, payload);
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("checksum-restore-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    metadata
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");
    let backend = LocalDirPersistentStorageBackend::new_with_root(root);
    fs::create_dir_all(
        backend
            .object_path(&cold_backing)
            .parent()
            .expect("cold object parent should exist"),
    )
    .expect("cold object parent create should succeed");
    fs::write(
        backend.object_path(&cold_backing),
        encode_backend_payload(
            corrupt,
            corrupt.len() as u64,
            Some(payload_checksum(corrupt)),
        )
        .expect("corrupt backend payload should encode"),
    )
    .expect("corrupt cold payload should seed");

    let error = client
        .get("checksum-restore-key")
        .expect_err("checksum mismatch should reject cold restore");
    assert!(error.to_string().contains("checksum mismatch"));
    let observed = client
        .query_route("checksum-restore-key")
        .expect("route query should succeed")
        .expect("route should still exist");
    assert!(observed.replicas.is_empty());
    assert_eq!(observed.version, RouteVersion(1));
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_missing_backend_object_does_not_promote_or_return_empty() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("missing-cold-object");
    let transport = Arc::new(TestTransport::new("missing-cold-object-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "missing-cold-object")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "missing-cold-object",
            ColdTierKind::Ssd,
            root,
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"missing-cold-payload";
    let key = ObjectKey::new("default::ns/default/default/missing-cold-object-key");
    let cold_backing = test_cold_backing(&client, "missing-cold-object", &key, payload);
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("missing-cold-object-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing),
    };
    metadata
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");

    let error = client
        .get("missing-cold-object-key")
        .expect_err("missing backend object should not restore");
    assert!(matches!(error, StoreError::NotFound(_)));
    assert!(error.to_string().contains("cold backing"));
    let observed = metadata
        .get_object_route(&key)
        .expect("route query should succeed")
        .expect("route should still exist");
    assert_eq!(observed.version, route.version);
    assert!(observed.replicas.is_empty());
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_unknown_cold_tier_id_does_not_use_default_backend() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("unknown-cold-tier-id");
    let default_root = root.clone();
    let transport = Arc::new(TestTransport::new("unknown-cold-tier-id-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "unknown-cold-tier-id")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "known-cold-tier",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"default-backend-payload";
    let key = ObjectKey::new("default::ns/default/default/unknown-cold-tier-key");
    let missing_backing = test_cold_backing(&client, "missing-cold-tier", &key, payload);
    let default_backing = mooncake_store_core::ColdBackingRoute {
        cold_tier_id: "known-cold-tier".to_string(),
        ..missing_backing.clone()
    };
    LocalDirPersistentStorageBackend::new_with_root(root)
        .put_object(&default_backing, payload)
        .expect("default backend bait payload should write");
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("unknown-cold-tier-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(missing_backing),
    };
    metadata
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");

    let error = client
        .get("unknown-cold-tier-key")
        .expect_err("unknown cold tier id should not fallback to default backend");
    assert!(error.to_string().contains("missing-cold-tier"));
    assert!(error.to_string().contains("not registered"));
    assert_eq!(
        LocalDirPersistentStorageBackend::new_with_root(default_root)
            .get_object(&default_backing)
            .expect("default backend bait should remain readable")
            .as_deref(),
        Some(payload.as_slice())
    );
    let observed = metadata
        .get_object_route(&key)
        .expect("route query should succeed")
        .expect("route should still exist");
    assert_eq!(observed.version, route.version);
    assert!(observed.replicas.is_empty());
}

fn test_cold_backing(
    client: &StoreClient,
    cold_tier_id: &str,
    key: &ObjectKey,
    payload: &[u8],
) -> mooncake_store_core::ColdBackingRoute {
    mooncake_store_core::ColdBackingRoute {
        owner: client.lease.runtime.clone(),
        cold_tier_id: cold_tier_id.to_string(),
        object_locator: key.0.clone(),
        length: payload.len() as u64,
        checksum: Some(payload_checksum(payload)),
        state: mooncake_store_core::ColdBackingState::Materialized,
        replicas: Vec::new(),
    }
}

fn seed_materialized_cold_only_route(
    client: &StoreClient,
    metadata: &Arc<InMemoryMetadataBackend>,
    cold_tier_id: &str,
    logical_key: &str,
    payload: &[u8],
) -> mooncake_store_core::ColdBackingRoute {
    let key = ObjectKey::new(format!("default::ns/default/default/{logical_key}"));
    let cold_backing = test_cold_backing(client, cold_tier_id, &key, payload);
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some(logical_key.to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    metadata
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");
    cold_backing
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_concurrent_reads_share_single_backend_flight() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("singleflight");
    let backend = Arc::new(CountingStorageBackend::new(
        LocalDirPersistentStorageBackend::new_with_root(root.clone()),
    ));
    let transport = Arc::new(TestTransport::new("singleflight-segment"));
    let client = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "singleflight")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config())
            .cold_tier_target(ColdTierTargetConfig::directory(
                "singleflight",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .cold_tier_backend_override("singleflight", backend.clone())
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"singleflight-cold-payload";
    let cold_backing = seed_materialized_cold_only_route(
        &client,
        &metadata,
        "singleflight",
        "singleflight-key",
        payload,
    );
    backend
        .inner
        .put_object(&cold_backing, payload)
        .expect("cold payload seed should succeed");

    let (resolved, _) = client
        .resolve_objects(&[ObjectRef::new("singleflight-key")])
        .expect("cold route should resolve");
    let mut leaders = Vec::new();
    for _ in 0..8 {
        let resolved = resolved[0].clone();
        let client = client.clone();
        leaders.push(std::thread::spawn(move || {
            let mut buffer = vec![0u8; resolved.replica.length as usize];
            restore_payload_from_cold_backing(&client, &resolved, &mut buffer)
                .map(|payload| payload.unwrap_or_else(|| Arc::new(buffer)))
        }));
    }
    let payloads = leaders
        .into_iter()
        .map(|leader| {
            leader
                .join()
                .expect("restore thread should join")
                .expect("restore should succeed")
        })
        .collect::<Vec<_>>();
    for restored in payloads {
        assert_eq!(restored.as_slice(), payload);
    }
    assert_eq!(backend.read_count(), 1);
}

#[test]
fn debug_evict_all_waits_for_pending_offload_claim_race() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("debug-evict-all-claim-race");
    let client = Arc::new(
        StoreClientBuilder::new(metadata, "debug-evict-all-claim-race")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .transport(Arc::new(TestTransport::new(
                "debug-evict-all-claim-race-segment",
            )))
            .local_memory(storage_config_with_bytes(512))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "debug-evict-all-claim-race",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"debug-evict-all-claim-race";
    let route = client.put("debug-evict-race", payload).expect("put");
    client.storage_owner.enqueue_pending_offload(&route);
    let claimed = client
        .storage_owner
        .pending_offloads
        .claim(&route.key, route.version)
        .expect("test should claim pending offload before evict-all");

    let evict_client = client.clone();
    let evict = std::thread::spawn(move || evict_client.debug_evict_all());
    sleep(Duration::from_millis(50));
    client.storage_owner.pending_offloads.retry(claimed);

    let result = evict.join().expect("evict thread should join").unwrap();
    assert!(result.completed, "evict-all should complete: {result:?}");
    assert_eq!(result.evicted, 1);
    let route = client
        .query_route("debug-evict-race")
        .expect("route query should succeed")
        .expect("route should remain cold-only");
    assert!(route.replicas.is_empty());
    assert_eq!(
        route
            .cold_backing
            .as_ref()
            .expect("cold backing should remain")
            .state,
        mooncake_store_core::ColdBackingState::Materialized
    );
    assert_eq!(client.get("debug-evict-race").expect("cold get"), payload);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn debug_evict_all_removes_all_dram_replicas() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("debug-evict-all");
    let client = StoreClientBuilder::new(metadata, "debug-evict-all")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new("debug-evict-all-segment")))
        .local_memory(storage_config_with_bytes(512))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "debug-evict-all",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload_a = b"debug-evict-all-a";
    let payload_b = b"debug-evict-all-b";
    client.put("debug-evict-a", payload_a).expect("put A");
    client.put("debug-evict-b", payload_b).expect("put B");
    wait_for_materialized_cold_backing(&client, "debug-evict-a");
    wait_for_materialized_cold_backing(&client, "debug-evict-b");

    let result = client.debug_evict_all().expect("debug evict-all");
    assert!(result.completed, "evict-all should complete: {result:?}");
    assert_eq!(result.evicted, 2);
    for key in ["debug-evict-a", "debug-evict-b"] {
        let route = client
            .query_route(key)
            .expect("route query should succeed")
            .expect("route should remain cold-only");
        assert!(route.replicas.is_empty(), "{key} still has DRAM replicas");
        assert_eq!(
            route
                .cold_backing
                .as_ref()
                .expect("cold backing should remain")
                .state,
            mooncake_store_core::ColdBackingState::Materialized
        );
    }
    assert_eq!(client.get("debug-evict-a").expect("cold get A"), payload_a);
    // The promotion worker may finish between cold reads; it must not drop shared local memory.
    client.wait_for_restore_promotions();
    assert_eq!(client.get("debug-evict-b").expect("cold get B"), payload_b);

    let _ = std::fs::remove_dir_all(root);
}

/// Regression test: evict-all must succeed when cold_backing is PendingOffload
/// and the device is disabled.  Before the fix, this hit MAX_NO_PROGRESS_RETRIES
/// because ensure_materialized_cold_backing_for_eviction returned Ok(None)
/// repeatedly — the delete_empty_route guard only checked cold_backing.is_none(),
/// missing the PendingOffload case (no cold copy on disk either).
#[test]
fn debug_evict_all_succeeds_with_pending_offload_and_disabled_device() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("debug-evict-all-pending-disabled");
    // Use EvictTriggered mode so the write path does NOT auto-publish
    // PendingOffload cold_backing.  This lets us CAS the route to
    // PendingOffload deterministically without racing the offload thread.
    // Default EmbeddedWrh route control is kept — we CAS through the
    // client's own route_ops which goes through the route_directory.
    let client = StoreClientBuilder::new(metadata.clone(), "debug-evict-pending-disabled")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "debug-evict-pending-disabled-seg",
        )))
        .local_memory(storage_config_with_bytes(512))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "debug-evict-pending-disabled",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .cold_tier_offload_mode(ColdTierOffloadMode::EvictTriggered)
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    // Write an object — in EvictTriggered mode cold_backing stays None.
    let payload = b"pending-offload-disabled-device";
    client.put("pending-disabled", payload).expect("put");

    // The builder internally calls metadata.for_tenant("default"), so routes
    // and devices are stored under the tenant-scoped keyspace.
    let scoped = metadata
        .for_tenant("default")
        .expect("for_tenant should return Some");

    // CAS the route to PendingOffload cold_backing — simulates the state
    // left by a Passthrough write whose background offload hadn't completed.
    let route = client
        .query_route("pending-disabled")
        .expect("route lookup")
        .expect("route should exist");
    assert!(
        route.cold_backing.is_none(),
        "EvictTriggered should leave cold_backing None"
    );
    let device = scoped
        .get_cold_tier_device("debug-evict-pending-disabled")
        .expect("device lookup")
        .expect("device should exist");
    let mut with_pending = route.clone();
    with_pending.version = route.version.next();
    with_pending.cold_backing = Some(mooncake_store_core::ColdBackingRoute {
        cold_tier_id: device.device_id.clone(),
        object_locator: format!("{}@v{}", route.key.0, route.version.0),
        owner: client.storage_owner.runtime.clone(),
        length: payload.len() as u64,
        checksum: route.replicas[0].checksum,
        state: mooncake_store_core::ColdBackingState::PendingOffload,
        replicas: Vec::new(),
    });
    let cas = client
        .storage_owner
        .route_ops
        .compare_and_swap_route(&route.key, Some(route.version), Some(&with_pending))
        .expect("CAS should succeed");
    assert!(
        cas.applied,
        "CAS to set PendingOffload should apply; expected version {:?}, current route: version={:?} cold_backing={:?}",
        route.version,
        cas.current.as_ref().map(|r| r.version),
        cas.current.as_ref().and_then(|r| r.cold_backing.as_ref().map(|b| b.state)),
    );

    // Disable the device and refresh the in-memory cache so that
    // has_usable_cold_tier_device() returns false.
    let mut update = mooncake_store_core::ColdTierDeviceUpdate::new(now_ms());
    update.expected_updated_at_ms = Some(device.updated_at_ms);
    update.state = Some(mooncake_store_core::ColdTierDeviceState::DisabledByAdmin);
    scoped
        .update_cold_tier_device("debug-evict-pending-disabled", update)
        .expect("device disable should succeed");
    refresh_cold_tier_device_cache(
        scoped.as_ref(),
        client.storage_owner.cold_tier_devices.cache(),
        "test_refresh_after_disable",
    )
    .expect("cache refresh should succeed");

    // Before the fix this would return Err(InvalidState("...no progress...")).
    let result = client
        .debug_evict_all()
        .expect("evict-all should succeed with PendingOffload + disabled device");
    assert!(result.completed, "evict-all should complete: {result:?}");
    assert_eq!(result.evicted, 1, "should evict the single DRAM replica");
    assert_eq!(result.remaining_hot_replicas, 0);
    assert_eq!(result.dropped_without_cold, 1);

    // Route should be fully deleted (no cold copy was on disk).
    assert!(
        client
            .query_route("pending-disabled")
            .expect("route query")
            .is_none(),
        "route should be deleted (no cold copy to keep)"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_distinct_flight_limit_rejects_new_keys_but_allows_waiters() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("restore-flight-limit");
    let backend = Arc::new(CountingStorageBackend::new(
        LocalDirPersistentStorageBackend::new_with_root(root.clone()),
    ));
    let client = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "restore-flight-limit")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(Arc::new(TestTransport::new("restore-flight-limit-segment")))
            .local_memory(storage_config())
            .cold_tier_target(ColdTierTargetConfig::directory(
                "restore-flight-limit",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .cold_tier_backend_override("restore-flight-limit", backend.clone())
            .cold_tier_rate_limits(
                ColdTierRateLimitConfig::default().restore_max_distinct_flights(1),
            )
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let first_payload = b"first-restore-flight";
    let second_payload = b"second-restore-flight";
    let first_backing = seed_materialized_cold_only_route(
        &client,
        &metadata,
        "restore-flight-limit",
        "restore-flight-limit-first",
        first_payload,
    );
    let second_backing = seed_materialized_cold_only_route(
        &client,
        &metadata,
        "restore-flight-limit",
        "restore-flight-limit-second",
        second_payload,
    );
    backend
        .inner
        .put_object(&first_backing, first_payload)
        .expect("first payload seed should succeed");
    backend
        .inner
        .put_object(&second_backing, second_payload)
        .expect("second payload seed should succeed");

    let resolved_first = client
        .resolve_objects(&[ObjectRef::new("restore-flight-limit-first")])
        .expect("first route should resolve")
        .0[0]
        .clone();
    let resolved_second = client
        .resolve_objects(&[ObjectRef::new("restore-flight-limit-second")])
        .expect("second route should resolve")
        .0[0]
        .clone();

    let leader_client = client.clone();
    let leader_resolved = resolved_first.clone();
    let leader = std::thread::spawn(move || {
        let mut buffer = vec![0u8; leader_resolved.replica.length as usize];
        restore_payload_from_cold_backing(&leader_client, &leader_resolved, &mut buffer)
            .map(|payload| payload.unwrap_or_else(|| Arc::new(buffer)))
    });
    sleep(Duration::from_millis(5));

    let waiter_client = client.clone();
    let waiter_resolved = resolved_first.clone();
    let waiter = std::thread::spawn(move || {
        let mut buffer = vec![0u8; waiter_resolved.replica.length as usize];
        restore_payload_from_cold_backing(&waiter_client, &waiter_resolved, &mut buffer)
            .map(|payload| payload.unwrap_or_else(|| Arc::new(buffer)))
    });

    let mut rejected_buffer = vec![0u8; resolved_second.replica.length as usize];
    let rejected =
        restore_payload_from_cold_backing(&client, &resolved_second, &mut rejected_buffer)
            .expect_err("second distinct restore should be rejected while first is in flight");
    assert!(rejected.to_string().contains("distinct restore flights"));

    assert_eq!(
        leader
            .join()
            .expect("leader should join")
            .expect("leader restore should succeed")
            .as_slice(),
        first_payload
    );
    assert_eq!(
        waiter
            .join()
            .expect("waiter should join")
            .expect("waiter restore should succeed")
            .as_slice(),
        first_payload
    );
    assert_eq!(backend.read_count(), 1);
    let prometheus = render_prometheus_metrics();
    assert!(prometheus.contains(
        "mooncake_store_cold_tier_operation_total{operation=\"restore\",result=\"admission_reject\",error_kind=\"distinct_flight_limit\"}"
    ));
}

#[test]
#[ignore = "too slow for default CI gate"]
fn restore_promotion_queue_drains_bounded_batches_without_stalling() {
    let queue = RestorePromotionQueue::new(8, 2, 2);
    for index in 0..5 {
        let key = ObjectKey::new(format!(
            "default::ns/default/default/promotion-drain-{index}"
        ));
        let route = ObjectRoute {
            key: key.clone(),
            namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
            logical_key: Some(format!("promotion-drain-{index}")),
            canonical_key: None,
            sharing_scope: Some("default".to_string()),
            qos_tier: Some("default".to_string()),
            version: RouteVersion(1),
            state: mooncake_store_core::RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: None,
        };
        let task = RestorePromotionTask {
            key: RestorePromotionKey {
                route_key: key,
                route_version: RouteVersion(1),
            },
            tenant: "default".to_string(),
            object_id: LogicalObjectId::new(
                NamespaceScope::with_defaults(Some("default"), None, None),
                format!("promotion-drain-{index}"),
            ),
            qos_tier: Some("default".to_string()),
            current: route,
            payload: Arc::new(vec![index as u8]),
            policy: ReplicationPolicy::new(),
            target_runtime: None,
            target_segment: None,
        };
        assert_eq!(queue.push(task), RestorePromotionPushOutcome::Accepted);
    }

    let first = queue.take_ready_batch();
    assert_eq!(first.len(), 2);
    assert!(queue.take_ready_batch().is_empty());
    for task in &first {
        queue.complete(&task.key);
    }

    let second = queue.take_next_worker_batch();
    assert_eq!(second.len(), 2);
    for task in &second {
        queue.complete(&task.key);
    }

    let third = queue.take_next_worker_batch();
    assert_eq!(third.len(), 1);
    for task in &third {
        queue.complete(&task.key);
    }
    assert!(queue.take_next_worker_batch().is_empty());
    queue.finish_worker();
    assert!(queue.take_ready_batch().is_empty());
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_promotion_enqueues_on_first_cold_hit() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        "default::ns/default/default/promotion-hot-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let root = cold_tier_test_root("promotion-hot-gate");
    let transport = Arc::new(TestTransport::new("promotion-hot-gate-segment"));
    let client = Arc::new(
        StoreClientBuilder::new(metadata, "promotion-hot-gate")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config_with_bytes(128))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "promotion-hot-gate",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"promotion-hot-payload";
    let key = ObjectKey::new("default::ns/default/default/promotion-hot-key");
    let cold_backing = test_cold_backing(&client, "promotion-hot-gate", &key, payload);
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("promotion-hot-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    inner
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");
    LocalDirPersistentStorageBackend::new_with_root(root.clone())
        .put_object(&cold_backing, payload)
        .expect("cold payload seed should succeed");

    blocking.arm_blocked_cas();
    let reader = client.clone();
    let get_thread = std::thread::spawn(move || reader.get("promotion-hot-key"));
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "first cold hit should enqueue async promotion and reach route CAS"
    );
    blocking.release_blocked_cas();
    assert_eq!(
        get_thread
            .join()
            .expect("get thread should join")
            .expect("first cold get should return payload"),
        payload
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let observed = inner
            .get_object_route(&key)
            .expect("route query should succeed")
            .expect("route should still exist");
        if !observed.replicas.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "promotion did not publish a hot replica"
        );
        sleep(Duration::from_millis(10));
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_promotion_conflict_returns_payload_without_overwrite() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        "default::ns/default/default/promotion-race-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let root = cold_tier_test_root("promotion-race");
    let transport = Arc::new(TestTransport::new("promotion-race-segment"));
    let client = Arc::new(
        StoreClientBuilder::new(metadata, "promotion-race")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config_with_bytes(128))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "promotion-race",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"promotion-race-payload";
    let key = ObjectKey::new("default::ns/default/default/promotion-race-key");
    let cold_backing = test_cold_backing(&client, "promotion-race", &key, payload);
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("promotion-race-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    inner
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");
    LocalDirPersistentStorageBackend::new_with_root(root)
        .put_object(&cold_backing, payload)
        .expect("cold payload seed should succeed");

    client
        .get("promotion-race-key")
        .expect("first cold get should seed promotion hotness");
    blocking.arm_blocked_cas();
    let reader = client.clone();
    let get_thread = std::thread::spawn(move || reader.get("promotion-race-key"));
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "restore promotion should reach blocked route CAS"
    );

    let mut newer = route.clone();
    newer.version = route.version.next();
    newer.cold_backing = None;
    assert!(
        inner
            .compare_and_swap_object_route(&key, Some(route.version), Some(&newer))
            .expect("newer route publish should succeed")
            .applied
    );
    blocking.release_blocked_cas();

    assert_eq!(
        get_thread
            .join()
            .expect("get thread should join")
            .expect("cold get should return payload despite promotion conflict"),
        payload
    );
    let observed = inner
        .get_object_route(&key)
        .expect("route query should succeed")
        .expect("route should still exist");
    assert_eq!(observed.version, newer.version);
    assert!(observed.cold_backing.is_none());
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_batch_get_into_preserves_payload_boundaries() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("cold-batch-restore-boundaries");
    let transport = Arc::new(TestTransport::new("cold-batch-restore-boundaries-segment"));
    let client = StoreClientBuilder::new(metadata, "cold-batch-restore-boundaries")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "cold-batch-restore-boundaries",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    client
        .put("cold-batch-a", b"kv-a-payload")
        .expect("first cold seed put should succeed");
    client
        .put("cold-batch-b", b"longer-kv-b-payload")
        .expect("second cold seed put should succeed");
    wait_for_materialized_cold_backing(&client, "cold-batch-a");
    wait_for_materialized_cold_backing(&client, "cold-batch-b");

    let mut first = vec![0u8; b"kv-a-payload".len()];
    let mut second = vec![0u8; b"longer-kv-b-payload".len()];
    let sizes = client
        .batch_get_into(&mut [
            GetRequest::new("cold-batch-a", &mut first),
            GetRequest::new("cold-batch-b", &mut second),
        ])
        .expect("cold batch restore into caller buffers should succeed");
    assert_eq!(
        sizes,
        vec![b"kv-a-payload".len(), b"longer-kv-b-payload".len()]
    );
    assert_eq!(&first, b"kv-a-payload");
    assert_eq!(&second, b"longer-kv-b-payload");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_batch_get_into_uses_one_backend_batch_read() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("batch-single-backend-read");
    let backend = Arc::new(CountingStorageBackend::new(
        LocalDirPersistentStorageBackend::new_with_root(root.clone()),
    ));
    let transport = Arc::new(TestTransport::new("batch-single-backend-read-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "batch-single-backend-read")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "batch-single-backend-read",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .cold_tier_backend_override("batch-single-backend-read", backend.clone())
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payloads = [
        ("batch-read-a", b"batch-read-a-payload".as_slice()),
        ("batch-read-b", b"batch-read-b-payload".as_slice()),
        ("batch-read-c", b"batch-read-c-payload".as_slice()),
    ];
    for (key, payload) in payloads {
        let cold_backing = seed_materialized_cold_only_route(
            &client,
            &metadata,
            "batch-single-backend-read",
            key,
            payload,
        );
        backend
            .inner
            .put_object(&cold_backing, payload)
            .expect("cold payload seed should succeed");
    }

    let mut first = vec![0u8; b"batch-read-a-payload".len()];
    let mut second = vec![0u8; b"batch-read-b-payload".len()];
    let mut third = vec![0u8; b"batch-read-c-payload".len()];
    client
        .batch_get_into(&mut [
            GetRequest::new("batch-read-a", &mut first),
            GetRequest::new("batch-read-b", &mut second),
            GetRequest::new("batch-read-c", &mut third),
        ])
        .expect("cold batch restore should succeed");
    assert_eq!(backend.batch_read_count(), 1);
    assert_eq!(backend.read_count(), 3);
    assert_eq!(&first, b"batch-read-a-payload");
    assert_eq!(&second, b"batch-read-b-payload");
    assert_eq!(&third, b"batch-read-c-payload");
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_large_checkpoint_batch_uses_caller_buffers_without_cross_object_bleed() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("large-checkpoint-batch");
    let transport = Arc::new(TestTransport::new("large-checkpoint-batch-segment"));
    let client = StoreClientBuilder::new(metadata, "large-checkpoint-batch")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 4096, 64))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "large-checkpoint-batch",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payloads = [
        ("checkpoint-shard-a", vec![0xA5u8; 257]),
        (
            "checkpoint-shard-b",
            (0..1024).map(|index| (index % 251) as u8).collect(),
        ),
        ("checkpoint-shard-c", vec![0x5Au8; 513]),
    ];
    for (key, payload) in &payloads {
        client
            .put(key, payload)
            .expect("checkpoint shard put should succeed");
    }
    for (key, _) in &payloads {
        wait_for_materialized_cold_backing(&client, key);
        force_cold_only_route(&client, key);
    }

    let mut first = vec![0x11u8; payloads[0].1.len() + 64];
    let mut second = vec![0x22u8; payloads[1].1.len() + 64];
    let mut third = vec![0x33u8; payloads[2].1.len() + 64];
    let sizes = client
        .batch_get_into(&mut [
            GetRequest::new(payloads[0].0, &mut first),
            GetRequest::new(payloads[1].0, &mut second),
            GetRequest::new(payloads[2].0, &mut third),
        ])
        .expect("checkpoint batch cold restore should succeed");
    assert_eq!(
        sizes,
        payloads
            .iter()
            .map(|(_, payload)| payload.len())
            .collect::<Vec<_>>()
    );
    assert_eq!(&first[..payloads[0].1.len()], payloads[0].1.as_slice());
    assert_eq!(&second[..payloads[1].1.len()], payloads[1].1.as_slice());
    assert_eq!(&third[..payloads[2].1.len()], payloads[2].1.as_slice());
    assert!(first[payloads[0].1.len()..]
        .iter()
        .all(|byte| *byte == 0x11));
    assert!(second[payloads[1].1.len()..]
        .iter()
        .all(|byte| *byte == 0x22));
    assert!(third[payloads[2].1.len()..]
        .iter()
        .all(|byte| *byte == 0x33));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_large_checkpoint_batch_rejects_corrupt_shard_without_partial_success() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("large-checkpoint-corrupt");
    let transport = Arc::new(TestTransport::new("large-checkpoint-corrupt-segment"));
    let client = StoreClientBuilder::new(metadata, "large-checkpoint-corrupt")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_layout(4096, 4096, 64))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "large-checkpoint-corrupt",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let healthy = vec![0x42u8; 384];
    let corrupt_expected = vec![0x7Bu8; 512];
    client
        .put("checkpoint-good", &healthy)
        .expect("healthy checkpoint shard put should succeed");
    client
        .put("checkpoint-corrupt", &corrupt_expected)
        .expect("corrupt checkpoint shard seed put should succeed");
    wait_for_materialized_cold_backing(&client, "checkpoint-good");
    force_cold_only_route(&client, "checkpoint-good");
    let corrupt_backing = wait_for_materialized_cold_backing(&client, "checkpoint-corrupt");
    force_cold_only_route(&client, "checkpoint-corrupt");
    let backend = LocalDirPersistentStorageBackend::new_with_root(root.clone());
    let mut encoded = encode_backend_payload(
        &corrupt_expected,
        corrupt_expected.len() as u64,
        Some(payload_checksum(&corrupt_expected)),
    )
    .expect("checkpoint payload should encode");
    *encoded
        .last_mut()
        .expect("encoded checkpoint payload should be non-empty") ^= 0xFF;
    fs::write(backend.object_path(&corrupt_backing), encoded)
        .expect("corrupt checkpoint payload should overwrite backend object");

    let mut good_buffer = vec![0xAAu8; healthy.len()];
    let mut corrupt_buffer = vec![0xBBu8; corrupt_expected.len()];
    let error = client
        .batch_get_into(&mut [
            GetRequest::new("checkpoint-good", &mut good_buffer),
            GetRequest::new("checkpoint-corrupt", &mut corrupt_buffer),
        ])
        .expect_err("corrupt checkpoint shard should fail the batch restore");
    assert!(error.to_string().contains("checksum mismatch"));
    let observed = client
        .query_route("checkpoint-corrupt")
        .expect("corrupt route query should succeed")
        .expect("corrupt route should remain visible");
    assert!(observed.replicas.is_empty());
    assert_eq!(
        observed
            .cold_backing
            .as_ref()
            .expect("corrupt route should retain cold backing")
            .object_locator,
        corrupt_backing.object_locator
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn concurrent_cold_restore_dedupes_in_flight_promotion() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        "default::ns/default/default/restore-stampede-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let root = cold_tier_test_root("restore-stampede");
    let transport = Arc::new(TestTransport::new("restore-stampede-segment"));
    let client = Arc::new(
        StoreClientBuilder::new(metadata, "restore-stampede")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config_with_bytes(128))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "restore-stampede",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"restore-stampede-payload";
    let key = ObjectKey::new("default::ns/default/default/restore-stampede-key");
    let cold_backing = test_cold_backing(&client, "restore-stampede", &key, payload);
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("restore-stampede-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    inner
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");
    LocalDirPersistentStorageBackend::new_with_root(root.clone())
        .put_object(&cold_backing, payload)
        .expect("cold payload seed should succeed");
    assert_eq!(
        client
            .get("restore-stampede-key")
            .expect("first cold get should seed promotion hotness"),
        payload
    );

    blocking.arm_blocked_cas();
    let first_reader = client.clone();
    let first_get = std::thread::spawn(move || first_reader.get("restore-stampede-key"));
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "first restore promotion should be in flight"
    );

    assert_eq!(
        client
            .get("restore-stampede-key")
            .expect("concurrent restore should return payload while promotion is in flight"),
        payload
    );
    blocking.release_blocked_cas();
    assert_eq!(
        first_get
            .join()
            .expect("first get thread should join")
            .expect("first restore should return payload"),
        payload
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let prometheus = render_prometheus_metrics();
        if prometheus.contains(
            "mooncake_store_cold_tier_operation_total{operation=\"restore_promote\",result=\"ok\",error_kind=\"none\"} 1",
        ) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restore promotion should complete once; metrics were:\n{prometheus}"
        );
        sleep(Duration::from_millis(10));
    }
    let _ = std::fs::remove_dir_all(root);
}

#[test]
#[ignore = "too slow for default CI gate"]
fn metadata_route_cas_rejects_stale_restore_after_reconnect() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        "default::ns/default/default/reconnect-fence-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let root = cold_tier_test_root("reconnect-fence");
    let transport = Arc::new(TestTransport::new("reconnect-fence-segment"));
    let client = Arc::new(
        StoreClientBuilder::new(metadata, "reconnect-fence")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config_with_bytes(128))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "reconnect-fence",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"reconnect-fenced-payload";
    let key = ObjectKey::new("default::ns/default/default/reconnect-fence-key");
    let cold_backing = test_cold_backing(&client, "reconnect-fence", &key, payload);
    let original = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("reconnect-fence-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(7),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    inner
        .compare_and_swap_object_route(&key, None, Some(&original))
        .expect("route seed should succeed");
    LocalDirPersistentStorageBackend::new_with_root(root)
        .put_object(&cold_backing, payload)
        .expect("cold payload seed should succeed");
    assert_eq!(
        client
            .get("reconnect-fence-key")
            .expect("first cold get should seed promotion hotness"),
        payload
    );

    blocking.arm_blocked_cas();
    let reader = client.clone();
    let get_thread = std::thread::spawn(move || reader.get("reconnect-fence-key"));
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "restore promotion should reach blocked route CAS"
    );

    let reconnected_route = ObjectRoute {
        version: original.version.next(),
        replicas: Vec::new(),
        cold_backing: Some(mooncake_store_core::ColdBackingRoute {
            object_locator: "reconnected-authoritative-locator".to_string(),
            checksum: Some(payload_checksum(b"new-authoritative-payload")),
            length: b"new-authoritative-payload".len() as u64,
            ..cold_backing.clone()
        }),
        ..original.clone()
    };
    assert!(
        inner
            .compare_and_swap_object_route(&key, Some(original.version), Some(&reconnected_route))
            .expect("reconnect replay should publish newer authoritative route")
            .applied
    );
    blocking.release_blocked_cas();

    assert_eq!(
        get_thread
            .join()
            .expect("get thread should join")
            .expect("stale restore should still return the snapshot payload"),
        payload
    );
    let observed = inner
        .get_object_route(&key)
        .expect("route query should succeed")
        .expect("route should still exist");
    assert_eq!(observed.version, reconnected_route.version);
    assert_eq!(
        observed
            .cold_backing
            .as_ref()
            .expect("newer cold backing should remain authoritative")
            .object_locator,
        "reconnected-authoritative-locator"
    );
}

#[test]
#[ignore = "too slow for default CI gate"]
fn cold_restore_delete_race_does_not_republish_deleted_route() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner.clone(),
        "default::ns/default/default/delete-race-key",
    ));
    let metadata: Arc<dyn MetadataBackend> = blocking.clone();
    let root = cold_tier_test_root("delete-race");
    let client = Arc::new(
        StoreClientBuilder::new(metadata, "delete-race")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(Arc::new(TestTransport::new("delete-race-segment")))
            .local_memory(storage_config_with_bytes(128))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "delete-race",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("client build should succeed"),
    );
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"delete-race-payload";
    let key = ObjectKey::new("default::ns/default/default/delete-race-key");
    let cold_backing = mooncake_store_core::ColdBackingRoute {
        owner: client.lease.runtime.clone(),
        cold_tier_id: "delete-race".to_string(),
        object_locator: key.0.clone(),
        length: payload.len() as u64,
        checksum: Some(payload_checksum(payload)),
        state: mooncake_store_core::ColdBackingState::Materialized,
        replicas: Vec::new(),
    };
    let route = ObjectRoute {
        key: key.clone(),
        namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
        logical_key: Some("delete-race-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("default".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: Some(cold_backing.clone()),
    };
    inner
        .compare_and_swap_object_route(&key, None, Some(&route))
        .expect("route seed should succeed");
    LocalDirPersistentStorageBackend::new_with_root(root)
        .put_object(&cold_backing, payload)
        .expect("cold payload seed should succeed");
    assert_eq!(
        client
            .get("delete-race-key")
            .expect("first cold get should seed promotion hotness"),
        payload
    );

    blocking.arm_blocked_cas();
    let reader = client.clone();
    let get_thread = std::thread::spawn(move || reader.get("delete-race-key"));
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "restore promotion should reach blocked route CAS"
    );
    assert!(
        inner
            .compare_and_swap_object_route(&key, Some(route.version), None)
            .expect("delete route CAS should succeed")
            .applied
    );
    blocking.release_blocked_cas();

    assert_eq!(
        get_thread
            .join()
            .expect("get thread should join")
            .expect("in-flight cold get may return its snapshot payload"),
        payload
    );
    assert!(inner
        .get_object_route(&key)
        .expect("route query should succeed")
        .is_none());
}

#[test]
#[ignore = "too slow for default CI gate"]
fn metadata_rollback_does_not_restore_route_from_orphaned_local_ssd_object() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("metadata-rollback-orphan");
    let transport = Arc::new(TestTransport::new("metadata-rollback-orphan-segment"));
    let client = StoreClientBuilder::new(metadata.clone(), "metadata-rollback-orphan")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(128))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "metadata-rollback-orphan",
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let payload = b"metadata-rollback-payload";
    let key = ObjectKey::new("default::ns/default/default/metadata-rollback-key");
    let cold_backing = mooncake_store_core::ColdBackingRoute {
        owner: client.lease.runtime.clone(),
        cold_tier_id: "metadata-rollback-orphan".to_string(),
        object_locator: key.0.clone(),
        length: payload.len() as u64,
        checksum: Some(payload_checksum(payload)),
        state: mooncake_store_core::ColdBackingState::Materialized,
        replicas: Vec::new(),
    };
    LocalDirPersistentStorageBackend::new_with_root(root)
        .put_object(&cold_backing, payload)
        .expect("orphaned local SSD object should write");

    let error = client
        .get("metadata-rollback-key")
        .expect_err("metadata rollback must not be recovered from local SSD alone");
    assert!(matches!(error, StoreError::NotFound(_)));
    assert!(metadata
        .get_object_route(&key)
        .expect("route query should succeed")
        .is_none());
}

fn restore_flight_key(logical_key: &str) -> ColdRestoreFlightKey {
    ColdRestoreFlightKey {
        route_key: ObjectKey::new(format!("default::ns/default/default/{logical_key}")),
        route_version: RouteVersion(1),
        cold_tier_id: "test-cold-tier".to_string(),
        object_locator: logical_key.to_string(),
        length: 7,
        checksum: Some(payload_checksum(b"payload")),
    }
}

fn test_restore_promotion_task(logical_key: &str) -> RestorePromotionTask {
    let route_key = ObjectKey::new(format!("default::ns/default/default/{logical_key}"));
    RestorePromotionTask {
        key: RestorePromotionKey {
            route_key: route_key.clone(),
            route_version: RouteVersion(1),
        },
        tenant: "default".to_string(),
        object_id: LogicalObjectId::new(
            NamespaceScope::with_defaults(Some("default"), None, None),
            logical_key.to_string(),
        ),
        qos_tier: Some("default".to_string()),
        current: ObjectRoute {
            key: route_key,
            namespace: Some(NamespaceScope::with_defaults(Some("default"), None, None)),
            logical_key: Some(logical_key.to_string()),
            canonical_key: None,
            sharing_scope: Some("default".to_string()),
            qos_tier: Some("default".to_string()),
            version: RouteVersion(1),
            state: mooncake_store_core::RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: None,
        },
        payload: Arc::new(b"payload".to_vec()),
        policy: ReplicationPolicy::new(),
        target_runtime: None,
        target_segment: None,
    }
}

#[test]
fn cold_restore_singleflight_allows_waiters_after_distinct_limit_is_hit() {
    let singleflight = Arc::new(ColdRestoreSingleflight::new(1));
    let first_key = restore_flight_key("first");
    let second_key = restore_flight_key("second");

    let leader = match singleflight.begin(first_key.clone()) {
        ColdRestoreFlightRegistration::Leader(leader) => leader,
        _ => panic!("expected leader registration"),
    };
    let waiter_flight = match singleflight.begin(first_key.clone()) {
        ColdRestoreFlightRegistration::Waiter(flight) => flight,
        _ => panic!("expected waiter registration"),
    };
    assert_eq!(waiter_flight.waiters.load(Ordering::Relaxed), 1);
    assert!(matches!(
        singleflight.begin(second_key),
        ColdRestoreFlightRegistration::Rejected
    ));

    let payload = Arc::new(b"payload".to_vec());
    assert_eq!(leader.finish(Ok(payload.clone())).unwrap(), payload);
    assert_eq!(singleflight.wait(&waiter_flight).unwrap(), payload);
    assert!(singleflight.state.lock().unwrap().flights.is_empty());
}

#[test]
fn cold_restore_singleflight_drop_unblocks_waiters_with_leader_error() {
    let singleflight = Arc::new(ColdRestoreSingleflight::new(1));
    let key = restore_flight_key("drop-leader");

    let leader = match singleflight.begin(key.clone()) {
        ColdRestoreFlightRegistration::Leader(leader) => leader,
        _ => panic!("expected leader registration"),
    };
    let waiter_flight = match singleflight.begin(key) {
        ColdRestoreFlightRegistration::Waiter(flight) => flight,
        _ => panic!("expected waiter registration"),
    };

    drop(leader);

    let error = singleflight
        .wait(&waiter_flight)
        .expect_err("waiter should observe leader drop error");
    assert!(error
        .to_string()
        .contains("leader exited before publishing result"));
    assert!(singleflight.state.lock().unwrap().flights.is_empty());
}

#[test]
fn cold_restore_singleflight_exports_role_and_state_metrics() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let singleflight = Arc::new(ColdRestoreSingleflight::new(1));
    let first_key = restore_flight_key("metrics-first");
    let second_key = restore_flight_key("metrics-second");

    let leader = match singleflight.begin(first_key.clone()) {
        ColdRestoreFlightRegistration::Leader(leader) => leader,
        _ => panic!("expected leader registration"),
    };
    let waiter_flight = match singleflight.begin(first_key.clone()) {
        ColdRestoreFlightRegistration::Waiter(flight) => flight,
        _ => panic!("expected waiter registration"),
    };
    assert!(matches!(
        singleflight.begin(second_key),
        ColdRestoreFlightRegistration::Rejected
    ));

    // Verify prometheus labels are emitted (exact counter values are not
    // checked because parallel tests may increment the same globals).
    let prometheus = render_prometheus_metrics();
    assert!(prometheus.contains(
        "mooncake_store_cold_restore_singleflight_total{event=\"begin\",result=\"leader\"}"
    ));
    assert!(prometheus.contains(
        "mooncake_store_cold_restore_singleflight_total{event=\"begin\",result=\"waiter\"}"
    ));
    assert!(prometheus.contains(
        "mooncake_store_cold_restore_singleflight_total{event=\"begin\",result=\"rejected\"}"
    ));

    let payload = Arc::new(b"payload".to_vec());
    assert_eq!(leader.finish(Ok(payload.clone())).unwrap(), payload);
    assert_eq!(singleflight.wait(&waiter_flight).unwrap(), payload);
}

#[test]
fn cold_restore_io_tracker_records_max_concurrent_io() {
    let tracker = ColdRestoreIoTracker::default();
    let key_a = ObjectKey("io-track-a".to_string());
    let key_b = ObjectKey("io-track-b".to_string());

    // Baseline: initialized to 1 (expected steady-state).
    assert_eq!(tracker.max_concurrent_io(), 1);

    // Single I/O for key_a: max stays 1.
    {
        let _guard = tracker.begin_io(&key_a);
        assert_eq!(tracker.max_concurrent_io(), 1);
    }
    // Guard dropped — high-water mark unchanged.
    assert_eq!(tracker.max_concurrent_io(), 1);

    // Two different keys concurrently: per-object max is still 1.
    {
        let _ga = tracker.begin_io(&key_a);
        let _gb = tracker.begin_io(&key_b);
        assert_eq!(tracker.max_concurrent_io(), 1);
    }

    // Two concurrent I/Os for the SAME key: max should become 2.
    // This simulates singleflight failure.
    {
        let _g1 = tracker.begin_io(&key_a);
        let _g2 = tracker.begin_io(&key_a);
        assert_eq!(tracker.max_concurrent_io(), 2);
    }
    // High-water mark stays at 2 even after both drop.
    assert_eq!(tracker.max_concurrent_io(), 2);
}

#[test]
fn restore_promotion_queue_dedupes_queued_keys_and_requeues_aborted_work() {
    let queue = RestorePromotionQueue::new(4, 1, 1);
    let first = test_restore_promotion_task("first");
    let second = test_restore_promotion_task("second");

    assert_eq!(
        queue.push(first.clone()),
        RestorePromotionPushOutcome::Accepted
    );
    assert_eq!(
        queue.push(first.clone()),
        RestorePromotionPushOutcome::Duplicate
    );
    assert_eq!(
        queue.push(second.clone()),
        RestorePromotionPushOutcome::Accepted
    );

    let batch = queue.take_ready_batch();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].key, first.key);
    assert_eq!(
        queue.push(first.clone()),
        RestorePromotionPushOutcome::InFlight
    );
    assert!(queue.take_next_worker_batch().is_empty());

    queue.abort_worker_batch(batch);

    let retried = queue.take_ready_batch();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].key, first.key);
    queue.complete(&retried[0].key);
    queue.finish_worker();

    let next = queue.take_ready_batch();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].key, second.key);
    queue.complete(&next[0].key);
    queue.finish_worker();
    assert!(queue.take_ready_batch().is_empty());
}

#[test]
fn decode_backend_payload_uses_route_checksum_even_without_stored_checksum() {
    let path = std::path::Path::new("/tmp/checksum-none.bin");
    let payload = b"payload";
    let encoded = encode_backend_payload(payload, payload.len() as u64, None)
        .expect("payload without stored checksum should encode");

    let decoded = decode_backend_payload(path, "cold object", encoded, payload.len() as u64, None)
        .expect("decode should succeed when route does not require checksum");
    assert_eq!(decoded, payload);

    let encoded = encode_backend_payload(payload, payload.len() as u64, None)
        .expect("payload without stored checksum should encode");
    let error = decode_backend_payload(
        path,
        "cold object",
        encoded,
        payload.len() as u64,
        Some(payload_checksum(b"different")),
    )
    .expect_err("route checksum must still be enforced for checksum-none payloads");
    assert!(error.to_string().contains("checksum mismatch"));
}

#[test]
fn restore_promotion_queue_rejects_when_full() {
    let queue = RestorePromotionQueue::new(2, 1, 4);
    let first = test_restore_promotion_task("full-a");
    let second = test_restore_promotion_task("full-b");
    let third = test_restore_promotion_task("full-c");

    assert_eq!(queue.push(first), RestorePromotionPushOutcome::Accepted);
    assert_eq!(queue.push(second), RestorePromotionPushOutcome::Accepted);
    assert_eq!(queue.push(third), RestorePromotionPushOutcome::Full);
}

#[test]
fn restore_promotion_queue_rejects_after_shutdown() {
    let queue = RestorePromotionQueue::new(8, 2, 4);
    let first = test_restore_promotion_task("shutdown-a");
    assert_eq!(
        queue.push(first.clone()),
        RestorePromotionPushOutcome::Accepted
    );
    queue.shutdown();
    let second = test_restore_promotion_task("shutdown-b");
    assert_eq!(queue.push(second), RestorePromotionPushOutcome::Shutdown);
    // Queue should be drained after shutdown
    assert!(queue.take_ready_batch().is_empty());
}

#[test]
fn restore_promotion_queue_shutdown_clears_pending_entries() {
    let queue = RestorePromotionQueue::new(8, 2, 4);
    for i in 0..4 {
        let task = test_restore_promotion_task(&format!("clear-{i}"));
        assert_eq!(queue.push(task), RestorePromotionPushOutcome::Accepted);
    }
    queue.shutdown();
    // After shutdown, take_ready_batch should return empty
    assert!(queue.take_ready_batch().is_empty());
    // And pushing should get Shutdown
    let task = test_restore_promotion_task("post-shutdown");
    assert_eq!(queue.push(task), RestorePromotionPushOutcome::Shutdown);
}

#[test]
fn restore_promotion_queue_in_flight_limit_blocks_batch() {
    let queue = RestorePromotionQueue::new(8, 4, 2);
    for i in 0..6 {
        let task = test_restore_promotion_task(&format!("inflight-{i}"));
        assert_eq!(queue.push(task), RestorePromotionPushOutcome::Accepted);
    }
    // batch_limit=4 but max_in_flight=2, so first batch should be 2
    let first = queue.take_ready_batch();
    assert_eq!(first.len(), 2);
    // Worker is active, next batch should respect in_flight limit
    let next = queue.take_next_worker_batch();
    assert!(next.is_empty()); // all 2 in-flight slots used
                              // Complete one slot
    queue.complete(&first[0].key);
    let next = queue.take_next_worker_batch();
    assert_eq!(next.len(), 1);
    // Complete the other original
    queue.complete(&first[1].key);
    queue.complete(&next[0].key);
    let next = queue.take_next_worker_batch();
    assert_eq!(next.len(), 2);
    for task in &next {
        queue.complete(&task.key);
    }
    let last = queue.take_next_worker_batch();
    assert_eq!(last.len(), 1);
    queue.complete(&last[0].key);
    assert!(queue.take_next_worker_batch().is_empty());
    queue.finish_worker();
}

#[test]
fn cold_restore_singleflight_allows_multiple_distinct_keys() {
    let singleflight = Arc::new(ColdRestoreSingleflight::new(4));
    let key_a = restore_flight_key("distinct-a");
    let key_b = restore_flight_key("distinct-b");

    let leader_a = match singleflight.begin(key_a.clone()) {
        ColdRestoreFlightRegistration::Leader(leader) => leader,
        _ => panic!("expected leader for key_a"),
    };
    let leader_b = match singleflight.begin(key_b.clone()) {
        ColdRestoreFlightRegistration::Leader(leader) => leader,
        _ => panic!("expected leader for key_b"),
    };
    // Both leaders can finish independently
    let payload_a = Arc::new(b"payload-a".to_vec());
    let payload_b = Arc::new(b"payload-b".to_vec());
    assert_eq!(leader_a.finish(Ok(payload_a.clone())).unwrap(), payload_a);
    assert_eq!(leader_b.finish(Ok(payload_b.clone())).unwrap(), payload_b);
    assert!(singleflight.state.lock().unwrap().flights.is_empty());
}

#[test]
#[ignore = "restart E2E; run explicitly for cold-tier recovery validation"]
fn embedded_wrh_cold_tier_data_recovered_from_manifest_after_restart() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("embedded-wrh-restart-recovery");

    let payload_a = b"embedded-wrh-restart-alpha";
    let payload_b = b"embedded-wrh-restart-bravo-longer-data";
    let (first_epoch, route_key_a, route_key_b) = {
        let transport = Arc::new(TestTransport::new("embedded-wrh-restart-segment-v1"));
        let client = StoreClientBuilder::new(metadata.clone(), "embedded-wrh-restart")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::EmbeddedWrh)
            .transport(transport)
            .local_memory(storage_config_with_bytes(512))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "embedded-wrh-restart",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("first incarnation build should succeed");
        client
            .register_local_memory()
            .expect("first incarnation local memory should register");

        let route_a = client
            .put("embedded-restart-key-a", payload_a)
            .expect("first put should succeed");
        let route_b = client
            .put("embedded-restart-key-b", payload_b)
            .expect("second put should succeed");
        wait_for_materialized_cold_backing(&client, "embedded-restart-key-a");
        wait_for_materialized_cold_backing(&client, "embedded-restart-key-b");

        assert!(
            metadata
                .get_object_route(&route_a.key)
                .expect("metadata route lookup should succeed")
                .is_none(),
            "EmbeddedWrh route should not be persisted to metadata before restart"
        );
        assert!(
            metadata
                .get_object_route(&route_b.key)
                .expect("metadata route lookup should succeed")
                .is_none(),
            "EmbeddedWrh route should not be persisted to metadata before restart"
        );
        assert_eq!(
            client
                .get("embedded-restart-key-a")
                .expect("pre-restart get A"),
            payload_a
        );
        assert_eq!(
            client
                .get("embedded-restart-key-b")
                .expect("pre-restart get B"),
            payload_b
        );

        (
            client.lease().runtime.epoch,
            route_a.key.clone(),
            route_b.key.clone(),
        )
    };

    assert!(
        metadata
            .get_object_route(&route_key_a)
            .expect("metadata route lookup should succeed")
            .is_none(),
        "EmbeddedWrh route should still be absent from metadata after client drop"
    );
    assert!(
        metadata
            .get_object_route(&route_key_b)
            .expect("metadata route lookup should succeed")
            .is_none(),
        "EmbeddedWrh route should still be absent from metadata after client drop"
    );

    {
        let transport = Arc::new(TestTransport::new("embedded-wrh-restart-segment-v2"));
        let client = StoreClientBuilder::new(metadata.clone(), "embedded-wrh-restart")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::EmbeddedWrh)
            .transport(transport)
            .local_memory(storage_config_with_bytes(512))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "embedded-wrh-restart",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("second incarnation build should succeed");
        client
            .register_local_memory()
            .expect("second incarnation local memory should register");

        assert!(
            client.lease().runtime.epoch > first_epoch,
            "second incarnation should allocate a higher epoch"
        );

        let recovered = client
            .query_route("embedded-restart-key-a")
            .expect("post-restart route query should succeed")
            .expect("route should be reverse-registered from cold manifest");
        assert!(
            recovered.replicas.is_empty(),
            "manifest recovery should recreate a cold-only route before hot promotion"
        );
        assert_eq!(
            recovered
                .cold_backing
                .as_ref()
                .expect("recovered route should have cold backing")
                .state,
            mooncake_store_core::ColdBackingState::Materialized,
            "recovered cold backing should be Materialized"
        );
        assert_eq!(
            recovered.logical_key.as_deref(),
            Some("embedded-restart-key-a"),
            "manifest recovery should preserve logical route identity"
        );

        assert_eq!(
            client
                .get("embedded-restart-key-a")
                .expect("post-restart get A should recover from cold manifest"),
            payload_a,
            "payload A content mismatch after EmbeddedWrh restart"
        );
        assert_eq!(
            client
                .get("embedded-restart-key-b")
                .expect("post-restart get B should recover from cold manifest"),
            payload_b,
            "payload B content mismatch after EmbeddedWrh restart"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}

/// E2E test: verifies that data written and offloaded to cold tier (SSD) can
/// be read back after a full process restart (new client epoch, same stable_id).
///
/// Scenario:
///   1. Client writes data → passthrough offload → SSD materialization
///   2. Client is dropped (simulates process crash/restart)
///   3. New client with same stable_id + same metadata + same cold tier root
///   4. Read data → should restore from cold tier successfully
#[test]
#[ignore = "too slow for default CI gate"]
fn cold_tier_data_readable_after_restart_new_epoch() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let root = cold_tier_test_root("restart-recovery-e2e");

    let payload_a = b"restart-recovery-payload-alpha";
    let payload_b = b"restart-recovery-payload-bravo-longer-data";

    // --- Phase 1: First incarnation writes data and offloads to SSD ---
    let first_epoch = {
        let transport = Arc::new(TestTransport::new("restart-recovery-segment-v1"));
        let client = StoreClientBuilder::new(metadata.clone(), "restart-recovery")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config_with_bytes(512))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "restart-recovery",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("first incarnation build should succeed");
        client
            .register_local_memory()
            .expect("first incarnation local memory should register");

        let route_a = client
            .put("restart-key-a", payload_a)
            .expect("first put should succeed");
        let route_b = client
            .put("restart-key-b", payload_b)
            .expect("second put should succeed");

        // Verify routes have cold backing assigned
        assert!(
            route_a.cold_backing.is_some(),
            "route_a should have cold backing"
        );
        assert!(
            route_b.cold_backing.is_some(),
            "route_b should have cold backing"
        );

        // Wait for offload materialization (PendingOffload → Materialized)
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let observed_a = client
                .query_route("restart-key-a")
                .expect("route query should succeed")
                .expect("route should exist");
            let materialized = observed_a
                .cold_backing
                .as_ref()
                .map(|cb| cb.state == mooncake_store_core::ColdBackingState::Materialized)
                .unwrap_or(false);
            if materialized {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "cold backing did not materialize within deadline; state={:?}",
                    observed_a.cold_backing.as_ref().map(|cb| cb.state)
                );
            }
            sleep(Duration::from_millis(50));
        }

        // Also wait for route_b
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let observed_b = client
                .query_route("restart-key-b")
                .expect("route query should succeed")
                .expect("route should exist");
            let materialized = observed_b
                .cold_backing
                .as_ref()
                .map(|cb| cb.state == mooncake_store_core::ColdBackingState::Materialized)
                .unwrap_or(false);
            if materialized {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "cold backing B did not materialize within deadline; state={:?}",
                    observed_b.cold_backing.as_ref().map(|cb| cb.state)
                );
            }
            sleep(Duration::from_millis(50));
        }

        // Verify data is readable while first incarnation is still alive
        assert_eq!(
            client.get("restart-key-a").expect("pre-restart get A"),
            payload_a
        );
        assert_eq!(
            client.get("restart-key-b").expect("pre-restart get B"),
            payload_b
        );

        let epoch = client.lease().runtime.epoch;
        // Drop the client — simulates process death
        drop(client);
        epoch
    };

    // --- Phase 2: Second incarnation (new epoch, same stable_id) reads data ---
    {
        let transport = Arc::new(TestTransport::new("restart-recovery-segment-v2"));
        let client = StoreClientBuilder::new(metadata.clone(), "restart-recovery")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
            .transport(transport)
            .local_memory(storage_config_with_bytes(512))
            .cold_tier_target(ColdTierTargetConfig::directory(
                "restart-recovery",
                ColdTierKind::Ssd,
                root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("second incarnation build should succeed");
        client
            .register_local_memory()
            .expect("second incarnation local memory should register");

        // Verify new epoch was allocated
        let second_epoch = client.lease().runtime.epoch;
        assert!(
            second_epoch > first_epoch,
            "second incarnation should have a higher epoch: first={:?} second={:?}",
            first_epoch,
            second_epoch
        );

        // Core assertion: data must be readable from cold tier after restart
        let result_a = client.get("restart-key-a");
        let result_b = client.get("restart-key-b");

        assert_eq!(
            result_a.expect("post-restart get A should succeed from cold tier"),
            payload_a,
            "payload A content mismatch after restart"
        );
        assert_eq!(
            result_b.expect("post-restart get B should succeed from cold tier"),
            payload_b,
            "payload B content mismatch after restart"
        );

        // Verify the cold backing state is still Materialized
        let observed_a = client
            .query_route("restart-key-a")
            .expect("post-restart route query should succeed")
            .expect("route should exist after restart");
        assert_eq!(
            observed_a
                .cold_backing
                .as_ref()
                .expect("cold backing should exist after restart")
                .state,
            mooncake_store_core::ColdBackingState::Materialized,
            "cold backing should remain Materialized after restart"
        );
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(&root);
}
