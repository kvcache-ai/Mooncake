use super::*;

struct WaveBackend {
    inner: LocalDirPersistentStorageBackend,
    rendezvous: Option<Arc<(StdMutex<usize>, Condvar)>>,
    fail_once: AtomicBool,
}

impl PersistentStorageBackend for WaveBackend {
    fn put_object(
        &self,
        backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> mooncake_store_core::Result<mooncake_store_core::ColdBackingRoute> {
        if let Some(gate) = &self.rendezvous {
            let (lock, ready) = &**gate;
            let mut arrived = lock.lock().unwrap();
            *arrived += 1;
            ready.notify_all();
            let (arrived, _) = ready
                .wait_timeout_while(arrived, Duration::from_secs(2), |arrived| *arrived < 2)
                .unwrap();
            if *arrived < 2 {
                return Err(StoreError::Transport("device groups ran serially".into()));
            }
        }
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "injected offload write failure".into(),
            ));
        }
        self.inner.put_object(backing, payload)
    }

    fn get_object(
        &self,
        backing: &mooncake_store_core::ColdBackingRoute,
    ) -> mooncake_store_core::Result<Option<Vec<u8>>> {
        self.inner.get_object(backing)
    }

    fn delete_object(
        &self,
        backing: &mooncake_store_core::ColdBackingRoute,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.delete_object(backing)
    }
    fn put_pending_source(
        &self,
        backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> mooncake_store_core::Result<()> {
        self.inner.put_pending_source(backing, payload)
    }
    fn get_pending_source(
        &self,
        backing: &mooncake_store_core::ColdBackingRoute,
    ) -> mooncake_store_core::Result<Option<Vec<u8>>> {
        self.inner.get_pending_source(backing)
    }
    fn delete_pending_source(
        &self,
        backing: &mooncake_store_core::ColdBackingRoute,
    ) -> mooncake_store_core::Result<bool> {
        self.inner.delete_pending_source(backing)
    }
}

fn wave_client(
    name: &str,
    devices: usize,
    device_limit: usize,
    parallel: bool,
    fail: bool,
) -> StoreClient {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let gate = Arc::new((StdMutex::new(0), Condvar::new()));
    let mut builder = StoreClientBuilder::new(metadata, name)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(Arc::new(TestTransport::new(&format!("{name}-segment"))))
        .local_memory(storage_config_with_bytes(4096))
        .cold_tier_rate_limits(
            ColdTierRateLimitConfig {
                pressure_offload_boost: device_limit,
                pressure_soft_threshold: usize::MAX - 1,
                pressure_hard_threshold: usize::MAX,
                offload_poll_interval_ms: 60_000,
                ..ColdTierRateLimitConfig::default()
            }
            .offload_runtime_max_in_flight(8)
            .offload_device_max_in_flight(device_limit),
        );
    let mut targets = Vec::new();
    for index in 0..devices {
        let id = format!("{name}-{index}");
        let root = cold_tier_test_root(&id);
        targets.push(ColdTierTargetConfig::directory(
            &id,
            ColdTierKind::Ssd,
            root.clone(),
        ));
        builder = builder.cold_tier_backend_override(
            &id,
            Arc::new(WaveBackend {
                inner: LocalDirPersistentStorageBackend::new_with_root(root),
                rendezvous: parallel.then(|| gate.clone()),
                fail_once: AtomicBool::new(fail && index == 0),
            }),
        );
    }
    let client = builder
        .cold_tier_targets(targets)
        .build(test_future_expiry_ms())
        .unwrap();
    client.register_local_memory().unwrap();
    client
}

fn wave_route(client: &StoreClient, name: &str, device: &str) -> ObjectRoute {
    client.put(name, &[17; 32]).unwrap();
    let route = client.query_route(name).unwrap().unwrap();
    let mut next = route.clone();
    next.version = route.version.next();
    let backing = next
        .cold_backing
        .as_mut()
        .expect("put should publish pending backing");
    backing.cold_tier_id = device.into();
    backing.state = mooncake_store_core::ColdBackingState::PendingOffload;
    assert!(
        client
            .route_ops()
            .compare_and_swap_route(&route.key, Some(route.version), Some(&next))
            .unwrap()
            .applied
    );
    client
        .storage_owner
        .pending_offloads
        .discard_for_debug_evict_all(&next.key);
    client.storage_owner.enqueue_pending_offload(&next);
    next
}

#[test]
fn offload_waves_advance_admission_deferred_in_same_round() {
    let _guard = enable_cold_tier_for_test();
    let client = wave_client("wave-admission", 1, 1, false, false);
    let routes = (0..3)
        .map(|i| wave_route(&client, &format!("admission-{i}"), "wave-admission-0"))
        .collect::<Vec<_>>();
    assert_eq!(
        cold_tier::materialize_pending_offload_routes_bounded(&client.storage_owner, &routes)
            .unwrap(),
        3
    );
    for route in routes {
        assert_eq!(
            client
                .route_ops()
                .load_route(&route.key)
                .unwrap()
                .unwrap()
                .cold_backing
                .unwrap()
                .state,
            mooncake_store_core::ColdBackingState::Materialized
        );
    }
}

#[test]
fn offload_waves_execute_device_groups_concurrently() {
    let _guard = enable_cold_tier_for_test();
    let client = wave_client("wave-parallel", 2, 1, true, false);
    let routes = vec![
        wave_route(&client, "parallel-a", "wave-parallel-0"),
        wave_route(&client, "parallel-b", "wave-parallel-1"),
    ];
    assert_eq!(
        cold_tier::materialize_pending_offload_routes_bounded(&client.storage_owner, &routes)
            .unwrap(),
        2
    );
}

#[test]
fn offload_waves_retry_failed_claim_without_losing_other_device() {
    let _guard = enable_cold_tier_for_test();
    let client = wave_client("wave-error", 2, 1, false, true);
    let routes = vec![
        wave_route(&client, "error-a", "wave-error-0"),
        wave_route(&client, "error-b", "wave-error-1"),
    ];
    assert_eq!(
        cold_tier::materialize_pending_offload_routes_bounded(&client.storage_owner, &routes)
            .unwrap(),
        1
    );
    assert_eq!(
        cold_tier::materialize_pending_offload_routes_bounded(&client.storage_owner, &routes)
            .unwrap(),
        1
    );
    for route in routes {
        assert_eq!(
            client
                .route_ops()
                .load_route(&route.key)
                .unwrap()
                .unwrap()
                .cold_backing
                .unwrap()
                .state,
            mooncake_store_core::ColdBackingState::Materialized
        );
    }
}

#[test]
fn offload_waves_retry_all_claims_after_prepare_lookup_failure() {
    let _guard = enable_cold_tier_for_test();
    let metadata = Arc::new(RecoverableMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let root = cold_tier_test_root("wave-prepare-error");
    let client = StoreClientBuilder::new(metadata.clone(), "wave-prepare-error")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(Arc::new(TestTransport::new("wave-prepare-error-segment")))
        .local_memory(storage_config_with_bytes(4096))
        .cold_tier_target(ColdTierTargetConfig::directory(
            "wave-prepare-error-0",
            ColdTierKind::Ssd,
            root,
        ))
        .build(test_future_expiry_ms())
        .unwrap();
    client.register_local_memory().unwrap();
    let routes = (0..3)
        .map(|i| {
            wave_route(
                &client,
                &format!("prepare-error-{i}"),
                "wave-prepare-error-0",
            )
        })
        .collect::<Vec<_>>();
    metadata.fail_route_reads_for(routes[1].key.clone());
    assert!(
        cold_tier::materialize_pending_offload_routes_bounded(&client.storage_owner, &routes)
            .is_err()
    );
    metadata.allow_route_reads_for(&routes[1].key);
    assert_eq!(
        cold_tier::materialize_pending_offload_routes_bounded(&client.storage_owner, &routes)
            .unwrap(),
        3
    );
}
