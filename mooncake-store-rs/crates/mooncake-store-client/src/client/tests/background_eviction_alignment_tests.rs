use super::*;

struct BackgroundEvictionRouteDirectory {
    inner: Arc<dyn RouteDirectory>,
    single_cas_calls: AtomicUsize,
    batch_cas_calls: AtomicUsize,
    max_cas_batch_size: AtomicUsize,
    lose_batch_reply_once: AtomicBool,
}

impl BackgroundEvictionRouteDirectory {
    fn new(inner: Arc<dyn RouteDirectory>) -> Self {
        Self {
            inner,
            single_cas_calls: AtomicUsize::new(0),
            batch_cas_calls: AtomicUsize::new(0),
            max_cas_batch_size: AtomicUsize::new(0),
            lose_batch_reply_once: AtomicBool::new(false),
        }
    }
}

impl RouteDirectory for BackgroundEvictionRouteDirectory {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.inner.get_object_route(observer, key)
    }

    fn get_object_routes_bounded(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<Vec<Option<ObjectRoute>>> {
        self.inner.get_object_routes_bounded(observer, keys)
    }

    fn confirm_replica_reference(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        replica: &ReplicaRoute,
    ) -> mooncake_store_core::Result<mooncake_store_core::RouteReplicaReference> {
        self.inner.confirm_replica_reference(observer, key, replica)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        self.single_cas_calls.fetch_add(1, Ordering::Relaxed);
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<mooncake_store_core::CasResult>>>
    {
        self.batch_cas_calls.fetch_add(1, Ordering::Relaxed);
        self.max_cas_batch_size
            .fetch_max(requests.len(), Ordering::Relaxed);
        let results = self
            .inner
            .compare_and_swap_object_routes(observer, requests)?;
        if self.lose_batch_reply_once.swap(false, Ordering::Relaxed) {
            assert!(results
                .iter()
                .all(|result| matches!(result, Ok(cas) if cas.applied)));
            return Err(StoreError::Transport(
                "batch CAS reply lost after apply".to_string(),
            ));
        }
        Ok(results)
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
}

#[test]
fn background_watermark_eviction_batches_route_cas_for_small_replicas() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("background-batch-evict-segment"));
    let client = StoreClientBuilder::new(metadata, "background-batch-evict")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(100))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let routes = (0..5)
        .map(|index| {
            let key = format!("background-batch-{index}");
            client
                .put(&key, &[index as u8; 20])
                .expect("put should succeed")
        })
        .collect::<Vec<_>>();
    let counting_directory = Arc::new(BackgroundEvictionRouteDirectory::new(
        client.route_directory.clone(),
    ));
    let storage_owner = test_storage_owner_state_with_route_directory(
        &client,
        counting_directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    storage_owner.track_routes(&routes);

    let evicted = storage_owner
        .evict_until_low_watermark(100, 40)
        .expect("background eviction should succeed");

    assert_eq!(evicted, 3);
    assert_eq!(client.allocator.lock().usage_bytes().0, 40);
    assert_eq!(
        counting_directory.single_cas_calls.load(Ordering::Relaxed),
        0,
        "background eviction should not publish one CAS per replica"
    );
    assert_eq!(
        counting_directory.batch_cas_calls.load(Ordering::Relaxed),
        1
    );
    assert!(
        counting_directory
            .max_cas_batch_size
            .load(Ordering::Relaxed)
            >= 3
    );

    let metrics = render_prometheus_metrics();
    let counter_value = |name: &str| {
        metrics
            .lines()
            .find(|line| line.starts_with(&format!("{name}{{")))
            .and_then(|line| line.split_whitespace().last())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or_default()
    };
    assert!(counter_value("mooncake_store_evicted_replicas_total") >= 3);
    assert!(counter_value("mooncake_store_evicted_bytes_total") >= 60);
}

#[test]
fn background_watermark_batch_keeps_allocation_when_route_cas_conflicts() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("background-batch-conflict-segment"));
    let client = StoreClientBuilder::new(metadata, "background-batch-conflict")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(60))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");

    let routes = (0..3)
        .map(|index| {
            let key = format!("background-batch-conflict-{index}");
            client
                .put(&key, &[index as u8; 20])
                .expect("put should succeed")
        })
        .collect::<Vec<_>>();
    let conflicted = Arc::new(AtomicBool::new(false));
    let route_directory = Arc::new(EmptyConflictRouteDirectory {
        inner: client.route_directory.clone(),
        conflict_key: routes[0].key.clone(),
        bounded_reads: Arc::new(AtomicUsize::new(0)),
        conflicted: conflicted.clone(),
    });
    let counting_directory = Arc::new(BackgroundEvictionRouteDirectory::new(route_directory));
    let storage_owner = test_storage_owner_state_with_route_directory(
        &client,
        counting_directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    storage_owner.track_routes(&routes);

    let evicted = storage_owner
        .evict_until_low_watermark(100, 0)
        .expect("conflicted batch eviction should preserve authoritative bytes");

    assert!(conflicted.load(Ordering::Relaxed));
    assert_eq!(
        counting_directory.single_cas_calls.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        counting_directory.batch_cas_calls.load(Ordering::Relaxed),
        2
    );
    assert_eq!(evicted, 2);
    assert_eq!(client.allocator.lock().usage_bytes().0, 20);
    assert!(client
        .query_route("background-batch-conflict-0")
        .expect("conflicted route query should succeed")
        .is_some());
}

#[test]
fn background_watermark_batch_retains_allocations_when_applied_reply_is_lost() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("background-batch-reply-loss-segment"));
    let client = StoreClientBuilder::new(metadata, "background-batch-reply-loss")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport)
        .local_memory(storage_config_with_bytes(60))
        .build(test_future_expiry_ms())
        .expect("client build should succeed");
    client
        .register_local_memory()
        .expect("local memory should register");
    let routes = (0..3)
        .map(|index| {
            client
                .put(&format!("batch-reply-loss-{index}"), &[index as u8; 20])
                .expect("put should succeed")
        })
        .collect::<Vec<_>>();
    let directory = Arc::new(BackgroundEvictionRouteDirectory::new(
        client.route_directory.clone(),
    ));
    directory
        .lose_batch_reply_once
        .store(true, Ordering::Relaxed);
    let owner = test_storage_owner_state_with_route_directory(
        &client,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    owner.track_routes(&routes);

    assert_eq!(
        owner
            .evict_until_low_watermark(60, 0)
            .expect("unknown batch outcome should retain allocations"),
        0
    );
    assert_eq!(directory.batch_cas_calls.load(Ordering::Relaxed), 1);
    assert!(!directory.lose_batch_reply_once.load(Ordering::Relaxed));
    assert_eq!(owner.pending_evictions.lock().len(), 3);
    assert_eq!(client.allocator.lock().usage_bytes().0, 60);
    for route in &routes {
        assert!(
            client
                .route_directory
                .get_object_route(&client.lease, &route.key)
                .expect("authoritative route query should succeed")
                .is_none(),
            "route CAS must really apply before the reply is lost"
        );
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut released = 0;
    while !owner.pending_evictions.lock().is_empty() && std::time::Instant::now() < deadline {
        released += owner
            .process_pending_evictions(3)
            .expect("real authority confirmation should succeed");
        sleep(Duration::from_millis(10));
    }
    assert_eq!(
        released, 3,
        "each retained allocation should release exactly once"
    );
    assert!(owner.pending_evictions.lock().is_empty());
    assert_eq!(client.allocator.lock().usage_bytes().0, 0);
    assert_eq!(
        owner
            .process_pending_evictions(3)
            .expect("empty pending queue should succeed"),
        0
    );
}
