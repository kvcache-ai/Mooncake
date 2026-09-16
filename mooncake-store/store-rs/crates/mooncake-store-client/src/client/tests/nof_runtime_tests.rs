use crate::{
    NofBackend, NofBacking, NofHealth, NofManagedAllocationRequest, NofManagedAllocator,
    NofManagedLimits, NofManagedLocator, NofManagedRead, NofManagedReadRequest, NofManagedWrite,
    NofManagedWriteRequest, NofObjectDelete, NofObjectLimits, NofObjectQuery, NofObjectRead,
    NofObjectShardWrite, NofObjectState, NofObjectWrite, NofPhysicalDelete,
    NofPhysicalDeleteRequest, NofPhysicalQuery, NofPhysicalQueryRequest, NofPhysicalRead,
    NofPhysicalReadRequest, NofPhysicalWrite, NofPhysicalWriteRequest, NofStorageHealth,
    NofTargetConfig,
};

use super::cold_tier::nof::managed_backend::NofManagedStorageBackend;
use super::cold_tier::nof::NofManagedRecovery;
use crate::client::RecoveredColdObject;

struct FakePhysicalNof {
    records: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    available: AtomicBool,
    fail_next_read: AtomicBool,
    fail_next_write: AtomicBool,
}

impl Default for FakePhysicalNof {
    fn default() -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            available: AtomicBool::new(true),
            fail_next_read: AtomicBool::new(false),
            fail_next_write: AtomicBool::new(false),
        }
    }
}

impl NofPhysicalWrite for FakePhysicalNof {
    fn put_batch(
        &self,
        requests: &[NofPhysicalWriteRequest<'_>],
    ) -> Vec<Result<()>> {
        let mut records = self.records.lock();
        requests
            .iter()
            .map(|request| {
                if self.fail_next_write.swap(false, Ordering::Relaxed) {
                    return Err(StoreError::Transport(
                        "injected NoF write failure".to_string(),
                    ));
                }
                records.insert(request.key.as_bytes().to_vec(), request.value.to_vec());
                Ok(())
            })
            .collect()
    }
}

impl NofPhysicalRead for FakePhysicalNof {
    fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
        let records = self.records.lock();
        requests
            .iter()
            .map(|request| {
                if self.fail_next_read.swap(false, Ordering::Relaxed) {
                    return Err(StoreError::Transport(
                        "injected NoF read failure".to_string(),
                    ));
                }
                Ok(records.get(request.key.as_bytes()).cloned())
            })
            .collect()
    }
}

impl NofPhysicalQuery for FakePhysicalNof {
    fn query_batch(&self, requests: &[NofPhysicalQueryRequest]) -> Vec<Result<Option<u64>>> {
        let records = self.records.lock();
        requests
            .iter()
            .map(|request| Ok(records.get(request.key.as_bytes()).map(|value| value.len() as u64)))
            .collect()
    }
}

impl NofPhysicalDelete for FakePhysicalNof {
    fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>> {
        let mut records = self.records.lock();
        requests
            .iter()
            .map(|request| {
                records.remove(request.key.as_bytes());
                Ok(())
            })
            .collect()
    }
}

impl NofBacking for FakePhysicalNof {
    fn physical_write(&self) -> Option<&dyn NofPhysicalWrite> {
        Some(self)
    }

    fn physical_read(&self) -> Option<&dyn NofPhysicalRead> {
        Some(self)
    }

    fn physical_query(&self) -> Option<&dyn NofPhysicalQuery> {
        Some(self)
    }

    fn physical_delete(&self) -> Option<&dyn NofPhysicalDelete> {
        Some(self)
    }

    fn health_capability(&self) -> Option<&dyn NofHealth> {
        Some(self)
    }
}

impl NofHealth for FakePhysicalNof {
    fn health(&self) -> Result<NofStorageHealth> {
        if self.available.load(Ordering::Relaxed) {
            Ok(NofStorageHealth::default())
        } else {
            Err(StoreError::Transport(
                "fake NoF target is unavailable".to_string(),
            ))
        }
    }
}

#[derive(Default)]
struct FakeManagedNof {
    records: Mutex<std::collections::HashMap<NofManagedLocator, Vec<u8>>>,
    writes: AtomicUsize,
    fail_next_reserve: AtomicBool,
    disable_recovery: AtomicBool,
    releases: AtomicUsize,
    recoveries: AtomicUsize,
    manifest_scans: AtomicUsize,
}

impl NofManagedWrite for FakeManagedNof {
    fn put_batch(&self, requests: &[NofManagedWriteRequest<'_>]) -> Vec<Result<()>> {
        let mut records = self.records.lock();
        requests
            .iter()
            .map(|request| {
                records.insert(request.locator.clone(), request.value.to_vec());
                self.writes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .collect()
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl NofManagedRead for FakeManagedNof {
    fn get_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
        let records = self.records.lock();
        requests
            .iter()
            .map(|request| Ok(records.get(&request.locator).cloned()))
            .collect()
    }
}

impl NofManagedAllocator for FakeManagedNof {
    fn recover(&self, records: &[NofManagedReadRequest]) -> Result<()> {
        self.recoveries.fetch_add(records.len(), Ordering::Relaxed);
        Ok(())
    }

    fn reserve_batch(
        &self,
        requests: &[NofManagedAllocationRequest],
    ) -> Vec<Result<NofManagedLocator>> {
        requests
            .iter()
            .map(|request| {
                if self.fail_next_reserve.swap(false, Ordering::Relaxed) {
                    return Err(StoreError::Backpressure(
                        "injected managed NoF reserve failure".to_string(),
                    ));
                }
                NofManagedLocator::new(request.key.as_bytes().to_vec())
            })
            .collect()
    }

    fn release_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<()>> {
        let mut records = self.records.lock();
        requests
            .iter()
            .map(|request| {
                records.remove(&request.locator);
                self.releases.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .collect()
    }
}

impl NofManagedRecovery for FakeManagedNof {
    fn scan_recovered_objects(&self, _target_id: &str) -> Result<Vec<RecoveredColdObject>> {
        self.manifest_scans.fetch_add(1, Ordering::Relaxed);
        Ok(Vec::new())
    }
}

impl NofBacking for FakeManagedNof {

    fn managed_limits(&self) -> Option<NofManagedLimits> {
        Some(NofManagedLimits {
            max_batch_items: 16,
            max_batch_bytes: 1024 * 1024,
        })
    }

    fn managed_write(&self) -> Option<&dyn NofManagedWrite> {
        Some(self)
    }

    fn managed_read(&self) -> Option<&dyn NofManagedRead> {
        Some(self)
    }

    fn managed_allocator(&self) -> Option<&dyn NofManagedAllocator> {
        Some(self)
    }

    fn managed_recovery(&self) -> Option<&dyn NofManagedRecovery> {
        (!self.disable_recovery.load(Ordering::Relaxed)).then_some(self)
    }

    fn health_capability(&self) -> Option<&dyn NofHealth> {
        Some(self)
    }
}

impl NofHealth for FakeManagedNof {
    fn health(&self) -> Result<NofStorageHealth> {
        let capacity = 1024usize * 1024;
        let used = self
            .records
            .lock()
            .values()
            .map(Vec::len)
            .sum::<usize>();
        Ok(NofStorageHealth {
            capacity_bytes: Some(capacity as u64),
            available_bytes: Some(capacity.saturating_sub(used) as u64),
        })
    }
}

#[derive(Default)]
struct FakeObjectNof {
    objects: Mutex<BTreeMap<(NamespaceScope, String), Vec<u8>>>,
    fail_next_delete: AtomicBool,
}

impl NofObjectWrite for FakeObjectNof {
    fn init_namespace(&self, _namespace: &NamespaceScope) -> Result<()> {
        Ok(())
    }

    fn put_shards(&self, requests: &[NofObjectShardWrite<'_>]) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| {
                if request.shard_id != 0 || request.total_shards != 1 {
                    return Err(StoreError::InvalidState(
                        "fake object target only accepts one shard".to_string(),
                    ));
                }
                self.objects.lock().insert(
                    (request.namespace.clone(), request.key.to_string()),
                    request.value.to_vec(),
                );
                Ok(())
            })
            .collect()
    }
}

impl NofObjectRead for FakeObjectNof {
    fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
        _known_length: Option<u64>,
    ) -> Result<NofObjectState<Vec<u8>>> {
        Ok(match self.objects.lock().get(&(namespace.clone(), key.to_string())) {
            Some(value) => NofObjectState::Found(value.clone()),
            None => NofObjectState::Missing,
        })
    }
}

impl NofObjectQuery for FakeObjectNof {
    fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<u64>> {
        Ok(match self.objects.lock().get(&(namespace.clone(), key.to_string())) {
            Some(value) => NofObjectState::Found(value.len() as u64),
            None => NofObjectState::Missing,
        })
    }
}

impl NofObjectDelete for FakeObjectNof {
    fn delete_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<()>> {
        if self.fail_next_delete.swap(false, Ordering::Relaxed) {
            return Err(StoreError::Transport(
                "injected NoF delete failure".to_string(),
            ));
        }
        Ok(if self
            .objects
            .lock()
            .remove(&(namespace.clone(), key.to_string()))
            .is_some()
        {
            NofObjectState::Found(())
        } else {
            NofObjectState::Missing
        })
    }
}

impl NofBacking for FakeObjectNof {
    fn object_limits(&self) -> Option<NofObjectLimits> {
        Some(NofObjectLimits {
            max_value_size: 1024,
            max_key_size: 1024,
        })
    }

    fn object_write(&self) -> Option<&dyn NofObjectWrite> {
        Some(self)
    }

    fn object_read(&self) -> Option<&dyn NofObjectRead> {
        Some(self)
    }

    fn object_query(&self) -> Option<&dyn NofObjectQuery> {
        Some(self)
    }

    fn object_delete(&self) -> Option<&dyn NofObjectDelete> {
        Some(self)
    }
}

fn wait_for_nof_reclaims(client: &StoreClient, mut is_empty: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        client
            .flush_due_reclaims()
            .expect("NoF reclaims should flush");
        if is_empty() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(10));
    }
}

fn physical_nof_client(
    stable_id: &str,
) -> (StoreClient, Arc<FakePhysicalNof>, Arc<FakePhysicalNof>) {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let primary = Arc::new(FakePhysicalNof::default());
    let replica = Arc::new(FakePhysicalNof::default());
    let targets = [
        NofTargetConfig::new(
            "nof-physical-a",
            NofBackend::new(primary.clone()).expect("primary NoF backend should build"),
        )
        .expect("primary NoF target should build"),
        NofTargetConfig::new(
            "nof-physical-b",
            NofBackend::new(replica.clone()).expect("replica NoF backend should build"),
        )
        .expect("replica NoF target should build"),
    ];
    let client = StoreClientBuilder::new(metadata, stable_id)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new("nof-physical-runtime-segment")))
        .local_memory(storage_config_with_bytes(512))
        .nof_targets(targets)
        .nof_replica_count(2)
        .build(test_future_expiry_ms())
        .expect("physical NoF client should build");
    client
        .register_local_memory()
        .expect("physical NoF local memory should register");
    (client, primary, replica)
}

#[test]
fn nof_targets_require_shared_embedded_route_metadata() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let target = NofTargetConfig::new(
        "nof-metadata-only",
        NofBackend::new(Arc::new(FakePhysicalNof::default()))
            .expect("physical NoF backend should build"),
    )
    .expect("NoF target should build");
    let error = match StoreClientBuilder::new(metadata, "nof-metadata-only-client")
        .route_control(RouteControlMode::MetadataOnly)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new("nof-metadata-only-segment")))
        .local_memory(storage_config_with_bytes(512))
        .nof_target(target)
        .build(test_future_expiry_ms())
    {
        Ok(_) => panic!("NoF owner metadata cannot use process-local MetadataOnly mode"),
        Err(error) => error,
    };
    assert!(matches!(error, StoreError::InvalidState(message) if message.contains("EmbeddedWrh")));
}

#[test]
fn provider_owned_physical_nof_keeps_placement_out_of_object_route() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let (client, primary, replica) = physical_nof_client("nof-physical-runtime");

    let payload = [42u8; 150];
    client
        .put("physical-nof-key", &payload)
        .expect("physical NoF put should succeed");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("physical NoF offload should run"),
        1
    );
    let route = client
        .query_route("physical-nof-key")
        .expect("physical NoF route query should succeed")
        .expect("physical NoF route should exist");
    assert!(route.cold_backing.is_none());
    assert!(!primary.records.lock().is_empty());
    assert!(!replica.records.lock().is_empty());

    assert_eq!(
        crate::client::cold_tier::repair_initial_write_cold_backings(
            client.storage_owner.as_ref(),
            32,
        )
        .expect("periodic local backing repair should ignore NoF routes"),
        0
    );
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("periodic repair must not enqueue another NoF write"),
        0
    );

    let mut replaced_route = route.clone();
    replaced_route.replicas.clear();
    client
        .schedule_route_reclaim(&replaced_route)
        .expect("storage-only reclaim should schedule");
    client
        .flush_due_reclaims()
        .expect("storage-only reclaim should flush");
    assert!(!primary.records.lock().is_empty());
    assert!(!replica.records.lock().is_empty());

    force_cold_only_route(&client, "physical-nof-key");
    assert!(client
        .is_exist("physical-nof-key")
        .expect("NoF existence probe should succeed"));
    assert_eq!(
        client
            .batch_is_exist(&[ObjectRef::new("physical-nof-key")])
            .expect("NoF batch existence probe should succeed"),
        vec![true]
    );
    primary.fail_next_read.store(true, Ordering::Relaxed);
    assert_eq!(
        client
            .get("physical-nof-key")
            .expect("physical NoF restore should fail over to the queried replica"),
        payload
    );
    client
        .remove("physical-nof-key", true)
        .expect("physical NoF delete should succeed");
    assert!(
        wait_for_nof_reclaims(&client, || {
            primary.records.lock().is_empty() && replica.records.lock().is_empty()
        }),
        "physical NoF records remain: primary={} replica={} route={:?}",
        primary.records.lock().len(),
        replica.records.lock().len(),
        client.query_route("physical-nof-key")
    );
    assert!(primary.records.lock().is_empty());
    assert!(replica.records.lock().is_empty());
}


#[test]
fn managed_nof_persists_route_reads_cold_and_releases_locator() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakeManagedNof::default());
    let transport = Arc::new(TestTransport::new("nof-managed-segment"));
    let target = NofTargetConfig::new(
        "nof-managed-a",
        NofBackend::new(backend.clone()).expect("managed NoF backend should build"),
    )
    .expect("managed NoF target should build");
    let client = StoreClientBuilder::new(metadata.clone(), "nof-managed-runtime")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(512))
        .nof_target(target)
        .build(test_future_expiry_ms())
        .expect("managed NoF client should build");
    client
        .register_local_memory()
        .expect("managed NoF local memory should register");

    let payload = [31u8; 150];
    client
        .put("managed-nof-key", &payload)
        .expect("managed NoF put should succeed");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("managed NoF offload should run"),
        1
    );
    let route = client
        .query_route("managed-nof-key")
        .expect("managed route query should succeed")
        .expect("managed route should exist");
    assert!(route.cold_backing.is_none());
    let backing = route
        .nof_backing
        .as_ref()
        .expect("managed route should persist NoF placement");
    assert_eq!(backing.target_id, "nof-managed-a");
    assert_eq!(
        backing.state,
        mooncake_store_core::ColdBackingState::Materialized
    );
    assert_eq!(backend.records.lock().len(), 1);

    let reader_target = NofTargetConfig::new(
        "nof-managed-a",
        NofBackend::new(backend.clone()).expect("managed NoF reader backend should build"),
    )
    .expect("managed NoF reader target should build");
    let reader = StoreClientBuilder::new(metadata, "nof-managed-reader")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .transport(Arc::new(transport.peer("nof-managed-reader-segment")))
        .local_memory(storage_config_with_bytes(0))
        .nof_target(reader_target)
        .build(test_future_expiry_ms())
        .expect("managed NoF reader should build without storage memory");
    reader
        .register_local_memory()
        .expect("managed NoF reader scratch memory should register");

    force_cold_only_route(&client, "managed-nof-key");
    assert_eq!(
        reader
            .batch_get(&[ObjectRef::new("managed-nof-key")])
            .expect("embedded client should batch-read managed NoF into its destination buffer"),
        vec![payload.to_vec()]
    );
    reader.wait_for_restore_promotions();
    let restored_route = reader
        .query_route("managed-nof-key")
        .expect("managed route query after read should succeed")
        .expect("managed route should remain");
    assert!(restored_route.replicas.is_empty());
    assert!(restored_route.nof_backing.is_some());

    client
        .remove("managed-nof-key", true)
        .expect("managed NoF delete should succeed");
    assert!(
        wait_for_nof_reclaims(&client, || backend.records.lock().is_empty()),
        "managed NoF locator remains allocated: {:?}",
        backend.records.lock().keys().collect::<Vec<_>>()
    );
}

#[test]
fn managed_nof_pending_allocation_is_not_released_before_publish() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakeManagedNof::default());
    let target = NofTargetConfig::new(
        "nof-managed-pending-release",
        NofBackend::new(backend.clone()).expect("managed NoF backend should build"),
    )
    .expect("managed NoF target should build");
    let client = StoreClientBuilder::new(metadata, "nof-managed-pending-release-runtime")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "nof-managed-pending-release-segment",
        )))
        .local_memory(storage_config_with_bytes(512))
        .nof_target(target)
        .build(test_future_expiry_ms())
        .expect("managed NoF client should build");
    client
        .register_local_memory()
        .expect("managed NoF memory should register");

    client
        .put("managed-nof-pending-release-key", &[37u8; 150])
        .expect("managed NoF put should reserve a locator");
    let pending = client
        .query_route("managed-nof-pending-release-key")
        .expect("pending route query should succeed")
        .expect("pending route should exist");
    assert_eq!(
        pending.nof_backing.as_ref().unwrap().state,
        mooncake_store_core::ColdBackingState::PendingOffload
    );

    let release = client
        .storage_owner
        .cold_tier_devices
        .nof_targets
        .release_managed_target("nof-managed-pending-release", pending);
    assert!(matches!(release, Err(StoreError::Backpressure(_))));
    assert_eq!(backend.releases.load(Ordering::Relaxed), 0);
    assert!(client
        .query_route("managed-nof-pending-release-key")
        .unwrap()
        .unwrap()
        .nof_backing
        .is_some());

    assert!(matches!(
        client.remove("managed-nof-pending-release-key", true),
        Err(StoreError::Backpressure(_))
    ));
    let materialized = (0..2)
        .map(|_| {
            client
                .storage_owner
                .materialize_pending_offloads_bounded(32)
                .expect("deleting pending allocation should finish materializing")
        })
        .sum::<usize>();
    assert_eq!(materialized, 1);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        client.flush_due_reclaims().unwrap();
        if client
            .query_route("managed-nof-pending-release-key")
            .unwrap()
            .is_none()
            && backend.records.lock().is_empty()
        {
            break;
        }
        assert!(Instant::now() < deadline, "deleting route did not finish");
        sleep(Duration::from_millis(10));
    }
}

#[test]
fn managed_nof_watermark_refreshes_capacity_and_stops_at_low() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let backend = Arc::new(FakeManagedNof::default());
    let target = NofTargetConfig::new(
        "nof-managed-watermark",
        NofBackend::new(backend.clone()).expect("managed NoF backend should build"),
    )
    .expect("managed NoF target should build");
    let client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "nof-managed-watermark-runtime",
    )
    .state(ClientLifecycleState::Active)
    .label("storage", "true")
    .transport(Arc::new(TestTransport::new(
        "nof-managed-watermark-segment",
    )))
    .local_memory(storage_config_with_bytes(1024))
    .cold_tier_watermarks(
        ColdTierWatermarkConfig::default()
            .high_bytes(250)
            .low_bytes(150),
    )
    .nof_target(target)
    .build(test_future_expiry_ms())
    .expect("managed NoF client should build");
    client.register_local_memory().unwrap();
    for index in 0..3 {
        let key = format!("managed-nof-watermark-{index}");
        client
            .put(&key, &[index as u8; 100])
            .unwrap();
    }
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .unwrap(),
        3
    );
    let result = client
        .storage_owner
        .free_cold_tier_until_low_watermark_bounded(1, 8)
        .unwrap();
    assert_eq!(result.freed_backings, 2);
    assert!(result.reached_low_watermark);
    assert_eq!(backend.records.lock().len(), 1);
}

#[test]
fn managed_nof_partial_prepare_rebuilds_deleting_offload_and_releases() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend_a = Arc::new(FakeManagedNof::default());
    let backend_b = Arc::new(FakeManagedNof::default());
    backend_b
        .fail_next_reserve
        .store(true, Ordering::Relaxed);
    let targets = [
        NofTargetConfig::new(
            "nof-managed-replica-a",
            NofBackend::new(backend_a.clone()).expect("first managed backend should build"),
        )
        .expect("first managed target should build"),
        NofTargetConfig::new(
            "nof-managed-replica-b",
            NofBackend::new(backend_b.clone()).expect("second managed backend should build"),
        )
        .expect("second managed target should build"),
    ];
    let client = StoreClientBuilder::new(metadata, "nof-managed-replica-runtime")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new(
            "nof-managed-replica-segment",
        )))
        .local_memory(storage_config_with_bytes(512))
        .nof_targets(targets)
        .nof_replica_count(2)
        .build(test_future_expiry_ms())
        .expect("managed NoF client should build");
    client
        .register_local_memory()
        .expect("managed NoF memory should register");

    let payload = [41u8; 150];
    client
        .put("managed-nof-replica-key", &payload)
        .expect("hot put should survive a temporary second-target reserve failure");
    let partial = client
        .query_route("managed-nof-replica-key")
        .unwrap()
        .unwrap();
    assert!(partial.nof_backing.as_ref().unwrap().replicas.is_empty());

    let publish = client
        .storage_owner
        .cold_tier_devices
        .nof_targets
        .publish_managed_route(partial.clone());
    assert!(matches!(publish, Err(StoreError::Backpressure(_))));

    client.storage_owner.pending_offloads.clear();
    assert!(matches!(
        client.remove("managed-nof-replica-key", true),
        Err(StoreError::Backpressure(_))
    ));

    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("retry should fill the missing target and materialize the payload"),
        1
    );
    assert_eq!(backend_a.writes.load(Ordering::Relaxed), 1);
    assert_eq!(backend_b.writes.load(Ordering::Relaxed), 1);
    client
        .storage_owner
        .free_cold_tier_until_low_watermark_bounded(8, 8)
        .unwrap();
    assert!(backend_a.records.lock().is_empty());
    assert!(backend_b.records.lock().is_empty());
    assert!(client.query_route("managed-nof-replica-key").unwrap().is_none());
}

#[test]
fn managed_nof_hot_replica_owner_writes_for_remote_target_owner() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakeManagedNof::default());
    let target = NofTargetConfig::new(
        "nof-managed-remote-owner-target",
        NofBackend::new(backend.clone()).expect("managed NoF backend should build"),
    )
    .expect("managed NoF target should build");
    let build = |stable_id: &str, segment: &str| {
        let client = StoreClientBuilder::new(metadata.clone(), stable_id)
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::EmbeddedWrh)
            .transport(Arc::new(TestTransport::new(segment)))
            .local_memory(storage_config_with_bytes(512))
            .nof_target(target.clone())
            .build(test_future_expiry_ms())
            .expect("managed NoF client should build");
        client
            .register_local_memory()
            .expect("managed NoF memory should register");
        client
    };
    let client_a = build("nof-managed-writer-a", "nof-managed-writer-segment-a");
    let client_b = build("nof-managed-writer-b", "nof-managed-writer-segment-b");

    let deadline = Instant::now() + Duration::from_secs(3);
    let target_owner = loop {
        let owner_a = client_a
            .storage_owner
            .cold_tier_devices
            .nof_targets
            .heartbeat_owner_for("nof-managed-remote-owner-target");
        let owner_b = client_b
            .storage_owner
            .cold_tier_devices
            .nof_targets
            .heartbeat_owner_for("nof-managed-remote-owner-target");
        if let (Some(owner_a), Some(owner_b)) = (&owner_a, &owner_b) {
            if owner_a == owner_b {
                break owner_a.clone();
            }
        }
        assert!(Instant::now() < deadline, "managed owner views did not converge");
        sleep(Duration::from_millis(25));
    };
    let (writer, non_writer) = if target_owner == *client_a.runtime_id() {
        (&client_b, &client_a)
    } else {
        assert_eq!(target_owner, *client_b.runtime_id());
        (&client_a, &client_b)
    };

    writer
        .put("managed-nof-remote-owner-key", &[43u8; 150])
        .expect("non-target-owner should publish a managed backing");
    let route = writer
        .query_route("managed-nof-remote-owner-key")
        .unwrap()
        .unwrap();
    let writer_replica = route
        .replicas
        .iter()
        .find(|replica| replica.owner == *writer.runtime_id())
        .cloned()
        .unwrap();
    let mut reassigned = route.clone();
    reassigned.version = route.version.next();
    reassigned.replicas.push(ReplicaRoute {
        owner: non_writer.runtime_id().clone(),
        segment_name: SegmentName::new("nof-managed-stale-writer-segment"),
        offset: None,
        segment_offset: 0,
        length: writer_replica.length,
        checksum: writer_replica.checksum,
        tier: mooncake_store_core::ReplicaTier::Dram,
        priority: writer_replica.priority.saturating_add(1),
    });
    assert!(
        writer
            .cas_route(
                "managed-nof-remote-owner-key",
                Some(route.version),
                Some(&reassigned),
            )
            .unwrap()
            .applied
    );
    non_writer.storage_owner.pending_offloads.push(
        reassigned.key.clone(),
        reassigned.version,
        Some(writer_replica.length),
    );
    assert_eq!(
        non_writer
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("non-writer must discard its stale offload entry"),
        0
    );
    assert_eq!(
        writer
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("hot replica owner should materialize the managed payload"),
        1
    );
    assert_eq!(backend.writes.load(Ordering::Relaxed), 1);
}

#[test]
fn concurrent_managed_nof_recovery_keeps_every_target_for_the_same_object() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(metadata.clone(), "nof-recovery-race-runtime")
        .state(ClientLifecycleState::Active)
        .transport(Arc::new(TestTransport::new("nof-recovery-race-segment")))
        .local_memory(storage_config_with_bytes(512))
        .build(test_future_expiry_ms())
        .expect("recovery test client should build");
    client
        .register_local_memory()
        .expect("recovery test memory should register");
    let directory = client.storage_owner.route_ops.directory().clone();
    let observer = client.storage_owner.route_ops.observer().clone();
    let workers = 8usize;
    let barrier = Arc::new(std::sync::Barrier::new(workers));
    let handles = (0..workers)
        .map(|index| {
            let metadata = metadata.clone();
            let directory = directory.clone();
            let observer = observer.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let target_id = format!("nof-recovery-target-{index}");
                let recovered = RecoveredColdObject {
                    manifest: super::ColdObjectManifest {
                        key: ObjectKey::new("nof-recovery-race-key"),
                        namespace: None,
                        logical_key: None,
                        canonical_key: None,
                        sharing_scope: None,
                        qos_tier: None,
                        route_version: RouteVersion(1),
                        cold_tier_id: target_id.clone(),
                        object_locator: format!("locator-{index}"),
                        length: 128,
                        checksum: Some(71),
                    },
                    path: std::path::PathBuf::from(format!("nof://{target_id}/{index}")),
                    metadata: super::ColdPayloadMetadata {
                        length: 128,
                        checksum: Some(71),
                    },
                };
                barrier.wait();
                super::cold_tier_storage_backend::try_register_recovered_cold_object(
                    metadata.as_ref(),
                    directory.as_ref(),
                    &observer,
                    &recovered,
                    super::cold_tier_storage_backend::RecoveredBackingKind::Nof,
                )
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle
            .join()
            .expect("recovery worker should not panic")
            .expect("concurrent recovery should reconcile its target");
    }

    let route = directory
        .get_object_route(&observer, &ObjectKey::new("nof-recovery-race-key"))
        .unwrap()
        .unwrap();
    let backing = route.nof_backing.unwrap();
    let targets = std::iter::once(backing.target_id)
        .chain(backing.replicas.into_iter().map(|replica| replica.target_id))
        .collect::<BTreeSet<_>>();
    assert_eq!(targets.len(), workers);
}

#[test]
fn managed_nof_resolved_read_keeps_selected_target() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakeManagedNof::default());
    let target = NofTargetConfig::new(
        "nof-managed-selected-b",
        NofBackend::new(backend).expect("managed NoF backend should build"),
    )
    .expect("managed NoF target should build");
    let client = StoreClientBuilder::new(metadata, "nof-managed-selected-reader")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new("nof-managed-selected-segment")))
        .local_memory(storage_config_with_bytes(512))
        .nof_target(target)
        .build(test_future_expiry_ms())
        .expect("managed NoF reader should build");
    client
        .register_local_memory()
        .expect("managed NoF reader memory should register");

    let primary_locator = NofManagedLocator::new(b"primary".to_vec())
        .expect("primary locator")
        .to_hex();
    let selected_locator = NofManagedLocator::new(b"selected".to_vec())
        .expect("selected locator")
        .to_hex();
    let mut route = ObjectRoute {
        key: ObjectKey::new("managed-nof-selected-key"),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: None,
        version: RouteVersion(1),
        state: mooncake_store_core::RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: None,
        nof_backing: Some(mooncake_store_core::NofBackingRoute {
            owner: ClientRuntimeId::new("nof-managed-primary-owner", ClientEpoch(1)),
            target_id: "nof-managed-selected-a".to_string(),
            object_locator: primary_locator,
            length: 16,
            checksum: Some(7),
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: vec![mooncake_store_core::NofBackingReplica {
                owner: client.runtime_id().clone(),
                target_id: "nof-managed-selected-b".to_string(),
                object_locator: selected_locator.clone(),
            }],
        }),
    };
    mooncake_store_core::apply_route_identity(
        &mut route,
        &mooncake_store_core::LogicalObjectId::new(
            mooncake_store_core::NamespaceScope::default(),
            "managed-nof-selected-key",
        ),
    );
    let mut readable = BTreeSet::new();
    readable.insert(client.runtime_id().clone());

    let resolved = client
        .resolve_cold_backing_read(
            route,
            "default",
            "managed-nof-selected-key",
            &readable,
        )
        .expect("managed NoF route should resolve through the local NoF target");
    let cold = super::cold_tier::resolved_cold_backing(&resolved)
        .expect("resolved NoF cold backing should be retained");

    assert_eq!(cold.cold_tier_id, "nof-managed-selected-b");
    assert_eq!(cold.object_locator, selected_locator);
}


#[test]
fn managed_nof_batch_write_keeps_original_indices_after_validation_error() {
    let backend = Arc::new(FakeManagedNof::default());
    let storage = NofManagedStorageBackend::new(
        NofBackend::new(backend.clone()).expect("managed NoF backend should build"),
    );
    let owner = ClientRuntimeId::new("managed-batch-owner", ClientEpoch(1));
    let invalid_locator = NofManagedLocator::new(b"invalid".to_vec())
        .expect("locator")
        .to_hex();
    let valid_locator = NofManagedLocator::new(b"valid".to_vec())
        .expect("locator")
        .to_hex();
    let invalid_backing = mooncake_store_core::ColdBackingRoute {
        owner: owner.clone(),
        cold_tier_id: "nof-managed-a".to_string(),
        object_locator: invalid_locator,
        length: 8,
        checksum: None,
        state: mooncake_store_core::ColdBackingState::PendingOffload,
        replicas: Vec::new(),
    };
    let valid_backing = mooncake_store_core::ColdBackingRoute {
        owner,
        cold_tier_id: "nof-managed-a".to_string(),
        object_locator: valid_locator.clone(),
        length: 3,
        checksum: None,
        state: mooncake_store_core::ColdBackingState::PendingOffload,
        replicas: Vec::new(),
    };
    let invalid_payload = [1u8; 3];
    let valid_payload = [7u8; 3];
    let writes = [
        ColdObjectWrite {
            route: None,
            cold_backing: &invalid_backing,
            payload: &invalid_payload,
        },
        ColdObjectWrite {
            route: None,
            cold_backing: &valid_backing,
            payload: &valid_payload,
        },
    ];

    let results = storage.put_objects_batch_profiled(&writes, &mut |_, _| {});

    assert!(results[0].is_err());
    let materialized = results[1]
        .as_ref()
        .expect("second write should succeed after first validation error");
    assert_eq!(materialized.object_locator, valid_locator);
    assert_eq!(
        materialized.state,
        mooncake_store_core::ColdBackingState::Materialized
    );
    let stored = backend.records.lock();
    assert_eq!(stored.len(), 1);
    let locator = NofManagedLocator::from_hex(&valid_locator).expect("valid locator");
    assert_eq!(stored.get(&locator).map(Vec::as_slice), Some(&valid_payload[..]));
}

#[test]
fn physical_nof_requires_the_configured_replica_count_before_offload() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakePhysicalNof::default());
    let target = NofTargetConfig::new(
        "nof-only-copy",
        NofBackend::new(backend.clone()).expect("NoF backend should build"),
    )
    .expect("NoF target should build");
    let client = StoreClientBuilder::new(metadata, "nof-redundancy-shortage")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new("nof-redundancy-shortage-segment")))
        .local_memory(storage_config_with_bytes(512))
        .nof_target(target)
        .nof_replica_count(2)
        .build(test_future_expiry_ms())
        .expect("NoF client should build");
    client
        .register_local_memory()
        .expect("local memory should register");

    client
        .put("nof-redundancy-key", &[23u8; 150])
        .expect("hot write should succeed");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("offload attempt should retain the hot copy"),
        0
    );
    assert!(backend.records.lock().is_empty());
    assert!(!client
        .query_route("nof-redundancy-key")
        .expect("route query should succeed")
        .expect("hot route should remain")
        .replicas
        .is_empty());
}

#[test]
fn physical_nof_retries_a_partial_replica_write() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let (client, primary, replica) = physical_nof_client("nof-partial-retry");
    replica.fail_next_write.store(true, Ordering::Relaxed);
    client
        .put("nof-partial-retry-key", &[29u8; 150])
        .expect("hot write should succeed");

    assert!(client
        .storage_owner
        .materialize_pending_offloads_bounded(32)
        .is_err());
    assert!(!primary.records.lock().is_empty());
    assert!(replica.records.lock().is_empty());
    sleep(Duration::from_millis(125));
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("NoF retry should complete the missing replica"),
        1
    );
    assert!(!primary.records.lock().is_empty());
    assert!(!replica.records.lock().is_empty());
}

#[test]
fn unavailable_physical_nof_target_is_pruned_after_replica_restore() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let (client, primary, _replica) = physical_nof_client("nof-physical-health");
    let payload = [11u8; 150];
    client
        .put("physical-nof-health-key", &payload)
        .expect("physical NoF put should succeed");
    client
        .storage_owner
        .materialize_pending_offloads_bounded(32)
        .expect("physical NoF offload should run");
    assert!(!primary.records.lock().is_empty());

    primary.available.store(false, Ordering::Relaxed);
    let health_deadline = Instant::now() + Duration::from_secs(5);
    while client
        .storage_owner
        .cold_tier_devices
        .runtime_backend_available("nof-physical-a")
    {
        assert!(
            Instant::now() < health_deadline,
            "NoF owner heartbeat did not fence the failed target"
        );
        sleep(Duration::from_millis(50));
    }
    force_cold_only_route(&client, "physical-nof-health-key");
    assert_eq!(
        client
            .get("physical-nof-health-key")
            .expect("healthy NoF replica should restore"),
        payload
    );
}

#[test]
fn managed_nof_graceful_handoff_recovers_from_snapshot() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakeManagedNof::default());
    let target = NofTargetConfig::new(
        "nof-handoff-target",
        NofBackend::new(backend.clone()).expect("handoff NoF backend should build"),
    )
    .expect("handoff NoF target should build");
    let build = |stable_id: &str, segment: &str| {
        let client = StoreClientBuilder::new(metadata.clone(), stable_id)
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::EmbeddedWrh)
            .transport(Arc::new(TestTransport::new(segment)))
            .local_memory(storage_config_with_bytes(512))
            .nof_target(target.clone())
            .build(test_future_expiry_ms())
            .expect("handoff client should build");
        client
            .register_local_memory()
            .expect("handoff client memory should register");
        client
    };
    let client_a = build("nof-handoff-a", "nof-handoff-segment-a");
    let client_b = build("nof-handoff-b", "nof-handoff-segment-b");

    let owner_deadline = Instant::now() + Duration::from_secs(3);
    let initial_owner = loop {
        let owner_a = client_a
            .storage_owner
            .cold_tier_devices
            .nof_targets
            .heartbeat_owner_for("nof-handoff-target");
        let owner_b = client_b
            .storage_owner
            .cold_tier_devices
            .nof_targets
            .heartbeat_owner_for("nof-handoff-target");
        if let (Some(owner_a), Some(owner_b)) = (&owner_a, &owner_b) {
            if owner_a == owner_b {
                break owner_a.clone();
            }
        }
        assert!(
            Instant::now() < owner_deadline,
            "NoF target owner views did not converge: a={owner_a:?} b={owner_b:?}"
        );
        sleep(Duration::from_millis(25));
    };
    let writer = if initial_owner == *client_a.runtime_id() {
        &client_b
    } else {
        assert_eq!(initial_owner, *client_b.runtime_id());
        &client_a
    };
    writer
        .put("nof-handoff-key", &[13u8; 150])
        .expect("handoff NoF put should succeed");
    writer
        .storage_owner
        .materialize_pending_offloads_bounded(32)
        .expect("non-owner writer should run the NoF offload");
    force_cold_only_route(writer, "nof-handoff-key");
    let recoveries_before = backend.recoveries.load(Ordering::Relaxed);
    let scans_before = backend.manifest_scans.load(Ordering::Relaxed);
    let (departed, survivor) = if initial_owner == *client_a.runtime_id() {
        (client_a, client_b)
    } else {
        assert_eq!(initial_owner, *client_b.runtime_id());
        (client_b, client_a)
    };
    drop(departed);

    let handoff_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let current = survivor
            .storage_owner
            .cold_tier_devices
            .nof_targets
            .heartbeat_owner_for("nof-handoff-target");
        if current.as_ref() == Some(survivor.runtime_id())
            && backend.recoveries.load(Ordering::Relaxed) > recoveries_before
        {
            break;
        }
        assert!(
            Instant::now() < handoff_deadline,
            "NoF target owner was not handed off after graceful exit: {current:?}"
        );
        sleep(Duration::from_millis(25));
    }
    assert!(
        backend.recoveries.load(Ordering::Relaxed) > recoveries_before,
        "successor should recover allocator state from the handoff snapshot"
    );
    assert_eq!(backend.manifest_scans.load(Ordering::Relaxed), scans_before);
    assert_eq!(
        survivor
            .get("nof-handoff-key")
            .expect("any client should restore the object after heartbeat handoff"),
        [13u8; 150]
    );

    survivor
        .remove("nof-handoff-key", true)
        .expect("any client should delete provider data after heartbeat handoff");
    assert!(
        wait_for_nof_reclaims(&survivor, || backend.records.lock().is_empty()),
        "NoF target data remained after owner handoff reclaim"
    );
}

#[test]
fn managed_nof_without_physical_recovery_uses_existing_routes() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakeManagedNof::default());
    backend.disable_recovery.store(true, Ordering::Relaxed);
    let target = NofTargetConfig::new(
        "nof-route-recovery-target",
        NofBackend::new(backend).expect("managed NoF backend should build without scan recovery"),
    )
    .expect("managed NoF target should build");
    let client = StoreClientBuilder::new(metadata, "nof-route-recovery-client")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(Arc::new(TestTransport::new(
            "nof-route-recovery-segment",
        )))
        .local_memory(storage_config_with_bytes(512))
        .nof_target(target)
        .build(test_future_expiry_ms())
        .expect("managed NoF client should build");
    client
        .register_local_memory()
        .expect("managed NoF client memory should register");

    let manager = &client.storage_owner.cold_tier_devices.nof_targets;
    let deadline = Instant::now() + Duration::from_secs(3);
    while !manager.available_for_io("nof-route-recovery-target") {
        assert!(
            Instant::now() < deadline,
            "managed target without physical recovery did not become available"
        );
        sleep(Duration::from_millis(25));
    }
}

#[test]
fn logical_object_nof_runtime_keeps_provider_owned_replication() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let provider = Arc::new(FakeObjectNof::default());
    let target = NofTargetConfig::new(
        "nof-object-provider",
        NofBackend::new(provider.clone()).expect("object NoF backend should build"),
    )
    .expect("object NoF target should build");
    let client = StoreClientBuilder::new(metadata, "nof-object-runtime")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(Arc::new(TestTransport::new("nof-object-runtime-segment")))
        .local_memory(storage_config_with_bytes(128))
        .nof_target(target)
        .nof_replica_count(2)
        .build(test_future_expiry_ms())
        .expect("object NoF client should build");
    client
        .register_local_memory()
        .expect("object NoF local memory should register");

    let payload = b"provider-owned-object-v1";
    client
        .put("object-nof-key", payload)
        .expect("object NoF put should succeed");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("object NoF offload should run"),
        1
    );
    let route = client
        .query_route("object-nof-key")
        .expect("object NoF route query should succeed")
        .expect("object NoF route should exist");
    assert!(route.cold_backing.is_none());
    assert!(route.nof_backing.is_none());
    assert_eq!(provider.objects.lock().len(), 1);

    let updated_payload = b"provider-owned-object-v2";
    let updated_route = client
        .put("object-nof-key", updated_payload)
        .expect("object NoF overwrite should succeed");
    assert!(
        updated_route.version > route.version,
        "object NoF overwrite should advance the route version"
    );
    assert!(updated_route.cold_backing.is_none());
    assert!(updated_route.nof_backing.is_none());
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("object NoF overwrite offload should run"),
        1
    );
    let updated_route_from_metadata = client
        .query_route("object-nof-key")
        .expect("updated object NoF route query should succeed")
        .expect("updated object NoF route should exist");
    assert!(updated_route_from_metadata.cold_backing.is_none());
    assert!(updated_route_from_metadata.nof_backing.is_none());
    assert_eq!(updated_route_from_metadata.version, updated_route.version);
    assert_eq!(provider.objects.lock().len(), 1);
    assert_eq!(
        provider.objects.lock().values().next().map(Vec::as_slice),
        Some(updated_payload.as_slice())
    );
    assert_eq!(
        client.get("object-nof-key").expect("updated object NoF read should succeed"),
        updated_payload
    );
    assert!(wait_for_nof_reclaims(&client, || provider.objects.lock().len() == 1));

    force_cold_only_route(&client, "object-nof-key");
    assert_eq!(
        client.get("object-nof-key").expect("object NoF restore should succeed"),
        updated_payload
    );
    provider.fail_next_delete.store(true, Ordering::Relaxed);
    assert!(matches!(
        client.remove("object-nof-key", false),
        Err(StoreError::Transport(_))
    ));
    assert_eq!(
        client
            .route_ops()
            .load_route(&client.scoped_key(client.default_tenant(), "object-nof-key"))
            .expect("deleting NoF route lookup should succeed")
            .expect("failed provider delete must retain the logical route")
            .state,
        mooncake_store_core::RouteState::Deleting
    );
    assert!(matches!(
        client.put("object-nof-key", b"must-not-replace-deleting-object"),
        Err(StoreError::Conflict(_))
    ));
    client
        .remove("object-nof-key", true)
        .expect("object NoF delete retry should succeed");
    assert!(wait_for_nof_reclaims(&client, || provider.objects.lock().is_empty()));
    assert!(provider.objects.lock().is_empty());
}
