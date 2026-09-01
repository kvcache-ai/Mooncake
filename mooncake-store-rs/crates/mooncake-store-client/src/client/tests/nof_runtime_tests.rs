use crate::{
    NofBackend, NofBacking, NofHealth, NofObject, NofObjectDelete, NofObjectLimits, NofObjectMetadata,
    NofObjectQuery, NofObjectRead, NofObjectShardWrite, NofObjectState, NofObjectWrite,
    NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalLimits, NofPhysicalRead,
    NofPhysicalReadRequest, NofPhysicalWrite, NofStorageHealth, NofTargetConfig, OpaquePhysicalKey,
};

struct FakePhysicalNof {
    records: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    available: AtomicBool,
}

impl Default for FakePhysicalNof {
    fn default() -> Self {
        Self {
            records: Mutex::new(BTreeMap::new()),
            available: AtomicBool::new(true),
        }
    }
}

impl NofPhysicalWrite for FakePhysicalNof {
    fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
        let mut records = self.records.lock();
        requests
            .iter()
            .map(|(key, value)| {
                records.insert(key.as_bytes().to_vec(), value.to_vec());
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
            .map(|request| Ok(records.get(request.key.as_bytes()).cloned()))
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
    fn physical_limits(&self) -> Option<NofPhysicalLimits> {
        Some(NofPhysicalLimits {
            max_value_size: 64,
            max_batch_items: 32,
            max_batch_bytes: 1024,
        })
    }

    fn physical_write(&self) -> Option<&dyn NofPhysicalWrite> {
        Some(self)
    }

    fn physical_read(&self) -> Option<&dyn NofPhysicalRead> {
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
struct FakeObjectNof {
    objects: Mutex<BTreeMap<(NamespaceScope, String), Vec<u8>>>,
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
    ) -> Result<NofObjectState<NofObject>> {
        Ok(match self.objects.lock().get(&(namespace.clone(), key.to_string())) {
            Some(value) => NofObjectState::Found(NofObject {
                metadata: NofObjectMetadata {
                    length: value.len() as u64,
                    total_shards: 1,
                },
                value: value.clone(),
            }),
            None => NofObjectState::Missing,
        })
    }
}

impl NofObjectQuery for FakeObjectNof {
    fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObjectMetadata>> {
        Ok(match self.objects.lock().get(&(namespace.clone(), key.to_string())) {
            Some(value) => NofObjectState::Found(NofObjectMetadata {
                length: value.len() as u64,
                total_shards: 1,
            }),
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

fn wait_for_materialized_nof_backing(client: &StoreClient, key: &str) -> ObjectRoute {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let route = client
            .query_route(key)
            .expect("NoF route query should succeed")
            .expect("NoF route should exist");
        if route.nof_backing.as_ref().is_some_and(|backing| {
            backing.state == mooncake_store_core::NofBackingState::Materialized
        }) {
            return route;
        }
        assert!(Instant::now() < deadline, "NoF backing did not materialize: {route:?}");
        sleep(Duration::from_millis(10));
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
fn physical_nof_runtime_reuses_offload_restore_replica_and_delete_flow() {
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
    let route = wait_for_materialized_nof_backing(&client, "physical-nof-key");
    let backing = route.nof_backing.as_ref().expect("NoF backing should exist");
    assert!(route.cold_backing.is_none());
    assert_eq!(backing.replicas.len(), 1);
    assert!(!primary.records.lock().is_empty());
    assert!(!replica.records.lock().is_empty());

    force_cold_only_route(&client, "physical-nof-key");
    assert_eq!(
        client.get("physical-nof-key").expect("physical NoF restore should succeed"),
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
fn unpublished_physical_nof_cleanup_removes_every_replica_target() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let (client, primary, replica) = physical_nof_client("nof-physical-rollback");
    client
        .put("physical-nof-rollback-key", &[7u8; 150])
        .expect("physical NoF put should succeed");
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("physical NoF offload should run"),
        1
    );
    let route = wait_for_materialized_nof_backing(&client, "physical-nof-rollback-key");
    let backing = crate::client::cold_tier::nof::nof_as_cold(
        route.nof_backing.as_ref().expect("NoF backing should exist"),
    );
    assert!(
        client
            .route_ops()
            .delete_route_with_version_fence(&route)
            .expect("route fence should succeed")
            .applied
    );

    crate::client::cold_tier::remove_unpublished_persistent_backing(
        client.storage_owner.as_ref(),
        &backing,
    );

    assert!(primary.records.lock().is_empty());
    assert!(replica.records.lock().is_empty());
}

#[test]
fn embedded_route_directory_rebuilds_pending_physical_nof_offload() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let (client, primary, replica) = physical_nof_client("nof-physical-recovery");
    client
        .put("physical-nof-recovery-key", &[9u8; 150])
        .expect("physical NoF put should succeed");

    assert_eq!(
        crate::client::cold_tier::rebuild_pending_offload_queue(
            client.storage_owner.as_ref(),
        )
        .expect("NoF pending queue should rebuild from the route directory"),
        1
    );
    assert_eq!(
        client
            .storage_owner
            .materialize_pending_offloads_bounded(32)
            .expect("recovered physical NoF offload should run"),
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
    let route = wait_for_materialized_nof_backing(&client, "physical-nof-health-key");
    assert_eq!(
        route.nof_backing.as_ref().unwrap().target_id,
        "nof-physical-a"
    );

    primary.available.store(false, Ordering::Relaxed);
    sleep(Duration::from_millis(1_100));
    force_cold_only_route(&client, "physical-nof-health-key");
    assert_eq!(
        client
            .get("physical-nof-health-key")
            .expect("healthy NoF replica should restore"),
        payload
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let route = client
            .query_route("physical-nof-health-key")
            .expect("route query should succeed")
            .expect("route should remain active");
        let backing = route.nof_backing.as_ref().expect("NoF backing should remain");
        if backing.target_id == "nof-physical-b"
            && backing
                .replicas
                .iter()
                .all(|replica| replica.target_id != "nof-physical-a")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "unavailable NoF target was not pruned: {backing:?}"
        );
        sleep(Duration::from_millis(10));
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
    let route = wait_for_materialized_nof_backing(&client, "object-nof-key");
    let backing = route.nof_backing.as_ref().expect("NoF backing should exist");
    assert!(route.cold_backing.is_none());
    assert!(backing.replicas.is_empty(), "provider-owned replication must not be duplicated");
    let first_locator = backing.object_locator.clone();

    let updated_payload = b"provider-owned-object-v2";
    client
        .put("object-nof-key", updated_payload)
        .expect("object NoF overwrite should succeed");
    client
        .storage_owner
        .materialize_pending_offloads_bounded(32)
        .expect("object NoF overwrite offload should run");
    let route = wait_for_materialized_nof_backing(&client, "object-nof-key");
    let backing = route.nof_backing.as_ref().expect("NoF backing should exist");
    assert_ne!(backing.object_locator, first_locator);
    assert!(wait_for_nof_reclaims(&client, || provider.objects.lock().len() == 1));

    force_cold_only_route(&client, "object-nof-key");
    assert_eq!(
        client.get("object-nof-key").expect("object NoF restore should succeed"),
        updated_payload
    );
    client
        .remove("object-nof-key", true)
        .expect("object NoF delete should succeed");
    assert!(wait_for_nof_reclaims(&client, || provider.objects.lock().is_empty()));
    assert!(provider.objects.lock().is_empty());
}
