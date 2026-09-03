use crate::{
    NofBackend, NofBacking, NofHealth, NofObjectDelete, NofObjectLimits,
    NofObjectQuery, NofObjectRead, NofObjectShardWrite, NofObjectState, NofObjectWrite,
    NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalQuery, NofPhysicalQueryRequest,
    NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalWrite, NofPhysicalWriteRequest,
    NofStorageHealth, NofTargetConfig,
};

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
fn nof_target_heartbeat_owner_handoff_does_not_gate_data_io() {
    let _cold_tier_env = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let backend = Arc::new(FakePhysicalNof::default());
    let target = NofTargetConfig::new(
        "nof-handoff-target",
        NofBackend::new(backend.clone()).expect("handoff NoF backend should build"),
    )
    .expect("handoff NoF target should build");
    let build = |stable_id: &str, segment: &str| {
        let client = StoreClientBuilder::new(metadata.clone(), stable_id)
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .route_control(RouteControlMode::MetadataOnly)
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
        if current.as_ref() == Some(survivor.runtime_id()) {
            break;
        }
        assert!(
            Instant::now() < handoff_deadline,
            "NoF target owner was not handed off after graceful exit: {current:?}"
        );
        sleep(Duration::from_millis(25));
    }
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

    let updated_payload = b"provider-owned-object-v2";
    let error = client
        .put("object-nof-key", updated_payload)
        .expect_err("object NoF duplicate put must preserve StoreClient semantics");
    assert!(matches!(error, StoreError::Conflict(_)));
    assert_eq!(provider.objects.lock().len(), 1);
    client
        .remove("object-nof-key", false)
        .expect("object NoF remove before reinsert should succeed");
    client
        .put("object-nof-key", updated_payload)
        .expect("object NoF reinsert should succeed");
    client
        .storage_owner
        .materialize_pending_offloads_bounded(32)
        .expect("object NoF reinsert offload should run");
    let route = client
        .query_route("object-nof-key")
        .expect("updated object NoF route query should succeed")
        .expect("updated object NoF route should exist");
    assert!(route.cold_backing.is_none());
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
