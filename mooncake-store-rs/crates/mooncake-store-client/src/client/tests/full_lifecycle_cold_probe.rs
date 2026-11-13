use super::*;
use crate::control_plane::ColdTierControlService;

pub(super) fn cold_probe_client(id: &str) -> StoreClient {
    let client = StoreClientBuilder::new(Arc::new(InMemoryMetadataBackend::new()), id)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(Arc::new(TestTransport::new(&format!("{id}-segment"))))
        .local_memory(storage_config_with_bytes(512))
        .cold_tier_offload_mode(ColdTierOffloadMode::EvictTriggered)
        .cold_tier_target(ColdTierTargetConfig::directory(
            id,
            ColdTierKind::Ssd,
            cold_tier_test_root(id),
        ))
        .build(test_future_expiry_ms())
        .expect("client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    client
}

pub(super) fn cold_probe_owner(
    client: &StoreClient,
    directory: Arc<dyn RouteDirectory>,
) -> Arc<StorageOwnerState> {
    let owner = test_storage_owner_state_with_route_directory(
        client,
        directory,
        ColdTierOffloadPriorityConfig::default(),
    );
    for device in client
        .metadata
        .list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())
        .expect("devices")
    {
        let lookup = mooncake_store_core::ColdBackingRoute {
            owner: client.runtime_id().clone(),
            cold_tier_id: device.device_id.clone(),
            object_locator: String::new(),
            length: 0,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: vec![],
        };
        let backend = client
            .storage_owner
            .cold_tier_devices
            .backend_for(&lookup)
            .expect("backend");
        owner
            .cold_tier_devices
            .resolver
            .lock()
            .insert_backend(device.device_id.clone(), backend);
        owner.cold_tier_devices.upsert(device);
    }
    owner
}

pub(super) fn cold_probe_prepare(
    owner: &StorageOwnerState,
    hot: &ObjectRoute,
) -> Vec<PendingOffloadMaterialization> {
    owner.track_route(hot);
    let pending = cold_tier::publish_pending_cold_backing_for_eviction(owner, hot)
        .expect("publish pending")
        .expect("pending route");
    let entry = owner
        .pending_offloads
        .claim(&pending.key, pending.version)
        .expect("claim offload");
    let prepared = owner
        .prepare_pending_offload_entries(vec![entry])
        .expect("prepare");
    assert_eq!(prepared.len(), 1);
    prepared
}

#[test]
fn full_lifecycle_cold_offload_applied_reply_loss_preserves_payload() {
    let _cold_guard = enable_cold_tier_for_test();
    let client = cold_probe_client("full-lifecycle-cold-reply-loss");
    let payload = b"cold-offload-reply-loss-payload";
    let hot = client.put("cold-offload-reply-loss", payload).expect("put");
    assert!(hot.cold_backing.is_none());
    let owner = cold_probe_owner(
        &client,
        Arc::new(ApplyBatchThenFailRouteDirectory {
            inner: client.route_directory.clone(),
            fail_once: AtomicBool::new(true),
        }),
    );
    let prepared = cold_probe_prepare(&owner, &hot);
    let mut first_error = None;
    let result = owner.materialize_prepared_pending_offload_batch(&prepared, &mut first_error);
    assert_eq!(result.expect("reply loss should be confirmed"), 1);
    assert!(first_error.is_none());
    let current = client
        .query_route("cold-offload-reply-loss")
        .expect("query")
        .expect("route");
    let materialized = current
        .cold_backing
        .as_ref()
        .expect("published cold backing");
    assert_eq!(
        materialized.state,
        mooncake_store_core::ColdBackingState::Materialized
    );
    let backend = owner
        .cold_tier_devices
        .backend_for(materialized)
        .expect("backend");
    let mut restored = vec![0; payload.len()];
    assert_eq!(
        backend
            .get_object_into(materialized, &mut restored)
            .expect("cold read"),
        Some(payload.len()),
        "applied publication must retain its cold payload after losing the CAS reply"
    );
    assert_eq!(restored, payload);
    assert_eq!(
        cold_probe_usage(&owner, materialized),
        (payload.len() as u64, 0)
    );
}

struct ColdOffloadReplyLossDirectory {
    inner: Arc<dyn RouteDirectory>,
    armed: AtomicBool,
    confirmation_fails: AtomicBool,
    apply_batch: bool,
}

impl RouteDirectory for ColdOffloadReplyLossDirectory {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.inner.get_object_route(observer, key)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        if self.armed.load(Ordering::SeqCst) && self.confirmation_fails.load(Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "injected offload confirmation failure".into(),
            ));
        }
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<mooncake_store_core::CasResult>>>
    {
        if !self.armed.load(Ordering::SeqCst) {
            return self
                .inner
                .compare_and_swap_object_routes(observer, requests);
        }
        if self.apply_batch {
            let results = self
                .inner
                .compare_and_swap_object_routes(observer, requests)?;
            assert!(results
                .iter()
                .all(|result| result.as_ref().is_ok_and(|cas| cas.applied)));
        }
        Ok(requests
            .iter()
            .map(|_| {
                Err(StoreError::Transport(
                    "injected per-item offload reply loss".into(),
                ))
            })
            .collect())
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
}

fn cold_probe_usage(
    owner: &StorageOwnerState,
    backing: &mooncake_store_core::ColdBackingRoute,
) -> (u64, u64) {
    let device = owner
        .route_ops
        .metadata()
        .get_cold_tier_device(&backing.cold_tier_id)
        .expect("load cold device")
        .expect("registered cold device");
    (device.used_bytes, device.reserved_bytes)
}

fn cold_probe_payload(
    owner: &StorageOwnerState,
    backing: &mooncake_store_core::ColdBackingRoute,
    payload: &[u8],
) {
    let backend = owner
        .cold_tier_devices
        .backend_for(backing)
        .expect("backend");
    let mut restored = vec![0; payload.len()];
    assert_eq!(
        backend
            .get_object_into(backing, &mut restored)
            .expect("cold read"),
        Some(payload.len())
    );
    assert_eq!(restored, payload);
}

fn cold_probe_retry(
    owner: &StorageOwnerState,
    route: &ObjectRoute,
) -> Result<Vec<PendingOffloadMaterialization>> {
    let entry = owner
        .pending_offloads
        .claim(&route.key, route.version)
        .expect("claim retry");
    owner.prepare_pending_offload_entries(vec![entry])
}

#[test]
fn full_lifecycle_cold_offload_per_item_reply_loss_confirms_accounting() {
    let _cold_guard = enable_cold_tier_for_test();
    let client = cold_probe_client("full-lifecycle-offload-item-loss");
    let payload = b"per-item-reply-loss";
    let hot = client.put("offload-item-loss", payload).expect("put");
    let directory = Arc::new(ColdOffloadReplyLossDirectory {
        inner: client.route_directory.clone(),
        armed: AtomicBool::new(false),
        confirmation_fails: AtomicBool::new(false),
        apply_batch: true,
    });
    let owner = cold_probe_owner(&client, directory.clone());
    let prepared = cold_probe_prepare(&owner, &hot);
    directory.armed.store(true, Ordering::SeqCst);
    let mut error = None;
    assert_eq!(
        owner
            .materialize_prepared_pending_offload_batch(&prepared, &mut error)
            .expect("confirm offload"),
        1
    );
    assert!(error.is_none());
    let route = client
        .query_route("offload-item-loss")
        .expect("query")
        .expect("route");
    let backing = route.cold_backing.as_ref().expect("cold backing");
    cold_probe_payload(&owner, backing, payload);
    assert_eq!(cold_probe_usage(&owner, backing), (payload.len() as u64, 0));
}

#[test]
fn full_lifecycle_cold_offload_unknown_retry_settles_once() {
    let _cold_guard = enable_cold_tier_for_test();
    let client = cold_probe_client("full-lifecycle-offload-unknown-retry");
    let payload = b"unknown-reply-loss";
    let hot = client.put("offload-unknown-retry", payload).expect("put");
    let directory = Arc::new(ColdOffloadReplyLossDirectory {
        inner: client.route_directory.clone(),
        armed: AtomicBool::new(false),
        confirmation_fails: AtomicBool::new(true),
        apply_batch: true,
    });
    let owner = cold_probe_owner(&client, directory.clone());
    let prepared = cold_probe_prepare(&owner, &hot);
    directory.armed.store(true, Ordering::SeqCst);
    let mut error = None;
    assert_eq!(
        owner
            .materialize_prepared_pending_offload_batch(&prepared, &mut error)
            .expect("retain unknown offload"),
        0
    );
    assert!(error.is_some());
    let route = client
        .query_route("offload-unknown-retry")
        .expect("query")
        .expect("route");
    let backing = route.cold_backing.as_ref().expect("published backing");
    cold_probe_payload(&owner, backing, payload);
    assert_eq!(cold_probe_usage(&owner, backing), (0, payload.len() as u64));
    assert!(cold_probe_retry(&owner, &route).is_err());
    cold_probe_payload(&owner, backing, payload);
    assert_eq!(cold_probe_usage(&owner, backing), (0, payload.len() as u64));
    directory.confirmation_fails.store(false, Ordering::SeqCst);
    assert!(cold_probe_retry(&owner, &route)
        .expect("confirm retained offload")
        .is_empty());
    cold_probe_payload(&owner, backing, payload);
    assert_eq!(cold_probe_usage(&owner, backing), (payload.len() as u64, 0));
    assert!(cold_probe_retry(&owner, &route)
        .expect("completed offload retry")
        .is_empty());
    assert_eq!(cold_probe_usage(&owner, backing), (payload.len() as u64, 0));
}

#[test]
fn full_lifecycle_cold_offload_unknown_conflict_cleans_payload_and_reservation() {
    let _cold_guard = enable_cold_tier_for_test();
    let client = cold_probe_client("full-lifecycle-offload-unknown-conflict");
    let payload = b"unknown-conflict-payload";
    let hot = client
        .put("offload-unknown-conflict", payload)
        .expect("put");
    let directory = Arc::new(ColdOffloadReplyLossDirectory {
        inner: client.route_directory.clone(),
        armed: AtomicBool::new(false),
        confirmation_fails: AtomicBool::new(true),
        apply_batch: false,
    });
    let owner = cold_probe_owner(&client, directory.clone());
    let prepared = cold_probe_prepare(&owner, &hot);
    directory.armed.store(true, Ordering::SeqCst);
    let mut error = None;
    assert_eq!(
        owner
            .materialize_prepared_pending_offload_batch(&prepared, &mut error)
            .expect("retain unknown offload"),
        0
    );
    assert!(error.is_some());
    let mut backing = prepared[0].cold_backing.clone();
    backing.state = mooncake_store_core::ColdBackingState::Materialized;
    cold_probe_payload(&owner, &backing, payload);
    assert_eq!(
        cold_probe_usage(&owner, &backing),
        (0, payload.len() as u64)
    );
    let mut next = prepared[0].route.clone();
    next.version = next.version.next();
    next.cold_backing = None;
    assert!(
        client
            .storage_owner
            .route_ops
            .compare_and_swap_route(&next.key, Some(prepared[0].route.version), Some(&next))
            .expect("supersede pending route")
            .applied
    );
    directory.confirmation_fails.store(false, Ordering::SeqCst);
    assert!(cold_probe_retry(&owner, &next)
        .expect("confirm route conflict")
        .is_empty());
    let backend = owner
        .cold_tier_devices
        .backend_for(&backing)
        .expect("backend");
    let mut restored = vec![0; payload.len()];
    assert_eq!(
        backend
            .get_object_into(&backing, &mut restored)
            .expect("cold read after cleanup"),
        None
    );
    assert_eq!(cold_probe_usage(&owner, &backing), (0, 0));
    assert!(client
        .query_route("offload-unknown-conflict")
        .expect("query")
        .expect("route")
        .cold_backing
        .is_none());
}

#[test]
fn full_lifecycle_cold_restore_applied_reply_loss_retains_published_allocation() {
    let _cold_guard = enable_cold_tier_for_test();
    let mut client = cold_probe_client("full-lifecycle-cold-restore-reply-loss");
    let payload = b"restore-payload!";
    let hot = client
        .put("restore-reply-loss", payload)
        .expect("initial put");
    let materializer = cold_probe_owner(&client, client.route_directory.clone());
    let prepared = cold_probe_prepare(&materializer, &hot);
    let mut error = None;
    assert_eq!(
        materializer
            .materialize_prepared_pending_offload_batch(&prepared, &mut error)
            .expect("offload"),
        1
    );
    drop(prepared);
    force_cold_only_route(&client, "restore-reply-loss");
    let owner = cold_probe_owner(
        &client,
        Arc::new(ApplyThenFailRouteDirectory {
            inner: client.route_directory.clone(),
            fail_once: AtomicBool::new(true),
            confirmation_fails: AtomicBool::new(false),
        }),
    );
    let adapter = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: owner.clone(),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
        allocation_lifecycle: AllocationLifecycleConfig::default(),
    };
    let response = adapter.read_from_cold(
        &owner.metadata.route_namespace(),
        &client.runtime_id().stable_id.0,
        "default",
        "restore-reply-loss",
        "",
        "",
    );
    let reply = response
        .result
        .expect("cold read should return staging payload");
    response
        .deferred_promote
        .expect("cold read should defer real promotion")();
    adapter
        .ack_cold_read_complete(&[(SegmentName::new(&reply.segment_name), reply.segment_offset)]);
    let published = client
        .query_route("restore-reply-loss")
        .expect("query promoted")
        .expect("promoted route");
    assert_eq!(
        published.replicas.len(),
        1,
        "CAS must actually publish before reply loss"
    );
    client.storage_owner = owner;
    let next = client
        .put("after-restore-reply-loss", b"overwrite-data!!")
        .expect("public put after restore");
    assert_eq!(
        client
            .get("restore-reply-loss")
            .expect("published restore payload should remain readable"),
        payload
    );
    let promoted = &published.replicas[0];
    let fresh = &next.replicas[0];
    assert!(
        promoted.segment_name != fresh.segment_name
            || promoted.segment_offset != fresh.segment_offset,
        "public put must not reuse an allocation still published by the restore CAS"
    );
}
