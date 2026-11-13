use super::*;

struct ReviewExistingHolderBarrier {
    local: mooncake_store_route::LocalRouteAuthority,
    before: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl mooncake_store_route::RouteAuthorityService for ReviewExistingHolderBarrier {
    fn publication_control(
        &self,
        _: &str,
        _: &ClientStableId,
        ops: &[mooncake_store_core::PublicationControl],
    ) -> mooncake_store_core::Result<
        Vec<mooncake_store_core::Result<mooncake_store_core::PublicationControlReply>>,
    > {
        if ops.iter().any(|op| {
            matches!(
                op,
                mooncake_store_core::PublicationControl::PruneExisting { .. }
            )
        }) {
            let callback = self.before.lock().take();
            if let Some(callback) = callback {
                callback();
            }
        }
        self.local.publication_control(ops)
    }
    fn get_route(
        &self,
        _: &str,
        _: &ClientStableId,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        self.local.get_route(key)
    }
    fn get_route_record(
        &self,
        _: &str,
        _: &ClientStableId,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<mooncake_store_route::RouteAuthorityRecord> {
        self.local.get_route_record(key)
    }
    fn list_routes_by_replica_owner(
        &self,
        _: &str,
        _: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.local.list_routes_by_replica_owner(owner)
    }
    fn compare_and_swap_route(
        &self,
        _: &str,
        _: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        let callback = self.before.lock().take();
        if let Some(callback) = callback {
            eprintln!(
                "REVIEW_BARRIER key={key:?} expected={expected:?} next={next:?} before={:?}",
                self.local.get_route(key)
            );
            callback();
            eprintln!("REVIEW_BARRIER_AFTER={:?}", self.local.get_route(key));
        }
        self.local.compare_and_swap_route(key, expected, next)
    }
    fn replace_route(
        &self,
        _: &str,
        _: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<()> {
        self.local.replace_route(key, next)
    }
}

#[test]
fn review_existing_holder_prune_must_not_shadow_concurrent_new_value() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("review-old-z-segment"));
    let z = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "review-old-z")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .live_client_sync_interval(Duration::ZERO)
            .transport(transport.clone())
            .local_memory(storage_config_with_bytes(256))
            .build(test_future_expiry_ms())
            .unwrap(),
    );
    let a = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "review-old-a")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .live_client_sync_interval(Duration::ZERO)
            .transport(Arc::new(transport.peer("review-old-a-segment")))
            .local_memory(storage_config_with_bytes(256))
            .build(test_future_expiry_ms())
            .unwrap(),
    );
    z.register_local_memory().unwrap();
    a.register_local_memory().unwrap();
    wait_for_membership_convergence(&[&z, &a]);
    let policy = ReplicationPolicy::new()
        .replica_count(2)
        .prefer_local(false)
        .preferred_storage_owners([z.runtime_id().storage_key(), a.runtime_id().storage_key()]);
    let original = z
        .put_with_policy("race", b"AAAAAAAAAAAAAAAA", &policy)
        .unwrap();
    assert_eq!(original.version, RouteVersion(1));
    assert_eq!(original.replicas.len(), 2);
    let mut peers = Vec::new();
    for i in 0..2 {
        let peer = Arc::new(
            StoreClientBuilder::new(metadata.clone(), format!("review-new-{i}"))
                .state(ClientLifecycleState::Active)
                .label("storage", "false")
                .label("route", "true")
                .label("route_weight", "1000000")
                .live_client_sync_interval(Duration::ZERO)
                .routed_writes(
                    PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
                    1,
                )
                .transport(Arc::new(transport.peer(&format!("review-new-{i}-segment"))))
                .local_memory(rw_only_config())
                .build(test_future_expiry_ms())
                .unwrap(),
        );
        peer.register_local_memory().unwrap();
        peers.push(peer);
    }
    wait_for_membership_convergence(&[&z, &a, &peers[0], &peers[1]]);
    for peer in &peers {
        let raw = mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            peer.runtime_id().stable_id.clone(),
        )
        .get_route_record(&original.key)
        .unwrap();
        assert!(raw.route.is_none() && raw.delete_version.is_none());
    }
    let victim = original
        .replicas
        .iter()
        .find(|r| r.owner == *z.runtime_id())
        .unwrap()
        .clone();
    let mut pruned = original.clone();
    pruned.version = original.version.next();
    pruned.replicas.retain(|r| r.owner != *z.runtime_id());
    for (i, r) in pruned.replicas.iter_mut().enumerate() {
        r.priority = i as u16;
    }
    let holder_runtime = victim.publication.as_ref().unwrap().primary.clone();
    let local = mooncake_store_route::LocalRouteAuthority::new(
        metadata.route_namespace(),
        holder_runtime.stable_id.clone(),
    );
    eprintln!("REVIEW_HOLDER={holder_runtime:?}");
    let writer = peers[0].clone();
    let target = z.runtime_id().storage_key();
    local.bind_service(Arc::new(ReviewExistingHolderBarrier {
        local: mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            holder_runtime.stable_id.clone(),
        ),
        before: Mutex::new(Some(Box::new(move || {
            let fresh = writer
                .batch_put(&[PutRequest::new("race", b"CCCCCCCCCCCCCCCC").replication(
                    ReplicationPolicy::new()
                        .prefer_local(false)
                        .preferred_storage_owners([target]),
                )])
                .unwrap()
                .remove(0);
            let mut next = fresh.clone();
            next.version = RouteVersion(2);
            if fresh.version != next.version {
                assert!(
                    writer
                        .cas_route("race", Some(fresh.version), Some(&next))
                        .unwrap()
                        .applied
                );
            }
            writer.route_directory.flush_pending_route_mirrors();
            assert_eq!(writer.get("race").unwrap(), b"CCCCCCCCCCCCCCCC");
            eprintln!("REVIEW_NEW_VALUE_COMMITTED version={:?}", next.version);
        }))),
    }));
    let receipt = z
        .route_ops()
        .evict_existing_route_replica(&original, &victim, Some(&pruned))
        .unwrap();
    eprintln!("REVIEW_OLD_HOLDER_RECEIPT={receipt:?}");
    let mut holder_lease = if holder_runtime == *z.runtime_id() {
        z.lease()
    } else {
        a.lease()
    };
    holder_lease
        .endpoints
        .labels
        .insert("route_weight".into(), "1000000000000000".into());
    metadata.upsert_client_lease(&holder_lease).unwrap();
    wait_for_membership_convergence(&[&z, &a, &peers[0], &peers[1]]);
    for client in [&z, &a, &peers[0], &peers[1]] {
        client.readable_runtime_set(true).unwrap();
    }
    let observed = peers[1].get("race").unwrap();
    eprintln!("REVIEW_GET_AFTER_HOLDER_REJOIN={observed:?}");
    assert_eq!(
        observed, b"CCCCCCCCCCCCCCCC",
        "existing-holder pruning must preserve the concurrently committed new payload"
    );
}

#[test]
fn publication_fence_batch_revoke_preserves_healthy_item_after_primary_failure() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let owner = StoreClientBuilder::new(metadata, "publication-batch-revoke-owner")
        .state(ClientLifecycleState::Active)
        .transport(Arc::new(TestTransport::new(
            "publication-batch-revoke-segment",
        )))
        .local_memory(storage_config_with_bytes(128))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let route = owner.put("healthy", b"healthy-batch-payload").unwrap();
    let replica = &route.replicas[0];
    let good = mooncake_store_core::PublicationRevokeRequest {
        attempt: replica.publication.clone().unwrap(),
        allocation: mooncake_store_core::AllocationRouteIdentity {
            owner: replica.owner.clone(),
            segment_name: replica.segment_name.clone(),
            offset_bytes: replica.segment_offset,
            length_bytes: replica.length,
            allocation_id: replica.allocation_id.unwrap(),
        },
        closed_receivers: Vec::new(),
        registration_generation: 0,
        receiver_permits: Vec::new(),
        superseded_before: None,
    };
    let mut failed = good.clone();
    failed.attempt.primary.stable_id.0.push_str("-absent");
    let results = owner
        .route_ops()
        .revoke_publications_batch(&[failed, good.clone()])
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].is_err());
    assert_eq!(
        results[1].as_ref().unwrap().reference.state,
        mooncake_store_core::AllocationRouteReferenceState::Exact
    );
    assert_eq!(owner.get("healthy").unwrap(), b"healthy-batch-payload");
    assert_eq!(owner.allocator.lock().usage_bytes().0, replica.length);
    owner.remove("healthy", true).unwrap();
    let results = owner
        .route_ops()
        .revoke_publications_batch(&[good])
        .unwrap();
    assert_eq!(
        results[0].as_ref().unwrap().reference.state,
        mooncake_store_core::AllocationRouteReferenceState::Absent
    );
}

#[test]
fn publication_fence_shrink_ignores_ungranted_unreachable_authorities_across_namespaces() {
    for iteration in 0..32 {
        eprintln!("publication shrink namespace iteration={iteration}");
        super::true_client_shrink_ignores_unreachable_route_authority();
    }
}

#[test]
fn publication_fence_batch_dynamic_receiver_failure_retains_only_its_item() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new(
        "publication-batch-dynamic-owner-segment",
    ));
    let owner = StoreClientBuilder::new(metadata.clone(), "publication-batch-dynamic-owner")
        .state(ClientLifecycleState::Active)
        .live_client_sync_interval(Duration::ZERO)
        .route_topk(2)
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(256))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let routes = [
        owner.put("registered", b"registered-payload").unwrap(),
        owner.put("healthy", b"healthy-payload").unwrap(),
    ];
    let requests = routes
        .iter()
        .map(|route| {
            let replica = &route.replicas[0];
            mooncake_store_core::PublicationRevokeRequest {
                attempt: replica.publication.clone().unwrap(),
                allocation: mooncake_store_core::AllocationRouteIdentity {
                    owner: replica.owner.clone(),
                    segment_name: replica.segment_name.clone(),
                    offset_bytes: replica.segment_offset,
                    length_bytes: replica.length,
                    allocation_id: replica.allocation_id.unwrap(),
                },
                closed_receivers: Vec::new(),
                registration_generation: 0,
                receiver_permits: Vec::new(),
                superseded_before: None,
            }
        })
        .collect::<Vec<_>>();
    let peer = StoreClientBuilder::new(metadata.clone(), "publication-batch-dynamic-peer")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(
            transport.peer("publication-batch-dynamic-peer-segment"),
        ))
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
    peer.register_local_memory().unwrap();
    wait_for_membership_convergence(&[&owner, &peer]);
    assert_eq!(peer.get("registered").unwrap(), b"registered-payload");
    let first = owner
        .route_ops()
        .revoke_publications_batch(&requests)
        .unwrap();
    assert!(first[0]
        .as_ref()
        .unwrap()
        .registered_receivers
        .contains(peer.runtime_id()));
    assert!(!first[1]
        .as_ref()
        .unwrap()
        .registered_receivers
        .contains(peer.runtime_id()));
    let used = owner.allocator.lock().usage_bytes().0;
    drop(peer);
    let second = owner
        .route_ops()
        .revoke_publications_batch(&requests)
        .unwrap();
    assert!(
        second[0].is_err(),
        "the registered receiver must remain required after failure"
    );
    assert_eq!(
        second[1].as_ref().unwrap().reference.state,
        mooncake_store_core::AllocationRouteReferenceState::Exact
    );
    assert_eq!(owner.allocator.lock().usage_bytes().0, used);
    assert_eq!(owner.get("healthy").unwrap(), b"healthy-payload");
}

struct LostDeleteMirrorDirectory {
    inner: Arc<dyn RouteDirectory>,
    primary: mooncake_store_route::LocalRouteAuthority,
    applied: Mutex<Vec<mooncake_store_core::CasResult>>,
    fail_revoke_once: AtomicBool,
    missing_reads: AtomicBool,
    prune_mirror_lost: bool,
}

impl RouteDirectory for LostDeleteMirrorDirectory {
    delegate_publication_setup!();

    fn revoke_publications(
        &self,
        observer: &ClientLease,
        requests: &[mooncake_store_core::PublicationRevokeRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::PublicationRevokeReply>> {
        if self.fail_revoke_once.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "injected first eviction closure failure".to_string(),
            ));
        }
        self.inner.revoke_publications(observer, requests)
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }

    fn confirm_replica_reference(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        replica: &ReplicaRoute,
    ) -> mooncake_store_core::Result<RouteReplicaReference> {
        if self.missing_reads.load(Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "injected exact confirmation unavailable".to_string(),
            ));
        }
        self.inner.confirm_replica_reference(observer, key, replica)
    }

    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        if self.missing_reads.load(Ordering::SeqCst) {
            return Ok(None);
        }
        self.inner.get_object_route(observer, key)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        if next.is_none() || self.prune_mirror_lost {
            // Model a successful primary CAS whose best-effort mirror message is lost.
            let result = self.primary.compare_and_swap_route(key, expected, next)?;
            self.applied.lock().push(result.clone());
            Ok(result)
        } else {
            self.inner
                .compare_and_swap_object_route(observer, key, expected, next)
        }
    }
}

#[test]
fn publication_eviction_applied_delete_retires_registered_stale_receiver() {
    publication_eviction_delete_proof_case(false, false);
}

#[test]
fn publication_eviction_applied_delete_proof_survives_release_retry() {
    publication_eviction_delete_proof_case(true, false);
}

#[test]
fn publication_eviction_delete_version_overflow_retains_allocation() {
    publication_eviction_delete_proof_case(false, true);
}

#[test]
fn publication_eviction_prune_uses_actual_surviving_route_version() {
    publication_eviction_prune_case(false);
}

#[test]
fn publication_existing_holder_prune_preserves_other_replica_and_payload() {
    publication_eviction_prune_case(true);
}

#[test]
fn publication_existing_holder_rejects_newer_route_and_reused_generation() {
    let owner = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "existing-holder-newer",
    )
    .state(ClientLifecycleState::Active)
    .transport(Arc::new(TestTransport::new(
        "existing-holder-newer-segment",
    )))
    .local_memory(storage_config_with_bytes(16))
    .build(test_future_expiry_ms())
    .unwrap();
    owner.register_local_memory().unwrap();
    let original = owner.put("same-key", b"AAAAAAAAAAAAAAAA").unwrap();
    let mut newer = original.clone();
    newer.version = newer.version.next();
    newer.replicas[0].priority = 7;
    assert!(
        owner
            .route_ops()
            .publish_route(&original.key, Some(original.version), &newer)
            .unwrap()
            .applied
    );
    let rejected = owner
        .route_ops()
        .evict_existing_route_replica(&original, &original.replicas[0], None)
        .unwrap();
    assert!(!rejected.applied);
    assert_eq!(rejected.current, Some(newer));
    assert_eq!(owner.get("same-key").unwrap(), b"AAAAAAAAAAAAAAAA");
    owner.remove("same-key", false).unwrap();
    assert_eq!(owner.storage_owner.process_pending_deletes(64).unwrap(), 1);
    let replacement = owner.put("same-key", b"BBBBBBBBBBBBBBBB").unwrap();
    assert_eq!(
        replacement.replicas[0].segment_offset,
        original.replicas[0].segment_offset
    );
    assert_ne!(
        replacement.replicas[0].allocation_id,
        original.replicas[0].allocation_id
    );
    assert!(
        !owner
            .route_ops()
            .evict_existing_route_replica(&original, &original.replicas[0], None)
            .unwrap()
            .applied
    );
    assert_eq!(owner.get("same-key").unwrap(), b"BBBBBBBBBBBBBBBB");
}

#[test]
fn publication_existing_holder_marker_preserves_equal_and_newer_references() {
    for surviving_version in [2, 3] {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("holder-marker-owner-segment"));
        let owner = StoreClientBuilder::new(metadata.clone(), "holder-marker-owner")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .live_client_sync_interval(Duration::ZERO)
            .transport(transport.clone())
            .local_memory(storage_config_with_bytes(16))
            .build(test_future_expiry_ms())
            .unwrap();
        let primary = StoreClientBuilder::new(metadata.clone(), "holder-marker-primary")
            .state(ClientLifecycleState::Active)
            .label("storage", "false")
            .label("route", "true")
            .label("route_weight", "1000000")
            .live_client_sync_interval(Duration::ZERO)
            .transport(Arc::new(transport.peer("holder-marker-primary-segment")))
            .local_memory(rw_only_config())
            .build(test_future_expiry_ms())
            .unwrap();
        owner.register_local_memory().unwrap();
        primary.register_local_memory().unwrap();
        wait_for_membership_convergence(&[&owner, &primary]);
        let original = owner.put("marker", b"AAAAAAAAAAAAAAAA").unwrap();
        let replica = &original.replicas[0];
        assert_eq!(
            replica.publication.as_ref().unwrap().primary,
            *primary.runtime_id()
        );
        assert_eq!(owner.get("marker").unwrap(), b"AAAAAAAAAAAAAAAA");
        let local = mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            owner.runtime_id().stable_id.clone(),
        );
        let remote = mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            primary.runtime_id().stable_id.clone(),
        );
        assert!(
            local
                .compare_and_swap_route(&original.key, Some(original.version), None)
                .unwrap()
                .applied
        );
        assert_eq!(
            owner
                .route_ops()
                .get_existing_holder_delete_version(&original.key, replica, original.version)
                .unwrap(),
            None
        );
        assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
        let mut surviving = original.clone();
        surviving.version = RouteVersion(surviving_version);
        assert!(
            local
                .compare_and_swap_route(&original.key, Some(original.version), Some(&surviving))
                .unwrap()
                .applied
        );
        assert!(
            remote
                .compare_and_swap_route(&original.key, Some(original.version), None)
                .unwrap()
                .applied
        );
        assert_eq!(
            owner
                .route_ops()
                .get_existing_holder_delete_version(&original.key, replica, original.version)
                .unwrap(),
            Some(original.version)
        );
        assert!(owner
            .storage_owner
            .release_with_publication_fence_before(
                &replica.owner,
                &replica.segment_name,
                replica.segment_offset,
                replica.length,
                replica.allocation_id.unwrap(),
                Some(RouteVersion(2)),
            )
            .is_err());
        assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
        assert_eq!(local.get_route(&original.key).unwrap(), Some(surviving));
        assert_eq!(owner.get("marker").unwrap(), b"AAAAAAAAAAAAAAAA");
    }
}

#[test]
fn publication_existing_holder_rejects_restarted_primary_marker() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("holder-epoch-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "holder-epoch-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(16))
        .build(test_future_expiry_ms())
        .unwrap();
    let primary = StoreClientBuilder::new(metadata.clone(), "holder-epoch-primary")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route_weight", "1000000")
        .label("route", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(transport.peer("holder-epoch-primary-segment")))
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    primary.register_local_memory().unwrap();
    wait_for_membership_convergence(&[&owner, &primary]);
    let original = owner.put("marker", b"AAAAAAAAAAAAAAAA").unwrap();
    let replica = &original.replicas[0];
    let old_primary = primary.runtime_id().clone();
    assert_eq!(replica.publication.as_ref().unwrap().primary, old_primary);
    assert_eq!(owner.get("marker").unwrap(), b"AAAAAAAAAAAAAAAA");
    drop(primary);
    let replacement = StoreClientBuilder::new(metadata.clone(), "holder-epoch-primary")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route_weight", "1000000")
        .label("route", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(transport.peer("holder-epoch-replacement-segment")))
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
    replacement.register_local_memory().unwrap();
    wait_for_membership_convergence(&[&owner, &replacement]);
    assert_ne!(replacement.runtime_id().epoch, old_primary.epoch);
    let authority = mooncake_store_route::LocalRouteAuthority::new(
        metadata.route_namespace(),
        replacement.runtime_id().stable_id.clone(),
    );
    let mut tombstone = original.clone();
    tombstone.version = RouteVersion(7);
    tombstone.state = RouteState::Tombstone;
    tombstone.replicas.clear();
    assert!(
        authority
            .compare_and_swap_route(&original.key, None, Some(&tombstone))
            .unwrap()
            .applied
    );
    assert_eq!(
        authority
            .get_route_record(&original.key)
            .unwrap()
            .delete_version,
        Some(RouteVersion(7))
    );
    assert!(owner
        .route_ops()
        .get_existing_holder_delete_version(&original.key, replica, original.version)
        .is_err());
    assert!(owner
        .storage_owner
        .release_with_publication_fence(
            &replica.owner,
            &replica.segment_name,
            replica.segment_offset,
            replica.length,
            replica.allocation_id.unwrap(),
        )
        .is_err());
    assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
}

fn publication_eviction_prune_case(existing_holder: bool) {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("publication-prune-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "publication-prune-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_topk(3)
        .transport(transport.clone())
        .live_client_sync_interval(Duration::ZERO)
        .local_memory(storage_config_with_bytes(16))
        .build(test_future_expiry_ms())
        .unwrap();
    let replica = StoreClientBuilder::new(metadata.clone(), "publication-prune-replica")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_topk(3)
        .transport(Arc::new(
            transport.peer("publication-prune-replica-segment"),
        ))
        .live_client_sync_interval(Duration::ZERO)
        .local_memory(storage_config_with_bytes(16))
        .build(test_future_expiry_ms())
        .unwrap();
    let peer = StoreClientBuilder::new(metadata.clone(), "publication-prune-peer")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route", "true")
        .route_topk(3)
        .transport(Arc::new(transport.peer("publication-prune-peer-segment")))
        .live_client_sync_interval(Duration::ZERO)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
    for client in [&owner, &replica, &peer] {
        client.register_local_memory().unwrap();
    }
    wait_for_membership_convergence(&[&owner, &replica, &peer]);
    let original = owner
        .put_with_policy(
            "prune",
            b"AAAAAAAAAAAAAAAA",
            &ReplicationPolicy::new().replica_count(2),
        )
        .unwrap();
    assert_eq!(original.replicas.len(), 2);
    assert_eq!(peer.get("prune").unwrap(), b"AAAAAAAAAAAAAAAA");
    for client in [&owner, &replica, &peer] {
        let authority = mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            client.runtime_id().stable_id.clone(),
        );
        assert!(authority.get_route(&original.key).unwrap().is_some());
    }
    let removed = original
        .replicas
        .iter()
        .find(|replica| replica.owner == *owner.runtime_id())
        .unwrap();
    let attempt = removed.publication.as_ref().unwrap();
    let directory = Arc::new(LostDeleteMirrorDirectory {
        inner: owner.route_directory.clone(),
        primary: mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            attempt.primary.stable_id.clone(),
        ),
        applied: Mutex::new(Vec::new()),
        fail_revoke_once: AtomicBool::new(false),
        missing_reads: AtomicBool::new(false),
        prune_mirror_lost: true,
    });
    let maintenance_directory: Arc<dyn RouteDirectory> = if existing_holder {
        owner.route_directory.clone()
    } else {
        directory.clone()
    };
    let maintenance = test_storage_owner_state_with_route_directory(
        &owner,
        maintenance_directory,
        ColdTierOffloadPriorityConfig::default(),
    );
    maintenance.sync_route(&original);
    if existing_holder {
        let index = original
            .replicas
            .iter()
            .position(|item| item == removed)
            .unwrap();
        let next = cold_tier::route_after_replica_eviction(&original, index, false).unwrap();
        let mut invalid = next.clone();
        invalid.replicas[0].checksum = Some(1);
        assert!(owner
            .route_ops()
            .evict_existing_route_replica(&original, removed, Some(&invalid))
            .is_err());
        assert_eq!(owner.get("prune").unwrap(), b"AAAAAAAAAAAAAAAA");
        let victim = maintenance
            .hot_replicas
            .clock
            .lock()
            .entry_ids_for_key(&original.key)
            .into_iter()
            .find(|entry| entry.allocation_id == removed.allocation_id)
            .unwrap();
        assert!(maintenance
            .evict_route_replica_with_next_mode(&victim, &original, index, Some(next), true)
            .unwrap());
        let surviving = replica.query_route("prune").unwrap().unwrap();
        assert_eq!(surviving.version, original.version.next());
        assert_eq!(surviving.replicas.len(), 1);
        assert_eq!(owner.allocator.lock().usage_bytes().0, 0);
        assert_eq!(replica.get("prune").unwrap(), b"AAAAAAAAAAAAAAAA");
        return;
    }
    maintenance.evict_until_low_watermark(90, 1).unwrap();
    let applied = directory.applied.lock();
    assert_eq!(applied.len(), 1);
    assert!(applied[0].applied);
    assert!(applied[0].delete_version.is_none());
    let current = applied[0].current.as_ref().unwrap();
    assert_eq!(current.version, original.version.next());
    assert_eq!(current.replicas.len(), 1);
    assert_eq!(current.replicas[0].owner, *replica.runtime_id());
    assert!(maintenance.pending_evictions.lock().is_empty());
    assert_eq!(owner.allocator.lock().usage_bytes().0, 0);
    assert_eq!(replica.get("prune").unwrap(), b"AAAAAAAAAAAAAAAA");
}

#[test]
fn publication_historical_clock_does_not_abort_to_reused_generation() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("publication-generation-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "publication-generation-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .transport(transport)
        .live_client_sync_interval(Duration::ZERO)
        .local_memory(storage_config_with_bytes(16).reclaim_grace_ms(0))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let original = owner.put("generation", b"AAAAAAAAAAAAAAAA").unwrap();
    let directory = Arc::new(LostDeleteMirrorDirectory {
        inner: owner.route_directory.clone(),
        primary: mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            owner.runtime_id().stable_id.clone(),
        ),
        applied: Mutex::new(Vec::new()),
        fail_revoke_once: AtomicBool::new(false),
        missing_reads: AtomicBool::new(true),
        prune_mirror_lost: false,
    });
    let maintenance = test_storage_owner_state_with_route_directory(
        &owner,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    maintenance.sync_route(&original);
    maintenance.evict_until_low_watermark(90, 1).unwrap();
    assert!(!maintenance.pending_evictions.lock().is_empty());
    assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
    owner.remove("generation", false).unwrap();
    owner.storage_owner.process_pending_deletes(64).unwrap();
    assert_eq!(owner.allocator.lock().usage_bytes().0, 0);
    let replacement = owner.put("generation", b"BBBBBBBBBBBBBBBB").unwrap();
    assert_eq!(
        replacement.replicas[0].segment_offset,
        original.replicas[0].segment_offset
    );
    assert_ne!(
        replacement.replicas[0].allocation_id,
        original.replicas[0].allocation_id
    );
    directory.missing_reads.store(false, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline && !maintenance.pending_evictions.lock().is_empty() {
        std::thread::sleep(Duration::from_millis(10));
        maintenance.process_pending_evictions(64).unwrap();
    }
    let pending = maintenance.pending_evictions.lock();
    assert!(pending.is_empty(), "pending={pending:?}");
    drop(pending);
    assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
    assert_eq!(owner.get("generation").unwrap(), b"BBBBBBBBBBBBBBBB");
    assert!(owner
        .storage_owner
        .hot_replicas
        .clock
        .lock()
        .entry_ids_for_key(&replacement.key)
        .iter()
        .any(|entry| entry.allocation_id == replacement.replicas[0].allocation_id));
}

#[test]
fn publication_historical_clock_new_cohort_absence_preserves_reclaim_progress() {
    publication_historical_clock_new_cohort_case(false, 0);
}

#[test]
fn publication_historical_clock_retired_import_preserves_reclaim_progress() {
    publication_historical_clock_new_cohort_case(true, 0);
}

#[test]
fn publication_historical_existing_holder_unknown_reply_preserves_reclaim_progress() {
    publication_historical_clock_new_cohort_case(true, 1);
    publication_historical_clock_new_cohort_case(true, 2);
}

#[test]
fn publication_historical_existing_holder_lost_applied_reply_with_old_mirror_reclaims() {
    publication_historical_clock_new_cohort_case(true, 3);
}

#[test]
fn publication_historical_existing_holder_max_delete_marker_retains_allocation() {
    publication_historical_clock_new_cohort_case(true, 4);
}

struct ExistingHolderReplyLoss {
    inner: Arc<dyn RouteDirectory>,
    remaining: AtomicBool,
    after_applied: bool,
}

impl RouteDirectory for ExistingHolderReplyLoss {
    delegate_publication_setup!();

    fn get_existing_holder_delete_version(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        replica: &ReplicaRoute,
        expected: RouteVersion,
    ) -> mooncake_store_core::Result<Option<RouteVersion>> {
        self.inner
            .get_existing_holder_delete_version(observer, key, replica, expected)
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }

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
    ) -> mooncake_store_core::Result<RouteReplicaReference> {
        self.inner.confirm_replica_reference(observer, key, replica)
    }

    fn revoke_publications(
        &self,
        observer: &ClientLease,
        requests: &[mooncake_store_core::PublicationRevokeRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::PublicationRevokeReply>> {
        self.inner.revoke_publications(observer, requests)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn evict_existing_route_replica(
        &self,
        observer: &ClientLease,
        current: &ObjectRoute,
        replica: &ReplicaRoute,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
        let lose = self.remaining.swap(false, Ordering::SeqCst);
        if lose && !self.after_applied {
            return Err(StoreError::Transport(
                "existing holder request lost before CAS".to_string(),
            ));
        }
        let result = self
            .inner
            .evict_existing_route_replica(observer, current, replica, next)?;
        if lose {
            assert!(result.applied);
            return Err(StoreError::Transport(
                "existing holder Applied reply lost".to_string(),
            ));
        }
        Ok(result)
    }
}

fn publication_historical_clock_new_cohort_case(retire_later_sequence: bool, loss: u8) {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("publication-historical-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "publication-historical-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(if retire_later_sequence {
            32
        } else {
            16
        }))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let old_mirror = (loss >= 3).then(|| {
        let peer = StoreClientBuilder::new(metadata.clone(), "publication-historical-old-mirror")
            .state(ClientLifecycleState::Active)
            .label("storage", "false")
            .label("route", "true")
            .label("route_weight", "1000000")
            .live_client_sync_interval(Duration::ZERO)
            .transport(Arc::new(
                transport.peer("publication-historical-old-mirror-segment"),
            ))
            .local_memory(rw_only_config())
            .build(test_future_expiry_ms())
            .unwrap();
        peer.register_local_memory().unwrap();
        wait_for_membership_convergence(&[&owner, &peer]);
        peer
    });
    let mut original = owner.put("historical", b"AAAAAAAAAAAAAAAA").unwrap();
    if old_mirror.is_some() {
        assert_eq!(original.replicas.len(), 1);
        assert_eq!(
            original.replicas[0]
                .publication
                .as_ref()
                .unwrap()
                .receivers
                .len(),
            2
        );
        assert_eq!(owner.get("historical").unwrap(), b"AAAAAAAAAAAAAAAA");
    }
    if loss == 4 {
        let mut maximum = original.clone();
        maximum.version = RouteVersion(u64::MAX);
        for client in [&owner, old_mirror.as_ref().unwrap()] {
            let authority = mooncake_store_route::LocalRouteAuthority::new(
                metadata.route_namespace(),
                client.runtime_id().stable_id.clone(),
            );
            assert!(
                authority
                    .compare_and_swap_route(&original.key, Some(original.version), Some(&maximum))
                    .unwrap()
                    .applied
            );
        }
        original = maximum;
    }
    let mut peers = Vec::new();
    for index in 0..2 {
        let peer = StoreClientBuilder::new(
            metadata.clone(),
            format!("publication-historical-peer-{index}"),
        )
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route", "true")
        .label("route_weight", "1000000000000")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(
            transport.peer(&format!("publication-historical-peer-{index}-segment")),
        ))
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
        peer.register_local_memory().unwrap();
        peers.push(peer);
    }
    wait_for_membership_convergence(&[&owner, &peers[0], &peers[1]]);
    if retire_later_sequence {
        let later = owner.put("later-floor", b"CCCCCCCCCCCCCCCC").unwrap();
        let attempt = later.replicas[0].publication.as_ref().unwrap();
        assert!(attempt.sequence > original.replicas[0].publication.as_ref().unwrap().sequence);
        assert_eq!(attempt.receivers.len(), 2);
        for peer in &peers {
            assert!(attempt.receivers.contains(peer.runtime_id()));
        }
        owner.remove("later-floor", false).unwrap();
        assert_eq!(owner.storage_owner.process_pending_deletes(64).unwrap(), 1);
        owner
            .storage_owner
            .recover_due_pending_publications()
            .unwrap();
        assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
    }
    let maintenance = test_storage_owner_state_with_route_directory(
        &owner,
        Arc::new(ExistingHolderReplyLoss {
            inner: owner.route_directory.clone(),
            remaining: AtomicBool::new(loss != 0),
            after_applied: loss >= 2,
        }),
        ColdTierOffloadPriorityConfig::default(),
    );
    maintenance.sync_route(&original);
    let authorities = [&owner, &peers[0], &peers[1]].map(|client| {
        mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            client.runtime_id().stable_id.clone(),
        )
    });
    assert_eq!(
        authorities[0].get_route(&original.key).unwrap(),
        Some(original.clone())
    );
    for authority in &authorities[1..] {
        let record = authority.get_route_record(&original.key).unwrap();
        assert!(record.route.is_none());
        assert!(record.delete_version.is_none());
    }
    if retire_later_sequence {
        assert_eq!(owner.get("historical").unwrap(), b"AAAAAAAAAAAAAAAA");
        for authority in &authorities[1..] {
            let record = authority.get_route_record(&original.key).unwrap();
            assert!(record.route.is_none());
            assert!(record.delete_version.is_none());
        }
    }
    maintenance
        .evict_until_low_watermark(if retire_later_sequence { 1 } else { 90 }, 1)
        .unwrap();
    let deadline = Instant::now() + Duration::from_millis(350);
    while Instant::now() < deadline && owner.allocator.lock().usage_bytes().0 != 0 {
        std::thread::sleep(Duration::from_millis(10));
        maintenance
            .evict_until_low_watermark(if retire_later_sequence { 1 } else { 90 }, 1)
            .unwrap();
    }
    for (index, authority) in authorities.iter().enumerate() {
        eprintln!(
            "historical retired={retire_later_sequence} authority={index} record={:?}",
            authority.get_route_record(&original.key).unwrap()
        );
    }
    if loss == 4 {
        assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
        assert!(!maintenance.pending_evictions.lock().is_empty());
        let primary = mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            original.replicas[0]
                .publication
                .as_ref()
                .unwrap()
                .primary
                .stable_id
                .clone(),
        );
        assert_eq!(
            primary
                .get_route_record(&original.key)
                .unwrap()
                .delete_version,
            Some(RouteVersion(u64::MAX))
        );
        return;
    }
    assert!(maintenance.pending_evictions.lock().is_empty());
    assert_eq!(owner.allocator.lock().usage_bytes().0, 0);
    for authority in &authorities {
        assert!(authority.get_route(&original.key).unwrap().is_none());
    }
    let replacement = owner.put("replacement", b"BBBBBBBBBBBBBBBB").unwrap();
    assert_eq!(
        replacement.replicas[0].segment_offset,
        original.replicas[0].segment_offset
    );
    assert_ne!(
        replacement.replicas[0].allocation_id,
        original.replicas[0].allocation_id
    );
    assert_eq!(owner.get("replacement").unwrap(), b"BBBBBBBBBBBBBBBB");
}

fn publication_eviction_delete_proof_case(fail_revoke_once: bool, overflow: bool) {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("publication-delete-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "publication-delete-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(16))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let mut original = owner.put("before", b"AAAAAAAAAAAAAAAA").unwrap();
    if overflow {
        let mut next = original.clone();
        next.version = RouteVersion(u64::MAX);
        assert!(
            owner
                .cas_route("before", Some(original.version), Some(&next))
                .unwrap()
                .applied
        );
        original = next;
    }
    let mut peers = Vec::new();
    for index in 0..2 {
        let peer =
            StoreClientBuilder::new(metadata.clone(), format!("publication-delete-peer-{index}"))
                .state(ClientLifecycleState::Active)
                .label("storage", "false")
                .label("route", "true")
                .label("route_weight", "1000000")
                .live_client_sync_interval(Duration::ZERO)
                .transport(Arc::new(
                    transport.peer(&format!("publication-delete-peer-{index}-segment")),
                ))
                .local_memory(rw_only_config())
                .build(test_future_expiry_ms())
                .unwrap();
        peer.register_local_memory().unwrap();
        peers.push(peer);
    }
    wait_for_membership_convergence(&[&owner, &peers[0], &peers[1]]);
    for peer in &peers {
        assert_eq!(peer.get("before").unwrap(), b"AAAAAAAAAAAAAAAA");
    }
    let authorities = [&owner, &peers[0], &peers[1]].map(|client| {
        mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            client.runtime_id().stable_id.clone(),
        )
    });
    for authority in &authorities {
        assert!(authority.get_route(&original.key).unwrap().is_some());
    }
    let directory = Arc::new(LostDeleteMirrorDirectory {
        inner: owner.route_directory.clone(),
        primary: mooncake_store_route::LocalRouteAuthority::new(
            metadata.route_namespace(),
            original.replicas[0]
                .publication
                .as_ref()
                .unwrap()
                .primary
                .stable_id
                .clone(),
        ),
        applied: Mutex::new(Vec::new()),
        fail_revoke_once: AtomicBool::new(fail_revoke_once),
        missing_reads: AtomicBool::new(false),
        prune_mirror_lost: false,
    });
    let maintenance = test_storage_owner_state_with_route_directory(
        &owner,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    maintenance.sync_route(&original);
    maintenance.evict_until_low_watermark(90, 1).unwrap();
    let applied = directory.applied.lock();
    assert_eq!(applied.len(), 1);
    assert!(applied[0].applied);
    assert!(applied[0].current.is_none());
    assert_eq!(applied[0].delete_version, Some(original.version));
    if fail_revoke_once {
        let pending = maintenance.pending_evictions.lock();
        let entry = pending
            .values()
            .next()
            .expect("failed closure must retain the token");
        assert_eq!(entry.phase, PendingEvictionPhase::ReadyToCommit);
        assert_eq!(entry.token.superseded_before, Some(original.version.next()));
    }
    assert!(
        authorities.iter().any(|authority| authority
            .get_route_record(&original.key)
            .unwrap()
            .delete_version
            .is_some()),
        "production watermark eviction must first delete the route"
    );
    let retry_deadline = Instant::now() + Duration::from_millis(350);
    while Instant::now() < retry_deadline && !maintenance.pending_evictions.lock().is_empty() {
        std::thread::sleep(Duration::from_millis(10));
        maintenance.process_pending_evictions(64).unwrap();
    }
    if overflow {
        let pending = maintenance.pending_evictions.lock();
        assert!(!pending.is_empty());
        assert!(pending
            .values()
            .all(|entry| entry.token.superseded_before.is_none()));
        assert_eq!(owner.allocator.lock().usage_bytes().0, 16);
        assert!(authorities
            .iter()
            .any(|authority| authority.get_route(&original.key).unwrap().is_some()));
        return;
    }
    assert!(
        maintenance.pending_evictions.lock().is_empty(),
        "an applied deletion must close and reclaim its stale receiver references"
    );
    let replacement = owner
        .put("after", b"BBBBBBBBBBBBBBBB")
        .expect("successful eviction must release the old allocation");
    assert_eq!(
        replacement.replicas[0].segment_offset,
        original.replicas[0].segment_offset
    );
    assert_ne!(
        replacement.replicas[0].allocation_id,
        original.replicas[0].allocation_id
    );
    assert_eq!(owner.get("after").unwrap(), b"BBBBBBBBBBBBBBBB");
    assert!(maintenance.pending_evictions.lock().is_empty());
    for authority in &authorities {
        assert!(authority.get_route(&original.key).unwrap().is_none());
    }
}

#[test]
fn publication_fence_owner_rechecks_receivers_registered_after_exact_revoke() {
    use mooncake_store_core::{AllocationRouteReferenceState, PublicationRevokeRequest};
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("publication-registration-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "publication-registration-owner")
        .state(ClientLifecycleState::Active)
        .live_client_sync_interval(Duration::ZERO)
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(64))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let route = owner.put("key", b"registered-data").unwrap();
    let replica = &route.replicas[0];
    let request = PublicationRevokeRequest {
        attempt: replica.publication.clone().unwrap(),
        allocation: mooncake_store_core::AllocationRouteIdentity {
            owner: replica.owner.clone(),
            segment_name: replica.segment_name.clone(),
            offset_bytes: replica.segment_offset,
            length_bytes: replica.length,
            allocation_id: replica.allocation_id.unwrap(),
        },
        closed_receivers: Vec::new(),
        registration_generation: 0,
        receiver_permits: Vec::new(),
        superseded_before: None,
    };
    let old = owner
        .route_ops()
        .revoke_publications(std::slice::from_ref(&request))
        .unwrap()
        .remove(0);
    assert_eq!(old.reference.state, AllocationRouteReferenceState::Exact);
    let peer = StoreClientBuilder::new(metadata.clone(), "publication-registration-peer")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(
            transport.peer("publication-registration-peer-segment"),
        ))
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
    peer.register_local_memory().unwrap();
    wait_for_membership_convergence(&[&owner, &peer]);
    assert_eq!(peer.get("key").unwrap(), b"registered-data");
    let primary = mooncake_store_route::LocalRouteAuthority::new(
        metadata.route_namespace(),
        owner.runtime_id().stable_id.clone(),
    );
    let receiver = mooncake_store_route::LocalRouteAuthority::new(
        metadata.route_namespace(),
        peer.runtime_id().stable_id.clone(),
    );
    assert!(
        receiver.get_route(&route.key).unwrap().is_some(),
        "public GET must import the route through receiver registration"
    );
    let mooncake_store_core::PublicationControlReply::Revoked(current) = primary
        .publication_control(&[mooncake_store_core::PublicationControl::Revoke(
            request.clone(),
        )])
        .unwrap()
        .remove(0)
        .unwrap()
    else {
        panic!("primary closure reply required");
    };
    assert!(current.registration_generation > old.registration_generation);
    assert!(current.registered_receivers.contains(peer.runtime_id()));
    let batch_closure = owner
        .route_ops()
        .revoke_publications_batch(std::slice::from_ref(&request))
        .unwrap()
        .remove(0)
        .unwrap();
    assert!(batch_closure
        .registered_receivers
        .contains(peer.runtime_id()));
    assert_eq!(
        batch_closure.registration_generation,
        current.registration_generation
    );
    assert_eq!(
        batch_closure.reference.state,
        AllocationRouteReferenceState::Exact
    );
    assert!(
        primary
            .compare_and_swap_route(&route.key, Some(route.version), None)
            .unwrap()
            .applied
    );
    assert!(receiver.get_route(&route.key).unwrap().is_some());
    assert!(!owner.allocator.lock().publication_provenance.is_empty());
    assert_eq!(
        receiver
            .get_allocation_route_references(std::slice::from_ref(&request.allocation))
            .unwrap(),
        vec![AllocationRouteReferenceState::Exact]
    );
    let release = AllocationReleaseRequest {
        storage_runtime: replica.owner.clone(),
        segment_name: replica.segment_name.clone(),
        offset_bytes: replica.segment_offset,
        length_bytes: replica.length,
        allocation_id: replica.allocation_id.unwrap(),
    };
    let retained = owner.release_segment_allocations_batch(std::slice::from_ref(&release));
    match retained {
        Err(StoreError::Backpressure(_)) => {
            assert!(owner.allocator.lock().usage_bytes().0 > 0);
            receiver.compare_and_swap_route(&route.key, Some(route.version), None).unwrap();
            owner.release_segment_allocations_batch(&[release]).unwrap();
        }
        Ok(()) => assert!(receiver.get_route(&route.key).unwrap().is_none(),
            "a successful release must first fence and remove the dynamically registered receiver reference"),
        Err(error) => panic!("unexpected owner release failure: {error}"),
    }
    assert_eq!(owner.allocator.lock().usage_bytes().0, 0);
    assert!(owner.allocator.lock().publication_provenance.is_empty());
    let mut replay = route.clone();
    replay.version = RouteVersion(1000);
    assert!(matches!(
        receiver.replace_route(&route.key, Some(&replay)),
        Err(StoreError::PublicationRetired(_))
    ));
    let replacement = owner.put("replacement", b"replacement-123").unwrap();
    assert_eq!(
        replacement.replicas[0].segment_offset,
        replica.segment_offset
    );
    assert_ne!(replacement.replicas[0].allocation_id, replica.allocation_id);
    assert_eq!(peer.get("replacement").unwrap(), b"replacement-123");
}

struct RetireOneBatchPublication {
    inner: Arc<dyn RouteDirectory>,
    initial: Mutex<Vec<ObjectRoute>>,
    retired: AtomicBool,
}

impl RouteDirectory for RetireOneBatchPublication {
    delegate_publication_setup!();
    delegate_publication_revoke!();
    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
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
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }
    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[mooncake_store_core::RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<mooncake_store_core::CasResult>>>
    {
        if !self.retired.swap(true, Ordering::SeqCst) {
            *self.initial.lock() = requests
                .iter()
                .filter_map(|request| request.next.clone())
                .collect();
            let route = requests[1].next.as_ref().unwrap();
            let replica = &route.replicas[0];
            let reply = self.inner.revoke_publications(
                observer,
                &[mooncake_store_core::PublicationRevokeRequest {
                    attempt: replica.publication.clone().unwrap(),
                    allocation: mooncake_store_core::AllocationRouteIdentity {
                        owner: replica.owner.clone(),
                        segment_name: replica.segment_name.clone(),
                        offset_bytes: replica.segment_offset,
                        length_bytes: replica.length,
                        allocation_id: replica.allocation_id.unwrap(),
                    },
                    closed_receivers: Vec::new(),
                    registration_generation: 0,
                    receiver_permits: Vec::new(),
                    superseded_before: None,
                }],
            )?;
            assert!(reply[0].registration_closed);
            assert_eq!(
                reply[0].reference.state,
                mooncake_store_core::AllocationRouteReferenceState::Absent
            );
        }
        self.inner
            .compare_and_swap_object_routes(observer, requests)
    }
}

#[test]
fn publication_fence_batch_retired_item_preserves_tenant_order_and_success_identity() {
    let (_storage, mut client, _transport) = super::batch_put_status_tests::routed_status_writer(
        Arc::new(InMemoryMetadataBackend::new()),
        "publication-batch-owner",
    );
    let directory = Arc::new(RetireOneBatchPublication {
        inner: client.route_directory.clone(),
        initial: Mutex::new(Vec::new()),
        retired: AtomicBool::new(false),
    });
    client.route_directory = directory.clone();
    let healthy_payload = b"healthy-payload";
    let retired_payload = b"retired-payload";
    client
        .register_buffer(
            healthy_payload.as_ptr() as *mut std::ffi::c_void,
            healthy_payload.len(),
        )
        .unwrap();
    client
        .register_buffer(
            retired_payload.as_ptr() as *mut std::ffi::c_void,
            retired_payload.len(),
        )
        .unwrap();
    let statuses = client.batch_put_from_statuses(&[
        PutFromRequest::new(
            "healthy",
            healthy_payload.as_ptr().cast(),
            healthy_payload.len(),
        )
        .tenant("other"),
        PutFromRequest::new(
            "retired",
            retired_payload.as_ptr().cast(),
            retired_payload.len(),
        )
        .tenant("other"),
    ]);
    client
        .unregister_buffer(
            healthy_payload.as_ptr() as *mut std::ffi::c_void,
            healthy_payload.len(),
        )
        .unwrap();
    client
        .unregister_buffer(
            retired_payload.as_ptr() as *mut std::ffi::c_void,
            retired_payload.len(),
        )
        .unwrap();
    assert_eq!(statuses.len(), 2);
    let healthy = statuses[0]
        .as_ref()
        .expect("healthy item must remain successful");
    let retried = statuses[1]
        .as_ref()
        .expect("retired item must freshly reserve and rewrite");
    let initial = directory.initial.lock();
    assert_eq!(healthy.key, initial[0].key);
    assert_eq!(retried.key, initial[1].key);
    assert_eq!(
        healthy.replicas[0].allocation_id,
        initial[0].replicas[0].allocation_id
    );
    assert_eq!(
        healthy.replicas[0].publication,
        initial[0].replicas[0].publication
    );
    assert_ne!(
        retried.replicas[0].allocation_id,
        initial[1].replicas[0].allocation_id
    );
    assert_ne!(
        retried.replicas[0].publication,
        initial[1].replicas[0].publication
    );
    assert_eq!(
        client.get_in_tenant("other", "healthy").unwrap(),
        b"healthy-payload"
    );
    assert_eq!(
        client.get_in_tenant("other", "retired").unwrap(),
        b"retired-payload"
    );
    assert!(client.query_route("healthy").unwrap().is_none());
    assert!(client.query_route("retired").unwrap().is_none());
}

#[test]
fn publication_fence_rejects_legacy_serialized_replay_after_real_reuse() {
    let client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "publication-legacy-replay-owner",
    )
    .state(ClientLifecycleState::Active)
    .transport(Arc::new(TestTransport::new(
        "publication-legacy-replay-segment",
    )))
    .local_memory(storage_config_with_bytes(32))
    .build(test_future_expiry_ms())
    .unwrap();
    client.register_local_memory().unwrap();
    let original = client.put("original", b"AAAAAAAAAAAAAAAA").unwrap();
    client.remove("original", true).unwrap();
    assert_eq!(client.storage_owner.process_pending_deletes(64).unwrap(), 1);
    client
        .storage_owner
        .recover_due_pending_publications()
        .unwrap();
    let replacement = client.put("replacement", b"BBBBBBBBBBBBBBBB").unwrap();
    assert_eq!(
        replacement.replicas[0].segment_offset,
        original.replicas[0].segment_offset
    );
    assert_ne!(
        replacement.replicas[0].allocation_id,
        original.replicas[0].allocation_id
    );
    let authority = mooncake_store_route::LocalRouteAuthority::new(
        client.metadata.route_namespace(),
        client.runtime_id().stable_id.clone(),
    );
    for omit_generation in [false, true] {
        let mut old_format = serde_json::to_value(&original).unwrap();
        let replica = old_format["replicas"][0].as_object_mut().unwrap();
        replica.remove("publication");
        if omit_generation {
            replica.remove("allocation_id");
        }
        let mut legacy: ObjectRoute = serde_json::from_value(old_format).unwrap();
        assert!(legacy.replicas[0].publication.is_none());
        legacy.version = RouteVersion(1000);
        match client.cas_route("original", None, Some(&legacy)) {
            Err(StoreError::PublicationRetired(_)) => {}
            Ok(cas) if cas.applied => {
                assert_eq!(client.get("original").unwrap(), b"AAAAAAAAAAAAAAAA",
                    "an old serialized route must not expose a reused allocation; omitted_generation={omit_generation}");
                panic!("a legacy replay must be rejected before publication");
            }
            other => panic!("unexpected legacy replay result: {other:?}"),
        }
        assert!(matches!(
            authority.replace_route(&legacy.key, Some(&legacy)),
            Err(StoreError::PublicationRetired(_))
        ));
    }
    assert_eq!(client.get("replacement").unwrap(), b"BBBBBBBBBBBBBBBB");
    assert!(client.query_route("original").unwrap().is_none());
}

struct MissingPublicationAuthority {
    inner: Arc<dyn RouteDirectory>,
}

impl RouteDirectory for MissingPublicationAuthority {
    fn publication_fencing_enabled(&self) -> bool {
        true
    }
    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
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
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }
}

#[test]
fn publication_fence_rejects_missing_authority_protocol_before_reserve() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "publication-missing-authority")
        .state(ClientLifecycleState::Active)
        .transport(Arc::new(TestTransport::new(
            "publication-missing-authority-segment",
        )))
        .local_memory(storage_config_with_bytes(1024))
        .build(test_future_expiry_ms())
        .unwrap();
    client.register_local_memory().unwrap();
    client.route_directory = Arc::new(MissingPublicationAuthority {
        inner: client.route_directory.clone(),
    });
    assert!(matches!(
        client.put("key", b"payload"),
        Err(StoreError::Unsupported(_))
    ));
    assert_eq!(client.allocator.lock().usage_bytes().0, 0);
    assert!(client.allocator.lock().publication_provenance.is_empty());
}

#[test]
fn publication_fence_owner_rejects_legacy_wire_reservation() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let client = StoreClientBuilder::new(metadata, "publication-legacy-writer-owner")
        .state(ClientLifecycleState::Active)
        .transport(Arc::new(TestTransport::new(
            "publication-legacy-owner-segment",
        )))
        .local_memory(storage_config_with_bytes(1024))
        .build(test_future_expiry_ms())
        .unwrap();
    client.register_local_memory().unwrap();
    let result = client
        .control_client
        .batch_reserve_any_with_pending_publication(&client.lease(), client.runtime_id(), &[16]);
    let error = result.expect_err("an old writer must not receive an unfenced reservation");
    assert!(
        error
            .to_string()
            .contains("publication provenance is required"),
        "the actual owner protocol guard must reject the request: {error}"
    );
    assert_eq!(client.allocator.lock().usage_bytes().0, 0);
    assert_eq!(
        client
            .storage_owner
            .recover_due_pending_publications()
            .unwrap(),
        0
    );
}

#[test]
fn publication_fence_continuous_public_put_drains_and_retires() {
    continuous_public_put_drains_and_retires(true);
}

#[test]
fn publication_fence_local_only_without_eviction_drains_and_retires() {
    continuous_public_put_drains_and_retires(false);
}

fn continuous_public_put_drains_and_retires(advertise_storage: bool) {
    const THREADS: usize = 8;
    const PER_THREAD: usize = 1024;
    let metadata = Arc::new(CountingMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let builder = StoreClientBuilder::new(metadata.clone(), "publication-continuous-owner")
        .state(ClientLifecycleState::Active);
    let builder = if advertise_storage {
        builder.label("storage", "true")
    } else {
        builder
    };
    let memory = LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(4 * 1024 * 1024)
        .scratch_bytes(64 * 1024)
        .alignment(1)
        .reclaim_grace_ms(0);
    let memory = if advertise_storage {
        memory
    } else {
        memory.eviction_poll_interval(Duration::ZERO)
    };
    let client = Arc::new(
        builder
            .transport(Arc::new(TestTransport::new(
                "publication-continuous-segment",
            )))
            .local_memory(memory)
            .build(test_future_expiry_ms())
            .expect("client should build"),
    );
    client
        .register_local_memory()
        .expect("memory should register");
    assert!(client
        .storage_owner
        .publication_finalization_wake
        .lock()
        .is_some());
    let before_route_gets = metadata.get_object_route_calls();
    let before_membership = metadata.list_live_clients_calls();
    let peak = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for worker in 0..THREADS {
            let client = &client;
            let peak = &peak;
            scope.spawn(move || {
                for index in 0..PER_THREAD {
                    let key = format!("publication-continuous-{worker}-{index}");
                    let value = [(worker + 1) as u8; 16];
                    let route = client
                        .put(&key, &value)
                        .expect("steady writes must not exhaust publication replay state");
                    assert!(route
                        .replicas
                        .iter()
                        .all(|replica| replica.publication.is_some()));
                    peak.fetch_max(
                        client.allocator.lock().publication_finalizations.len(),
                        Ordering::Relaxed,
                    );
                    if index % 128 == 0 {
                        assert_eq!(client.get(&key).expect("sample should be readable"), value);
                    }
                }
            });
        }
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !client.allocator.lock().publication_finalizations.is_empty() && Instant::now() < deadline
    {
        sleep(Duration::from_millis(1));
    }
    assert!(
        client.allocator.lock().publication_finalizations.is_empty(),
        "the real background worker must drain successful publication ACKs"
    );
    assert_eq!(client.allocator.lock().pending_publication_count, 0);
    for worker in 0..THREADS {
        assert_eq!(
            client
                .get(&format!("publication-continuous-{worker}-0"))
                .unwrap(),
            [(worker + 1) as u8; 16]
        );
    }
    assert!(peak.load(Ordering::Relaxed) <= 4096);
    assert_eq!(
        metadata.get_object_route_calls(),
        before_route_gets,
        "Embedded WRH writes must not add metadata route reads"
    );
    println!(
        "publication steady writes={} queue_peak={} route_backend_gets={} membership_refreshes={}",
        THREADS * PER_THREAD,
        peak.load(Ordering::Relaxed),
        metadata.get_object_route_calls() - before_route_gets,
        metadata.list_live_clients_calls() - before_membership
    );
    for worker in 0..THREADS {
        for index in 0..PER_THREAD {
            client
                .remove(&format!("publication-continuous-{worker}-{index}"), true)
                .expect("delete should complete");
        }
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        client
            .flush_all_reclaims()
            .expect("the production reclaim drain should complete");
        client
            .storage_owner
            .process_pending_deletes(256)
            .expect("the production owner maintenance should progress");
        client.storage_owner.reconcile_stale_clock_candidates(None, true).expect("overflowed best-effort delete notifications must remain reclaimable through owner reconciliation");
        let allocator = client.allocator.lock();
        if allocator.publication_finalizations.is_empty()
            && allocator.publication_provenance.is_empty()
            && allocator.usage_bytes().0 == 0
        {
            break;
        }
        assert!(Instant::now() < deadline,
            "deleted allocations and publication proof must retire: used={} provenance={} finalizations={} delete_pending={}",
            allocator.usage_bytes().0, allocator.publication_provenance.len(), allocator.publication_finalizations.len(),
            client.storage_owner.pending_deletes.lock().entries.len());
        drop(allocator);
        sleep(Duration::from_millis(1));
    }
}
