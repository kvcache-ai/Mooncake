use super::*;

struct PublicRollbackFaultAllocator {
    inner: StaticAllocatorAdapter,
    fail_release: Arc<std::sync::atomic::AtomicBool>,
    release_calls: Arc<AtomicUsize>,
}
impl AllocatorService for PublicRollbackFaultAllocator {
    fn bind_publication(
        &self,
        reservation: SegmentReservation,
        attempt: mooncake_store_core::PublicationAttempt,
    ) -> Result<SegmentReservation> {
        self.inner.bind_publication(reservation, attempt)
    }

    fn batch_reserve_any_with_pending_publication(
        &self,
        owner: &ClientRuntimeId,
        lengths: &[u64],
    ) -> Vec<Result<SegmentReservation>> {
        self.inner
            .batch_reserve_any_with_pending_publication(owner, lengths)
    }

    fn batch_reserve_specific_with_pending_publication(
        &self,
        owner: &ClientRuntimeId,
        requests: &[crate::control_plane::ReserveSpecificOp],
    ) -> Vec<Result<SegmentReservation>> {
        self.inner
            .batch_reserve_specific_with_pending_publication(owner, requests)
    }

    fn reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
        self.inner.reserve_any(owner, length_bytes)
    }
    fn reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
        self.inner.reserve_specific(owner, segment, length_bytes)
    }
    fn release(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset: u64,
        length: u64,
        allocation_id: u64,
    ) -> mooncake_store_core::Result<()> {
        self.release_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_release.load(Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "injected transient allocator release error".to_string(),
            ));
        }
        self.inner
            .release(owner, segment, offset, length, allocation_id)
    }
}
#[test]
fn public_put_partial_reservation_release_failure_is_retryable() {
    let fail_release = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let release_calls = Arc::new(AtomicUsize::new(0));
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("writer-prefer-segment"));
    let remote_transport = Arc::new(transport.peer("seg-prefer"));
    remote_transport.add_external_segment("seg-prefer", 64);
    let remote_target_chunks = test_segment_target_chunks(&remote_transport, "seg-prefer");
    let remote_owner = ClientRuntimeId::new("storage-prefer", ClientEpoch(1));
    let remote_allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
    remote_allocator.lock().upsert(&SegmentAnnouncement {
        owner: remote_owner.clone(),
        segment_name: SegmentName::new("seg-prefer"),
        transport_endpoint: None,
        transport_segment_descriptor: None,
        capacity_bytes: 64,
        used_bytes: 0,
        target_chunks: remote_target_chunks.clone(),
        state: SegmentLifecycleState::Active,
        alignment_bytes: 1,
        tags: vec!["dram".to_string()],
    });
    let mut remote_storage_owner =
        test_storage_owner_state(&remote_owner, remote_allocator.clone());
    let observer = ClientLease {
        runtime: remote_owner.clone(),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: test_future_expiry_ms(),
    };
    let remote_live_client_cache = Arc::new(Mutex::new(super::super::LiveClientCache::default()));
    let route_directory = build_route_directory(
        crate::route_directory::RouteDirectoryBuildConfig {
            mode: RouteControlMode::EmbeddedWrh,
            route_topk: 2,
            delete_marker_budget_bytes: 64 * 1024 * 1024,
        },
        metadata.clone(),
        &observer,
        Arc::new(ControlPlaneClient::new().expect("allocator control client should build")),
        remote_live_client_cache.clone(),
        Arc::new(Mutex::new(super::super::SuspectRuntimeCache::default())),
    );
    let owner_state = Arc::get_mut(&mut remote_storage_owner)
        .expect("fixture storage owner must remain uniquely owned during setup");
    owner_state.metadata = metadata.clone();
    owner_state.route_ops = mooncake_store_route::RouteOperations::new(route_directory, observer);

    let remote_control = ControlPlaneHandle::spawn(
        "127.0.0.1",
        Arc::new(LocalAuthorityAdapter {
            lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
            route_write_gate: test_route_write_gate(),
        }),
        Arc::new(PublicRollbackFaultAllocator {
            fail_release: fail_release.clone(),
            release_calls: release_calls.clone(),
            inner: StaticAllocatorAdapter {
                runtime: remote_owner.clone(),
                storage_owner: remote_storage_owner,
                allocator: remote_allocator.clone(),
            },
        }),
        Arc::new(NoopEvictionService),
    )
    .expect("static storage control plane should spawn");
    // Admission supports pending publication; rollback still traverses the
    // fault-injected allocator release RPC.
    let remote_compatibility = CompatibilityDescriptor::default();
    let mut endpoints = ClientEndpointSet {
        rpc_address: "127.0.0.1:0".to_string(),
        segment_name: Some(SegmentName::new("seg-prefer")),
        labels: Default::default(),
    };
    endpoints
        .labels
        .insert("pool".to_string(), "pool-a".to_string());
    endpoints
        .labels
        .insert("storage".to_string(), "false".to_string());
    endpoints
        .labels
        .insert("route".to_string(), "false".to_string());
    endpoints.labels.insert(
        control_address_label().to_string(),
        remote_control.address().to_string(),
    );
    metadata
        .upsert_client_lease(&ClientLease {
            runtime: remote_owner.clone(),
            state: ClientLifecycleState::Active,
            compatibility: remote_compatibility,
            endpoints,
            expires_at_ms: test_future_expiry_ms(),
        })
        .expect("static storage lease should upsert");
    metadata
        .publish_segment(&SegmentAnnouncement {
            owner: remote_owner.clone(),
            segment_name: SegmentName::new("seg-prefer"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 64,
            used_bytes: 0,
            target_chunks: remote_target_chunks,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: vec!["dram".to_string()],
        })
        .expect("static storage segment should publish");

    let client = StoreClientBuilder::new(metadata, "writer-partial-rollback-public")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "true")
        .live_client_sync_interval(Duration::from_secs(60))
        .transport(transport)
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .expect("writer should build");
    client
        .register_local_memory()
        .expect("writer buffers should register");
    super::super::refresh_live_client_cache(
        client.metadata.as_ref(),
        &remote_live_client_cache,
        "public_rollback_fixture_membership_prewarm",
    )
    .expect("allocator route directory must prewarm the shared live membership");
    let policy = ReplicationPolicy::new()
        .replica_count(2)
        .preferred_segments(["seg-prefer", "missing-required-public-rollback-segment"]);
    let result =
        client.put_with_policy("partial-rollback-public-key", b"0123456789abcdef", &policy);
    assert!(
        matches!(result, Err(StoreError::NotFound(_))),
        "public put must hit the missing second preferred segment: {result:?}"
    );
    assert!(
        release_calls.load(Ordering::SeqCst) > 0,
        "production rollback must issue the real allocator RPC"
    );
    assert_eq!(
        remote_allocator.lock().usage_bytes().0,
        16,
        "the injected RPC failure must leave the reservation live"
    );
    fail_release.store(false, Ordering::SeqCst);
    client
        .flush_due_reclaims()
        .expect("public reclaim flush should succeed after recovery");
    assert_eq!(
        remote_allocator.lock().usage_bytes().0,
        0,
        "failed placement rollback must remain retryable through the public flush path"
    );
}
