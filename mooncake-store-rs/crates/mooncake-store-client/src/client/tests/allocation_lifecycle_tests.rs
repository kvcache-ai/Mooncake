use super::*;

#[test]
#[ignore = "known late-publication race: expired absence does not fence a delayed CAS"]
fn expired_pending_publication_cannot_publish_after_owner_reuses_span() {
    struct BeforePublishGate {
        inner: Arc<dyn RouteDirectory>,
        gate: Arc<(StdMutex<(bool, bool)>, Condvar)>,
    }
    impl RouteDirectory for BeforePublishGate {
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
            let (lock, changed) = &*self.gate;
            let mut state = lock.lock().expect("gate lock should succeed");
            state.0 = true;
            changed.notify_all();
            while !state.1 {
                state = changed.wait(state).expect("gate wait should succeed");
            }
            drop(state);
            self.inner
                .compare_and_swap_object_route(observer, key, expected, next)
        }

        fn list_routes_by_replica_owner(
            &self,
            observer: &ClientLease,
            owner: &ClientRuntimeId,
        ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
            self.inner.list_routes_by_replica_owner(observer, owner)
        }

        fn metadata(&self) -> &dyn MetadataBackend {
            self.inner.metadata()
        }
    }

    let _metrics_guard = metrics_test_lock().lock();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("late-publication-owner-segment"));
    let owner = StoreClientBuilder::new(metadata.clone(), "late-publication-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .request_timeout(Duration::from_millis(100))
        .transport(transport.clone())
        .local_memory(storage_config_with_bytes(16))
        .build(test_future_expiry_ms())
        .expect("owner should build");
    owner
        .register_local_memory()
        .expect("owner should register");
    let mut writer = StoreClientBuilder::new(metadata, "late-publication-writer")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .route_control(RouteControlMode::EmbeddedWrh)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .request_timeout(Duration::from_secs(5))
        .transport(Arc::new(transport.peer("late-publication-writer-segment")))
        .local_memory(storage_config_with_bytes(0))
        .build(test_future_expiry_ms())
        .expect("writer should build");
    writer
        .register_local_memory()
        .expect("writer should register");
    wait_for_membership_convergence(&[&owner, &writer]);
    let gate = Arc::new((StdMutex::new((false, false)), Condvar::new()));
    writer.route_directory = Arc::new(BeforePublishGate {
        inner: writer.route_directory.clone(),
        gate: gate.clone(),
    });
    let writer = Arc::new(writer);
    let task_writer = writer.clone();
    let pending_write =
        std::thread::spawn(move || task_writer.put("late-publication", b"0123456789abcdef"));
    let (lock, changed) = &*gate;
    let mut state = lock.lock().expect("gate lock should succeed");
    while !state.0 {
        let result = changed
            .wait_timeout(state, Duration::from_secs(2))
            .expect("writer should reach publication");
        state = result.0;
        if result.1.timed_out() {
            state.1 = true;
            changed.notify_all();
            panic!("writer did not reach publication CAS");
        }
    }
    drop(state);
    let publication_deadline = owner
        .allocator
        .lock()
        .pending_publication_ready
        .keys()
        .next()
        .copied()
        .expect("remote reserve must create a publication fence");
    sleep(Duration::from_millis(
        publication_deadline.saturating_sub(now_ms()) + 20,
    ));
    let recovered = owner.storage_owner.recover_due_pending_publications();
    let replacement = owner.put("late-publication-replacement", b"abcdefghijklmnop");
    {
        let mut state = lock.lock().expect("gate lock should succeed");
        state.1 = true;
        changed.notify_all();
    }
    let published = pending_write.join().expect("writer should finish");
    recovered.expect("recovery should run");
    if replacement.is_ok() {
        assert_eq!(
            owner
                .get("late-publication-replacement")
                .expect("replacement must remain readable"),
            b"abcdefghijklmnop",
        );
    }
    if let Ok(published) = published {
        if let Ok(replacement) = replacement {
            assert_ne!(
                published.replicas[0].allocation_id,
                replacement.replicas[0].allocation_id,
            );
            assert_eq!(
                published.replicas[0].segment_offset, replacement.replicas[0].segment_offset,
                "test must exercise actual physical span reuse",
            );
        }
        assert_eq!(
            writer
                .get("late-publication")
                .expect("a successful put must remain readable"),
            b"0123456789abcdef",
            "expired publication must not publish an allocation already reused by the owner",
        );
    }
}

#[test]
fn explicit_move_lost_cas_reply_retains_published_target_allocation() {
    let _metrics_guard = metrics_test_lock().lock();
    use super::super::{ExplicitMigrationMode, ExplicitMigrationPlan, ReplicaReadSelector};

    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("move-lost-reply-source-segment"));
    let mut compatibility = CompatibilityDescriptor::default();
    compatibility
        .capabilities
        .insert(mooncake_store_core::ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY.to_string());
    let build = |name: &str, transport: Arc<TestTransport>, storage: &str| {
        StoreClientBuilder::new(metadata.clone(), name)
            .state(ClientLifecycleState::Active)
            .label("storage", storage)
            .route_control(RouteControlMode::EmbeddedWrh)
            .compatibility(compatibility.clone())
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(transport)
            .local_memory(storage_config_with_bytes(64))
            .build(test_future_expiry_ms())
            .expect("client should build")
    };
    let source = build("move-lost-reply-source", transport.clone(), "true");
    source
        .register_local_memory()
        .expect("source memory should register");
    let key = "move-lost-reply-key";
    let payload = b"0123456789abcdef";
    let original = source.put(key, payload).expect("seed write should succeed");
    let target = build(
        "move-lost-reply-target",
        Arc::new(transport.peer("move-lost-reply-target-segment")),
        "true",
    );
    let mut coordinator = build(
        "move-lost-reply-coordinator",
        Arc::new(transport.peer("move-lost-reply-coordinator-segment")),
        "false",
    );
    target
        .register_local_memory()
        .expect("target memory should register");
    coordinator
        .register_local_memory()
        .expect("coordinator memory should register");
    wait_for_membership_convergence(&[&source, &target, &coordinator]);
    coordinator.route_directory = Arc::new(AppliedReplyFaultRouteDirectory::after_apply(
        coordinator.route_directory.clone(),
        original.key.clone(),
    ));
    let object = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(coordinator.default_tenant()), None, None),
        key,
    );
    let error = coordinator
        .execute_explicit_route_migration(
            &object,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Move,
                source: ReplicaReadSelector::Segment(original.replicas[0].segment_name.clone()),
                target_segments: vec![target.segment_name().expect("target segment should exist")],
                all_or_nothing: true,
            },
        )
        .expect_err("the applied CAS reply should be lost");
    assert!(error.to_string().contains("injected route cas failure"));
    let published = target
        .query_route(key)
        .expect("route lookup should succeed")
        .expect("the CAS must have published the move");
    assert_eq!(published.replicas.len(), 1);
    assert_eq!(published.replicas[0].owner, *target.runtime_id());
    assert_eq!(
        target.allocator.lock().usage_bytes().0,
        payload.len() as u64
    );
    assert_eq!(
        coordinator
            .get(key)
            .expect("published target must remain readable"),
        payload
    );
    {
        let mut allocator = target.allocator.lock();
        assert!(!allocator.pending_allocations(now_ms()).is_empty());
        let replica = &published.replicas[0];
        allocator.mark_pending_publication(
            &mooncake_store_core::SegmentReservation {
                owner: target.runtime_id().clone(),
                segment_name: replica.segment_name.clone(),
                offset_bytes: replica.segment_offset,
                length_bytes: replica.length,
                allocation_id: replica
                    .allocation_id
                    .expect("target must carry its generation"),
            },
            0,
        );
    }
    assert_eq!(
        target
            .storage_owner
            .recover_due_pending_publications()
            .expect("owner recovery should confirm the committed move"),
        0
    );
    assert!(target
        .allocator
        .lock()
        .pending_allocations(now_ms())
        .is_empty());
    target
        .put("move-lost-reply-next", b"fedcba9876543210")
        .expect("another write should use independent capacity");
    assert_eq!(
        coordinator
            .get(key)
            .expect("later writes must preserve moved bytes"),
        payload
    );
}

#[test]
fn metadata_only_does_not_advertise_allocation_lifecycle() {
    let _metrics_guard = metrics_test_lock().lock();
    let client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "metadata-only-allocation-lifecycle",
    )
    .state(ClientLifecycleState::Active)
    .route_control(RouteControlMode::MetadataOnly)
    .transport(Arc::new(TestTransport::new(
        "metadata-only-lifecycle-segment",
    )))
    .local_memory(storage_config_with_bytes(32))
    .build(test_future_expiry_ms())
    .expect("metadata-only client should build");
    assert!(!client
        .lease()
        .compatibility
        .supports(mooncake_store_core::ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY,));
    client
        .register_local_memory()
        .expect("memory should register");
    let reservation = client
        .allocator
        .lock()
        .reserve_any_with_pending_publication(client.runtime_id(), 16, 0)
        .expect("a pre-existing publication fence should be representable");
    assert_eq!(
        client
            .storage_owner
            .recover_due_pending_publications()
            .expect("unsupported exact confirmation should defer recovery"),
        0
    );
    assert_eq!(
        client.allocator.lock().usage_bytes().0,
        reservation.length_bytes
    );
    assert!(
        !client
            .allocator
            .lock()
            .pending_allocations(now_ms())
            .is_empty(),
        "an unavailable authority must retain the recovery fence"
    );
}

#[test]
fn forced_stale_candidate_confirms_live_allocation_without_unbounded_route_reads() {
    let _metrics_guard = metrics_test_lock().lock();
    let client = StoreClientBuilder::new(
        Arc::new(InMemoryMetadataBackend::new()),
        "forced-stale-no-owner-scan",
    )
    .state(ClientLifecycleState::Active)
    .label("storage", "true")
    .route_control(RouteControlMode::EmbeddedWrh)
    .transport(Arc::new(TestTransport::new(
        "forced-stale-no-owner-scan-segment",
    )))
    .local_memory(storage_config_with_bytes(32))
    .build(test_future_expiry_ms())
    .expect("client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    let route = client
        .put("forced-stale-no-owner-scan", b"0123456789abcdef")
        .expect("put should succeed");
    wait_for_storage_clock_route(&client, &route, false);
    client.storage_owner.hot_replicas.remove_key(&route.key);
    let directory = Arc::new(CountingRouteDirectory::new(client.route_directory.clone()));
    let owner = test_storage_owner_state_with_route_directory(
        &client,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    owner.track_route(&route);
    assert!(!owner
        .hot_replicas
        .clock
        .lock()
        .allocation_spans_for_key(&route.key)
        .is_empty());
    owner
        .stale_candidate_reconcile_completed_at_ms
        .store(0, Ordering::Relaxed);
    assert_eq!(
        owner
            .reconcile_stale_clock_candidates(None, true)
            .expect("forced candidate reclaim should confirm the live identity"),
        0
    );
    assert_eq!(client.allocator.lock().usage_bytes().0, 16);
    assert_eq!(directory.full_reads(), 0);
    assert!(
        directory.bounded_reads() > 0,
        "the nonempty CLOCK candidate must reach exact route confirmation"
    );
    assert_eq!(
        client
            .get("forced-stale-no-owner-scan")
            .expect("live bytes must survive"),
        b"0123456789abcdef"
    );
}

struct AppliedReplyFaultRouteDirectory {
    inner: Arc<dyn RouteDirectory>,
    fail_key: ObjectKey,
    apply_before_error: bool,
}

impl AppliedReplyFaultRouteDirectory {
    fn after_apply(inner: Arc<dyn RouteDirectory>, fail_key: ObjectKey) -> Self {
        Self {
            inner,
            fail_key,
            apply_before_error: true,
        }
    }
}

impl RouteDirectory for AppliedReplyFaultRouteDirectory {
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
    ) -> mooncake_store_core::Result<CasResult> {
        if *key == self.fail_key {
            if self.apply_before_error {
                self.inner
                    .compare_and_swap_object_route(observer, key, expected, next)?;
            }
            return Err(StoreError::Transport(
                "injected route cas failure".to_string(),
            ));
        }
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<CasResult>>> {
        Ok(requests
            .iter()
            .map(|request| {
                self.compare_and_swap_object_route(
                    observer,
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                )
            })
            .collect::<Vec<_>>())
    }

    fn list_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        self.inner.list_routes_by_replica_owner(observer, owner)
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
}
