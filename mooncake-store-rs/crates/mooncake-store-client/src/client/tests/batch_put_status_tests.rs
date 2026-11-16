use super::*;

fn routed_status_writer(
    metadata: Arc<dyn MetadataBackend>,
    name: &str,
) -> (StoreClient, StoreClient, Arc<TestTransport>) {
    let storage_transport = Arc::new(TestTransport::new(&format!("{name}-storage-segment")));
    let writer_transport = Arc::new(storage_transport.peer(&format!("{name}-writer-segment")));
    let storage = StoreClientBuilder::new(metadata.clone(), format!("{name}-storage"))
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(storage_transport)
        .local_memory(storage_config_with_bytes(256 * 1024))
        .build(test_future_expiry_ms())
        .expect("storage should build");
    storage
        .register_local_memory()
        .expect("storage should register");
    let writer = StoreClientBuilder::new(metadata.clone(), name)
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(writer_transport.clone())
        .local_memory(storage_config_with_bytes(0).scratch_bytes(64 * 1024))
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .build(test_future_expiry_ms())
        .expect("writer should build");
    writer
        .register_local_memory()
        .expect("writer should register");
    wait_for_membership_convergence(&[&storage, &writer]);
    (storage, writer, writer_transport)
}

#[test]
fn routed_batch_statuses_preserve_mixed_publish_results_without_replay() {
    let metadata = Arc::new(NoHotPathMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let (_storage, mut writer, transport) = routed_status_writer(metadata, "mixed-status");
    let directory = Arc::new(CountingRouteDirectory::new(writer.route_directory.clone()));
    writer.route_directory = directory.clone();
    let first_value = vec![1u8; 4096];
    let mut batch_value = vec![2u8; 4096];
    writer
        .register_buffer(batch_value.as_mut_ptr().cast(), batch_value.len())
        .expect("batch source should register");

    for registered in [false, true] {
        for fresh_first in [false, true] {
            let keys = (0..6)
                .map(|index| format!("mixed-{registered}-{fresh_first}-{index}"))
                .collect::<Vec<_>>();
            for key in &keys[..3] {
                writer
                    .put(key, &first_value)
                    .expect("initial put should succeed");
            }
            let mut ordered_keys = keys.iter().collect::<Vec<_>>();
            if fresh_first {
                ordered_keys.rotate_left(3);
            }
            let cas_before = directory.batch_cas_calls();
            let reads_before = directory.full_reads() + directory.bounded_reads();
            let transfers_before = transport.submitted_batch_bytes().len();
            let statuses = writer.batch_put_from_statuses(
                &ordered_keys
                    .iter()
                    .map(|key| {
                        PutFromRequest::new(key, batch_value.as_ptr().cast(), batch_value.len())
                    })
                    .collect::<Vec<_>>(),
            );
            assert_eq!(statuses.len(), 6);
            for (index, status) in statuses.iter().enumerate() {
                let existing = (index < 3) != fresh_first;
                if existing {
                    assert!(
                        status.is_ok(),
                        "existing routes should remain accepted: {statuses:?}"
                    );
                } else {
                    assert!(status.is_ok(), "{statuses:?}");
                }
            }
            assert_eq!(directory.batch_cas_calls() - cas_before, 1);
            assert_eq!(directory.max_cas_batch_size(), 6);
            assert_eq!(
                directory.full_reads() + directory.bounded_reads() - reads_before,
                0
            );
            assert_eq!(
                &transport.submitted_batch_bytes()[transfers_before..],
                &[6 * 4096]
            );
            for (index, key) in keys.iter().enumerate() {
                let expected = if index < 3 {
                    &first_value
                } else {
                    &batch_value
                };
                assert_eq!(
                    &writer.get(key).expect("written key should be readable"),
                    expected
                );
            }
        }
    }
    writer
        .unregister_buffer(batch_value.as_mut_ptr().cast(), batch_value.len())
        .expect("batch source should unregister");
}

struct BatchReplyFaultRouteDirectory {
    inner: Arc<dyn RouteDirectory>,
    key: ObjectKey,
    version_retry: bool,
    calls: AtomicUsize,
}

impl RouteDirectory for BatchReplyFaultRouteDirectory {
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
        requests: &[RouteCasRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<mooncake_store_core::CasResult>>>
    {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.version_retry && call > 0 {
            self.inner
                .compare_and_swap_object_routes(observer, requests)?;
            return Err(StoreError::Transport(
                "injected version retry reply loss".to_string(),
            ));
        }
        Ok(requests
            .iter()
            .map(|request| {
                if request.key == self.key && self.version_retry {
                    Ok(mooncake_store_core::CasResult {
                        applied: false,
                        current: None,
                        delete_version: None,
                        version_floor: Some(request.next.as_ref().unwrap().version.next()),
                    })
                } else {
                    let result = self.inner.compare_and_swap_object_route(
                        observer,
                        &request.key,
                        request.expected,
                        request.next.as_ref(),
                    );
                    if request.key == self.key {
                        result?;
                        Err(StoreError::Transport(
                            "injected item reply loss".to_string(),
                        ))
                    } else {
                        result
                    }
                }
            })
            .collect())
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
}

fn assert_pending_publication_retains_allocation(storage: &StoreClient, route: &ObjectRoute) {
    let replica = route
        .replicas
        .first()
        .expect("published route should keep its replica");
    assert_eq!(replica.owner, *storage.runtime_id());
    let pending = PendingPublication {
        identity: AllocationIdentity {
            segment_name: replica.segment_name.clone(),
            offset_bytes: replica.segment_offset,
            length_bytes: replica.length,
            allocation_id: replica
                .allocation_id
                .expect("lifecycle route needs allocation id"),
        },
    };
    let allocation = AllocationSpan {
        segment_name: pending.identity.segment_name.clone(),
        offset_bytes: pending.identity.offset_bytes,
        length_bytes: pending.identity.length_bytes,
    };
    let allocator = storage.allocator.lock();
    assert_eq!(
        allocator.allocation_identity(&allocation),
        Some(pending.identity.clone())
    );
    assert!(
        matches!(
            allocator.pending_allocations.get(&allocation),
            Some(PendingAllocation::Publication { allocation_id, .. })
                if *allocation_id == pending.identity.allocation_id
        ),
        "unknown publication must retain its exact pending allocation"
    );
}

#[test]
fn routed_batch_statuses_preserve_success_when_another_publish_reply_is_lost() {
    for version_retry in [false, true] {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let name = format!("reply-status-{version_retry}");
        let (storage, mut writer, transport) = routed_status_writer(metadata, &name);
        let directory = Arc::new(BatchReplyFaultRouteDirectory {
            inner: writer.route_directory.clone(),
            key: ObjectKey::from_logical_id(&LogicalObjectId::new(
                NamespaceScope::default(),
                "unknown",
            )),
            version_retry,
            calls: AtomicUsize::new(0),
        });
        writer.route_directory = directory.clone();
        let statuses = registered_batch_statuses(
            &writer,
            &[
                PutRequest::new("success", b"first"),
                PutRequest::new("unknown", b"second"),
            ],
        );
        assert_eq!(statuses.len(), 2);
        assert!(statuses[0].is_ok(), "{statuses:?}");
        assert!(
            matches!(&statuses[1], Err(StoreError::Transport(_))),
            "{statuses:?}"
        );
        assert_eq!(
            directory.calls.load(Ordering::SeqCst),
            if version_retry { 2 } else { 1 }
        );
        assert_eq!(transport.submitted_batch_bytes(), vec![11]);
        let unknown_route = writer
            .query_route("unknown")
            .expect("unknown route lookup should succeed")
            .expect("unknown publication should have applied");
        assert_pending_publication_retains_allocation(&storage, &unknown_route);
        wait_for_storage_clock_route(&storage, statuses[0].as_ref().unwrap(), false);
        assert_eq!(
            writer.get("success").expect("successful value should read"),
            b"first"
        );
        assert_eq!(
            writer
                .get("unknown")
                .expect("applied value should remain allocated"),
            b"second"
        );
    }
}

#[test]
fn routed_batch_statuses_retain_unknown_batch_outcomes_without_replay() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let (storage, mut writer, transport) = routed_status_writer(metadata, "unknown-batch-status");
    let counter = Arc::new(CountingRouteDirectory::new(writer.route_directory.clone()));
    writer.route_directory = Arc::new(ApplyBatchThenFailRouteDirectory {
        inner: counter.clone(),
        fail_once: AtomicBool::new(true),
    });
    let statuses = registered_batch_statuses(
        &writer,
        &[
            PutRequest::new("first", b"first"),
            PutRequest::new("second", b"second"),
        ],
    );
    assert_eq!(statuses.len(), 2);
    assert!(
        statuses
            .iter()
            .all(|status| matches!(status, Err(StoreError::Transport(_)))),
        "{statuses:?}"
    );
    assert_eq!(counter.batch_cas_calls(), 1);
    assert_eq!(transport.submitted_batch_bytes(), vec![11]);
    for key in ["first", "second"] {
        let route = writer
            .query_route(key)
            .expect("unknown route lookup should succeed")
            .expect("unknown publication should have applied");
        assert_pending_publication_retains_allocation(&storage, &route);
    }
    assert_eq!(
        writer
            .get("first")
            .expect("first applied value should read"),
        b"first"
    );
    assert_eq!(
        writer
            .get("second")
            .expect("second applied value should read"),
        b"second"
    );
}

#[test]
fn routed_batch_statuses_preserve_pending_quota_after_finalize_failure() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let scope = TenantPolicyScope::new("default", None::<String>, None::<String>);
    inner
        .put_tenant_policy(
            &TenantPolicy {
                scope: scope.clone(),
                spec: TenantPolicySpec {
                    quota: Some(TenantQuotaPolicy {
                        max_bytes: Some(16),
                        max_objects: Some(2),
                    }),
                    ..TenantPolicySpec::default()
                },
                version: 1,
                updated_at_ms: 10,
                updated_by: "admin".to_string(),
            },
            None,
        )
        .expect("quota policy should store");
    let metadata = Arc::new(FinalizeFailureMetadataBackend::new(
        inner.clone(),
        "quota-status:1:default:1",
    ));
    let (_storage, mut writer, _) = routed_status_writer(metadata, "quota-status");
    let directory = Arc::new(CountingRouteDirectory::new(writer.route_directory.clone()));
    writer.route_directory = directory.clone();
    let statuses = registered_batch_statuses(
        &writer,
        &[
            PutRequest::new("a-failed", b"1234"),
            PutRequest::new("b-success", b"5678"),
        ],
    );
    assert_eq!(statuses.len(), 2);
    assert!(
        matches!(&statuses[0], Err(StoreError::InvalidState(message)) if message.contains("injected finalize failure")),
        "{statuses:?}"
    );
    assert!(statuses[1].is_ok(), "{statuses:?}");
    assert_eq!(directory.batch_cas_calls(), 1);
    let quota = inner
        .get_tenant_quota_state(&scope)
        .expect("quota should load")
        .expect("quota should exist");
    assert_eq!(quota.used_bytes, 4);
    assert_eq!(quota.used_objects, 1);
    assert_eq!(quota.pending_reserved_bytes, 4);
    assert_eq!(quota.pending_reserved_objects, 1);
    assert!(writer
        .query_route("a-failed")
        .expect("published route lookup should succeed")
        .is_some());
    assert_eq!(
        writer
            .get("b-success")
            .expect("successful value should read"),
        b"5678"
    );
}

// The community status API consumes registered buffers.
fn registered_batch_statuses(
    writer: &StoreClient,
    requests: &[PutRequest<'_>],
) -> Vec<Result<ObjectRoute>> {
    let regions = requests
        .iter()
        .map(|request| (request.value.as_ptr() as usize, request.value.len()))
        .collect::<BTreeSet<_>>();
    for (address, length) in &regions {
        writer
            .register_buffer(*address as *mut std::ffi::c_void, *length)
            .expect("status source should register");
    }
    let from = requests
        .iter()
        .map(|request| PutFromRequest {
            tenant: request.tenant,
            domain: request.domain,
            object_set: request.object_set,
            qos_tier: request.qos_tier,
            key: request.key,
            buffer: request.value.as_ptr().cast(),
            size: request.value.len(),
            policy: request.policy.clone(),
        })
        .collect::<Vec<_>>();
    let statuses = writer.batch_put_from_statuses(&from);
    for (address, length) in regions {
        writer
            .unregister_buffer(address as *mut std::ffi::c_void, length)
            .expect("status source should unregister");
    }
    statuses
}
