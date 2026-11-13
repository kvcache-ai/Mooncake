use super::*;

struct DelayedPublicationDirectory {
    delayed: AtomicBool,
    inner: Arc<dyn RouteDirectory>,
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

impl RouteDirectory for DelayedPublicationDirectory {
    fn invalidate_publication(&self, attempt: &mooncake_store_core::PublicationAttempt) {
        self.inner.invalidate_publication(attempt)
    }
    fn publication_fencing_enabled(&self) -> bool {
        self.inner.publication_fencing_enabled()
    }

    fn prepare_publication(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        sequence: u64,
    ) -> mooncake_store_core::Result<mooncake_store_core::PublicationAttempt> {
        self.inner.prepare_publication(observer, key, sequence)
    }

    fn publication_receivers(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Vec<ClientRuntimeId>> {
        self.inner.publication_receivers(observer, key)
    }

    fn revoke_publications(
        &self,
        observer: &ClientLease,
        requests: &[mooncake_store_core::PublicationRevokeRequest],
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::PublicationRevokeReply>> {
        self.inner.revoke_publications(observer, requests)
    }

    fn finalize_publications(
        &self,
        observer: &ClientLease,
        requests: &[mooncake_store_core::PublicationRevokeRequest],
        released: bool,
    ) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<()>>> {
        self.inner
            .finalize_publications(observer, requests, released)
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
        if key.0 == "default::late-publication"
            && next.is_some()
            && !self.delayed.swap(true, Ordering::SeqCst)
        {
            self.entered.wait();
            self.release.wait();
        }
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }
}

#[test]
fn late_publication_after_real_pending_deadline_preserves_payload() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("late-publication-segment"));
    let mut client = StoreClientBuilder::new(metadata, "late-publication-owner")
        .state(ClientLifecycleState::Active)
        .request_timeout(Duration::from_millis(25))
        .transport(transport)
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(64)
                .scratch_bytes(4096)
                .alignment(1)
                .eviction_poll_interval(Duration::ZERO),
        )
        .build(test_future_expiry_ms())
        .expect("client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    client.route_directory = Arc::new(DelayedPublicationDirectory {
        delayed: AtomicBool::new(false),
        inner: client.route_directory.clone(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let client = Arc::new(client);
    let writer_client = client.clone();
    let writer =
        std::thread::spawn(move || writer_client.put("late-publication", b"AAAAAAAAAAAAAAAA"));
    entered.wait();
    let (original_identity, original_attempt) = {
        let allocator = client.allocator.lock();
        assert_eq!(allocator.publication_provenance.len(), 1);
        allocator
            .publication_provenance
            .iter()
            .next()
            .map(|(identity, attempt)| (identity.clone(), attempt.clone()))
            .expect("real reservation must carry publication provenance")
    };
    assert_eq!(original_attempt.key.0, "default::late-publication");
    // The route RPC remains in flight beyond the real reservation deadline.
    // No allocator fields or publication tokens are modified by this probe.
    sleep(Duration::from_millis(400));
    let recovered = client.storage_owner.recover_due_pending_publications();
    let replacement = client.put("replacement", b"BBBBBBBBBBBBBBBB");
    release.wait();
    let original = writer.join().expect("writer should finish");
    assert_eq!(recovered.expect("publication recovery should complete"), 1);
    let replacement = replacement.expect("replacement write should complete");
    assert_eq!(
        replacement.replicas[0].segment_offset, original_identity.offset_bytes,
        "the proof must permit actual reuse of the revoked allocation"
    );
    assert_ne!(
        replacement.replicas[0].allocation_id,
        Some(original_identity.allocation_id)
    );
    if original.is_ok() {
        let published = original.as_ref().unwrap();
        assert_ne!(
            published.replicas[0].allocation_id,
            Some(original_identity.allocation_id)
        );
        assert_ne!(
            published.replicas[0].publication.as_ref(),
            Some(&original_attempt)
        );
        assert_eq!(
            client
                .get("late-publication")
                .expect("successful publication should be readable"),
            b"AAAAAAAAAAAAAAAA",
            "a delayed successful publication must retain its original payload"
        );
    } else {
        assert!(
            matches!(original, Err(StoreError::PublicationRetired(_))),
            "a delayed revoked CAS must fail with the terminal publication error: {original:?}"
        );
    }
    client
        .storage_owner
        .recover_due_pending_publications()
        .expect("recovery should drain");
    assert_eq!(client.allocator.lock().pending_publication_count, 0);
    assert!(
        client.state.lock().pending_reclaims.is_empty(),
        "a stale retired allocation must not remain in the reclaim queue"
    );
}

#[test]
fn inflight_lower_sequence_survives_higher_sequence_reclaim_probe() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "sequence-floor-probe-owner")
        .state(ClientLifecycleState::Active)
        .transport(Arc::new(TestTransport::new("sequence-floor-probe-segment")))
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(128)
                .scratch_bytes(4096)
                .alignment(1)
                .reclaim_grace_ms(0)
                .eviction_poll_interval(Duration::ZERO),
        )
        .build(test_future_expiry_ms())
        .unwrap();
    client.register_local_memory().unwrap();
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    client.route_directory = Arc::new(DelayedPublicationDirectory {
        delayed: AtomicBool::new(false),
        inner: client.route_directory.clone(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let client = Arc::new(client);
    let writer_client = client.clone();
    let writer =
        std::thread::spawn(move || writer_client.put("late-publication", b"AAAAAAAAAAAAAAAA"));
    entered.wait();
    let (first_identity, first_sequence, deadline_ms) = {
        let allocator = client.allocator.lock();
        let (identity, attempt) = allocator.publication_provenance.iter().next().unwrap();
        let deadline_ms = allocator
            .pending_allocations
            .values()
            .find_map(|pending| match pending {
                super::super::PendingAllocation::Publication {
                    allocation_id,
                    deadline_ms,
                    ..
                } if *allocation_id == identity.allocation_id => Some(*deadline_ms),
                _ => None,
            })
            .unwrap();
        (identity.clone(), attempt.sequence, deadline_ms)
    };
    let later = client.put("later-sequence", b"BBBBBBBBBBBBBBBB").unwrap();
    let later_sequence = later.replicas[0].publication.as_ref().unwrap().sequence;
    assert!(later_sequence > first_sequence);
    let later_id = later.replicas[0].allocation_id.unwrap();
    client.remove("later-sequence", false).unwrap();
    assert_eq!(client.storage_owner.process_pending_deletes(64).unwrap(), 1);
    client
        .storage_owner
        .recover_due_pending_publications()
        .unwrap();
    {
        let allocator = client.allocator.lock();
        assert!(!allocator
            .publication_provenance
            .keys()
            .any(|identity| identity.allocation_id == later_id));
        assert!(allocator
            .publication_provenance
            .contains_key(&first_identity));
    }
    let before_release_ms = now_ms();
    release.wait();
    let first = writer
        .join()
        .unwrap()
        .expect("a retired in-flight attempt must retry from reserve/write");
    assert!(
        before_release_ms < deadline_ms,
        "the normal reservation deadline must not expire"
    );
    assert_ne!(
        first.replicas[0].allocation_id,
        Some(first_identity.allocation_id)
    );
    assert!(first.replicas[0].publication.as_ref().unwrap().sequence > later_sequence);
    assert_eq!(client.get("late-publication").unwrap(), b"AAAAAAAAAAAAAAAA");
    assert_eq!(client.allocator.lock().pending_publication_count, 0);
}
