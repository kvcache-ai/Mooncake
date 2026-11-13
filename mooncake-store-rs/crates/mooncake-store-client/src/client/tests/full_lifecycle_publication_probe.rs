use super::*;

struct DelayedPublicationDirectory {
    inner: Arc<dyn RouteDirectory>,
    entered: Arc<std::sync::Barrier>,
    release: Arc<std::sync::Barrier>,
}

impl RouteDirectory for DelayedPublicationDirectory {
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
        if key.0 == "default::late-publication" && next.is_some() {
            self.entered.wait();
            self.release.wait();
        }
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }
}

#[test]
#[ignore = "Open publication fencing blocker; requires a separate protocol change"]
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
        inner: client.route_directory.clone(),
        entered: entered.clone(),
        release: release.clone(),
    });
    let client = Arc::new(client);
    let writer_client = client.clone();
    let writer =
        std::thread::spawn(move || writer_client.put("late-publication", b"AAAAAAAAAAAAAAAA"));
    entered.wait();
    // The route RPC remains in flight beyond the real reservation deadline.
    // No allocator fields or publication tokens are modified by this probe.
    sleep(Duration::from_millis(400));
    let recovered = client.storage_owner.recover_due_pending_publications();
    let replacement = client.put("replacement", b"BBBBBBBBBBBBBBBB");
    release.wait();
    let original = writer.join().expect("writer should finish");
    recovered.expect("publication recovery should complete");
    replacement.expect("replacement write should complete");
    if original.is_ok() {
        assert_eq!(
            client
                .get("late-publication")
                .expect("successful publication should be readable"),
            b"AAAAAAAAAAAAAAAA",
            "a delayed successful publication must retain its original payload"
        );
    }
}
