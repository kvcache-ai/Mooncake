use super::*;

#[test]
fn rebuild_during_publication_preserves_public_write_progress() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "rebuild-publication")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(Arc::new(TestTransport::new("rebuild-publication-segment")))
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(48)
                .scratch_bytes(4096)
                .alignment(1)
                .eviction_poll_interval(Duration::ZERO),
        )
        .build(test_future_expiry_ms())
        .expect("client should build");
    client
        .register_local_memory()
        .expect("memory should register");
    let sentinel = client.put("sentinel", &[1; 16]).expect("sentinel put");
    let directory = Arc::new(CountingRouteDirectory::new(client.route_directory.clone()));
    let owner = test_storage_owner_state_with_route_directory(
        &client,
        directory.clone(),
        ColdTierOffloadPriorityConfig::default(),
    );
    owner.track_route(&sentinel);
    owner
        .hot_replicas
        .clock
        .lock()
        .mark_hot_keys(std::slice::from_ref(&sentinel.key));
    assert_eq!(
        owner
            .hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&BTreeSet::new()),
        1
    );
    client.storage_owner = owner.clone();
    let client = Arc::new(client);
    sleep(Duration::from_millis(
        STALE_CANDIDATE_RECONCILE_MIN_INTERVAL_MS + 20,
    ));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    *directory.owner_visit_hook.lock() = Some(Box::new(move || {
        entered_tx
            .send(())
            .expect("signal rebuild snapshot captured");
        resume_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("resume scan");
    }));
    let scan_owner = owner.clone();
    let scan = std::thread::spawn(move || scan_owner.rebuild_clock());
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("scan should reach lookup");
    client
        .put("target", &[2; 32])
        .expect("concurrent public put");
    assert_eq!(
        owner
            .hot_replicas
            .clock
            .lock()
            .tracked_replica_count_excluding(&BTreeSet::new()),
        2
    );
    resume_tx.send(()).expect("release scan");
    scan.join()
        .expect("scan should join")
        .expect("reconcile should succeed");
    let (tracked_after_rebuild, sentinel_credit) = {
        let clock = owner.hot_replicas.clock.lock();
        let sentinel_entry = clock
            .entries
            .iter()
            .flatten()
            .find(|entry| entry.id.route_key == sentinel.key)
            .expect("rebuild should retain sentinel");
        (
            clock.tracked_replica_count_excluding(&BTreeSet::new()),
            sentinel_entry.hot_credit,
        )
    };
    client.remove("sentinel", true).expect("remove sentinel");
    sleep(Duration::from_millis(120));
    let mut recovered = false;
    for _ in 0..4 {
        if client.put("after-rebuild", &[3; 32]).is_ok() {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(120));
    }
    assert!(
        recovered,
        "public put must evict the concurrently published target"
    );
    assert_eq!(tracked_after_rebuild, 2);
    assert_eq!(
        sentinel_credit, 2,
        "rebuild must preserve existing read-access credits"
    );
    assert!(owner.pending_evictions.lock().is_empty());
}
