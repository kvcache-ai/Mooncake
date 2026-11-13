use super::*;

#[test]
fn publication_during_reclaim_snapshot_preserves_public_write_progress() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let mut client = StoreClientBuilder::new(metadata, "snapshot-publication")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::EmbeddedWrh)
        .transport(Arc::new(TestTransport::new("snapshot-publication-segment")))
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
    *directory.bounded_read_hook.lock() = Some(Box::new(move || {
        entered_tx.send(()).expect("signal snapshot captured");
        resume_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("resume scan");
    }));
    let scan_owner = owner.clone();
    let scan = std::thread::spawn(move || scan_owner.reconcile_stale_clock_candidates(None, true));
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
    client.remove("sentinel", true).expect("remove sentinel");
    sleep(Duration::from_millis(120));
    let mut recovered = false;
    for _ in 0..4 {
        if client.put("after-snapshot", &[3; 32]).is_ok() {
            recovered = true;
            break;
        }
        sleep(Duration::from_millis(120));
    }
    assert!(
        recovered,
        "public put must evict the concurrently published target"
    );
    assert!(owner.pending_evictions.lock().is_empty());
}
