use super::*;
#[test]
fn publication_primary_loss_public_paths_probe() {
    let metadata = Arc::new(RecoverableMetadataBackend::new(Arc::new(
        InMemoryMetadataBackend::new(),
    )));
    let transport = Arc::new(TestTransport::new("pub-loss-store-segment"));
    let store = StoreClientBuilder::new(metadata.clone(), "pub-loss-store")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .label("route", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(transport.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .unwrap();
    let authority = StoreClientBuilder::new(metadata.clone(), "pub-loss-authority")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route", "true")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(transport.peer("pub-loss-authority-segment")))
        .local_memory(rw_only_config())
        .build(test_future_expiry_ms())
        .unwrap();
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(metadata.clone(), "pub-loss-writer")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .label("route", "false")
        .live_client_sync_interval(Duration::ZERO)
        .transport(Arc::new(transport.peer("pub-loss-writer-segment")))
        .local_memory(rw_only_config())
        .routed_writes(planner, 1)
        .build(test_future_expiry_ms())
        .unwrap();
    store.register_local_memory().unwrap();
    authority.register_local_memory().unwrap();
    writer.register_local_memory().unwrap();
    wait_for_membership_convergence(&[&store, &authority, &writer]);
    let mut chosen = None;
    for index in 0..64 {
        let key = format!("pub-loss-key-{index}");
        let route = writer
            .put_with_policy(
                &key,
                b"healthy-owner-payload",
                &ReplicationPolicy::new()
                    .prefer_local(false)
                    .preferred_storage_owners([store.runtime_id().storage_key()]),
            )
            .unwrap();
        let attempt = route.replicas[0]
            .publication
            .as_ref()
            .expect("real PUT must carry provenance");
        if attempt.primary == *authority.runtime_id() {
            chosen = Some((key, route));
            break;
        }
    }
    let (key, route) = chosen.expect("must select independent authority primary");
    let namespace = metadata.route_namespace();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if authority_get(&namespace, &store.runtime_id().stable_id, &route.key)
            .unwrap()
            .is_some()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "healthy mirror must exist before failure"
        );
        sleep(Duration::from_millis(5));
    }
    let primary = authority.runtime_id().clone();
    metadata.hide_runtime_metadata(&primary);
    drop(authority);
    writer.live_clients_snapshot(true).unwrap();
    store.live_clients_snapshot(true).unwrap();
    let replica = &route.replicas[0];
    let release = writer.release_segment_allocations_batch(&[AllocationReleaseRequest {
        storage_runtime: replica.owner.clone(),
        segment_name: replica.segment_name.clone(),
        offset_bytes: replica.segment_offset,
        length_bytes: replica.length,
        allocation_id: replica.allocation_id.unwrap(),
    }]);
    eprintln!("PRIMARY_LOSS_RELEASE={release:?}");
    assert!(matches!(release, Err(StoreError::Backpressure(_))),
        "an unavailable publication authority must retain the allocation without reporting its healthy owner as unreachable: {release:?}");
    assert!(!writer.runtime_is_suspect(store.runtime_id()));
    let query = writer.query_route(&key);
    eprintln!("PRIMARY_LOSS_QUERY={query:?}");
    assert!(query.as_ref().unwrap().is_some(), "healthy mirror query");
    let read = writer.get(&key);
    eprintln!("PRIMARY_LOSS_GET={read:?}");
    assert_eq!(read.unwrap(), b"healthy-owner-payload");
    let current = writer.query_route(&key).unwrap().unwrap();
    let mut next = current.clone();
    next.version = current.version.next();
    next.replicas[0].priority = 7;
    let mutation = writer.cas_route(&key, Some(current.version), Some(&next));
    eprintln!("PRIMARY_LOSS_METADATA_MUTATION={mutation:?}");
    assert!(
        mutation.unwrap().applied,
        "retained allocation mutation should use healthy mirror"
    );
    assert_eq!(writer.get(&key).unwrap(), b"healthy-owner-payload");
    let delete = writer.remove(&key, false);
    eprintln!("PRIMARY_LOSS_DELETE={delete:?}");
    assert!(
        delete.is_ok(),
        "public delete should remain available via healthy mirror"
    );
    assert!(writer.query_route(&key).unwrap().is_none());
}
