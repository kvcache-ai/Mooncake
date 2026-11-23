use super::*;

#[test]
fn review_bounded_gc_advances_wraps_and_keeps_device_scans_independent() {
    let _guard = enable_cold_tier_for_test();
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let live_root = cold_tier_test_root("review-gc-cursor-live");
    let expired_root = cold_tier_test_root("review-gc-cursor-expired");
    let mut live = review_owner(metadata.clone(), "review-gc-cursor-live", &live_root);
    let mut expired = review_owner(metadata.clone(), "review-gc-cursor-expired", &expired_root);
    for key in ["a-live", "z-live"] {
        live.put(key, b"live payload").unwrap();
        wait_for_materialized_cold_backing(&live, key);
    }
    let expired_route = expired.put("b-expired", b"expired payload").unwrap();
    let expired_backing = wait_for_materialized_cold_backing(&expired, "b-expired");
    // Joining both workers settles accounting and leaves reclamation to index GC.
    live.cold_tier.shutdown();
    expired.cold_tier.shutdown();
    for key in ["a-live", "z-live"] {
        live.remove(key, true).unwrap();
    }
    expired.remove("b-expired", true).unwrap();
    let mut lease = expired.lease();
    lease.expires_at_ms = 0;
    metadata.upsert_client_lease(&lease).unwrap();
    let observer = StoreClientBuilder::new(metadata.clone(), "review-gc-cursor-observer")
        .route_control(RouteControlMode::MetadataOnly)
        .build(test_future_expiry_ms())
        .unwrap();
    let owner = &observer.storage_owner;
    let scan = |device| {
        owner
            .garbage_collect_pending_delete_backings_bounded_for_device(device, 1)
            .unwrap()
    };
    let first = scan(None);
    assert_eq!(first.scanned_routes, 1);
    assert_eq!(first.cas_removed_routes, 0);
    // Advancing this device's cursor to z must not advance the global cursor.
    for _ in 0..2 {
        let page = scan(Some("review-gc-cursor-live"));
        assert_eq!(page.scanned_routes, 1);
        assert_eq!(page.cas_removed_routes, 0);
    }
    assert_eq!(scan(None).cas_removed_routes, 1);
    assert!(metadata
        .get_object_route(&expired_route.key)
        .unwrap()
        .is_none());
    assert!(expired
        .storage_owner
        .cold_tier_devices
        .backend_for(&expired_backing)
        .unwrap()
        .get_object(&expired_backing)
        .unwrap()
        .is_none());
    assert!(
        metadata
            .get_cold_tier_device("review-gc-cursor-live")
            .unwrap()
            .unwrap()
            .used_bytes
            > 0
    );

    // Deleted cursor keys remain valid bounds. Revisit the lower key after wrap.
    let mut live_lease = live.lease();
    live_lease.expires_at_ms = 0;
    metadata.upsert_client_lease(&live_lease).unwrap();
    assert_eq!(scan(None).cas_removed_routes, 1); // z-live
    assert_eq!(scan(None).scanned_routes, 0); // end of sweep
    assert_eq!(scan(None).cas_removed_routes, 1); // a-live after wrap
    assert_eq!(
        metadata
            .get_cold_tier_device("review-gc-cursor-live")
            .unwrap()
            .unwrap()
            .used_bytes,
        0
    );
}

fn review_owner(
    metadata: Arc<dyn MetadataBackend>,
    id: &str,
    root: &std::path::Path,
) -> StoreClient {
    let client = StoreClientBuilder::new(metadata, id)
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(Arc::new(TestTransport::new(id)))
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(512)
                .scratch_bytes(4096)
                .alignment(1)
                .reclaim_grace_ms(0)
                .eviction_poll_interval(Duration::ZERO),
        )
        .cold_tier_target(ColdTierTargetConfig::directory(
            id,
            ColdTierKind::Ssd,
            root.to_path_buf(),
        ))
        .build(test_future_expiry_ms())
        .unwrap();
    client.register_local_memory().unwrap();
    client
}

#[test]
// Reproduction supplied in https://github.com/kvcache-ai/Mooncake/pull/5649#issuecomment-6998321851.
fn review_blocked_live_offload_does_not_starve_expired_owner_default_gc() {
    let _guard = enable_cold_tier_for_test();
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let metadata = Arc::new(RecoverableMetadataBackend::new(inner.clone()));
    let root_a = cold_tier_test_root("review-gc-blocked-live");
    let root_b = cold_tier_test_root("review-gc-blocked-expired");
    let mut live = review_owner(metadata.clone(), "review-gc-blocked-live", &root_a);
    let mut expired = review_owner(metadata.clone(), "review-gc-blocked-expired", &root_b);
    let observer = StoreClientBuilder::new(metadata.clone(), "review-gc-blocked-observer")
        .route_control(RouteControlMode::MetadataOnly)
        .build(test_future_expiry_ms())
        .unwrap();

    let expired_route = expired.put("b-expired-delete", b"expired payload").unwrap();
    let expired_backing = wait_for_materialized_cold_backing(&expired, "b-expired-delete");
    expired.cold_tier.shutdown();
    let expired_backend = expired
        .storage_owner
        .cold_tier_devices
        .backend_for(&expired_backing)
        .unwrap();
    let mut live_routes = Vec::new();
    for index in 0..DEFAULT_PENDING_DELETE_GC_BATCH {
        let key = format!("a-live-delete-{index:03}");
        let route = live.put(&key, b"a").unwrap();
        wait_for_materialized_cold_backing(&live, &key);
        live_routes.push((key, route));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let device = inner
            .get_cold_tier_device("review-gc-blocked-live")
            .unwrap()
            .unwrap();
        if device.used_bytes == DEFAULT_PENDING_DELETE_GC_BATCH as u64 && device.reserved_bytes == 0
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "initial live offloads must settle"
        );
        sleep(Duration::from_millis(5));
    }

    // Block the real worker on a later object's metadata settlement, after the
    // first scan page is fully settled and independently safe to reclaim.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *metadata.cold_usage_settlement_gate.lock().unwrap() = Some((entered_tx, release_rx));
    live.put("z-offload-blocker", b"blocked payload").unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let observations = (|| -> mooncake_store_core::Result<_> {
        for (key, _) in &live_routes {
            live.remove(key, true)?;
        }
        expired.remove("b-expired-delete", true)?;
        let mut lease = expired.lease();
        lease.expires_at_ms = 0;
        inner.upsert_client_lease(&lease)?;
        let first_page = inner.list_object_routes_by_cold_backing(
            &mooncake_store_core::ColdBackingRouteFilter {
                limit: Some(DEFAULT_PENDING_DELETE_GC_BATCH),
                ..Default::default()
            },
        )?;
        let setup_valid = first_page.len() == DEFAULT_PENDING_DELETE_GC_BATCH
            && first_page.iter().all(|route| {
                route.state == RouteState::Deleting
                    && live_routes
                        .iter()
                        .any(|(_, live_route)| live_route.key == route.key)
            });
        let mut collected = Vec::new();
        for _ in 0..4 {
            live.prepare_heartbeat(test_future_expiry_ms()).publish()?;
            collected.push(
                observer
                    .storage_owner
                    .garbage_collect_pending_delete_backings()?,
            );
        }
        Ok((
            setup_valid,
            collected,
            inner.get_object_route(&expired_route.key)?.is_none(),
            expired_backend.get_object(&expired_backing)?.is_none(),
            inner.get_client_lease(&live.lease().runtime)?.is_some(),
            inner.get_client_lease(&lease.runtime)?.is_none(),
        ))
    })();
    release_tx.send(()).unwrap();
    live.cold_tier.shutdown();
    let (setup_valid, collected, route_gone, payload_gone, live_lease, expired_lease) =
        observations.unwrap();
    assert!(
        setup_valid,
        "the first default-sized scan page must contain only settled live-owner deletes"
    );
    assert!(
        live_lease && expired_lease,
        "live heartbeat and exact expired owner must remain distinct"
    );
    assert!(route_gone && payload_gone,
        "expired owner remained behind blocked live worker after default GC: collected={collected:?} route_gone={route_gone} payload_gone={payload_gone}");
}
