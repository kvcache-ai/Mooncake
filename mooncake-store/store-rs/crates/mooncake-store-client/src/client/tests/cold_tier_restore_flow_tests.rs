#[allow(unused_imports)]
use super::*;

struct ColdTierEnvGuard {
    previous: Option<String>,
}

impl Drop for ColdTierEnvGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => std::env::set_var("MC_STORE_RS_ENABLE_COLD_TIER", value),
            None => std::env::remove_var("MC_STORE_RS_ENABLE_COLD_TIER"),
        }
    }
}

fn with_cold_tier_enabled<T>(f: impl FnOnce() -> T) -> T {
    let _metrics_guard = metrics_test_lock().lock();
    let _env_guard = ColdTierEnvGuard {
        previous: std::env::var("MC_STORE_RS_ENABLE_COLD_TIER").ok(),
    };
    std::env::set_var("MC_STORE_RS_ENABLE_COLD_TIER", "1");
    f()
}

fn evict_until_cold_only(client: &StoreClient, key: &str) -> ObjectRoute {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let route = client
            .query_route(key)
            .expect("route query should succeed")
            .expect("route should exist");
        if route.replicas.is_empty()
            && route.cold_backing.as_ref().is_some_and(|backing| {
                backing.state == mooncake_store_core::ColdBackingState::Materialized
            })
        {
            return route;
        }
        assert!(
            Instant::now() < deadline,
            "object should be evicted to cold-only route in time: route={route:?}"
        );
        assert!(
            client
                .storage_owner
                .evict_one_blocking(None)
                .expect("manual eviction should succeed"),
            "manual eviction should find a hot victim"
        );
    }
}

#[test]
fn embedded_client_put_get_reads_cold_only_payload_from_disk_with_second_client() {
    with_cold_tier_enabled(|| {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let cold_root = cold_tier_test_root("embedded-put-get-cold-disk");
        let transport = Arc::new(TestTransport::new("embedded-put-get-cold-disk-owner-segment"));
        let owner_transport_factory = transport.factory();
        let embedded = StoreClientBuilder::new(metadata.clone(), "embedded-put-get-cold-disk-owner")
            .state(ClientLifecycleState::Active)
            .label("storage", "true")
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(transport.clone())
            .transport_factory(owner_transport_factory)
            .local_memory(storage_config_with_bytes(128))
            .cold_tier_target(cold_tier_test_config_with_root(
                "embedded-put-get-cold-disk",
                cold_root.clone(),
            ))
            .build(test_future_expiry_ms())
            .expect("embedded owner build should succeed");
        embedded
            .register_local_memory()
            .expect("embedded owner local memory should register");

        let peer_transport = Arc::new(transport.peer("embedded-put-get-cold-disk-peer-segment"));
        let peer = StoreClientBuilder::new(metadata, "embedded-put-get-cold-disk-peer")
            .state(ClientLifecycleState::Active)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(peer_transport)
            .local_memory(storage_config_with_bytes(128))
            .build(test_future_expiry_ms())
            .expect("second client build should succeed");
        peer.register_local_memory()
            .expect("second client local memory should register");

        let payload = b"embedded-put-get-cold-disk-payload";
        embedded
            .put("embedded-put-get-cold-disk-key", payload)
            .expect("embedded client put should succeed");
        let cold_backing = wait_for_materialized_cold_backing(
            &embedded,
            "embedded-put-get-cold-disk-key",
        );
        assert_eq!(
            cold_backing.state,
            mooncake_store_core::ColdBackingState::Materialized
        );
        let cold_only_route = evict_until_cold_only(&embedded, "embedded-put-get-cold-disk-key");
        assert!(
            cold_only_route.replicas.is_empty(),
            "test must evict to a disk-backed cold-only route before get"
        );
        assert!(
            cold_only_route.cold_backing.as_ref().is_some_and(|backing| {
                backing.state == mooncake_store_core::ColdBackingState::Materialized
            }),
            "test route must keep materialized cold backing"
        );

        let restored = embedded
            .get("embedded-put-get-cold-disk-key")
            .expect("embedded client get should restore payload from cold tier disk");
        assert_eq!(restored, payload);

        let promoted = wait_for_route_replica_on_owner(
            &peer,
            "embedded-put-get-cold-disk-key",
            &embedded.lease.runtime,
        );
        assert_eq!(promoted.replicas.len(), 1);
        assert_eq!(promoted.replicas[0].owner, embedded.lease.runtime);

        evict_until_cold_only(&embedded, "embedded-put-get-cold-disk-key");
        let peer_restored = peer
            .get("embedded-put-get-cold-disk-key")
            .expect("second client should trigger owner cold read from disk");
        assert_eq!(peer_restored, payload);

        let _ = std::fs::remove_dir_all(cold_root);
    });
}
