use super::*;

use mooncake_store_core::{NofBackingRouteFilter, RouteState};
use mooncake_store_rs_metadata::{RedisMetadataBackend, RedisMetadataConfig};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};

struct RedisProcess {
    child: Child,
    dir: std::path::PathBuf,
}

impl Drop for RedisProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn redis_backend_for_test() -> Option<(RedisProcess, Arc<RedisMetadataBackend>)> {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        eprintln!("Redis integration unavailable: cannot bind a local TCP socket");
        return None;
    };
    let address = listener.local_addr().unwrap();
    drop(listener);
    let dir = std::env::temp_dir().join(format!("mooncake-nof-index-{}", address.port()));
    fs::create_dir_all(&dir).unwrap();
    let child = match Command::new("redis-server")
        .args([
            "--save",
            "",
            "--appendonly",
            "no",
            "--bind",
            "127.0.0.1",
            "--port",
        ])
        .arg(address.port().to_string())
        .arg("--dir")
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::remove_dir_all(dir).unwrap();
            eprintln!("Redis integration unavailable: redis-server is not installed");
            return None;
        }
        Err(error) => panic!("start Redis: {error}"),
    };
    let server = RedisProcess { child, dir };
    let deadline = Instant::now() + Duration::from_secs(3);
    while TcpStream::connect(address).is_err() {
        assert!(Instant::now() < deadline, "Redis did not start");
        sleep(Duration::from_millis(10));
    }
    let metadata = Arc::new(
        RedisMetadataBackend::new(RedisMetadataConfig::new(format!("redis://{address}/0")))
            .unwrap(),
    );
    Some((server, metadata))
}

fn hot_route(key: String) -> ObjectRoute {
    ObjectRoute {
        key: ObjectKey::new(key),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: None,
        version: RouteVersion(1),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: Vec::new(),
        cold_backing: None,
        nof_backing: None,
    }
}

fn seed_unrelated_hot_routes(scoped: &dyn MetadataBackend) {
    for i in 0..4097 {
        let route = hot_route(format!("unrelated-hot-{i}"));
        assert!(
            scoped
                .compare_and_swap_object_route(&route.key, None, Some(&route))
                .unwrap()
                .applied
        );
    }
}

#[test]
fn store_client_keeps_indexed_nof_queries_for_large_redis_namespaces() {
    let Some((_server, metadata)) = redis_backend_for_test() else {
        return;
    };
    seed_unrelated_hot_routes(metadata.for_tenant("default").unwrap().as_ref());
    let client = StoreClientBuilder::new(metadata, "indexed-nof-observer")
        .transport(Arc::new(TestTransport::new("indexed-nof-observer-segment")))
        .build(test_future_expiry_ms())
        .expect("client should build with normal observed metadata");
    let observed = client.metadata.for_tenant("default").unwrap();
    for target in ["empty-a", "empty-b"] {
        assert!(observed
            .list_object_routes_by_nof_backing(&NofBackingRouteFilter {
                target_id: Some(target.into()),
                ..Default::default()
            })
            .expect("NoF target lookup must retain the Redis override through StoreClient")
            .is_empty());
    }
}

#[test]
fn periodic_redis_gc_ignores_hot_routes_and_keeps_device_delete_cleanup() {
    use mooncake_store_core::{
        ColdBackingRoute, ColdBackingRouteFilter, ColdBackingState, ColdTierUsageDelta,
    };
    let _cold_guard = enable_cold_tier_for_test();
    let Some((_server, metadata)) = redis_backend_for_test() else {
        return;
    };
    let scoped = metadata.for_tenant("default").unwrap();
    seed_unrelated_hot_routes(scoped.as_ref());
    let device = "indexed-redis-gc";
    let root = cold_tier_test_root(device);
    let client = StoreClientBuilder::new(metadata, "indexed-redis-gc-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .route_control(RouteControlMode::MetadataOnly)
        .transport(Arc::new(TestTransport::new("indexed-redis-gc-segment")))
        .local_memory(storage_config())
        .cold_tier_target(ColdTierTargetConfig::directory(
            device,
            ColdTierKind::Ssd,
            root.clone(),
        ))
        .build(test_future_expiry_ms())
        .unwrap();
    // An empty pending index must retain the Redis fast path through observation.
    assert!(client
        .metadata
        .for_tenant("default")
        .unwrap()
        .scan_object_routes_by_cold_backing(
            &ColdBackingRouteFilter {
                state: Some(ColdBackingState::PendingDelete),
                ..Default::default()
            },
            None,
            1
        )
        .unwrap()
        .routes
        .is_empty());
    assert_eq!(
        client
            .storage_owner
            .garbage_collect_pending_delete_backings_bounded_for_device(None, 1)
            .unwrap()
            .scanned_routes,
        0
    );
    assert_eq!(
        client
            .storage_owner
            .garbage_collect_pending_delete_backings_for_device_bounded(device, 1)
            .unwrap(),
        0
    );

    let payload = b"gc-payload";
    let mut keys = Vec::new();
    for (name, state, backing_state) in [
        (
            "indexed-pending",
            RouteState::Active,
            ColdBackingState::PendingDelete,
        ),
        (
            "indexed-deleting",
            RouteState::Deleting,
            ColdBackingState::Materialized,
        ),
    ] {
        let mut route = hot_route(client.scoped_object_key_for_tenant("default", name).0);
        route.state = state;
        let backing = ColdBackingRoute {
            owner: client.runtime_id().clone(),
            cold_tier_id: device.into(),
            object_locator: name.into(),
            length: payload.len() as u64,
            checksum: Some(payload_checksum(payload)),
            state: backing_state,
            replicas: Vec::new(),
        };
        store_test_cold_payload(&backing, payload);
        route.cold_backing = Some(backing);
        keys.push(route.key.clone());
        assert!(
            scoped
                .compare_and_swap_object_route(&route.key, None, Some(&route))
                .unwrap()
                .applied
        );
    }
    scoped
        .apply_cold_tier_usage_delta(
            device,
            ColdTierUsageDelta {
                used_bytes: (payload.len() * 2) as i64,
                reserved_bytes: 0,
            },
            now_ms(),
        )
        .unwrap();

    let periodic = client
        .storage_owner
        .garbage_collect_pending_delete_backings_bounded_for_device(None, 1)
        .unwrap();
    assert_eq!(periodic.scanned_routes, 1);
    assert_eq!(periodic.cas_removed_routes, 1);
    assert_eq!(periodic.used_bytes_released, payload.len() as u64);
    assert!(scoped.get_object_route(&keys[0]).unwrap().is_none());
    assert!(scoped.get_object_route(&keys[1]).unwrap().is_some());
    assert_eq!(
        client
            .storage_owner
            .garbage_collect_pending_delete_backings_for_device_bounded(device, 1)
            .unwrap(),
        1
    );
    assert!(scoped.get_object_route(&keys[1]).unwrap().is_none());
    assert_eq!(
        scoped
            .get_cold_tier_device(device)
            .unwrap()
            .unwrap()
            .used_bytes,
        0
    );
    drop(client);
    fs::remove_dir_all(root).unwrap();
}
