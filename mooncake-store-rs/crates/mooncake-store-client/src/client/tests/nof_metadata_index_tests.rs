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

#[test]
fn store_client_keeps_indexed_nof_queries_for_large_redis_namespaces() {
    let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
        eprintln!("Redis integration unavailable: cannot bind a local TCP socket");
        return;
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
            return;
        }
        Err(error) => panic!("start Redis: {error}"),
    };
    let _server = RedisProcess { child, dir };
    let deadline = Instant::now() + Duration::from_secs(3);
    while TcpStream::connect(address).is_err() {
        assert!(Instant::now() < deadline, "Redis did not start");
        sleep(Duration::from_millis(10));
    }
    let metadata = Arc::new(
        RedisMetadataBackend::new(RedisMetadataConfig::new(format!("redis://{address}/0")))
            .unwrap(),
    );
    let scoped = metadata.for_tenant("default").unwrap();
    for i in 0..4097 {
        let route = ObjectRoute {
            key: ObjectKey::new(format!("unrelated-hot-{i}")),
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
        };
        assert!(
            scoped
                .compare_and_swap_object_route(&route.key, None, Some(&route))
                .unwrap()
                .applied
        );
    }
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
