use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_store_client::{
    LocalMemoryConfig, PlacementPlanner, RouteControlMode, StoreClient, StoreClientBuilder,
    TentTransportFactory,
};
use mooncake_store_core::{ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, Result};
use mooncake_transport::TentEngine;

pub use crate::config::CompatSetupArgs;

pub struct CompatRuntime {
    pub client: StoreClient,
    pub stable_id: String,
    pub epoch: ClientEpoch,
    pub initial_state: ClientLifecycleState,
    pub segment_name: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct CompatRuntimeArgs {
    pub setup: CompatSetupArgs,
    pub local_segment_name: Option<String>,
    pub epoch: ClientEpoch,
    pub initial_state: ClientLifecycleState,
    pub route_control: RouteControlMode,
}

impl CompatRuntimeArgs {
    pub fn build(self) -> Result<CompatRuntime> {
        let plan = self.setup.build()?;
        let planner_metadata = plan.routed_writes.then(|| plan.metadata.clone());
        let stable_id = plan.stable_id.clone();
        let expires_at_ms = plan.expires_at_ms;
        let segment_name = self
            .local_segment_name
            .unwrap_or_else(|| default_segment_name(&stable_id));
        let engine = Arc::new(TentEngine::new(
            &plan
                .tent_config
                .clone()
                .set("local_segment_name", &segment_name),
        )?);
        let factory = Arc::new(TentTransportFactory::new(plan.tent_config));
        let mut local_memory = LocalMemoryConfig::new()
            .storage_bytes(plan.storage_bytes)
            .scratch_bytes(plan.scratch_bytes)
            .location("cpu:0");
        if let Some(use_hugepage) = plan.use_hugepage {
            local_memory = local_memory.use_hugepage(use_hugepage);
        }
        if let Some(hugepage_size_bytes) = plan.hugepage_size_bytes {
            local_memory = local_memory.hugepage_size_bytes(hugepage_size_bytes);
        }

        let mut builder = StoreClientBuilder::new(plan.metadata, stable_id.clone())
            .epoch(self.epoch)
            .state(self.initial_state)
            .compatibility(CompatibilityDescriptor::default())
            .tenant(plan.tenant)
            .local_memory(local_memory)
            .with_tent(engine)
            .transport_factory(factory)
            .route_control(self.route_control);

        for (key, value) in plan.labels {
            builder = builder.label(key, value);
        }
        if let Some(metadata) = planner_metadata {
            builder = builder.routed_writes(
                PlacementPlanner::new(metadata).require_label("storage", "true"),
                plan.replica_count,
            );
        }

        Ok(CompatRuntime {
            client: builder.build(expires_at_ms)?,
            stable_id,
            epoch: self.epoch,
            initial_state: self.initial_state,
            segment_name,
            expires_at_ms,
        })
    }
}

fn default_segment_name(stable_id: &str) -> String {
    format!("{}-segment-{}", stable_id.replace(':', "-"), now_ms())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic")
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Mutex, OnceLock};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use mooncake_store_client::{
        GetRequest, MooncakeCompatibilityFacade, ObjectRef, PutRequest, RouteControlMode,
    };
    use mooncake_store_core::{ClientEpoch, ClientLifecycleState, HandoffKind, StoreError};

    use super::{default_segment_name, now_ms, CompatRuntimeArgs};
    use crate::config::CompatSetupArgs;

    struct RedisTestServer {
        child: Child,
        url: String,
        dir: PathBuf,
    }

    impl RedisTestServer {
        fn start() -> Option<Self> {
            Self::start_with_password(None)
        }

        fn start_with_password(password: Option<&str>) -> Option<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let port = listener.local_addr().ok()?.port();
            drop(listener);

            let dir = std::env::temp_dir().join(format!("mooncake-store-rs-runtime-redis-{port}"));
            let _ = std::fs::create_dir_all(&dir);
            let mut command = Command::new("redis-server");
            command
                .arg("--save")
                .arg("")
                .arg("--appendonly")
                .arg("no")
                .arg("--bind")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(port.to_string())
                .arg("--dir")
                .arg(&dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            if let Some(password) = password {
                command.arg("--requirepass").arg(password);
            }
            let child = command.spawn().ok()?;

            let url = match password {
                Some(password) => format!("redis://:{password}@127.0.0.1:{port}/0"),
                None => format!("redis://127.0.0.1:{port}/0"),
            };
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    return Some(Self { child, url, dir });
                }
                sleep(Duration::from_millis(25));
            }
            None
        }

        fn url(&self) -> &str {
            &self.url
        }
    }

    impl Drop for RedisTestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn sample_args(protocol: &str, metadata_url: &str) -> CompatRuntimeArgs {
        CompatRuntimeArgs {
            setup: CompatSetupArgs {
                local_hostname: "127.0.0.1".to_string(),
                metadata_url: metadata_url.to_string(),
                transport_metadata_url: None,
                global_segment_size: 1024,
                local_buffer_size: 1024,
                protocol: protocol.to_string(),
                _rdma_devices: String::new(),
                transport_rpc_port: None,
                stable_id: Some("py-runtime".to_string()),
                tenant: "default".to_string(),
                labels: BTreeMap::new(),
                routed_writes: false,
                replica_count: 1,
                keyspace: None,
                expires_at_ms: Some(10_000),
                use_hugepage: None,
                hugepage_size_bytes: None,
            },
            local_segment_name: None,
            epoch: ClientEpoch(1),
            initial_state: ClientLifecycleState::Active,
            route_control: RouteControlMode::EmbeddedWrh,
        }
    }

    #[test]
    fn default_segment_name_embeds_stable_id_and_timestamp() {
        let name = default_segment_name("node:1");
        assert!(name.starts_with("node-1-segment-"));
    }

    #[test]
    fn now_ms_is_monotonic_enough_for_runtime_ids() {
        let before = now_ms();
        let after = now_ms();
        assert!(after >= before);
    }

    #[test]
    fn runtime_build_rejects_invalid_transport_protocol_before_engine_start() {
        let error = match sample_args("bogus", "redis://127.0.0.1:6379/0").build() {
            Ok(_) => panic!("invalid protocol should fail during build plan construction"),
            Err(error) => error,
        };
        assert!(matches!(error, StoreError::Unsupported(_)));
    }

    #[test]
    fn runtime_builds_real_tent_clients_and_moves_remote_bytes() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let metadata_url = format!("{}/{}", server.url().trim_end_matches("/0"), 0);

        let mut writer = sample_args("tcp", &metadata_url);
        writer.setup.stable_id = Some("runtime-writer".to_string());
        writer.setup.keyspace = Some("runtime/test".to_string());
        writer.local_segment_name = Some("runtime-writer-segment".to_string());
        writer.route_control = RouteControlMode::MetadataOnly;

        let mut reader = sample_args("tcp", &metadata_url);
        reader.setup.stable_id = Some("runtime-reader".to_string());
        reader.setup.keyspace = Some("runtime/test".to_string());
        reader.local_segment_name = Some("runtime-reader-segment".to_string());
        reader.route_control = RouteControlMode::MetadataOnly;

        let writer = writer.build().expect("writer runtime should build");
        let reader = reader.build().expect("reader runtime should build");

        writer
            .client
            .register_local_memory()
            .expect("writer local memory should register");
        reader
            .client
            .register_local_memory()
            .expect("reader local memory should register");

        let expanded = writer
            .client
            .expand_local_memory(256)
            .expect("writer should expand local memory");
        writer
            .client
            .drain_segment(&expanded.segment_name)
            .expect("new segment should drain");
        assert!(writer
            .client
            .retire_segment(&expanded.segment_name)
            .expect("empty draining segment should retire"));

        writer
            .client
            .put("alpha", b"hello-tent")
            .expect("writer put should succeed");
        writer
            .client
            .batch_put(&[
                PutRequest::new("beta", b"beta"),
                PutRequest::new("gamma", b"gamma"),
            ])
            .expect("writer batch put should succeed");

        assert_eq!(
            reader
                .client
                .get("alpha")
                .expect("reader get should succeed"),
            b"hello-tent"
        );
        assert_eq!(
            reader
                .client
                .batch_get(&[ObjectRef::new("beta"), ObjectRef::new("gamma")])
                .expect("reader batch get should succeed"),
            vec![b"beta".to_vec(), b"gamma".to_vec()]
        );

        let mut buffer = [0u8; 10];
        assert_eq!(
            reader
                .client
                .get_into("alpha", &mut buffer)
                .expect("reader get_into should succeed"),
            10
        );
        assert_eq!(&buffer, b"hello-tent");

        let mut beta = [0u8; 4];
        let mut gamma = [0u8; 5];
        assert_eq!(
            reader
                .client
                .batch_get_into(&mut [
                    GetRequest::new("beta", &mut beta),
                    GetRequest::new("gamma", &mut gamma),
                ])
                .expect("reader batch_get_into should succeed"),
            vec![4, 5]
        );
        assert_eq!(&beta, b"beta");
        assert_eq!(&gamma, b"gamma");

        let route = writer
            .client
            .query_route("alpha")
            .expect("route query should succeed")
            .expect("route should exist");
        assert_eq!(route.replicas[0].owner.stable_id.0, writer.stable_id);
        assert!(writer.segment_name.contains("runtime-writer-segment"));
        assert_eq!(reader.expires_at_ms, 10_000);
    }

    #[test]
    fn runtime_builds_real_tent_clients_with_password_protected_redis() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        let Some(server) = RedisTestServer::start_with_password(Some("runtime-secret")) else {
            return;
        };

        let _env_guard = EnvVarGuard::replace_many(&[
            ("MC_REDIS_USERNAME", None),
            ("MC_REDIS_PASSWORD", None),
            ("MC_REDIS_DB_INDEX", None),
        ]);

        let mut writer = sample_args("tcp", server.url());
        writer.setup.stable_id = Some("runtime-auth-writer".to_string());
        writer.setup.keyspace = Some("runtime/auth".to_string());
        writer.local_segment_name = Some("runtime-auth-writer-segment".to_string());
        writer.route_control = RouteControlMode::MetadataOnly;

        let mut reader = sample_args("tcp", server.url());
        reader.setup.stable_id = Some("runtime-auth-reader".to_string());
        reader.setup.keyspace = Some("runtime/auth".to_string());
        reader.local_segment_name = Some("runtime-auth-reader-segment".to_string());
        reader.route_control = RouteControlMode::MetadataOnly;

        let writer = writer
            .build()
            .expect("writer runtime should build with password-protected redis");
        let reader = reader
            .build()
            .expect("reader runtime should build with password-protected redis");

        writer
            .client
            .register_local_memory()
            .expect("writer local memory should register");
        reader
            .client
            .register_local_memory()
            .expect("reader local memory should register");

        writer
            .client
            .put("auth-alpha", b"hello-auth")
            .expect("writer put should succeed");
        assert_eq!(
            reader
                .client
                .get("auth-alpha")
                .expect("reader remote get should succeed with redis url auth"),
            b"hello-auth"
        );
    }

    #[test]
    fn runtime_hot_upgrade_preserves_payload_on_successor() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let metadata_url = format!("{}/{}", server.url().trim_end_matches("/0"), 0);

        let mut predecessor = sample_args("tcp", &metadata_url);
        predecessor.setup.stable_id = Some("runtime-hot-upgrade".to_string());
        predecessor.setup.keyspace = Some("runtime/hot-upgrade".to_string());
        predecessor.setup.expires_at_ms = Some(now_ms() + 10_000);
        predecessor.local_segment_name = Some("runtime-hot-upgrade-old".to_string());
        predecessor.route_control = RouteControlMode::MetadataOnly;
        predecessor
            .setup
            .labels
            .insert("pool".to_string(), "pool-a".to_string());
        predecessor
            .setup
            .labels
            .insert("storage".to_string(), "true".to_string());
        predecessor
            .setup
            .labels
            .insert("route".to_string(), "false".to_string());

        let mut successor = predecessor.clone();
        successor.epoch = ClientEpoch(2);
        successor.initial_state = ClientLifecycleState::Standby;
        successor.local_segment_name = Some("runtime-hot-upgrade-new".to_string());

        let mut reader = sample_args("tcp", &metadata_url);
        reader.setup.stable_id = Some("runtime-hot-upgrade-reader".to_string());
        reader.setup.keyspace = Some("runtime/hot-upgrade".to_string());
        reader.setup.expires_at_ms = Some(now_ms() + 10_000);
        reader.local_segment_name = Some("runtime-hot-upgrade-reader".to_string());
        reader.route_control = RouteControlMode::MetadataOnly;
        reader
            .setup
            .labels
            .insert("pool".to_string(), "pool-a".to_string());
        reader
            .setup
            .labels
            .insert("storage".to_string(), "false".to_string());
        reader
            .setup
            .labels
            .insert("route".to_string(), "false".to_string());

        let mut predecessor = predecessor
            .build()
            .expect("predecessor runtime should build");
        let mut successor = successor.build().expect("successor runtime should build");
        let reader = reader.build().expect("reader runtime should build");

        predecessor
            .client
            .register_local_memory()
            .expect("predecessor memory should register");
        successor
            .client
            .register_local_memory()
            .expect("successor memory should register");
        reader
            .client
            .register_local_memory()
            .expect("reader memory should register");

        predecessor
            .client
            .put("alpha", b"runtime-hot-upgrade-payload")
            .expect("seed put should succeed");

        predecessor
            .client
            .enter_draining()
            .expect("predecessor should drain");
        predecessor
            .client
            .plan_handoff(
                ClientEpoch(2),
                HandoffKind::HotUpgrade,
                1,
                100,
                Some(u64::MAX),
            )
            .expect("handoff planning should succeed");
        successor
            .client
            .activate_if_targeted_handoff()
            .expect("successor activation should succeed")
            .expect("targeted handoff should be visible");

        let successor_runtime = successor.client.runtime_id().clone();
        let migrated = predecessor
            .client
            .evacuate_owned_replicas_to_runtime(&successor_runtime)
            .expect("targeted hot-upgrade evacuation should succeed");
        assert_eq!(migrated, 1);

        let route = reader
            .client
            .query_route("alpha")
            .expect("route query should succeed")
            .expect("route should exist after promotion");
        assert_eq!(route.replicas[0].owner, successor_runtime);
        assert_eq!(
            reader
                .client
                .get("alpha")
                .expect("reader get should succeed"),
            b"runtime-hot-upgrade-payload"
        );
    }

    struct EnvVarGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvVarGuard {
        fn replace_many(pairs: &[(&'static str, Option<&str>)]) -> Self {
            let mut saved = Vec::with_capacity(pairs.len());
            for (key, value) in pairs {
                saved.push((*key, std::env::var(key).ok()));
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
            Self { saved }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..).rev() {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }
}
