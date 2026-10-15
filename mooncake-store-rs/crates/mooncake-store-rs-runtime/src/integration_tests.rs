#![allow(unused_imports)]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::net::TcpListener;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::dummy_service::{pb, start_dummy_store_server, DummyStoreServerHandle};
use crate::test_support::{
    bind_addr, build_client, build_client_with_metadata, build_client_with_metadata_and_expiry,
    build_client_with_metadata_and_state, build_client_with_metadata_and_transport, env_test_lock,
    BlockingHealthMetadata, EnvVarGuard, RecoveryCountingMetadata, TestTransport,
};
use crate::{
    finalize_real_dispatcher_setup, CompatNamespaceScope, CompatTimeoutConfig, DummySession,
    StoreDispatcher,
};
use mooncake_store_client::{
    snapshot_metrics, LocalMemoryConfig, MooncakeCompatibilityFacade, PlacementPlanner,
    RouteControlMode, StoreClient, StoreClientBuilder, StoreTransport,
};
use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
    CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey, ObjectRoute,
    ReplicaRoute, ReplicaTier, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
    TenantObjectAccounting, TenantPolicy, TenantPolicyScope, TenantQuotaAbortOutcome,
    TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest, TenantQuotaReservation,
    TenantQuotaReservationOutcome, TenantQuotaReservationRequest, TenantQuotaState,
};
use mooncake_store_rs_metadata::InMemoryMetadataBackend;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

fn lease_expires_at_ms(metadata: &InMemoryMetadataBackend, runtime: &ClientRuntimeId) -> u64 {
    metadata
        .list_live_clients()
        .expect("leases should list")
        .into_iter()
        .find(|lease| lease.runtime == *runtime)
        .expect("target lease should exist")
        .expires_at_ms
}

fn wait_until_lease_after(
    metadata: &InMemoryMetadataBackend,
    runtime: &ClientRuntimeId,
    previous_expires_at_ms: u64,
) -> bool {
    for _ in 0..160 {
        if lease_expires_at_ms(metadata, runtime) > previous_expires_at_ms {
            return true;
        }
        sleep(Duration::from_millis(25));
    }
    false
}

struct BlockingDummyServerHandle {
    address: String,
    release: Arc<AtomicBool>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl BlockingDummyServerHandle {
    fn address(&self) -> &str {
        &self.address
    }

    fn release(&self) {
        self.release.store(true, Ordering::SeqCst);
    }

    fn shutdown(mut self) {
        self.release();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn start_blocking_dummy_server() -> BlockingDummyServerHandle {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener addr should resolve")
        .to_string();
    drop(listener);
    let socket_addr = address
        .parse()
        .expect("blocking dummy server address should parse");
    let release = Arc::new(AtomicBool::new(false));
    let service = BlockingDummyStoreService {
        release: release.clone(),
    };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("blocking-dummy-server".to_string())
        .spawn(move || {
            let runtime =
                tokio::runtime::Runtime::new().expect("blocking dummy runtime should build");
            runtime.block_on(async move {
                tonic::transport::Server::builder()
                    .add_service(
                        pb::dummy_store_service_server::DummyStoreServiceServer::new(service),
                    )
                    .serve_with_shutdown(socket_addr, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("blocking dummy server should run");
            });
        })
        .expect("blocking dummy server thread should spawn");
    DummySession::connect(&address, "blocking-worker")
        .expect("blocking dummy server should accept connections");
    BlockingDummyServerHandle {
        address,
        release,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    }
}

#[test]
fn dispatcher_register_local_memory_uses_startup_timeout_not_request_timeout() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(AtomicBool::new(false));
    let metadata = Arc::new(BlockingHealthMetadata::new_with_publish_segment(
        Arc::new(AtomicBool::new(true)),
        release.clone(),
        entered_tx,
    ));
    let timeouts = CompatTimeoutConfig {
        request_timeout: Duration::from_millis(50),
        startup_timeout_override: Some(Duration::from_millis(400)),
        heartbeat_timeout: Duration::from_secs(1),
        transfer_stall_timeout: Duration::from_secs(1),
        dummy_rpc_timeout: Duration::from_millis(50),
    };
    let dispatcher = Arc::new(
        StoreDispatcher::spawn_with_timeout_config(
            build_client_with_metadata("dispatcher-startup-timeout-success", metadata),
            "dispatcher-startup-timeout-success".to_string(),
            timeouts,
        )
        .expect("dispatcher should spawn"),
    );

    let worker = std::thread::Builder::new()
        .name("dispatcher-startup-timeout-success".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || dispatcher.register_local_memory()
        })
        .expect("startup registration worker should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("startup registration should enter metadata publish");
    sleep(Duration::from_millis(200));
    release.store(true, Ordering::SeqCst);

    worker
        .join()
        .expect("startup registration worker should join")
        .expect("startup registration should outlive request timeout");
}

#[test]
fn dispatcher_register_local_memory_times_out_with_startup_budget() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let release = Arc::new(AtomicBool::new(false));
    let metadata = Arc::new(BlockingHealthMetadata::new_with_publish_segment(
        Arc::new(AtomicBool::new(true)),
        release.clone(),
        entered_tx,
    ));
    let timeouts = CompatTimeoutConfig {
        request_timeout: Duration::from_secs(5),
        startup_timeout_override: Some(Duration::from_millis(120)),
        heartbeat_timeout: Duration::from_secs(1),
        transfer_stall_timeout: Duration::from_secs(1),
        dummy_rpc_timeout: Duration::from_secs(5),
    };
    let dispatcher = Arc::new(
        StoreDispatcher::spawn_with_timeout_config(
            build_client_with_metadata("dispatcher-startup-timeout-fail", metadata),
            "dispatcher-startup-timeout-fail".to_string(),
            timeouts,
        )
        .expect("dispatcher should spawn"),
    );

    let worker = std::thread::Builder::new()
        .name("dispatcher-startup-timeout-fail".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || dispatcher.register_local_memory()
        })
        .expect("startup registration worker should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("startup registration should enter metadata publish");
    let error = worker
        .join()
        .expect("startup registration worker should join")
        .expect_err("startup registration should time out");
    assert!(
        matches!(error, StoreError::Transport(ref message) if message.contains("startup registration timed out after 120ms")),
        "unexpected error: {error}"
    );
    release.store(true, Ordering::SeqCst);
}

#[test]
fn dispatcher_buffer_registration_timeout_uses_explicit_override() {
    let timeouts = CompatTimeoutConfig {
        request_timeout: Duration::from_millis(50),
        startup_timeout_override: Some(Duration::from_millis(400)),
        heartbeat_timeout: Duration::from_secs(1),
        transfer_stall_timeout: Duration::from_secs(1),
        dummy_rpc_timeout: Duration::from_millis(50),
    };
    let dispatcher = StoreDispatcher::spawn_with_timeout_config(
        build_client("dispatcher-buffer-timeout-override"),
        "dispatcher-buffer-timeout-override".to_string(),
        timeouts,
    )
    .expect("dispatcher should spawn");
    assert_eq!(
        dispatcher.registration_timeout_for_bytes(64),
        Duration::from_millis(400)
    );
}

#[test]
fn dispatcher_buffer_registration_timeout_uses_adaptive_floor_without_override() {
    let timeouts = CompatTimeoutConfig {
        request_timeout: Duration::from_millis(50),
        startup_timeout_override: None,
        heartbeat_timeout: Duration::from_secs(1),
        transfer_stall_timeout: Duration::from_secs(1),
        dummy_rpc_timeout: Duration::from_millis(50),
    };
    let dispatcher = StoreDispatcher::spawn_with_timeout_config(
        build_client("dispatcher-buffer-timeout-floor"),
        "dispatcher-buffer-timeout-floor".to_string(),
        timeouts,
    )
    .expect("dispatcher should spawn");
    assert_eq!(
        dispatcher.registration_timeout_for_bytes(64),
        Duration::from_secs(20)
    );
}

#[test]
fn dummy_clients_share_hot_cache_shm_hits() {
    let _guard = env_test_lock().lock();
    let _cache_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
    let _block_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "1024");
    let _use_shm = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");

    let dispatcher = Arc::new(
        StoreDispatcher::spawn(build_client("dummy-hot-cache"), "dummy-hot-cache")
            .expect("dispatcher should spawn"),
    );
    dispatcher
        .register_local_memory()
        .expect("local memory should register");
    let server = start_dummy_store_server(dispatcher.clone(), &bind_addr(), "shared-hot-cache")
        .expect("server should start");
    let dummy_one = DummySession::connect(server.address(), "shared-hot-cache")
        .expect("dummy one should connect");
    let dummy_two = DummySession::connect(server.address(), "shared-hot-cache")
        .expect("dummy two should connect");
    assert!(dummy_one.has_hot_cache_mapping());
    assert!(dummy_two.has_hot_cache_mapping());

    dispatcher
        .run(|client| client.put("alpha", b"one"))
        .expect("put should succeed");
    let (status, value) = dummy_one
        .get("alpha", None)
        .expect("first dummy get should work");
    assert_eq!(status, 0);
    assert_eq!(value, b"one");

    let dispatcher_for_remove = dispatcher
        .fork_with_scope("shared-hot-cache")
        .expect("scoped dispatcher should fork");
    dispatcher_for_remove
        .run(|client| client.remove("alpha", true))
        .expect("raw remove should succeed");
    let (status, value) = dummy_two
        .get("alpha", None)
        .expect("second dummy should hit shared hot cache");
    assert_eq!(status, 0);
    assert_eq!(value, b"one");

    dummy_one.close();
    dummy_two.close();
    server.shutdown().expect("server should stop");
}

#[test]
fn dummy_worker_scope_isolates_hot_cache_shm_hits() {
    let _guard = env_test_lock().lock();
    let _cache_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
    let _block_size = EnvVarGuard::set("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "1024");
    let _use_shm = EnvVarGuard::set("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "1");

    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client("dummy-hot-cache-isolated"),
            "dummy-hot-cache-isolated",
        )
        .expect("dispatcher should spawn"),
    );
    dispatcher
        .register_local_memory()
        .expect("local memory should register");
    let server = start_dummy_store_server(dispatcher.clone(), &bind_addr(), "scope-a")
        .expect("server should start");
    let dummy_a =
        DummySession::connect(server.address(), "scope-a").expect("scope a client should connect");
    let dummy_b =
        DummySession::connect(server.address(), "scope-b").expect("scope b client should connect");
    assert!(dummy_a.has_hot_cache_mapping());
    assert!(!dummy_b.has_hot_cache_mapping());

    let address = bind_addr();
    let server_b = start_dummy_store_server(dispatcher.clone(), &address, "scope-b")
        .expect("scope b server should start");
    let dummy_b_scoped = DummySession::connect(server_b.address(), "scope-b")
        .expect("scope b scoped client should connect");
    assert!(dummy_b_scoped.has_hot_cache_mapping());

    dispatcher
        .run(|client| client.put("alpha", b"one"))
        .expect("put should succeed");
    let (status, value) = dummy_a.get("alpha", None).expect("scope a get should work");
    assert_eq!(status, 0);
    assert_eq!(value, b"one");

    let dispatcher_for_remove = dispatcher
        .fork_with_scope("scope-a")
        .expect("scoped dispatcher should fork");
    dispatcher_for_remove
        .run(|client| client.remove("alpha", true))
        .expect("raw remove should succeed");
    let (status, value) = dummy_b_scoped
        .get("alpha", None)
        .expect("scope b scoped get should complete");
    assert_eq!(status, -1);
    assert!(value.is_empty());

    dummy_a.close();
    dummy_b.close();
    dummy_b_scoped.close();
    server.shutdown().expect("server should stop");
    server_b.shutdown().expect("scope b server should stop");
}

#[test]
fn dummy_rpc_returns_timeout_instead_of_hanging() {
    let server = start_blocking_dummy_server();
    let session = DummySession::connect_with_rpc_timeout(
        server.address(),
        "blocking-worker",
        Duration::from_millis(200),
    )
    .expect("dummy client should connect to blocking server");
    let started = std::time::Instant::now();
    let error = session
        .get("blocked", None)
        .expect_err("blocked dummy rpc should time out");
    assert!(
        matches!(error, StoreError::Transport(ref message) if message.contains("timed out")),
        "unexpected error: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "dummy rpc timeout should fail fast"
    );
    server.shutdown();
}

#[test]
fn dispatcher_async_bridge_wakes_foreign_runtime() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(build_client("dispatcher-foreign-runtime"), "dispatcher")
            .expect("dispatcher should spawn"),
    );
    let worker = std::thread::Builder::new()
        .name("dispatcher-foreign-runtime-test".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                let runtime = tokio::runtime::Runtime::new().expect("foreign runtime should build");
                runtime.block_on(async move {
                    for expected in 0..16u32 {
                        let value = dispatcher
                            .run_async(move |_client| Ok::<_, StoreError>(expected))
                            .await
                            .expect("foreign runtime call should complete");
                        assert_eq!(value, expected);
                    }
                });
            }
        })
        .expect("foreign runtime worker should spawn");
    worker.join().expect("foreign runtime worker should join");
}

#[test]
fn dispatcher_async_wait_returns_timeout_instead_of_hanging() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn_with_timeouts(
            build_client("dispatcher-timeout"),
            "dispatcher-timeout".to_string(),
            Duration::from_millis(200),
            Duration::from_secs(15),
        )
        .expect("dispatcher should spawn"),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let started = std::time::Instant::now();
    let worker = std::thread::Builder::new()
        .name("dispatcher-timeout-test".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            }
        })
        .expect("dispatcher timeout worker should spawn");
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dispatcher task should enter");
    let error = worker
        .join()
        .expect("dispatcher timeout worker should join")
        .expect_err("blocked dispatcher request should time out");
    assert!(
        matches!(error, StoreError::Transport(ref message) if message.contains("timed out")),
        "unexpected error: {error}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "dispatcher timeout should fail fast"
    );
    let _ = release_tx.send(());
}

#[test]
fn dispatcher_shared_requests_do_not_head_of_line_block() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client("dispatcher-shared"),
            "dispatcher-shared".to_string(),
        )
        .expect("dispatcher should spawn"),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-shared-read-holder".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            }
        })
        .expect("dispatcher shared test worker should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first shared request should enter");
    let started = std::time::Instant::now();
    let second = dispatcher
        .run(|_client| Ok::<_, StoreError>(7usize))
        .expect("second shared request should not block behind first");
    assert_eq!(second, 7);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "second shared request should complete quickly"
    );
    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher shared worker should join")
        .expect("first shared request should finish after release");
}

#[test]
fn dispatcher_async_shared_requests_do_not_head_of_line_block() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client("dispatcher-async-shared"),
            "dispatcher-async-shared".to_string(),
        )
        .expect("dispatcher should spawn"),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-async-shared-holder".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                let runtime = tokio::runtime::Runtime::new().expect("foreign runtime should build");
                runtime.block_on(async move {
                    dispatcher
                        .run_async(move |_client| {
                            let _ = entered_tx.send(());
                            let _ = release_rx.recv();
                            Ok::<_, StoreError>(())
                        })
                        .await
                })
            }
        })
        .expect("dispatcher async shared test worker should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first async shared request should enter");

    let started = std::time::Instant::now();
    let runtime = tokio::runtime::Runtime::new().expect("foreign runtime should build");
    let second = runtime
        .block_on(async {
            dispatcher
                .run_async(|_client| Ok::<_, StoreError>(17usize))
                .await
        })
        .expect("second async shared request should not block behind first");
    assert_eq!(second, 17);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "second async shared request should complete quickly"
    );

    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher async shared worker should join")
        .expect("first async shared request should finish after release");
}

#[test]
fn dispatcher_worker_runtimes_are_isolated() {
    let dispatcher_a = StoreDispatcher::spawn(
        build_client("dispatcher-runtime-a"),
        "dispatcher-runtime-a".to_string(),
    )
    .expect("dispatcher a should spawn");
    let dispatcher_b = StoreDispatcher::spawn(
        build_client("dispatcher-runtime-b"),
        "dispatcher-runtime-b".to_string(),
    )
    .expect("dispatcher b should spawn");

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-runtime-a-holder".to_string())
        .spawn(move || {
            dispatcher_a.run(move |_client| {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
                Ok::<_, StoreError>(())
            })
        })
        .expect("dispatcher runtime holder should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dispatcher a request should enter");
    let started = std::time::Instant::now();
    let second = dispatcher_b
        .run(|_client| Ok::<_, StoreError>(11usize))
        .expect("dispatcher b should stay responsive");
    assert_eq!(second, 11);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "dispatcher b should not block behind dispatcher a"
    );
    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher runtime holder should join")
        .expect("dispatcher a request should complete");
}

#[test]
fn dispatcher_heartbeat_uses_independent_health_channel() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client("dispatcher-health-read-lock"),
            "dispatcher-health-read-lock".to_string(),
        )
        .expect("dispatcher should spawn"),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-health-read-holder".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            }
        })
        .expect("dispatcher health read-holder should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("read-holder should enter");

    let started = std::time::Instant::now();
    dispatcher
        .heartbeat(60_000)
        .expect("heartbeat should not wait for read lock");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "heartbeat should use independent health channel"
    );

    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher health read-holder should join")
        .expect("read-holder should finish after release");
}

#[test]
fn dispatcher_state_updates_use_independent_health_channel() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client("dispatcher-state-read-lock"),
            "dispatcher-state-read-lock".to_string(),
        )
        .expect("dispatcher should spawn"),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-state-read-holder".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            }
        })
        .expect("dispatcher state read-holder should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("read-holder should enter");

    let started = std::time::Instant::now();
    dispatcher
        .enter_draining()
        .expect("state update should not wait for read lock");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "state update should use independent health channel"
    );

    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher state read-holder should join")
        .expect("read-holder should finish after release");
}

#[test]
fn dispatcher_state_update_changes_inner_lifecycle() {
    let dispatcher = StoreDispatcher::spawn(
        build_client_with_metadata_and_state(
            "dispatcher-standby-activate",
            Arc::new(InMemoryMetadataBackend::new()),
            ClientLifecycleState::Standby,
        ),
        "dispatcher-standby-activate".to_string(),
    )
    .expect("dispatcher should spawn");
    dispatcher
        .register_local_memory()
        .expect("local memory should register");

    assert_eq!(
        dispatcher
            .run(|client| Ok::<_, StoreError>(client.lifecycle_state()))
            .expect("lifecycle read should succeed"),
        ClientLifecycleState::Standby
    );
    dispatcher.activate().expect("activate should succeed");
    assert_eq!(
        dispatcher
            .run(|client| Ok::<_, StoreError>(client.lifecycle_state()))
            .expect("lifecycle read should succeed"),
        ClientLifecycleState::Active
    );
    dispatcher
        .run(|client| client.put("activated-write", b"ok"))
        .expect("activated storage should accept writes");
    dispatcher
        .enter_draining()
        .expect("draining should succeed");
    assert_eq!(
        dispatcher
            .run(|client| Ok::<_, StoreError>(client.lifecycle_state()))
            .expect("lifecycle read should succeed"),
        ClientLifecycleState::Draining
    );
}

#[test]
fn dispatcher_auto_heartbeat_extends_lease_until_stopped() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let dispatcher = StoreDispatcher::spawn(
        build_client_with_metadata("dispatcher-auto-heartbeat", metadata.clone()),
        "dispatcher-auto-heartbeat".to_string(),
    )
    .expect("dispatcher should spawn");
    let runtime = ClientRuntimeId::new("dispatcher-auto-heartbeat", ClientEpoch(1));
    let initial_expires_at = lease_expires_at_ms(&metadata, &runtime);

    dispatcher
        .start_heartbeat_loop(90_000, Some(25))
        .expect("heartbeat loop should start");
    let refreshed = wait_until_lease_after(&metadata, &runtime, initial_expires_at);
    dispatcher.stop_heartbeat_loop();

    assert!(
        refreshed,
        "background heartbeat should refresh the lease expiry"
    );
    let stopped_expires_at = lease_expires_at_ms(&metadata, &runtime);
    sleep(Duration::from_millis(100));
    assert_eq!(
        lease_expires_at_ms(&metadata, &runtime),
        stopped_expires_at,
        "stopped heartbeat loop must not keep mutating the lease"
    );
    dispatcher
        .enter_offline()
        .expect("offline transition should succeed");
    dispatcher.shutdown();
    let state = metadata
        .list_live_clients()
        .expect("leases should list")
        .into_iter()
        .find(|lease| lease.runtime == runtime)
        .map(|lease| lease.state);
    assert_eq!(state, Some(ClientLifecycleState::Offline));
}

#[test]
fn dispatcher_plan_handoff_does_not_wait_for_read_lock() {
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client("dispatcher-plan-handoff"),
            "dispatcher-plan-handoff".to_string(),
        )
        .expect("dispatcher should spawn"),
    );
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-plan-handoff-read-holder".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            }
        })
        .expect("dispatcher plan-handoff read-holder should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("read-holder should enter");

    let started = std::time::Instant::now();
    let plan = dispatcher
        .plan_handoff(ClientEpoch(2), HandoffKind::HotUpgrade, 7, 1_000, None)
        .expect("plan_handoff should not wait for read lock");
    assert_eq!(plan.to.epoch, ClientEpoch(2));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "plan_handoff should use shared dispatcher path"
    );

    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher plan-handoff read-holder should join")
        .expect("read-holder should finish after release");
}

#[test]
fn dispatcher_targeted_handoff_activation_does_not_wait_for_read_lock() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let dispatcher = Arc::new(
        StoreDispatcher::spawn(
            build_client_with_metadata("dispatcher-targeted-handoff", metadata.clone()),
            "dispatcher-targeted-handoff".to_string(),
        )
        .expect("dispatcher should spawn"),
    );
    dispatcher
        .enter_standby()
        .expect("standby transition should succeed");
    let runtime = dispatcher
        .run(|client| Ok::<_, StoreError>(client.runtime_id().clone()))
        .expect("runtime id should load");
    metadata
        .put_handoff(&HandoffPlan {
            stable_id: runtime.stable_id.clone(),
            from: runtime.clone(),
            to: runtime.clone(),
            kind: HandoffKind::HotUpgrade,
            barrier_version: 9,
            created_at_ms: 2_000,
            deadline_ms: None,
        })
        .expect("targeted handoff should publish");

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("dispatcher-targeted-handoff-read-holder".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || {
                dispatcher.run(move |_client| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                    Ok::<_, StoreError>(())
                })
            }
        })
        .expect("dispatcher targeted-handoff read-holder should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("read-holder should enter");

    let started = std::time::Instant::now();
    let plan = dispatcher
        .activate_if_targeted_handoff()
        .expect("activate_if_targeted_handoff should not wait for read lock")
        .expect("targeted handoff should activate");
    assert_eq!(plan.to, runtime);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "activate_if_targeted_handoff should not wait for read lock"
    );

    let _ = release_tx.send(());
    worker
        .join()
        .expect("dispatcher targeted-handoff read-holder should join")
        .expect("read-holder should finish after release");
}

#[test]
fn dispatcher_heartbeat_timeout_does_not_block_shared_requests() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let block_enabled = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let metadata = Arc::new(BlockingHealthMetadata::new(
        block_enabled.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        release.clone(),
        entered_tx,
    ));
    let dispatcher = Arc::new(
        StoreDispatcher::spawn_with_heartbeat_timeout(
            build_client_with_metadata("dispatcher-heartbeat", metadata),
            "dispatcher-heartbeat".to_string(),
            Duration::from_millis(200),
        )
        .expect("dispatcher should spawn"),
    );
    block_enabled.store(true, Ordering::SeqCst);

    let worker = std::thread::Builder::new()
        .name("dispatcher-heartbeat-timeout".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || dispatcher.heartbeat(60_000)
        })
        .expect("heartbeat timeout worker should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("heartbeat publish should enter metadata");

    let started = std::time::Instant::now();
    let shared = dispatcher
        .run(|_client| Ok::<_, StoreError>(11usize))
        .expect("shared request should proceed while heartbeat publish is stuck");
    assert_eq!(shared, 11);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "shared request should not wait for stuck heartbeat publish"
    );

    let error = worker
        .join()
        .expect("heartbeat timeout worker should join")
        .expect_err("stuck heartbeat should time out");
    assert!(
        matches!(error, StoreError::Transport(ref message) if message.contains("heartbeat publish timed out")),
        "unexpected error: {error}"
    );

    let failed_metrics = snapshot_metrics();
    let runtime = "dispatcher-heartbeat:1".to_string();
    let consecutive_failures = failed_metrics
        .heartbeat_consecutive_failures
        .iter()
        .find(|sample| sample.key.runtime == runtime)
        .map(|sample| sample.value as u64)
        .unwrap_or_default();
    let last_success_ms = failed_metrics
        .heartbeat_last_success_ms
        .iter()
        .find(|sample| sample.key.runtime == runtime)
        .map(|sample| sample.value as u64)
        .unwrap_or_default();
    assert_eq!(consecutive_failures, 1);
    assert_eq!(last_success_ms, 0);

    let inflight = dispatcher
        .heartbeat(60_000)
        .expect_err("second heartbeat should see the first publish still in flight");
    assert!(
        matches!(inflight, StoreError::Transport(ref message) if message.contains("still in flight")),
        "unexpected in-flight error: {inflight}"
    );

    release.store(true, Ordering::SeqCst);
    for _ in 0..50 {
        if dispatcher.heartbeat(60_000).is_ok() {
            let recovered_metrics = snapshot_metrics();
            let consecutive_failures = recovered_metrics
                .heartbeat_consecutive_failures
                .iter()
                .find(|sample| sample.key.runtime == runtime)
                .map(|sample| sample.value as u64)
                .unwrap_or_default();
            let last_success_ms = recovered_metrics
                .heartbeat_last_success_ms
                .iter()
                .find(|sample| sample.key.runtime == runtime)
                .map(|sample| sample.value as u64)
                .unwrap_or_default();
            assert_eq!(consecutive_failures, 0);
            assert!(last_success_ms > 0);
            return;
        }
        sleep(Duration::from_millis(20));
    }
    panic!("heartbeat should recover after the blocked publish is released");
}

#[test]
fn dispatcher_heartbeat_recovery_republishes_local_segments() {
    let metadata = Arc::new(RecoveryCountingMetadata::new());
    let transport = Arc::new(TestTransport::new("dispatcher-recovery-segment"));
    let client = match StoreClientBuilder::new(metadata.clone(), "dispatcher-recovery")
        .state(ClientLifecycleState::Active)
        .compatibility(CompatibilityDescriptor::default())
        .route_control(RouteControlMode::MetadataOnly)
        .transport(transport.clone())
        .local_memory(
            LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(4 * 1024)
                .scratch_bytes(4 * 1024)
                .alignment(1)
                .reclaim_grace_ms(0),
        )
        .build(60_000)
    {
        Ok(client) => client,
        Err(StoreError::Transport(message)) if message.contains("Operation not permitted") => {
            return;
        }
        Err(error) => panic!("dispatcher recovery client should build: {error}"),
    };
    let dispatcher = StoreDispatcher::spawn(client, "dispatcher-recovery".to_string())
        .expect("dispatcher should spawn");
    dispatcher
        .register_local_memory()
        .expect("local memory should register");
    let initial_publish_calls = metadata.publish_segment_calls();
    assert!(initial_publish_calls >= 1);

    metadata.fail_next_lease_upserts(2);
    assert!(dispatcher.heartbeat(60_000).is_err());
    assert!(dispatcher.heartbeat(60_000).is_err());
    let before_repair = metadata.publish_segment_calls();

    dispatcher
        .heartbeat(60_000)
        .expect("recovered heartbeat should repair local segment metadata");
    assert!(
        metadata.publish_segment_calls() > before_repair,
        "recovered dispatcher heartbeat should republish local segments"
    );
}

#[test]
fn dispatcher_state_update_timeout_does_not_block_shared_requests() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let block_enabled = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let metadata = Arc::new(BlockingHealthMetadata::new(
        block_enabled.clone(),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        release.clone(),
        entered_tx,
    ));
    let dispatcher = Arc::new(
        StoreDispatcher::spawn_with_heartbeat_timeout(
            build_client_with_metadata("dispatcher-activate", metadata),
            "dispatcher-activate".to_string(),
            Duration::from_millis(200),
        )
        .expect("dispatcher should spawn"),
    );
    block_enabled.store(true, Ordering::SeqCst);

    let worker = std::thread::Builder::new()
        .name("dispatcher-state-update-timeout".to_string())
        .spawn({
            let dispatcher = dispatcher.clone();
            move || dispatcher.activate()
        })
        .expect("state update timeout worker should spawn");

    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("state update publish should enter metadata");

    let started = std::time::Instant::now();
    let shared = dispatcher
        .run(|_client| Ok::<_, StoreError>(13usize))
        .expect("shared request should proceed while state update publish is stuck");
    assert_eq!(shared, 13);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "shared request should not wait for stuck state update"
    );

    let error = worker
        .join()
        .expect("state update timeout worker should join")
        .expect_err("stuck state update should time out");
    assert!(
        matches!(error, StoreError::Transport(ref message) if message.contains("state update publish timed out")),
        "unexpected error: {error}"
    );

    let inflight = dispatcher
        .enter_draining()
        .expect_err("second health update should see the first publish still in flight");
    assert!(
        matches!(inflight, StoreError::Transport(ref message) if message.contains("still in flight")),
        "unexpected in-flight error: {inflight}"
    );

    release.store(true, Ordering::SeqCst);
    for _ in 0..50 {
        if dispatcher.enter_draining().is_ok() {
            return;
        }
        sleep(Duration::from_millis(20));
    }
    panic!("state update should recover after the blocked publish is released");
}

#[test]
fn finalize_real_dispatcher_setup_starts_auto_heartbeat_loop() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let lease_ttl_ms = 1_500;
    let expires_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should move forward")
        .as_millis() as u64
        + lease_ttl_ms;
    let dispatcher = StoreDispatcher::spawn(
        build_client_with_metadata_and_expiry(
            "dispatcher-setup-auto-heartbeat",
            metadata.clone(),
            ClientLifecycleState::Active,
            expires_at_ms,
        ),
        "dispatcher-setup-auto-heartbeat".to_string(),
    )
    .expect("dispatcher should spawn");
    finalize_real_dispatcher_setup(&dispatcher, ClientLifecycleState::Active, lease_ttl_ms)
        .expect("setup finalization should succeed");

    let runtime = ClientRuntimeId::new("dispatcher-setup-auto-heartbeat", ClientEpoch(1));
    let expires_after_setup = lease_expires_at_ms(&metadata, &runtime);
    let refreshed = wait_until_lease_after(&metadata, &runtime, expires_after_setup);
    dispatcher.stop_heartbeat_loop();

    assert!(
        refreshed,
        "setup finalization should start the background heartbeat loop"
    );
    dispatcher.shutdown();
}

#[derive(Clone)]
struct BlockingDummyStoreService {
    release: Arc<AtomicBool>,
}

impl BlockingDummyStoreService {
    async fn wait(&self) {
        while !self.release.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[tonic::async_trait]
impl pb::dummy_store_service_server::DummyStoreService for BlockingDummyStoreService {
    async fn health(
        &self,
        _request: tonic::Request<pb::HealthRequest>,
    ) -> std::result::Result<tonic::Response<pb::HealthReply>, tonic::Status> {
        Ok(tonic::Response::new(pb::HealthReply { status: 0 }))
    }

    async fn put(
        &self,
        _request: tonic::Request<pb::PutRequest>,
    ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
    }

    async fn get(
        &self,
        _request: tonic::Request<pb::GetRequest>,
    ) -> std::result::Result<tonic::Response<pb::GetReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::GetReply {
            status: 0,
            value: b"released".to_vec(),
        }))
    }

    async fn acquire_hot_cache(
        &self,
        _request: tonic::Request<pb::HotCacheAcquireRequest>,
    ) -> std::result::Result<tonic::Response<pb::HotCacheAcquireReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::HotCacheAcquireReply {
            status: -1,
            ..Default::default()
        }))
    }

    async fn release_hot_cache(
        &self,
        _request: tonic::Request<pb::HotCacheReleaseRequest>,
    ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
    }

    async fn batch_acquire_hot_cache(
        &self,
        _request: tonic::Request<pb::BatchHotCacheAcquireRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchHotCacheAcquireReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchHotCacheAcquireReply {
            items: vec![],
        }))
    }

    async fn batch_release_hot_cache(
        &self,
        _request: tonic::Request<pb::BatchHotCacheReleaseRequest>,
    ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
    }

    async fn batch_is_exist(
        &self,
        _request: tonic::Request<pb::BatchIsExistRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchIsExistReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchIsExistReply {
            statuses: vec![],
        }))
    }

    async fn batch_put_from(
        &self,
        _request: tonic::Request<pb::BatchPutFromRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchStatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchStatusReply {
            statuses: vec![],
        }))
    }

    async fn batch_put_from_multi_buffers(
        &self,
        _request: tonic::Request<pb::BatchPutFromMultiBuffersRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchStatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchStatusReply {
            statuses: vec![],
        }))
    }

    async fn batch_get_into(
        &self,
        _request: tonic::Request<pb::BatchGetIntoRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchGetIntoReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchGetIntoReply {
            lengths: vec![],
        }))
    }

    async fn batch_get_into_multi_buffers(
        &self,
        _request: tonic::Request<pb::BatchGetIntoMultiBuffersRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchGetIntoReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchGetIntoReply {
            lengths: vec![],
        }))
    }

    async fn remove(
        &self,
        _request: tonic::Request<pb::RemoveRequest>,
    ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
    }

    async fn batch_remove(
        &self,
        _request: tonic::Request<pb::BatchRemoveRequest>,
    ) -> std::result::Result<tonic::Response<pb::BatchStatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::BatchStatusReply {
            statuses: vec![],
        }))
    }

    async fn remove_all(
        &self,
        _request: tonic::Request<pb::RemoveAllRequest>,
    ) -> std::result::Result<tonic::Response<pb::RemoveAllReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::RemoveAllReply {
            status: 0,
            removed: 0,
        }))
    }

    async fn unregister_region(
        &self,
        _request: tonic::Request<pb::UnregisterRegionRequest>,
    ) -> std::result::Result<tonic::Response<pb::StatusReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::StatusReply { status: 0 }))
    }

    async fn get_into_ranges(
        &self,
        _request: tonic::Request<pb::GetIntoRangesRequest>,
    ) -> std::result::Result<tonic::Response<pb::GetIntoRangesReply>, tonic::Status> {
        self.wait().await;
        Ok(tonic::Response::new(pb::GetIntoRangesReply {
            buffer_results: vec![],
        }))
    }
}
