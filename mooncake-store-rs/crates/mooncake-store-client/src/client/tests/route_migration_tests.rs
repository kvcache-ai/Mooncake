use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc, Arc,
};
use std::thread::sleep;
use std::time::{Duration, Instant};

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, LogicalObjectId,
    NamespaceScope, ObjectKey, ObjectRoute, Result as StoreResult, RouteCasRequest, RouteDirectory,
    RouteVersion, StoreError,
};
use mooncake_store_test_utils::transport::TestTransport;
use parking_lot::Mutex;

use crate::{
    control_plane::pb, RouteControlMode, StoreClientBuilder, StoreTransport, StoreTransportFactory,
};

use super::super::{
    ExplicitMigrationMode, ExplicitMigrationPlan, LocalMigrationAdapter, MigrationExecutionRecord,
    MigrationService, MigrationWorkItem, ReplicaReadSelector,
};
use super::*;

struct CasErrorRouteDirectory {
    inner: Arc<dyn RouteDirectory>,
    fail_key: ObjectKey,
}

impl CasErrorRouteDirectory {
    fn new(inner: Arc<dyn RouteDirectory>, fail_key: ObjectKey) -> Self {
        Self { inner, fail_key }
    }
}

impl RouteDirectory for CasErrorRouteDirectory {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> StoreResult<Option<ObjectRoute>> {
        self.inner.get_object_route(observer, key)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> StoreResult<CasResult> {
        if *key == self.fail_key {
            return Err(StoreError::Transport(
                "injected route cas failure".to_string(),
            ));
        }
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> StoreResult<Vec<StoreResult<CasResult>>> {
        Ok(requests
            .iter()
            .map(|request| {
                self.compare_and_swap_object_route(
                    observer,
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                )
            })
            .collect::<Vec<_>>())
    }

    fn list_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> StoreResult<Vec<ObjectRoute>> {
        self.inner.list_routes_by_replica_owner(observer, owner)
    }
}

struct PanickingOnceTransportFactory {
    inner: Arc<dyn StoreTransportFactory>,
    did_panic: AtomicBool,
}

impl PanickingOnceTransportFactory {
    fn new(inner: Arc<dyn StoreTransportFactory>) -> Self {
        Self {
            inner,
            did_panic: AtomicBool::new(false),
        }
    }
}

impl StoreTransportFactory for PanickingOnceTransportFactory {
    fn create(&self, segment_name: &str) -> StoreResult<Arc<dyn StoreTransport>> {
        if !self.did_panic.swap(true, Ordering::SeqCst) {
            panic!("injected helper transport panic for {segment_name}");
        }
        self.inner.create(segment_name)
    }
}

#[test]
fn local_migration_adapter_marks_failed_when_worker_channel_is_closed() {
    let (task_sender, task_receiver) = mpsc::channel::<MigrationWorkItem>();
    drop(task_receiver);

    let executions = Arc::new(Mutex::new(
        BTreeMap::<String, MigrationExecutionRecord>::new(),
    ));
    let adapter = LocalMigrationAdapter {
        executor_stable_id: ClientStableId::new("closed-executor"),
        executions: executions.clone(),
        next_execution_id: Arc::new(AtomicU64::new(1)),
        task_sender,
    };

    let error = MigrationService::submit_task(
        &adapter,
        &pb::SubmitMigrationTaskRequest {
            namespace: "default".to_string(),
            authority: "closed-executor".to_string(),
            tenant: "default".to_string(),
            domain: String::new(),
            object_set: String::new(),
            key: "closed-key".to_string(),
            mode: pb::MigrationMode::Copy as i32,
            source_segment: "source-segment".to_string(),
            target_segments: vec!["target-segment".to_string()],
            task_executor: "closed-executor".to_string(),
            max_retries: 1,
        },
    )
    .expect_err("closed worker channel should reject new tasks");
    assert!(matches!(error, StoreError::Transport(_)));

    let record = executions
        .lock()
        .get("closed-executor-1")
        .cloned()
        .expect("failed submission should still leave an execution record");
    assert_eq!(record.state, pb::MigrationExecutionState::Failed);
    assert_eq!(record.attempts, 0);
    assert!(
        record.last_error.contains("not accepting new tasks"),
        "unexpected last_error: {}",
        record.last_error
    );
}

#[test]
fn explicit_copy_migration_releases_target_allocation_when_route_cas_errors() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("explicit-copy-cas-error-store-a"));
    let store_b_transport = Arc::new(store_a_transport.peer("explicit-copy-cas-error-store-b"));
    let reader_transport = Arc::new(store_a_transport.peer("explicit-copy-cas-error-reader"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "explicit-copy-cas-error-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let mut store_b = StoreClientBuilder::new(metadata.clone(), "explicit-copy-cas-error-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata, "explicit-copy-cas-error-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &reader]);

    let key = "explicit-copy-cas-error-key";
    let payload = b"explicit-copy-cas-error-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.clone();
    let source_owner = current.replicas[0].owner.clone();
    let object_key = current.key.clone();
    let target_segment = store_b
        .segment_name()
        .expect("store-b should expose a primary segment");
    let used_before = store_b
        .list_segments()
        .expect("pre-migration segment list should succeed")
        .into_iter()
        .map(|segment| segment.used_bytes)
        .sum::<u64>();
    store_b.route_directory = Arc::new(CasErrorRouteDirectory::new(
        store_b.route_directory.clone(),
        object_key,
    ));

    let object_id = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(store_b.default_tenant()), None, None),
        key,
    );
    let error = store_b
        .execute_explicit_route_migration(
            &object_id,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Copy,
                source: ReplicaReadSelector::OwnerAndSegment {
                    owner: source_owner,
                    segment_name: source_segment,
                },
                target_segments: vec![target_segment],
                all_or_nothing: true,
            },
        )
        .expect_err("cas failure should fail explicit migration");
    assert!(matches!(error, StoreError::Transport(_)));

    let used_after = store_b
        .list_segments()
        .expect("post-migration segment list should succeed")
        .into_iter()
        .map(|segment| segment.used_bytes)
        .sum::<u64>();
    assert_eq!(used_after, used_before);
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after failed migration should succeed"),
        payload
    );
}

#[test]
fn control_plane_submit_migration_task_recovers_after_executor_panic() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cp-panic-store-a-segment"));
    let executor_factory = Arc::new(PanickingOnceTransportFactory::new(
        store_a_transport.factory(),
    )) as Arc<dyn StoreTransportFactory>;
    let store_b_transport = Arc::new(store_a_transport.peer("cp-panic-store-b-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-panic-reader-segment"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-panic-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-panic-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .transport_factory(executor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-panic-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &reader]);

    let key = "cp-panic-key";
    let payload = b"cp-panic-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.0.clone();
    let target_segment = store_b
        .segment_name()
        .expect("store-b should expose a primary segment")
        .0;
    let executor_lease = store_b.lease();

    let first_execution = reader
        .control_client
        .submit_migration_task(
            &executor_lease,
            pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: pb::MigrationMode::Move as i32,
                source_segment: source_segment.clone(),
                target_segments: vec![target_segment.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("first migration task submit should succeed");

    let started = Instant::now();
    let first_detail = loop {
        let detail = reader
            .control_client
            .get_migration_execution_status_detail(
                &executor_lease,
                pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: first_execution.clone(),
                },
            )
            .expect("first migration status query should succeed");
        match pb::MigrationExecutionState::try_from(detail.state).expect("state should decode") {
            pb::MigrationExecutionState::Failed => break detail,
            pb::MigrationExecutionState::Succeeded => {
                panic!("panic-injected migration task should not succeed")
            }
            _ => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "panic-injected migration task should fail quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    };
    assert!(
        first_detail.last_error.contains("panicked"),
        "unexpected panic detail: {}",
        first_detail.last_error
    );

    let second_execution = reader
        .control_client
        .submit_migration_task(
            &executor_lease,
            pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: pb::MigrationMode::Move as i32,
                source_segment,
                target_segments: vec![target_segment.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("worker should continue accepting tasks after panic");

    let second_started = Instant::now();
    loop {
        let state = reader
            .control_client
            .get_migration_execution_status(
                &executor_lease,
                pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: second_execution.clone(),
                },
            )
            .expect("second migration status query should succeed");
        match state {
            pb::MigrationExecutionState::Succeeded => break,
            pb::MigrationExecutionState::Failed => {
                panic!("worker should survive panic and process the second task")
            }
            _ => {
                assert!(
                    second_started.elapsed() < Duration::from_secs(5),
                    "second migration task should complete quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    }

    assert_eq!(
        reader
            .get(key)
            .expect("reader get after recovered migration should succeed"),
        payload
    );
}

#[test]
fn control_plane_submit_migration_task_executes_explicit_move() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cp-move-store-a-segment"));
    let executor_factory = store_a_transport.factory();
    let store_b_transport = Arc::new(store_a_transport.peer("cp-move-store-b-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-move-reader-segment"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-move-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-move-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .transport_factory(executor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-move-reader")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "false")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(reader_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("reader build should succeed");

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &reader]);

    let key = "cp-move-key";
    let payload = b"cp-move-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.0.clone();
    let target_segment = store_b
        .segment_name()
        .expect("store-b should expose a primary segment")
        .0;
    let executor_lease = store_b.lease();
    let execution_id = reader
        .control_client
        .submit_migration_task(
            &executor_lease,
            pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: pb::MigrationMode::Move as i32,
                source_segment,
                target_segments: vec![target_segment.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("migration task submit should succeed");
    assert!(!execution_id.is_empty());

    let started = Instant::now();
    loop {
        let state = reader
            .control_client
            .get_migration_execution_status(
                &executor_lease,
                pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )
            .expect("migration status query should succeed");
        match state {
            pb::MigrationExecutionState::Succeeded => break,
            pb::MigrationExecutionState::Failed => {
                panic!("migration task should not fail")
            }
            _ => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "migration task should complete quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    }

    let route = reader
        .query_route(key)
        .expect("post-migration route query should succeed")
        .expect("post-migration route should exist");
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, *store_b.runtime_id());
    assert_eq!(
        route.replicas[0].segment_name,
        SegmentName::new(target_segment)
    );
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after control-plane move should succeed"),
        payload
    );
}
