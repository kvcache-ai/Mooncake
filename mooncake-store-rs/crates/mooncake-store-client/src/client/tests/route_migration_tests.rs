use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc, Arc, Barrier,
};
use std::thread::sleep;
use std::time::{Duration, Instant};

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
    CompatibilityDescriptor, LogicalObjectId, NamespaceScope, ObjectKey, ObjectRoute, ReplicaRoute,
    ReplicaTier, Result as StoreResult, RouteCasRequest, RouteDirectory, RouteState, RouteVersion,
    SegmentName, StoreError,
};
use mooncake_store_test_utils::transport::TestTransport;
use parking_lot::Mutex;

use crate::{
    MooncakeCompatibilityFacade, ObjectRef, PutRequest, RouteControlMode, StoreClient,
    StoreClientBuilder, StoreTransport, StoreTransportFactory,
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
fn explicit_copy_route_delta_preserves_existing_replicas_and_appends_targets() {
    let source_owner = ClientRuntimeId::new("store-a", ClientEpoch(1));
    let replica_b_owner = ClientRuntimeId::new("store-b", ClientEpoch(1));
    let target_c_owner = ClientRuntimeId::new("store-c", ClientEpoch(1));
    let target_d_owner = ClientRuntimeId::new("store-d", ClientEpoch(1));
    let current = ObjectRoute {
        key: ObjectKey::new("tenant-a::copy-key"),
        namespace: Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None)),
        logical_key: Some("copy-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("tenant-a".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(7),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![
            ReplicaRoute {
                owner: source_owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                offset: 0,
                segment_offset: 0,
                length: 11,
                checksum: Some(11),
                tier: ReplicaTier::Dram,
                priority: 0,
            },
            ReplicaRoute {
                owner: replica_b_owner.clone(),
                segment_name: SegmentName::new("seg-b"),
                offset: 128,
                segment_offset: 128,
                length: 11,
                checksum: Some(11),
                tier: ReplicaTier::Dram,
                priority: 1,
            },
        ],
    };

    let next = StoreClient::build_explicit_copy_route_delta(
        &current,
        &SegmentName::new("seg-a"),
        vec![
            ReplicaRoute {
                owner: target_c_owner.clone(),
                segment_name: SegmentName::new("seg-c"),
                offset: 256,
                segment_offset: 256,
                length: 11,
                checksum: Some(11),
                tier: ReplicaTier::Dram,
                priority: 99,
            },
            ReplicaRoute {
                owner: target_d_owner.clone(),
                segment_name: SegmentName::new("seg-d"),
                offset: 384,
                segment_offset: 384,
                length: 11,
                checksum: Some(11),
                tier: ReplicaTier::Dram,
                priority: 100,
            },
        ],
    )
    .expect("copy delta should succeed");

    assert_eq!(next.version, RouteVersion(8));
    assert_eq!(next.state, RouteState::Active);
    assert_eq!(next.replicas.len(), 4);
    assert_eq!(next.replicas[0].owner, source_owner);
    assert_eq!(next.replicas[0].segment_name, SegmentName::new("seg-a"));
    assert_eq!(next.replicas[1].owner, replica_b_owner);
    assert_eq!(next.replicas[1].segment_name, SegmentName::new("seg-b"));
    assert_eq!(next.replicas[2].owner, target_c_owner);
    assert_eq!(next.replicas[2].segment_name, SegmentName::new("seg-c"));
    assert_eq!(next.replicas[3].owner, target_d_owner);
    assert_eq!(next.replicas[3].segment_name, SegmentName::new("seg-d"));
    assert_eq!(
        next.replicas
            .iter()
            .map(|replica| replica.priority)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[test]
fn explicit_move_route_delta_replaces_source_replica_with_target() {
    let source_owner = ClientRuntimeId::new("store-a", ClientEpoch(1));
    let replica_b_owner = ClientRuntimeId::new("store-b", ClientEpoch(1));
    let target_c_owner = ClientRuntimeId::new("store-c", ClientEpoch(1));
    let current = ObjectRoute {
        key: ObjectKey::new("tenant-a::move-key"),
        namespace: Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None)),
        logical_key: Some("move-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("tenant-a".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(3),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![
            ReplicaRoute {
                owner: source_owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                offset: 0,
                segment_offset: 0,
                length: 17,
                checksum: Some(17),
                tier: ReplicaTier::Dram,
                priority: 0,
            },
            ReplicaRoute {
                owner: replica_b_owner.clone(),
                segment_name: SegmentName::new("seg-b"),
                offset: 128,
                segment_offset: 128,
                length: 17,
                checksum: Some(17),
                tier: ReplicaTier::Dram,
                priority: 1,
            },
        ],
    };

    let next = StoreClient::build_explicit_move_route_delta(
        &current,
        &SegmentName::new("seg-a"),
        ReplicaRoute {
            owner: target_c_owner.clone(),
            segment_name: SegmentName::new("seg-c"),
            offset: 256,
            segment_offset: 256,
            length: 17,
            checksum: Some(17),
            tier: ReplicaTier::Dram,
            priority: 99,
        },
    )
    .expect("move delta should succeed");

    assert_eq!(next.version, RouteVersion(4));
    assert_eq!(next.replicas.len(), 2);
    assert_eq!(next.replicas[0].owner, replica_b_owner);
    assert_eq!(next.replicas[0].segment_name, SegmentName::new("seg-b"));
    assert_eq!(next.replicas[1].owner, target_c_owner);
    assert_eq!(next.replicas[1].segment_name, SegmentName::new("seg-c"));
    assert!(next
        .replicas
        .iter()
        .all(|replica| replica.segment_name != SegmentName::new("seg-a")));
    assert_eq!(
        next.replicas
            .iter()
            .map(|replica| replica.priority)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn explicit_route_delta_rejects_missing_source_or_duplicate_targets() {
    let source_owner = ClientRuntimeId::new("store-a", ClientEpoch(1));
    let current = ObjectRoute {
        key: ObjectKey::new("tenant-a::invalid-key"),
        namespace: Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None)),
        logical_key: Some("invalid-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("tenant-a".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(1),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: source_owner,
            segment_name: SegmentName::new("seg-a"),
            offset: 0,
            segment_offset: 0,
            length: 9,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
    };

    let missing_source = StoreClient::build_explicit_copy_route_delta(
        &current,
        &SegmentName::new("seg-missing"),
        vec![ReplicaRoute {
            owner: ClientRuntimeId::new("store-b", ClientEpoch(1)),
            segment_name: SegmentName::new("seg-b"),
            offset: 64,
            segment_offset: 64,
            length: 9,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 5,
        }],
    )
    .expect_err("missing source should fail");
    assert!(matches!(missing_source, StoreError::NotFound(_)));

    let duplicate_target = StoreClient::build_explicit_copy_route_delta(
        &current,
        &SegmentName::new("seg-a"),
        vec![ReplicaRoute {
            owner: ClientRuntimeId::new("store-b", ClientEpoch(1)),
            segment_name: SegmentName::new("seg-a"),
            offset: 64,
            segment_offset: 64,
            length: 9,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 5,
        }],
    )
    .expect_err("duplicate target segment should fail");
    assert!(matches!(duplicate_target, StoreError::Conflict(_)));

    let current_with_existing_target = ObjectRoute {
        replicas: vec![
            current.replicas[0].clone(),
            ReplicaRoute {
                owner: ClientRuntimeId::new("store-c", ClientEpoch(1)),
                segment_name: SegmentName::new("seg-b"),
                offset: 128,
                segment_offset: 128,
                length: 9,
                checksum: Some(9),
                tier: ReplicaTier::Dram,
                priority: 1,
            },
        ],
        ..current.clone()
    };
    let existing_target = StoreClient::build_explicit_copy_route_delta(
        &current_with_existing_target,
        &SegmentName::new("seg-a"),
        vec![ReplicaRoute {
            owner: ClientRuntimeId::new("store-b", ClientEpoch(1)),
            segment_name: SegmentName::new("seg-b"),
            offset: 128,
            segment_offset: 128,
            length: 9,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 5,
        }],
    )
    .expect_err("copy target segment that already exists in route should fail");
    assert!(matches!(existing_target, StoreError::Conflict(_)));

    let move_same_target = StoreClient::build_explicit_move_route_delta(
        &current,
        &SegmentName::new("seg-a"),
        ReplicaRoute {
            owner: ClientRuntimeId::new("store-b", ClientEpoch(1)),
            segment_name: SegmentName::new("seg-a"),
            offset: 64,
            segment_offset: 64,
            length: 9,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 5,
        },
    )
    .expect_err("move to same segment should fail");
    assert!(matches!(move_same_target, StoreError::Conflict(_)));
}

#[test]
fn explicit_migration_policy_rejects_partial_success() {
    let error = StoreClient::explicit_migration_policy(&ExplicitMigrationPlan {
        mode: ExplicitMigrationMode::Copy,
        source: ReplicaReadSelector::Segment(SegmentName::new("seg-a")),
        target_segments: vec![SegmentName::new("seg-b")],
        all_or_nothing: false,
    })
    .expect_err("partial-success explicit migration should be rejected");
    assert!(matches!(error, StoreError::Unsupported(_)));
}

#[test]
fn explicit_copy_migration_keeps_source_and_adds_target_replica() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("explicit-copy-store-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("explicit-copy-store-b-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("explicit-copy-reader-segment"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "explicit-copy-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "explicit-copy-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata, "explicit-copy-reader")
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

    let key = "explicit-copy-key";
    let payload = b"explicit-copy-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    assert_eq!(current.replicas.len(), 1);
    let source_segment = current.replicas[0].segment_name.clone();
    let source_owner = current.replicas[0].owner.clone();
    let target_segment = store_b
        .segment_name()
        .expect("store-b should expose a primary segment");
    let target_owner = store_b.runtime_id().clone();

    let object_id = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(store_b.default_tenant()), None, None),
        key,
    );
    let next = store_b
        .execute_explicit_route_migration(
            &object_id,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Copy,
                source: ReplicaReadSelector::Segment(source_segment.clone()),
                target_segments: vec![target_segment.clone()],
                all_or_nothing: true,
            },
        )
        .expect("explicit copy migration should succeed");

    assert_eq!(next.replicas.len(), 2);
    assert!(next
        .replicas
        .iter()
        .any(|replica| replica.owner == source_owner && replica.segment_name == source_segment));
    assert!(next
        .replicas
        .iter()
        .any(|replica| replica.owner == target_owner && replica.segment_name == target_segment));
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after copy should succeed"),
        payload
    );
}

#[test]
fn explicit_copy_migration_supports_multiple_targets_on_same_storage_runtime() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("explicit-copy-same-owner-store-a"));
    let store_b_transport = Arc::new(store_a_transport.peer("explicit-copy-same-owner-store-b"));
    let store_c_transport = Arc::new(store_a_transport.peer("explicit-copy-same-owner-store-c"));
    let reader_transport = Arc::new(store_a_transport.peer("explicit-copy-same-owner-reader"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "explicit-copy-same-owner-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "explicit-copy-same-owner-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport.clone())
        .transport_factory(store_b_transport.factory())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "explicit-copy-same-owner-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let reader = StoreClientBuilder::new(metadata, "explicit-copy-same-owner-reader")
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
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    let extra_segment = store_b
        .expand_local_memory(128)
        .expect("store-b should expose a second target segment");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &reader]);

    let key = "explicit-copy-same-owner-key";
    let payload = b"explicit-copy-same-owner-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.clone();
    let source_owner = current.replicas[0].owner.clone();
    let primary_target = store_b
        .segment_name()
        .expect("store-b should expose a primary segment");

    let object_id = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(store_b.default_tenant()), None, None),
        key,
    );
    let next = store_b
        .execute_explicit_route_migration(
            &object_id,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Copy,
                source: ReplicaReadSelector::Segment(source_segment.clone()),
                target_segments: vec![primary_target.clone(), extra_segment.segment_name.clone()],
                all_or_nothing: true,
            },
        )
        .expect("same-owner explicit copy migration should succeed");

    assert_eq!(next.replicas.len(), 3);
    assert!(next
        .replicas
        .iter()
        .any(|replica| replica.owner == source_owner && replica.segment_name == source_segment));
    assert!(next.replicas.iter().any(|replica| {
        replica.owner == *store_b.runtime_id() && replica.segment_name == primary_target
    }));
    assert!(next.replicas.iter().any(|replica| {
        replica.owner == *store_b.runtime_id() && replica.segment_name == extra_segment.segment_name
    }));
    assert!(
        next.replicas
            .iter()
            .all(|replica| replica.owner != *store_c.runtime_id()),
        "same-owner targets must not drift to fallback runtimes"
    );
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after same-owner copy should succeed"),
        payload
    );
}

#[test]
fn explicit_copy_migration_with_one_invalid_target_keeps_route_unchanged() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("explicit-copy-invalid-store-a"));
    let store_b_transport = Arc::new(store_a_transport.peer("explicit-copy-invalid-store-b"));
    let reader_transport = Arc::new(store_a_transport.peer("explicit-copy-invalid-reader"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "explicit-copy-invalid-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "explicit-copy-invalid-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata, "explicit-copy-invalid-reader")
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

    let key = "explicit-copy-invalid-key";
    let payload = b"explicit-copy-invalid-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.clone();
    let source_owner = current.replicas[0].owner.clone();
    let valid_target = store_b
        .segment_name()
        .expect("store-b should expose a primary segment");

    let object_id = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(store_b.default_tenant()), None, None),
        key,
    );
    let error = store_b
        .execute_explicit_route_migration(
            &object_id,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Copy,
                source: ReplicaReadSelector::Segment(source_segment.clone()),
                target_segments: vec![
                    valid_target.clone(),
                    SegmentName::new("missing-explicit-copy-target"),
                ],
                all_or_nothing: true,
            },
        )
        .expect_err("copy with one invalid target should fail");
    let error_text = error.to_string();
    assert!(
        error_text.contains("missing-explicit-copy-target") || error_text.contains("unavailable"),
        "unexpected error: {error_text}"
    );

    let route = reader
        .query_route(key)
        .expect("route query after failed copy should succeed")
        .expect("route should still exist after failed copy");
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, source_owner);
    assert_eq!(route.replicas[0].segment_name, source_segment);
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after failed copy should still succeed"),
        payload
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
                source: ReplicaReadSelector::Segment(source_segment.clone()),
                target_segments: vec![target_segment.clone()],
                all_or_nothing: true,
            },
        )
        .expect_err("CAS transport failure should fail the migration");
    assert!(
        error.to_string().contains("injected route cas failure"),
        "unexpected error: {error}"
    );

    let used_after = store_b
        .list_segments()
        .expect("post-failure segment list should succeed")
        .into_iter()
        .map(|segment| segment.used_bytes)
        .sum::<u64>();
    assert_eq!(
        used_after, used_before,
        "target reservations should be released when route CAS errors"
    );

    let route = reader
        .query_route(key)
        .expect("route query after failed copy should succeed")
        .expect("route should still exist after failed copy");
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, source_owner);
    assert_eq!(route.replicas[0].segment_name, source_segment);
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after failed CAS should still succeed"),
        payload
    );
}

#[test]
fn explicit_move_migration_replaces_source_with_target_replica() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("explicit-move-store-a-segment"));
    let store_b_transport = Arc::new(store_a_transport.peer("explicit-move-store-b-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("explicit-move-reader-segment"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "explicit-move-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "explicit-move-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata, "explicit-move-reader")
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

    let key = "explicit-move-key";
    let payload = b"explicit-move-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    assert_eq!(current.replicas.len(), 1);
    let source_segment = current.replicas[0].segment_name.clone();
    let source_owner = current.replicas[0].owner.clone();
    let target_segment = store_b
        .segment_name()
        .expect("store-b should expose a primary segment");
    let target_owner = store_b.runtime_id().clone();

    let object_id = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(store_b.default_tenant()), None, None),
        key,
    );
    let next = store_b
        .execute_explicit_route_migration(
            &object_id,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Move,
                source: ReplicaReadSelector::OwnerAndSegment {
                    owner: source_owner.clone(),
                    segment_name: source_segment.clone(),
                },
                target_segments: vec![target_segment.clone()],
                all_or_nothing: true,
            },
        )
        .expect("explicit move migration should succeed");

    assert_eq!(next.replicas.len(), 1);
    assert!(next.replicas.iter().all(|replica| {
        !(replica.owner == source_owner && replica.segment_name == source_segment)
    }));
    assert!(next
        .replicas
        .iter()
        .any(|replica| replica.owner == target_owner && replica.segment_name == target_segment));
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after move should succeed"),
        payload
    );
}

#[test]
fn explicit_copy_migration_same_key_same_source_only_one_concurrent_commit_wins() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner,
        "default::explicit-copy-race-key",
    ));
    let metadata: Arc<dyn mooncake_store_core::MetadataBackend> = blocking.clone();
    let store_a_transport = Arc::new(TestTransport::new("explicit-copy-race-store-a"));
    let store_b_transport = Arc::new(store_a_transport.peer("explicit-copy-race-store-b"));
    let store_c_transport = Arc::new(store_a_transport.peer("explicit-copy-race-store-c"));
    let reader_transport = Arc::new(store_a_transport.peer("explicit-copy-race-reader"));

    let store_a = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "explicit-copy-race-a")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "true")
            .label("route", "false")
            .route_control(RouteControlMode::MetadataOnly)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(store_a_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("store-a build should succeed"),
    );
    let store_b = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "explicit-copy-race-b")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "true")
            .label("route", "false")
            .route_control(RouteControlMode::MetadataOnly)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(store_b_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("store-b build should succeed"),
    );
    let store_c = Arc::new(
        StoreClientBuilder::new(metadata.clone(), "explicit-copy-race-c")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "true")
            .label("route", "false")
            .route_control(RouteControlMode::MetadataOnly)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(store_c_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("store-c build should succeed"),
    );
    let reader = Arc::new(
        StoreClientBuilder::new(metadata, "explicit-copy-race-reader")
            .state(ClientLifecycleState::Active)
            .label("pool", "pool-a")
            .label("storage", "false")
            .label("route", "false")
            .route_control(RouteControlMode::MetadataOnly)
            .live_client_sync_interval(fast_live_client_sync_interval())
            .transport(reader_transport)
            .local_memory(storage_config())
            .build(test_future_expiry_ms())
            .expect("reader build should succeed"),
    );

    store_a
        .register_local_memory()
        .expect("store-a memory should register");
    store_b
        .register_local_memory()
        .expect("store-b memory should register");
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[
        store_a.as_ref(),
        store_b.as_ref(),
        store_c.as_ref(),
        reader.as_ref(),
    ]);

    let key = "explicit-copy-race-key";
    let payload = b"explicit-copy-race-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.clone();
    let source_owner = current.replicas[0].owner.clone();
    let target_segment_b = store_b
        .segment_name()
        .expect("store-b should expose a primary segment");
    let target_segment_c = store_c
        .segment_name()
        .expect("store-c should expose a primary segment");
    let target_owner_b = store_b.runtime_id().clone();
    let target_owner_c = store_c.runtime_id().clone();
    let object_id = LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(store_b.default_tenant()), None, None),
        key,
    );
    let barrier = Arc::new(Barrier::new(3));

    let barrier_b = barrier.clone();
    let store_b_for_thread = store_b.clone();
    let object_id_b = object_id.clone();
    let source_segment_b = source_segment.clone();
    let source_owner_b = source_owner.clone();
    let target_segment_b_for_thread = target_segment_b.clone();
    let thread_b = std::thread::spawn(move || {
        barrier_b.wait();
        store_b_for_thread.execute_explicit_route_migration(
            &object_id_b,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Copy,
                source: ReplicaReadSelector::OwnerAndSegment {
                    owner: source_owner_b,
                    segment_name: source_segment_b,
                },
                target_segments: vec![target_segment_b_for_thread],
                all_or_nothing: true,
            },
        )
    });

    let barrier_c = barrier.clone();
    let store_c_for_thread = store_c.clone();
    let object_id_c = object_id.clone();
    let source_segment_c = source_segment.clone();
    let source_owner_c = source_owner.clone();
    let target_segment_c_for_thread = target_segment_c.clone();
    let thread_c = std::thread::spawn(move || {
        barrier_c.wait();
        store_c_for_thread.execute_explicit_route_migration(
            &object_id_c,
            &ExplicitMigrationPlan {
                mode: ExplicitMigrationMode::Copy,
                source: ReplicaReadSelector::OwnerAndSegment {
                    owner: source_owner_c,
                    segment_name: source_segment_c,
                },
                target_segments: vec![target_segment_c_for_thread],
                all_or_nothing: true,
            },
        )
    });

    blocking.arm_blocked_cas();
    barrier.wait();
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "one concurrent copy should reach the blocked CAS point"
    );
    sleep(Duration::from_millis(100));
    blocking.release_blocked_cas();

    let result_b = thread_b.join().expect("copy thread b should join");
    let result_c = thread_c.join().expect("copy thread c should join");
    let success_count = usize::from(result_b.is_ok()) + usize::from(result_c.is_ok());
    assert_eq!(
        success_count, 1,
        "exactly one concurrent copy should succeed"
    );
    let failure = if let Err(error) = &result_b {
        Some(error.to_string())
    } else if let Err(error) = &result_c {
        Some(error.to_string())
    } else {
        None
    }
    .expect("one concurrent copy should fail");
    assert!(
        failure.contains("route") || failure.contains("conflict") || failure.contains("CAS"),
        "unexpected loser error: {failure}"
    );

    let route = reader
        .query_route(key)
        .expect("route query after concurrent copy should succeed")
        .expect("route should exist after concurrent copy");
    assert_eq!(route.replicas.len(), 2);
    assert!(
        route
            .replicas
            .iter()
            .any(|replica| replica.owner == source_owner && replica.segment_name == source_segment),
        "source replica should remain after the winning copy"
    );
    let moved_to_b = route
        .replicas
        .iter()
        .any(|replica| replica.owner == target_owner_b && replica.segment_name == target_segment_b);
    let moved_to_c = route
        .replicas
        .iter()
        .any(|replica| replica.owner == target_owner_c && replica.segment_name == target_segment_c);
    assert_ne!(
        moved_to_b, moved_to_c,
        "exactly one target should win the race"
    );
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after concurrent copy should succeed"),
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
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Move as i32,
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
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )
            .expect("migration status query should succeed");
        match state {
            crate::control_plane::pb::MigrationExecutionState::Succeeded => break,
            crate::control_plane::pb::MigrationExecutionState::Failed => {
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

#[test]
fn control_plane_submit_migration_task_executes_explicit_copy_to_multiple_targets() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cp-copy-store-a-segment"));
    let executor_factory = store_a_transport.factory();
    let store_b_transport = Arc::new(store_a_transport.peer("cp-copy-store-b-segment"));
    let store_c_transport = Arc::new(store_a_transport.peer("cp-copy-store-c-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-copy-reader-segment"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-copy-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-copy-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .transport_factory(executor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "cp-copy-store-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-copy-reader")
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
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &reader]);

    let key = "cp-copy-key";
    let payload = b"cp-copy-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    assert_eq!(current.replicas.len(), 1);
    let source_owner = current.replicas[0].owner.clone();
    let source_segment = current.replicas[0].segment_name.0.clone();
    let target_segment_b = store_b
        .segment_name()
        .expect("store-b should expose a primary segment")
        .0;
    let target_segment_c = store_c
        .segment_name()
        .expect("store-c should expose a primary segment")
        .0;
    let executor_lease = store_b.lease();
    let execution_id = reader
        .control_client
        .submit_migration_task(
            &executor_lease,
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Copy as i32,
                source_segment,
                target_segments: vec![target_segment_b.clone(), target_segment_c.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("copy migration task submit should succeed");
    assert!(!execution_id.is_empty());

    let started = Instant::now();
    loop {
        let state = reader
            .control_client
            .get_migration_execution_status(
                &executor_lease,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )
            .expect("copy migration status query should succeed");
        match state {
            crate::control_plane::pb::MigrationExecutionState::Succeeded => break,
            crate::control_plane::pb::MigrationExecutionState::Failed => {
                panic!("copy migration task should not fail")
            }
            _ => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "copy migration task should complete quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    }

    let route = reader
        .query_route(key)
        .expect("post-copy route query should succeed")
        .expect("post-copy route should exist");
    assert_eq!(route.replicas.len(), 3);
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == source_owner));
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == *store_b.runtime_id()
            && replica.segment_name == SegmentName::new(target_segment_b.clone())));
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == *store_c.runtime_id()
            && replica.segment_name == SegmentName::new(target_segment_c.clone())));
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after control-plane copy should succeed"),
        payload
    );
}

#[test]
fn control_plane_submit_migration_task_executes_explicit_copy_to_multiple_targets_on_same_runtime()
{
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cp-copy-same-owner-store-a"));
    let store_b_transport = Arc::new(store_a_transport.peer("cp-copy-same-owner-store-b"));
    let store_c_transport = Arc::new(store_a_transport.peer("cp-copy-same-owner-store-c"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-copy-same-owner-reader"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-copy-same-owner-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-copy-same-owner-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport.clone())
        .transport_factory(store_b_transport.factory())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "cp-copy-same-owner-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-copy-same-owner-reader")
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
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    let extra_segment = store_b
        .expand_local_memory(128)
        .expect("store-b should expose a second target segment");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &reader]);

    let key = "cp-copy-same-owner-key";
    let payload = b"cp-copy-same-owner-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_owner = current.replicas[0].owner.clone();
    let source_segment = current.replicas[0].segment_name.0.clone();
    let primary_target = store_b
        .segment_name()
        .expect("store-b should expose a primary segment")
        .0;
    let executor_lease = store_b.lease();
    let execution_id = reader
        .control_client
        .submit_migration_task(
            &executor_lease,
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Copy as i32,
                source_segment,
                target_segments: vec![primary_target.clone(), extra_segment.segment_name.0.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("copy migration task submit should succeed");
    assert!(!execution_id.is_empty());

    let started = Instant::now();
    loop {
        let detail = reader
            .control_client
            .get_migration_execution_status_detail(
                &executor_lease,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )
            .expect("copy migration status query should succeed");
        let state = crate::control_plane::pb::MigrationExecutionState::try_from(detail.state)
            .expect("copy migration status should be valid");
        match state {
            crate::control_plane::pb::MigrationExecutionState::Succeeded => break,
            crate::control_plane::pb::MigrationExecutionState::Failed => {
                panic!(
                    "same-owner copy migration task should not fail: {}",
                    detail.last_error
                )
            }
            _ => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "same-owner copy migration task should complete quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    }

    let route = reader
        .query_route(key)
        .expect("post-copy route query should succeed")
        .expect("post-copy route should exist");
    assert_eq!(route.replicas.len(), 3);
    assert!(route
        .replicas
        .iter()
        .any(|replica| replica.owner == source_owner));
    assert!(route.replicas.iter().any(|replica| {
        replica.owner == *store_b.runtime_id()
            && replica.segment_name == SegmentName::new(primary_target.clone())
    }));
    assert!(route.replicas.iter().any(|replica| {
        replica.owner == *store_b.runtime_id() && replica.segment_name == extra_segment.segment_name
    }));
    assert!(
        route
            .replicas
            .iter()
            .all(|replica| replica.owner != *store_c.runtime_id()),
        "control-plane same-owner targets must not drift to fallback runtimes"
    );
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after same-owner control-plane copy should succeed"),
        payload
    );
}

#[test]
fn control_plane_submit_migration_task_surfaces_failed_execution_status_detail() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cp-failed-store-a-segment"));
    let executor_factory = store_a_transport.factory();
    let store_b_transport = Arc::new(store_a_transport.peer("cp-failed-store-b-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-failed-reader-segment"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-failed-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-failed-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .transport_factory(executor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-failed-reader")
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

    let key = "cp-failed-key";
    let payload = b"cp-failed-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.0.clone();
    let executor_lease = store_b.lease();
    let execution_id = reader
        .control_client
        .submit_migration_task(
            &executor_lease,
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Copy as i32,
                source_segment,
                target_segments: vec!["missing-target-segment".to_string()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("failed migration task submit should still succeed");
    assert!(!execution_id.is_empty());

    let started = Instant::now();
    let detail = loop {
        let detail = reader
            .control_client
            .get_migration_execution_status_detail(
                &executor_lease,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )
            .expect("failed migration status query should succeed");
        match crate::control_plane::pb::MigrationExecutionState::try_from(detail.state)
            .expect("state should decode")
        {
            crate::control_plane::pb::MigrationExecutionState::Failed => break detail,
            crate::control_plane::pb::MigrationExecutionState::Succeeded => {
                panic!("failed migration task should not succeed")
            }
            _ => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "failed migration task should complete quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    };

    assert_eq!(detail.attempts, 1);
    assert!(
        detail.last_error.contains("missing-target-segment")
            || detail.last_error.contains("unavailable"),
        "unexpected last_error: {}",
        detail.last_error
    );
    let route = reader
        .query_route(key)
        .expect("route query after failed migration should succeed")
        .expect("route should still exist after failed migration");
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after failed migration should still succeed"),
        payload
    );
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
        &crate::control_plane::pb::SubmitMigrationTaskRequest {
            namespace: "default".to_string(),
            authority: "closed-executor".to_string(),
            tenant: "default".to_string(),
            key: "closed-key".to_string(),
            mode: crate::control_plane::pb::MigrationMode::Copy as i32,
            source_segment: "source-segment".to_string(),
            target_segments: vec!["target-segment".to_string()],
            task_executor: "closed-executor".to_string(),
            max_retries: 1,
            domain: String::new(),
            object_set: String::new(),
        },
    )
    .expect_err("closed worker channel should reject new tasks");
    assert!(matches!(error, StoreError::Transport(_)));

    let record = executions
        .lock()
        .get("closed-executor-1")
        .cloned()
        .expect("failed submission should still leave an execution record");
    assert_eq!(
        record.state,
        crate::control_plane::pb::MigrationExecutionState::Failed
    );
    assert_eq!(record.attempts, 0);
    assert!(
        record.last_error.contains("not accepting new tasks"),
        "unexpected last_error: {}",
        record.last_error
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
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Move as i32,
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
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: first_execution.clone(),
                },
            )
            .expect("first migration status query should succeed");
        match crate::control_plane::pb::MigrationExecutionState::try_from(detail.state)
            .expect("state should decode")
        {
            crate::control_plane::pb::MigrationExecutionState::Failed => break detail,
            crate::control_plane::pb::MigrationExecutionState::Succeeded => {
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
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Move as i32,
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
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: second_execution.clone(),
                },
            )
            .expect("second migration status query should succeed");
        match state {
            crate::control_plane::pb::MigrationExecutionState::Succeeded => break,
            crate::control_plane::pb::MigrationExecutionState::Failed => {
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
fn control_plane_submit_copy_task_same_key_only_one_executor_wins() {
    let inner = Arc::new(InMemoryMetadataBackend::new());
    let blocking = Arc::new(BlockingCasMetadataBackend::new(
        inner,
        "default::cp-concurrent-key",
    ));
    let metadata: Arc<dyn mooncake_store_core::MetadataBackend> = blocking.clone();
    let store_a_transport = Arc::new(TestTransport::new("cp-concurrent-store-a"));
    let executor_factory = store_a_transport.factory();
    let store_b_transport = Arc::new(store_a_transport.peer("cp-concurrent-store-b"));
    let store_c_transport = Arc::new(store_a_transport.peer("cp-concurrent-store-c"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-concurrent-reader"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-concurrent-a")
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
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-concurrent-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .transport_factory(executor_factory.clone())
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let store_c = StoreClientBuilder::new(metadata.clone(), "cp-concurrent-c")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .label("route", "false")
        .route_control(RouteControlMode::MetadataOnly)
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_c_transport)
        .transport_factory(executor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-c build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-concurrent-reader")
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
    store_c
        .register_local_memory()
        .expect("store-c memory should register");
    reader
        .register_local_memory()
        .expect("reader memory should register");
    wait_for_membership_convergence(&[&store_a, &store_b, &store_c, &reader]);

    let key = "cp-concurrent-key";
    let payload = b"cp-concurrent-payload";
    store_a.put(key, payload).expect("seed put should succeed");
    let current = store_a
        .query_route(key)
        .expect("initial route query should succeed")
        .expect("initial route should exist");
    let source_segment = current.replicas[0].segment_name.0.clone();
    let source_segment_name = current.replicas[0].segment_name.clone();
    let target_segment_b = store_b
        .segment_name()
        .expect("store-b should expose a primary segment")
        .0;
    let target_segment_c = store_c
        .segment_name()
        .expect("store-c should expose a primary segment")
        .0;
    let executor_lease_b = store_b.lease();
    let executor_lease_c = store_c.lease();
    blocking.arm_blocked_cas();
    let execution_id_b = reader
        .control_client
        .submit_migration_task(
            &executor_lease_b,
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Copy as i32,
                source_segment: source_segment.clone(),
                target_segments: vec![target_segment_b.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("migration task submit to executor b should succeed");
    let execution_id_c = reader
        .control_client
        .submit_migration_task(
            &executor_lease_c,
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_c.runtime_id().stable_id.0.clone(),
                tenant: "default".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Copy as i32,
                source_segment,
                target_segments: vec![target_segment_c.clone()],
                task_executor: store_c.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("migration task submit to executor c should succeed");
    assert!(!execution_id_b.is_empty());
    assert!(!execution_id_c.is_empty());
    assert!(
        blocking.wait_until_blocked(Duration::from_secs(1)),
        "one control-plane copy should reach the blocked CAS point"
    );
    sleep(Duration::from_millis(100));
    blocking.release_blocked_cas();

    let started = Instant::now();
    let detail_b = loop {
        let detail = reader
            .control_client
            .get_migration_execution_status_detail(
                &executor_lease_b,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id_b.clone(),
                },
            )
            .expect("status query for executor b should succeed");
        let state = crate::control_plane::pb::MigrationExecutionState::try_from(detail.state)
            .expect("state should decode");
        if matches!(
            state,
            crate::control_plane::pb::MigrationExecutionState::Succeeded
                | crate::control_plane::pb::MigrationExecutionState::Failed
        ) {
            break detail;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "executor b migration should finish"
        );
        sleep(Duration::from_millis(10));
    };
    let detail_c = loop {
        let detail = reader
            .control_client
            .get_migration_execution_status_detail(
                &executor_lease_c,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_c.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id_c.clone(),
                },
            )
            .expect("status query for executor c should succeed");
        let state = crate::control_plane::pb::MigrationExecutionState::try_from(detail.state)
            .expect("state should decode");
        if matches!(
            state,
            crate::control_plane::pb::MigrationExecutionState::Succeeded
                | crate::control_plane::pb::MigrationExecutionState::Failed
        ) {
            break detail;
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "executor c migration should finish"
        );
        sleep(Duration::from_millis(10));
    };
    let state_b = crate::control_plane::pb::MigrationExecutionState::try_from(detail_b.state)
        .expect("executor b state should decode");
    let state_c = crate::control_plane::pb::MigrationExecutionState::try_from(detail_c.state)
        .expect("executor c state should decode");
    assert_ne!(
        state_b, state_c,
        "one concurrent control-plane copy must fail"
    );
    let loser_error = if matches!(
        state_b,
        crate::control_plane::pb::MigrationExecutionState::Failed
    ) {
        detail_b.last_error
    } else {
        detail_c.last_error
    };
    assert!(
        loser_error.contains("route")
            || loser_error.contains("conflict")
            || loser_error.contains("CAS"),
        "unexpected loser error: {loser_error}"
    );

    let route = reader
        .query_route(key)
        .expect("route query after concurrent control-plane copy should succeed")
        .expect("route should exist after concurrent control-plane copy");
    assert_eq!(route.replicas.len(), 2);
    assert!(
        route
            .replicas
            .iter()
            .any(|replica| replica.segment_name == source_segment_name),
        "source replica should remain after the winning copy"
    );
    let moved_to_b = route.replicas.iter().any(|replica| {
        replica.owner == *store_b.runtime_id()
            && replica.segment_name == SegmentName::new(target_segment_b.clone())
    });
    let moved_to_c = route.replicas.iter().any(|replica| {
        replica.owner == *store_c.runtime_id()
            && replica.segment_name == SegmentName::new(target_segment_c.clone())
    });
    assert_ne!(
        moved_to_b, moved_to_c,
        "exactly one target should win the race"
    );
    assert_eq!(
        reader
            .get(key)
            .expect("reader get after concurrent control-plane copy should succeed"),
        payload
    );
}

#[test]
fn control_plane_submit_migration_task_preserves_namespace_scope() {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let store_a_transport = Arc::new(TestTransport::new("cp-scope-store-a-segment"));
    let executor_factory = store_a_transport.factory();
    let store_b_transport = Arc::new(store_a_transport.peer("cp-scope-store-b-segment"));
    let reader_transport = Arc::new(store_a_transport.peer("cp-scope-reader-segment"));
    let scope = NamespaceScope::with_defaults(Some("tenant-a"), Some("domain-a"), Some("set-a"));

    let store_a = StoreClientBuilder::new(metadata.clone(), "cp-scope-store-a")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_a_transport)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-a build should succeed");
    let store_b = StoreClientBuilder::new(metadata.clone(), "cp-scope-store-b")
        .state(ClientLifecycleState::Active)
        .label("pool", "pool-a")
        .label("storage", "true")
        .live_client_sync_interval(fast_live_client_sync_interval())
        .transport(store_b_transport)
        .transport_factory(executor_factory)
        .local_memory(storage_config())
        .build(test_future_expiry_ms())
        .expect("store-b build should succeed");
    let reader = StoreClientBuilder::new(metadata.clone(), "cp-scope-reader")
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

    let key = "cp-scope-key";
    let payload = b"cp-scope-payload";
    store_a
        .batch_put(&[PutRequest::new(key, payload)
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")])
        .expect("seed scoped put should succeed");
    let current = store_a
        .query_route_in_scope(&scope, key)
        .expect("initial scoped route query should succeed")
        .expect("initial scoped route should exist");
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
            crate::control_plane::pb::SubmitMigrationTaskRequest {
                namespace: metadata.route_namespace(),
                authority: store_b.runtime_id().stable_id.0.clone(),
                tenant: "tenant-a".to_string(),
                domain: "domain-a".to_string(),
                object_set: "set-a".to_string(),
                key: key.to_string(),
                mode: crate::control_plane::pb::MigrationMode::Move as i32,
                source_segment,
                target_segments: vec![target_segment.clone()],
                task_executor: store_b.runtime_id().stable_id.0.clone(),
                max_retries: 1,
            },
        )
        .expect("scoped migration task submit should succeed");
    assert!(!execution_id.is_empty());

    let started = Instant::now();
    loop {
        let state = reader
            .control_client
            .get_migration_execution_status(
                &executor_lease,
                crate::control_plane::pb::GetMigrationExecutionStatusRequest {
                    namespace: metadata.route_namespace(),
                    authority: store_b.runtime_id().stable_id.0.clone(),
                    execution_id: execution_id.clone(),
                },
            )
            .expect("scoped migration status query should succeed");
        match state {
            crate::control_plane::pb::MigrationExecutionState::Succeeded => break,
            crate::control_plane::pb::MigrationExecutionState::Failed => {
                panic!("scoped migration task should not fail")
            }
            _ => {
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "scoped migration task should complete quickly"
                );
                sleep(Duration::from_millis(10));
            }
        }
    }

    let route = reader
        .query_route_in_scope(&scope, key)
        .expect("post-migration scoped route query should succeed")
        .expect("post-migration scoped route should exist");
    assert_eq!(route.namespace.as_ref(), Some(&scope));
    assert_eq!(route.replicas.len(), 1);
    assert_eq!(route.replicas[0].owner, *store_b.runtime_id());
    assert_eq!(
        route.replicas[0].segment_name,
        SegmentName::new(target_segment)
    );
    let values = reader
        .batch_get(&[ObjectRef::new(key)
            .tenant("tenant-a")
            .domain("domain-a")
            .object_set("set-a")])
        .expect("scoped batch_get should succeed");
    assert_eq!(values[0], payload);
}

#[test]
fn explicit_source_selector_resolves_route_replica_by_segment_and_owner() {
    let source_owner = ClientRuntimeId::new("store-a", ClientEpoch(1));
    let replica_b_owner = ClientRuntimeId::new("store-b", ClientEpoch(1));
    let current = ObjectRoute {
        key: ObjectKey::new("tenant-a::selector-key"),
        namespace: Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None)),
        logical_key: Some("selector-key".to_string()),
        canonical_key: None,
        sharing_scope: Some("tenant-a".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(2),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![
            ReplicaRoute {
                owner: source_owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                offset: 0,
                segment_offset: 0,
                length: 13,
                checksum: Some(13),
                tier: ReplicaTier::Dram,
                priority: 0,
            },
            ReplicaRoute {
                owner: replica_b_owner.clone(),
                segment_name: SegmentName::new("seg-b"),
                offset: 128,
                segment_offset: 128,
                length: 13,
                checksum: Some(13),
                tier: ReplicaTier::Dram,
                priority: 1,
            },
        ],
    };

    let by_segment = StoreClient::resolve_explicit_source_replica(
        &current,
        &ReplicaReadSelector::Segment(SegmentName::new("seg-a")),
    )
    .expect("segment selector should resolve");
    assert_eq!(by_segment.owner, source_owner);
    assert_eq!(by_segment.segment_name, SegmentName::new("seg-a"));

    let by_owner_and_segment = StoreClient::resolve_explicit_source_replica(
        &current,
        &ReplicaReadSelector::OwnerAndSegment {
            owner: replica_b_owner.clone(),
            segment_name: SegmentName::new("seg-b"),
        },
    )
    .expect("owner+segment selector should resolve");
    assert_eq!(by_owner_and_segment.owner, replica_b_owner);
    assert_eq!(by_owner_and_segment.segment_name, SegmentName::new("seg-b"));
}

#[test]
fn explicit_source_selector_rejects_missing_or_owner_mismatched_replicas() {
    let source_owner = ClientRuntimeId::new("store-a", ClientEpoch(1));
    let current = ObjectRoute {
        key: ObjectKey::new("tenant-a::selector-invalid"),
        namespace: Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None)),
        logical_key: Some("selector-invalid".to_string()),
        canonical_key: None,
        sharing_scope: Some("tenant-a".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(5),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: source_owner.clone(),
            segment_name: SegmentName::new("seg-a"),
            offset: 0,
            segment_offset: 0,
            length: 7,
            checksum: Some(7),
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
    };

    let missing = StoreClient::resolve_explicit_source_replica(
        &current,
        &ReplicaReadSelector::Segment(SegmentName::new("seg-missing")),
    )
    .expect_err("missing selector should fail");
    assert!(matches!(missing, StoreError::NotFound(_)));

    let owner_mismatch = StoreClient::resolve_explicit_source_replica(
        &current,
        &ReplicaReadSelector::OwnerAndSegment {
            owner: ClientRuntimeId::new("store-b", ClientEpoch(1)),
            segment_name: SegmentName::new("seg-a"),
        },
    )
    .expect_err("owner mismatch should fail");
    assert!(matches!(owner_mismatch, StoreError::NotFound(_)));
}
