use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};

use super::{
    control_address, control_address_label, decode_error, ensure_batch_len, fail_stream_session,
    normalize_control_uri, pb_cas_result, pb_compatibility, pb_error, pb_object_route,
    pb_replica_route, pb_replica_tier, pb_route_state, pb_runtime_id, pb_segment_reservation,
    status_to_store_error, store_error_from_pb, try_cas_result, try_compatibility,
    try_object_route, try_replica_route, try_replica_tier, try_route_state, try_runtime_id,
    try_segment_reservation, AllocatorService, AuthorityService, ControlPlaneClient,
    ControlPlaneHandle, ControlStreamSession, ReleaseOp, ReserveSpecificOp,
};
use crate::control_plane::pb;
use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier,
    RouteCasRequest, RouteState, RouteVersion, SegmentName, SegmentReservation, StoreError,
};
use tonic::Status;

#[derive(Default)]
struct TestAuthority {
    routes: Mutex<BTreeMap<String, ObjectRoute>>,
}

impl TestAuthority {
    fn insert(&self, route: ObjectRoute) {
        self.routes.lock().insert(route.key.0.clone(), route);
    }
}

impl AuthorityService for TestAuthority {
    fn get_route(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        key: &ObjectKey,
    ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
        Ok(self.routes.lock().get(&key.0).cloned())
    }

    fn list_routes_by_replica_owner(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
        Ok(self
            .routes
            .lock()
            .values()
            .filter(|route| route.replicas.iter().any(|replica| &replica.owner == owner))
            .cloned()
            .collect())
    }

    fn compare_and_swap_route(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<CasResult> {
        let mut routes = self.routes.lock();
        let current = routes.get(&key.0).cloned();
        let current_version = current.as_ref().map(|route| route.version);
        let applied = current_version == expected;
        if applied {
            match next {
                Some(route) => {
                    routes.insert(key.0.clone(), route.clone());
                }
                None => {
                    routes.remove(&key.0);
                }
            }
        }
        Ok(CasResult { applied, current })
    }

    fn replace_route(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> mooncake_store_core::Result<()> {
        let mut routes = self.routes.lock();
        match next {
            Some(route) => {
                routes.insert(key.0.clone(), route.clone());
            }
            None => {
                routes.remove(&key.0);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct TestAllocator {
    next_offset: Mutex<u64>,
    released: Mutex<Vec<ReleaseOp>>,
}

impl AllocatorService for TestAllocator {
    fn reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<SegmentReservation> {
        let mut next_offset = self.next_offset.lock();
        let offset_bytes = *next_offset;
        *next_offset += length_bytes.max(1);
        Ok(SegmentReservation {
            owner: owner.clone(),
            segment_name: SegmentName::new("auto-segment"),
            offset_bytes,
            length_bytes,
        })
    }

    fn reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<SegmentReservation> {
        Ok(SegmentReservation {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            offset_bytes: 4_096,
            length_bytes,
        })
    }

    fn release(
        &self,
        _owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> mooncake_store_core::Result<()> {
        self.released.lock().push(ReleaseOp {
            segment_name: segment_name.clone(),
            offset_bytes,
            length_bytes,
        });
        Ok(())
    }
}

fn sample_owner() -> ClientRuntimeId {
    ClientRuntimeId::new("writer", ClientEpoch(7))
}

fn sample_route(key: &str, version: u64, owner: &ClientRuntimeId) -> ObjectRoute {
    ObjectRoute {
        key: ObjectKey::new(key),
        version: RouteVersion(version),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: owner.clone(),
            segment_name: SegmentName::new("segment-a"),
            offset: 128,
            segment_offset: 128,
            length: 16,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 1,
        }],
    }
}

fn sample_lease(address: &str) -> ClientLease {
    let mut endpoints = ClientEndpointSet {
        rpc_address: address.to_string(),
        segment_name: Some(SegmentName::new("segment-a")),
        labels: BTreeMap::new(),
    };
    endpoints
        .labels
        .insert(control_address_label().to_string(), address.to_string());
    ClientLease {
        runtime: ClientRuntimeId::new("authority", ClientEpoch(1)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints,
        expires_at_ms: 10_000,
    }
}

#[test]
fn control_plane_client_round_trips_routes_and_allocator_calls() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = ControlPlaneHandle::spawn("127.0.0.1", authority.clone(), allocator.clone())
        .expect("control plane server should start");
    let lease = sample_lease(handle.address());
    let client = ControlPlaneClient::new().expect("control plane client should start");
    let authority_id = ClientStableId::new("authority");

    let get_results = client
        .batch_get_routes(
            &lease,
            "ns-a",
            &authority_id,
            &[ObjectKey::new("alpha"), ObjectKey::new("missing")],
        )
        .expect("batch get should succeed");
    assert_eq!(get_results.len(), 2);
    assert_eq!(
        get_results[0]
            .as_ref()
            .expect("first route should decode")
            .as_ref()
            .map(|route| route.key.0.as_str()),
        Some("alpha")
    );
    assert!(get_results[1]
        .as_ref()
        .expect("second route should decode")
        .is_none());
    assert_eq!(client.active_stream_sessions(), 1);

    let cas_results = client
        .batch_compare_and_swap_routes(
            &lease,
            "ns-a",
            &authority_id,
            &[
                RouteCasRequest {
                    key: ObjectKey::new("alpha"),
                    expected: Some(RouteVersion(1)),
                    next: Some(sample_route("alpha", 2, &owner)),
                },
                RouteCasRequest {
                    key: ObjectKey::new("beta"),
                    expected: Some(RouteVersion(3)),
                    next: Some(sample_route("beta", 1, &owner)),
                },
            ],
        )
        .expect("batch cas should succeed");
    assert!(
        cas_results[0]
            .as_ref()
            .expect("first cas should decode")
            .applied
    );
    assert!(
        !cas_results[1]
            .as_ref()
            .expect("second cas should decode")
            .applied
    );

    let replace_results = client
        .batch_replace_routes(
            &lease,
            "ns-a",
            &authority_id,
            &[
                RouteCasRequest {
                    key: ObjectKey::new("beta"),
                    expected: None,
                    next: Some(sample_route("beta", 1, &owner)),
                },
                RouteCasRequest {
                    key: ObjectKey::new("alpha"),
                    expected: None,
                    next: None,
                },
            ],
        )
        .expect("batch replace should succeed");
    assert!(replace_results[0].is_ok());
    assert!(replace_results[1].is_ok());

    let listed = client
        .list_routes_by_replica_owner(&lease, "ns-a", &authority_id, &owner)
        .expect("list by owner should succeed");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key.0, "beta");

    let single_reservation = client
        .reserve_any(&lease, &owner, 32)
        .expect("single reserve_any should succeed");
    assert_eq!(single_reservation.length_bytes, 32);

    let batch_any = client
        .batch_reserve_any(&lease, &owner, &[8, 16])
        .expect("batch reserve_any should succeed");
    assert_eq!(batch_any.len(), 2);
    assert_eq!(
        batch_any[1]
            .as_ref()
            .expect("batch reserve_any item should decode")
            .length_bytes,
        16
    );

    let single_specific = client
        .reserve_specific(&lease, &owner, &SegmentName::new("segment-b"), 24)
        .expect("single reserve_specific should succeed");
    assert_eq!(single_specific.segment_name.0, "segment-b");

    let batch_specific = client
        .batch_reserve_specific(
            &lease,
            &owner,
            &[ReserveSpecificOp {
                segment_name: SegmentName::new("segment-c"),
                length_bytes: 48,
            }],
        )
        .expect("batch reserve_specific should succeed");
    assert_eq!(
        batch_specific[0]
            .as_ref()
            .expect("batch reserve_specific item should decode")
            .segment_name
            .0,
        "segment-c"
    );

    let release_results = client
        .batch_release(
            &lease,
            &owner,
            &[ReleaseOp {
                segment_name: SegmentName::new("segment-c"),
                offset_bytes: 4_096,
                length_bytes: 48,
            }],
        )
        .expect("batch release should succeed");
    assert!(release_results[0].is_ok());
    assert_eq!(allocator.released.lock().len(), 1);

    client.clear_channels();
    assert_eq!(client.active_stream_sessions(), 0);
    handle.shutdown();
}

#[test]
fn control_plane_helpers_round_trip_and_report_validation_errors() {
    decode_error(None).expect("missing error detail should decode");
    let decoded = decode_error(Some(pb_error(StoreError::Conflict("boom".to_string()))))
        .expect_err("error detail should decode into store error");
    assert!(matches!(decoded, StoreError::Conflict(_)));

    ensure_batch_len("get", 2, 2).expect("matching lengths should pass");
    let error = ensure_batch_len("get", 2, 1).expect_err("mismatched lengths must fail");
    assert!(matches!(error, StoreError::Transport(_)));

    assert_eq!(
        normalize_control_uri("127.0.0.1:9000"),
        "http://127.0.0.1:9000"
    );
    assert_eq!(
        normalize_control_uri("https://control.local"),
        "https://control.local"
    );

    let lease = ClientLease {
        runtime: ClientRuntimeId::new("lease", ClientEpoch(2)),
        state: ClientLifecycleState::Active,
        compatibility: CompatibilityDescriptor::default(),
        endpoints: ClientEndpointSet::default(),
        expires_at_ms: 1_000,
    };
    assert!(matches!(
        control_address(&lease),
        Err(StoreError::Unsupported(_))
    ));

    let runtime = sample_owner();
    assert_eq!(
        try_runtime_id(&pb_runtime_id(&runtime)).expect("runtime should round-trip"),
        runtime
    );

    let compatibility = CompatibilityDescriptor::default();
    assert_eq!(
        try_compatibility(&pb_compatibility(&compatibility)),
        compatibility
    );

    assert_eq!(
        try_replica_tier(pb_replica_tier(ReplicaTier::Nvme)).expect("tier should round-trip"),
        ReplicaTier::Nvme
    );
    assert_eq!(
        try_route_state(pb_route_state(RouteState::Deleting)).expect("state should round-trip"),
        RouteState::Deleting
    );

    let route = sample_route("gamma", 5, &runtime);
    assert_eq!(
        try_object_route(pb_object_route(&route)).expect("route should round-trip"),
        route
    );

    let cas = CasResult {
        applied: true,
        current: Some(route.clone()),
    };
    assert_eq!(
        try_cas_result(pb_cas_result(&cas)).expect("cas result should round-trip"),
        cas
    );

    let reservation = SegmentReservation {
        owner: runtime.clone(),
        segment_name: SegmentName::new("segment-r"),
        offset_bytes: 7,
        length_bytes: 11,
    };
    assert_eq!(
        try_segment_reservation(pb_segment_reservation(&reservation))
            .expect("reservation should round-trip"),
        reservation
    );

    let replica_error = try_replica_route(pb::ReplicaRoute {
        owner: None,
        segment_name: "segment-a".to_string(),
        offset: 0,
        segment_offset: 0,
        length: 1,
        checksum: None,
        tier: pb_replica_tier(ReplicaTier::Dram),
        priority: 1,
    })
    .expect_err("replica without owner must fail");
    assert!(matches!(replica_error, StoreError::Transport(_)));

    let status_error = status_to_store_error(Status::aborted("rpc down"));
    assert!(matches!(status_error, StoreError::Transport(_)));
    let round_trip_error = store_error_from_pb(pb_error(StoreError::Metadata("oops".to_string())));
    assert!(matches!(round_trip_error, StoreError::Metadata(_)));
    let pb_replica = pb_replica_route(&route.replicas[0]);
    assert_eq!(
        try_replica_route(pb_replica).expect("replica should round-trip"),
        route.replicas[0]
    );
}

#[test]
fn fail_stream_session_marks_session_closed_and_drains_waiters() {
    let (sender, _receiver) = mpsc::channel(1);
    let (reply_tx, reply_rx) = oneshot::channel();
    let session = Arc::new(ControlStreamSession {
        sender,
        pending: Arc::new(Mutex::new(BTreeMap::from([(7, reply_tx)]))),
        next_request_id: 7.into(),
        closed: false.into(),
    });

    fail_stream_session(&session, "stream exploded".to_string());

    let error = reply_rx
        .blocking_recv()
        .expect("pending waiter should receive a result")
        .expect_err("failed session should surface store error");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(session.closed.load(Ordering::Relaxed));
    assert!(session.pending.lock().is_empty());
}
