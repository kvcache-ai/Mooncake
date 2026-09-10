use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

use super::{
    control_address, control_address_label, control_plane_server_threads_from_env, decode_error,
    ensure_batch_len, fail_stream_session, handle_control_stream_request, normalize_control_uri,
    pb_cas_result, pb_cold_backing_route, pb_compatibility, pb_error, pb_object_route,
    pb_replica_route, pb_replica_tier, pb_route_state, pb_runtime_id, pb_segment_reservation,
    status_to_store_error, store_error_from_pb, try_cas_result, try_cold_backing_route,
    try_compatibility, try_object_route, try_replica_route, try_replica_tier, try_route_state,
    try_runtime_id, try_segment_reservation, AllocatorService, AuthorityService,
    ControlPlaneClient, ControlPlaneHandle, ControlStreamSession, EvictionService,
    GrpcControlPlaneService, ReleaseOp, ReserveSpecificOp, RouteTrafficReport,
    CONTROL_PLANE_SERVER_THREADS_ENV, CONTROL_PLANE_THREADS_ENV,
};
use crate::control_plane::pb;
use crate::control_plane::pb::control_plane_service_server::ControlPlaneService as _;
use crate::observability::{metrics_test_lock, render_prometheus_metrics, reset_metrics};
use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, ColdBackingReplica, ColdBackingRoute, ColdBackingState,
    CompatibilityDescriptor, NamespaceScope, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier,
    RouteCasRequest, RouteState, RouteVersion, SegmentName, SegmentReservation, StoreError,
};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct TestAuthority {
    routes: Mutex<BTreeMap<String, ObjectRoute>>,
}

impl TestAuthority {
    fn insert(&self, route: ObjectRoute) {
        self.routes.lock().insert(route.key.0.clone(), route);
    }
}

impl mooncake_store_route::RouteAuthorityService for TestAuthority {
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

    fn list_routes_by_replica_owner_page(
        &self,
        _namespace: &str,
        _authority: &ClientStableId,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        limit: usize,
    ) -> mooncake_store_core::Result<mooncake_store_route::RouteOwnerPage> {
        let routes = self.routes.lock();
        let mut matching = routes
            .iter()
            .filter(|(key, route)| {
                cursor.map_or(true, |cursor| key.as_str() > cursor)
                    && route.replicas.iter().any(|replica| &replica.owner == owner)
            })
            .map(|(_, route)| route.clone())
            .take(limit.max(1).saturating_add(1))
            .collect::<Vec<_>>();
        let next_cursor = if matching.len() > limit.max(1) {
            matching.pop();
            matching.last().map(|route| route.key.0.clone())
        } else {
            None
        };
        Ok(mooncake_store_route::RouteOwnerPage {
            routes: matching,
            next_cursor,
        })
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
        Ok(CasResult {
            applied,
            current,
            version_floor: None,
        })
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

#[derive(Default)]
struct TestEviction {
    reported: Mutex<Vec<ObjectKey>>,
}

impl EvictionService for TestEviction {
    fn batch_report_route_hits(
        &self,
        keys: &[ObjectKey],
    ) -> mooncake_store_core::Result<RouteTrafficReport> {
        self.reported.lock().extend_from_slice(keys);
        Ok(RouteTrafficReport::new(keys.len(), keys.len() as u64 * 16))
    }

    fn batch_track_routes(
        &self,
        routes: &[ObjectRoute],
    ) -> mooncake_store_core::Result<RouteTrafficReport> {
        self.reported
            .lock()
            .extend(routes.iter().map(|route| route.key.clone()));
        let bytes = routes
            .iter()
            .flat_map(|route| route.replicas.iter())
            .map(|replica| replica.length)
            .sum();
        Ok(RouteTrafficReport::new(routes.len(), bytes))
    }
}

struct ClosingStreamService {
    inner: GrpcControlPlaneService,
}

#[tonic::async_trait]
impl pb::control_plane_service_server::ControlPlaneService for ClosingStreamService {
    type ControlStreamStream = ReceiverStream<std::result::Result<pb::ControlStreamReply, Status>>;

    async fn get_route(
        &self,
        request: Request<pb::GetRouteRequest>,
    ) -> std::result::Result<Response<pb::GetRouteReply>, Status> {
        self.inner.get_route(request).await
    }

    async fn batch_get_routes(
        &self,
        request: Request<pb::BatchGetRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchGetRoutesReply>, Status> {
        self.inner.batch_get_routes(request).await
    }

    async fn batch_contains_routes(
        &self,
        request: Request<pb::BatchContainsRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchContainsRoutesReply>, Status> {
        self.inner.batch_contains_routes(request).await
    }

    async fn compare_and_swap_route(
        &self,
        request: Request<pb::CompareAndSwapRouteRequest>,
    ) -> std::result::Result<Response<pb::CompareAndSwapRouteReply>, Status> {
        self.inner.compare_and_swap_route(request).await
    }

    async fn list_routes_by_replica_owner(
        &self,
        request: Request<pb::ListRoutesByReplicaOwnerRequest>,
    ) -> std::result::Result<Response<pb::ListRoutesByReplicaOwnerReply>, Status> {
        self.inner.list_routes_by_replica_owner(request).await
    }

    async fn batch_compare_and_swap_routes(
        &self,
        request: Request<pb::BatchCompareAndSwapRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchCompareAndSwapRoutesReply>, Status> {
        self.inner.batch_compare_and_swap_routes(request).await
    }

    async fn replace_route(
        &self,
        request: Request<pb::ReplaceRouteRequest>,
    ) -> std::result::Result<Response<pb::ReplaceRouteReply>, Status> {
        self.inner.replace_route(request).await
    }

    async fn batch_replace_routes(
        &self,
        request: Request<pb::BatchReplaceRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchReplaceRoutesReply>, Status> {
        self.inner.batch_replace_routes(request).await
    }

    async fn trigger_cold_tier_offload(
        &self,
        request: Request<pb::TriggerColdTierOffloadRequest>,
    ) -> std::result::Result<Response<pb::TriggerColdTierOffloadReply>, Status> {
        self.inner.trigger_cold_tier_offload(request).await
    }

    async fn manual_cold_tier_gc(
        &self,
        request: Request<pb::ManualColdTierGcRequest>,
    ) -> std::result::Result<Response<pb::ManualColdTierGcReply>, Status> {
        self.inner.manual_cold_tier_gc(request).await
    }

    async fn manual_cold_tier_free(
        &self,
        request: Request<pb::ManualColdTierFreeRequest>,
    ) -> std::result::Result<Response<pb::ManualColdTierFreeReply>, Status> {
        self.inner.manual_cold_tier_free(request).await
    }

    async fn probe_cold_tier_device(
        &self,
        request: Request<pb::ProbeColdTierDeviceRequest>,
    ) -> std::result::Result<Response<pb::ProbeColdTierDeviceReply>, Status> {
        self.inner.probe_cold_tier_device(request).await
    }

    async fn read_from_cold(
        &self,
        request: Request<pb::ReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::ReadFromColdReply>, Status> {
        self.inner.read_from_cold(request).await
    }

    async fn batch_read_from_cold(
        &self,
        request: Request<pb::BatchReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::BatchReadFromColdReply>, Status> {
        self.inner.batch_read_from_cold(request).await
    }

    async fn ack_cold_read_complete(
        &self,
        request: Request<pb::AckColdReadCompleteRequest>,
    ) -> std::result::Result<Response<pb::AckColdReadCompleteReply>, Status> {
        self.inner.ack_cold_read_complete(request).await
    }

    async fn batch_reclaim_cold_backings(
        &self,
        request: Request<pb::BatchReclaimColdBackingsRequest>,
    ) -> std::result::Result<Response<pb::BatchReclaimColdBackingsReply>, Status> {
        self.inner.batch_reclaim_cold_backings(request).await
    }

    async fn pin_for_read(
        &self,
        request: Request<pb::PinForReadRequest>,
    ) -> std::result::Result<Response<pb::PinForReadReply>, Status> {
        self.inner.pin_for_read(request).await
    }

    async fn reserve_any(
        &self,
        request: Request<pb::ReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::ReserveAnyReply>, Status> {
        self.inner.reserve_any(request).await
    }

    async fn batch_reserve_any(
        &self,
        request: Request<pb::BatchReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveAnyReply>, Status> {
        self.inner.batch_reserve_any(request).await
    }

    async fn reserve_specific(
        &self,
        request: Request<pb::ReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::ReserveSpecificReply>, Status> {
        self.inner.reserve_specific(request).await
    }

    async fn batch_reserve_specific(
        &self,
        request: Request<pb::BatchReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveSpecificReply>, Status> {
        self.inner.batch_reserve_specific(request).await
    }

    async fn release(
        &self,
        request: Request<pb::ReleaseRequest>,
    ) -> std::result::Result<Response<pb::ReleaseReply>, Status> {
        self.inner.release(request).await
    }

    async fn batch_release(
        &self,
        request: Request<pb::BatchReleaseRequest>,
    ) -> std::result::Result<Response<pb::BatchReleaseReply>, Status> {
        self.inner.batch_release(request).await
    }

    async fn batch_report_route_hits(
        &self,
        request: Request<pb::BatchReportRouteHitsRequest>,
    ) -> std::result::Result<Response<pb::BatchReportRouteHitsReply>, Status> {
        self.inner.batch_report_route_hits(request).await
    }

    async fn batch_track_replica_routes(
        &self,
        request: Request<pb::BatchTrackReplicaRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchTrackReplicaRoutesReply>, Status> {
        self.inner.batch_track_replica_routes(request).await
    }

    async fn submit_migration_task(
        &self,
        request: Request<pb::SubmitMigrationTaskRequest>,
    ) -> std::result::Result<Response<pb::SubmitMigrationTaskReply>, Status> {
        self.inner.submit_migration_task(request).await
    }

    async fn get_migration_execution_status(
        &self,
        request: Request<pb::GetMigrationExecutionStatusRequest>,
    ) -> std::result::Result<Response<pb::GetMigrationExecutionStatusReply>, Status> {
        self.inner.get_migration_execution_status(request).await
    }

    async fn control_stream(
        &self,
        _request: Request<tonic::Streaming<pb::ControlStreamRequest>>,
    ) -> std::result::Result<Response<Self::ControlStreamStream>, Status> {
        Err(Status::unavailable("stream disabled"))
    }
}

struct ClosingStreamHandle {
    address: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl ClosingStreamHandle {
    fn spawn(
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
    ) -> mooncake_store_core::Result<Self> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
            StoreError::Transport(format!("closing stream bind failed: {error}"))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!("closing stream listener setup failed: {error}"))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!("closing stream local addr failed: {error}"))
            })?
            .to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name(format!("closing-stream-{address}"))
            .spawn(move || {
                let runtime = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should start");
                let service = ClosingStreamService {
                    inner: GrpcControlPlaneService::new(authority, allocator, eviction),
                };
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("listener conversion should succeed");
                    Server::builder()
                        .tcp_nodelay(true)
                        .add_service(
                            pb::control_plane_service_server::ControlPlaneServiceServer::new(
                                service,
                            ),
                        )
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(listener),
                            async move {
                                let _ = shutdown_rx.await;
                            },
                        )
                        .await
                        .expect("closing stream server should run");
                });
            })
            .map_err(|error| {
                StoreError::Transport(format!("closing stream spawn failed: {error}"))
            })?;
        Ok(Self {
            address,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for ClosingStreamHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct DelayedUnaryService {
    inner: GrpcControlPlaneService,
    batch_get_delay: Duration,
}

#[tonic::async_trait]
impl pb::control_plane_service_server::ControlPlaneService for DelayedUnaryService {
    type ControlStreamStream = ReceiverStream<std::result::Result<pb::ControlStreamReply, Status>>;

    async fn get_route(
        &self,
        request: Request<pb::GetRouteRequest>,
    ) -> std::result::Result<Response<pb::GetRouteReply>, Status> {
        self.inner.get_route(request).await
    }

    async fn batch_get_routes(
        &self,
        request: Request<pb::BatchGetRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchGetRoutesReply>, Status> {
        tokio::time::sleep(self.batch_get_delay).await;
        self.inner.batch_get_routes(request).await
    }

    async fn batch_contains_routes(
        &self,
        request: Request<pb::BatchContainsRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchContainsRoutesReply>, Status> {
        self.inner.batch_contains_routes(request).await
    }

    async fn compare_and_swap_route(
        &self,
        request: Request<pb::CompareAndSwapRouteRequest>,
    ) -> std::result::Result<Response<pb::CompareAndSwapRouteReply>, Status> {
        self.inner.compare_and_swap_route(request).await
    }

    async fn list_routes_by_replica_owner(
        &self,
        request: Request<pb::ListRoutesByReplicaOwnerRequest>,
    ) -> std::result::Result<Response<pb::ListRoutesByReplicaOwnerReply>, Status> {
        self.inner.list_routes_by_replica_owner(request).await
    }

    async fn batch_compare_and_swap_routes(
        &self,
        request: Request<pb::BatchCompareAndSwapRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchCompareAndSwapRoutesReply>, Status> {
        self.inner.batch_compare_and_swap_routes(request).await
    }

    async fn replace_route(
        &self,
        request: Request<pb::ReplaceRouteRequest>,
    ) -> std::result::Result<Response<pb::ReplaceRouteReply>, Status> {
        self.inner.replace_route(request).await
    }

    async fn batch_replace_routes(
        &self,
        request: Request<pb::BatchReplaceRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchReplaceRoutesReply>, Status> {
        self.inner.batch_replace_routes(request).await
    }

    async fn trigger_cold_tier_offload(
        &self,
        request: Request<pb::TriggerColdTierOffloadRequest>,
    ) -> std::result::Result<Response<pb::TriggerColdTierOffloadReply>, Status> {
        self.inner.trigger_cold_tier_offload(request).await
    }

    async fn manual_cold_tier_gc(
        &self,
        request: Request<pb::ManualColdTierGcRequest>,
    ) -> std::result::Result<Response<pb::ManualColdTierGcReply>, Status> {
        self.inner.manual_cold_tier_gc(request).await
    }

    async fn manual_cold_tier_free(
        &self,
        request: Request<pb::ManualColdTierFreeRequest>,
    ) -> std::result::Result<Response<pb::ManualColdTierFreeReply>, Status> {
        self.inner.manual_cold_tier_free(request).await
    }

    async fn probe_cold_tier_device(
        &self,
        request: Request<pb::ProbeColdTierDeviceRequest>,
    ) -> std::result::Result<Response<pb::ProbeColdTierDeviceReply>, Status> {
        self.inner.probe_cold_tier_device(request).await
    }

    async fn read_from_cold(
        &self,
        request: Request<pb::ReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::ReadFromColdReply>, Status> {
        self.inner.read_from_cold(request).await
    }

    async fn batch_read_from_cold(
        &self,
        request: Request<pb::BatchReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::BatchReadFromColdReply>, Status> {
        self.inner.batch_read_from_cold(request).await
    }

    async fn ack_cold_read_complete(
        &self,
        request: Request<pb::AckColdReadCompleteRequest>,
    ) -> std::result::Result<Response<pb::AckColdReadCompleteReply>, Status> {
        self.inner.ack_cold_read_complete(request).await
    }

    async fn batch_reclaim_cold_backings(
        &self,
        request: Request<pb::BatchReclaimColdBackingsRequest>,
    ) -> std::result::Result<Response<pb::BatchReclaimColdBackingsReply>, Status> {
        self.inner.batch_reclaim_cold_backings(request).await
    }

    async fn pin_for_read(
        &self,
        request: Request<pb::PinForReadRequest>,
    ) -> std::result::Result<Response<pb::PinForReadReply>, Status> {
        self.inner.pin_for_read(request).await
    }

    async fn reserve_any(
        &self,
        request: Request<pb::ReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::ReserveAnyReply>, Status> {
        self.inner.reserve_any(request).await
    }

    async fn batch_reserve_any(
        &self,
        request: Request<pb::BatchReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveAnyReply>, Status> {
        self.inner.batch_reserve_any(request).await
    }

    async fn reserve_specific(
        &self,
        request: Request<pb::ReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::ReserveSpecificReply>, Status> {
        self.inner.reserve_specific(request).await
    }

    async fn batch_reserve_specific(
        &self,
        request: Request<pb::BatchReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveSpecificReply>, Status> {
        self.inner.batch_reserve_specific(request).await
    }

    async fn release(
        &self,
        request: Request<pb::ReleaseRequest>,
    ) -> std::result::Result<Response<pb::ReleaseReply>, Status> {
        self.inner.release(request).await
    }

    async fn batch_release(
        &self,
        request: Request<pb::BatchReleaseRequest>,
    ) -> std::result::Result<Response<pb::BatchReleaseReply>, Status> {
        self.inner.batch_release(request).await
    }

    async fn batch_report_route_hits(
        &self,
        request: Request<pb::BatchReportRouteHitsRequest>,
    ) -> std::result::Result<Response<pb::BatchReportRouteHitsReply>, Status> {
        self.inner.batch_report_route_hits(request).await
    }

    async fn batch_track_replica_routes(
        &self,
        request: Request<pb::BatchTrackReplicaRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchTrackReplicaRoutesReply>, Status> {
        self.inner.batch_track_replica_routes(request).await
    }

    async fn submit_migration_task(
        &self,
        request: Request<pb::SubmitMigrationTaskRequest>,
    ) -> std::result::Result<Response<pb::SubmitMigrationTaskReply>, Status> {
        self.inner.submit_migration_task(request).await
    }

    async fn get_migration_execution_status(
        &self,
        request: Request<pb::GetMigrationExecutionStatusRequest>,
    ) -> std::result::Result<Response<pb::GetMigrationExecutionStatusReply>, Status> {
        self.inner.get_migration_execution_status(request).await
    }

    async fn control_stream(
        &self,
        _request: Request<tonic::Streaming<pb::ControlStreamRequest>>,
    ) -> std::result::Result<Response<Self::ControlStreamStream>, Status> {
        Err(Status::unavailable("stream disabled"))
    }
}

struct DelayedUnaryHandle {
    address: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl DelayedUnaryHandle {
    fn spawn(
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
        batch_get_delay: Duration,
    ) -> mooncake_store_core::Result<Self> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
            StoreError::Transport(format!("delayed unary bind failed: {error}"))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!("delayed unary listener setup failed: {error}"))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!("delayed unary local addr failed: {error}"))
            })?
            .to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name(format!("delayed-unary-{address}"))
            .spawn(move || {
                let runtime = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should start");
                let service = DelayedUnaryService {
                    inner: GrpcControlPlaneService::new(authority, allocator, eviction),
                    batch_get_delay,
                };
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("listener conversion should succeed");
                    Server::builder()
                        .tcp_nodelay(true)
                        .add_service(
                            pb::control_plane_service_server::ControlPlaneServiceServer::new(
                                service,
                            ),
                        )
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(listener),
                            async move {
                                let _ = shutdown_rx.await;
                            },
                        )
                        .await
                        .expect("delayed unary server should run");
                });
            })
            .map_err(|error| {
                StoreError::Transport(format!("delayed unary spawn failed: {error}"))
            })?;
        Ok(Self {
            address,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for DelayedUnaryHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct InvalidMigrationStatusService {
    inner: GrpcControlPlaneService,
    invalid_state: i32,
}

#[tonic::async_trait]
impl pb::control_plane_service_server::ControlPlaneService for InvalidMigrationStatusService {
    type ControlStreamStream = ReceiverStream<std::result::Result<pb::ControlStreamReply, Status>>;

    async fn get_route(
        &self,
        request: Request<pb::GetRouteRequest>,
    ) -> std::result::Result<Response<pb::GetRouteReply>, Status> {
        self.inner.get_route(request).await
    }

    async fn batch_get_routes(
        &self,
        request: Request<pb::BatchGetRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchGetRoutesReply>, Status> {
        self.inner.batch_get_routes(request).await
    }

    async fn batch_contains_routes(
        &self,
        request: Request<pb::BatchContainsRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchContainsRoutesReply>, Status> {
        self.inner.batch_contains_routes(request).await
    }

    async fn compare_and_swap_route(
        &self,
        request: Request<pb::CompareAndSwapRouteRequest>,
    ) -> std::result::Result<Response<pb::CompareAndSwapRouteReply>, Status> {
        self.inner.compare_and_swap_route(request).await
    }

    async fn list_routes_by_replica_owner(
        &self,
        request: Request<pb::ListRoutesByReplicaOwnerRequest>,
    ) -> std::result::Result<Response<pb::ListRoutesByReplicaOwnerReply>, Status> {
        self.inner.list_routes_by_replica_owner(request).await
    }

    async fn batch_compare_and_swap_routes(
        &self,
        request: Request<pb::BatchCompareAndSwapRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchCompareAndSwapRoutesReply>, Status> {
        self.inner.batch_compare_and_swap_routes(request).await
    }

    async fn replace_route(
        &self,
        request: Request<pb::ReplaceRouteRequest>,
    ) -> std::result::Result<Response<pb::ReplaceRouteReply>, Status> {
        self.inner.replace_route(request).await
    }

    async fn batch_replace_routes(
        &self,
        request: Request<pb::BatchReplaceRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchReplaceRoutesReply>, Status> {
        self.inner.batch_replace_routes(request).await
    }

    async fn trigger_cold_tier_offload(
        &self,
        request: Request<pb::TriggerColdTierOffloadRequest>,
    ) -> std::result::Result<Response<pb::TriggerColdTierOffloadReply>, Status> {
        self.inner.trigger_cold_tier_offload(request).await
    }

    async fn manual_cold_tier_gc(
        &self,
        request: Request<pb::ManualColdTierGcRequest>,
    ) -> std::result::Result<Response<pb::ManualColdTierGcReply>, Status> {
        self.inner.manual_cold_tier_gc(request).await
    }

    async fn manual_cold_tier_free(
        &self,
        request: Request<pb::ManualColdTierFreeRequest>,
    ) -> std::result::Result<Response<pb::ManualColdTierFreeReply>, Status> {
        self.inner.manual_cold_tier_free(request).await
    }

    async fn probe_cold_tier_device(
        &self,
        request: Request<pb::ProbeColdTierDeviceRequest>,
    ) -> std::result::Result<Response<pb::ProbeColdTierDeviceReply>, Status> {
        self.inner.probe_cold_tier_device(request).await
    }

    async fn read_from_cold(
        &self,
        request: Request<pb::ReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::ReadFromColdReply>, Status> {
        self.inner.read_from_cold(request).await
    }

    async fn batch_read_from_cold(
        &self,
        request: Request<pb::BatchReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::BatchReadFromColdReply>, Status> {
        self.inner.batch_read_from_cold(request).await
    }

    async fn ack_cold_read_complete(
        &self,
        request: Request<pb::AckColdReadCompleteRequest>,
    ) -> std::result::Result<Response<pb::AckColdReadCompleteReply>, Status> {
        self.inner.ack_cold_read_complete(request).await
    }

    async fn batch_reclaim_cold_backings(
        &self,
        request: Request<pb::BatchReclaimColdBackingsRequest>,
    ) -> std::result::Result<Response<pb::BatchReclaimColdBackingsReply>, Status> {
        self.inner.batch_reclaim_cold_backings(request).await
    }

    async fn pin_for_read(
        &self,
        request: Request<pb::PinForReadRequest>,
    ) -> std::result::Result<Response<pb::PinForReadReply>, Status> {
        self.inner.pin_for_read(request).await
    }

    async fn reserve_any(
        &self,
        request: Request<pb::ReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::ReserveAnyReply>, Status> {
        self.inner.reserve_any(request).await
    }

    async fn batch_reserve_any(
        &self,
        request: Request<pb::BatchReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveAnyReply>, Status> {
        self.inner.batch_reserve_any(request).await
    }

    async fn reserve_specific(
        &self,
        request: Request<pb::ReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::ReserveSpecificReply>, Status> {
        self.inner.reserve_specific(request).await
    }

    async fn batch_reserve_specific(
        &self,
        request: Request<pb::BatchReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveSpecificReply>, Status> {
        self.inner.batch_reserve_specific(request).await
    }

    async fn release(
        &self,
        request: Request<pb::ReleaseRequest>,
    ) -> std::result::Result<Response<pb::ReleaseReply>, Status> {
        self.inner.release(request).await
    }

    async fn batch_release(
        &self,
        request: Request<pb::BatchReleaseRequest>,
    ) -> std::result::Result<Response<pb::BatchReleaseReply>, Status> {
        self.inner.batch_release(request).await
    }

    async fn batch_report_route_hits(
        &self,
        request: Request<pb::BatchReportRouteHitsRequest>,
    ) -> std::result::Result<Response<pb::BatchReportRouteHitsReply>, Status> {
        self.inner.batch_report_route_hits(request).await
    }

    async fn batch_track_replica_routes(
        &self,
        request: Request<pb::BatchTrackReplicaRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchTrackReplicaRoutesReply>, Status> {
        self.inner.batch_track_replica_routes(request).await
    }

    async fn submit_migration_task(
        &self,
        request: Request<pb::SubmitMigrationTaskRequest>,
    ) -> std::result::Result<Response<pb::SubmitMigrationTaskReply>, Status> {
        self.inner.submit_migration_task(request).await
    }

    async fn get_migration_execution_status(
        &self,
        request: Request<pb::GetMigrationExecutionStatusRequest>,
    ) -> std::result::Result<Response<pb::GetMigrationExecutionStatusReply>, Status> {
        let execution_id = request.into_inner().execution_id;
        Ok(Response::new(pb::GetMigrationExecutionStatusReply {
            execution_id,
            state: self.invalid_state,
            attempts: 1,
            last_error: String::new(),
            error: None,
        }))
    }

    async fn control_stream(
        &self,
        _request: Request<tonic::Streaming<pb::ControlStreamRequest>>,
    ) -> std::result::Result<Response<Self::ControlStreamStream>, Status> {
        Err(Status::unavailable("stream disabled"))
    }
}

struct InvalidMigrationStatusHandle {
    address: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl InvalidMigrationStatusHandle {
    fn spawn(
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
        invalid_state: i32,
    ) -> mooncake_store_core::Result<Self> {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|error| {
            StoreError::Transport(format!("invalid status bind failed: {error}"))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!("invalid status listener setup failed: {error}"))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!("invalid status local addr failed: {error}"))
            })?
            .to_string();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name(format!("invalid-status-{address}"))
            .spawn(move || {
                let runtime = RuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should start");
                let service = InvalidMigrationStatusService {
                    inner: GrpcControlPlaneService::new(authority, allocator, eviction),
                    invalid_state,
                };
                runtime.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("listener conversion should succeed");
                    Server::builder()
                        .tcp_nodelay(true)
                        .add_service(
                            pb::control_plane_service_server::ControlPlaneServiceServer::new(
                                service,
                            ),
                        )
                        .serve_with_incoming_shutdown(
                            TcpListenerStream::new(listener),
                            async move {
                                let _ = shutdown_rx.await;
                            },
                        )
                        .await
                        .expect("invalid status server should run");
                });
            })
            .map_err(|error| {
                StoreError::Transport(format!("invalid status spawn failed: {error}"))
            })?;
        Ok(Self {
            address,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for InvalidMigrationStatusHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn with_env_var<T>(key: &str, value: Option<&str>, f: impl FnOnce() -> T) -> T {
    let _guard = crate::observability::test_process_lock().lock();
    let previous = std::env::var(key).ok();
    match value {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
    let result = f();
    match previous {
        Some(value) => std::env::set_var(key, value),
        None => std::env::remove_var(key),
    }
    result
}

fn sample_owner() -> ClientRuntimeId {
    ClientRuntimeId::new("writer", ClientEpoch(7))
}

fn sample_route(key: &str, version: u64, owner: &ClientRuntimeId) -> ObjectRoute {
    ObjectRoute {
        key: ObjectKey::new(key),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: None,
        version: RouteVersion(version),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: owner.clone(),
            segment_name: SegmentName::new("segment-a"),
            offset: Some(128),
            segment_offset: 128,
            length: 16,
            checksum: Some(9),
            tier: ReplicaTier::Dram,
            priority: 1,
        }],
        cold_backing: None,
        nof_backing: None,
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

fn prometheus_sample_value(metrics: &str, sample: &str) -> u64 {
    metrics
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix(sample)?.strip_prefix(' ')?;
            value.parse::<u64>().ok()
        })
        .unwrap_or(0)
}

fn assert_metric_delta_at_least(before: &str, after: &str, sample: &str, expected: u64) {
    let previous = prometheus_sample_value(before, sample);
    let current = prometheus_sample_value(after, sample);
    assert!(
        current >= previous.saturating_add(expected),
        "expected {sample} to increase by at least {expected}, before={previous}, after={current}"
    );
}

#[test]
fn control_plane_client_round_trips_routes_and_allocator_calls() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = ControlPlaneHandle::spawn(
        "127.0.0.1",
        authority.clone(),
        allocator.clone(),
        eviction.clone(),
    )
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

    assert_eq!(
        client
            .batch_report_route_hits(&lease, &[ObjectKey::new("beta"), ObjectKey::new("alpha")])
            .expect("batch report route hits should succeed"),
        2
    );
    assert_eq!(
        client
            .batch_track_replica_routes(&lease, &[sample_route("delta", 3, &owner)])
            .expect("batch track replica routes should succeed"),
        1
    );
    assert_eq!(eviction.reported.lock().len(), 3);

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
    drop(client);
    handle.shutdown();
}

#[test]
fn control_plane_client_can_be_called_from_tokio_runtime_thread() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = ControlPlaneHandle::spawn("127.0.0.1", authority, allocator, eviction)
        .expect("control plane server should start");
    let lease = sample_lease(handle.address());
    let client = ControlPlaneClient::new().expect("control plane client should start");
    let authority_id = ClientStableId::new("authority");

    let runtime = RuntimeBuilder::new_multi_thread()
        .worker_threads(1)
        .thread_name("control-plane-nested-runtime-regression")
        .enable_all()
        .build()
        .expect("test runtime should start");
    let get_results = runtime.block_on(async move {
        client.batch_get_routes(
            &lease,
            "ns-a",
            &authority_id,
            &[ObjectKey::new("alpha"), ObjectKey::new("missing")],
        )
    });

    let get_results =
        get_results.expect("control client call inside Tokio runtime should not panic");
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
    handle.shutdown();
}

#[test]
fn control_plane_client_reopens_closed_cached_stream_session() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = ControlPlaneHandle::spawn("127.0.0.1", authority.clone(), allocator, eviction)
        .expect("control plane server should start");
    let lease = sample_lease(handle.address());
    let client = Arc::new(ControlPlaneClient::new().expect("control plane client should start"));
    let authority_id = ClientStableId::new("authority");

    let warmup = client
        .batch_get_routes(&lease, "ns-a", &authority_id, &[ObjectKey::new("alpha")])
        .expect("warmup route lookup should succeed");
    assert_eq!(warmup.len(), 1);
    assert_eq!(client.active_stream_sessions(), 1);

    let cached = client
        .streams
        .lock()
        .get(handle.address())
        .cloned()
        .expect("warmup lookup should cache a control stream session");
    fail_stream_session(&cached, "forced closed cached session".to_string());
    assert!(
        cached.closed.load(Ordering::Relaxed),
        "test should force the cached session closed before reopening"
    );

    let replay_client = client.clone();
    let replay_lease = lease.clone();
    let replay_authority = authority_id.clone();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let result = replay_client.batch_get_routes(
            &replay_lease,
            "ns-a",
            &replay_authority,
            &[ObjectKey::new("alpha")],
        );
        let _ = done_tx.send(result);
    });

    let replay = done_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("closed cached session reopen should not deadlock");
    let routes = replay.expect("replayed route lookup should succeed");
    assert_eq!(routes.len(), 1);
    assert_eq!(
        routes[0]
            .as_ref()
            .expect("replayed route should decode")
            .as_ref()
            .map(|route| route.key.0.as_str()),
        Some("alpha")
    );
    assert_eq!(client.active_stream_sessions(), 1);

    drop(client);
    handle.shutdown();
}

#[test]
fn control_plane_migration_entrypoints_reject_invalid_requests() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = ControlPlaneHandle::spawn("127.0.0.1", authority, allocator, eviction)
        .expect("control plane server should start");
    let lease = sample_lease(handle.address());
    let client = ControlPlaneClient::new().expect("control plane client should start");

    let submit_error = client
        .submit_migration_task(
            &lease,
            pb::SubmitMigrationTaskRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                tenant: "tenant-a".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: "alpha".to_string(),
                mode: pb::MigrationMode::Move as i32,
                source_segment: "segment-a".to_string(),
                target_segments: vec!["segment-b".to_string(), "segment-c".to_string()],
                task_executor: "executor-a".to_string(),
                max_retries: 3,
            },
        )
        .expect_err("move migration with multiple targets should be rejected");
    assert!(matches!(submit_error, StoreError::InvalidState(_)));

    let invalid_mode_error = client
        .submit_migration_task(
            &lease,
            pb::SubmitMigrationTaskRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                tenant: "tenant-a".to_string(),
                domain: String::new(),
                object_set: String::new(),
                key: "alpha".to_string(),
                mode: 999,
                source_segment: "segment-a".to_string(),
                target_segments: vec!["segment-b".to_string()],
                task_executor: "executor-a".to_string(),
                max_retries: 3,
            },
        )
        .expect_err("invalid migration mode should be rejected");
    assert!(matches!(invalid_mode_error, StoreError::InvalidState(_)));

    let status_error = client
        .get_migration_execution_status(
            &lease,
            pb::GetMigrationExecutionStatusRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                execution_id: String::new(),
            },
        )
        .expect_err("status lookup with empty execution_id should be rejected");
    assert!(matches!(status_error, StoreError::InvalidState(_)));

    drop(client);
    handle.shutdown();
}

#[test]
fn control_plane_migration_status_rejects_invalid_reply_state() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = InvalidMigrationStatusHandle::spawn(authority, allocator, eviction, 999)
        .expect("invalid-status control plane server should start");
    let lease = sample_lease(handle.address());
    let client = ControlPlaneClient::new().expect("control plane client should start");

    let error = client
        .get_migration_execution_status(
            &lease,
            pb::GetMigrationExecutionStatusRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                execution_id: "migration-1".to_string(),
            },
        )
        .expect_err("invalid migration execution state should be rejected");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(
        error.to_string().contains("invalid state"),
        "unexpected error: {error}"
    );

    drop(client);
    handle.shutdown();
}

#[test]
fn control_plane_client_does_not_serialize_concurrent_unary_rpcs_on_runtime_lock() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle = DelayedUnaryHandle::spawn(
        authority.clone(),
        allocator,
        eviction,
        Duration::from_millis(500),
    )
    .expect("delayed unary control plane should start");
    let lease = sample_lease(handle.address());
    let authority_id = ClientStableId::new("authority");
    let client = Arc::new(ControlPlaneClient::new().expect("control plane client should start"));

    let slow_client = client.clone();
    let slow_lease = lease.clone();
    let slow_authority = authority_id.clone();
    let slow = thread::spawn(move || {
        slow_client
            .batch_get_routes(
                &slow_lease,
                "ns-a",
                &slow_authority,
                &[ObjectKey::new("alpha")],
            )
            .expect("slow batch get should succeed")
    });

    thread::sleep(Duration::from_millis(100));

    let fast_client = client.clone();
    let fast_lease = lease.clone();
    let fast_authority = authority_id.clone();
    let started = Instant::now();
    let fast = thread::spawn(move || {
        fast_client
            .list_routes_by_replica_owner(&fast_lease, "ns-a", &fast_authority, &owner)
            .expect("fast list should succeed")
    });

    let fast_result = fast.join().expect("fast request thread should join");
    let fast_elapsed = started.elapsed();
    let slow_result = slow.join().expect("slow request thread should join");

    assert_eq!(fast_result.len(), 1);
    assert_eq!(slow_result.len(), 1);
    assert!(
        fast_elapsed < Duration::from_millis(350),
        "fast unary rpc should not wait behind another caller's runtime lock: {fast_elapsed:?}"
    );

    handle.shutdown();
}

#[test]
fn control_plane_client_times_out_slow_unary_rpc() {
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle =
        DelayedUnaryHandle::spawn(authority, allocator, eviction, Duration::from_millis(500))
            .expect("delayed unary control plane should start");
    let lease = sample_lease(handle.address());
    let authority_id = ClientStableId::new("authority");
    let client = ControlPlaneClient::with_request_timeout(Duration::from_millis(100))
        .expect("control plane client should start");

    let started = Instant::now();
    let error = client
        .batch_get_routes(&lease, "ns-a", &authority_id, &[ObjectKey::new("alpha")])
        .expect_err("slow unary rpc should time out");
    assert!(matches!(error, StoreError::Transport(_)));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "control plane timeout should fail fast"
    );

    handle.shutdown();
}

#[test]
fn control_plane_client_runtime_threads_are_configurable_via_env() {
    with_env_var(CONTROL_PLANE_THREADS_ENV, Some("1"), || {
        let client = ControlPlaneClient::new().expect("control plane client should start");
        drop(client);
    });

    with_env_var(CONTROL_PLANE_THREADS_ENV, Some("8"), || {
        let client = ControlPlaneClient::new().expect("control plane client should start");
        drop(client);
    });

    with_env_var(CONTROL_PLANE_THREADS_ENV, Some("0"), || {
        let client = ControlPlaneClient::new().expect("invalid env should fall back to default");
        drop(client);
    });

    with_env_var(CONTROL_PLANE_THREADS_ENV, Some("bad"), || {
        let client = ControlPlaneClient::new().expect("invalid env should fall back to default");
        drop(client);
    });
}

#[test]
fn control_plane_server_runtime_threads_are_configurable_via_env() {
    with_env_var(CONTROL_PLANE_SERVER_THREADS_ENV, None, || {
        assert_eq!(control_plane_server_threads_from_env(), 4);
    });

    with_env_var(CONTROL_PLANE_SERVER_THREADS_ENV, Some("16"), || {
        assert_eq!(control_plane_server_threads_from_env(), 16);
    });

    with_env_var(CONTROL_PLANE_SERVER_THREADS_ENV, Some("0"), || {
        assert_eq!(control_plane_server_threads_from_env(), 4);
    });

    with_env_var(CONTROL_PLANE_SERVER_THREADS_ENV, Some("bad"), || {
        assert_eq!(control_plane_server_threads_from_env(), 4);
    });
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
        version_floor: None,
    };
    assert_eq!(
        try_cas_result(pb_cas_result(&cas)).expect("cas result should round-trip"),
        cas
    );

    let cas_with_floor = CasResult {
        applied: false,
        current: None,
        version_floor: Some(RouteVersion(42)),
    };
    assert_eq!(
        try_cas_result(pb_cas_result(&cas_with_floor))
            .expect("cas result with version_floor should round-trip"),
        cas_with_floor
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
        offset: Some(0),
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

    let mut legacy_replica = route.replicas[0].clone();
    legacy_replica.offset = None;
    assert_eq!(
        try_replica_route(pb_replica_route(&legacy_replica))
            .expect("legacy replica should round-trip"),
        legacy_replica
    );
}

#[test]
fn cold_backing_route_round_trip_preserves_replicas() {
    let route = ColdBackingRoute {
        owner: ClientRuntimeId::new("cold-primary", ClientEpoch(3)),
        cold_tier_id: "cold-primary-device".to_string(),
        object_locator: "primary-object".to_string(),
        length: 4096,
        checksum: Some(12345),
        state: ColdBackingState::Materialized,
        replicas: vec![ColdBackingReplica {
            owner: ClientRuntimeId::new("cold-replica", ClientEpoch(4)),
            cold_tier_id: "cold-replica-device".to_string(),
            object_locator: "replica-object".to_string(),
        }],
    };

    assert_eq!(
        try_cold_backing_route(pb_cold_backing_route(&route))
            .expect("cold backing should round-trip"),
        route
    );
}

#[test]
fn object_route_round_trip_preserves_namespace_fields() {
    let owner = sample_owner();
    let route = ObjectRoute {
        key: ObjectKey::new("tenant-a::key-a"),
        namespace: Some(NamespaceScope::with_defaults(Some("tenant-a"), None, None)),
        logical_key: Some("key-a".to_string()),
        canonical_key: Some("tenant-a/default/default/key-a".to_string()),
        sharing_scope: Some("tenant-a".to_string()),
        qos_tier: Some("default".to_string()),
        version: RouteVersion(3),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner,
            segment_name: SegmentName::new("segment-a"),
            offset: Some(128),
            segment_offset: 64,
            length: 16,
            checksum: None,
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
        cold_backing: None,
        nof_backing: None,
    };

    assert_eq!(
        try_object_route(pb_object_route(&route)).expect("route should round-trip"),
        route
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

#[test]
fn control_plane_client_short_circuits_empty_batches_and_rejects_bad_uris() {
    let client = ControlPlaneClient::new().expect("control plane client should build");
    let lease = sample_lease("not a valid uri");
    let authority = ClientStableId::new("authority");
    let owner = sample_owner();

    assert!(client
        .batch_get_routes(&lease, "ns", &authority, &[])
        .expect("empty route get should short-circuit")
        .is_empty());
    assert!(client
        .batch_compare_and_swap_routes(&lease, "ns", &authority, &[])
        .expect("empty cas should short-circuit")
        .is_empty());
    assert!(client
        .batch_replace_routes(&lease, "ns", &authority, &[])
        .expect("empty replace should short-circuit")
        .is_empty());
    assert!(client
        .batch_reserve_any(&lease, &owner, &[])
        .expect("empty reserve_any should short-circuit")
        .is_empty());
    assert!(client
        .batch_reserve_specific(&lease, &owner, &[])
        .expect("empty reserve_specific should short-circuit")
        .is_empty());
    assert!(client
        .batch_release(&lease, &owner, &[])
        .expect("empty release should short-circuit")
        .is_empty());

    let error = client
        .batch_get_routes(&lease, "ns", &authority, &[ObjectKey::new("bad-key")])
        .expect_err("invalid control uri should fail");
    assert!(matches!(error, StoreError::Transport(_)));
}

#[test]
fn control_plane_server_direct_paths_cover_validation_and_stream_dispatch() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));
    let service = GrpcControlPlaneService::new(authority, allocator, eviction.clone());
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should start");

    runtime.block_on(async {
        let metrics_before = render_prometheus_metrics();

        let get_reply = service
            .get_route(Request::new(pb::GetRouteRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                key: "alpha".to_string(),
            }))
            .await
            .expect("unary get_route should succeed")
            .into_inner();
        assert!(get_reply.route.is_some());

        let valid_route = sample_route("beta", 1, &owner);
        let valid_cas_reply = service
            .compare_and_swap_route(Request::new(pb::CompareAndSwapRouteRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                key: "beta".to_string(),
                expected_version: None,
                next: Some(pb_object_route(&valid_route)),
            }))
            .await
            .expect("valid compare_and_swap_route should reply")
            .into_inner();
        assert!(valid_cas_reply.error.is_none());

        let invalid_route = pb::ObjectRoute {
            key: "broken".to_string(),
            version: 9,
            state: pb_route_state(RouteState::Active),
            compatibility: None,
            replicas: vec![],
            tenant: String::new(),
            domain: String::new(),
            object_set: String::new(),
            logical_key: String::new(),
            canonical_key: String::new(),
            sharing_scope: String::new(),
            qos_tier: String::new(),
            cold_backing: None,
        };

        let cas_reply = service
            .compare_and_swap_route(Request::new(pb::CompareAndSwapRouteRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                key: "broken".to_string(),
                expected_version: None,
                next: Some(invalid_route.clone()),
            }))
            .await
            .expect("compare_and_swap_route should reply")
            .into_inner();
        assert!(cas_reply.error.is_some());

        let first_page = service
            .list_routes_by_replica_owner(Request::new(pb::ListRoutesByReplicaOwnerRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                owner: Some(pb_runtime_id(&owner)),
                cursor: None,
                limit: 1,
            }))
            .await
            .expect("first owner route page should reply")
            .into_inner();
        assert_eq!(first_page.routes.len(), 1);
        assert_eq!(first_page.routes[0].key, "alpha");
        assert_eq!(first_page.next_cursor.as_deref(), Some("alpha"));

        let second_page = service
            .list_routes_by_replica_owner(Request::new(pb::ListRoutesByReplicaOwnerRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                owner: Some(pb_runtime_id(&owner)),
                cursor: first_page.next_cursor,
                limit: 1,
            }))
            .await
            .expect("second owner route page should reply")
            .into_inner();
        assert_eq!(second_page.routes.len(), 1);
        assert_eq!(second_page.routes[0].key, "beta");
        assert_eq!(second_page.next_cursor, None);

        let list_error = service
            .list_routes_by_replica_owner(Request::new(pb::ListRoutesByReplicaOwnerRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                owner: None,
                cursor: None,
                limit: 0,
            }))
            .await
            .expect_err("missing owner should be rejected");
        assert_eq!(list_error.code(), tonic::Code::InvalidArgument);

        let batch_cas = service
            .batch_compare_and_swap_routes(Request::new(pb::BatchCompareAndSwapRoutesRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                entries: vec![pb::RouteCasEntry {
                    key: "broken".to_string(),
                    expected_version: None,
                    next: Some(invalid_route.clone()),
                }],
            }))
            .await
            .expect("batch cas should reply")
            .into_inner();
        assert_eq!(batch_cas.replies.len(), 1);
        assert!(batch_cas.replies[0].error.is_some());

        let replace = service
            .replace_route(Request::new(pb::ReplaceRouteRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                key: "broken".to_string(),
                next: Some(invalid_route.clone()),
            }))
            .await
            .expect("replace should reply")
            .into_inner();
        assert!(replace.error.is_some());

        let batch_replace = service
            .batch_replace_routes(Request::new(pb::BatchReplaceRoutesRequest {
                namespace: "ns-a".to_string(),
                authority: "authority".to_string(),
                entries: vec![pb::RouteReplaceEntry {
                    key: "broken".to_string(),
                    next: Some(invalid_route),
                }],
            }))
            .await
            .expect("batch replace should reply")
            .into_inner();
        assert_eq!(batch_replace.replies.len(), 1);
        assert!(batch_replace.replies[0].error.is_some());

        let reserve_any = service
            .reserve_any(Request::new(pb::ReserveAnyRequest {
                owner: None,
                length_bytes: 64,
            }))
            .await
            .expect("reserve_any should reply")
            .into_inner();
        assert!(reserve_any.error.is_some());

        let reserve_any_error = service
            .batch_reserve_any(Request::new(pb::BatchReserveAnyRequest {
                owner: None,
                length_bytes: vec![8, 16],
            }))
            .await
            .expect_err("batch reserve_any requires owner");
        assert_eq!(reserve_any_error.code(), tonic::Code::InvalidArgument);

        let reserve_specific = service
            .reserve_specific(Request::new(pb::ReserveSpecificRequest {
                owner: None,
                segment_name: "segment-a".to_string(),
                length_bytes: 32,
            }))
            .await
            .expect("reserve_specific should reply")
            .into_inner();
        assert!(reserve_specific.error.is_some());

        let reserve_specific_error = service
            .batch_reserve_specific(Request::new(pb::BatchReserveSpecificRequest {
                owner: None,
                entries: vec![pb::ReserveSpecificEntry {
                    segment_name: "segment-a".to_string(),
                    length_bytes: 32,
                }],
            }))
            .await
            .expect_err("batch reserve_specific requires owner");
        assert_eq!(reserve_specific_error.code(), tonic::Code::InvalidArgument);

        let release = service
            .release(Request::new(pb::ReleaseRequest {
                owner: None,
                segment_name: "segment-a".to_string(),
                offset_bytes: 0,
                length_bytes: 8,
            }))
            .await
            .expect("release should reply")
            .into_inner();
        assert!(release.error.is_some());

        let release_error = service
            .batch_release(Request::new(pb::BatchReleaseRequest {
                owner: None,
                entries: vec![pb::ReleaseEntry {
                    segment_name: "segment-a".to_string(),
                    offset_bytes: 0,
                    length_bytes: 8,
                }],
            }))
            .await
            .expect_err("batch release requires owner");
        assert_eq!(release_error.code(), tonic::Code::InvalidArgument);

        let hit_reply = service
            .batch_report_route_hits(Request::new(pb::BatchReportRouteHitsRequest {
                keys: vec!["alpha".to_string(), "beta".to_string()],
            }))
            .await
            .expect("batch report route hits should reply")
            .into_inner();
        assert_eq!(hit_reply.accepted, 2);
        assert_eq!(eviction.reported.lock().len(), 2);

        let track_reply = service
            .batch_track_replica_routes(Request::new(pb::BatchTrackReplicaRoutesRequest {
                routes: vec![pb_object_route(&sample_route("delta", 4, &owner))],
            }))
            .await
            .expect("batch track routes should reply")
            .into_inner();
        assert_eq!(track_reply.accepted, 1);
        assert_eq!(eviction.reported.lock().len(), 3);

        let metrics = render_prometheus_metrics();
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_request_bytes_total{tenant=\"default\",operation=\"storage_owner_report_route_hits\",direction=\"read\",scope=\"control\"}",
            32,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_request_bytes_total{tenant=\"default\",operation=\"storage_owner_track_replica_routes\",direction=\"write\",scope=\"control\"}",
            16,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_transport_bytes_total{tenant=\"default\",direction=\"read\",peer_kind=\"client\"}",
            32,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_transport_bytes_total{tenant=\"default\",direction=\"write\",peer_kind=\"client\"}",
            16,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_transport_operation_total{tenant=\"default\",direction=\"read\",peer_kind=\"client\",result=\"ok\"}",
            1,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_transport_operation_total{tenant=\"default\",direction=\"write\",peer_kind=\"client\",result=\"ok\"}",
            1,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_checksum_validation_total{tenant=\"default\",result=\"ok\"}",
            2,
        );
        assert_metric_delta_at_least(
            &metrics_before,
            &metrics,
            "mooncake_store_replication_publish_duration_seconds_count{tenant=\"default\",result=\"ok\"}",
            1,
        );

        let stream_reply = handle_control_stream_request(
            &service,
            pb::ControlStreamRequest {
                request_id: 99,
                body: None,
            },
        )
        .await;
        assert_eq!(stream_reply.request_id, 99);
        assert!(stream_reply.error.is_some());
        assert!(stream_reply.body.is_none());

        let hit_stream_reply = handle_control_stream_request(
            &service,
            pb::ControlStreamRequest {
                request_id: 100,
                body: Some(pb::control_stream_request::Body::EvictionReportRouteHits(
                    pb::BatchReportRouteHitsRequest {
                        keys: vec!["gamma".to_string()],
                    },
                )),
            },
        )
        .await;
        let pb::control_stream_reply::Body::EvictionReportRouteHits(body) = hit_stream_reply
            .body
            .expect("hit stream reply should include body")
        else {
            panic!("unexpected hit stream reply body");
        };
        assert_eq!(body.accepted, 1);

        let track_stream_reply = handle_control_stream_request(
            &service,
            pb::ControlStreamRequest {
                request_id: 101,
                body: Some(
                    pb::control_stream_request::Body::EvictionTrackReplicaRoutes(
                        pb::BatchTrackReplicaRoutesRequest {
                            routes: vec![pb_object_route(&sample_route("omega", 1, &owner))],
                        },
                    ),
                ),
            },
        )
        .await;
        let pb::control_stream_reply::Body::EvictionTrackReplicaRoutes(body) = track_stream_reply
            .body
            .expect("track stream reply should include body")
        else {
            panic!("unexpected track stream reply body");
        };
        assert_eq!(body.accepted, 1);
    });
}

#[test]
fn owner_route_rpc_defaults_and_clamps_page_limit() {
    let authority = Arc::new(TestAuthority::default());
    let owner = sample_owner();
    for index in 0..=mooncake_store_route::DEFAULT_ROUTE_OWNER_PAGE_SIZE {
        authority.insert(sample_route(&format!("route-{index:04}"), 1, &owner));
    }
    let service = GrpcControlPlaneService::new(
        authority,
        Arc::new(TestAllocator::default()),
        Arc::new(TestEviction::default()),
    );
    let runtime = RuntimeBuilder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime should start");

    runtime.block_on(async {
        for limit in [0, u32::MAX] {
            let reply = service
                .list_routes_by_replica_owner(Request::new(pb::ListRoutesByReplicaOwnerRequest {
                    namespace: "ns-a".to_string(),
                    authority: "authority".to_string(),
                    owner: Some(pb_runtime_id(&owner)),
                    cursor: None,
                    limit,
                }))
                .await
                .expect("bounded owner route request should reply")
                .into_inner();
            assert_eq!(
                reply.routes.len(),
                mooncake_store_route::DEFAULT_ROUTE_OWNER_PAGE_SIZE
            );
            assert_eq!(
                reply.next_cursor.as_deref(),
                reply.routes.last().map(|route| route.key.as_str())
            );
        }
    });
}

#[test]
fn control_plane_client_falls_back_to_unary_when_streaming_is_disabled() {
    let _guard = metrics_test_lock().lock();
    reset_metrics();
    let authority = Arc::new(TestAuthority::default());
    let allocator = Arc::new(TestAllocator::default());
    let eviction = Arc::new(TestEviction::default());
    let owner = sample_owner();
    authority.insert(sample_route("alpha", 1, &owner));

    let mut handle =
        ClosingStreamHandle::spawn(authority.clone(), allocator.clone(), eviction.clone())
            .expect("closing stream server should start");
    let lease = sample_lease(handle.address());
    let client = ControlPlaneClient::new().expect("control plane client should build");
    let authority_id = ClientStableId::new("authority");

    let get_results = client
        .batch_get_routes(&lease, "ns-a", &authority_id, &[ObjectKey::new("alpha")])
        .expect("fallback batch get should succeed");
    assert_eq!(get_results.len(), 1);
    assert!(get_results[0]
        .as_ref()
        .expect("route decode should succeed")
        .is_some());

    let cas_results = client
        .batch_compare_and_swap_routes(
            &lease,
            "ns-a",
            &authority_id,
            &[RouteCasRequest {
                key: ObjectKey::new("alpha"),
                expected: Some(RouteVersion(1)),
                next: Some(sample_route("alpha", 2, &owner)),
            }],
        )
        .expect("fallback cas should succeed");
    assert!(cas_results[0].as_ref().expect("cas should decode").applied);

    let listed = client
        .list_routes_by_replica_owner(&lease, "ns-a", &authority_id, &owner)
        .expect("fallback list should succeed");
    assert_eq!(listed.len(), 1);

    let replace_results = client
        .batch_replace_routes(
            &lease,
            "ns-a",
            &authority_id,
            &[RouteCasRequest {
                key: ObjectKey::new("alpha"),
                expected: None,
                next: Some(sample_route("alpha", 3, &owner)),
            }],
        )
        .expect("fallback replace should succeed");
    assert!(replace_results[0].is_ok());

    let reserve_any = client
        .batch_reserve_any(&lease, &owner, &[8, 16])
        .expect("fallback reserve_any should succeed");
    assert_eq!(reserve_any.len(), 2);
    assert!(reserve_any.iter().all(|item| item.is_ok()));

    let reserve_specific = client
        .batch_reserve_specific(
            &lease,
            &owner,
            &[ReserveSpecificOp {
                segment_name: SegmentName::new("segment-z"),
                length_bytes: 24,
            }],
        )
        .expect("fallback reserve_specific should succeed");
    assert_eq!(reserve_specific.len(), 1);
    assert!(reserve_specific[0].is_ok());

    let release = client
        .batch_release(
            &lease,
            &owner,
            &[ReleaseOp {
                segment_name: SegmentName::new("segment-z"),
                offset_bytes: 4_096,
                length_bytes: 24,
            }],
        )
        .expect("fallback release should succeed");
    assert!(release[0].is_ok());
    assert_eq!(allocator.released.lock().len(), 1);

    assert_eq!(
        client
            .batch_report_route_hits(&lease, &[ObjectKey::new("alpha")])
            .expect("fallback hit reporting should succeed"),
        1
    );
    assert_eq!(
        client
            .batch_track_replica_routes(&lease, &[sample_route("track-alpha", 5, &owner)])
            .expect("fallback route tracking should succeed"),
        1
    );
    assert_eq!(eviction.reported.lock().len(), 2);

    client.clear_channels();
    assert_eq!(client.active_stream_sessions(), 0);
    drop(client);
    handle.shutdown();
}
