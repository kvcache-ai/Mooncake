use super::codec::*;
use super::cold_tier_server::{
    handle_ack_cold_read_complete, handle_batch_read_from_cold, handle_pin_for_read,
    handle_read_from_cold,
};
use super::pb::control_plane_service_server::ControlPlaneService as _;
use super::*;
use crate::observability::registry;
use mooncake_store_route::{
    serve_route_control_request, RouteControlRequest, RouteControlResponse,
};
use std::time::{Duration, Instant};

const SLOW_CONTROL_REQUEST_LOG_THRESHOLD: Duration = Duration::from_millis(1000);

pub(crate) struct ControlPlaneHandle {
    address: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl ControlPlaneHandle {
    pub(crate) fn detached() -> Self {
        Self {
            address: String::new(),
            shutdown: None,
            thread: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn spawn(
        bind_host: &str,
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
    ) -> Result<Self> {
        Self::spawn_with_migration(
            bind_host,
            authority,
            allocator,
            eviction,
            Arc::new(UnsupportedMigrationService),
            Arc::new(UnsupportedColdTierControlService),
        )
    }

    pub(crate) fn spawn_with_migration(
        bind_host: &str,
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
        migration: Arc<dyn MigrationService>,
        cold_tier: Arc<dyn ColdTierControlService>,
    ) -> Result<Self> {
        let listener = std::net::TcpListener::bind((bind_host, 0)).map_err(|error| {
            StoreError::Transport(format!("control plane bind on {bind_host} failed: {error}"))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!("control plane listener setup failed: {error}"))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!("control plane local addr failed: {error}"))
            })?
            .to_string();
        let server_threads = control_plane_server_threads_from_env();
        let runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(server_threads)
            .thread_name("mooncake-control-server")
            .enable_all()
            .build()
            .map_err(|error| {
                StoreError::Transport(format!("control plane server runtime init failed: {error}"))
            })?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let services = ControlPlaneServices {
            authority,
            allocator,
            eviction,
            migration,
            cold_tier,
        };
        let thread = thread::Builder::new()
            .name(format!("store-control-{address}"))
            .spawn(move || run_server(runtime, listener, shutdown_rx, services))
            .map_err(|error| {
                StoreError::Transport(format!("control plane spawn failed: {error}"))
            })?;
        Ok(Self {
            address,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }

    pub(crate) fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.thread.take();
    }
}

pub(super) fn control_plane_server_threads_from_env() -> usize {
    let Some(raw) = std::env::var(CONTROL_PLANE_SERVER_THREADS_ENV).ok() else {
        return DEFAULT_CONTROL_PLANE_SERVER_THREADS;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return DEFAULT_CONTROL_PLANE_SERVER_THREADS;
    }
    match trimmed.parse::<usize>() {
        Ok(threads) if threads > 0 => threads,
        _ => {
            warn!(
                env = CONTROL_PLANE_SERVER_THREADS_ENV,
                value = trimmed,
                default = DEFAULT_CONTROL_PLANE_SERVER_THREADS,
                "invalid control-plane server runtime thread count; falling back to default"
            );
            DEFAULT_CONTROL_PLANE_SERVER_THREADS
        }
    }
}

struct ControlPlaneServices {
    authority: Arc<dyn AuthorityService>,
    allocator: Arc<dyn AllocatorService>,
    eviction: Arc<dyn EvictionService>,
    migration: Arc<dyn MigrationService>,
    cold_tier: Arc<dyn ColdTierControlService>,
}

#[derive(Default)]
struct ControlPlaneServerStats {
    active_streams: AtomicUsize,
    inflight_requests: AtomicUsize,
}

struct ActiveControlStream {
    stats: Arc<ControlPlaneServerStats>,
}

impl Drop for ActiveControlStream {
    fn drop(&mut self) {
        self.stats.active_streams.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ControlPlaneHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_server(
    runtime: Runtime,
    listener: std::net::TcpListener,
    shutdown: oneshot::Receiver<()>,
    services: ControlPlaneServices,
) {
    let service = GrpcControlPlaneService::new_with_migration(
        services.authority,
        services.allocator,
        services.eviction,
        services.migration,
        services.cold_tier,
    );
    let result = runtime.block_on(async move {
        let listener = tokio::net::TcpListener::from_std(listener).map_err(|error| {
            StoreError::Transport(format!("control plane listener conversion failed: {error}"))
        })?;
        Server::builder()
            .tcp_nodelay(true)
            .add_service(pb::control_plane_service_server::ControlPlaneServiceServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown.await;
            })
            .await
            .map_err(|error| {
                StoreError::Transport(format!("control plane gRPC server failed: {error}"))
            })
    });
    if let Err(error) = result {
        warn!(error = %error, "control plane server terminated with error");
    }
}

pub(super) struct GrpcControlPlaneService {
    authority: Arc<dyn AuthorityService>,
    allocator: Arc<dyn AllocatorService>,
    eviction: Arc<dyn EvictionService>,
    migration: Arc<dyn MigrationService>,
    cold_tier: Arc<dyn ColdTierControlService>,
    stats: Arc<ControlPlaneServerStats>,
}

impl GrpcControlPlaneService {
    pub(super) fn new(
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
    ) -> Self {
        Self::new_with_migration(
            authority,
            allocator,
            eviction,
            Arc::new(UnsupportedMigrationService),
            Arc::new(UnsupportedColdTierControlService),
        )
    }

    pub(super) fn new_with_migration(
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
        migration: Arc<dyn MigrationService>,
        cold_tier: Arc<dyn ColdTierControlService>,
    ) -> Self {
        Self {
            authority,
            allocator,
            eviction,
            migration,
            cold_tier,
            stats: Arc::new(ControlPlaneServerStats::default()),
        }
    }
}

#[tonic::async_trait]
impl pb::control_plane_service_server::ControlPlaneService for GrpcControlPlaneService {
    type ControlStreamStream = ReceiverStream<std::result::Result<pb::ControlStreamReply, Status>>;

    async fn submit_migration_task(
        &self,
        request: Request<pb::SubmitMigrationTaskRequest>,
    ) -> std::result::Result<Response<pb::SubmitMigrationTaskReply>, Status> {
        let request = request.into_inner();
        let reply = match validate_submit_migration_task_request(&request) {
            Ok(()) => match self.migration.submit_task(&request) {
                Ok(execution_id) => pb::SubmitMigrationTaskReply {
                    execution_id,
                    error: None,
                },
                Err(error) => pb::SubmitMigrationTaskReply {
                    execution_id: String::new(),
                    error: Some(pb_error(error)),
                },
            },
            Err(error) => pb::SubmitMigrationTaskReply {
                execution_id: String::new(),
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn get_migration_execution_status(
        &self,
        request: Request<pb::GetMigrationExecutionStatusRequest>,
    ) -> std::result::Result<Response<pb::GetMigrationExecutionStatusReply>, Status> {
        let request = request.into_inner();
        let execution_id = request.execution_id.clone();
        let reply = match validate_get_migration_execution_status_request(&request) {
            Ok(()) => match self.migration.get_execution_status(&execution_id) {
                Ok(status) => pb::GetMigrationExecutionStatusReply {
                    execution_id,
                    state: status.state as i32,
                    attempts: status.attempts,
                    last_error: status.last_error,
                    error: None,
                },
                Err(error) => pb::GetMigrationExecutionStatusReply {
                    execution_id,
                    state: pb::MigrationExecutionState::Unspecified as i32,
                    attempts: 0,
                    last_error: String::new(),
                    error: Some(pb_error(error)),
                },
            },
            Err(error) => pb::GetMigrationExecutionStatusReply {
                execution_id,
                state: pb::MigrationExecutionState::Unspecified as i32,
                attempts: 0,
                last_error: String::new(),
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn read_from_cold(
        &self,
        request: Request<pb::ReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::ReadFromColdReply>, Status> {
        handle_read_from_cold(self.cold_tier.clone(), request).await
    }

    async fn batch_read_from_cold(
        &self,
        request: Request<pb::BatchReadFromColdRequest>,
    ) -> std::result::Result<Response<pb::BatchReadFromColdReply>, Status> {
        handle_batch_read_from_cold(self.cold_tier.clone(), request).await
    }

    async fn ack_cold_read_complete(
        &self,
        request: Request<pb::AckColdReadCompleteRequest>,
    ) -> std::result::Result<Response<pb::AckColdReadCompleteReply>, Status> {
        handle_ack_cold_read_complete(self.cold_tier.clone(), request).await
    }

    async fn pin_for_read(
        &self,
        request: Request<pb::PinForReadRequest>,
    ) -> std::result::Result<Response<pb::PinForReadReply>, Status> {
        handle_pin_for_read(self.cold_tier.clone(), request).await
    }

    async fn get_route(
        &self,
        request: Request<pb::GetRouteRequest>,
    ) -> std::result::Result<Response<pb::GetRouteReply>, Status> {
        let request = request.into_inner();
        let route_request = RouteControlRequest::BatchGet {
            namespace: request.namespace,
            authority: ClientStableId::new(request.authority),
            keys: vec![ObjectKey::new(request.key)],
        };
        let reply = match serve_route_control_request(self.authority.as_ref(), route_request) {
            Ok(RouteControlResponse::BatchGet(mut routes)) => match routes.pop() {
                Some(Ok(route)) => pb::GetRouteReply {
                    route: route.as_ref().map(pb_object_route),
                    error: None,
                },
                Some(Err(error)) => pb::GetRouteReply {
                    route: None,
                    error: Some(pb_error(error)),
                },
                None => pb::GetRouteReply {
                    route: None,
                    error: Some(pb_error(StoreError::Transport(
                        "control plane get_route reply is missing batch item".to_string(),
                    ))),
                },
            },
            Ok(_) => pb::GetRouteReply {
                route: None,
                error: Some(pb_error(StoreError::Transport(
                    "control plane get_route returned non-get route response".to_string(),
                ))),
            },
            Err(error) => pb::GetRouteReply {
                route: None,
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_get_routes(
        &self,
        request: Request<pb::BatchGetRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchGetRoutesReply>, Status> {
        let request = request.into_inner();
        let keys = request
            .keys
            .into_iter()
            .map(ObjectKey::new)
            .collect::<Vec<_>>();
        let item_count = keys.len();
        let route_request = RouteControlRequest::BatchGet {
            namespace: request.namespace,
            authority: ClientStableId::new(request.authority),
            keys,
        };
        let replies = match serve_route_control_request(self.authority.as_ref(), route_request) {
            Ok(RouteControlResponse::BatchGet(routes)) => routes
                .into_iter()
                .map(|result| match result {
                    Ok(route) => pb::GetRouteReply {
                        route: route.as_ref().map(pb_object_route),
                        error: None,
                    },
                    Err(error) => pb::GetRouteReply {
                        route: None,
                        error: Some(pb_error(error)),
                    },
                })
                .collect(),
            Ok(_) => {
                let detail = pb_error(StoreError::Transport(
                    "control plane batch_get_routes returned non-get route response".to_string(),
                ));
                (0..item_count)
                    .map(|_| pb::GetRouteReply {
                        route: None,
                        error: Some(detail.clone()),
                    })
                    .collect()
            }
            Err(error) => {
                let detail = pb_error(error);
                (0..item_count)
                    .map(|_| pb::GetRouteReply {
                        route: None,
                        error: Some(detail.clone()),
                    })
                    .collect()
            }
        };
        Ok(Response::new(pb::BatchGetRoutesReply { replies }))
    }

    async fn batch_contains_routes(
        &self,
        request: Request<pb::BatchContainsRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchContainsRoutesReply>, Status> {
        let request = request.into_inner();
        let keys = request
            .keys
            .into_iter()
            .map(ObjectKey::new)
            .collect::<Vec<_>>();
        let item_count = keys.len();
        let route_request = RouteControlRequest::BatchContains {
            namespace: request.namespace,
            authority: ClientStableId::new(request.authority),
            keys,
        };
        let replies = match serve_route_control_request(self.authority.as_ref(), route_request) {
            Ok(RouteControlResponse::BatchContains(results)) => results
                .into_iter()
                .map(|result| match result {
                    Ok(exists) => pb::ContainsRouteReply {
                        exists,
                        error: None,
                    },
                    Err(error) => pb::ContainsRouteReply {
                        exists: false,
                        error: Some(pb_error(error)),
                    },
                })
                .collect(),
            Ok(_) => {
                let detail = pb_error(StoreError::Transport(
                    "control plane batch_contains_routes returned non-contains route response"
                        .to_string(),
                ));
                (0..item_count)
                    .map(|_| pb::ContainsRouteReply {
                        exists: false,
                        error: Some(detail.clone()),
                    })
                    .collect()
            }
            Err(error) => {
                let detail = pb_error(error);
                (0..item_count)
                    .map(|_| pb::ContainsRouteReply {
                        exists: false,
                        error: Some(detail.clone()),
                    })
                    .collect()
            }
        };
        Ok(Response::new(pb::BatchContainsRoutesReply { replies }))
    }

    async fn compare_and_swap_route(
        &self,
        request: Request<pb::CompareAndSwapRouteRequest>,
    ) -> std::result::Result<Response<pb::CompareAndSwapRouteReply>, Status> {
        let request = request.into_inner();
        let next = request.next.as_ref().map(try_object_route_ref).transpose();
        let started = Instant::now();
        let result = next.and_then(|next| {
            let route_request = RouteControlRequest::BatchCompareAndSwap {
                namespace: request.namespace,
                authority: ClientStableId::new(request.authority),
                requests: vec![RouteCasRequest {
                    key: ObjectKey::new(request.key),
                    expected: request.expected_version.map(RouteVersion),
                    next,
                }],
            };
            match serve_route_control_request(self.authority.as_ref(), route_request)? {
                RouteControlResponse::BatchCompareAndSwap(mut results) => {
                    results.pop().ok_or_else(|| {
                        StoreError::Transport(
                            "control plane compare_and_swap_route reply is missing batch item"
                                .to_string(),
                        )
                    })?
                }
                _ => Err(StoreError::Transport(
                    "control plane compare_and_swap_route returned non-cas route response"
                        .to_string(),
                )),
            }
        });
        registry::record_replication_publish(route_cas_result_label(&result), started.elapsed());
        let reply = match result {
            Ok(result) => pb::CompareAndSwapRouteReply {
                result: Some(pb_cas_result(&result)),
                error: None,
            },
            Err(error) => pb::CompareAndSwapRouteReply {
                result: None,
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn list_routes_by_replica_owner(
        &self,
        request: Request<pb::ListRoutesByReplicaOwnerRequest>,
    ) -> std::result::Result<Response<pb::ListRoutesByReplicaOwnerReply>, Status> {
        let request = request.into_inner();
        let owner = request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let owner = owner.ok_or_else(|| {
            Status::invalid_argument(
                "control plane list_routes_by_replica_owner request is missing owner",
            )
        })?;
        let route_request = RouteControlRequest::ListByReplicaOwner {
            namespace: request.namespace,
            authority: ClientStableId::new(request.authority),
            owner,
        };
        let reply = match serve_route_control_request(self.authority.as_ref(), route_request) {
            Ok(RouteControlResponse::ListByReplicaOwner(routes)) => {
                pb::ListRoutesByReplicaOwnerReply {
                    routes: routes.iter().map(pb_object_route).collect(),
                    error: None,
                }
            }
            Ok(_) => pb::ListRoutesByReplicaOwnerReply {
                routes: Vec::new(),
                error: Some(pb_error(StoreError::Transport(
                    "control plane list_routes_by_replica_owner returned non-list route response"
                        .to_string(),
                ))),
            },
            Err(error) => pb::ListRoutesByReplicaOwnerReply {
                routes: Vec::new(),
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_report_route_hits(
        &self,
        request: Request<pb::BatchReportRouteHitsRequest>,
    ) -> std::result::Result<Response<pb::BatchReportRouteHitsReply>, Status> {
        let request = request.into_inner();
        let keys = request
            .keys
            .into_iter()
            .map(ObjectKey::new)
            .collect::<Vec<_>>();
        let reply = match self.eviction.batch_report_route_hits(&keys) {
            Ok(report) => {
                registry::record_request_bytes(
                    "storage_owner_report_route_hits",
                    "read",
                    "control",
                    report.bytes,
                );
                registry::record_transport_operation("read", "client", "ok");
                registry::record_transport_bytes("read", "client", report.bytes);
                registry::record_checksum_validations("ok", report.accepted as u64);
                pb::BatchReportRouteHitsReply {
                    accepted: report.accepted as u64,
                    error: None,
                }
            }
            Err(error) => {
                registry::record_transport_operation("read", "client", "error");
                pb::BatchReportRouteHitsReply {
                    accepted: 0,
                    error: Some(pb_error(error)),
                }
            }
        };
        Ok(Response::new(reply))
    }

    async fn batch_track_replica_routes(
        &self,
        request: Request<pb::BatchTrackReplicaRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchTrackReplicaRoutesReply>, Status> {
        let request = request.into_inner();
        let routes = request
            .routes
            .into_iter()
            .map(try_object_route)
            .collect::<Result<Vec<_>>>();
        let reply = match routes.and_then(|routes| self.eviction.batch_track_routes(&routes)) {
            Ok(report) => {
                registry::record_request_bytes(
                    "storage_owner_track_replica_routes",
                    "write",
                    "control",
                    report.bytes,
                );
                registry::record_transport_operation("write", "client", "ok");
                registry::record_transport_bytes("write", "client", report.bytes);
                pb::BatchTrackReplicaRoutesReply {
                    accepted: report.accepted as u64,
                    error: None,
                }
            }
            Err(error) => {
                registry::record_transport_operation("write", "client", "error");
                pb::BatchTrackReplicaRoutesReply {
                    accepted: 0,
                    error: Some(pb_error(error)),
                }
            }
        };
        Ok(Response::new(reply))
    }

    async fn batch_compare_and_swap_routes(
        &self,
        request: Request<pb::BatchCompareAndSwapRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchCompareAndSwapRoutesReply>, Status> {
        let request = request.into_inner();
        let authority = ClientStableId::new(request.authority);
        let item_count = request.entries.len();
        let entries = request
            .entries
            .into_iter()
            .map(|entry| {
                entry
                    .next
                    .as_ref()
                    .map(try_object_route_ref)
                    .transpose()
                    .map(|next| RouteCasRequest {
                        key: ObjectKey::new(entry.key),
                        expected: entry.expected_version.map(RouteVersion),
                        next,
                    })
            })
            .collect::<Result<Vec<_>>>();
        let replies = match entries {
            Ok(entries) => {
                let started = Instant::now();
                let route_request = RouteControlRequest::BatchCompareAndSwap {
                    namespace: request.namespace,
                    authority,
                    requests: entries,
                };
                let results = match serve_route_control_request(
                    self.authority.as_ref(),
                    route_request,
                ) {
                    Ok(RouteControlResponse::BatchCompareAndSwap(results)) => results,
                    Ok(_) => {
                        let error = StoreError::Transport(
                            "control plane batch_compare_and_swap_routes returned non-cas route response"
                                .to_string(),
                        );
                        (0..item_count).map(|_| Err(error.clone())).collect()
                    }
                    Err(error) => (0..item_count).map(|_| Err(error.clone())).collect(),
                };
                registry::record_replication_publish(
                    batch_route_cas_result_label(&results),
                    started.elapsed(),
                );
                results
                    .into_iter()
                    .map(|result| match result {
                        Ok(result) => pb::CompareAndSwapRouteReply {
                            result: Some(pb_cas_result(&result)),
                            error: None,
                        },
                        Err(error) => pb::CompareAndSwapRouteReply {
                            result: None,
                            error: Some(pb_error(error)),
                        },
                    })
                    .collect()
            }
            Err(error) => {
                registry::record_replication_publish("error", Duration::ZERO);
                let detail = pb_error(error);
                (0..item_count)
                    .map(|_| pb::CompareAndSwapRouteReply {
                        result: None,
                        error: Some(detail.clone()),
                    })
                    .collect()
            }
        };
        Ok(Response::new(pb::BatchCompareAndSwapRoutesReply {
            replies,
        }))
    }

    async fn replace_route(
        &self,
        request: Request<pb::ReplaceRouteRequest>,
    ) -> std::result::Result<Response<pb::ReplaceRouteReply>, Status> {
        let request = request.into_inner();
        let reply = match request
            .next
            .as_ref()
            .map(try_object_route_ref)
            .transpose()
            .and_then(|next| {
                let route_request = RouteControlRequest::BatchReplace {
                    namespace: request.namespace,
                    authority: ClientStableId::new(request.authority),
                    requests: vec![RouteCasRequest {
                        key: ObjectKey::new(request.key),
                        expected: None,
                        next,
                    }],
                };
                match serve_route_control_request(self.authority.as_ref(), route_request)? {
                    RouteControlResponse::BatchReplace(mut results) => {
                        results.pop().ok_or_else(|| {
                            StoreError::Transport(
                                "control plane replace_route reply is missing batch item"
                                    .to_string(),
                            )
                        })?
                    }
                    _ => Err(StoreError::Transport(
                        "control plane replace_route returned non-replace route response"
                            .to_string(),
                    )),
                }
            }) {
            Ok(()) => pb::ReplaceRouteReply { error: None },
            Err(error) => pb::ReplaceRouteReply {
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_replace_routes(
        &self,
        request: Request<pb::BatchReplaceRoutesRequest>,
    ) -> std::result::Result<Response<pb::BatchReplaceRoutesReply>, Status> {
        let request = request.into_inner();
        let authority = ClientStableId::new(request.authority);
        let item_count = request.entries.len();
        let entries = request
            .entries
            .into_iter()
            .map(|entry| {
                entry
                    .next
                    .as_ref()
                    .map(try_object_route_ref)
                    .transpose()
                    .map(|next| RouteCasRequest {
                        key: ObjectKey::new(entry.key),
                        expected: None,
                        next,
                    })
            })
            .collect::<Result<Vec<_>>>();
        let replies = match entries {
            Ok(entries) => {
                let route_request = RouteControlRequest::BatchReplace {
                    namespace: request.namespace,
                    authority,
                    requests: entries,
                };
                match serve_route_control_request(self.authority.as_ref(), route_request) {
                    Ok(RouteControlResponse::BatchReplace(results)) => results
                        .into_iter()
                        .map(|result| match result {
                            Ok(()) => pb::ReplaceRouteReply { error: None },
                            Err(error) => pb::ReplaceRouteReply {
                                error: Some(pb_error(error)),
                            },
                        })
                        .collect(),
                    Ok(_) => {
                        let detail = pb_error(StoreError::Transport(
                            "control plane batch_replace_routes returned non-replace route response"
                                .to_string(),
                        ));
                        (0..item_count)
                            .map(|_| pb::ReplaceRouteReply {
                                error: Some(detail.clone()),
                            })
                            .collect()
                    }
                    Err(error) => {
                        let detail = pb_error(error);
                        (0..item_count)
                            .map(|_| pb::ReplaceRouteReply {
                                error: Some(detail.clone()),
                            })
                            .collect()
                    }
                }
            }
            Err(error) => {
                let detail = pb_error(error);
                (0..item_count)
                    .map(|_| pb::ReplaceRouteReply {
                        error: Some(detail.clone()),
                    })
                    .collect()
            }
        };
        Ok(Response::new(pb::BatchReplaceRoutesReply { replies }))
    }

    async fn reserve_any(
        &self,
        request: Request<pb::ReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::ReserveAnyReply>, Status> {
        let request = request.into_inner();
        let reply = match request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .and_then(|owner| {
                let owner = owner.ok_or_else(|| {
                    StoreError::InvalidState(
                        "control plane reserve_any request is missing owner".to_string(),
                    )
                })?;
                self.allocator.reserve_any(&owner, request.length_bytes)
            }) {
            Ok(reservation) => pb::ReserveAnyReply {
                reservation: Some(pb_segment_reservation(&reservation)),
                error: None,
            },
            Err(error) => pb::ReserveAnyReply {
                reservation: None,
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_reserve_any(
        &self,
        request: Request<pb::BatchReserveAnyRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveAnyReply>, Status> {
        let request = request.into_inner();
        let owner = request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let owner = owner.ok_or_else(|| {
            Status::invalid_argument("control plane batch reserve_any request is missing owner")
        })?;
        let replies = self
            .allocator
            .batch_reserve_any(&owner, &request.length_bytes)
            .into_iter()
            .map(|result| match result {
                Ok(reservation) => pb::ReserveAnyReply {
                    reservation: Some(pb_segment_reservation(&reservation)),
                    error: None,
                },
                Err(error) => pb::ReserveAnyReply {
                    reservation: None,
                    error: Some(pb_error(error)),
                },
            })
            .collect();
        Ok(Response::new(pb::BatchReserveAnyReply { replies }))
    }

    async fn reserve_specific(
        &self,
        request: Request<pb::ReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::ReserveSpecificReply>, Status> {
        let request = request.into_inner();
        let reply = match request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .and_then(|owner| {
                let owner = owner.ok_or_else(|| {
                    StoreError::InvalidState(
                        "control plane reserve_specific request is missing owner".to_string(),
                    )
                })?;
                self.allocator.reserve_specific(
                    &owner,
                    &SegmentName::new(request.segment_name),
                    request.length_bytes,
                )
            }) {
            Ok(reservation) => pb::ReserveSpecificReply {
                reservation: Some(pb_segment_reservation(&reservation)),
                error: None,
            },
            Err(error) => pb::ReserveSpecificReply {
                reservation: None,
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_reserve_specific(
        &self,
        request: Request<pb::BatchReserveSpecificRequest>,
    ) -> std::result::Result<Response<pb::BatchReserveSpecificReply>, Status> {
        let request = request.into_inner();
        let owner = request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let owner = owner.ok_or_else(|| {
            Status::invalid_argument(
                "control plane batch reserve_specific request is missing owner",
            )
        })?;
        let entries = request
            .entries
            .into_iter()
            .map(|entry| ReserveSpecificOp {
                segment_name: SegmentName::new(entry.segment_name),
                length_bytes: entry.length_bytes,
            })
            .collect::<Vec<_>>();
        let replies = self
            .allocator
            .batch_reserve_specific(&owner, &entries)
            .into_iter()
            .map(|result| match result {
                Ok(reservation) => pb::ReserveSpecificReply {
                    reservation: Some(pb_segment_reservation(&reservation)),
                    error: None,
                },
                Err(error) => pb::ReserveSpecificReply {
                    reservation: None,
                    error: Some(pb_error(error)),
                },
            })
            .collect();
        Ok(Response::new(pb::BatchReserveSpecificReply { replies }))
    }

    async fn release(
        &self,
        request: Request<pb::ReleaseRequest>,
    ) -> std::result::Result<Response<pb::ReleaseReply>, Status> {
        let request = request.into_inner();
        let reply = match request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .and_then(|owner| {
                let owner = owner.ok_or_else(|| {
                    StoreError::InvalidState(
                        "control plane release request is missing owner".to_string(),
                    )
                })?;
                self.allocator.release(
                    &owner,
                    &SegmentName::new(request.segment_name),
                    request.offset_bytes,
                    request.length_bytes,
                )
            }) {
            Ok(()) => pb::ReleaseReply { error: None },
            Err(error) => pb::ReleaseReply {
                error: Some(pb_error(error)),
            },
        };
        Ok(Response::new(reply))
    }

    async fn batch_release(
        &self,
        request: Request<pb::BatchReleaseRequest>,
    ) -> std::result::Result<Response<pb::BatchReleaseReply>, Status> {
        let request = request.into_inner();
        let owner = request
            .owner
            .as_ref()
            .map(try_runtime_id)
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let owner = owner.ok_or_else(|| {
            Status::invalid_argument("control plane batch release request is missing owner")
        })?;
        let entries = request
            .entries
            .into_iter()
            .map(|entry| ReleaseOp {
                segment_name: SegmentName::new(entry.segment_name),
                offset_bytes: entry.offset_bytes,
                length_bytes: entry.length_bytes,
            })
            .collect::<Vec<_>>();
        let replies = self
            .allocator
            .batch_release(&owner, &entries)
            .into_iter()
            .map(|result| match result {
                Ok(()) => pb::ReleaseReply { error: None },
                Err(error) => pb::ReleaseReply {
                    error: Some(pb_error(error)),
                },
            })
            .collect();
        Ok(Response::new(pb::BatchReleaseReply { replies }))
    }

    async fn control_stream(
        &self,
        request: Request<tonic::Streaming<pb::ControlStreamRequest>>,
    ) -> std::result::Result<Response<Self::ControlStreamStream>, Status> {
        let service = GrpcControlPlaneService::new(
            self.authority.clone(),
            self.allocator.clone(),
            self.eviction.clone(),
        );
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(128);
        let stats = self.stats.clone();
        let active_streams = stats.active_streams.fetch_add(1, Ordering::Relaxed) + 1;
        trace!(active_streams, "control stream opened");
        tokio::spawn(async move {
            let _active = ActiveControlStream {
                stats: stats.clone(),
            };
            loop {
                let reply = match inbound.next().await {
                    Some(Ok(request)) => {
                        let operation = control_stream_request_operation(&request);
                        let started = Instant::now();
                        let inflight = stats.inflight_requests.fetch_add(1, Ordering::Relaxed) + 1;
                        let reply = handle_control_stream_request(&service, request).await;
                        let elapsed = started.elapsed();
                        let remaining = stats
                            .inflight_requests
                            .fetch_sub(1, Ordering::Relaxed)
                            .saturating_sub(1);
                        if elapsed >= SLOW_CONTROL_REQUEST_LOG_THRESHOLD {
                            warn!(
                                operation,
                                elapsed_ms = elapsed.as_millis() as u64,
                                inflight_at_start = inflight,
                                inflight_after = remaining,
                                active_streams = stats.active_streams.load(Ordering::Relaxed),
                                "slow control stream request"
                            );
                        }
                        reply
                    }
                    Some(Err(status)) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                    None => break,
                };
                if tx.send(Ok(reply)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

fn control_stream_request_operation(request: &pb::ControlStreamRequest) -> &'static str {
    match request.body.as_ref() {
        Some(pb::control_stream_request::Body::RouteGet(_)) => "batch_get_routes",
        Some(pb::control_stream_request::Body::RouteContains(_)) => "batch_contains_routes",
        Some(pb::control_stream_request::Body::RouteCas(_)) => "batch_compare_and_swap_routes",
        Some(pb::control_stream_request::Body::RouteListByReplicaOwner(_)) => {
            "list_routes_by_replica_owner"
        }
        Some(pb::control_stream_request::Body::EvictionReportRouteHits(_)) => {
            "batch_report_route_hits"
        }
        Some(pb::control_stream_request::Body::EvictionTrackReplicaRoutes(_)) => {
            "batch_track_replica_routes"
        }
        Some(pb::control_stream_request::Body::RouteReplace(_)) => "batch_replace_routes",
        Some(pb::control_stream_request::Body::AllocatorReserveAny(_)) => "batch_reserve_any",
        Some(pb::control_stream_request::Body::AllocatorReserveSpecific(_)) => {
            "batch_reserve_specific"
        }
        Some(pb::control_stream_request::Body::AllocatorRelease(_)) => "batch_release",
        None => "missing_body",
    }
}

fn route_cas_result_label(result: &Result<CasResult>) -> &'static str {
    match result {
        Ok(cas) if cas.applied => "ok",
        Ok(_) | Err(StoreError::Conflict(_)) => "conflict",
        Err(_) => "error",
    }
}

fn batch_route_cas_result_label(results: &[Result<CasResult>]) -> &'static str {
    if results
        .iter()
        .any(|result| matches!(result, Err(error) if !matches!(error, StoreError::Conflict(_))))
    {
        return "error";
    }
    if results.iter().any(|result| {
        matches!(result, Err(StoreError::Conflict(_)))
            || result.as_ref().is_ok_and(|cas| !cas.applied)
    }) {
        return "conflict";
    }
    "ok"
}

fn validate_submit_migration_task_request(
    request: &pb::SubmitMigrationTaskRequest,
) -> mooncake_store_core::Result<()> {
    require_non_empty_control_field("submit_migration_task", "namespace", &request.namespace)?;
    require_non_empty_control_field("submit_migration_task", "authority", &request.authority)?;
    require_non_empty_control_field("submit_migration_task", "tenant", &request.tenant)?;
    require_non_empty_control_field("submit_migration_task", "key", &request.key)?;
    require_non_empty_control_field(
        "submit_migration_task",
        "source_segment",
        &request.source_segment,
    )?;
    require_non_empty_control_field(
        "submit_migration_task",
        "task_executor",
        &request.task_executor,
    )?;
    match pb::MigrationMode::try_from(request.mode) {
        Ok(pb::MigrationMode::Copy) => {
            if request.target_segments.is_empty() {
                return Err(StoreError::InvalidState(
                    "control plane submit_migration_task request is missing target_segments"
                        .to_string(),
                ));
            }
        }
        Ok(pb::MigrationMode::Move) => {
            if request.target_segments.len() != 1 {
                return Err(StoreError::InvalidState(
                    "control plane submit_migration_task move requests require exactly one target_segment"
                        .to_string(),
                ));
            }
        }
        Ok(pb::MigrationMode::Unspecified) => {
            return Err(StoreError::InvalidState(
                "control plane submit_migration_task request is missing mode".to_string(),
            ));
        }
        Err(_) => {
            return Err(StoreError::InvalidState(format!(
                "control plane submit_migration_task request has invalid mode value {}",
                request.mode
            )));
        }
    }
    Ok(())
}

fn validate_get_migration_execution_status_request(
    request: &pb::GetMigrationExecutionStatusRequest,
) -> mooncake_store_core::Result<()> {
    require_non_empty_control_field(
        "get_migration_execution_status",
        "namespace",
        &request.namespace,
    )?;
    require_non_empty_control_field(
        "get_migration_execution_status",
        "authority",
        &request.authority,
    )?;
    require_non_empty_control_field(
        "get_migration_execution_status",
        "execution_id",
        &request.execution_id,
    )?;
    Ok(())
}

fn require_non_empty_control_field(
    operation: &str,
    field: &str,
    value: &str,
) -> mooncake_store_core::Result<()> {
    if value.trim().is_empty() {
        return Err(StoreError::InvalidState(format!(
            "control plane {operation} request is missing {field}",
        )));
    }
    Ok(())
}

pub(super) async fn handle_control_stream_request(
    service: &GrpcControlPlaneService,
    request: pb::ControlStreamRequest,
) -> pb::ControlStreamReply {
    let request_id = request.request_id;
    let response = match request.body {
        Some(pb::control_stream_request::Body::RouteGet(batch)) => service
            .batch_get_routes(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::RouteGet),
        Some(pb::control_stream_request::Body::RouteContains(batch)) => service
            .batch_contains_routes(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::RouteContains),
        Some(pb::control_stream_request::Body::RouteCas(batch)) => service
            .batch_compare_and_swap_routes(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::RouteCas),
        Some(pb::control_stream_request::Body::RouteListByReplicaOwner(batch)) => service
            .list_routes_by_replica_owner(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::RouteListByReplicaOwner),
        Some(pb::control_stream_request::Body::EvictionReportRouteHits(batch)) => service
            .batch_report_route_hits(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::EvictionReportRouteHits),
        Some(pb::control_stream_request::Body::EvictionTrackReplicaRoutes(batch)) => service
            .batch_track_replica_routes(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::EvictionTrackReplicaRoutes),
        Some(pb::control_stream_request::Body::RouteReplace(batch)) => service
            .batch_replace_routes(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::RouteReplace),
        Some(pb::control_stream_request::Body::AllocatorReserveAny(batch)) => service
            .batch_reserve_any(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::AllocatorReserveAny),
        Some(pb::control_stream_request::Body::AllocatorReserveSpecific(batch)) => service
            .batch_reserve_specific(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::AllocatorReserveSpecific),
        Some(pb::control_stream_request::Body::AllocatorRelease(batch)) => service
            .batch_release(Request::new(batch))
            .await
            .map(Response::into_inner)
            .map(pb::control_stream_reply::Body::AllocatorRelease),
        None => Err(Status::invalid_argument(
            "control stream request is missing body",
        )),
    };
    match response {
        Ok(body) => pb::ControlStreamReply {
            request_id,
            body: Some(body),
            error: None,
        },
        Err(status) => pb::ControlStreamReply {
            request_id,
            body: None,
            error: Some(pb_error(status_to_store_error(status))),
        },
    }
}
