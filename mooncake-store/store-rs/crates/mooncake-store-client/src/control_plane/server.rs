use super::codec::*;
use super::pb::control_plane_service_server::ControlPlaneService as _;
use super::*;

pub(crate) struct ControlPlaneHandle {
    address: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl ControlPlaneHandle {
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
        )
    }

    pub(crate) fn spawn_with_migration(
        bind_host: &str,
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
        migration: Arc<dyn MigrationService>,
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
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                StoreError::Transport(format!("control plane server runtime init failed: {error}"))
            })?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let thread = thread::Builder::new()
            .name(format!("store-control-{address}"))
            .spawn(move || {
                run_server(
                    runtime,
                    listener,
                    shutdown_rx,
                    authority,
                    allocator,
                    eviction,
                    migration,
                )
            })
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

impl Drop for ControlPlaneHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn run_server(
    runtime: Runtime,
    listener: std::net::TcpListener,
    shutdown: oneshot::Receiver<()>,
    authority: Arc<dyn AuthorityService>,
    allocator: Arc<dyn AllocatorService>,
    eviction: Arc<dyn EvictionService>,
    migration: Arc<dyn MigrationService>,
) {
    let service =
        GrpcControlPlaneService::new_with_migration(authority, allocator, eviction, migration);
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
        )
    }

    pub(super) fn new_with_migration(
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
        eviction: Arc<dyn EvictionService>,
        migration: Arc<dyn MigrationService>,
    ) -> Self {
        Self {
            authority,
            allocator,
            eviction,
            migration,
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

    async fn get_route(
        &self,
        request: Request<pb::GetRouteRequest>,
    ) -> std::result::Result<Response<pb::GetRouteReply>, Status> {
        let request = request.into_inner();
        let reply = match self.authority.get_route(
            &request.namespace,
            &ClientStableId::new(request.authority),
            &ObjectKey::new(request.key),
        ) {
            Ok(route) => pb::GetRouteReply {
                route: route.as_ref().map(pb_object_route),
                error: None,
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
        let authority = ClientStableId::new(request.authority);
        let keys = request
            .keys
            .into_iter()
            .map(ObjectKey::new)
            .collect::<Vec<_>>();
        let replies = self
            .authority
            .batch_get_routes(&request.namespace, &authority, &keys)
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
            .collect();
        Ok(Response::new(pb::BatchGetRoutesReply { replies }))
    }

    async fn compare_and_swap_route(
        &self,
        request: Request<pb::CompareAndSwapRouteRequest>,
    ) -> std::result::Result<Response<pb::CompareAndSwapRouteReply>, Status> {
        let request = request.into_inner();
        let next = request.next.as_ref().map(try_object_route_ref).transpose();
        let reply = match next.and_then(|next| {
            self.authority.compare_and_swap_route(
                &request.namespace,
                &ClientStableId::new(request.authority),
                &ObjectKey::new(request.key),
                request.expected_version.map(RouteVersion),
                next.as_ref(),
            )
        }) {
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
        let reply = match self.authority.list_routes_by_replica_owner(
            &request.namespace,
            &ClientStableId::new(request.authority),
            &owner,
        ) {
            Ok(routes) => pb::ListRoutesByReplicaOwnerReply {
                routes: routes.iter().map(pb_object_route).collect(),
                error: None,
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
            Ok(accepted) => pb::BatchReportRouteHitsReply {
                accepted: accepted as u64,
                error: None,
            },
            Err(error) => pb::BatchReportRouteHitsReply {
                accepted: 0,
                error: Some(pb_error(error)),
            },
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
            Ok(accepted) => pb::BatchTrackReplicaRoutesReply {
                accepted: accepted as u64,
                error: None,
            },
            Err(error) => pb::BatchTrackReplicaRoutesReply {
                accepted: 0,
                error: Some(pb_error(error)),
            },
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
            Ok(entries) => self
                .authority
                .batch_compare_and_swap_routes(&request.namespace, &authority, &entries)
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
                .collect(),
            Err(error) => {
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
                self.authority.replace_route(
                    &request.namespace,
                    &ClientStableId::new(request.authority),
                    &ObjectKey::new(request.key),
                    next.as_ref(),
                )
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
            Ok(entries) => self
                .authority
                .batch_replace_routes(&request.namespace, &authority, &entries)
                .into_iter()
                .map(|result| match result {
                    Ok(()) => pb::ReplaceRouteReply { error: None },
                    Err(error) => pb::ReplaceRouteReply {
                        error: Some(pb_error(error)),
                    },
                })
                .collect(),
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
        tokio::spawn(async move {
            loop {
                let reply = match inbound.next().await {
                    Some(Ok(request)) => handle_control_stream_request(&service, request).await,
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
