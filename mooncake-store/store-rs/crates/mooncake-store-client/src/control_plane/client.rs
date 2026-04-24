use super::codec::*;
use super::*;

impl ControlPlaneClient {
    pub(crate) fn new() -> Result<Self> {
        Self::with_request_timeout(control_request_timeout_from_env())
    }

    pub(crate) fn with_request_timeout(request_timeout: Duration) -> Result<Self> {
        let worker_threads = control_plane_runtime_threads_from_env();
        let runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(worker_threads)
            .thread_name("mooncake-control-client")
            .enable_all()
            .build()
            .map_err(|error| {
                StoreError::Transport(format!("control plane runtime init failed: {error}"))
            })?;
        Ok(Self {
            runtime: Some(runtime),
            channels: Mutex::new(BTreeMap::new()),
            streams: Mutex::new(BTreeMap::new()),
            request_timeout: request_timeout.max(Duration::from_millis(1)),
        })
    }

    fn with_runtime<T>(&self, f: impl FnOnce(&Runtime) -> T) -> T {
        let runtime = self
            .runtime
            .as_ref()
            .expect("control plane runtime should remain available while client is alive");
        f(runtime)
    }

    pub(crate) fn batch_get_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<Option<ObjectRoute>>>> {
        let tracker = OperationTracker::new("control_route_batch_get");
        let result = (|| {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let request = pb::BatchGetRoutesRequest {
                namespace: namespace.to_string(),
                authority: authority.0.clone(),
                keys: keys.iter().map(|key| key.0.clone()).collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::RouteGet(request.clone())),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::RouteGet(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_get_routes reply is missing body".to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_get_routes reply type mismatch".to_string(),
                        ));
                    };
                    ensure_batch_len("batch_get_routes", keys.len(), reply.replies.len())?;
                    return Ok(reply
                        .replies
                        .into_iter()
                        .map(|entry| {
                            decode_error(entry.error)?;
                            entry.route.map(try_object_route).transpose()
                        })
                        .collect());
                }
                Err(error) => {
                    warn!(
                        authority = %authority,
                        items = keys.len(),
                        error = %error,
                        "control stream batch_get_routes failed; falling back to unary batch rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move { client.batch_get_routes(Request::new(request)).await },
                channel,
            )?;
            ensure_batch_len("batch_get_routes", keys.len(), reply.replies.len())?;
            Ok(reply
                .replies
                .into_iter()
                .map(|entry| {
                    decode_error(entry.error)?;
                    entry.route.map(try_object_route).transpose()
                })
                .collect())
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn batch_compare_and_swap_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>> {
        let tracker = OperationTracker::new("control_route_batch_cas");
        let result = (|| {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let request = pb::BatchCompareAndSwapRoutesRequest {
                namespace: namespace.to_string(),
                authority: authority.0.clone(),
                entries: requests
                    .iter()
                    .map(|request| pb::RouteCasEntry {
                        key: request.key.0.clone(),
                        expected_version: request.expected.map(|version| version.0),
                        next: request.next.as_ref().map(pb_object_route),
                    })
                    .collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::RouteCas(request.clone())),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::RouteCas(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_compare_and_swap_routes reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_compare_and_swap_routes reply type mismatch"
                                .to_string(),
                        ));
                    };
                    ensure_batch_len(
                        "batch_compare_and_swap_routes",
                        requests.len(),
                        reply.replies.len(),
                    )?;
                    return Ok(reply
                        .replies
                        .into_iter()
                        .map(|entry| {
                            decode_error(entry.error)?;
                            let result = entry.result.ok_or_else(|| {
                                StoreError::Transport(
                                    "control stream batch cas reply is missing result".to_string(),
                                )
                            })?;
                            try_cas_result(result)
                        })
                        .collect());
                }
                Err(error) => {
                    warn!(
                        authority = %authority,
                        items = requests.len(),
                        error = %error,
                        "control stream batch_compare_and_swap_routes failed; falling back to unary batch rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move {
                    client
                        .batch_compare_and_swap_routes(Request::new(request))
                        .await
                },
                channel,
            )?;
            ensure_batch_len(
                "batch_compare_and_swap_routes",
                requests.len(),
                reply.replies.len(),
            )?;
            Ok(reply
                .replies
                .into_iter()
                .map(|entry| {
                    decode_error(entry.error)?;
                    let result = entry.result.ok_or_else(|| {
                        StoreError::Transport(
                            "control plane batch cas reply is missing result".to_string(),
                        )
                    })?;
                    try_cas_result(result)
                })
                .collect())
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn list_routes_by_replica_owner(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        let tracker = OperationTracker::new("control_route_list_by_replica_owner");
        let result = (|| {
            let request = pb::ListRoutesByReplicaOwnerRequest {
                namespace: namespace.to_string(),
                authority: authority.0.clone(),
                owner: Some(pb_runtime_id(owner)),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::RouteListByReplicaOwner(
                    request.clone(),
                )),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::RouteListByReplicaOwner(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream list_routes_by_replica_owner reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream list_routes_by_replica_owner reply type mismatch"
                                .to_string(),
                        ));
                    };
                    decode_error(reply.error)?;
                    return reply.routes.into_iter().map(try_object_route).collect();
                }
                Err(error) => {
                    warn!(
                        authority = %authority,
                        owner = %owner,
                        error = %error,
                        "control stream list_routes_by_replica_owner failed; falling back to unary rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move {
                    client
                        .list_routes_by_replica_owner(Request::new(request))
                        .await
                },
                channel,
            )?;
            decode_error(reply.error)?;
            reply.routes.into_iter().map(try_object_route).collect()
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn batch_replace_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<()>>> {
        let tracker = OperationTracker::new("control_route_batch_replace");
        let result = (|| {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let request = pb::BatchReplaceRoutesRequest {
                namespace: namespace.to_string(),
                authority: authority.0.clone(),
                entries: requests
                    .iter()
                    .map(|request| pb::RouteReplaceEntry {
                        key: request.key.0.clone(),
                        next: request.next.as_ref().map(pb_object_route),
                    })
                    .collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::RouteReplace(
                    request.clone(),
                )),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::RouteReplace(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_replace_routes reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_replace_routes reply type mismatch".to_string(),
                        ));
                    };
                    ensure_batch_len("batch_replace_routes", requests.len(), reply.replies.len())?;
                    return Ok(reply
                        .replies
                        .into_iter()
                        .map(|entry| decode_error(entry.error))
                        .collect());
                }
                Err(error) => {
                    warn!(
                        authority = %authority,
                        items = requests.len(),
                        error = %error,
                        "control stream batch_replace_routes failed; falling back to unary batch rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move { client.batch_replace_routes(Request::new(request)).await },
                channel,
            )?;
            ensure_batch_len("batch_replace_routes", requests.len(), reply.replies.len())?;
            Ok(reply
                .replies
                .into_iter()
                .map(|entry| decode_error(entry.error))
                .collect())
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn batch_report_route_hits(
        &self,
        lease: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<usize> {
        let tracker = OperationTracker::new("control_eviction_batch_report_route_hits");
        let result = (|| {
            if keys.is_empty() {
                return Ok(0);
            }
            let request = pb::BatchReportRouteHitsRequest {
                keys: keys.iter().map(|key| key.0.clone()).collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::EvictionReportRouteHits(
                    request.clone(),
                )),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::EvictionReportRouteHits(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_report_route_hits reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_report_route_hits reply type mismatch"
                                .to_string(),
                        ));
                    };
                    decode_error(reply.error)?;
                    return Ok(reply.accepted as usize);
                }
                Err(error) => {
                    warn!(
                        items = keys.len(),
                        error = %error,
                        "control stream batch_report_route_hits failed; falling back to unary rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply =
                self.rpc(
                    |mut client| async move {
                        client.batch_report_route_hits(Request::new(request)).await
                    },
                    channel,
                )?;
            decode_error(reply.error)?;
            Ok(reply.accepted as usize)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn batch_track_replica_routes(
        &self,
        lease: &ClientLease,
        routes: &[ObjectRoute],
    ) -> Result<usize> {
        let tracker = OperationTracker::new("control_eviction_batch_track_replica_routes");
        let result = (|| {
            if routes.is_empty() {
                return Ok(0);
            }
            let request = pb::BatchTrackReplicaRoutesRequest {
                routes: routes.iter().map(pb_object_route).collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(
                    pb::control_stream_request::Body::EvictionTrackReplicaRoutes(request.clone()),
                ),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::EvictionTrackReplicaRoutes(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_track_replica_routes reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_track_replica_routes reply type mismatch"
                                .to_string(),
                        ));
                    };
                    decode_error(reply.error)?;
                    return Ok(reply.accepted as usize);
                }
                Err(error) => {
                    warn!(
                        items = routes.len(),
                        error = %error,
                        "control stream batch_track_replica_routes failed; falling back to unary rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move {
                    client
                        .batch_track_replica_routes(Request::new(request))
                        .await
                },
                channel,
            )?;
            decode_error(reply.error)?;
            Ok(reply.accepted as usize)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn submit_migration_task(
        &self,
        lease: &ClientLease,
        request: pb::SubmitMigrationTaskRequest,
    ) -> Result<String> {
        let tracker = OperationTracker::new("control_migration_submit");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let reply =
                self.rpc(
                    |mut client| async move {
                        client.submit_migration_task(Request::new(request)).await
                    },
                    channel,
                )?;
            decode_error(reply.error)?;
            if reply.execution_id.trim().is_empty() {
                return Err(StoreError::Transport(
                    "control plane submit_migration_task reply is missing execution_id".to_string(),
                ));
            }
            Ok(reply.execution_id)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn get_migration_execution_status_detail(
        &self,
        lease: &ClientLease,
        request: pb::GetMigrationExecutionStatusRequest,
    ) -> Result<pb::GetMigrationExecutionStatusReply> {
        let tracker = OperationTracker::new("control_migration_status_detail");
        let result = (|| {
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move {
                    client
                        .get_migration_execution_status(Request::new(request))
                        .await
                },
                channel,
            )?;
            decode_error(reply.error.clone())?;
            Ok(reply)
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn get_migration_execution_status(
        &self,
        lease: &ClientLease,
        request: pb::GetMigrationExecutionStatusRequest,
    ) -> Result<pb::MigrationExecutionState> {
        let tracker = OperationTracker::new("control_migration_status");
        let result = self
            .get_migration_execution_status_detail(lease, request)
            .and_then(|reply| {
                pb::MigrationExecutionState::try_from(reply.state).map_err(|_| {
                    StoreError::Transport(format!(
                        "control plane get_migration_execution_status reply has invalid state: {}",
                        reply.state
                    ))
                })
            });
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn get_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        // batch_get_routes 保证返回结果顺序与输入 keys 顺序一致。
        let mut replies =
            self.batch_get_routes(lease, namespace, authority, std::slice::from_ref(key))?;
        replies.pop().ok_or_else(|| {
            StoreError::Transport(
                "control plane get_route batch helper returned no replies".to_string(),
            )
        })?
    }

    pub(crate) fn reserve_any(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let mut replies = self.batch_reserve_any(lease, owner, &[length_bytes])?;
        replies.pop().ok_or_else(|| {
            StoreError::Transport(
                "control plane reserve_any reply is missing batch item".to_string(),
            )
        })?
    }

    pub(crate) fn batch_reserve_any(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        length_bytes: &[u64],
    ) -> Result<Vec<Result<SegmentReservation>>> {
        let tracker = OperationTracker::new("control_allocator_batch_reserve_any");
        let result = (|| {
            if length_bytes.is_empty() {
                return Ok(Vec::new());
            }
            let request = pb::BatchReserveAnyRequest {
                owner: Some(pb_runtime_id(owner)),
                length_bytes: length_bytes.to_vec(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::AllocatorReserveAny(
                    request.clone(),
                )),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::AllocatorReserveAny(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_reserve_any reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_reserve_any reply type mismatch".to_string(),
                        ));
                    };
                    ensure_batch_len("batch_reserve_any", length_bytes.len(), reply.replies.len())?;
                    return Ok(reply
                        .replies
                        .into_iter()
                        .map(|entry| {
                            decode_error(entry.error)?;
                            let reservation = entry.reservation.ok_or_else(|| {
                                StoreError::Transport(
                                    "control stream batch reserve_any reply is missing reservation"
                                        .to_string(),
                                )
                            })?;
                            try_segment_reservation(reservation)
                        })
                        .collect());
                }
                Err(error) => {
                    warn!(
                        owner = %owner,
                        items = length_bytes.len(),
                        error = %error,
                        "control stream batch_reserve_any failed; falling back to unary batch rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move { client.batch_reserve_any(Request::new(request)).await },
                channel,
            )?;
            ensure_batch_len("batch_reserve_any", length_bytes.len(), reply.replies.len())?;
            Ok(reply
                .replies
                .into_iter()
                .map(|entry| {
                    decode_error(entry.error)?;
                    let reservation = entry.reservation.ok_or_else(|| {
                        StoreError::Transport(
                            "control plane batch reserve_any reply is missing reservation"
                                .to_string(),
                        )
                    })?;
                    try_segment_reservation(reservation)
                })
                .collect())
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn reserve_specific(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let mut results = self.batch_reserve_specific(
            lease,
            owner,
            &[ReserveSpecificOp {
                segment_name: segment_name.clone(),
                length_bytes,
            }],
        )?;
        results.pop().ok_or_else(|| {
            StoreError::Transport(
                "control plane reserve_specific reply is missing batch item".to_string(),
            )
        })?
    }

    pub(crate) fn batch_reserve_specific(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        requests: &[ReserveSpecificOp],
    ) -> Result<Vec<Result<SegmentReservation>>> {
        let tracker = OperationTracker::new("control_allocator_batch_reserve_specific");
        let result = (|| {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let request = pb::BatchReserveSpecificRequest {
                owner: Some(pb_runtime_id(owner)),
                entries: requests
                    .iter()
                    .map(|request| pb::ReserveSpecificEntry {
                        segment_name: request.segment_name.0.clone(),
                        length_bytes: request.length_bytes,
                    })
                    .collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::AllocatorReserveSpecific(
                    request.clone(),
                )),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::AllocatorReserveSpecific(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_reserve_specific reply is missing body"
                                    .to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_reserve_specific reply type mismatch".to_string(),
                        ));
                    };
                    ensure_batch_len(
                        "batch_reserve_specific",
                        requests.len(),
                        reply.replies.len(),
                    )?;
                    return Ok(reply
                        .replies
                        .into_iter()
                        .map(|entry| {
                            decode_error(entry.error)?;
                            let reservation = entry.reservation.ok_or_else(|| {
                                StoreError::Transport(
                                    "control stream batch reserve_specific reply is missing reservation"
                                        .to_string(),
                                )
                            })?;
                            try_segment_reservation(reservation)
                        })
                        .collect());
                }
                Err(error) => {
                    warn!(
                        owner = %owner,
                        items = requests.len(),
                        error = %error,
                        "control stream batch_reserve_specific failed; falling back to unary batch rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply =
                self.rpc(
                    |mut client| async move {
                        client.batch_reserve_specific(Request::new(request)).await
                    },
                    channel,
                )?;
            ensure_batch_len(
                "batch_reserve_specific",
                requests.len(),
                reply.replies.len(),
            )?;
            Ok(reply
                .replies
                .into_iter()
                .map(|entry| {
                    decode_error(entry.error)?;
                    let reservation = entry.reservation.ok_or_else(|| {
                        StoreError::Transport(
                            "control plane batch reserve_specific reply is missing reservation"
                                .to_string(),
                        )
                    })?;
                    try_segment_reservation(reservation)
                })
                .collect())
        })();
        tracker.finish(&result, 0);
        result
    }

    pub(crate) fn batch_release(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        requests: &[ReleaseOp],
    ) -> Result<Vec<Result<()>>> {
        let tracker = OperationTracker::new("control_allocator_batch_release");
        let result = (|| {
            if requests.is_empty() {
                return Ok(Vec::new());
            }
            let request = pb::BatchReleaseRequest {
                owner: Some(pb_runtime_id(owner)),
                entries: requests
                    .iter()
                    .map(|request| pb::ReleaseEntry {
                        segment_name: request.segment_name.0.clone(),
                        offset_bytes: request.offset_bytes,
                        length_bytes: request.length_bytes,
                    })
                    .collect(),
            };
            match self.stream_request(lease, |request_id| pb::ControlStreamRequest {
                request_id,
                body: Some(pb::control_stream_request::Body::AllocatorRelease(
                    request.clone(),
                )),
            }) {
                Ok(reply) => {
                    decode_error(reply.error)?;
                    let pb::control_stream_reply::Body::AllocatorRelease(reply) =
                        reply.body.ok_or_else(|| {
                            StoreError::Transport(
                                "control stream batch_release reply is missing body".to_string(),
                            )
                        })?
                    else {
                        return Err(StoreError::Transport(
                            "control stream batch_release reply type mismatch".to_string(),
                        ));
                    };
                    ensure_batch_len("batch_release", requests.len(), reply.replies.len())?;
                    return Ok(reply
                        .replies
                        .into_iter()
                        .map(|entry| decode_error(entry.error))
                        .collect());
                }
                Err(error) => {
                    warn!(
                        owner = %owner,
                        items = requests.len(),
                        error = %error,
                        "control stream batch_release failed; falling back to unary batch rpc"
                    );
                }
            }
            let channel = self.channel_for(lease)?;
            let reply = self.rpc(
                |mut client| async move { client.batch_release(Request::new(request)).await },
                channel,
            )?;
            ensure_batch_len("batch_release", requests.len(), reply.replies.len())?;
            Ok(reply
                .replies
                .into_iter()
                .map(|entry| decode_error(entry.error))
                .collect())
        })();
        tracker.finish(&result, 0);
        result
    }

    fn stream_session_for(
        &self,
        lease: &ClientLease,
        address: &str,
    ) -> Result<Arc<ControlStreamSession>> {
        if let Some(session) = self.streams.lock().get(address).cloned() {
            if !session.closed.load(Ordering::Relaxed) {
                return Ok(session);
            }
            self.streams.lock().remove(address);
        }

        let channel = self.channel_for(lease)?;
        let request_timeout = self.request_timeout;
        let session = self.with_runtime(|runtime| {
            runtime.block_on(async move {
                tokio::time::timeout(request_timeout, async move {
                    let (sender, receiver) = mpsc::channel(128);
                    let outbound = ReceiverStream::new(receiver);
                    let mut client =
                        pb::control_plane_service_client::ControlPlaneServiceClient::new(channel);
                    let inbound = client
                        .control_stream(Request::new(outbound))
                        .await
                        .map(Response::into_inner)
                        .map_err(status_to_store_error)?;
                    let session = Arc::new(ControlStreamSession {
                        sender,
                        pending: Arc::new(Mutex::new(BTreeMap::new())),
                        next_request_id: AtomicU64::new(1),
                        closed: AtomicBool::new(false),
                    });
                    spawn_stream_reader(session.clone(), inbound);
                    Ok::<_, StoreError>(session)
                })
                .await
                .map_err(|_| control_timeout_error("control stream open", request_timeout))?
            })
        })?;
        self.streams
            .lock()
            .insert(address.to_string(), session.clone());
        Ok(session)
    }

    fn invalidate_stream_session(&self, address: &str) {
        self.streams.lock().remove(address);
    }

    fn stream_request(
        &self,
        lease: &ClientLease,
        build: impl Fn(u64) -> pb::ControlStreamRequest,
    ) -> Result<pb::ControlStreamReply> {
        let address = control_address(lease)?;
        let mut last_error = None;
        for attempt in 0..2 {
            let session = self.stream_session_for(lease, &address)?;
            let request_id = session.next_request_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            session.pending.lock().insert(request_id, tx);
            let request = build(request_id);
            let sender = session.sender.clone();
            let send_result = self
                .with_runtime(|runtime| runtime.block_on(async move { sender.send(request).await }))
                .map_err(|error| {
                    StoreError::Transport(format!("control stream send failed: {error}"))
                });
            if let Err(error) = send_result {
                session.pending.lock().remove(&request_id);
                session.closed.store(true, Ordering::Relaxed);
                self.invalidate_stream_session(&address);
                debug!(
                    address,
                    request_id,
                    attempt,
                    error = %error,
                    "control stream send failed; invalidating session"
                );
                last_error = Some(error);
                continue;
            }
            let request_timeout = self.request_timeout;
            let reply = self.with_runtime(|runtime| {
                runtime.block_on(async move { tokio::time::timeout(request_timeout, rx).await })
            });
            let reply = match reply {
                Ok(reply) => reply.map_err(|_| {
                    StoreError::Transport("control stream response channel closed".to_string())
                })?,
                Err(_) => {
                    session.pending.lock().remove(&request_id);
                    session.closed.store(true, Ordering::Relaxed);
                    self.invalidate_stream_session(&address);
                    let error = control_timeout_error("control stream request", request_timeout);
                    debug!(
                        address,
                        request_id,
                        attempt,
                        error = %error,
                        "control stream request timed out; invalidating session"
                    );
                    last_error = Some(error);
                    continue;
                }
            };
            match reply {
                Ok(reply) => return Ok(reply),
                Err(error) => {
                    session.closed.store(true, Ordering::Relaxed);
                    self.invalidate_stream_session(&address);
                    debug!(
                        address,
                        request_id,
                        attempt,
                        error = %error,
                        "control stream reply failed; invalidating session"
                    );
                    last_error = Some(error);
                }
            }
        }
        Err(last_error
            .unwrap_or_else(|| StoreError::Transport("control stream request failed".to_string())))
    }

    fn channel_for(&self, lease: &ClientLease) -> Result<Channel> {
        let address = control_address(lease)?;
        if let Some(channel) = self.channels.lock().get(&address).cloned() {
            return Ok(channel);
        }

        let uri = normalize_control_uri(&address);
        let endpoint = Endpoint::from_shared(uri.clone())
            .map_err(|error| {
                StoreError::Transport(format!("invalid control plane uri {uri}: {error}"))
            })?
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_nodelay(true);
        let channel = self
            .with_runtime(|runtime| runtime.block_on(endpoint.connect()))
            .map_err(|error| {
                StoreError::Transport(format!(
                    "control plane connect to {address} failed: {error}"
                ))
            })?;
        debug!(address, "opened new control plane channel");
        self.channels.lock().insert(address, channel.clone());
        Ok(channel)
    }

    fn rpc<F, Fut, T>(&self, f: F, channel: Channel) -> Result<T>
    where
        F: FnOnce(pb::control_plane_service_client::ControlPlaneServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<Response<T>, Status>>,
    {
        let client = pb::control_plane_service_client::ControlPlaneServiceClient::new(channel);
        let request_timeout = self.request_timeout;
        self.with_runtime(|runtime| {
            runtime.block_on(async move {
                tokio::time::timeout(request_timeout, f(client))
                    .await
                    .map_err(|_| control_timeout_error("control unary rpc", request_timeout))?
                    .map(Response::into_inner)
                    .map_err(status_to_store_error)
            })
        })
    }

    pub(crate) fn clear_channels(&self) {
        self.streams.lock().clear();
        self.channels.lock().clear();
    }

    pub(crate) fn probe_reachability(&self, lease: &ClientLease) -> ControlPlaneReachability {
        let address = match control_address(lease) {
            Ok(address) => address,
            Err(error) => return ControlPlaneReachability::Unknown(error),
        };

        let uri = normalize_control_uri(&address);
        let endpoint = match Endpoint::from_shared(uri.clone()) {
            Ok(endpoint) => endpoint.connect_timeout(CONNECT_TIMEOUT).tcp_nodelay(true),
            Err(error) => {
                return ControlPlaneReachability::Unknown(StoreError::Transport(format!(
                    "invalid control plane uri {uri}: {error}"
                )));
            }
        };

        match self.with_runtime(|runtime| runtime.block_on(endpoint.connect())) {
            Ok(_) => ControlPlaneReachability::Reachable,
            Err(_) => ControlPlaneReachability::Unreachable,
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn active_stream_sessions(&self) -> usize {
        self.streams.lock().len()
    }
}

fn control_plane_runtime_threads_from_env() -> usize {
    const DEFAULT_CONTROL_PLANE_THREADS: usize = 2;

    let Some(raw) = std::env::var(CONTROL_PLANE_THREADS_ENV).ok() else {
        return DEFAULT_CONTROL_PLANE_THREADS;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return DEFAULT_CONTROL_PLANE_THREADS;
    }
    match trimmed.parse::<usize>() {
        Ok(threads) if threads > 0 => threads,
        _ => {
            warn!(
                env = CONTROL_PLANE_THREADS_ENV,
                value = trimmed,
                default = DEFAULT_CONTROL_PLANE_THREADS,
                "invalid control-plane runtime thread count; falling back to default"
            );
            DEFAULT_CONTROL_PLANE_THREADS
        }
    }
}

fn control_request_timeout_from_env() -> Duration {
    crate::client::duration_from_env_ms(&[CONTROL_REQUEST_TIMEOUT_ENV])
        .unwrap_or(DEFAULT_CONTROL_REQUEST_TIMEOUT)
}

fn control_timeout_error(context: &'static str, timeout: Duration) -> StoreError {
    StoreError::Transport(format!(
        "{context} timed out after {}ms",
        timeout.as_millis()
    ))
}

impl Drop for ControlPlaneClient {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn spawn_stream_reader(
    session: Arc<ControlStreamSession>,
    mut inbound: tonic::Streaming<pb::ControlStreamReply>,
) {
    tokio::spawn(async move {
        loop {
            match inbound.next().await {
                Some(Ok(reply)) => {
                    if let Some(tx) = session.pending.lock().remove(&reply.request_id) {
                        let _ = tx.send(Ok(reply));
                    }
                }
                Some(Err(status)) => {
                    fail_stream_session(
                        &session,
                        format!("control stream receive failed: {status}"),
                    );
                    break;
                }
                None => {
                    fail_stream_session(
                        &session,
                        "control stream closed by remote peer".to_string(),
                    );
                    break;
                }
            }
        }
    });
}

pub(super) fn fail_stream_session(session: &ControlStreamSession, message: String) {
    session.closed.store(true, Ordering::Relaxed);
    let pending = std::mem::take(&mut *session.pending.lock());
    for (_, tx) in pending {
        let _ = tx.send(Err(StoreError::Transport(message.clone())));
    }
}
