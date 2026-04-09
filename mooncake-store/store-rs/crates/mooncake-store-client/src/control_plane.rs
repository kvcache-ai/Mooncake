use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientRuntimeId, ClientStableId, CompatibilityDescriptor,
    ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteCasRequest, RouteVersion,
    SegmentName, SegmentReservation, StoreError,
};
use parking_lot::Mutex;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use self::pb::control_plane_service_server::ControlPlaneService as _;
use crate::observability::OperationTracker;

const CONTROL_ADDR_LABEL: &str = "control_addr";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) trait AuthorityService: Send + Sync {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>>;

    fn compare_and_swap_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

    fn replace_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()>;

    fn batch_get_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<Option<ObjectRoute>>> {
        keys.iter()
            .map(|key| self.get_route(namespace, authority, key))
            .collect()
    }

    fn batch_compare_and_swap_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<CasResult>> {
        requests
            .iter()
            .map(|request| {
                self.compare_and_swap_route(
                    namespace,
                    authority,
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                )
            })
            .collect()
    }

    fn batch_replace_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| {
                self.replace_route(namespace, authority, &request.key, request.next.as_ref())
            })
            .collect()
    }
}

pub(crate) trait AllocatorService: Send + Sync {
    fn reserve_any(&self, owner: &ClientRuntimeId, length_bytes: u64)
        -> Result<SegmentReservation>;

    fn reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation>;

    fn release(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()>;

    fn batch_reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: &[u64],
    ) -> Vec<Result<SegmentReservation>> {
        length_bytes
            .iter()
            .map(|length| self.reserve_any(owner, *length))
            .collect()
    }

    fn batch_reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        requests: &[ReserveSpecificOp],
    ) -> Vec<Result<SegmentReservation>> {
        requests
            .iter()
            .map(|request| {
                self.reserve_specific(owner, &request.segment_name, request.length_bytes)
            })
            .collect()
    }

    fn batch_release(&self, owner: &ClientRuntimeId, requests: &[ReleaseOp]) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| {
                self.release(
                    owner,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                )
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReserveSpecificOp {
    pub segment_name: SegmentName,
    pub length_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ReleaseOp {
    pub segment_name: SegmentName,
    pub offset_bytes: u64,
    pub length_bytes: u64,
}

pub(crate) struct ControlPlaneClient {
    runtime: Runtime,
    channels: Mutex<BTreeMap<String, Channel>>,
    streams: Mutex<BTreeMap<String, Arc<ControlStreamSession>>>,
}

struct ControlStreamSession {
    sender: mpsc::Sender<pb::ControlStreamRequest>,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Result<pb::ControlStreamReply>>>>>,
    next_request_id: AtomicU64,
    closed: AtomicBool,
}

impl ControlPlaneClient {
    pub(crate) fn new() -> Result<Self> {
        let runtime = RuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                StoreError::Transport(format!("control plane runtime init failed: {error}"))
            })?;
        Ok(Self {
            runtime,
            channels: Mutex::new(BTreeMap::new()),
            streams: Mutex::new(BTreeMap::new()),
        })
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
        let session = self.runtime.block_on(async move {
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
                .runtime
                .block_on(async move { sender.send(request).await })
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
            let reply = self.runtime.block_on(rx).map_err(|_| {
                StoreError::Transport("control stream response channel closed".to_string())
            })?;
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
        let channel = self.runtime.block_on(endpoint.connect()).map_err(|error| {
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
        self.runtime
            .block_on(async move { f(client).await.map(Response::into_inner) })
            .map_err(status_to_store_error)
    }

    pub(crate) fn clear_channels(&self) {
        self.streams.lock().clear();
        self.channels.lock().clear();
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn active_stream_sessions(&self) -> usize {
        self.streams.lock().len()
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

fn fail_stream_session(session: &ControlStreamSession, message: String) {
    session.closed.store(true, Ordering::Relaxed);
    let pending = std::mem::take(&mut *session.pending.lock());
    for (_, tx) in pending {
        let _ = tx.send(Err(StoreError::Transport(message.clone())));
    }
}

pub(crate) struct ControlPlaneHandle {
    address: String,
    shutdown: Option<oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl ControlPlaneHandle {
    pub(crate) fn spawn(
        bind_host: &str,
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
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
            .spawn(move || run_server(runtime, listener, shutdown_rx, authority, allocator))
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

pub(crate) fn control_address_label() -> &'static str {
    CONTROL_ADDR_LABEL
}

fn run_server(
    runtime: Runtime,
    listener: std::net::TcpListener,
    shutdown: oneshot::Receiver<()>,
    authority: Arc<dyn AuthorityService>,
    allocator: Arc<dyn AllocatorService>,
) {
    let service = GrpcControlPlaneService {
        authority,
        allocator,
    };
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

struct GrpcControlPlaneService {
    authority: Arc<dyn AuthorityService>,
    allocator: Arc<dyn AllocatorService>,
}

#[tonic::async_trait]
impl pb::control_plane_service_server::ControlPlaneService for GrpcControlPlaneService {
    type ControlStreamStream = ReceiverStream<std::result::Result<pb::ControlStreamReply, Status>>;

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
        let service = GrpcControlPlaneService {
            authority: self.authority.clone(),
            allocator: self.allocator.clone(),
        };
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

async fn handle_control_stream_request(
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

fn decode_error(error: Option<pb::ErrorDetail>) -> Result<()> {
    match error {
        Some(error) => Err(store_error_from_pb(error)),
        None => Ok(()),
    }
}

fn ensure_batch_len(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        return Ok(());
    }
    Err(StoreError::Transport(format!(
        "control plane {operation} reply length mismatch: expected={expected} actual={actual}"
    )))
}

fn status_to_store_error(status: Status) -> StoreError {
    StoreError::Transport(format!("control plane rpc failed: {status}"))
}

fn control_address(lease: &ClientLease) -> Result<String> {
    lease
        .endpoints
        .labels
        .get(CONTROL_ADDR_LABEL)
        .cloned()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            StoreError::Unsupported(format!(
                "client {} is missing control plane address",
                lease.runtime
            ))
        })
}

fn normalize_control_uri(address: &str) -> String {
    if address.contains("://") {
        return address.to_string();
    }
    format!("http://{address}")
}

fn pb_error(error: StoreError) -> pb::ErrorDetail {
    let (kind, message) = match error {
        StoreError::NotFound(message) => (pb::ErrorKind::NotFound, message),
        StoreError::Conflict(message) => (pb::ErrorKind::Conflict, message),
        StoreError::InvalidState(message) => (pb::ErrorKind::InvalidState, message),
        StoreError::StaleEpoch(message) => (pb::ErrorKind::StaleEpoch, message),
        StoreError::Unsupported(message) => (pb::ErrorKind::Unsupported, message),
        StoreError::Allocator(message) => (pb::ErrorKind::Allocator, message),
        StoreError::Metadata(message) => (pb::ErrorKind::Metadata, message),
        StoreError::Transport(message) => (pb::ErrorKind::Transport, message),
    };
    pb::ErrorDetail {
        kind: kind as i32,
        message,
    }
}

fn store_error_from_pb(error: pb::ErrorDetail) -> StoreError {
    match pb::ErrorKind::try_from(error.kind).unwrap_or(pb::ErrorKind::Transport) {
        pb::ErrorKind::NotFound => StoreError::NotFound(error.message),
        pb::ErrorKind::Conflict => StoreError::Conflict(error.message),
        pb::ErrorKind::InvalidState => StoreError::InvalidState(error.message),
        pb::ErrorKind::StaleEpoch => StoreError::StaleEpoch(error.message),
        pb::ErrorKind::Unsupported => StoreError::Unsupported(error.message),
        pb::ErrorKind::Allocator => StoreError::Allocator(error.message),
        pb::ErrorKind::Metadata => StoreError::Metadata(error.message),
        pb::ErrorKind::Transport | pb::ErrorKind::Unspecified => {
            StoreError::Transport(error.message)
        }
    }
}

fn pb_runtime_id(runtime: &ClientRuntimeId) -> pb::ClientRuntimeId {
    pb::ClientRuntimeId {
        stable_id: runtime.stable_id.0.clone(),
        epoch: runtime.epoch.0,
    }
}

fn try_runtime_id(runtime: &pb::ClientRuntimeId) -> Result<ClientRuntimeId> {
    Ok(ClientRuntimeId {
        stable_id: ClientStableId::new(runtime.stable_id.clone()),
        epoch: ClientEpoch(runtime.epoch),
    })
}

fn pb_compatibility(descriptor: &CompatibilityDescriptor) -> pb::CompatibilityDescriptor {
    pb::CompatibilityDescriptor {
        store_api_version: descriptor.store_api_version,
        metadata_schema_version: descriptor.metadata_schema_version,
        transport_api_version: descriptor.transport_api_version,
        capabilities: descriptor.capabilities.iter().cloned().collect(),
    }
}

fn try_compatibility(descriptor: &pb::CompatibilityDescriptor) -> CompatibilityDescriptor {
    CompatibilityDescriptor {
        store_api_version: descriptor.store_api_version,
        metadata_schema_version: descriptor.metadata_schema_version,
        transport_api_version: descriptor.transport_api_version,
        capabilities: descriptor.capabilities.iter().cloned().collect(),
    }
}

fn pb_replica_tier(tier: ReplicaTier) -> i32 {
    match tier {
        ReplicaTier::Dram => pb::ReplicaTier::Dram as i32,
        ReplicaTier::Nvme => pb::ReplicaTier::Nvme as i32,
        ReplicaTier::File => pb::ReplicaTier::File as i32,
        ReplicaTier::Unknown => pb::ReplicaTier::Unknown as i32,
    }
}

fn try_replica_tier(tier: i32) -> Result<ReplicaTier> {
    Ok(
        match pb::ReplicaTier::try_from(tier).unwrap_or(pb::ReplicaTier::Unknown) {
            pb::ReplicaTier::Dram => ReplicaTier::Dram,
            pb::ReplicaTier::Nvme => ReplicaTier::Nvme,
            pb::ReplicaTier::File => ReplicaTier::File,
            pb::ReplicaTier::Unknown | pb::ReplicaTier::Unspecified => ReplicaTier::Unknown,
        },
    )
}

fn pb_route_state(state: mooncake_store_core::RouteState) -> i32 {
    match state {
        mooncake_store_core::RouteState::Active => pb::RouteState::Active as i32,
        mooncake_store_core::RouteState::Deleting => pb::RouteState::Deleting as i32,
        mooncake_store_core::RouteState::Tombstone => pb::RouteState::Tombstone as i32,
    }
}

fn try_route_state(state: i32) -> Result<mooncake_store_core::RouteState> {
    Ok(
        match pb::RouteState::try_from(state).unwrap_or(pb::RouteState::Unspecified) {
            pb::RouteState::Active => mooncake_store_core::RouteState::Active,
            pb::RouteState::Deleting => mooncake_store_core::RouteState::Deleting,
            pb::RouteState::Tombstone | pb::RouteState::Unspecified => {
                mooncake_store_core::RouteState::Tombstone
            }
        },
    )
}

fn pb_replica_route(replica: &ReplicaRoute) -> pb::ReplicaRoute {
    pb::ReplicaRoute {
        owner: Some(pb_runtime_id(&replica.owner)),
        segment_name: replica.segment_name.0.clone(),
        offset: replica.offset,
        segment_offset: replica.segment_offset,
        length: replica.length,
        checksum: replica.checksum,
        tier: pb_replica_tier(replica.tier),
        priority: u32::from(replica.priority),
    }
}

fn try_replica_route(replica: pb::ReplicaRoute) -> Result<ReplicaRoute> {
    let owner = replica
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane replica route is missing owner".to_string())
        })?;
    let priority = u16::try_from(replica.priority).map_err(|_| {
        StoreError::Transport(format!(
            "control plane replica route priority {} does not fit u16",
            replica.priority
        ))
    })?;
    Ok(ReplicaRoute {
        owner,
        segment_name: SegmentName::new(replica.segment_name),
        offset: replica.offset,
        segment_offset: replica.segment_offset,
        length: replica.length,
        checksum: replica.checksum,
        tier: try_replica_tier(replica.tier)?,
        priority,
    })
}

fn pb_object_route(route: &ObjectRoute) -> pb::ObjectRoute {
    pb::ObjectRoute {
        key: route.key.0.clone(),
        version: route.version.0,
        state: pb_route_state(route.state),
        compatibility: Some(pb_compatibility(&route.compatibility)),
        replicas: route.replicas.iter().map(pb_replica_route).collect(),
    }
}

fn try_object_route(route: pb::ObjectRoute) -> Result<ObjectRoute> {
    let compatibility = route
        .compatibility
        .as_ref()
        .map(try_compatibility)
        .ok_or_else(|| {
            StoreError::Transport("control plane object route is missing compatibility".to_string())
        })?;
    Ok(ObjectRoute {
        key: ObjectKey::new(route.key),
        version: RouteVersion(route.version),
        state: try_route_state(route.state)?,
        compatibility,
        replicas: route
            .replicas
            .into_iter()
            .map(try_replica_route)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn try_object_route_ref(route: &pb::ObjectRoute) -> Result<ObjectRoute> {
    try_object_route(route.clone())
}

fn pb_cas_result(result: &CasResult) -> pb::CasResult {
    pb::CasResult {
        applied: result.applied,
        current: result.current.as_ref().map(pb_object_route),
    }
}

fn try_cas_result(result: pb::CasResult) -> Result<CasResult> {
    Ok(CasResult {
        applied: result.applied,
        current: result.current.map(try_object_route).transpose()?,
    })
}

fn pb_segment_reservation(reservation: &SegmentReservation) -> pb::SegmentReservation {
    pb::SegmentReservation {
        owner: Some(pb_runtime_id(&reservation.owner)),
        segment_name: reservation.segment_name.0.clone(),
        offset_bytes: reservation.offset_bytes,
        length_bytes: reservation.length_bytes,
    }
}

fn try_segment_reservation(reservation: pb::SegmentReservation) -> Result<SegmentReservation> {
    let owner = reservation
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane segment reservation is missing owner".to_string())
        })?;
    Ok(SegmentReservation {
        owner,
        segment_name: SegmentName::new(reservation.segment_name),
        offset_bytes: reservation.offset_bytes,
        length_bytes: reservation.length_bytes,
    })
}

pub(crate) mod pb {
    tonic::include_proto!("mooncake.store.control");
}
