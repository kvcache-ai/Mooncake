use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientRuntimeId, ClientStableId, CompatibilityDescriptor,
    ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteVersion, SegmentName,
    SegmentReservation, StoreError,
};
use parking_lot::Mutex;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};
use tracing::warn;

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
}

pub(crate) struct ControlPlaneClient {
    runtime: Runtime,
    channels: Mutex<BTreeMap<String, Channel>>,
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
        })
    }

    pub(crate) fn get_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        let channel = self.channel_for(lease)?;
        let request = pb::GetRouteRequest {
            namespace: namespace.to_string(),
            authority: authority.0.clone(),
            key: key.0.clone(),
        };
        let reply = self.rpc(
            |mut client| async move { client.get_route(Request::new(request)).await },
            channel,
        )?;
        decode_error(reply.error)?;
        reply.route.map(try_object_route).transpose()
    }

    pub(crate) fn compare_and_swap_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let channel = self.channel_for(lease)?;
        let request = pb::CompareAndSwapRouteRequest {
            namespace: namespace.to_string(),
            authority: authority.0.clone(),
            key: key.0.clone(),
            expected_version: expected.map(|version| version.0),
            next: next.map(pb_object_route),
        };
        let reply = self.rpc(
            |mut client| async move { client.compare_and_swap_route(Request::new(request)).await },
            channel,
        )?;
        decode_error(reply.error)?;
        let result = reply.result.ok_or_else(|| {
            StoreError::Transport("control plane cas reply is missing result".to_string())
        })?;
        try_cas_result(result)
    }

    pub(crate) fn replace_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()> {
        let channel = self.channel_for(lease)?;
        let request = pb::ReplaceRouteRequest {
            namespace: namespace.to_string(),
            authority: authority.0.clone(),
            key: key.0.clone(),
            next: next.map(pb_object_route),
        };
        let reply = self.rpc(
            |mut client| async move { client.replace_route(Request::new(request)).await },
            channel,
        )?;
        decode_error(reply.error)
    }

    pub(crate) fn reserve_any(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let channel = self.channel_for(lease)?;
        let request = pb::ReserveAnyRequest {
            owner: Some(pb_runtime_id(owner)),
            length_bytes,
        };
        let reply = self.rpc(
            |mut client| async move { client.reserve_any(Request::new(request)).await },
            channel,
        )?;
        decode_error(reply.error)?;
        let reservation = reply.reservation.ok_or_else(|| {
            StoreError::Transport(
                "control plane reserve_any reply is missing reservation".to_string(),
            )
        })?;
        try_segment_reservation(reservation)
    }

    pub(crate) fn reserve_specific(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let channel = self.channel_for(lease)?;
        let request = pb::ReserveSpecificRequest {
            owner: Some(pb_runtime_id(owner)),
            segment_name: segment_name.0.clone(),
            length_bytes,
        };
        let reply = self.rpc(
            |mut client| async move { client.reserve_specific(Request::new(request)).await },
            channel,
        )?;
        decode_error(reply.error)?;
        let reservation = reply.reservation.ok_or_else(|| {
            StoreError::Transport(
                "control plane reserve_specific reply is missing reservation".to_string(),
            )
        })?;
        try_segment_reservation(reservation)
    }

    pub(crate) fn release(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let channel = self.channel_for(lease)?;
        let request = pb::ReleaseRequest {
            owner: Some(pb_runtime_id(owner)),
            segment_name: segment_name.0.clone(),
            offset_bytes,
            length_bytes,
        };
        let reply = self.rpc(
            |mut client| async move { client.release(Request::new(request)).await },
            channel,
        )?;
        decode_error(reply.error)
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
        self.channels.lock().clear();
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
}

fn decode_error(error: Option<pb::ErrorDetail>) -> Result<()> {
    match error {
        Some(error) => Err(store_error_from_pb(error)),
        None => Ok(()),
    }
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
