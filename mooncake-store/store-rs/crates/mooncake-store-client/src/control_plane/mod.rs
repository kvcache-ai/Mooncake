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

use crate::observability::OperationTracker;

const CONTROL_ADDR_LABEL: &str = "control_addr";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_PLANE_THREADS_ENV: &str = "MC_STORE_RS_CONTROL_PLANE_THREADS";

pub(crate) trait AuthorityService: Send + Sync {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>>;

    fn list_routes_by_replica_owner(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>>;

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

pub(crate) trait EvictionService: Send + Sync {
    fn batch_report_route_hits(&self, keys: &[ObjectKey]) -> Result<usize>;

    fn batch_track_routes(&self, routes: &[ObjectRoute]) -> Result<usize>;
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
    runtime: Option<Runtime>,
    channels: Mutex<BTreeMap<String, Channel>>,
    streams: Mutex<BTreeMap<String, Arc<ControlStreamSession>>>,
}

struct ControlStreamSession {
    sender: mpsc::Sender<pb::ControlStreamRequest>,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<Result<pb::ControlStreamReply>>>>>,
    next_request_id: AtomicU64,
    closed: AtomicBool,
}

pub(crate) fn control_address_label() -> &'static str {
    CONTROL_ADDR_LABEL
}

mod client;
mod codec;
mod server;

pub(crate) use server::ControlPlaneHandle;

#[cfg(test)]
use self::client::fail_stream_session;
#[cfg(test)]
use self::codec::{
    control_address, decode_error, ensure_batch_len, normalize_control_uri, pb_cas_result,
    pb_compatibility, pb_error, pb_object_route, pb_replica_route, pb_replica_tier, pb_route_state,
    pb_runtime_id, pb_segment_reservation, status_to_store_error, store_error_from_pb,
    try_cas_result, try_compatibility, try_object_route, try_replica_route, try_replica_tier,
    try_route_state, try_runtime_id, try_segment_reservation,
};
#[cfg(test)]
use self::server::{handle_control_stream_request, GrpcControlPlaneService};

#[cfg(test)]
mod tests;

pub(crate) mod pb {
    tonic::include_proto!("mooncake.store.control");
}
