use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, ClientStableId, ObjectKey, ObjectRoute, Result,
    RouteVersion, SegmentName, SegmentReservation, StoreError,
};
use serde::{Deserialize, Serialize};

const CONTROL_ADDR_LABEL: &str = "control_addr";
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(5);

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

#[derive(Default)]
pub(crate) struct ControlPlaneClient;

impl ControlPlaneClient {
    pub(crate) fn get_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        match self.call(
            lease,
            &ControlRequest::GetRoute {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                key: key.clone(),
            },
        )? {
            ControlResponse::Route(route) => Ok(route),
            response => Err(unexpected_response("route", response)),
        }
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
        match self.call(
            lease,
            &ControlRequest::CompareAndSwapRoute {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                key: key.clone(),
                expected,
                next: next.cloned(),
            },
        )? {
            ControlResponse::Cas(result) => Ok(result),
            response => Err(unexpected_response("cas", response)),
        }
    }

    pub(crate) fn replace_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()> {
        match self.call(
            lease,
            &ControlRequest::ReplaceRoute {
                namespace: namespace.to_string(),
                authority: authority.clone(),
                key: key.clone(),
                next: next.cloned(),
            },
        )? {
            ControlResponse::Unit => Ok(()),
            response => Err(unexpected_response("replace route", response)),
        }
    }

    pub(crate) fn reserve_any(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        match self.call(
            lease,
            &ControlRequest::ReserveAny {
                owner: owner.clone(),
                length_bytes,
            },
        )? {
            ControlResponse::Reservation(reservation) => Ok(reservation),
            response => Err(unexpected_response("reserve any", response)),
        }
    }

    pub(crate) fn reserve_specific(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        match self.call(
            lease,
            &ControlRequest::ReserveSpecific {
                owner: owner.clone(),
                segment_name: segment_name.clone(),
                length_bytes,
            },
        )? {
            ControlResponse::Reservation(reservation) => Ok(reservation),
            response => Err(unexpected_response("reserve specific", response)),
        }
    }

    pub(crate) fn release(
        &self,
        lease: &ClientLease,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        match self.call(
            lease,
            &ControlRequest::Release {
                owner: owner.clone(),
                segment_name: segment_name.clone(),
                offset_bytes,
                length_bytes,
            },
        )? {
            ControlResponse::Unit => Ok(()),
            response => Err(unexpected_response("release", response)),
        }
    }

    fn call(&self, lease: &ClientLease, request: &ControlRequest) -> Result<ControlResponse> {
        let address = control_address(lease)?;
        let mut stream = TcpStream::connect(address.as_str()).map_err(|error| {
            StoreError::Transport(format!(
                "control plane connect to {address} failed: {error}"
            ))
        })?;
        let _ = stream.set_nodelay(true);
        write_json(&mut stream, request)?;
        stream.shutdown(Shutdown::Write).map_err(|error| {
            StoreError::Transport(format!(
                "control plane shutdown to {address} failed: {error}"
            ))
        })?;
        read_json::<ControlEnvelope>(&mut stream)?.into_result()
    }
}

pub(crate) struct ControlPlaneHandle {
    address: String,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ControlPlaneHandle {
    pub(crate) fn spawn(
        bind_host: &str,
        authority: Arc<dyn AuthorityService>,
        allocator: Arc<dyn AllocatorService>,
    ) -> Result<Self> {
        let listener = TcpListener::bind((bind_host, 0)).map_err(|error| {
            StoreError::Transport(format!("control plane bind on {bind_host} failed: {error}"))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!("control plane configure listener failed: {error}"))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!("control plane local addr failed: {error}"))
            })?
            .to_string();
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = shutdown.clone();
        let thread = thread::Builder::new()
            .name(format!("store-control-{address}"))
            .spawn(move || serve(listener, thread_shutdown, authority, allocator))
            .map_err(|error| {
                StoreError::Transport(format!("control plane spawn failed: {error}"))
            })?;
        Ok(Self {
            address,
            shutdown,
            thread: Some(thread),
        })
    }

    pub(crate) fn address(&self) -> &str {
        &self.address
    }
}

impl Drop for ControlPlaneHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.address.as_str());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) fn control_address_label() -> &'static str {
    CONTROL_ADDR_LABEL
}

fn serve(
    listener: TcpListener,
    shutdown: Arc<AtomicBool>,
    authority: Arc<dyn AuthorityService>,
    allocator: Arc<dyn AllocatorService>,
) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let envelope =
                    match handle_request(&mut stream, authority.as_ref(), allocator.as_ref()) {
                        Ok(response) => ControlEnvelope::ok(response),
                        Err(error) => ControlEnvelope::err(error),
                    };
                let _ = write_json(&mut stream, &envelope);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(_) => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
        }
    }
}

fn handle_request(
    stream: &mut TcpStream,
    authority: &dyn AuthorityService,
    allocator: &dyn AllocatorService,
) -> Result<ControlResponse> {
    let request = read_json::<ControlRequest>(stream)?;
    match request {
        ControlRequest::GetRoute {
            namespace,
            authority: stable_id,
            key,
        } => authority
            .get_route(&namespace, &stable_id, &key)
            .map(ControlResponse::Route),
        ControlRequest::CompareAndSwapRoute {
            namespace,
            authority: stable_id,
            key,
            expected,
            next,
        } => authority
            .compare_and_swap_route(&namespace, &stable_id, &key, expected, next.as_ref())
            .map(ControlResponse::Cas),
        ControlRequest::ReplaceRoute {
            namespace,
            authority: stable_id,
            key,
            next,
        } => authority
            .replace_route(&namespace, &stable_id, &key, next.as_ref())
            .map(|_| ControlResponse::Unit),
        ControlRequest::ReserveAny {
            owner,
            length_bytes,
        } => allocator
            .reserve_any(&owner, length_bytes)
            .map(ControlResponse::Reservation),
        ControlRequest::ReserveSpecific {
            owner,
            segment_name,
            length_bytes,
        } => allocator
            .reserve_specific(&owner, &segment_name, length_bytes)
            .map(ControlResponse::Reservation),
        ControlRequest::Release {
            owner,
            segment_name,
            offset_bytes,
            length_bytes,
        } => allocator
            .release(&owner, &segment_name, offset_bytes, length_bytes)
            .map(|_| ControlResponse::Unit),
    }
}

fn write_json<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<()> {
    let payload = serde_json::to_vec(value)
        .map_err(|error| StoreError::Transport(format!("control plane encode failed: {error}")))?;
    stream
        .write_all(&payload)
        .map_err(|error| StoreError::Transport(format!("control plane write failed: {error}")))
}

fn read_json<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> Result<T> {
    let mut payload = Vec::new();
    stream
        .read_to_end(&mut payload)
        .map_err(|error| StoreError::Transport(format!("control plane read failed: {error}")))?;
    serde_json::from_slice(&payload)
        .map_err(|error| StoreError::Transport(format!("control plane decode failed: {error}")))
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

fn unexpected_response(operation: &str, response: ControlResponse) -> StoreError {
    StoreError::Transport(format!(
        "control plane {operation} returned unexpected response: {response:?}"
    ))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ControlRequest {
    GetRoute {
        namespace: String,
        authority: ClientStableId,
        key: ObjectKey,
    },
    CompareAndSwapRoute {
        namespace: String,
        authority: ClientStableId,
        key: ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<ObjectRoute>,
    },
    ReplaceRoute {
        namespace: String,
        authority: ClientStableId,
        key: ObjectKey,
        next: Option<ObjectRoute>,
    },
    ReserveAny {
        owner: ClientRuntimeId,
        length_bytes: u64,
    },
    ReserveSpecific {
        owner: ClientRuntimeId,
        segment_name: SegmentName,
        length_bytes: u64,
    },
    Release {
        owner: ClientRuntimeId,
        segment_name: SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum ControlResponse {
    Route(Option<ObjectRoute>),
    Cas(CasResult),
    Reservation(SegmentReservation),
    Unit,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ControlEnvelope {
    result: Option<ControlResponse>,
    error: Option<WireError>,
}

impl ControlEnvelope {
    fn ok(result: ControlResponse) -> Self {
        Self {
            result: Some(result),
            error: None,
        }
    }

    fn err(error: StoreError) -> Self {
        Self {
            result: None,
            error: Some(WireError::from(error)),
        }
    }

    fn into_result(self) -> Result<ControlResponse> {
        match (self.result, self.error) {
            (Some(result), None) => Ok(result),
            (None, Some(error)) => Err(error.into()),
            _ => Err(StoreError::Transport(
                "control plane returned an invalid envelope".to_string(),
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WireError {
    kind: WireErrorKind,
    message: String,
}

impl From<StoreError> for WireError {
    fn from(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(message) => Self {
                kind: WireErrorKind::NotFound,
                message,
            },
            StoreError::Conflict(message) => Self {
                kind: WireErrorKind::Conflict,
                message,
            },
            StoreError::InvalidState(message) => Self {
                kind: WireErrorKind::InvalidState,
                message,
            },
            StoreError::StaleEpoch(message) => Self {
                kind: WireErrorKind::StaleEpoch,
                message,
            },
            StoreError::Unsupported(message) => Self {
                kind: WireErrorKind::Unsupported,
                message,
            },
            StoreError::Allocator(message) => Self {
                kind: WireErrorKind::Allocator,
                message,
            },
            StoreError::Metadata(message) => Self {
                kind: WireErrorKind::Metadata,
                message,
            },
            StoreError::Transport(message) => Self {
                kind: WireErrorKind::Transport,
                message,
            },
        }
    }
}

impl From<WireError> for StoreError {
    fn from(error: WireError) -> Self {
        match error.kind {
            WireErrorKind::NotFound => StoreError::NotFound(error.message),
            WireErrorKind::Conflict => StoreError::Conflict(error.message),
            WireErrorKind::InvalidState => StoreError::InvalidState(error.message),
            WireErrorKind::StaleEpoch => StoreError::StaleEpoch(error.message),
            WireErrorKind::Unsupported => StoreError::Unsupported(error.message),
            WireErrorKind::Allocator => StoreError::Allocator(error.message),
            WireErrorKind::Metadata => StoreError::Metadata(error.message),
            WireErrorKind::Transport => StoreError::Transport(error.message),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum WireErrorKind {
    NotFound,
    Conflict,
    InvalidState,
    StaleEpoch,
    Unsupported,
    Allocator,
    Metadata,
    Transport,
}
