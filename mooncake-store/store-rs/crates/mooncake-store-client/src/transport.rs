use std::collections::BTreeMap;
use std::ffi::c_void;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::slice;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mooncake_store_core::{MetadataBackend, Result, StoreError};
use mooncake_transport::{
    ClassicEngineConfig, ClassicTransferEngine, Opcode, SegmentBuffer, SegmentInfo, SegmentKind,
    TentEngine, TentEngineConfig, TransferBatchHints, TransferProgress, TransferRequest,
    TransferStatus,
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BatchWaitFailureKind {
    PollFailed,
    TerminalStatus(TransferStatus),
    StallTimeout,
    RequestDeadlineExceeded,
}

#[derive(Clone, Debug)]
pub(crate) struct BatchWaitError {
    kind: BatchWaitFailureKind,
    error: StoreError,
}

impl BatchWaitError {
    fn poll_failed(batch_id: u64, error: StoreError) -> Self {
        Self {
            kind: BatchWaitFailureKind::PollFailed,
            error: StoreError::Transport(format!(
                "transport batch {batch_id} status poll failed: {error}"
            )),
        }
    }

    fn terminal_status(batch_id: u64, status: TransferStatus, transferred_bytes: u64) -> Self {
        let kind = match status {
            TransferStatus::Timeout => BatchWaitFailureKind::StallTimeout,
            _ => BatchWaitFailureKind::TerminalStatus(status),
        };
        let message = match status {
            TransferStatus::Timeout => format!(
                "transport batch {batch_id} stalled after {} byte(s) without progress",
                transferred_bytes
            ),
            _ => format!(
                "transport batch {batch_id} failed with status {:?} after {} byte(s)",
                status, transferred_bytes
            ),
        };
        Self {
            kind,
            error: StoreError::Transport(message),
        }
    }

    fn stall_timeout(batch_id: u64, stall_timeout: Duration, transferred_bytes: u64) -> Self {
        Self {
            kind: BatchWaitFailureKind::StallTimeout,
            error: StoreError::Transport(format!(
                "transport batch {batch_id} stalled for {}ms after {} byte(s)",
                stall_timeout.as_millis(),
                transferred_bytes
            )),
        }
    }

    fn request_deadline_exceeded(batch_id: u64, elapsed: Duration, transferred_bytes: u64) -> Self {
        Self {
            kind: BatchWaitFailureKind::RequestDeadlineExceeded,
            error: StoreError::Transport(format!(
                "request deadline exceeded for transport batch {batch_id} after {}ms with {} byte(s) transferred",
                elapsed.as_millis(),
                transferred_bytes
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> BatchWaitFailureKind {
        self.kind
    }

    pub(crate) fn marks_runtime_suspect(&self) -> bool {
        !matches!(self.kind, BatchWaitFailureKind::RequestDeadlineExceeded)
    }
}

impl From<BatchWaitError> for StoreError {
    fn from(error: BatchWaitError) -> Self {
        error.error
    }
}

pub trait StoreTransport: Send + Sync {
    fn segment_name(&self) -> Result<String>;
    fn rpc_server_address(&self) -> Result<(String, u16)>;
    fn open_segment(&self, segment_name: &str) -> Result<u64>;
    fn close_segment(&self, handle: u64) -> Result<()>;
    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo>;
    fn republish_local_metadata(&self) -> Result<()> {
        Ok(())
    }
    fn adopt_local_memory(&self, _addr: *mut c_void, _size: usize, _location: &str) -> Result<()> {
        Ok(())
    }
    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void>;
    fn free_memory(&self, addr: *mut c_void) -> Result<()>;
    fn max_registration_bytes(&self) -> Option<usize> {
        None
    }
    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()>;
    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()>;
    fn allocate_batch(&self, batch_size: usize) -> Result<u64>;
    fn free_batch(&self, batch_id: u64) -> Result<()>;
    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()>;
    fn submit_with_hints(
        &self,
        batch_id: u64,
        requests: &[TransferRequest],
        _hints: &TransferBatchHints,
    ) -> Result<()> {
        self.submit(batch_id, requests)
    }
    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress>;
    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress>;
}

pub trait StoreTransportFactory: Send + Sync {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>>;
}

fn registration_chunks(
    addr: *mut c_void,
    size: usize,
    max_registration_bytes: Option<usize>,
) -> Result<Vec<(*mut c_void, usize)>> {
    let chunk_bytes = max_registration_bytes
        .filter(|value| *value > 0)
        .unwrap_or(size.max(1));
    let mut chunks = Vec::new();
    if size == 0 {
        chunks.push((addr, 0));
        return Ok(chunks);
    }

    let base = addr as usize;
    let mut offset = 0usize;
    while offset < size {
        let chunk_len = (size - offset).min(chunk_bytes);
        let chunk_addr = base.checked_add(offset).ok_or_else(|| {
            StoreError::Transport("registration chunk address overflow".to_string())
        })? as *mut c_void;
        chunks.push((chunk_addr, chunk_len));
        offset = offset.checked_add(chunk_len).ok_or_else(|| {
            StoreError::Transport("registration chunk offset overflow".to_string())
        })?;
    }
    Ok(chunks)
}

fn for_each_registration_chunk<F>(
    addr: *mut c_void,
    size: usize,
    max_registration_bytes: Option<usize>,
    mut operation: F,
) -> Result<()>
where
    F: FnMut(*mut c_void, usize) -> Result<()>,
{
    for (chunk_addr, chunk_len) in registration_chunks(addr, size, max_registration_bytes)? {
        operation(chunk_addr, chunk_len)?;
    }
    Ok(())
}

const HTTP_TRANSPORT_PATH_OPEN_SEGMENT: &str = "/v1/transport/open-segment";
const HTTP_TRANSPORT_PATH_SUBMIT_BATCH: &str = "/v1/transport/submit-batch";
const HTTP_TRANSPORT_PATH_BATCH_STATUS: &str = "/v1/transport/batch-status";
const HTTP_TRANSPORT_LABEL: &str = "transport_http_address";
const HTTP_TRANSPORT_READ_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct HttpStoreTransportFactory {
    metadata: Arc<dyn MetadataBackend>,
    local_runtime_rpc_address: String,
}

impl HttpStoreTransportFactory {
    pub fn new(
        metadata: Arc<dyn MetadataBackend>,
        local_runtime_rpc_address: impl Into<String>,
    ) -> Self {
        Self {
            metadata,
            local_runtime_rpc_address: local_runtime_rpc_address.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TentTransportFactory {
    config: TentEngineConfig,
}

impl TentTransportFactory {
    pub fn new(config: TentEngineConfig) -> Self {
        Self { config }
    }
}

impl StoreTransportFactory for TentTransportFactory {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>> {
        Ok(Arc::new(TentEngine::new(
            &self.config.clone().set("local_segment_name", segment_name),
        )?))
    }
}

impl StoreTransportFactory for HttpStoreTransportFactory {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>> {
        Ok(Arc::new(HttpStoreTransport::new(
            self.metadata.clone(),
            segment_name,
            &self.local_runtime_rpc_address,
        )?))
    }
}

#[derive(Clone, Debug)]
pub struct ClassicTeTransportFactory {
    config: ClassicEngineConfig,
}

impl ClassicTeTransportFactory {
    pub fn new(config: ClassicEngineConfig) -> Self {
        Self { config }
    }
}

impl StoreTransportFactory for ClassicTeTransportFactory {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>> {
        Ok(Arc::new(ClassicTeTransport::new(
            self.config.clone(),
            segment_name,
        )?))
    }
}

impl StoreTransport for TentEngine {
    fn segment_name(&self) -> Result<String> {
        TentEngine::segment_name(self)
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        TentEngine::rpc_server_address(self)
    }

    fn open_segment(&self, segment_name: &str) -> Result<u64> {
        TentEngine::open_segment(self, segment_name)
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        TentEngine::close_segment(self, handle)
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        TentEngine::get_segment_info(self, handle)
    }

    fn republish_local_metadata(&self) -> Result<()> {
        TentEngine::republish_local_metadata(self)
    }

    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        TentEngine::allocate_memory(self, size, location)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        TentEngine::free_memory(self, addr)
    }

    fn max_registration_bytes(&self) -> Option<usize> {
        TentEngine::max_registration_bytes(self)
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        for_each_registration_chunk(
            addr,
            size,
            self.max_registration_bytes(),
            |chunk_addr, chunk_len| TentEngine::register_memory(self, chunk_addr, chunk_len),
        )
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        for_each_registration_chunk(
            addr,
            size,
            self.max_registration_bytes(),
            |chunk_addr, chunk_len| TentEngine::unregister_memory(self, chunk_addr, chunk_len),
        )
    }

    fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        TentEngine::allocate_batch(self, batch_size)
    }

    fn free_batch(&self, batch_id: u64) -> Result<()> {
        TentEngine::free_batch(self, batch_id)
    }

    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        TentEngine::submit(self, batch_id, requests)
    }

    fn submit_with_hints(
        &self,
        batch_id: u64,
        requests: &[TransferRequest],
        _hints: &TransferBatchHints,
    ) -> Result<()> {
        TentEngine::submit(self, batch_id, requests)
    }

    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        TentEngine::task_status(self, batch_id, task_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        TentEngine::overall_status(self, batch_id)
    }
}

#[derive(Clone, Debug)]
enum ClassicAllocationOwner {
    Borrowed,
    Posix,
    Numa,
}

#[derive(Clone, Debug)]
struct HttpAllocationRecord {
    owned: bool,
}

#[derive(Clone, Debug)]
struct HttpSegmentHandle {
    remote_runtime_rpc: String,
    remote_segment_name: String,
}

#[derive(Clone, Debug)]
struct HttpBatchRecord {
    remote_runtime_rpc: String,
}

pub struct HttpStoreTransport {
    metadata: Arc<dyn MetadataBackend>,
    segment_name: String,
    local_runtime_rpc_address: String,
    allocations: Mutex<BTreeMap<usize, HttpAllocationRecord>>,
    next_handle: Mutex<u64>,
    next_batch: Mutex<u64>,
    segment_handles: Mutex<BTreeMap<u64, HttpSegmentHandle>>,
    batches: Mutex<BTreeMap<u64, HttpBatchRecord>>,
}

impl HttpStoreTransport {
    fn new(
        metadata: Arc<dyn MetadataBackend>,
        segment_name: &str,
        local_runtime_rpc_address: &str,
    ) -> Result<Self> {
        Ok(Self {
            metadata,
            segment_name: segment_name.to_string(),
            local_runtime_rpc_address: local_runtime_rpc_address.to_string(),
            allocations: Mutex::new(BTreeMap::new()),
            next_handle: Mutex::new(1),
            next_batch: Mutex::new(1),
            segment_handles: Mutex::new(BTreeMap::new()),
            batches: Mutex::new(BTreeMap::new()),
        })
    }

    fn next_segment_handle(&self) -> u64 {
        let mut next = self.next_handle.lock();
        let handle = *next;
        *next = next.saturating_add(1);
        handle
    }

    fn next_batch_id(&self) -> u64 {
        let mut next = self.next_batch.lock();
        let batch_id = *next;
        *next = next.saturating_add(1);
        batch_id
    }

    fn resolve_remote_runtime_rpc(&self, segment_name: &str) -> Result<String> {
        let segments = self.metadata.list_segments(None)?;
        let owner = segments
            .into_iter()
            .find(|segment| segment.segment_name.0 == segment_name)
            .map(|segment| segment.owner)
            .ok_or_else(|| StoreError::NotFound(format!("segment {segment_name} not found")))?;
        let lease = self
            .metadata
            .list_live_clients()?
            .into_iter()
            .find(|lease| lease.runtime == owner)
            .ok_or_else(|| StoreError::NotFound(format!("runtime {} is not available", owner)))?;
        lease
            .endpoints
            .labels
            .get(HTTP_TRANSPORT_LABEL)
            .cloned()
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "runtime {} does not expose {}",
                    lease.runtime, HTTP_TRANSPORT_LABEL
                ))
            })
    }
}

#[derive(Clone, Debug)]
struct ClassicAllocationRecord {
    location: String,
    size: usize,
    owner: ClassicAllocationOwner,
}

pub struct ClassicTeTransport {
    engine: RwLock<ClassicTransferEngine>,
    config: ClassicEngineConfig,
    segment_name: String,
    max_registration_bytes: Option<usize>,
    allocations: Mutex<BTreeMap<usize, ClassicAllocationRecord>>,
}

impl ClassicTeTransport {
    fn new(config: ClassicEngineConfig, segment_name: &str) -> Result<Self> {
        Ok(Self {
            engine: RwLock::new(ClassicTransferEngine::new(&config, segment_name)?),
            max_registration_bytes: matches!(
                config.transport_protocol(),
                mooncake_transport::ClassicTransportProtocol::Rdma
            )
            .then(ClassicTransferEngine::rdma_max_registration_size)
            .flatten(),
            config,
            segment_name: segment_name.to_string(),
            allocations: Mutex::new(BTreeMap::new()),
        })
    }

    fn register_memory_with_engine(
        &self,
        engine: &ClassicTransferEngine,
        addr: *mut c_void,
        size: usize,
        location: &str,
    ) -> Result<()> {
        for_each_registration_chunk(
            addr,
            size,
            self.max_registration_bytes,
            |chunk_addr, chunk_len| {
                engine.register_local_memory(chunk_addr, chunk_len, location, true)
            },
        )
    }

    fn unregister_memory_with_engine(
        &self,
        engine: &ClassicTransferEngine,
        addr: *mut c_void,
        size: usize,
    ) -> Result<()> {
        for_each_registration_chunk(addr, size, self.max_registration_bytes, |chunk_addr, _| {
            engine.unregister_local_memory(chunk_addr)
        })
    }

    fn buffer_location(&self, base: u64, length: u64) -> String {
        let allocations = self.allocations.lock();
        classic_buffer_location(&allocations, base, length)
    }

    fn rebuild_engine_for_metadata_recovery(&self) -> Result<()> {
        let allocations = self
            .allocations
            .lock()
            .iter()
            .map(|(addr, record)| (*addr, record.clone()))
            .collect::<Vec<_>>();
        let replacement = ClassicTransferEngine::new(&self.config, &self.segment_name)?;
        for (addr, record) in &allocations {
            self.register_memory_with_engine(
                &replacement,
                *addr as *mut c_void,
                record.size,
                &record.location,
            )?;
        }
        let mut engine = self.engine.write();
        let previous = std::mem::replace(&mut *engine, replacement);
        drop(previous);
        engine.republish_local_metadata()
    }
}

impl StoreTransport for HttpStoreTransport {
    fn segment_name(&self) -> Result<String> {
        Ok(self.segment_name.clone())
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        parse_rpc_server_address(&self.local_runtime_rpc_address)
    }

    fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let remote_runtime_rpc = self.resolve_remote_runtime_rpc(segment_name)?;
        let request = HttpOpenSegmentRequest {
            segment_name: segment_name.to_string(),
        };
        let _: HttpJsonResponse<HttpOpenSegmentResponse> = http_json_request(
            &remote_runtime_rpc,
            "POST",
            HTTP_TRANSPORT_PATH_OPEN_SEGMENT,
            Some(&request),
            &[],
        )?;
        let handle = self.next_segment_handle();
        self.segment_handles.lock().insert(
            handle,
            HttpSegmentHandle {
                remote_runtime_rpc,
                remote_segment_name: segment_name.to_string(),
            },
        );
        Ok(handle)
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        self.segment_handles
            .lock()
            .remove(&handle)
            .map(|_| ())
            .ok_or_else(|| StoreError::NotFound(format!("segment handle {handle} not found")))
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        let segment = self
            .segment_handles
            .lock()
            .get(&handle)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(format!("segment handle {handle} not found")))?;
        let path = format!(
            "{}/{}",
            HTTP_TRANSPORT_PATH_OPEN_SEGMENT, segment.remote_segment_name
        );
        let response: HttpJsonResponse<HttpOpenSegmentResponse> = http_json_request(
            &segment.remote_runtime_rpc,
            "GET",
            &path,
            None::<&HttpEmptyBody>,
            &[],
        )?;
        Ok(response.json.segment)
    }

    fn adopt_local_memory(&self, addr: *mut c_void, _size: usize, _location: &str) -> Result<()> {
        self.allocations
            .lock()
            .entry(addr as usize)
            .or_insert_with(|| HttpAllocationRecord { owned: false });
        Ok(())
    }

    fn allocate_memory(&self, size: usize, _location: &str) -> Result<*mut c_void> {
        let alignment = default_classic_alignment();
        let mut addr = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut addr, alignment, size.max(1)) };
        if rc != 0 {
            return Err(StoreError::Allocator(format!(
                "http transport posix_memalign failed with rc={rc} size={size} alignment={alignment}"
            )));
        }
        self.allocations
            .lock()
            .insert(addr as usize, HttpAllocationRecord { owned: true });
        Ok(addr)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        let record = self
            .allocations
            .lock()
            .remove(&(addr as usize))
            .ok_or_else(|| {
                StoreError::Allocator(format!("http transport does not own allocation {:p}", addr))
            })?;
        if record.owned {
            unsafe { libc::free(addr) };
        }
        Ok(())
    }

    fn register_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
        Ok(())
    }

    fn unregister_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
        Ok(())
    }

    fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        if batch_size == 0 {
            return Err(StoreError::Transport(
                "batch_size must be greater than zero".to_string(),
            ));
        }
        Ok(self.next_batch_id())
    }

    fn free_batch(&self, batch_id: u64) -> Result<()> {
        self.batches.lock().remove(&batch_id);
        Ok(())
    }

    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        self.submit_with_hints(batch_id, requests, &TransferBatchHints::default())
    }

    fn submit_with_hints(
        &self,
        batch_id: u64,
        requests: &[TransferRequest],
        hints: &TransferBatchHints,
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        let handles = self.segment_handles.lock();
        let first = handles
            .get(&requests[0].target_id)
            .cloned()
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "segment handle {} not found",
                    requests[0].target_id
                ))
            })?;
        for request in requests.iter().skip(1) {
            let current = handles.get(&request.target_id).ok_or_else(|| {
                StoreError::NotFound(format!("segment handle {} not found", request.target_id))
            })?;
            if current.remote_runtime_rpc != first.remote_runtime_rpc {
                return Err(StoreError::Unsupported(
                    "http transport batches must target a single runtime".to_string(),
                ));
            }
        }
        drop(handles);

        let mut body = Vec::new();
        let items = requests
            .iter()
            .map(|request| {
                let segment = self
                    .segment_handles
                    .lock()
                    .get(&request.target_id)
                    .cloned()
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "segment handle {} not found",
                            request.target_id
                        ))
                    })?;
                let body_offset = body.len() as u64;
                let bytes = unsafe {
                    slice::from_raw_parts(request.source.cast::<u8>(), request.length as usize)
                };
                body.extend_from_slice(bytes);
                Ok(HttpTransferItem {
                    opcode: request.opcode,
                    segment_name: segment.remote_segment_name,
                    target_offset: request.target_offset,
                    length: request.length,
                    body_offset,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let request = HttpSubmitBatchRequest {
            batch_id,
            hints: hints.clone(),
            items,
        };
        let response: HttpJsonResponse<HttpSubmitBatchWireResponse> = http_json_request(
            &first.remote_runtime_rpc,
            "POST",
            HTTP_TRANSPORT_PATH_SUBMIT_BATCH,
            Some(&request),
            &body,
        )?;
        let expected_read_body_length =
            usize::try_from(response.json.read_body_length).map_err(|_| {
                StoreError::Transport("http read response length does not fit usize".to_string())
            })?;
        if expected_read_body_length != response.raw_body.len() {
            return Err(StoreError::Transport(
                "http transport read response length header does not match payload".to_string(),
            ));
        }
        let mut read_cursor = 0usize;
        for request in requests {
            if request.opcode != Opcode::Read {
                continue;
            }
            let len = usize::try_from(request.length).map_err(|_| {
                StoreError::Transport("request length does not fit usize".to_string())
            })?;
            let end = read_cursor
                .checked_add(len)
                .ok_or_else(|| StoreError::Transport("http read response overflow".to_string()))?;
            if end > response.raw_body.len() {
                return Err(StoreError::Transport(
                    "http transport read response shorter than expected".to_string(),
                ));
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    response.raw_body[read_cursor..end].as_ptr(),
                    request.source.cast::<u8>(),
                    len,
                );
            }
            read_cursor = end;
        }
        if read_cursor != response.raw_body.len() {
            return Err(StoreError::Transport(
                "http transport read response contains unexpected trailing bytes".to_string(),
            ));
        }
        self.batches.lock().insert(
            batch_id,
            HttpBatchRecord {
                remote_runtime_rpc: first.remote_runtime_rpc.clone(),
            },
        );
        if response.json.progress.status == TransferStatus::Completed {
            return Ok(());
        }
        Err(StoreError::Transport(format!(
            "http transport batch {batch_id} returned status {:?}",
            response.json.progress.status
        )))
    }

    fn task_status(&self, batch_id: u64, _task_id: usize) -> Result<TransferProgress> {
        self.overall_status(batch_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        let batch = self
            .batches
            .lock()
            .get(&batch_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(format!("batch {batch_id} not found")))?;
        let path = format!("{}/{}", HTTP_TRANSPORT_PATH_BATCH_STATUS, batch_id);
        let response: HttpJsonResponse<HttpBatchStatusResponse> = http_json_request(
            &batch.remote_runtime_rpc,
            "GET",
            &path,
            None::<&HttpEmptyBody>,
            &[],
        )?;
        Ok(response.json.progress)
    }
}

impl StoreTransport for ClassicTeTransport {
    fn segment_name(&self) -> Result<String> {
        Ok(self.segment_name.clone())
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        let engine = self.engine.read();
        parse_rpc_server_address(&engine.rpc_server_address_text()?)
    }

    fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let engine = self.engine.read();
        Ok(engine.open_segment(segment_name)? as u64)
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        let handle = i32::try_from(handle).map_err(|_| {
            StoreError::Transport(format!("classic segment handle {handle} does not fit i32"))
        })?;
        self.engine.read().close_segment(handle)
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        let handle = i32::try_from(handle).map_err(|_| {
            StoreError::Transport(format!("classic segment handle {handle} does not fit i32"))
        })?;
        let engine = self.engine.read();
        let buffers = engine
            .segment_buffers(handle)?
            .into_iter()
            .map(|(base, length)| SegmentBuffer {
                base,
                length,
                location: self.buffer_location(base, length),
            })
            .collect::<Vec<_>>();
        Ok(SegmentInfo {
            kind: SegmentKind::Memory,
            buffers,
        })
    }

    fn republish_local_metadata(&self) -> Result<()> {
        self.rebuild_engine_for_metadata_recovery()
    }

    fn adopt_local_memory(&self, addr: *mut c_void, _size: usize, location: &str) -> Result<()> {
        self.allocations
            .lock()
            .entry(addr as usize)
            .or_insert_with(|| ClassicAllocationRecord {
                location: location.to_string(),
                size: _size,
                owner: ClassicAllocationOwner::Borrowed,
            });
        Ok(())
    }

    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        let (addr, owner) = match try_allocate_classic_numa_memory(size, location)? {
            Some((addr, owner)) => (addr, owner),
            None => {
                let alignment = default_classic_alignment();
                let mut addr = std::ptr::null_mut();
                let rc = unsafe { libc::posix_memalign(&mut addr, alignment, size.max(1)) };
                if rc != 0 {
                    return Err(StoreError::Allocator(format!(
                        "classic posix_memalign failed with rc={rc} size={size} alignment={alignment}"
                    )));
                }
                (addr, ClassicAllocationOwner::Posix)
            }
        };
        self.allocations.lock().insert(
            addr as usize,
            ClassicAllocationRecord {
                location: location.to_string(),
                size,
                owner,
            },
        );
        Ok(addr)
    }

    fn max_registration_bytes(&self) -> Option<usize> {
        self.max_registration_bytes
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        let record = self
            .allocations
            .lock()
            .remove(&(addr as usize))
            .ok_or_else(|| {
                StoreError::Allocator(format!(
                    "classic transport does not own allocation {:p}",
                    addr
                ))
            })?;
        match record.owner {
            ClassicAllocationOwner::Borrowed => Ok(()),
            ClassicAllocationOwner::Posix => {
                unsafe { libc::free(addr) };
                Ok(())
            }
            ClassicAllocationOwner::Numa => {
                classic_numa_free(addr, record.size);
                Ok(())
            }
        }
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let location = {
            let mut allocations = self.allocations.lock();
            allocations
                .entry(addr as usize)
                .or_insert_with(|| ClassicAllocationRecord {
                    location: "cpu:0".to_string(),
                    size,
                    owner: ClassicAllocationOwner::Borrowed,
                })
                .location
                .clone()
        };
        let engine = self.engine.read();
        self.register_memory_with_engine(&engine, addr, size, &location)
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let engine = self.engine.read();
        self.unregister_memory_with_engine(&engine, addr, size)
    }

    fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        self.engine.read().allocate_batch(batch_size)
    }

    fn free_batch(&self, batch_id: u64) -> Result<()> {
        self.engine.read().free_batch(batch_id)
    }

    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        self.engine.read().submit(batch_id, requests)
    }

    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        self.engine.read().task_status(batch_id, task_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        self.engine.read().batch_status(batch_id)
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct HttpEmptyBody;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HttpOpenSegmentRequest {
    segment_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HttpOpenSegmentResponse {
    segment: SegmentInfo,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HttpTransferItem {
    opcode: Opcode,
    segment_name: String,
    target_offset: u64,
    length: u64,
    body_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HttpSubmitBatchRequest {
    batch_id: u64,
    hints: TransferBatchHints,
    items: Vec<HttpTransferItem>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HttpBatchStatusResponse {
    progress: TransferProgress,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HttpSubmitBatchWireResponse {
    progress: TransferProgress,
    read_body_length: u64,
}

#[derive(Clone, Debug)]
struct HttpJsonResponse<R> {
    json: R,
    raw_body: Vec<u8>,
}

fn http_json_request<T: Serialize, R: for<'de> Deserialize<'de>>(
    address: &str,
    method: &str,
    path: &str,
    body: Option<&T>,
    raw_body: &[u8],
) -> Result<HttpJsonResponse<R>> {
    let json = match body {
        Some(body) => serde_json::to_vec(body).map_err(|error| {
            StoreError::Transport(format!("http transport failed to encode json: {error}"))
        })?,
        None => Vec::new(),
    };
    let header = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Mooncake-Body-Length: {}\r\nConnection: close\r\n\r\n",
        json.len(),
        raw_body.len()
    );
    let mut stream = TcpStream::connect(address).map_err(|error| {
        StoreError::Transport(format!(
            "http transport failed to connect {address}: {error}"
        ))
    })?;
    stream
        .set_read_timeout(Some(HTTP_TRANSPORT_READ_TIMEOUT))
        .map_err(|error| {
            StoreError::Transport(format!("http transport failed to set timeout: {error}"))
        })?;
    stream.write_all(header.as_bytes()).map_err(|error| {
        StoreError::Transport(format!(
            "http transport failed to write request header: {error}"
        ))
    })?;
    if !json.is_empty() {
        stream.write_all(&json).map_err(|error| {
            StoreError::Transport(format!(
                "http transport failed to write request body: {error}"
            ))
        })?;
    }
    if !raw_body.is_empty() {
        stream.write_all(raw_body).map_err(|error| {
            StoreError::Transport(format!(
                "http transport failed to write payload body: {error}"
            ))
        })?;
    }
    stream.flush().map_err(|error| {
        StoreError::Transport(format!("http transport failed to flush request: {error}"))
    })?;

    let mut response = Vec::new();
    stream.read_to_end(&mut response).map_err(|error| {
        StoreError::Transport(format!("http transport failed to read response: {error}"))
    })?;
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            StoreError::Transport("http transport response missing header terminator".to_string())
        })?;
    let header_text = String::from_utf8_lossy(&response[..header_end]);
    let status_line = header_text.lines().next().unwrap_or_default().to_string();
    if !status_line.contains(" 200 ") {
        return Err(StoreError::Transport(format!(
            "http transport request failed: {status_line}"
        )));
    }
    let body = &response[header_end + 4..];
    let json_length = parse_http_header_usize(&response[..header_end], "content-length");
    let raw_length = parse_http_header_usize(&response[..header_end], "x-mooncake-body-length");
    if body.len() < json_length.saturating_add(raw_length) {
        return Err(StoreError::Transport(
            "http transport response body shorter than declared lengths".to_string(),
        ));
    }
    let json = serde_json::from_slice(&body[..json_length]).map_err(|error| {
        StoreError::Transport(format!(
            "http transport failed to decode response json: {error}"
        ))
    })?;
    Ok(HttpJsonResponse {
        json,
        raw_body: body[json_length..json_length + raw_length].to_vec(),
    })
}

pub fn http_transport_label() -> &'static str {
    HTTP_TRANSPORT_LABEL
}

pub struct HttpTransportServerHandle {
    address: String,
    shutdown: Sender<()>,
    thread: Option<JoinHandle<()>>,
    state: Arc<HttpTransportServerState>,
}

struct HttpTransportServerState {
    segments: Mutex<BTreeMap<String, HttpServerSegment>>,
    batches: Mutex<BTreeMap<u64, TransferProgress>>,
    inflight_by_group: Mutex<BTreeMap<String, u64>>,
}

struct HttpServerSegment {
    base_addr: usize,
    length: usize,
}

impl HttpTransportServerHandle {
    pub fn start(bind_addr: &str) -> Result<Self> {
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            StoreError::Transport(format!(
                "http transport server failed to bind {bind_addr}: {error}"
            ))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!(
                "http transport server failed to enable nonblocking mode: {error}"
            ))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!(
                    "http transport server failed to read local address: {error}"
                ))
            })?
            .to_string();
        let state = Arc::new(HttpTransportServerState {
            segments: Mutex::new(BTreeMap::new()),
            batches: Mutex::new(BTreeMap::new()),
            inflight_by_group: Mutex::new(BTreeMap::new()),
        });
        let (shutdown, shutdown_rx) = mpsc::channel();
        let thread_state = state.clone();
        let thread = thread::Builder::new()
            .name(format!("mooncake-http-transport-{address}"))
            .spawn(move || run_http_transport_server(listener, shutdown_rx, thread_state))
            .map_err(|error| {
                StoreError::Transport(format!(
                    "http transport server failed to spawn worker thread: {error}"
                ))
            })?;
        Ok(Self {
            address,
            shutdown,
            thread: Some(thread),
            state,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn publish_segment(&self, segment_name: &str, base_addr: *mut c_void, length: usize) {
        self.state.segments.lock().insert(
            segment_name.to_string(),
            HttpServerSegment {
                base_addr: base_addr as usize,
                length,
            },
        );
    }

    pub fn shutdown(&mut self) -> Result<()> {
        let _ = self.shutdown.send(());
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| {
                StoreError::InvalidState("http transport server panicked".to_string())
            })?;
        }
        Ok(())
    }
}

impl Drop for HttpTransportServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn run_http_transport_server(
    listener: TcpListener,
    shutdown_rx: Receiver<()>,
    state: Arc<HttpTransportServerState>,
) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => handle_http_transport_connection(stream, &state),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if shutdown_rx.recv_timeout(Duration::from_millis(50)).is_ok() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn handle_http_transport_connection(mut stream: TcpStream, state: &Arc<HttpTransportServerState>) {
    if stream
        .set_read_timeout(Some(HTTP_TRANSPORT_READ_TIMEOUT))
        .is_err()
    {
        return;
    }
    let request = match read_http_transport_request(&mut stream) {
        Ok(request) => request,
        Err(_) => return,
    };
    let response = route_http_transport_request(state, request);
    let _ = stream.write_all(&response);
    let _ = stream.flush();
}

#[derive(Debug)]
struct HttpTransportRequest {
    method: String,
    path: String,
    json_body: Vec<u8>,
    raw_body: Vec<u8>,
}

fn route_http_transport_request(
    state: &Arc<HttpTransportServerState>,
    request: HttpTransportRequest,
) -> Vec<u8> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/healthz") | ("GET", "/livez") => http_transport_text_response("200 OK", "ok\n"),
        ("POST", HTTP_TRANSPORT_PATH_OPEN_SEGMENT) => {
            let payload = match serde_json::from_slice::<HttpOpenSegmentRequest>(&request.json_body)
            {
                Ok(payload) => payload,
                Err(error) => {
                    return http_transport_error_response(
                        "400 Bad Request",
                        &format!("invalid JSON body: {error}"),
                    )
                }
            };
            let segments = state.segments.lock();
            let Some(segment) = segments.get(&payload.segment_name) else {
                return http_transport_error_response("404 Not Found", "segment not found");
            };
            http_transport_json_response(
                "200 OK",
                &HttpOpenSegmentResponse {
                    segment: SegmentInfo {
                        kind: SegmentKind::Memory,
                        buffers: vec![SegmentBuffer {
                            base: segment.base_addr as u64,
                            length: segment.length as u64,
                            location: "cpu:0".to_string(),
                        }],
                    },
                },
            )
        }
        ("GET", path)
            if path.starts_with(&(HTTP_TRANSPORT_PATH_OPEN_SEGMENT.to_string() + "/")) =>
        {
            let segment_name =
                path.trim_start_matches(&(HTTP_TRANSPORT_PATH_OPEN_SEGMENT.to_string() + "/"));
            let segments = state.segments.lock();
            let Some(segment) = segments.get(segment_name) else {
                return http_transport_error_response("404 Not Found", "segment not found");
            };
            http_transport_json_response(
                "200 OK",
                &HttpOpenSegmentResponse {
                    segment: SegmentInfo {
                        kind: SegmentKind::Memory,
                        buffers: vec![SegmentBuffer {
                            base: segment.base_addr as u64,
                            length: segment.length as u64,
                            location: "cpu:0".to_string(),
                        }],
                    },
                },
            )
        }
        ("POST", HTTP_TRANSPORT_PATH_SUBMIT_BATCH) => {
            let payload = match serde_json::from_slice::<HttpSubmitBatchRequest>(&request.json_body)
            {
                Ok(payload) => payload,
                Err(error) => {
                    return http_transport_error_response(
                        "400 Bad Request",
                        &format!("invalid JSON body: {error}"),
                    )
                }
            };
            let total_bytes = payload.items.iter().map(|item| item.length).sum::<u64>();
            if let Some(limit) = payload.hints.max_inflight_bytes {
                if total_bytes > limit {
                    return http_transport_error_response(
                        "400 Bad Request",
                        &format!("batch exceeds max_inflight_bytes limit: bytes={total_bytes} limit={limit}"),
                    );
                }
            }
            if let Some(group) = payload.hints.pacing_group.as_ref() {
                let mut inflight = state.inflight_by_group.lock();
                let current = inflight.get(group).copied().unwrap_or_default();
                if let Some(limit) = payload.hints.max_inflight_bytes {
                    if current.saturating_add(total_bytes) > limit {
                        return http_transport_error_response(
                            "429 Too Many Requests",
                            &format!("inflight bytes exceed limit for group {group}"),
                        );
                    }
                }
                inflight.insert(group.clone(), current.saturating_add(total_bytes));
            }
            let result = apply_http_batch(state, &payload, &request.raw_body);
            if let Some(group) = payload.hints.pacing_group.as_ref() {
                let mut inflight = state.inflight_by_group.lock();
                let current = inflight.get(group).copied().unwrap_or_default();
                let next = current.saturating_sub(total_bytes);
                if next == 0 {
                    inflight.remove(group);
                } else {
                    inflight.insert(group.clone(), next);
                }
            }
            match result {
                Ok((progress, read_body)) => {
                    state.batches.lock().insert(payload.batch_id, progress);
                    http_transport_json_response_with_raw(
                        "200 OK",
                        &HttpSubmitBatchWireResponse {
                            progress,
                            read_body_length: read_body.len() as u64,
                        },
                        &read_body,
                    )
                }
                Err(error) => http_transport_error_response("400 Bad Request", &error.to_string()),
            }
        }
        ("GET", path)
            if path.starts_with(&(HTTP_TRANSPORT_PATH_BATCH_STATUS.to_string() + "/")) =>
        {
            let batch_id = match path
                .trim_start_matches(&(HTTP_TRANSPORT_PATH_BATCH_STATUS.to_string() + "/"))
                .parse::<u64>()
            {
                Ok(batch_id) => batch_id,
                Err(error) => {
                    return http_transport_error_response(
                        "400 Bad Request",
                        &format!("invalid batch id: {error}"),
                    )
                }
            };
            let Some(progress) = state.batches.lock().get(&batch_id).copied() else {
                return http_transport_error_response("404 Not Found", "batch not found");
            };
            http_transport_json_response("200 OK", &HttpBatchStatusResponse { progress })
        }
        _ => http_transport_error_response("404 Not Found", "not found"),
    }
}

fn apply_http_batch(
    state: &Arc<HttpTransportServerState>,
    payload: &HttpSubmitBatchRequest,
    raw_body: &[u8],
) -> Result<(TransferProgress, Vec<u8>)> {
    let segments = state.segments.lock();
    let mut transferred = 0u64;
    let mut read_body = Vec::new();
    for item in &payload.items {
        let segment = segments.get(&item.segment_name).ok_or_else(|| {
            StoreError::NotFound(format!("segment {} not found", item.segment_name))
        })?;
        let len = usize::try_from(item.length)
            .map_err(|_| StoreError::Transport("item length does not fit usize".to_string()))?;
        let target_offset = usize::try_from(item.target_offset)
            .map_err(|_| StoreError::Transport("target offset does not fit usize".to_string()))?;
        let target_end = target_offset
            .checked_add(len)
            .ok_or_else(|| StoreError::Transport("target range overflow".to_string()))?;
        if target_end > segment.length {
            return Err(StoreError::Transport(format!(
                "segment {} transfer exceeds bounds",
                item.segment_name
            )));
        }
        match item.opcode {
            Opcode::Write => {
                let start = usize::try_from(item.body_offset).map_err(|_| {
                    StoreError::Transport("body offset does not fit usize".to_string())
                })?;
                let end = start
                    .checked_add(len)
                    .ok_or_else(|| StoreError::Transport("body slice overflow".to_string()))?;
                if end > raw_body.len() {
                    return Err(StoreError::Transport(
                        "raw body is shorter than declared item payload".to_string(),
                    ));
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        raw_body[start..end].as_ptr(),
                        (segment.base_addr as *mut u8).add(target_offset),
                        len,
                    );
                }
            }
            Opcode::Read => unsafe {
                let source =
                    slice::from_raw_parts((segment.base_addr as *const u8).add(target_offset), len);
                read_body.extend_from_slice(source);
            },
        }
        transferred = transferred.saturating_add(item.length);
    }
    Ok((
        TransferProgress {
            status: TransferStatus::Completed,
            transferred_bytes: transferred,
        },
        read_body,
    ))
}

fn read_http_transport_request(stream: &mut TcpStream) -> std::io::Result<HttpTransportRequest> {
    let mut request = Vec::with_capacity(4096);
    let mut buffer = [0_u8; 1024];
    let mut header_end = None;
    let mut json_length = 0usize;
    let mut raw_length = 0usize;
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if header_end.is_none() {
            header_end = request.windows(4).position(|window| window == b"\r\n\r\n");
            if let Some(index) = header_end {
                json_length = parse_http_header_usize(&request[..index + 4], "content-length");
                raw_length =
                    parse_http_header_usize(&request[..index + 4], "x-mooncake-body-length");
                let body_len = request.len().saturating_sub(index + 4);
                if body_len >= json_length.saturating_add(raw_length) {
                    break;
                }
            }
        } else if let Some(index) = header_end {
            let body_len = request.len().saturating_sub(index + 4);
            if body_len >= json_length.saturating_add(raw_length) {
                break;
            }
        }
        if request.len() >= 8 * 1024 * 1024 {
            break;
        }
    }
    let header_end = header_end.unwrap_or(request.len());
    let header_bytes = &request[..header_end];
    let header_text = String::from_utf8_lossy(header_bytes);
    let first_line = header_text.lines().next().unwrap_or_default();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let body_start = usize::min(request.len(), header_end.saturating_add(4));
    let json_end = usize::min(request.len(), body_start.saturating_add(json_length));
    let raw_end = usize::min(request.len(), json_end.saturating_add(raw_length));
    Ok(HttpTransportRequest {
        method,
        path,
        json_body: request[body_start..json_end].to_vec(),
        raw_body: request[json_end..raw_end].to_vec(),
    })
}

fn parse_http_header_usize(headers: &[u8], key: &str) -> usize {
    let text = String::from_utf8_lossy(headers);
    text.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case(key)
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

fn http_transport_text_response(status: &str, body: &str) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nX-Mooncake-Body-Length: 0\r\nConnection: close\r\n\r\n",
        body.len(),
    )
    .into_bytes();
    response.extend_from_slice(body.as_bytes());
    response
}

fn http_transport_json_response<T: Serialize>(status: &str, body: &T) -> Vec<u8> {
    http_transport_json_response_with_raw(status, body, &[])
}

fn http_transport_json_response_with_raw<T: Serialize>(
    status: &str,
    body: &T,
    raw_body: &[u8],
) -> Vec<u8> {
    match serde_json::to_vec(body) {
        Ok(encoded) => {
            let mut response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Mooncake-Body-Length: {}\r\nConnection: close\r\n\r\n",
                encoded.len(),
                raw_body.len(),
            )
            .into_bytes();
            response.extend_from_slice(&encoded);
            response.extend_from_slice(raw_body);
            response
        }
        Err(error) => http_transport_error_response(
            "500 Internal Server Error",
            &format!("failed to serialize response: {error}"),
        ),
    }
}

#[derive(Clone, Debug, Serialize)]
struct HttpTransportErrorResponse {
    error: String,
}

fn http_transport_error_response(status: &str, message: &str) -> Vec<u8> {
    http_transport_json_response(
        status,
        &HttpTransportErrorResponse {
            error: message.to_string(),
        },
    )
}

fn default_classic_alignment() -> usize {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        4096
    } else {
        usize::try_from(page).unwrap_or(4096).max(64)
    }
}

fn try_allocate_classic_numa_memory(
    size: usize,
    location: &str,
) -> Result<Option<(*mut c_void, ClassicAllocationOwner)>> {
    let Some(node) = parse_classic_cpu_location(location) else {
        return Ok(None);
    };
    #[cfg(target_os = "linux")]
    {
        if !classic_numa_available() {
            if node == 0 {
                return Ok(None);
            }
            return Err(StoreError::Allocator(format!(
                "NUMA is unavailable; cannot allocate classic transport memory on {location}"
            )));
        }
        let node_count = unsafe { classic_linux_numa::numa_num_configured_nodes() };
        if node >= node_count {
            return Err(StoreError::Allocator(format!(
                "NUMA node {node} is out of range for location {location}"
            )));
        }
        let addr = unsafe { classic_linux_numa::numa_alloc_onnode(size.max(1), node) };
        if addr.is_null() {
            return Err(StoreError::Allocator(format!(
                "classic numa_alloc_onnode failed for location {location} size={size}"
            )));
        }
        return Ok(Some((addr, ClassicAllocationOwner::Numa)));
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = size;
        let _ = location;
        Ok(None)
    }
}

fn parse_classic_cpu_location(location: &str) -> Option<i32> {
    let node = location.strip_prefix("cpu:")?.parse::<i32>().ok()?;
    (node >= 0).then_some(node)
}

fn classic_buffer_location(
    allocations: &BTreeMap<usize, ClassicAllocationRecord>,
    base: u64,
    length: u64,
) -> String {
    let Some(start) = usize::try_from(base).ok() else {
        return "*".to_string();
    };
    let Some(len) = usize::try_from(length).ok() else {
        return "*".to_string();
    };
    let Some(end) = start.checked_add(len) else {
        return "*".to_string();
    };
    allocations
        .range(..=start)
        .next_back()
        .and_then(|(allocation_base, record)| {
            let allocation_end = allocation_base.checked_add(record.size)?;
            (end <= allocation_end).then(|| record.location.clone())
        })
        .unwrap_or_else(|| "*".to_string())
}

fn classic_numa_free(addr: *mut c_void, size: usize) {
    #[cfg(target_os = "linux")]
    unsafe {
        classic_linux_numa::numa_free(addr, size.max(1));
    }
    #[cfg(not(target_os = "linux"))]
    unsafe {
        libc::free(addr);
        let _ = size;
    }
}

#[cfg(target_os = "linux")]
fn classic_numa_available() -> bool {
    unsafe { classic_linux_numa::numa_available() >= 0 }
}

#[cfg(target_os = "linux")]
mod classic_linux_numa {
    use std::ffi::c_void;

    #[link(name = "numa")]
    extern "C" {
        pub fn numa_available() -> i32;
        pub fn numa_num_configured_nodes() -> i32;
        pub fn numa_alloc_onnode(size: usize, node: i32) -> *mut c_void;
        pub fn numa_free(mem: *mut c_void, size: usize);
    }
}

fn parse_rpc_server_address(text: &str) -> Result<(String, u16)> {
    let trimmed = text.trim();
    if let Some(inner) = trimmed.strip_prefix('[') {
        if let Some((host, suffix)) = inner.split_once(']') {
            let port = suffix
                .strip_prefix(':')
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or_default();
            return Ok((host.to_string(), port));
        }
    }
    if trimmed.matches(':').count() == 1 {
        if let Some((host, port)) = trimmed.rsplit_once(':') {
            if let Ok(port) = port.parse::<u16>() {
                return Ok((host.to_string(), port));
            }
        }
    }
    Ok((trimmed.to_string(), 0))
}

pub fn wait_for_batch_completion(
    transport: &dyn StoreTransport,
    batch_id: u64,
    timeout: Duration,
) -> Result<()> {
    wait_for_batch_completion_detailed(transport, batch_id, timeout, Instant::now() + timeout)
        .map_err(StoreError::from)
}

pub(crate) fn wait_for_batch_completion_detailed(
    transport: &dyn StoreTransport,
    batch_id: u64,
    stall_timeout: Duration,
    request_deadline: Instant,
) -> std::result::Result<(), BatchWaitError> {
    let started = Instant::now();
    let mut last_progress_at = started;
    let mut last_transferred_bytes = 0u64;
    loop {
        let progress = transport
            .overall_status(batch_id)
            .map_err(|error| BatchWaitError::poll_failed(batch_id, error))?;
        let now = Instant::now();
        if progress.transferred_bytes > last_transferred_bytes {
            last_transferred_bytes = progress.transferred_bytes;
            last_progress_at = now;
        }
        match progress.status {
            TransferStatus::Completed => return Ok(()),
            TransferStatus::Failed
            | TransferStatus::Canceled
            | TransferStatus::Invalid
            | TransferStatus::Timeout => {
                return Err(BatchWaitError::terminal_status(
                    batch_id,
                    progress.status,
                    last_transferred_bytes,
                ));
            }
            TransferStatus::Waiting | TransferStatus::Pending => {}
        }
        if now >= request_deadline {
            return Err(BatchWaitError::request_deadline_exceeded(
                batch_id,
                started.elapsed(),
                last_transferred_bytes,
            ));
        }
        if now.duration_since(last_progress_at) >= stall_timeout {
            return Err(BatchWaitError::stall_timeout(
                batch_id,
                stall_timeout,
                last_transferred_bytes,
            ));
        }
        thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::ffi::c_void;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use mooncake_store_core::{ClientLease, Result, StoreError};
    use mooncake_transport::{
        Opcode, SegmentInfo, TransferBatchHints, TransferPacingMode, TransferProgress,
        TransferRequest, TransferStatus,
    };
    use parking_lot::Mutex;

    use super::{
        classic_buffer_location, parse_rpc_server_address, registration_chunks,
        wait_for_batch_completion, wait_for_batch_completion_detailed, BatchWaitFailureKind,
        ClassicAllocationOwner, ClassicAllocationRecord, HttpStoreTransport,
        HttpTransportServerHandle, StoreTransport,
    };

    struct ScriptedTransport {
        statuses: Arc<Mutex<VecDeque<TransferProgress>>>,
    }

    impl ScriptedTransport {
        fn new(statuses: impl IntoIterator<Item = TransferProgress>) -> Self {
            Self {
                statuses: Arc::new(Mutex::new(statuses.into_iter().collect())),
            }
        }
    }

    impl StoreTransport for ScriptedTransport {
        fn segment_name(&self) -> Result<String> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn rpc_server_address(&self) -> Result<(String, u16)> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn open_segment(&self, _segment_name: &str) -> Result<u64> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn close_segment(&self, _handle: u64) -> Result<()> {
            Ok(())
        }

        fn get_segment_info(&self, _handle: u64) -> Result<SegmentInfo> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn allocate_memory(&self, _size: usize, _location: &str) -> Result<*mut c_void> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn free_memory(&self, _addr: *mut c_void) -> Result<()> {
            Ok(())
        }

        fn register_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
            Ok(())
        }

        fn unregister_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
            Ok(())
        }

        fn allocate_batch(&self, _batch_size: usize) -> Result<u64> {
            Ok(1)
        }

        fn free_batch(&self, _batch_id: u64) -> Result<()> {
            Ok(())
        }

        fn submit(&self, _batch_id: u64, _requests: &[TransferRequest]) -> Result<()> {
            Ok(())
        }

        fn submit_with_hints(
            &self,
            batch_id: u64,
            requests: &[TransferRequest],
            _hints: &TransferBatchHints,
        ) -> Result<()> {
            self.submit(batch_id, requests)
        }

        fn task_status(&self, batch_id: u64, _task_id: usize) -> Result<TransferProgress> {
            self.overall_status(batch_id)
        }

        fn overall_status(&self, _batch_id: u64) -> Result<TransferProgress> {
            let progress = self
                .statuses
                .lock()
                .pop_front()
                .unwrap_or(TransferProgress {
                    status: TransferStatus::Completed,
                    transferred_bytes: 0,
                });
            Ok(progress)
        }
    }

    #[test]
    fn wait_for_batch_completion_returns_on_completed_status() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Waiting,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            },
        ]);

        wait_for_batch_completion(&transport, 7, Duration::from_millis(20))
            .expect("completed batch should succeed");
        transport
            .adopt_local_memory(std::ptr::null_mut(), 0, "cpu:0")
            .expect("default adopt_local_memory should be a no-op");
    }

    #[test]
    fn wait_for_batch_completion_reports_terminal_failures() {
        let transport = ScriptedTransport::new([TransferProgress {
            status: TransferStatus::Failed,
            transferred_bytes: 13,
        }]);

        let error = wait_for_batch_completion(&transport, 9, Duration::from_millis(20))
            .expect_err("failed batch should surface transport error");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(error.to_string().contains("failed"));
    }

    #[test]
    fn wait_for_batch_completion_times_out_when_progress_never_completes() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Waiting,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
        ]);

        let error = wait_for_batch_completion(&transport, 11, Duration::from_millis(3))
            .expect_err("stuck batch should time out");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(
            error.to_string().contains("stalled")
                || error.to_string().contains("request deadline exceeded")
        );
    }

    #[test]
    fn wait_for_batch_completion_uses_request_deadline_when_progress_keeps_advancing() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 1,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 2,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 3,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 4,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 5,
            },
        ]);

        let error = wait_for_batch_completion_detailed(
            &transport,
            15,
            Duration::from_millis(3),
            Instant::now() + Duration::from_millis(7),
        )
        .expect_err("steady progress should consume request budget instead of tripping stall");
        assert_eq!(error.kind(), BatchWaitFailureKind::RequestDeadlineExceeded);
        assert!(!error.marks_runtime_suspect());
        let error = StoreError::from(error);
        assert!(error.to_string().contains("request deadline exceeded"));
    }

    #[test]
    fn wait_for_batch_completion_allows_steady_progress_past_stall_timeout() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 1,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 2,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 3,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 4,
            },
            TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 4,
            },
        ]);

        wait_for_batch_completion_detailed(
            &transport,
            17,
            Duration::from_millis(3),
            Instant::now() + Duration::from_millis(16),
        )
        .expect("steady progress should keep the stall watchdog from firing");
    }

    #[test]
    fn scripted_transport_exposes_contract_methods_for_smoke_coverage() {
        let transport = ScriptedTransport::new([TransferProgress {
            status: TransferStatus::Completed,
            transferred_bytes: 0,
        }]);
        assert!(matches!(
            transport.segment_name(),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            transport.rpc_server_address(),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            transport.open_segment("remote"),
            Err(StoreError::Unsupported(_))
        ));
        transport.close_segment(1).expect("close is a no-op");
        assert!(matches!(
            transport.get_segment_info(1),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            transport.allocate_memory(16, "cpu:0"),
            Err(StoreError::Unsupported(_))
        ));
        transport
            .free_memory(std::ptr::null_mut())
            .expect("free is a no-op");
        transport
            .register_memory(std::ptr::null_mut(), 0)
            .expect("register is a no-op");
        transport
            .unregister_memory(std::ptr::null_mut(), 0)
            .expect("unregister is a no-op");
        let batch_id = transport.allocate_batch(1).expect("batch should allocate");
        transport.submit(batch_id, &[]).expect("submit is a no-op");
        assert_eq!(
            transport
                .task_status(batch_id, 0)
                .expect("task status should delegate")
                .status,
            TransferStatus::Completed
        );
        transport
            .free_batch(batch_id)
            .expect("free batch is a no-op");
    }

    #[test]
    fn rpc_server_address_parser_handles_ipv4_ipv6_and_bare_hosts() {
        assert_eq!(
            parse_rpc_server_address("127.0.0.1:17111").expect("ipv4 should parse"),
            ("127.0.0.1".to_string(), 17111)
        );
        assert_eq!(
            parse_rpc_server_address("[::1]:17112").expect("ipv6 should parse"),
            ("::1".to_string(), 17112)
        );
        assert_eq!(
            parse_rpc_server_address("runtime-a").expect("bare host should parse"),
            ("runtime-a".to_string(), 0)
        );
    }

    #[test]
    fn registration_chunks_split_ranges_by_max_registration_size() {
        let chunks = registration_chunks(0x1000usize as *mut c_void, 10, Some(4))
            .expect("chunking should succeed");
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], (0x1000usize as *mut c_void, 4));
        assert_eq!(chunks[1], (0x1004usize as *mut c_void, 4));
        assert_eq!(chunks[2], (0x1008usize as *mut c_void, 2));
    }

    #[test]
    fn registration_chunks_handle_zero_length_without_overflow() {
        let chunks = registration_chunks(std::ptr::null_mut(), 0, Some(4))
            .expect("zero-length registration should succeed");
        assert_eq!(chunks, vec![(std::ptr::null_mut(), 0)]);
    }

    #[test]
    fn classic_buffer_location_resolves_owned_ranges_without_fake_cpu0() {
        let allocations = BTreeMap::from([(
            0x1000usize,
            ClassicAllocationRecord {
                location: "cpu:1".to_string(),
                size: 0x100,
                owner: ClassicAllocationOwner::Borrowed,
            },
        )]);

        assert_eq!(classic_buffer_location(&allocations, 0x1000, 0x80), "cpu:1");
        assert_eq!(classic_buffer_location(&allocations, 0x1080, 0x80), "cpu:1");
        assert_eq!(classic_buffer_location(&allocations, 0x1080, 0x81), "*");
        assert_eq!(classic_buffer_location(&allocations, 0x2000, 0x10), "*");
    }

    #[derive(Clone)]
    struct TestMetadataBackend {
        leases: Vec<ClientLease>,
        segments: Vec<mooncake_store_core::SegmentAnnouncement>,
    }

    impl mooncake_store_core::MetadataBackend for TestMetadataBackend {
        fn route_namespace(&self) -> String {
            "test".to_string()
        }

        fn upsert_client_lease(&self, _lease: &ClientLease) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn update_client_state(
            &self,
            _runtime: &mooncake_store_core::ClientRuntimeId,
            _next: mooncake_store_core::ClientLifecycleState,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
            Ok(self.leases.clone())
        }

        fn publish_segment(
            &self,
            _segment: &mooncake_store_core::SegmentAnnouncement,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn unpublish_segment(
            &self,
            _owner: &mooncake_store_core::ClientRuntimeId,
            _segment: &mooncake_store_core::SegmentName,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn list_segments(
            &self,
            _owner: Option<&mooncake_store_core::ClientRuntimeId>,
        ) -> Result<Vec<mooncake_store_core::SegmentAnnouncement>> {
            Ok(self.segments.clone())
        }

        fn update_segment_state(
            &self,
            _owner: &mooncake_store_core::ClientRuntimeId,
            _segment: &mooncake_store_core::SegmentName,
            _next: mooncake_store_core::SegmentLifecycleState,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn reserve_segment(
            &self,
            _owner: &mooncake_store_core::ClientRuntimeId,
            _segment: &mooncake_store_core::SegmentName,
            _length_bytes: u64,
        ) -> Result<mooncake_store_core::SegmentReservation> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn release_segment(
            &self,
            _owner: &mooncake_store_core::ClientRuntimeId,
            _segment: &mooncake_store_core::SegmentName,
            _offset_bytes: u64,
            _length_bytes: u64,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn get_object_route(
            &self,
            _key: &mooncake_store_core::ObjectKey,
        ) -> Result<Option<mooncake_store_core::ObjectRoute>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn list_object_routes(&self) -> Result<Vec<mooncake_store_core::ObjectRoute>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn compare_and_swap_object_route(
            &self,
            _key: &mooncake_store_core::ObjectKey,
            _expected: Option<mooncake_store_core::RouteVersion>,
            _next: Option<&mooncake_store_core::ObjectRoute>,
        ) -> Result<mooncake_store_core::CasResult> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn get_route_policy(
            &self,
            _domain: &mooncake_store_core::RoutePolicyDomain,
        ) -> Result<Option<mooncake_store_core::RoutePolicy>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn put_route_policy_if_absent(
            &self,
            _domain: &mooncake_store_core::RoutePolicyDomain,
            _policy: &mooncake_store_core::RoutePolicy,
        ) -> Result<bool> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn put_route_policy(
            &self,
            _domain: &mooncake_store_core::RoutePolicyDomain,
            _policy: &mooncake_store_core::RoutePolicy,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn delete_route_policy(
            &self,
            _domain: &mooncake_store_core::RoutePolicyDomain,
        ) -> Result<bool> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn list_route_policies(
            &self,
        ) -> Result<
            Vec<(
                mooncake_store_core::RoutePolicyDomain,
                mooncake_store_core::RoutePolicy,
            )>,
        > {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn get_tenant_policy(
            &self,
            _scope: &mooncake_store_core::TenantPolicyScope,
        ) -> Result<Option<mooncake_store_core::TenantPolicy>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn list_tenant_policies(&self) -> Result<Vec<mooncake_store_core::TenantPolicy>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn put_tenant_policy(
            &self,
            _policy: &mooncake_store_core::TenantPolicy,
            _expected_version: Option<u64>,
        ) -> Result<mooncake_store_core::TenantPolicy> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn delete_tenant_policy(
            &self,
            _scope: &mooncake_store_core::TenantPolicyScope,
            _expected_version: Option<u64>,
        ) -> Result<bool> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn get_tenant_quota_state(
            &self,
            _scope: &mooncake_store_core::TenantPolicyScope,
        ) -> Result<Option<mooncake_store_core::TenantQuotaState>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn get_tenant_object_accounting(
            &self,
            _key: &mooncake_store_core::ObjectKey,
        ) -> Result<Option<mooncake_store_core::TenantObjectAccounting>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn list_tenant_quota_reservations(
            &self,
            _scope: &mooncake_store_core::TenantPolicyScope,
        ) -> Result<Vec<mooncake_store_core::TenantQuotaReservation>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn reserve_tenant_quota(
            &self,
            _request: &mooncake_store_core::TenantQuotaReservationRequest,
        ) -> Result<mooncake_store_core::TenantQuotaReservationOutcome> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn finalize_tenant_quota(
            &self,
            _request: &mooncake_store_core::TenantQuotaFinalizeRequest,
        ) -> Result<mooncake_store_core::TenantQuotaFinalizeOutcome> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn abort_tenant_quota(
            &self,
            _reservation_id: &str,
        ) -> Result<mooncake_store_core::TenantQuotaAbortOutcome> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn put_handoff(&self, _handoff: &mooncake_store_core::HandoffPlan) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn get_handoff(
            &self,
            _stable_id: &mooncake_store_core::ClientStableId,
        ) -> Result<Option<mooncake_store_core::HandoffPlan>> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }
    }

    #[test]
    fn http_transport_moves_write_and_read_payloads_over_http() {
        let mut remote_segment = vec![0u8; 64];
        let server = HttpTransportServerHandle::start("127.0.0.1:0")
            .expect("http transport server should start");
        server.publish_segment(
            "remote-segment",
            remote_segment.as_mut_ptr().cast::<c_void>(),
            remote_segment.len(),
        );

        let runtime = mooncake_store_core::ClientRuntimeId::new(
            "remote-runtime",
            mooncake_store_core::ClientEpoch(1),
        );
        let mut endpoints = mooncake_store_core::ClientEndpointSet::default();
        endpoints.labels.insert(
            super::http_transport_label().to_string(),
            server.address().to_string(),
        );
        let metadata = Arc::new(TestMetadataBackend {
            leases: vec![ClientLease {
                runtime: runtime.clone(),
                state: mooncake_store_core::ClientLifecycleState::Active,
                compatibility: mooncake_store_core::CompatibilityDescriptor::default(),
                endpoints,
                expires_at_ms: u64::MAX,
            }],
            segments: vec![mooncake_store_core::SegmentAnnouncement {
                owner: runtime,
                segment_name: mooncake_store_core::SegmentName::new("remote-segment"),
                capacity_bytes: remote_segment.len() as u64,
                used_bytes: 0,
                state: mooncake_store_core::SegmentLifecycleState::Active,
                alignment_bytes: 1,
                tags: Vec::new(),
            }],
        });

        let transport = HttpStoreTransport::new(metadata, "local-segment", "127.0.0.1:17000")
            .expect("http transport should build");
        let handle = transport
            .open_segment("remote-segment")
            .expect("open_segment should resolve remote runtime");

        let write_batch = transport
            .allocate_batch(1)
            .expect("write batch should allocate");
        let write_payload = b"hello-http-read";
        let write_request = TransferRequest {
            opcode: Opcode::Write,
            source: write_payload.as_ptr().cast::<c_void>() as *mut c_void,
            target_id: handle,
            target_offset: 5,
            length: write_payload.len() as u64,
        };
        transport
            .submit_with_hints(
                write_batch,
                &[write_request],
                &TransferBatchHints {
                    pacing_group: Some("tenant/default".to_string()),
                    mode: TransferPacingMode::LatencySensitive,
                    max_inflight_bytes: Some(1024),
                },
            )
            .expect("http write should succeed");
        assert_eq!(&remote_segment[5..5 + write_payload.len()], write_payload);

        let read_batch = transport
            .allocate_batch(1)
            .expect("read batch should allocate");
        let mut read_buffer = vec![0u8; write_payload.len()];
        let read_request = TransferRequest {
            opcode: Opcode::Read,
            source: read_buffer.as_mut_ptr().cast::<c_void>(),
            target_id: handle,
            target_offset: 5,
            length: write_payload.len() as u64,
        };
        remote_segment[5..5 + write_payload.len()].copy_from_slice(write_payload);
        transport
            .submit_with_hints(
                read_batch,
                &[read_request],
                &TransferBatchHints {
                    pacing_group: Some("tenant/default".to_string()),
                    mode: TransferPacingMode::ThroughputOptimized,
                    max_inflight_bytes: Some(1024),
                },
            )
            .expect("http read should succeed");
        assert_eq!(read_buffer, write_payload);
    }
}
