use std::collections::BTreeMap;
use std::ffi::c_void;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport::{
    ClassicEngineConfig, ClassicTransferEngine, SegmentBuffer, SegmentInfo, SegmentKind,
    TentEngine, TentEngineConfig, TransferBatchHints, TransferProgress, TransferRequest,
    TransferStatus,
};
use parking_lot::{Mutex, RwLock};

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

    use mooncake_store_core::{Result, StoreError};
    use mooncake_transport::{
        SegmentInfo, TransferBatchHints, TransferPacingMode, TransferProgress, TransferRequest,
        TransferStatus,
    };
    use parking_lot::Mutex;

    use super::{
        classic_buffer_location, parse_rpc_server_address, registration_chunks,
        wait_for_batch_completion, wait_for_batch_completion_detailed, BatchWaitFailureKind,
        ClassicAllocationOwner, ClassicAllocationRecord, StoreTransport,
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
            hints: &TransferBatchHints,
        ) -> Result<()> {
            assert_eq!(hints.mode, TransferPacingMode::Standard);
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
}
