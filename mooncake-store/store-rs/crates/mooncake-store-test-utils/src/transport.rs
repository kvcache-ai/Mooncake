use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::{Result, StoreError};
use mooncake_store_transport_core::{StoreTransport, StoreTransportFactory};
use mooncake_transport::{
    Opcode, SegmentBuffer, SegmentInfo, SegmentKind, TransferBatchHints, TransferPacingMode,
    TransferProgress, TransferRequest, TransferStatus,
};
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Internal segment and state types
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TestSegment {
    pub base: usize,
    pub len: usize,
    pub buffers: Vec<TestSegmentBuffer>,
}

#[derive(Clone, Copy)]
pub struct TestSegmentBuffer {
    pub base: usize,
    pub len: usize,
}

impl TestSegment {
    fn new(base: usize, len: usize) -> Self {
        Self {
            base,
            len,
            buffers: vec![TestSegmentBuffer { base, len }],
        }
    }

    fn add_buffer(&mut self, base: usize, len: usize) {
        if !self.buffers.iter().any(|buffer| buffer.base == base) {
            self.buffers.push(TestSegmentBuffer { base, len });
        }
    }

    fn remove_buffer(&mut self, base: usize) -> bool {
        let Some(index) = self.buffers.iter().position(|buffer| buffer.base == base) else {
            return false;
        };
        self.buffers.remove(index);
        if let Some(buffer) = self.buffers.first().copied() {
            self.base = buffer.base;
            self.len = buffer.len;
        }
        true
    }
}

pub struct TestTransportState {
    pub next_handle: u64,
    pub next_batch: u64,
    pub allocations: BTreeMap<usize, Box<[u8]>>,
    pub segments_by_name: BTreeMap<String, u64>,
    pub segments_by_handle: BTreeMap<u64, TestSegment>,
    pub live_batches: BTreeSet<u64>,
    pub registered_memory: BTreeMap<usize, usize>,
    pub republish_local_metadata_calls: usize,
    pub fail_next_submit_segments: BTreeSet<String>,
    pub max_registration_bytes: Option<usize>,
    pub supports_parallel_startup_registration: bool,
    pub submitted_batch_sizes: Vec<usize>,
    pub submitted_batch_bytes: Vec<u64>,
    pub submitted_batch_hints: Vec<(Option<String>, TransferPacingMode, Option<u64>)>,
    pub submitted_request_sources: Vec<Vec<usize>>,
    pub submitted_request_opcodes: Vec<Vec<Opcode>>,
    pub local_segment_descriptor: Option<String>,
    pub local_segment_descriptors: BTreeMap<String, String>,
    pub cached_segment_descriptors: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// TestTransport — in-process, zero-copy transport backed by boxed memory
// ---------------------------------------------------------------------------

pub struct TestTransport {
    local_segment: String,
    pub state: Arc<Mutex<TestTransportState>>,
}

pub struct TestTransportFactory {
    pub state: Arc<Mutex<TestTransportState>>,
}

impl TestTransport {
    pub fn new(local_segment: &str) -> Self {
        Self {
            local_segment: local_segment.to_string(),
            state: Arc::new(Mutex::new(TestTransportState {
                next_handle: 1,
                next_batch: 1,
                allocations: BTreeMap::new(),
                segments_by_name: BTreeMap::new(),
                segments_by_handle: BTreeMap::new(),
                live_batches: BTreeSet::new(),
                registered_memory: BTreeMap::new(),
                republish_local_metadata_calls: 0,
                fail_next_submit_segments: BTreeSet::new(),
                max_registration_bytes: None,
                supports_parallel_startup_registration: false,
                submitted_batch_sizes: Vec::new(),
                submitted_batch_bytes: Vec::new(),
                submitted_batch_hints: Vec::new(),
                submitted_request_sources: Vec::new(),
                submitted_request_opcodes: Vec::new(),
                local_segment_descriptor: None,
                local_segment_descriptors: BTreeMap::new(),
                cached_segment_descriptors: Vec::new(),
            })),
        }
    }

    pub fn peer(&self, local_segment: &str) -> Self {
        Self {
            local_segment: local_segment.to_string(),
            state: self.state.clone(),
        }
    }

    pub fn factory(&self) -> Arc<dyn StoreTransportFactory> {
        Arc::new(TestTransportFactory {
            state: self.state.clone(),
        })
    }

    pub fn set_max_registration_bytes(&self, value: Option<usize>) {
        self.state.lock().max_registration_bytes = value;
    }

    pub fn set_supports_parallel_startup_registration(&self, value: bool) {
        self.state.lock().supports_parallel_startup_registration = value;
    }

    pub fn add_external_segment(&self, segment_name: &str, size: usize) -> u64 {
        let mut state = self.state.lock();
        let base = allocate_boxed_region(&mut state, size);
        register_segment(&mut state, segment_name.to_string(), base, size)
    }

    pub fn segment_bounds(&self, segment_name: &str) -> Option<(u64, u64)> {
        let state = self.state.lock();
        let handle = state.segments_by_name.get(segment_name)?;
        let segment = state.segments_by_handle.get(handle)?;
        Some((segment.base as u64, segment.len as u64))
    }

    pub fn restart_external_segment(&self, segment_name: &str) -> u64 {
        let mut state = self.state.lock();
        let handle = state
            .segments_by_name
            .remove(segment_name)
            .expect("segment should exist before restart");
        let segment = state
            .segments_by_handle
            .remove(&handle)
            .expect("segment handle should exist before restart");
        let next_handle = state.next_handle;
        state.next_handle += 1;
        state
            .segments_by_name
            .insert(segment_name.to_string(), next_handle);
        state.segments_by_handle.insert(next_handle, segment);
        next_handle
    }

    pub fn fail_next_submit_for_segment(&self, segment_name: &str) {
        self.state
            .lock()
            .fail_next_submit_segments
            .insert(segment_name.to_string());
    }

    pub fn submitted_batch_sizes(&self) -> Vec<usize> {
        self.state.lock().submitted_batch_sizes.clone()
    }

    pub fn submitted_batch_bytes(&self) -> Vec<u64> {
        self.state.lock().submitted_batch_bytes.clone()
    }

    pub fn submitted_batch_hints(&self) -> Vec<(Option<String>, TransferPacingMode, Option<u64>)> {
        self.state.lock().submitted_batch_hints.clone()
    }

    pub fn submitted_request_sources(&self) -> Vec<Vec<usize>> {
        self.state.lock().submitted_request_sources.clone()
    }

    pub fn submitted_request_opcodes(&self) -> Vec<Vec<Opcode>> {
        self.state.lock().submitted_request_opcodes.clone()
    }

    pub fn republish_local_metadata_calls(&self) -> usize {
        self.state.lock().republish_local_metadata_calls
    }

    pub fn set_local_segment_descriptor(&self, descriptor: impl Into<String>) {
        self.state.lock().local_segment_descriptor = Some(descriptor.into());
    }

    pub fn set_local_segment_descriptor_for_segment(
        &self,
        segment_name: impl Into<String>,
        descriptor: impl Into<String>,
    ) {
        self.state
            .lock()
            .local_segment_descriptors
            .insert(segment_name.into(), descriptor.into());
    }

    pub fn cached_segment_descriptors(&self) -> Vec<(String, String)> {
        self.state.lock().cached_segment_descriptors.clone()
    }
}

impl StoreTransportFactory for TestTransportFactory {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>> {
        Ok(Arc::new(TestTransport {
            local_segment: segment_name.to_string(),
            state: self.state.clone(),
        }))
    }
}

impl StoreTransport for TestTransport {
    fn segment_name(&self) -> Result<String> {
        Ok(self.local_segment.clone())
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        Ok(("127.0.0.1".to_string(), 0))
    }

    fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let state = self.state.lock();
        state
            .segments_by_name
            .get(segment_name)
            .copied()
            .ok_or_else(|| StoreError::NotFound(format!("segment {segment_name} not found")))
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        let state = self.state.lock();
        if state.segments_by_handle.contains_key(&handle) {
            return Ok(());
        }
        Err(StoreError::NotFound(format!(
            "segment handle {handle} not found"
        )))
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        let state = self.state.lock();
        let segment = state
            .segments_by_handle
            .get(&handle)
            .ok_or_else(|| StoreError::NotFound(format!("segment handle {handle} not found")))?;
        Ok(SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: segment
                .buffers
                .iter()
                .map(|buffer| SegmentBuffer {
                    base: buffer.base as u64,
                    length: buffer.len as u64,
                    location: "cpu:0".to_string(),
                })
                .collect(),
        })
    }

    fn adopt_local_memory(&self, addr: *mut c_void, size: usize, _location: &str) -> Result<()> {
        let mut state = self.state.lock();
        let segment_name = self.local_segment.clone();
        if let Some(handle) = state.segments_by_name.get(&segment_name).copied() {
            if let Some(segment) = state.segments_by_handle.get_mut(&handle) {
                segment.add_buffer(addr as usize, size);
            }
            return Ok(());
        }
        register_segment(&mut state, segment_name, addr as usize, size);
        Ok(())
    }

    fn allocate_memory(&self, size: usize, _location: &str) -> Result<*mut c_void> {
        let mut state = self.state.lock();
        let base = allocate_boxed_region(&mut state, size);
        if let Some(handle) = state.segments_by_name.get(&self.local_segment).copied() {
            if let Some(segment) = state.segments_by_handle.get_mut(&handle) {
                segment.add_buffer(base, size);
            }
        } else {
            register_segment(&mut state, self.local_segment.clone(), base, size);
        }
        Ok(base as *mut c_void)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        let mut state = self.state.lock();
        let key = addr as usize;
        if state.allocations.remove(&key).is_none() {
            return Err(StoreError::NotFound(format!(
                "allocation {:p} not found",
                addr
            )));
        }
        remove_segment_buffer(&mut state, key);
        state.registered_memory.remove(&key);
        Ok(())
    }

    fn max_registration_bytes(&self) -> Option<usize> {
        self.state.lock().max_registration_bytes
    }

    fn supports_parallel_startup_registration(&self) -> bool {
        self.state.lock().supports_parallel_startup_registration
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        self.state
            .lock()
            .registered_memory
            .insert(addr as usize, size);
        Ok(())
    }

    fn republish_local_metadata(&self) -> Result<()> {
        self.state.lock().republish_local_metadata_calls += 1;
        Ok(())
    }

    fn local_segment_descriptor(&self) -> Result<Option<String>> {
        let state = self.state.lock();
        Ok(state
            .local_segment_descriptors
            .get(&self.local_segment)
            .cloned()
            .or_else(|| state.local_segment_descriptor.clone()))
    }

    fn cache_remote_segment_descriptor(
        &self,
        segment_name: &str,
        descriptor_json: &str,
    ) -> Result<()> {
        self.state
            .lock()
            .cached_segment_descriptors
            .push((segment_name.to_string(), descriptor_json.to_string()));
        Ok(())
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let mut state = self.state.lock();
        let key = addr as usize;
        let registered = match state.registered_memory.get(&key) {
            Some(&s) => s,
            None => {
                return Err(StoreError::NotFound(format!(
                    "registered allocation {:p} not found",
                    addr
                )))
            }
        };
        if registered != size {
            return Err(StoreError::Allocator(format!(
                "registered size mismatch: expected={registered} got={size}"
            )));
        }
        state.registered_memory.remove(&key);
        remove_segment_buffer(&mut state, key);
        Ok(())
    }

    fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        if batch_size == 0 {
            return Err(StoreError::Transport(
                "batch_size must be greater than zero".to_string(),
            ));
        }
        let mut state = self.state.lock();
        let batch_id = state.next_batch;
        state.next_batch += 1;
        state.live_batches.insert(batch_id);
        Ok(batch_id)
    }

    fn free_batch(&self, batch_id: u64) -> Result<()> {
        if self.state.lock().live_batches.remove(&batch_id) {
            return Ok(());
        }
        Err(StoreError::NotFound(format!("batch {batch_id} not found")))
    }

    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        let hints = TransferBatchHints::default();
        self.submit_with_hints(batch_id, requests, &hints)
    }

    fn submit_with_hints(
        &self,
        batch_id: u64,
        requests: &[TransferRequest],
        hints: &TransferBatchHints,
    ) -> Result<()> {
        let mut state = self.state.lock();
        if !state.live_batches.contains(&batch_id) {
            return Err(StoreError::NotFound(format!("batch {batch_id} not found")));
        }

        let mut failed_segment = None;
        for request in requests {
            let segment_name = state
                .segments_by_name
                .iter()
                .find_map(|(name, handle)| (*handle == request.target_id).then_some(name.clone()));
            if let Some(segment_name) = segment_name {
                if state.fail_next_submit_segments.remove(&segment_name) {
                    failed_segment = Some(segment_name);
                    break;
                }
            }
        }
        if let Some(segment_name) = failed_segment {
            return Err(StoreError::Transport(format!(
                "injected submit failure for segment {segment_name}"
            )));
        }

        state.submitted_batch_sizes.push(requests.len());
        state
            .submitted_batch_bytes
            .push(requests.iter().map(|request| request.length).sum());
        state.submitted_batch_hints.push((
            hints.pacing_group.clone(),
            hints.mode,
            hints.max_inflight_bytes,
        ));
        state.submitted_request_sources.push(
            requests
                .iter()
                .map(|request| request.source as usize)
                .collect(),
        );
        state
            .submitted_request_opcodes
            .push(requests.iter().map(|request| request.opcode).collect());

        for request in requests {
            let segment = state
                .segments_by_handle
                .get(&request.target_id)
                .ok_or_else(|| {
                    StoreError::NotFound(format!("segment handle {} not found", request.target_id))
                })?;
            validate_request_bounds(segment, request)?;
            unsafe {
                match request.opcode {
                    Opcode::Write => ptr::copy_nonoverlapping(
                        request.source.cast::<u8>(),
                        request.target_offset as *mut u8,
                        request.length as usize,
                    ),
                    Opcode::Read => ptr::copy_nonoverlapping(
                        request.target_offset as *const u8,
                        request.source.cast::<u8>(),
                        request.length as usize,
                    ),
                }
            }
        }
        Ok(())
    }

    fn task_status(&self, batch_id: u64, _task_id: usize) -> Result<TransferProgress> {
        self.overall_status(batch_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        if self.state.lock().live_batches.contains(&batch_id) {
            return Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            });
        }
        Err(StoreError::NotFound(format!("batch {batch_id} not found")))
    }
}

// ---------------------------------------------------------------------------
// Memory management helpers
// ---------------------------------------------------------------------------

pub fn allocate_boxed_region(state: &mut TestTransportState, size: usize) -> usize {
    let mut memory = vec![0u8; size].into_boxed_slice();
    let base = memory.as_mut_ptr() as usize;
    state.allocations.insert(base, memory);
    base
}

pub fn register_segment(
    state: &mut TestTransportState,
    segment_name: String,
    base: usize,
    size: usize,
) -> u64 {
    let handle = state.next_handle;
    state.next_handle += 1;
    state.segments_by_name.insert(segment_name, handle);
    state
        .segments_by_handle
        .insert(handle, TestSegment::new(base, size));
    handle
}

fn remove_segment_buffer(state: &mut TestTransportState, base: usize) {
    let empty_handles = state
        .segments_by_handle
        .iter_mut()
        .filter_map(|(handle, segment)| {
            (segment.remove_buffer(base) && segment.buffers.is_empty()).then_some(*handle)
        })
        .collect::<Vec<_>>();
    for handle in empty_handles {
        state.segments_by_handle.remove(&handle);
        state
            .segments_by_name
            .retain(|_, current_handle| *current_handle != handle);
    }
}

fn validate_request_bounds(segment: &TestSegment, request: &TransferRequest) -> Result<()> {
    let start = usize::try_from(request.target_offset)
        .map_err(|_| StoreError::Transport("target offset does not fit usize".to_string()))?;
    let end = start
        .checked_add(request.length as usize)
        .ok_or_else(|| StoreError::Transport("request length overflow".to_string()))?;
    if segment.buffers.iter().any(|buffer| {
        buffer
            .base
            .checked_add(buffer.len)
            .is_some_and(|buffer_end| start >= buffer.base && end <= buffer_end)
    }) {
        return Ok(());
    }
    Err(StoreError::Transport(format!(
        "request out of segment bounds: start={start} end={end} buffers={:?}",
        segment
            .buffers
            .iter()
            .map(|buffer| (buffer.base, buffer.len))
            .collect::<Vec<_>>()
    )))
}

// ---------------------------------------------------------------------------
// FaultyTransport — wraps TestTransport with injectable faults and latency
// ---------------------------------------------------------------------------

/// Controls fault injection behaviour for FaultyTransport.
///
/// All fields are atomically accessible so tests can mutate them from any
/// thread without holding a lock on the transport itself.
pub struct FaultConfig {
    pub disconnected: AtomicBool,
    pub submit_failures_remaining: AtomicU64,
    pub open_failures_remaining: AtomicU64,
    pub submit_latency: parking_lot::Mutex<Duration>,
    pub open_latency: parking_lot::Mutex<Duration>,
    /// Deterministic jitter amplitude in milliseconds (0 = disabled).
    pub jitter_ms: AtomicU64,
    pub submit_call_count: AtomicU64,
    pub open_call_count: AtomicU64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            disconnected: AtomicBool::new(false),
            submit_failures_remaining: AtomicU64::new(0),
            open_failures_remaining: AtomicU64::new(0),
            submit_latency: parking_lot::Mutex::new(Duration::ZERO),
            open_latency: parking_lot::Mutex::new(Duration::ZERO),
            jitter_ms: AtomicU64::new(0),
            submit_call_count: AtomicU64::new(0),
            open_call_count: AtomicU64::new(0),
        }
    }
}

impl FaultConfig {
    pub fn set_submit_latency(&self, d: Duration) {
        *self.submit_latency.lock() = d;
    }

    pub fn set_open_latency(&self, d: Duration) {
        *self.open_latency.lock() = d;
    }

    pub fn fail_next_submits(&self, n: u64) {
        self.submit_failures_remaining.store(n, Ordering::SeqCst);
    }

    pub fn fail_next_opens(&self, n: u64) {
        self.open_failures_remaining.store(n, Ordering::SeqCst);
    }

    pub fn disconnect(&self) {
        self.disconnected.store(true, Ordering::SeqCst);
    }

    pub fn reconnect(&self) {
        self.disconnected.store(false, Ordering::SeqCst);
    }

    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::SeqCst)
    }

    pub fn set_jitter_ms(&self, ms: u64) {
        self.jitter_ms.store(ms, Ordering::SeqCst);
    }

    pub fn reset_all(&self) {
        self.disconnected.store(false, Ordering::SeqCst);
        self.submit_failures_remaining.store(0, Ordering::SeqCst);
        self.open_failures_remaining.store(0, Ordering::SeqCst);
        *self.submit_latency.lock() = Duration::ZERO;
        *self.open_latency.lock() = Duration::ZERO;
        self.jitter_ms.store(0, Ordering::SeqCst);
        self.submit_call_count.store(0, Ordering::SeqCst);
        self.open_call_count.store(0, Ordering::SeqCst);
    }

    pub fn reset_counters(&self) {
        self.submit_call_count.store(0, Ordering::SeqCst);
        self.open_call_count.store(0, Ordering::SeqCst);
    }

    /// Deterministic delay: delay_ms = (call_count * 7 + 13) % jitter_ms.
    fn apply_jitter(&self, call_count: u64) -> Duration {
        let jitter = self.jitter_ms.load(Ordering::SeqCst);
        if jitter == 0 {
            return Duration::ZERO;
        }
        let delay_ms = (call_count.wrapping_mul(7).wrapping_add(13)) % jitter;
        Duration::from_millis(delay_ms)
    }

    fn should_fail_submit(&self) -> bool {
        if self.disconnected.load(Ordering::SeqCst) {
            return true;
        }
        loop {
            let remaining = self.submit_failures_remaining.load(Ordering::SeqCst);
            if remaining == 0 {
                return false;
            }
            if self
                .submit_failures_remaining
                .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn should_fail_open(&self) -> bool {
        if self.disconnected.load(Ordering::SeqCst) {
            return true;
        }
        loop {
            let remaining = self.open_failures_remaining.load(Ordering::SeqCst);
            if remaining == 0 {
                return false;
            }
            if self
                .open_failures_remaining
                .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
        }
    }
}

/// Wraps a `TestTransport` with fault injection controlled by a shared `FaultConfig`.
pub struct FaultyTransport {
    inner: Arc<TestTransport>,
    pub faults: Arc<FaultConfig>,
}

impl FaultyTransport {
    /// Create a new `FaultyTransport` wrapping `inner`.
    ///
    /// Returns the transport and the shared `FaultConfig` handle.  Tests hold
    /// the config handle to inject faults after construction.
    pub fn new(inner: Arc<TestTransport>) -> (Self, Arc<FaultConfig>) {
        let faults = Arc::new(FaultConfig::default());
        let transport = Self {
            inner,
            faults: faults.clone(),
        };
        (transport, faults)
    }

    pub fn faults(&self) -> Arc<FaultConfig> {
        self.faults.clone()
    }
}

impl StoreTransport for FaultyTransport {
    fn segment_name(&self) -> Result<String> {
        self.inner.segment_name()
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        self.inner.rpc_server_address()
    }

    fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let call_count = self.faults.open_call_count.fetch_add(1, Ordering::SeqCst);
        let jitter = self.faults.apply_jitter(call_count);
        if !jitter.is_zero() {
            std::thread::sleep(jitter);
        }
        let latency = *self.faults.open_latency.lock();
        if !latency.is_zero() {
            std::thread::sleep(latency);
        }
        if self.faults.should_fail_open() {
            return Err(StoreError::Transport(
                "faulty transport: injected open failure".to_string(),
            ));
        }
        self.inner.open_segment(segment_name)
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        self.inner.close_segment(handle)
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        self.inner.get_segment_info(handle)
    }

    fn adopt_local_memory(&self, addr: *mut c_void, size: usize, location: &str) -> Result<()> {
        self.inner.adopt_local_memory(addr, size, location)
    }

    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        self.inner.allocate_memory(size, location)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        self.inner.free_memory(addr)
    }

    fn max_registration_bytes(&self) -> Option<usize> {
        self.inner.max_registration_bytes()
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        self.inner.register_memory(addr, size)
    }

    fn republish_local_metadata(&self) -> Result<()> {
        self.inner.republish_local_metadata()
    }

    fn local_segment_descriptor(&self) -> Result<Option<String>> {
        self.inner.local_segment_descriptor()
    }

    fn cache_remote_segment_descriptor(
        &self,
        segment_name: &str,
        descriptor_json: &str,
    ) -> Result<()> {
        self.inner
            .cache_remote_segment_descriptor(segment_name, descriptor_json)
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        self.inner.unregister_memory(addr, size)
    }

    fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        self.inner.allocate_batch(batch_size)
    }

    fn free_batch(&self, batch_id: u64) -> Result<()> {
        self.inner.free_batch(batch_id)
    }

    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        let hints = TransferBatchHints::default();
        self.submit_with_hints(batch_id, requests, &hints)
    }

    fn submit_with_hints(
        &self,
        batch_id: u64,
        requests: &[TransferRequest],
        hints: &TransferBatchHints,
    ) -> Result<()> {
        let call_count = self.faults.submit_call_count.fetch_add(1, Ordering::SeqCst);
        let jitter = self.faults.apply_jitter(call_count);
        if !jitter.is_zero() {
            std::thread::sleep(jitter);
        }
        let latency = *self.faults.submit_latency.lock();
        if !latency.is_zero() {
            std::thread::sleep(latency);
        }
        if self.faults.should_fail_submit() {
            return Err(StoreError::Transport(
                "faulty transport: injected submit failure".to_string(),
            ));
        }
        self.inner.submit_with_hints(batch_id, requests, hints)
    }

    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        if self.faults.disconnected.load(Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "faulty transport: disconnected".to_string(),
            ));
        }
        self.inner.task_status(batch_id, task_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        if self.faults.disconnected.load(Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "faulty transport: disconnected".to_string(),
            ));
        }
        self.inner.overall_status(batch_id)
    }
}
