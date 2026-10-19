// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Safe, zero-copy wrapper around the Transfer Engine C API.

use std::collections::HashMap;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::error::EngineError;
use crate::ffi;
use crate::types::{
    BatchId, BufferEntry, NicLoadStat, NotifyMsg, Opcode, SegmentId, TransferRequest,
    TransferStatus, TransferStatusCode, INVALID_BATCH, WILDCARD_LOCATION,
};

/// Options for [`TransferEngine::create`].
#[derive(Debug, Clone)]
pub struct TransferEngineOptions<'a> {
    pub metadata_uri: &'a str,
    pub local_server_name: &'a str,
    pub ip_or_host_name: &'a str,
    pub rpc_port: u64,
    pub auto_discover: bool,
}

impl<'a> TransferEngineOptions<'a> {
    pub fn new(metadata_uri: &'a str, local_server_name: &'a str) -> Self {
        Self {
            metadata_uri,
            local_server_name,
            ip_or_host_name: local_server_name,
            rpc_port: 0,
            auto_discover: false,
        }
    }

    pub fn ip_or_host_name(mut self, value: &'a str) -> Self {
        self.ip_or_host_name = value;
        self
    }

    pub fn rpc_port(mut self, value: u64) -> Self {
        self.rpc_port = value;
        self
    }

    pub fn auto_discover(mut self, value: bool) -> Self {
        self.auto_discover = value;
        self
    }
}

/// Handle to a Transfer Engine instance.
///
/// The underlying C object is internally synchronized, so the handle is
/// `Send + Sync`. Registered memory must remain valid until unregistered.
///
/// Hot-path methods (`submit_transfer`, `get_transfer_status`, `wait_all`)
/// do not allocate.
pub struct TransferEngine {
    engine: ffi::transfer_engine_t,
    segments: Mutex<HashMap<String, SegmentId>>,
}

// The C++ Transfer Engine serializes metadata and completion internally.
unsafe impl Send for TransferEngine {}
unsafe impl Sync for TransferEngine {}

impl TransferEngine {
    /// Create an engine with `auto_discover` disabled. `local_server_name` is
    /// also used as `ip_or_host_name`.
    pub fn new(
        metadata_uri: &str,
        local_server_name: &str,
        rpc_port: u64,
    ) -> Result<Self, EngineError> {
        Self::create(TransferEngineOptions::new(metadata_uri, local_server_name).rpc_port(rpc_port))
    }

    /// Create an engine from explicit options.
    pub fn create(opts: TransferEngineOptions<'_>) -> Result<Self, EngineError> {
        let metadata_uri_c = CString::new(opts.metadata_uri)?;
        let local_server_name_c = CString::new(opts.local_server_name)?;
        let ip_or_host_name_c = CString::new(opts.ip_or_host_name)?;
        let engine = unsafe {
            ffi::createTransferEngine(
                metadata_uri_c.as_ptr(),
                local_server_name_c.as_ptr(),
                ip_or_host_name_c.as_ptr(),
                opts.rpc_port,
                i32::from(opts.auto_discover),
            )
        };
        if engine.is_null() {
            return Err(EngineError::NullHandle);
        }
        Ok(Self {
            engine,
            segments: Mutex::new(HashMap::new()),
        })
    }

    /// Python-style constructor: create the engine and install `protocol`.
    ///
    /// `local_hostname` may be `"host:port"`. For `"rdma"`, topology
    /// auto-discovery is enabled and no extra transport is installed. For
    /// `"tcp"` / `"efa"` / `"cxi"`, the named transport is installed
    /// explicitly. `device_name` is accepted for API compatibility; set
    /// `MC_TE_FILTERS` to restrict NICs (the C ABI has no device-name argument).
    pub fn initialize(
        local_hostname: &str,
        metadata_server: &str,
        protocol: &str,
        _device_name: &str,
    ) -> Result<Self, EngineError> {
        let (host, port) = parse_host_port(local_hostname);
        let auto = protocol.eq_ignore_ascii_case("rdma");
        let engine = Self::create(
            TransferEngineOptions::new(metadata_server, local_hostname)
                .ip_or_host_name(&host)
                .rpc_port(port)
                .auto_discover(auto),
        )?;
        if !auto {
            if protocol.eq_ignore_ascii_case("efa") || protocol.eq_ignore_ascii_case("cxi") {
                engine.discover_topology()?;
            }
            engine.install_transport(protocol)?;
        }
        Ok(engine)
    }

    fn check_rc(rc: i32) -> Result<(), EngineError> {
        if rc != 0 {
            Err(EngineError::OperationFailed(rc))
        } else {
            Ok(())
        }
    }

    fn check_nonneg(rc: i32) -> Result<i32, EngineError> {
        if rc < 0 {
            Err(EngineError::OperationFailed(rc))
        } else {
            Ok(rc)
        }
    }

    pub fn discover_topology(&self) -> Result<(), EngineError> {
        let rc = unsafe { ffi::discoverTopology(self.engine) };
        Self::check_rc(rc)
    }

    pub fn local_ip_and_port(&self) -> Result<String, EngineError> {
        let mut buf = [0u8; 256];
        let rc = unsafe {
            ffi::getLocalIpAndPort(self.engine, buf.as_mut_ptr() as *mut c_char, buf.len())
        };
        Self::check_rc(rc)?;
        let cstr = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) };
        Ok(cstr.to_string_lossy().into_owned())
    }

    pub fn install_transport(&self, proto: &str) -> Result<(), EngineError> {
        let proto_c = CString::new(proto)?;
        let handle =
            unsafe { ffi::installTransport(self.engine, proto_c.as_ptr(), std::ptr::null_mut()) };
        if handle.is_null() {
            Err(EngineError::OperationFailed(-1))
        } else {
            Ok(())
        }
    }

    pub fn uninstall_transport(&self, proto: &str) -> Result<(), EngineError> {
        let proto_c = CString::new(proto)?;
        let rc = unsafe { ffi::uninstallTransport(self.engine, proto_c.as_ptr()) };
        Self::check_rc(rc)
    }

    pub fn open_segment(&self, name: &str) -> Result<SegmentId, EngineError> {
        let name_c = CString::new(name)?;
        let id = unsafe { ffi::openSegment(self.engine, name_c.as_ptr()) };
        Self::check_nonneg(id)
    }

    pub fn open_segment_no_cache(&self, name: &str) -> Result<SegmentId, EngineError> {
        let name_c = CString::new(name)?;
        let id = unsafe { ffi::openSegmentNoCache(self.engine, name_c.as_ptr()) };
        Self::check_nonneg(id)
    }

    /// Open `name`, caching the segment id for later [`Self::segment`] lookups.
    pub fn open_segment_cached(&self, name: &str) -> Result<SegmentId, EngineError> {
        {
            let guard = self.segments.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(&id) = guard.get(name) {
                return Ok(id);
            }
        }
        let id = self.open_segment(name)?;
        let mut guard = self.segments.lock().unwrap_or_else(|e| e.into_inner());
        guard.insert(name.to_string(), id);
        Ok(id)
    }

    pub fn close_segment(&self, segment_id: SegmentId) -> Result<(), EngineError> {
        let rc = unsafe { ffi::closeSegment(self.engine, segment_id) };
        Self::check_rc(rc)
    }

    pub fn warmup_efa_segment(&self, name: &str) -> Result<(), EngineError> {
        let name_c = CString::new(name)?;
        let rc = unsafe { ffi::warmupEfaSegment(self.engine, name_c.as_ptr()) };
        Self::check_rc(rc)
    }

    pub fn remove_local_segment(&self, name: &str) -> Result<(), EngineError> {
        let name_c = CString::new(name)?;
        let rc = unsafe { ffi::removeLocalSegment(self.engine, name_c.as_ptr()) };
        Self::check_rc(rc)
    }

    /// Register `addr..addr+length` for transfers. `location` is typically
    /// `"cpu:0"` or [`WILDCARD_LOCATION`].
    ///
    /// # Safety
    ///
    /// `addr` must point to at least `length` bytes that remain valid until
    /// [`Self::unregister_local_memory`].
    pub unsafe fn register_local_memory(
        &self,
        addr: *mut c_void,
        length: usize,
        location: &str,
    ) -> Result<(), EngineError> {
        self.register_local_memory_ex(addr, length, location, true)
    }

    /// Like [`Self::register_local_memory`], with an explicit `remote_accessible` flag.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Self::register_local_memory`].
    pub unsafe fn register_local_memory_ex(
        &self,
        addr: *mut c_void,
        length: usize,
        location: &str,
        remote_accessible: bool,
    ) -> Result<(), EngineError> {
        let location_c = CString::new(location)?;
        let rc = ffi::registerLocalMemory(
            self.engine,
            addr,
            length,
            location_c.as_ptr(),
            i32::from(remote_accessible),
        );
        Self::check_rc(rc)
    }

    /// Python-compatible alias of [`Self::register_local_memory`] using
    /// [`WILDCARD_LOCATION`].
    ///
    /// # Safety
    ///
    /// Same requirements as [`Self::register_local_memory`].
    pub unsafe fn register_memory(
        &self,
        addr: *mut c_void,
        length: usize,
    ) -> Result<(), EngineError> {
        self.register_local_memory(addr, length, WILDCARD_LOCATION)
    }

    /// # Safety
    ///
    /// `addr` must be a pointer previously passed to `register_local_memory`.
    pub unsafe fn unregister_local_memory(&self, addr: *mut c_void) -> Result<(), EngineError> {
        let rc = ffi::unregisterLocalMemory(self.engine, addr);
        Self::check_rc(rc)
    }

    /// # Safety
    ///
    /// Same as [`Self::unregister_local_memory`].
    pub unsafe fn unregister_memory(&self, addr: *mut c_void) -> Result<(), EngineError> {
        self.unregister_local_memory(addr)
    }

    /// Batch-register buffers. Zero-copy: `buffer_list` is passed to C as-is.
    ///
    /// # Safety
    ///
    /// Each entry must satisfy [`Self::register_local_memory`].
    pub unsafe fn register_local_memory_batch(
        &self,
        buffer_list: &[BufferEntry],
        location: &str,
    ) -> Result<(), EngineError> {
        if buffer_list.is_empty() {
            return Ok(());
        }
        let location_c = CString::new(location)?;
        let rc = ffi::registerLocalMemoryBatch(
            self.engine,
            buffer_list.as_ptr() as *mut ffi::buffer_entry_t,
            buffer_list.len(),
            location_c.as_ptr(),
        );
        Self::check_rc(rc)
    }

    /// # Safety
    ///
    /// Each address must have been registered.
    pub unsafe fn unregister_local_memory_batch(
        &self,
        addrs: &[*mut c_void],
    ) -> Result<(), EngineError> {
        if addrs.is_empty() {
            return Ok(());
        }
        let mut addr_list = addrs.to_vec();
        let rc =
            ffi::unregisterLocalMemoryBatch(self.engine, addr_list.as_mut_ptr(), addr_list.len());
        Self::check_rc(rc)
    }

    pub fn allocate_batch_id(&self, batch_size: usize) -> Result<BatchId, EngineError> {
        let id = unsafe { ffi::allocateBatchID(self.engine, batch_size) };
        if id == INVALID_BATCH {
            Err(EngineError::OperationFailed(-1))
        } else {
            Ok(BatchId(id))
        }
    }

    /// Submit `requests` without allocating. `TransferRequest` is `#[repr(C)]`
    /// and matches `transfer_request_t`.
    ///
    /// # Safety
    ///
    /// Every `source` pointer must refer to registered memory that stays valid
    /// until the batch completes.
    pub unsafe fn submit_transfer(
        &self,
        batch_id: BatchId,
        requests: &[TransferRequest],
    ) -> Result<(), EngineError> {
        if requests.is_empty() {
            return Ok(());
        }
        let rc = ffi::submitTransfer(
            self.engine,
            batch_id.0,
            requests.as_ptr() as *mut ffi::transfer_request_t,
            requests.len(),
        );
        Self::check_rc(rc)
    }

    /// Submit plus a completion notification to the peer.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Self::submit_transfer`].
    pub unsafe fn submit_transfer_with_notify(
        &self,
        batch_id: BatchId,
        requests: &[TransferRequest],
        notify: &NotifyMsg,
    ) -> Result<(), EngineError> {
        if requests.is_empty() {
            return Ok(());
        }
        let name_c = CString::new(notify.name.as_str())?;
        let msg_c = CString::new(notify.msg.as_str())?;
        let notify_c = ffi::notify_msg_t {
            name: name_c.as_ptr() as *mut c_char,
            msg: msg_c.as_ptr() as *mut c_char,
        };
        let rc = ffi::submitTransferWithNotify(
            self.engine,
            batch_id.0,
            requests.as_ptr() as *mut ffi::transfer_request_t,
            requests.len(),
            notify_c,
        );
        Self::check_rc(rc)
    }

    pub fn get_transfer_status(
        &self,
        batch_id: BatchId,
        task_id: usize,
    ) -> Result<TransferStatus, EngineError> {
        let mut status = TransferStatus {
            status: TransferStatusCode::Waiting,
            transferred_bytes: 0,
        };
        let rc = unsafe {
            ffi::getTransferStatus(
                self.engine,
                batch_id.0,
                task_id,
                &mut status as *mut TransferStatus as *mut ffi::transfer_status_t,
            )
        };
        Self::check_rc(rc)?;
        Ok(status)
    }

    /// Poll every task in `0..count` until all are terminal, or `timeout`.
    /// Does not allocate. Does not free the batch id.
    pub fn wait_all(
        &self,
        batch_id: BatchId,
        count: usize,
        timeout: Option<Duration>,
    ) -> Result<(), EngineError> {
        let deadline = timeout.map(|d| Instant::now() + d);
        for task_id in 0..count {
            loop {
                let status = self.get_transfer_status(batch_id, task_id)?;
                match status.status {
                    TransferStatusCode::Completed => break,
                    TransferStatusCode::Failed
                    | TransferStatusCode::Timeout
                    | TransferStatusCode::Canceled
                    | TransferStatusCode::Invalid => {
                        return Err(EngineError::TransferFailed);
                    }
                    _ => {
                        if deadline.is_some_and(|d| Instant::now() >= d) {
                            return Err(EngineError::Timeout);
                        }
                        std::hint::spin_loop();
                    }
                }
            }
        }
        Ok(())
    }

    /// Allocate a batch, submit `requests`, wait, then free the batch.
    ///
    /// # Safety
    ///
    /// Same requirements as [`Self::submit_transfer`].
    pub unsafe fn submit_and_wait(
        &self,
        requests: &[TransferRequest],
        timeout: Option<Duration>,
    ) -> Result<(), EngineError> {
        if requests.is_empty() {
            return Ok(());
        }
        let batch_id = self.allocate_batch_id(requests.len())?;
        let result = (|| {
            self.submit_transfer(batch_id, requests)?;
            self.wait_all(batch_id, requests.len(), timeout)
        })();
        let _ = self.free_batch_id(batch_id);
        result
    }

    pub fn free_batch_id(&self, batch_id: BatchId) -> Result<(), EngineError> {
        let rc = unsafe { ffi::freeBatchID(self.engine, batch_id.0) };
        Self::check_rc(rc)
    }

    pub fn sync_segment_cache(&self) -> Result<(), EngineError> {
        let rc = unsafe { ffi::syncSegmentCache(self.engine) };
        Self::check_rc(rc)
    }

    pub fn take_notifies(&self) -> Result<Vec<NotifyMsg>, EngineError> {
        let mut size: i32 = 0;
        let ptr = unsafe { ffi::getNotifsFromEngine(self.engine, &mut size) };
        if ptr.is_null() {
            return Ok(Vec::new());
        }
        let slice = unsafe { std::slice::from_raw_parts(ptr, size as usize) };
        let mut out = Vec::with_capacity(slice.len());
        for msg in slice {
            let name = unsafe { CStr::from_ptr(msg.name) }
                .to_string_lossy()
                .into_owned();
            let text = unsafe { CStr::from_ptr(msg.msg) }
                .to_string_lossy()
                .into_owned();
            out.push(NotifyMsg { name, msg: text });
        }
        unsafe {
            ffi::freeNotifsMsgBuf(ptr, size);
        }
        Ok(out)
    }

    pub fn send_notify(&self, target_id: u64, notify: &NotifyMsg) -> Result<(), EngineError> {
        let name_c = CString::new(notify.name.as_str())?;
        let msg_c = CString::new(notify.msg.as_str())?;
        let notify_c = ffi::notify_msg_t {
            name: name_c.as_ptr() as *mut c_char,
            msg: msg_c.as_ptr() as *mut c_char,
        };
        let rc = unsafe { ffi::genNotifyInEngine(self.engine, target_id, notify_c) };
        Self::check_rc(rc)
    }

    pub fn nic_load_stats(&self) -> Result<Vec<NicLoadStat>, EngineError> {
        const FIRST: usize = 32;
        let mut stats: Vec<ffi::nic_load_stat_t> =
            (0..FIRST).map(|_| unsafe { std::mem::zeroed() }).collect();
        let mut count = FIRST;
        let rc = unsafe { ffi::getNicLoadStats(self.engine, stats.as_mut_ptr(), &mut count) };
        Self::check_rc(rc)?;
        if count > FIRST {
            stats.resize_with(count, || unsafe { std::mem::zeroed() });
            let mut recount = count;
            let rc = unsafe { ffi::getNicLoadStats(self.engine, stats.as_mut_ptr(), &mut recount) };
            Self::check_rc(rc)?;
            count = recount.min(stats.len());
        }
        Ok(stats
            .into_iter()
            .take(count)
            .map(|s| {
                let name = unsafe { CStr::from_ptr(s.device_name.as_ptr()) }
                    .to_string_lossy()
                    .into_owned();
                NicLoadStat {
                    device_name: name,
                    inflight_bytes: s.inflight_bytes,
                    ewma_bandwidth_bps: s.ewma_bandwidth_bps,
                }
            })
            .collect())
    }

    pub fn enable_graceful_shutdown(&self) {
        unsafe { ffi::enableGracefulShutdown(self.engine) };
    }

    pub fn show_links(&self, json: bool) -> Result<String, EngineError> {
        let mut buf = vec![0u8; 65_536];
        let rc = unsafe {
            ffi::showLinks(
                self.engine,
                buf.as_mut_ptr() as *mut c_char,
                buf.len(),
                i32::from(json),
            )
        };
        Self::check_rc(rc)?;
        let cstr = unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) };
        Ok(cstr.to_string_lossy().into_owned())
    }

    // -----------------------------------------------------------------------
    // Python-shaped convenience transfers (hostname → cached segment id)
    // -----------------------------------------------------------------------

    /// Synchronous write of `length` bytes from `local` to `remote_offset` on
    /// `target_hostname`.
    ///
    /// # Safety
    ///
    /// `local` must be registered and remain valid for the duration of the call.
    pub unsafe fn transfer_sync_write(
        &self,
        target_hostname: &str,
        local: *mut c_void,
        remote_offset: u64,
        length: u64,
    ) -> Result<(), EngineError> {
        self.transfer_sync(target_hostname, local, remote_offset, length, Opcode::Write)
    }

    /// Synchronous read of `length` bytes from `remote_offset` on
    /// `target_hostname` into `local`.
    ///
    /// # Safety
    ///
    /// `local` must be registered and remain valid for the duration of the call.
    pub unsafe fn transfer_sync_read(
        &self,
        target_hostname: &str,
        local: *mut c_void,
        remote_offset: u64,
        length: u64,
    ) -> Result<(), EngineError> {
        self.transfer_sync(target_hostname, local, remote_offset, length, Opcode::Read)
    }

    /// # Safety
    ///
    /// `local` must be registered and remain valid for the duration of the call.
    pub unsafe fn transfer_sync(
        &self,
        target_hostname: &str,
        local: *mut c_void,
        remote_offset: u64,
        length: u64,
        opcode: Opcode,
    ) -> Result<(), EngineError> {
        let target_id = self.open_segment_cached(target_hostname)?;
        let req = TransferRequest {
            opcode,
            source: local,
            target_id,
            target_offset: remote_offset,
            length,
        };
        self.submit_and_wait(&[req], None)
    }

    /// Batch synchronous transfer. Slice lengths must match.
    ///
    /// # Safety
    ///
    /// Every local pointer must be registered and remain valid.
    pub unsafe fn batch_transfer_sync(
        &self,
        target_hostname: &str,
        locals: &[*mut c_void],
        remote_offsets: &[u64],
        lengths: &[u64],
        opcode: Opcode,
    ) -> Result<(), EngineError> {
        if locals.len() != remote_offsets.len() || locals.len() != lengths.len() {
            return Err(EngineError::InvalidArgument(
                "locals, remote_offsets, and lengths must have the same length",
            ));
        }
        if locals.is_empty() {
            return Ok(());
        }
        let target_id = self.open_segment_cached(target_hostname)?;
        let mut reqs = Vec::with_capacity(locals.len());
        for i in 0..locals.len() {
            reqs.push(TransferRequest {
                opcode,
                source: locals[i],
                target_id,
                target_offset: remote_offsets[i],
                length: lengths[i],
            });
        }
        self.submit_and_wait(&reqs, None)
    }

    pub unsafe fn batch_transfer_sync_write(
        &self,
        target_hostname: &str,
        locals: &[*mut c_void],
        remote_offsets: &[u64],
        lengths: &[u64],
    ) -> Result<(), EngineError> {
        self.batch_transfer_sync(
            target_hostname,
            locals,
            remote_offsets,
            lengths,
            Opcode::Write,
        )
    }

    pub unsafe fn batch_transfer_sync_read(
        &self,
        target_hostname: &str,
        locals: &[*mut c_void],
        remote_offsets: &[u64],
        lengths: &[u64],
    ) -> Result<(), EngineError> {
        self.batch_transfer_sync(
            target_hostname,
            locals,
            remote_offsets,
            lengths,
            Opcode::Read,
        )
    }

    /// Submit an async write and return the batch id. Caller must
    /// [`Self::free_batch_id`] after completion.
    ///
    /// # Safety
    ///
    /// `local` must be registered until the batch completes.
    pub unsafe fn transfer_submit_write(
        &self,
        target_hostname: &str,
        local: *mut c_void,
        remote_offset: u64,
        length: u64,
    ) -> Result<BatchId, EngineError> {
        let target_id = self.open_segment_cached(target_hostname)?;
        let batch_id = self.allocate_batch_id(1)?;
        let req = TransferRequest::write(local, target_id, remote_offset, length);
        if let Err(e) = self.submit_transfer(batch_id, &[req]) {
            let _ = self.free_batch_id(batch_id);
            return Err(e);
        }
        Ok(batch_id)
    }

    /// Python-compatible status poll: `Completed`, `Failed`/`Timeout`/`Invalid`
    /// are terminal. Does **not** free the batch (unlike the Python wrapper);
    /// call [`Self::free_batch_id`] after a terminal status.
    pub fn transfer_check_status(
        &self,
        batch_id: BatchId,
    ) -> Result<TransferStatusCode, EngineError> {
        Ok(self.get_transfer_status(batch_id, 0)?.status)
    }
}

impl Drop for TransferEngine {
    fn drop(&mut self) {
        if !self.engine.is_null() {
            unsafe { ffi::destroyTransferEngine(self.engine) };
            self.engine = std::ptr::null_mut();
        }
    }
}

fn parse_host_port(local_hostname: &str) -> (String, u64) {
    if let Some(stripped) = local_hostname.strip_prefix('[') {
        if let Some((host, rest)) = stripped.split_once("]:") {
            if let Ok(port) = rest.parse::<u64>() {
                return (host.to_string(), port);
            }
        }
        return (local_hostname.to_string(), 0);
    }
    if let Some((host, port)) = local_hostname.rsplit_once(':') {
        if !host.is_empty() {
            if let Ok(p) = port.parse::<u64>() {
                return (host.to_string(), p);
            }
        }
    }
    (local_hostname.to_string(), 0)
}

#[cfg(test)]
mod tests {
    use super::parse_host_port;

    #[test]
    fn parse_host_port_splits_ipv4() {
        assert_eq!(
            parse_host_port("127.0.0.1:12345"),
            ("127.0.0.1".into(), 12345)
        );
        assert_eq!(parse_host_port("localhost"), ("localhost".into(), 0));
        assert_eq!(
            parse_host_port("host:notaport"),
            ("host:notaport".into(), 0)
        );
        assert_eq!(parse_host_port("[::1]:12345"), ("::1".into(), 12345));
    }
}
