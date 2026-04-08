use std::collections::BTreeMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::path::{Path, PathBuf};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport_sys::tent as ffi;

use crate::{Opcode, TransferProgress, TransferRequest, TransferStatus};

#[derive(Clone, Debug, Default)]
pub struct TentEngineConfig {
    config_path: Option<PathBuf>,
    overrides: BTreeMap<String, String>,
}

impl TentEngineConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    pub fn set(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.overrides.insert(key.into(), value.into());
        self
    }

    fn apply(&self) -> Result<()> {
        if let Some(path) = &self.config_path {
            let path = cstring_from_path(path)?;
            unsafe { ffi::tent_load_config_from_file(path.as_ptr()) };
        }
        for (key, value) in &self.overrides {
            let key = to_cstring("key", key)?;
            let value = to_cstring("value", value)?;
            unsafe { ffi::tent_set_config(key.as_ptr(), value.as_ptr()) };
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentInfo {
    pub kind: SegmentKind,
    pub buffers: Vec<SegmentBuffer>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SegmentKind {
    Memory,
    File,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentBuffer {
    pub base: u64,
    pub length: u64,
    pub location: String,
}

pub struct TentEngine {
    raw: ffi::TentEngineHandle,
}

impl TentEngine {
    pub fn new(config: &TentEngineConfig) -> Result<Self> {
        config.apply()?;
        let raw = unsafe { ffi::tent_create_engine() };
        if raw.is_null() {
            return Err(StoreError::Transport(
                "tent_create_engine returned a null pointer".to_string(),
            ));
        }
        Ok(Self { raw })
    }

    pub fn segment_name(&self) -> Result<String> {
        read_engine_string(self.raw, ffi::tent_segment_name, "tent_segment_name")
    }

    pub fn rpc_server_address(&self) -> Result<(String, u16)> {
        let mut buffer = vec![0 as c_char; 256];
        let mut port = 0u16;
        let rc = unsafe {
            ffi::tent_rpc_server_addr_port(self.raw, buffer.as_mut_ptr(), buffer.len(), &mut port)
        };
        check_zero(rc, "tent_rpc_server_addr_port")?;
        Ok((read_string(&buffer), port))
    }

    pub fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let segment_name = to_cstring("segment_name", segment_name)?;
        let mut handle = 0u64;
        let rc = unsafe { ffi::tent_open_segment(self.raw, &mut handle, segment_name.as_ptr()) };
        check_zero(rc, "tent_open_segment")?;
        Ok(handle)
    }

    pub fn close_segment(&self, handle: u64) -> Result<()> {
        let rc = unsafe { ffi::tent_close_segment(self.raw, handle) };
        check_zero(rc, "tent_close_segment")
    }

    pub fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        let mut info = ffi::TentSegmentInfo {
            kind: ffi::TYPE_MEMORY,
            num_buffers: 0,
            buffers: std::ptr::null_mut(),
        };
        let rc = unsafe { ffi::tent_get_segment_info(self.raw, handle, &mut info) };
        check_zero(rc, "tent_get_segment_info")?;
        let buffers = unsafe { read_segment_buffers(&info) };
        unsafe { ffi::tent_free_segment_info(&mut info) };
        Ok(SegmentInfo {
            kind: match info.kind {
                ffi::TYPE_MEMORY => SegmentKind::Memory,
                _ => SegmentKind::File,
            },
            buffers,
        })
    }

    pub fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        let location = to_cstring("location", location)?;
        let mut addr = std::ptr::null_mut();
        let rc = unsafe { ffi::tent_allocate_memory(self.raw, &mut addr, size, location.as_ptr()) };
        check_zero(rc, "tent_allocate_memory")?;
        Ok(addr)
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        let rc = unsafe { ffi::tent_free_memory(self.raw, addr) };
        check_zero(rc, "tent_free_memory")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let rc = unsafe { ffi::tent_register_memory(self.raw, addr, size) };
        check_zero(rc, "tent_register_memory")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let rc = unsafe { ffi::tent_unregister_memory(self.raw, addr, size) };
        check_zero(rc, "tent_unregister_memory")
    }

    pub fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        let batch_id = unsafe { ffi::tent_allocate_batch(self.raw, batch_size) };
        if batch_id == u64::MAX {
            return Err(StoreError::Transport(format!(
                "tent_allocate_batch failed for batch_size={batch_size}"
            )));
        }
        Ok(batch_id)
    }

    pub fn free_batch(&self, batch_id: u64) -> Result<()> {
        let rc = unsafe { ffi::tent_free_batch(self.raw, batch_id) };
        check_zero(rc, "tent_free_batch")
    }

    pub fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        let mut native_requests = requests
            .iter()
            .map(|request| ffi::TentRequest {
                opcode: encode_opcode(request.opcode),
                source: request.source,
                target_id: request.target_id,
                target_offset: request.target_offset,
                length: request.length,
            })
            .collect::<Vec<_>>();
        let rc = unsafe {
            ffi::tent_submit(
                self.raw,
                batch_id,
                native_requests.as_mut_ptr(),
                native_requests.len(),
            )
        };
        check_zero(rc, "tent_submit")
    }

    pub fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        let mut status = ffi::TentStatus::default();
        let rc = unsafe { ffi::tent_task_status(self.raw, batch_id, task_id, &mut status) };
        check_zero(rc, "tent_task_status")?;
        Ok(TransferProgress {
            status: decode_status(status.status),
            transferred_bytes: status.transferred_bytes,
        })
    }

    pub fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        let mut status = ffi::TentStatus::default();
        let rc = unsafe { ffi::tent_overall_status(self.raw, batch_id, &mut status) };
        check_zero(rc, "tent_overall_status")?;
        Ok(TransferProgress {
            status: decode_status(status.status),
            transferred_bytes: status.transferred_bytes,
        })
    }
}

impl Drop for TentEngine {
    fn drop(&mut self) {
        unsafe { ffi::tent_destroy_engine(self.raw) };
    }
}

fn to_cstring(field: &str, value: &str) -> Result<CString> {
    CString::new(value)
        .map_err(|_| StoreError::Transport(format!("{field} contains an embedded NUL byte")))
}

fn cstring_from_path(path: &Path) -> Result<CString> {
    let text = path.to_str().ok_or_else(|| {
        StoreError::Transport(format!("path is not valid UTF-8: {}", path.display()))
    })?;
    to_cstring("path", text)
}

fn check_zero(rc: i32, operation: &str) -> Result<()> {
    if rc == 0 {
        Ok(())
    } else {
        Err(StoreError::Transport(format!(
            "{operation} failed with rc={rc}"
        )))
    }
}

fn encode_opcode(opcode: Opcode) -> i32 {
    match opcode {
        Opcode::Read => ffi::OPCODE_READ,
        Opcode::Write => ffi::OPCODE_WRITE,
    }
}

fn decode_status(status: i32) -> TransferStatus {
    match status {
        ffi::STATUS_WAITING => TransferStatus::Waiting,
        ffi::STATUS_PENDING => TransferStatus::Pending,
        ffi::STATUS_INVALID => TransferStatus::Invalid,
        ffi::STATUS_CANCELED => TransferStatus::Canceled,
        ffi::STATUS_COMPLETED => TransferStatus::Completed,
        ffi::STATUS_TIMEOUT => TransferStatus::Timeout,
        _ => TransferStatus::Failed,
    }
}

fn read_engine_string(
    engine: ffi::TentEngineHandle,
    function: unsafe extern "C" fn(ffi::TentEngineHandle, *mut c_char, usize) -> i32,
    operation: &str,
) -> Result<String> {
    let mut buffer = vec![0 as c_char; 256];
    let rc = unsafe { function(engine, buffer.as_mut_ptr(), buffer.len()) };
    check_zero(rc, operation)?;
    Ok(read_string(&buffer))
}

fn read_string(buffer: &[c_char]) -> String {
    let ptr = buffer.as_ptr();
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

unsafe fn read_segment_buffers(info: &ffi::TentSegmentInfo) -> Vec<SegmentBuffer> {
    if info.buffers.is_null() || info.num_buffers <= 0 {
        return Vec::new();
    }

    let slice = std::slice::from_raw_parts(info.buffers, info.num_buffers as usize);
    slice
        .iter()
        .map(|entry| SegmentBuffer {
            base: entry.base,
            length: entry.length,
            location: CStr::from_ptr(entry.location.as_ptr())
                .to_string_lossy()
                .into_owned(),
        })
        .collect()
}
