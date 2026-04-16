use std::collections::BTreeMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport_sys::tent as ffi;

use crate::env::EnvOverrideGuard;
use crate::{Opcode, TransferProgress, TransferRequest, TransferStatus};

#[derive(Clone, Default)]
pub struct TentEngineConfig {
    config_path: Option<PathBuf>,
    overrides: BTreeMap<String, String>,
    redis_username: Option<String>,
    redis_password: Option<String>,
    redis_db_index: Option<String>,
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

    pub fn redis_username(mut self, username: impl Into<String>) -> Self {
        self.redis_username = Some(username.into());
        self
    }

    pub fn redis_password(mut self, password: impl Into<String>) -> Self {
        self.redis_password = Some(password.into());
        self
    }

    pub fn redis_db_index(mut self, db_index: impl Into<String>) -> Self {
        self.redis_db_index = Some(db_index.into());
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

impl std::fmt::Debug for TentEngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TentEngineConfig")
            .field("config_path", &self.config_path)
            .field("overrides", &self.overrides)
            .field("redis_username", &self.redis_username)
            .field(
                "redis_password",
                &self.redis_password.as_ref().map(|_| "<redacted>"),
            )
            .field("redis_db_index", &self.redis_db_index)
            .finish()
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
    raw: RwLock<ffi::TentEngineHandle>,
    config: TentEngineConfig,
    registered_regions: Mutex<BTreeMap<usize, usize>>,
    owned_allocations: Mutex<BTreeMap<usize, TentOwnedAllocation>>,
    generation: AtomicU64,
}

unsafe impl Send for TentEngine {}
unsafe impl Sync for TentEngine {}

#[derive(Copy, Clone, Debug)]
struct TentOwnedAllocation {
    generation: u64,
    size: usize,
}

impl TentEngine {
    pub fn new(config: &TentEngineConfig) -> Result<Self> {
        let raw = Self::create_engine_handle(config)?;
        Ok(Self {
            raw: RwLock::new(raw),
            config: config.clone(),
            registered_regions: Mutex::new(BTreeMap::new()),
            owned_allocations: Mutex::new(BTreeMap::new()),
            generation: AtomicU64::new(0),
        })
    }

    fn create_engine_handle(config: &TentEngineConfig) -> Result<ffi::TentEngineHandle> {
        let _lock = tent_engine_create_lock()
            .lock()
            .expect("tent engine create lock poisoned");
        let _env = TentRedisEnvGuard::apply(config);
        config.apply()?;
        let raw = unsafe { ffi::tent_create_engine() };
        if raw.is_null() {
            return Err(StoreError::Transport(
                "tent_create_engine returned a null pointer".to_string(),
            ));
        }
        Ok(raw)
    }

    fn lock_error(name: &str) -> StoreError {
        StoreError::Transport(format!("{name} lock poisoned"))
    }

    fn engine_handle(&self) -> Result<ffi::TentEngineHandle> {
        self.raw
            .read()
            .map(|guard| *guard)
            .map_err(|_| Self::lock_error("tent raw"))
    }

    fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    fn snapshot_registered_regions(&self) -> Result<Vec<(*mut c_void, usize)>> {
        Ok(self
            .registered_regions
            .lock()
            .map_err(|_| Self::lock_error("tent registered_regions"))?
            .iter()
            .map(|(addr, size)| (*addr as *mut c_void, *size))
            .collect::<Vec<_>>())
    }

    fn mark_owned_allocation(&self, addr: *mut c_void, size: usize) -> Result<()> {
        self.owned_allocations
            .lock()
            .map_err(|_| Self::lock_error("tent owned_allocations"))?
            .insert(
                addr as usize,
                TentOwnedAllocation {
                    generation: self.current_generation(),
                    size,
                },
            );
        Ok(())
    }

    fn take_owned_allocation(&self, addr: *mut c_void) -> Result<Option<TentOwnedAllocation>> {
        Ok(self
            .owned_allocations
            .lock()
            .map_err(|_| Self::lock_error("tent owned_allocations"))?
            .remove(&(addr as usize)))
    }

    fn rebuild_engine_for_metadata_recovery(&self) -> Result<ffi::TentEngineHandle> {
        let replacement = Self::create_engine_handle(&self.config)?;
        let previous = {
            let mut raw = self.raw.write().map_err(|_| Self::lock_error("tent raw"))?;
            std::mem::replace(&mut *raw, replacement)
        };
        unsafe { ffi::tent_destroy_engine(previous) };
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.engine_handle()
    }

    pub fn segment_name(&self) -> Result<String> {
        read_engine_string(
            self.engine_handle()?,
            ffi::tent_segment_name,
            "tent_segment_name",
        )
    }

    pub fn rpc_server_address(&self) -> Result<(String, u16)> {
        let mut buffer = vec![0 as c_char; 256];
        let mut port = 0u16;
        let raw = self.engine_handle()?;
        let rc = unsafe {
            ffi::tent_rpc_server_addr_port(raw, buffer.as_mut_ptr(), buffer.len(), &mut port)
        };
        check_zero(rc, "tent_rpc_server_addr_port")?;
        Ok((read_string(&buffer), port))
    }

    pub fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let segment_name = to_cstring("segment_name", segment_name)?;
        let mut handle = 0u64;
        let rc = unsafe {
            ffi::tent_open_segment(self.engine_handle()?, &mut handle, segment_name.as_ptr())
        };
        check_zero(rc, "tent_open_segment")?;
        Ok(handle)
    }

    pub fn close_segment(&self, handle: u64) -> Result<()> {
        let rc = unsafe { ffi::tent_close_segment(self.engine_handle()?, handle) };
        check_zero(rc, "tent_close_segment")
    }

    pub fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        let mut info = ffi::TentSegmentInfo {
            kind: ffi::TYPE_MEMORY,
            num_buffers: 0,
            buffers: std::ptr::null_mut(),
        };
        let rc = unsafe { ffi::tent_get_segment_info(self.engine_handle()?, handle, &mut info) };
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
        let rc = unsafe {
            ffi::tent_allocate_memory(self.engine_handle()?, &mut addr, size, location.as_ptr())
        };
        check_zero(rc, "tent_allocate_memory")?;
        self.mark_owned_allocation(addr, size)?;
        Ok(addr)
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        self.registered_regions
            .lock()
            .map_err(|_| Self::lock_error("tent registered_regions"))?
            .remove(&(addr as usize));
        let Some(allocation) = self.take_owned_allocation(addr)? else {
            let rc = unsafe { ffi::tent_free_memory(self.engine_handle()?, addr) };
            return check_zero(rc, "tent_free_memory");
        };
        if allocation.generation == self.current_generation() {
            let rc = unsafe { ffi::tent_free_memory(self.engine_handle()?, addr) };
            return check_zero(rc, "tent_free_memory");
        }
        let rc = unsafe { ffi::mooncake_tent_platform_free_memory(addr, allocation.size) };
        check_zero(rc, "mooncake_tent_platform_free_memory")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let rc = unsafe { ffi::tent_register_memory(self.engine_handle()?, addr, size) };
        check_zero(rc, "tent_register_memory")?;
        self.registered_regions
            .lock()
            .map_err(|_| Self::lock_error("tent registered_regions"))?
            .insert(addr as usize, size);
        Ok(())
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let rc = unsafe { ffi::tent_unregister_memory(self.engine_handle()?, addr, size) };
        check_zero(rc, "tent_unregister_memory")?;
        self.registered_regions
            .lock()
            .map_err(|_| Self::lock_error("tent registered_regions"))?
            .remove(&(addr as usize));
        Ok(())
    }

    pub fn republish_local_metadata(&self) -> Result<()> {
        let regions = self.snapshot_registered_regions()?;
        let raw = self.rebuild_engine_for_metadata_recovery()?;
        for (addr, size) in regions {
            let register_rc = unsafe { ffi::tent_register_memory(raw, addr, size) };
            check_zero(register_rc, "tent_register_memory")?;
        }
        Ok(())
    }

    pub fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        let batch_id = unsafe { ffi::tent_allocate_batch(self.engine_handle()?, batch_size) };
        if batch_id == u64::MAX {
            return Err(StoreError::Transport(format!(
                "tent_allocate_batch failed for batch_size={batch_size}"
            )));
        }
        Ok(batch_id)
    }

    pub fn free_batch(&self, batch_id: u64) -> Result<()> {
        let rc = unsafe { ffi::tent_free_batch(self.engine_handle()?, batch_id) };
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
                self.engine_handle()?,
                batch_id,
                native_requests.as_mut_ptr(),
                native_requests.len(),
            )
        };
        check_zero(rc, "tent_submit")
    }

    pub fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        let mut status = ffi::TentStatus::default();
        let rc =
            unsafe { ffi::tent_task_status(self.engine_handle()?, batch_id, task_id, &mut status) };
        check_zero(rc, "tent_task_status")?;
        Ok(TransferProgress {
            status: decode_status(status.status),
            transferred_bytes: status.transferred_bytes,
        })
    }

    pub fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        let mut status = ffi::TentStatus::default();
        let rc = unsafe { ffi::tent_overall_status(self.engine_handle()?, batch_id, &mut status) };
        check_zero(rc, "tent_overall_status")?;
        Ok(TransferProgress {
            status: decode_status(status.status),
            transferred_bytes: status.transferred_bytes,
        })
    }
}

impl Drop for TentEngine {
    fn drop(&mut self) {
        let raw = self
            .raw
            .write()
            .map(|mut guard| std::mem::replace(&mut *guard, std::ptr::null_mut()))
            .unwrap_or(std::ptr::null_mut());
        if !raw.is_null() {
            unsafe { ffi::tent_destroy_engine(raw) };
        }
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

fn tent_engine_create_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct TentRedisEnvGuard {
    _env: EnvOverrideGuard,
}

impl TentRedisEnvGuard {
    fn apply(config: &TentEngineConfig) -> Self {
        let mut env = EnvOverrideGuard::new();
        env.set_optional("MC_REDIS_USERNAME", config.redis_username.as_deref());
        env.set_optional("MC_REDIS_PASSWORD", config.redis_password.as_deref());
        env.set_optional("MC_REDIS_DB_INDEX", config.redis_db_index.as_deref());
        Self { _env: env }
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

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::os::raw::c_char;
    use std::path::PathBuf;

    use mooncake_transport_sys::tent as ffi;

    use super::{
        check_zero, cstring_from_path, decode_status, encode_opcode, read_segment_buffers,
        read_string, to_cstring, TentEngineConfig,
    };
    use crate::{Opcode, TransferStatus};

    #[test]
    fn tent_config_builders_accumulate_overrides() {
        let config = TentEngineConfig::new()
            .config_path("tent.toml")
            .set("log_level", "debug")
            .set("rpc_server_port", "0")
            .redis_username("user-a")
            .redis_password("secret-pass")
            .redis_db_index("7");
        let debug = format!("{config:?}");
        assert!(debug.contains("tent.toml"));
        assert!(debug.contains("log_level"));
        assert!(debug.contains("rpc_server_port"));
        assert!(debug.contains("user-a"));
        assert!(debug.contains("<redacted>"));
        assert!(debug.contains("7"));
        assert!(!debug.contains("secret-pass"));
    }

    #[test]
    fn tent_string_helpers_validate_inputs() {
        assert!(to_cstring("key", "value").is_ok());
        assert!(to_cstring("key", "bad\0value").is_err());
        assert!(cstring_from_path(&PathBuf::from("config.toml")).is_ok());
    }

    #[test]
    fn tent_codecs_match_ffi_constants() {
        check_zero(0, "tent").expect("zero rc should pass");
        assert!(check_zero(1, "tent").is_err());
        assert_eq!(encode_opcode(Opcode::Read), ffi::OPCODE_READ);
        assert_eq!(encode_opcode(Opcode::Write), ffi::OPCODE_WRITE);
        assert_eq!(decode_status(ffi::STATUS_WAITING), TransferStatus::Waiting);
        assert_eq!(decode_status(ffi::STATUS_PENDING), TransferStatus::Pending);
        assert_eq!(decode_status(ffi::STATUS_INVALID), TransferStatus::Invalid);
        assert_eq!(
            decode_status(ffi::STATUS_CANCELED),
            TransferStatus::Canceled
        );
        assert_eq!(
            decode_status(ffi::STATUS_COMPLETED),
            TransferStatus::Completed
        );
        assert_eq!(decode_status(ffi::STATUS_TIMEOUT), TransferStatus::Timeout);
        assert_eq!(decode_status(ffi::STATUS_FAILED), TransferStatus::Failed);
    }

    #[test]
    fn tent_buffer_helpers_read_c_strings_and_buffers() {
        let text = CString::new("tent-segment").expect("cstring should build");
        let mut chars = [0 as c_char; 32];
        for (dst, src) in chars.iter_mut().zip(text.as_bytes_with_nul()) {
            *dst = *src as c_char;
        }
        assert_eq!(read_string(&chars), "tent-segment");

        let location = CString::new("cpu:0").expect("cstring should build");
        let mut location_buf = [0 as c_char; 64];
        for (dst, src) in location_buf
            .iter_mut()
            .zip(location.as_bytes_with_nul().iter().copied())
        {
            *dst = src as c_char;
        }
        let mut buffers = vec![ffi::TentBufferInfo {
            base: 11,
            length: 22,
            location: location_buf,
        }];
        let info = ffi::TentSegmentInfo {
            kind: ffi::TYPE_MEMORY,
            num_buffers: buffers.len() as i32,
            buffers: buffers.as_mut_ptr(),
        };
        let parsed = unsafe { read_segment_buffers(&info) };
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].base, 11);
        assert_eq!(parsed[0].length, 22);
        assert_eq!(parsed[0].location, "cpu:0");
    }
}
