use std::ffi::{c_void, CString};
use std::sync::{Mutex, OnceLock};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport_sys::classic as ffi;

use crate::env::EnvOverrideGuard;
use crate::{
    clamp_registration_size, rdma_device_max_registration_size, Opcode, TransferProgress,
    TransferRequest, TransferStatus,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassicEngineConfig {
    metadata_uri: String,
    rpc_bind_host: String,
    rpc_port: Option<u16>,
    protocol: ClassicTransportProtocol,
    slice_timeout_ms: Option<u64>,
    redis_username: Option<String>,
    redis_password: Option<String>,
    redis_db_index: Option<String>,
    gid_index: Option<String>,
}

impl ClassicEngineConfig {
    pub fn new(metadata_uri: impl Into<String>, rpc_bind_host: impl Into<String>) -> Self {
        Self {
            metadata_uri: metadata_uri.into(),
            rpc_bind_host: rpc_bind_host.into(),
            rpc_port: None,
            protocol: ClassicTransportProtocol::Tcp,
            slice_timeout_ms: None,
            redis_username: None,
            redis_password: None,
            redis_db_index: None,
            gid_index: None,
        }
    }

    pub fn protocol(mut self, protocol: ClassicTransportProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    pub fn rpc_port(mut self, rpc_port: u16) -> Self {
        self.rpc_port = Some(rpc_port);
        self
    }

    pub fn slice_timeout(mut self, timeout: std::time::Duration) -> Self {
        let timeout_ms = timeout.as_millis().min(u128::from(u64::MAX)) as u64;
        self.slice_timeout_ms = Some(timeout_ms.max(1));
        self
    }

    pub fn redis_username(mut self, redis_username: impl Into<String>) -> Self {
        self.redis_username = Some(redis_username.into());
        self
    }

    pub fn redis_password(mut self, redis_password: impl Into<String>) -> Self {
        self.redis_password = Some(redis_password.into());
        self
    }

    pub fn redis_db_index(mut self, redis_db_index: impl Into<String>) -> Self {
        self.redis_db_index = Some(redis_db_index.into());
        self
    }

    pub fn gid_index(mut self, gid_index: impl Into<String>) -> Self {
        self.gid_index = Some(gid_index.into());
        self
    }

    pub fn metadata_uri(&self) -> &str {
        &self.metadata_uri
    }

    pub fn uses_p2p_handshake_metadata(&self) -> bool {
        self.metadata_uri.eq_ignore_ascii_case("P2PHANDSHAKE")
    }

    pub fn rpc_bind_host(&self) -> &str {
        &self.rpc_bind_host
    }

    pub fn rpc_port_value(&self) -> Option<u16> {
        self.rpc_port
    }

    pub fn transport_protocol(&self) -> ClassicTransportProtocol {
        self.protocol
    }

    pub fn slice_timeout_ms_value(&self) -> Option<u64> {
        self.slice_timeout_ms
    }

    pub fn redis_username_value(&self) -> Option<&str> {
        self.redis_username.as_deref()
    }

    pub fn redis_password_value(&self) -> Option<&str> {
        self.redis_password.as_deref()
    }

    pub fn redis_db_index_value(&self) -> Option<&str> {
        self.redis_db_index.as_deref()
    }

    pub fn gid_index_value(&self) -> Option<&str> {
        self.gid_index.as_deref()
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ClassicTransportProtocol {
    Tcp,
    Rdma,
}

pub struct ClassicTransferEngine {
    raw: ffi::TransferEngineHandle,
}

impl ClassicTransferEngine {
    pub fn new(config: &ClassicEngineConfig, local_server_name: &str) -> Result<Self> {
        let _lock = classic_engine_create_lock()
            .lock()
            .expect("classic engine create lock poisoned");
        let _env = ClassicCreateEnvGuard::apply(config);

        let metadata_uri = to_cstring("metadata_uri", config.metadata_uri())?;
        let local_server_name = to_cstring("local_server_name", local_server_name)?;
        let ip_or_host_name = to_cstring("ip_or_host_name", config.rpc_bind_host())?;
        let auto_discover = matches!(config.transport_protocol(), ClassicTransportProtocol::Rdma);

        let raw = unsafe {
            ffi::createTransferEngine(
                metadata_uri.as_ptr(),
                local_server_name.as_ptr(),
                ip_or_host_name.as_ptr(),
                u64::from(config.rpc_port_value().unwrap_or_default()),
                auto_discover as i32,
            )
        };
        if raw.is_null() {
            return Err(StoreError::Transport(
                "createTransferEngine returned a null pointer".to_string(),
            ));
        }

        let engine = Self { raw };
        if matches!(config.transport_protocol(), ClassicTransportProtocol::Tcp) {
            engine.install_transport("tcp")?;
        }
        Ok(engine)
    }

    pub fn rpc_server_address_text(&self) -> Result<String> {
        let mut buffer = vec![0i8; 256];
        let rc = unsafe { ffi::getLocalIpAndPort(self.raw, buffer.as_mut_ptr(), buffer.len()) };
        check_zero(rc, "getLocalIpAndPort")?;
        Ok(read_c_buffer(&buffer))
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn register_local_memory(
        &self,
        addr: *mut c_void,
        length: usize,
        location: &str,
        remote_accessible: bool,
    ) -> Result<()> {
        let location = to_cstring("location", location)?;
        let rc = unsafe {
            ffi::registerLocalMemory(
                self.raw,
                addr,
                length,
                location.as_ptr(),
                remote_accessible as i32,
            )
        };
        check_zero(rc, "registerLocalMemory")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn register_local_memory_batch(
        &self,
        entries: &[(*mut c_void, usize)],
        location: &str,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let location = to_cstring("location", location)?;
        let mut native_entries = entries
            .iter()
            .map(|(addr, length)| ffi::BufferEntry {
                addr: *addr,
                length: *length,
            })
            .collect::<Vec<_>>();
        let rc = unsafe {
            ffi::registerLocalMemoryBatch(
                self.raw,
                native_entries.as_mut_ptr(),
                native_entries.len(),
                location.as_ptr(),
            )
        };
        check_zero(rc, "registerLocalMemoryBatch")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn unregister_local_memory(&self, addr: *mut c_void) -> Result<()> {
        let rc = unsafe { ffi::unregisterLocalMemory(self.raw, addr) };
        check_zero(rc, "unregisterLocalMemory")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    pub fn unregister_local_memory_batch(&self, addrs: &[*mut c_void]) -> Result<()> {
        if addrs.is_empty() {
            return Ok(());
        }
        let mut native_addrs = addrs.to_vec();
        let rc = unsafe {
            ffi::unregisterLocalMemoryBatch(self.raw, native_addrs.as_mut_ptr(), native_addrs.len())
        };
        check_zero(rc, "unregisterLocalMemoryBatch")
    }

    pub fn open_segment(&self, segment_name: &str) -> Result<i32> {
        let segment_name = to_cstring("segment_name", segment_name)?;
        let handle = unsafe { ffi::openSegment(self.raw, segment_name.as_ptr()) };
        if handle >= 0 {
            return Ok(handle);
        }
        let refreshed = unsafe { ffi::openSegmentNoCache(self.raw, segment_name.as_ptr()) };
        if refreshed < 0 {
            return Err(StoreError::Transport(format!(
                "openSegment failed for {:?}",
                segment_name
            )));
        }
        Ok(refreshed)
    }

    pub fn close_segment(&self, segment_id: i32) -> Result<()> {
        let rc = unsafe { ffi::closeSegment(self.raw, segment_id) };
        check_zero(rc, "closeSegment")
    }

    pub fn first_buffer(&self, segment_id: i32) -> Result<(u64, u64)> {
        let mut addr = 0u64;
        let mut length = 0u64;
        let rc = unsafe {
            ffi::mooncake_classic_get_segment_first_buffer(
                self.raw,
                segment_id,
                &mut addr,
                &mut length,
            )
        };
        check_zero(rc, "mooncake_classic_get_segment_first_buffer")?;
        Ok((addr, length))
    }

    pub fn segment_buffers(&self, segment_id: i32) -> Result<Vec<(u64, u64)>> {
        let mut count = 0usize;
        let rc = unsafe {
            ffi::mooncake_classic_get_segment_buffer_count(self.raw, segment_id, &mut count)
        };
        check_zero(rc, "mooncake_classic_get_segment_buffer_count")?;
        let mut buffers = Vec::with_capacity(count);
        for index in 0..count {
            let mut addr = 0u64;
            let mut length = 0u64;
            let rc = unsafe {
                ffi::mooncake_classic_get_segment_buffer(
                    self.raw,
                    segment_id,
                    index,
                    &mut addr,
                    &mut length,
                )
            };
            check_zero(rc, "mooncake_classic_get_segment_buffer")?;
            buffers.push((addr, length));
        }
        Ok(buffers)
    }

    pub fn rdma_max_registration_size() -> Option<usize> {
        let configured_cap = usize::try_from(unsafe { ffi::mooncake_classic_get_max_mr_size() })
            .ok()
            .filter(|value| *value > 0);
        clamp_registration_size(rdma_device_max_registration_size(), configured_cap)
    }

    pub fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        let batch_id = unsafe { ffi::allocateBatchID(self.raw, batch_size) };
        if batch_id == u64::MAX {
            return Err(StoreError::Transport(format!(
                "allocateBatchID failed for batch_size={batch_size}"
            )));
        }
        Ok(batch_id)
    }

    pub fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        let mut native_requests = encode_requests(requests)?;
        let rc = unsafe {
            ffi::submitTransfer(
                self.raw,
                batch_id,
                native_requests.as_mut_ptr(),
                native_requests.len(),
            )
        };
        check_zero(rc, "submitTransfer")
    }

    pub fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        let mut status = ffi::TransferStatus::default();
        let rc = unsafe { ffi::getTransferStatus(self.raw, batch_id, task_id, &mut status) };
        check_zero(rc, "getTransferStatus")?;
        Ok(TransferProgress {
            status: decode_status(status.status),
            transferred_bytes: status.transferred_bytes,
        })
    }

    pub fn batch_status(&self, batch_id: u64) -> Result<TransferProgress> {
        let mut status = ffi::TransferStatus::default();
        let rc = unsafe {
            ffi::mooncake_classic_get_batch_transfer_status(self.raw, batch_id, &mut status)
        };
        check_zero(rc, "mooncake_classic_get_batch_transfer_status")?;
        Ok(TransferProgress {
            status: decode_status(status.status),
            transferred_bytes: status.transferred_bytes,
        })
    }

    pub fn free_batch(&self, batch_id: u64) -> Result<()> {
        let rc = unsafe { ffi::freeBatchID(self.raw, batch_id) };
        check_zero(rc, "freeBatchID")
    }

    pub fn sync_segment_cache(&self) -> Result<()> {
        let rc = unsafe { ffi::syncSegmentCache(self.raw) };
        check_zero(rc, "syncSegmentCache")
    }

    pub fn republish_local_metadata(&self) -> Result<()> {
        let rc = unsafe { ffi::mooncake_classic_republish_local_metadata(self.raw) };
        check_zero(rc, "mooncake_classic_republish_local_metadata")
    }

    fn install_transport(&self, protocol: &str) -> Result<()> {
        let protocol = to_cstring("protocol", protocol)?;
        let transport =
            unsafe { ffi::installTransport(self.raw, protocol.as_ptr(), std::ptr::null_mut()) };
        if transport.is_null() {
            return Err(StoreError::Transport(format!(
                "installTransport failed for {}",
                protocol.to_string_lossy()
            )));
        }
        Ok(())
    }
}

impl Drop for ClassicTransferEngine {
    fn drop(&mut self) {
        unsafe { ffi::destroyTransferEngine(self.raw) };
    }
}

unsafe impl Send for ClassicTransferEngine {}
unsafe impl Sync for ClassicTransferEngine {}

fn classic_engine_create_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct ClassicCreateEnvGuard {
    _env: EnvOverrideGuard,
}

impl ClassicCreateEnvGuard {
    fn apply(config: &ClassicEngineConfig) -> Self {
        let mut env = EnvOverrideGuard::new();
        let slice_timeout = config
            .slice_timeout_ms_value()
            .map(|value| value.to_string());
        env.set_optional("MC_USE_TENT", None);
        env.set_optional("MC_USE_TEV1", None);
        env.set_optional(
            "MC_TCP_BIND_ADDRESS",
            (!config.rpc_bind_host().is_empty()).then_some(config.rpc_bind_host()),
        );
        env.set_optional("MC_SLICE_TIMEOUT", slice_timeout.as_deref());
        env.set_optional("MC_REDIS_USERNAME", config.redis_username_value());
        env.set_optional("MC_REDIS_PASSWORD", config.redis_password_value());
        env.set_optional("MC_REDIS_DB_INDEX", config.redis_db_index_value());
        env.set_optional("MC_GID_INDEX", config.gid_index_value());
        Self { _env: env }
    }
}

fn to_cstring(field: &str, value: &str) -> Result<CString> {
    CString::new(value)
        .map_err(|_| StoreError::Transport(format!("{field} contains an embedded NUL byte")))
}

fn read_c_buffer(buffer: &[i8]) -> String {
    let bytes = buffer
        .iter()
        .copied()
        .take_while(|byte| *byte != 0)
        .map(|byte| byte as u8)
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&bytes).into_owned()
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

fn encode_request(request: &TransferRequest) -> Result<ffi::TransferRequest> {
    let target_id = i32::try_from(request.target_id).map_err(|_| {
        StoreError::Transport(format!(
            "target_id {} does not fit classic segment id",
            request.target_id
        ))
    })?;
    Ok(ffi::TransferRequest {
        opcode: encode_opcode(request.opcode),
        source: request.source,
        target_id,
        target_offset: request.target_offset,
        length: request.length,
    })
}

fn encode_requests(requests: &[TransferRequest]) -> Result<Vec<ffi::TransferRequest>> {
    requests.iter().map(encode_request).collect()
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

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::mem::ManuallyDrop;

    use mooncake_transport_sys::classic as ffi;

    use super::{
        check_zero, classic_engine_create_lock, decode_status, encode_opcode, encode_request,
        encode_requests, read_c_buffer, to_cstring, ClassicCreateEnvGuard, ClassicEngineConfig,
        ClassicTransferEngine, ClassicTransportProtocol,
    };
    use crate::{Opcode, TransferRequest, TransferStatus};

    #[test]
    fn cstring_conversion_rejects_embedded_nul() {
        assert!(to_cstring("field", "hello").is_ok());
        assert!(to_cstring("field", "bad\0value").is_err());
    }

    #[test]
    fn read_c_buffer_stops_at_first_nul() {
        let buffer = vec![b'a' as i8, b'b' as i8, 0, b'c' as i8];
        assert_eq!(read_c_buffer(&buffer), "ab");
    }

    #[test]
    fn check_zero_preserves_success_and_errors() {
        check_zero(0, "demo").expect("zero rc should succeed");
        let error = check_zero(-1, "demo").expect_err("non-zero rc should fail");
        assert!(error.to_string().contains("demo failed"));
    }

    #[test]
    fn opcode_and_status_codecs_match_ffi_constants() {
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
        assert_eq!(decode_status(i32::MAX), TransferStatus::Failed);
    }

    #[test]
    fn classic_env_guard_applies_slice_timeout_override() {
        let _guard = classic_engine_create_lock()
            .lock()
            .expect("classic engine create lock poisoned");
        std::env::remove_var("MC_SLICE_TIMEOUT");
        let config = ClassicEngineConfig::new("redis://127.0.0.1:6379/0", "127.0.0.1")
            .slice_timeout(std::time::Duration::from_millis(1234));
        {
            let _env = ClassicCreateEnvGuard::apply(&config);
            assert_eq!(std::env::var("MC_SLICE_TIMEOUT").as_deref(), Ok("1234"));
        }
        assert!(std::env::var("MC_SLICE_TIMEOUT").is_err());
    }

    #[test]
    fn classic_engine_config_tracks_protocol_and_hosts() {
        let config = ClassicEngineConfig::new("redis://127.0.0.1:6379/0", "127.0.0.1")
            .protocol(ClassicTransportProtocol::Rdma)
            .rpc_port(17111)
            .redis_username("user")
            .redis_password("pass")
            .redis_db_index("4")
            .gid_index("1");
        assert_eq!(config.metadata_uri(), "redis://127.0.0.1:6379/0");
        assert_eq!(config.rpc_bind_host(), "127.0.0.1");
        assert_eq!(config.rpc_port_value(), Some(17111));
        assert_eq!(config.transport_protocol(), ClassicTransportProtocol::Rdma);
        assert_eq!(config.redis_username_value(), Some("user"));
        assert_eq!(config.redis_password_value(), Some("pass"));
        assert_eq!(config.redis_db_index_value(), Some("4"));
        assert_eq!(config.gid_index_value(), Some("1"));
        assert!(!config.uses_p2p_handshake_metadata());
    }

    #[test]
    fn classic_engine_config_detects_p2p_handshake_metadata() {
        let config = ClassicEngineConfig::new("p2phandshake", "127.0.0.1");
        assert!(config.uses_p2p_handshake_metadata());
    }

    #[test]
    fn create_guard_temporarily_clears_tent_envs() {
        let _lock = super::classic_engine_create_lock()
            .lock()
            .expect("classic env test lock poisoned");
        std::env::set_var("MC_USE_TENT", "1");
        std::env::set_var("MC_USE_TEV1", "1");
        std::env::set_var("MC_TCP_BIND_ADDRESS", "old");
        std::env::set_var("MC_REDIS_USERNAME", "old-user");
        std::env::set_var("MC_REDIS_PASSWORD", "old-pass");
        std::env::set_var("MC_REDIS_DB_INDEX", "9");
        std::env::set_var("MC_GID_INDEX", "7");
        let config = ClassicEngineConfig::new("redis://127.0.0.1:6379", "127.0.0.1")
            .redis_username("new-user")
            .redis_password("new-pass")
            .redis_db_index("4")
            .gid_index("1");
        {
            let _guard = ClassicCreateEnvGuard::apply(&config);
            assert!(std::env::var("MC_USE_TENT").is_err());
            assert!(std::env::var("MC_USE_TEV1").is_err());
            assert_eq!(
                std::env::var("MC_TCP_BIND_ADDRESS").as_deref(),
                Ok("127.0.0.1")
            );
            assert_eq!(
                std::env::var("MC_REDIS_USERNAME").as_deref(),
                Ok("new-user")
            );
            assert_eq!(
                std::env::var("MC_REDIS_PASSWORD").as_deref(),
                Ok("new-pass")
            );
            assert_eq!(std::env::var("MC_REDIS_DB_INDEX").as_deref(), Ok("4"));
            assert_eq!(std::env::var("MC_GID_INDEX").as_deref(), Ok("1"));
        }
        assert_eq!(std::env::var("MC_USE_TENT").as_deref(), Ok("1"));
        assert_eq!(std::env::var("MC_USE_TEV1").as_deref(), Ok("1"));
        assert_eq!(std::env::var("MC_TCP_BIND_ADDRESS").as_deref(), Ok("old"));
        assert_eq!(
            std::env::var("MC_REDIS_USERNAME").as_deref(),
            Ok("old-user")
        );
        assert_eq!(
            std::env::var("MC_REDIS_PASSWORD").as_deref(),
            Ok("old-pass")
        );
        assert_eq!(std::env::var("MC_REDIS_DB_INDEX").as_deref(), Ok("9"));
        assert_eq!(std::env::var("MC_GID_INDEX").as_deref(), Ok("7"));
        std::env::remove_var("MC_USE_TENT");
        std::env::remove_var("MC_USE_TEV1");
        std::env::remove_var("MC_TCP_BIND_ADDRESS");
        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
        std::env::remove_var("MC_REDIS_DB_INDEX");
        std::env::remove_var("MC_GID_INDEX");
    }

    #[test]
    fn classic_constructor_rejects_embedded_nul_before_touching_ffi() {
        let config = ClassicEngineConfig::new("redis://ok", "127.0.0.1");
        assert!(ClassicTransferEngine::new(
            &ClassicEngineConfig::new("bad\0uri", "127.0.0.1"),
            "local"
        )
        .is_err());
        assert!(ClassicTransferEngine::new(&config, "lo\0cal").is_err());
    }

    #[test]
    fn classic_methods_validate_inputs_before_calling_ffi() {
        let engine = ManuallyDrop::new(ClassicTransferEngine {
            raw: std::ptr::null_mut(),
        });

        assert!(engine
            .register_local_memory(std::ptr::null_mut::<c_void>(), 0, "cpu:0\0", false)
            .is_err());
        assert!(engine.open_segment("bad\0segment").is_err());

        let overflow = crate::TransferRequest {
            opcode: Opcode::Write,
            source: std::ptr::null_mut(),
            target_id: (i32::MAX as u64) + 1,
            target_offset: 0,
            length: 1,
        };
        assert!(engine.submit(7, &[overflow]).is_err());
    }

    #[test]
    fn classic_request_encoding_is_explicit_and_checked() {
        let request = TransferRequest {
            opcode: Opcode::Write,
            source: std::ptr::null_mut(),
            target_id: 17,
            target_offset: 23,
            length: 29,
        };
        let native = encode_request(&request).expect("request should encode");
        assert_eq!(native.opcode, ffi::OPCODE_WRITE);
        assert_eq!(native.target_id, 17);
        assert_eq!(native.target_offset, 23);
        assert_eq!(native.length, 29);

        let read = TransferRequest {
            opcode: Opcode::Read,
            ..request
        };
        let batch = encode_requests(&[request, read]).expect("batch should encode");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[1].opcode, ffi::OPCODE_READ);

        let overflow = TransferRequest {
            opcode: Opcode::Write,
            source: std::ptr::null_mut(),
            target_id: (i32::MAX as u64) + 1,
            target_offset: 0,
            length: 1,
        };
        assert!(encode_request(&overflow).is_err());
    }
}
