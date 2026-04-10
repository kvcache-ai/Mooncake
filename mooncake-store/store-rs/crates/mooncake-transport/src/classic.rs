use std::ffi::{c_void, CString};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport_sys::classic as ffi;

use crate::{Opcode, TransferProgress, TransferRequest, TransferStatus};

pub struct ClassicTransferEngine {
    raw: ffi::TransferEngineHandle,
}

impl ClassicTransferEngine {
    pub fn new(
        metadata_uri: &str,
        local_server_name: &str,
        ip_or_host_name: &str,
        rpc_port: u64,
    ) -> Result<Self> {
        let metadata_uri = to_cstring("metadata_uri", metadata_uri)?;
        let local_server_name = to_cstring("local_server_name", local_server_name)?;
        let ip_or_host_name = to_cstring("ip_or_host_name", ip_or_host_name)?;

        let raw = unsafe {
            ffi::createTransferEngine(
                metadata_uri.as_ptr(),
                local_server_name.as_ptr(),
                ip_or_host_name.as_ptr(),
                rpc_port,
                0,
            )
        };
        if raw.is_null() {
            return Err(StoreError::Transport(
                "createTransferEngine returned a null pointer".to_string(),
            ));
        }
        Ok(Self { raw })
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
    pub fn unregister_local_memory(&self, addr: *mut c_void) -> Result<()> {
        let rc = unsafe { ffi::unregisterLocalMemory(self.raw, addr) };
        check_zero(rc, "unregisterLocalMemory")
    }

    pub fn open_segment(&self, segment_name: &str) -> Result<i32> {
        let segment_name = to_cstring("segment_name", segment_name)?;
        let handle = unsafe { ffi::openSegment(self.raw, segment_name.as_ptr()) };
        if handle < 0 {
            return Err(StoreError::Transport(format!(
                "openSegment failed for {segment_name:?}"
            )));
        }
        Ok(handle)
    }

    pub fn close_segment(&self, segment_id: i32) -> Result<()> {
        let rc = unsafe { ffi::closeSegment(self.raw, segment_id) };
        check_zero(rc, "closeSegment")
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

    pub fn free_batch(&self, batch_id: u64) -> Result<()> {
        let rc = unsafe { ffi::freeBatchID(self.raw, batch_id) };
        check_zero(rc, "freeBatchID")
    }

    pub fn sync_segment_cache(&self) -> Result<()> {
        let rc = unsafe { ffi::syncSegmentCache(self.raw) };
        check_zero(rc, "syncSegmentCache")
    }
}

impl Drop for ClassicTransferEngine {
    fn drop(&mut self) {
        unsafe { ffi::destroyTransferEngine(self.raw) };
    }
}

fn to_cstring(field: &str, value: &str) -> Result<CString> {
    CString::new(value)
        .map_err(|_| StoreError::Transport(format!("{field} contains an embedded NUL byte")))
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
        check_zero, decode_status, encode_opcode, encode_request, encode_requests, to_cstring,
        ClassicTransferEngine,
    };
    use crate::{Opcode, TransferRequest, TransferStatus};

    #[test]
    fn cstring_conversion_rejects_embedded_nul() {
        assert!(to_cstring("field", "hello").is_ok());
        assert!(to_cstring("field", "bad\0value").is_err());
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
    fn classic_constructor_rejects_embedded_nul_before_touching_ffi() {
        assert!(ClassicTransferEngine::new("bad\0uri", "local", "127.0.0.1", 1).is_err());
        assert!(ClassicTransferEngine::new("redis://ok", "lo\0cal", "127.0.0.1", 1).is_err());
        assert!(ClassicTransferEngine::new("redis://ok", "local", "127.0.0.1\0", 1).is_err());
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
