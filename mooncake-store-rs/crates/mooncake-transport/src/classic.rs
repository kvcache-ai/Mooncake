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
        let mut native_requests = requests
            .iter()
            .map(|request| {
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
            })
            .collect::<Result<Vec<_>>>()?;

        let rc =
            unsafe { ffi::submitTransfer(self.raw, batch_id, native_requests.as_mut_ptr(), native_requests.len()) };
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
        Err(StoreError::Transport(format!("{operation} failed with rc={rc}")))
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
