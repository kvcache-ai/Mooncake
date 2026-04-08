use std::ffi::c_void;
use std::thread;
use std::time::{Duration, Instant};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport::{TentEngine, TransferProgress, TransferRequest, TransferStatus};

pub trait StoreTransport {
    fn segment_name(&self) -> Result<String>;
    fn rpc_server_address(&self) -> Result<(String, u16)>;
    fn open_segment(&self, segment_name: &str) -> Result<u64>;
    fn close_segment(&self, handle: u64) -> Result<()>;
    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void>;
    fn free_memory(&self, addr: *mut c_void) -> Result<()>;
    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()>;
    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()>;
    fn allocate_batch(&self, batch_size: usize) -> Result<u64>;
    fn free_batch(&self, batch_id: u64) -> Result<()>;
    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()>;
    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress>;
    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress>;
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

    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        TentEngine::allocate_memory(self, size, location)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        TentEngine::free_memory(self, addr)
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        TentEngine::register_memory(self, addr, size)
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        TentEngine::unregister_memory(self, addr, size)
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

    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        TentEngine::task_status(self, batch_id, task_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        TentEngine::overall_status(self, batch_id)
    }
}

pub fn wait_for_batch_completion(
    transport: &dyn StoreTransport,
    batch_id: u64,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = transport.overall_status(batch_id)?;
        match status.status {
            TransferStatus::Completed => return Ok(()),
            TransferStatus::Failed
            | TransferStatus::Canceled
            | TransferStatus::Invalid
            | TransferStatus::Timeout => {
                return Err(StoreError::Transport(format!(
                    "transport batch {batch_id} failed with status {:?}",
                    status.status
                )));
            }
            TransferStatus::Waiting | TransferStatus::Pending => {}
        }
        if Instant::now() >= deadline {
            return Err(StoreError::Transport(format!(
                "transport batch {batch_id} timed out after {:?}",
                timeout
            )));
        }
        thread::sleep(Duration::from_millis(2));
    }
}
