use std::ffi::c_void;
use std::sync::Arc;

use mooncake_store_core::Result;
use mooncake_transport::{SegmentInfo, TransferBatchHints, TransferProgress, TransferRequest};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryRegistration {
    pub addr: *mut c_void,
    pub size: usize,
}

// ---------------------------------------------------------------------------
// StoreTransport — abstract data-plane contract for a single local segment
// ---------------------------------------------------------------------------

pub trait StoreTransport: Send + Sync {
    fn segment_name(&self) -> Result<String>;
    fn rpc_server_address(&self) -> Result<(String, u16)>;
    fn open_segment(&self, segment_name: &str) -> Result<u64>;
    fn close_segment(&self, handle: u64) -> Result<()>;
    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo>;

    fn republish_local_metadata(&self) -> Result<()> {
        Ok(())
    }

    fn local_segment_descriptor(&self) -> Result<Option<String>> {
        Ok(None)
    }

    fn cache_remote_segment_descriptor(
        &self,
        _segment_name: &str,
        _descriptor_json: &str,
    ) -> Result<()> {
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

    fn supports_parallel_startup_registration(&self) -> bool {
        false
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()>;

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()>;

    fn register_memory_batch(&self, entries: &[MemoryRegistration]) -> Result<()> {
        let mut registered = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Err(error) = self.register_memory(entry.addr, entry.size) {
                let _ = self.unregister_memory_batch(&registered);
                return Err(error);
            }
            registered.push(*entry);
        }
        Ok(())
    }

    fn register_startup_memory_batch(&self, entries: &[MemoryRegistration]) -> Result<()> {
        self.register_memory_batch(entries)
    }

    fn unregister_memory_batch(&self, entries: &[MemoryRegistration]) -> Result<()> {
        for entry in entries.iter().rev() {
            self.unregister_memory(entry.addr, entry.size)?;
        }
        Ok(())
    }

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

// ---------------------------------------------------------------------------
// StoreTransportFactory — creates one transport per local segment
// ---------------------------------------------------------------------------

pub trait StoreTransportFactory: Send + Sync {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>>;
}
