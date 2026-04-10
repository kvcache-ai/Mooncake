use std::ffi::c_void;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport::{
    SegmentInfo, TentEngine, TentEngineConfig, TransferProgress, TransferRequest, TransferStatus,
};

pub trait StoreTransport: Send + Sync {
    fn segment_name(&self) -> Result<String>;
    fn rpc_server_address(&self) -> Result<(String, u16)>;
    fn open_segment(&self, segment_name: &str) -> Result<u64>;
    fn close_segment(&self, handle: u64) -> Result<()>;
    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo>;
    fn adopt_local_memory(&self, _addr: *mut c_void, _size: usize, _location: &str) -> Result<()> {
        Ok(())
    }
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

pub trait StoreTransportFactory: Send + Sync {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>>;
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ffi::c_void;
    use std::sync::Arc;
    use std::time::Duration;

    use mooncake_store_core::{Result, StoreError};
    use mooncake_transport::{SegmentInfo, TransferProgress, TransferRequest, TransferStatus};
    use parking_lot::Mutex;

    use super::{wait_for_batch_completion, StoreTransport};

    struct ScriptedTransport {
        statuses: Arc<Mutex<VecDeque<TransferStatus>>>,
    }

    impl ScriptedTransport {
        fn new(statuses: impl IntoIterator<Item = TransferStatus>) -> Self {
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

        fn task_status(&self, batch_id: u64, _task_id: usize) -> Result<TransferProgress> {
            self.overall_status(batch_id)
        }

        fn overall_status(&self, _batch_id: u64) -> Result<TransferProgress> {
            let status = self
                .statuses
                .lock()
                .pop_front()
                .unwrap_or(TransferStatus::Completed);
            Ok(TransferProgress {
                status,
                transferred_bytes: 0,
            })
        }
    }

    #[test]
    fn wait_for_batch_completion_returns_on_completed_status() {
        let transport = ScriptedTransport::new([
            TransferStatus::Waiting,
            TransferStatus::Pending,
            TransferStatus::Completed,
        ]);

        wait_for_batch_completion(&transport, 7, Duration::from_millis(20))
            .expect("completed batch should succeed");
        transport
            .adopt_local_memory(std::ptr::null_mut(), 0, "cpu:0")
            .expect("default adopt_local_memory should be a no-op");
    }

    #[test]
    fn wait_for_batch_completion_reports_terminal_failures() {
        let transport = ScriptedTransport::new([TransferStatus::Failed]);

        let error = wait_for_batch_completion(&transport, 9, Duration::from_millis(20))
            .expect_err("failed batch should surface transport error");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(error.to_string().contains("failed"));
    }

    #[test]
    fn wait_for_batch_completion_times_out_when_progress_never_completes() {
        let transport = ScriptedTransport::new([
            TransferStatus::Waiting,
            TransferStatus::Pending,
            TransferStatus::Pending,
            TransferStatus::Pending,
        ]);

        let error = wait_for_batch_completion(&transport, 11, Duration::from_millis(3))
            .expect_err("stuck batch should time out");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(error.to_string().contains("timed out"));
    }

    #[test]
    fn scripted_transport_exposes_contract_methods_for_smoke_coverage() {
        let transport = ScriptedTransport::new([TransferStatus::Completed]);
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
}
