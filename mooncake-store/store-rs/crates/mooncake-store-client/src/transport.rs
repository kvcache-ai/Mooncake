use std::collections::BTreeMap;
use std::ffi::c_void;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use mooncake_store_core::{Result, StoreError};
use mooncake_transport::{
    ClassicEngineConfig, ClassicTransferEngine, SegmentBuffer, SegmentInfo, SegmentKind,
    TentEngine, TentEngineConfig, TransferProgress, TransferRequest, TransferStatus,
};
use parking_lot::{Mutex, RwLock};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BatchWaitFailureKind {
    PollFailed,
    TerminalStatus(TransferStatus),
    StallTimeout,
    RequestDeadlineExceeded,
}

#[derive(Clone, Debug)]
pub(crate) struct BatchWaitError {
    kind: BatchWaitFailureKind,
    error: StoreError,
}

impl BatchWaitError {
    fn poll_failed(batch_id: u64, error: StoreError) -> Self {
        Self {
            kind: BatchWaitFailureKind::PollFailed,
            error: StoreError::Transport(format!(
                "transport batch {batch_id} status poll failed: {error}"
            )),
        }
    }

    fn terminal_status(batch_id: u64, status: TransferStatus, transferred_bytes: u64) -> Self {
        let kind = match status {
            TransferStatus::Timeout => BatchWaitFailureKind::StallTimeout,
            _ => BatchWaitFailureKind::TerminalStatus(status),
        };
        let message = match status {
            TransferStatus::Timeout => format!(
                "transport batch {batch_id} stalled after {} byte(s) without progress",
                transferred_bytes
            ),
            _ => format!(
                "transport batch {batch_id} failed with status {:?} after {} byte(s)",
                status, transferred_bytes
            ),
        };
        Self {
            kind,
            error: StoreError::Transport(message),
        }
    }

    fn stall_timeout(batch_id: u64, stall_timeout: Duration, transferred_bytes: u64) -> Self {
        Self {
            kind: BatchWaitFailureKind::StallTimeout,
            error: StoreError::Transport(format!(
                "transport batch {batch_id} stalled for {}ms after {} byte(s)",
                stall_timeout.as_millis(),
                transferred_bytes
            )),
        }
    }

    fn request_deadline_exceeded(batch_id: u64, elapsed: Duration, transferred_bytes: u64) -> Self {
        Self {
            kind: BatchWaitFailureKind::RequestDeadlineExceeded,
            error: StoreError::Transport(format!(
                "request deadline exceeded for transport batch {batch_id} after {}ms with {} byte(s) transferred",
                elapsed.as_millis(),
                transferred_bytes
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn kind(&self) -> BatchWaitFailureKind {
        self.kind
    }

    pub(crate) fn marks_runtime_suspect(&self) -> bool {
        !matches!(self.kind, BatchWaitFailureKind::RequestDeadlineExceeded)
    }
}

impl From<BatchWaitError> for StoreError {
    fn from(error: BatchWaitError) -> Self {
        error.error
    }
}

pub trait StoreTransport: Send + Sync {
    fn segment_name(&self) -> Result<String>;
    fn rpc_server_address(&self) -> Result<(String, u16)>;
    fn open_segment(&self, segment_name: &str) -> Result<u64>;
    fn close_segment(&self, handle: u64) -> Result<()>;
    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo>;
    fn republish_local_metadata(&self) -> Result<()> {
        Ok(())
    }
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

#[derive(Clone, Debug)]
pub struct ClassicTeTransportFactory {
    config: ClassicEngineConfig,
}

impl ClassicTeTransportFactory {
    pub fn new(config: ClassicEngineConfig) -> Self {
        Self { config }
    }
}

impl StoreTransportFactory for ClassicTeTransportFactory {
    fn create(&self, segment_name: &str) -> Result<Arc<dyn StoreTransport>> {
        Ok(Arc::new(ClassicTeTransport::new(
            self.config.clone(),
            segment_name,
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

    fn republish_local_metadata(&self) -> Result<()> {
        TentEngine::republish_local_metadata(self)
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

#[derive(Clone, Debug)]
struct ClassicAllocationRecord {
    location: String,
    size: usize,
    owned: bool,
}

pub struct ClassicTeTransport {
    engine: RwLock<ClassicTransferEngine>,
    config: ClassicEngineConfig,
    segment_name: String,
    allocations: Mutex<BTreeMap<usize, ClassicAllocationRecord>>,
}

impl ClassicTeTransport {
    fn new(config: ClassicEngineConfig, segment_name: &str) -> Result<Self> {
        Ok(Self {
            engine: RwLock::new(ClassicTransferEngine::new(&config, segment_name)?),
            config,
            segment_name: segment_name.to_string(),
            allocations: Mutex::new(BTreeMap::new()),
        })
    }

    fn rebuild_engine_for_metadata_recovery(&self) -> Result<()> {
        let allocations = self
            .allocations
            .lock()
            .iter()
            .map(|(addr, record)| (*addr, record.clone()))
            .collect::<Vec<_>>();
        let replacement = ClassicTransferEngine::new(&self.config, &self.segment_name)?;
        for (addr, record) in &allocations {
            replacement.register_local_memory(
                *addr as *mut c_void,
                record.size,
                &record.location,
                true,
            )?;
        }
        let mut engine = self.engine.write();
        let previous = std::mem::replace(&mut *engine, replacement);
        drop(previous);
        engine.republish_local_metadata()
    }
}

impl StoreTransport for ClassicTeTransport {
    fn segment_name(&self) -> Result<String> {
        Ok(self.segment_name.clone())
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        let engine = self.engine.read();
        parse_rpc_server_address(&engine.rpc_server_address_text()?)
    }

    fn open_segment(&self, segment_name: &str) -> Result<u64> {
        let engine = self.engine.read();
        Ok(engine.open_segment(segment_name)? as u64)
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        let handle = i32::try_from(handle).map_err(|_| {
            StoreError::Transport(format!("classic segment handle {handle} does not fit i32"))
        })?;
        self.engine.read().close_segment(handle)
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        let handle = i32::try_from(handle).map_err(|_| {
            StoreError::Transport(format!("classic segment handle {handle} does not fit i32"))
        })?;
        let engine = self.engine.read();
        let (base, length) = engine.first_buffer(handle)?;
        Ok(SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: vec![SegmentBuffer {
                base,
                length,
                location: "cpu:0".to_string(),
            }],
        })
    }

    fn republish_local_metadata(&self) -> Result<()> {
        self.rebuild_engine_for_metadata_recovery()
    }

    fn adopt_local_memory(&self, addr: *mut c_void, _size: usize, location: &str) -> Result<()> {
        self.allocations
            .lock()
            .entry(addr as usize)
            .or_insert_with(|| ClassicAllocationRecord {
                location: location.to_string(),
                size: _size,
                owned: false,
            });
        Ok(())
    }

    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        let alignment = default_classic_alignment();
        let mut addr = std::ptr::null_mut();
        let rc = unsafe { libc::posix_memalign(&mut addr, alignment, size.max(1)) };
        if rc != 0 {
            return Err(StoreError::Allocator(format!(
                "classic posix_memalign failed with rc={rc} size={size} alignment={alignment}"
            )));
        }
        self.allocations.lock().insert(
            addr as usize,
            ClassicAllocationRecord {
                location: location.to_string(),
                size,
                owned: true,
            },
        );
        Ok(addr)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        let record = self
            .allocations
            .lock()
            .remove(&(addr as usize))
            .ok_or_else(|| {
                StoreError::Allocator(format!(
                    "classic transport does not own allocation {:p}",
                    addr
                ))
            })?;
        if record.owned {
            unsafe { libc::free(addr) };
        }
        Ok(())
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        let location = {
            let mut allocations = self.allocations.lock();
            allocations
                .entry(addr as usize)
                .or_insert_with(|| ClassicAllocationRecord {
                    location: "cpu:0".to_string(),
                    size,
                    owned: false,
                })
                .location
                .clone()
        };
        self.engine
            .read()
            .register_local_memory(addr, size, &location, true)
    }

    fn unregister_memory(&self, addr: *mut c_void, _size: usize) -> Result<()> {
        self.engine.read().unregister_local_memory(addr)
    }

    fn allocate_batch(&self, batch_size: usize) -> Result<u64> {
        self.engine.read().allocate_batch(batch_size)
    }

    fn free_batch(&self, batch_id: u64) -> Result<()> {
        self.engine.read().free_batch(batch_id)
    }

    fn submit(&self, batch_id: u64, requests: &[TransferRequest]) -> Result<()> {
        self.engine.read().submit(batch_id, requests)
    }

    fn task_status(&self, batch_id: u64, task_id: usize) -> Result<TransferProgress> {
        self.engine.read().task_status(batch_id, task_id)
    }

    fn overall_status(&self, batch_id: u64) -> Result<TransferProgress> {
        self.engine.read().batch_status(batch_id)
    }
}

fn default_classic_alignment() -> usize {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page <= 0 {
        4096
    } else {
        usize::try_from(page).unwrap_or(4096).max(64)
    }
}

fn parse_rpc_server_address(text: &str) -> Result<(String, u16)> {
    let trimmed = text.trim();
    if let Some(inner) = trimmed.strip_prefix('[') {
        if let Some((host, suffix)) = inner.split_once(']') {
            let port = suffix
                .strip_prefix(':')
                .and_then(|value| value.parse::<u16>().ok())
                .unwrap_or_default();
            return Ok((host.to_string(), port));
        }
    }
    if trimmed.matches(':').count() == 1 {
        if let Some((host, port)) = trimmed.rsplit_once(':') {
            if let Ok(port) = port.parse::<u16>() {
                return Ok((host.to_string(), port));
            }
        }
    }
    Ok((trimmed.to_string(), 0))
}

pub fn wait_for_batch_completion(
    transport: &dyn StoreTransport,
    batch_id: u64,
    timeout: Duration,
) -> Result<()> {
    wait_for_batch_completion_detailed(transport, batch_id, timeout, Instant::now() + timeout)
        .map_err(StoreError::from)
}

pub(crate) fn wait_for_batch_completion_detailed(
    transport: &dyn StoreTransport,
    batch_id: u64,
    stall_timeout: Duration,
    request_deadline: Instant,
) -> std::result::Result<(), BatchWaitError> {
    let started = Instant::now();
    let mut last_progress_at = started;
    let mut last_transferred_bytes = 0u64;
    loop {
        let progress = transport
            .overall_status(batch_id)
            .map_err(|error| BatchWaitError::poll_failed(batch_id, error))?;
        let now = Instant::now();
        if progress.transferred_bytes > last_transferred_bytes {
            last_transferred_bytes = progress.transferred_bytes;
            last_progress_at = now;
        }
        match progress.status {
            TransferStatus::Completed => return Ok(()),
            TransferStatus::Failed
            | TransferStatus::Canceled
            | TransferStatus::Invalid
            | TransferStatus::Timeout => {
                return Err(BatchWaitError::terminal_status(
                    batch_id,
                    progress.status,
                    last_transferred_bytes,
                ));
            }
            TransferStatus::Waiting | TransferStatus::Pending => {}
        }
        if now >= request_deadline {
            return Err(BatchWaitError::request_deadline_exceeded(
                batch_id,
                started.elapsed(),
                last_transferred_bytes,
            ));
        }
        if now.duration_since(last_progress_at) >= stall_timeout {
            return Err(BatchWaitError::stall_timeout(
                batch_id,
                stall_timeout,
                last_transferred_bytes,
            ));
        }
        thread::sleep(Duration::from_millis(2));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::ffi::c_void;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use mooncake_store_core::{Result, StoreError};
    use mooncake_transport::{SegmentInfo, TransferProgress, TransferRequest, TransferStatus};
    use parking_lot::Mutex;

    use super::{
        parse_rpc_server_address, wait_for_batch_completion, wait_for_batch_completion_detailed,
        BatchWaitFailureKind, StoreTransport,
    };

    struct ScriptedTransport {
        statuses: Arc<Mutex<VecDeque<TransferProgress>>>,
    }

    impl ScriptedTransport {
        fn new(statuses: impl IntoIterator<Item = TransferProgress>) -> Self {
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
            let progress = self
                .statuses
                .lock()
                .pop_front()
                .unwrap_or(TransferProgress {
                    status: TransferStatus::Completed,
                    transferred_bytes: 0,
                });
            Ok(progress)
        }
    }

    #[test]
    fn wait_for_batch_completion_returns_on_completed_status() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Waiting,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            },
        ]);

        wait_for_batch_completion(&transport, 7, Duration::from_millis(20))
            .expect("completed batch should succeed");
        transport
            .adopt_local_memory(std::ptr::null_mut(), 0, "cpu:0")
            .expect("default adopt_local_memory should be a no-op");
    }

    #[test]
    fn wait_for_batch_completion_reports_terminal_failures() {
        let transport = ScriptedTransport::new([TransferProgress {
            status: TransferStatus::Failed,
            transferred_bytes: 13,
        }]);

        let error = wait_for_batch_completion(&transport, 9, Duration::from_millis(20))
            .expect_err("failed batch should surface transport error");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(error.to_string().contains("failed"));
    }

    #[test]
    fn wait_for_batch_completion_times_out_when_progress_never_completes() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Waiting,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 0,
            },
        ]);

        let error = wait_for_batch_completion(&transport, 11, Duration::from_millis(3))
            .expect_err("stuck batch should time out");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(
            error.to_string().contains("stalled")
                || error.to_string().contains("request deadline exceeded")
        );
    }

    #[test]
    fn wait_for_batch_completion_uses_request_deadline_when_progress_keeps_advancing() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 1,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 2,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 3,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 4,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 5,
            },
        ]);

        let error = wait_for_batch_completion_detailed(
            &transport,
            15,
            Duration::from_millis(3),
            Instant::now() + Duration::from_millis(7),
        )
        .expect_err("steady progress should consume request budget instead of tripping stall");
        assert_eq!(error.kind(), BatchWaitFailureKind::RequestDeadlineExceeded);
        assert!(!error.marks_runtime_suspect());
        let error = StoreError::from(error);
        assert!(error.to_string().contains("request deadline exceeded"));
    }

    #[test]
    fn wait_for_batch_completion_allows_steady_progress_past_stall_timeout() {
        let transport = ScriptedTransport::new([
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 1,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 2,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 3,
            },
            TransferProgress {
                status: TransferStatus::Pending,
                transferred_bytes: 4,
            },
            TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 4,
            },
        ]);

        wait_for_batch_completion_detailed(
            &transport,
            17,
            Duration::from_millis(3),
            Instant::now() + Duration::from_millis(16),
        )
        .expect("steady progress should keep the stall watchdog from firing");
    }

    #[test]
    fn scripted_transport_exposes_contract_methods_for_smoke_coverage() {
        let transport = ScriptedTransport::new([TransferProgress {
            status: TransferStatus::Completed,
            transferred_bytes: 0,
        }]);
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

    #[test]
    fn rpc_server_address_parser_handles_ipv4_ipv6_and_bare_hosts() {
        assert_eq!(
            parse_rpc_server_address("127.0.0.1:17111").expect("ipv4 should parse"),
            ("127.0.0.1".to_string(), 17111)
        );
        assert_eq!(
            parse_rpc_server_address("[::1]:17112").expect("ipv6 should parse"),
            ("::1".to_string(), 17112)
        );
        assert_eq!(
            parse_rpc_server_address("runtime-a").expect("bare host should parse"),
            ("runtime-a".to_string(), 0)
        );
    }
}
