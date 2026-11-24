use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::{ClientLifecycleState, Result, SegmentName, StoreError};
use mooncake_store_rs_metadata::InMemoryMetadataBackend;
use mooncake_store_rs_transport::{SegmentInfo, TransferProgress, TransferRequest, TransferStatus};
use mooncake_store_test_utils::transport::TestTransport;
use parking_lot::{Condvar, Mutex};

use crate::transport::StoreTransport;
use crate::{
    BandwidthShaping, MooncakeCompatibilityFacade, PlacementPlanner, StoreClient,
    StoreClientBuilder,
};

use super::{
    storage_config_with_bytes, storage_config_with_layout, test_future_expiry_ms,
    ReplicaWriteTarget,
};

const SCRATCH_BYTES: usize = 256;

#[derive(Clone, Copy, PartialEq)]
enum DelayPoint {
    Submit,
    Completion,
    Free,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum FailurePoint {
    Allocate,
    Submit,
    Poll,
    Timeout,
    Free,
}

#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Gate {
    fn pause(&self) {
        let mut state = self.state.lock();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            let timeout = self.changed.wait_for(&mut state, Duration::from_secs(10));
            assert!(
                !timeout.timed_out() || state.1,
                "test did not release the delayed transfer"
            );
        }
    }

    fn wait_until_paused(&self) -> bool {
        let mut state = self.state.lock();
        while !state.0 {
            if self
                .changed
                .wait_for(&mut state, Duration::from_secs(10))
                .timed_out()
                && !state.0
            {
                return false;
            }
        }
        true
    }

    fn release(&self) {
        self.state.lock().1 = true;
        self.changed.notify_all();
    }
}

struct DelayedTransport {
    inner: TestTransport,
    point: DelayPoint,
    armed: AtomicBool,
    gate: Arc<Gate>,
    failure: Mutex<Option<FailurePoint>>,
}

impl DelayedTransport {
    fn pause_once(&self, point: DelayPoint) {
        if self.point == point && self.armed.swap(false, Ordering::SeqCst) {
            self.gate.pause();
        }
    }
}

impl StoreTransport for DelayedTransport {
    fn segment_name(&self) -> Result<String> {
        self.inner.segment_name()
    }

    fn rpc_server_address(&self) -> Result<(String, u16)> {
        self.inner.rpc_server_address()
    }

    fn open_segment(&self, name: &str) -> Result<u64> {
        self.inner.open_segment(name)
    }

    fn close_segment(&self, handle: u64) -> Result<()> {
        self.inner.close_segment(handle)
    }

    fn get_segment_info(&self, handle: u64) -> Result<SegmentInfo> {
        self.inner.get_segment_info(handle)
    }

    fn allocate_memory(&self, size: usize, location: &str) -> Result<*mut c_void> {
        self.inner.allocate_memory(size, location)
    }

    fn free_memory(&self, addr: *mut c_void) -> Result<()> {
        self.inner.free_memory(addr)
    }

    fn max_registration_bytes(&self) -> Option<usize> {
        self.inner.max_registration_bytes()
    }

    fn register_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        self.inner.register_memory(addr, size)
    }

    fn unregister_memory(&self, addr: *mut c_void, size: usize) -> Result<()> {
        self.inner.unregister_memory(addr, size)
    }

    fn allocate_batch(&self, size: usize) -> Result<u64> {
        if *self.failure.lock() == Some(FailurePoint::Allocate) {
            return Err(StoreError::Transport("injected allocation failure".into()));
        }
        self.inner.allocate_batch(size)
    }

    fn free_batch(&self, batch: u64) -> Result<()> {
        self.pause_once(DelayPoint::Free);
        self.inner.free_batch(batch)?;
        if *self.failure.lock() == Some(FailurePoint::Free) {
            return Err(StoreError::Transport("injected cleanup failure".into()));
        }
        Ok(())
    }

    fn submit(&self, batch: u64, requests: &[TransferRequest]) -> Result<()> {
        // Consume the source only after another PUT has had a chance to stage
        // its payload. The default TestTransport copies synchronously and
        // otherwise hides reuse of a released scratch reservation.
        self.pause_once(DelayPoint::Submit);
        if *self.failure.lock() == Some(FailurePoint::Submit) {
            return Err(StoreError::Transport("injected submit failure".into()));
        }
        self.inner.submit(batch, requests)
    }

    fn task_status(&self, batch: u64, task: usize) -> Result<TransferProgress> {
        self.inner.task_status(batch, task)
    }

    fn overall_status(&self, batch: u64) -> Result<TransferProgress> {
        self.pause_once(DelayPoint::Completion);
        match *self.failure.lock() {
            Some(FailurePoint::Poll) => {
                return Err(StoreError::Transport("injected poll failure".into()))
            }
            Some(FailurePoint::Timeout) => {
                return Ok(TransferProgress {
                    status: TransferStatus::Pending,
                    transferred_bytes: 0,
                })
            }
            _ => {}
        }
        self.inner.overall_status(batch)
    }
}

fn clients(point: DelayPoint) -> (StoreClient, StoreClient, Arc<DelayedTransport>) {
    let metadata = Arc::new(InMemoryMetadataBackend::new());
    let base_transport = TestTransport::new("scratch-owner-segment");
    let gate = Arc::new(Gate::default());
    let transport = Arc::new(DelayedTransport {
        inner: base_transport.peer("scratch-writer-segment"),
        point,
        armed: AtomicBool::new(true),
        gate: gate.clone(),
        failure: Mutex::new(None),
    });
    let owner = StoreClientBuilder::new(metadata.clone(), "scratch-owner")
        .state(ClientLifecycleState::Active)
        .label("storage", "true")
        .segment_name("scratch-owner-segment")
        .transport(Arc::new(base_transport))
        .local_memory(storage_config_with_bytes(4096))
        .build(test_future_expiry_ms())
        .unwrap();
    owner.register_local_memory().unwrap();
    let writer = StoreClientBuilder::new(metadata.clone(), "scratch-writer")
        .state(ClientLifecycleState::Active)
        .label("storage", "false")
        .segment_name("scratch-writer-segment")
        .transport(transport.clone())
        .local_memory(storage_config_with_layout(0, SCRATCH_BYTES, 1))
        .routed_writes(
            PlacementPlanner::new(metadata).require_label("storage", "true"),
            1,
        )
        .bandwidth_shaping(BandwidthShaping::new().max_remote_batch_burst_items(1))
        .request_timeout(Duration::from_secs(15))
        .build(test_future_expiry_ms())
        .unwrap();
    writer.register_local_memory().unwrap();
    (owner, writer, transport)
}

fn overlapping_puts(point: DelayPoint, split_transfers: bool) {
    let (owner, writer, transport) = clients(point);
    let gate = &transport.gate;
    // Change the request split cap after registration so the scratch pool is
    // still a single region; the 64-byte payload spans four transfer requests.
    if split_transfers {
        transport.inner.set_max_registration_bytes(Some(16));
    }
    let first = [0x31; 64];
    let second = [0xa7; 64];
    let (paused, reserved, first_result, second_result) = std::thread::scope(|scope| {
        let first_write = scope.spawn(|| writer.put("first", &first));
        let paused = gate.wait_until_paused();
        let reserved = writer
            .state
            .lock()
            .memory_ref()
            .unwrap()
            .plan_scratch(&[SCRATCH_BYTES])
            .is_err();
        let second_result = writer.put("second", &second);
        gate.release();
        (paused, reserved, first_write.join().unwrap(), second_result)
    });
    assert!(paused, "first PUT must reach the controlled delay");
    first_result.expect("first PUT should complete");
    second_result.expect("second PUT should complete while the first is delayed");
    // The synthetic split cap must not change the owner's view of the memory
    // registered before the test enabled request splitting.
    transport.inner.set_max_registration_bytes(None);
    assert_eq!(owner.get("first").unwrap(), first);
    assert_eq!(owner.get("second").unwrap(), second);
    assert!(
        reserved,
        "in-flight PUT must retain its scratch reservation"
    );
    writer
        .state
        .lock()
        .memory_ref()
        .unwrap()
        .plan_scratch(&[SCRATCH_BYTES])
        .expect("completed writes must release all scratch space");
    if split_transfers {
        assert!(transport.inner.submitted_batch_sizes().len() >= 8);
    }
}

#[test]
fn concurrent_remote_put_keeps_scratch_until_submit_consumes_it() {
    overlapping_puts(DelayPoint::Submit, false);
}

#[test]
fn concurrent_remote_put_keeps_scratch_until_completion() {
    overlapping_puts(DelayPoint::Completion, false);
}

#[test]
fn chunked_remote_put_keeps_scratch_through_batch_cleanup() {
    overlapping_puts(DelayPoint::Free, true);
}

#[test]
fn remote_put_releases_scratch_on_error_return() {
    // This transport does not leave asynchronous I/O behind: even its timeout
    // mode has already copied the payload. This checks reservation cleanup,
    // not the native engines' deferred-cleanup/timeout safety contract.
    for (failure, expected) in [
        (FailurePoint::Allocate, "injected allocation failure"),
        (FailurePoint::Submit, "injected submit failure"),
        (FailurePoint::Poll, "injected poll failure"),
        (FailurePoint::Timeout, "stalled"),
        (FailurePoint::Free, "injected cleanup failure"),
    ] {
        let (owner, mut writer, transport) = clients(DelayPoint::Submit);
        transport.armed.store(false, Ordering::SeqCst);
        writer.transfer_stall_timeout = Duration::from_millis(5);
        let target = ReplicaWriteTarget {
            storage_runtime: owner.runtime_id().clone(),
            segment_name: SegmentName::new("scratch-owner-segment"),
            transport_endpoint: None,
            target_chunks: Vec::new(),
        };
        let reservation = owner
            .allocator
            .lock()
            .reserve_any(owner.runtime_id(), 64)
            .unwrap();
        *transport.failure.lock() = Some(failure);
        let error = writer
            .write_reserved_replicas(&[target], &[reservation], &[7; 64], None)
            .expect_err("the injected transfer failure should reach the caller");
        assert!(
            matches!(&error, StoreError::Transport(message) if message.contains(expected)),
            "expected {failure:?}, got {error:?}"
        );
        *transport.failure.lock() = None;
        writer
            .state
            .lock()
            .memory_ref()
            .unwrap()
            .plan_scratch(&[SCRATCH_BYTES])
            .expect("an error return must release its scratch reservation");
        assert!(transport.inner.state.lock().live_batches.is_empty());
    }
}

#[test]
fn registered_remote_put_does_not_reserve_scratch() {
    let (owner, writer, transport) = clients(DelayPoint::Submit);
    transport.armed.store(false, Ordering::SeqCst);
    let scratch = writer
        .state
        .lock()
        .memory_ref()
        .unwrap()
        .plan_scratch(&[SCRATCH_BYTES])
        .unwrap();
    let mut payload = [0x53; 64];
    writer
        .register_buffer(payload.as_mut_ptr().cast(), payload.len())
        .unwrap();
    let result = writer.put_from("registered", payload.as_ptr().cast(), payload.len());
    writer
        .unregister_buffer(payload.as_mut_ptr().cast(), payload.len())
        .unwrap();
    result.expect("registered sources should work with no available scratch space");
    assert_eq!(owner.get("registered").unwrap(), payload);
    drop(scratch);
}
