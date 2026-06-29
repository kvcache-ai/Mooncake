use super::*;

#[cfg(feature = "gds")]
use std::ffi::CString;
use std::fs::{File as ExtentStoreFile, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::ops::DerefMut;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path as ExtentStorePath, PathBuf as ExtentStorePathBuf};
use std::ptr::NonNull;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::mpsc::{channel as extent_store_channel, Sender};
use std::thread::{self, JoinHandle};

use io_uring::{opcode, squeue, types, IoUring};
use parking_lot::Condvar;

const EXTENT_STORE_LOCATOR_PREFIX: &str = "extent-store-v1";
const EXTENT_STORE_MAGIC: u32 = 0x4d45_5354; // MEST
const EXTENT_STORE_HEADER_LEN: usize = 64;
const EXTENT_STORE_RECORD_KIND_SINGLE: u16 = 0;
const EXTENT_STORE_RECORD_KIND_PACKED: u16 = 1;
const EXTENT_STORE_DENSE_RECORD_MAX_VALUE_LEN: u64 = 1024 * 1024;
const EXTENT_STORE_DENSE_RECORD_MAX_VALUE_BYTES_ENV: &str =
    "MC_STORE_RS_EXTENT_DENSE_RECORD_MAX_VALUE_BYTES";
const EXTENT_STORE_DENSE_COALESCE_MAX_VALUE_LEN: u64 = 4 * 1024;
const EXTENT_STORE_PACKED_BLOCK_TARGET_BYTES: u64 = 320 * 1024;
const EXTENT_STORE_PACKED_BLOCK_MAX_ENTRIES: u64 = 64;
const EXTENT_STORE_PACKED_BLOCK_MAX_PAYLOAD_BYTES: u64 = EXTENT_STORE_PACKED_BLOCK_TARGET_BYTES / 2;
const EXTENT_STORE_PACKED_ENTRY_INDEX_LEN: u64 = 24;
const EXTENT_STORE_DIRECT_SCRATCH_DEFAULT_MAX_RECORD_LEN: u64 = 16 * 1024;
const EXTENT_STORE_DIRECT_SCRATCH_DEFAULT_MAX_READ_LEN: u64 = 16 * 1024;
const EXTENT_STORE_DIRECT_SCRATCH_MAX_RECORD_LEN: u64 = 16 * 1024 * 1024;
const EXTENT_STORE_DIRECT_SCRATCH_MAX_WRITE_BYTES_ENV: &str =
    "MC_STORE_RS_EXTENT_DIRECT_SCRATCH_MAX_WRITE_BYTES";
const EXTENT_STORE_DIRECT_READS_ENV: &str = "MC_STORE_RS_EXTENT_DIRECT_READS";
const EXTENT_STORE_DIRECT_SCRATCH_MAX_READ_BYTES_ENV: &str =
    "MC_STORE_RS_EXTENT_DIRECT_SCRATCH_MAX_READ_BYTES";
const EXTENT_STORE_TAIL_TRACE_MS_ENV: &str = "MC_STORE_RS_EXTENT_TAIL_TRACE_MS";
const EXTENT_STORE_PARALLEL_BUFFERED_READS_ENV: &str = "MC_STORE_RS_EXTENT_PARALLEL_BUFFERED_READS";
const EXTENT_STORE_PINNED_READS_ENV: &str = "MC_STORE_RS_EXTENT_PINNED_READS";
const EXTENT_STORE_PINNED_MIN_VALUE_BYTES_ENV: &str = "MC_STORE_RS_EXTENT_PINNED_MIN_VALUE_BYTES";
const EXTENT_STORE_PINNED_MAX_MMAP_SEGMENTS_ENV: &str =
    "MC_STORE_RS_EXTENT_PINNED_MAX_MMAP_SEGMENTS";
const EXTENT_STORE_PINNED_MAX_MMAP_BYTES_ENV: &str = "MC_STORE_RS_EXTENT_PINNED_MAX_MMAP_BYTES";
const EXTENT_STORE_PINNED_MAX_ACTIVE_REFS_ENV: &str = "MC_STORE_RS_EXTENT_PINNED_MAX_ACTIVE_REFS";
const EXTENT_STORE_PINNED_MAX_ACTIVE_BYTES_ENV: &str = "MC_STORE_RS_EXTENT_PINNED_MAX_ACTIVE_BYTES";
const EXTENT_STORE_PINNED_PREFETCH_ENV: &str = "MC_STORE_RS_EXTENT_PINNED_PREFETCH";
const EXTENT_STORE_PINNED_PREFETCH_MAX_GAP_BYTES: u64 = 64 * 1024;
const EXTENT_STORE_PINNED_COLD_ORDINARY_FALLBACK_ENV: &str =
    "MC_STORE_RS_EXTENT_PINNED_COLD_ORDINARY_FALLBACK";
const EXTENT_STORE_PINNED_COLD_ORDINARY_MAX_GAP_BYTES: u64 = 64 * 1024;
const EXTENT_STORE_PINNED_COLD_ORDINARY_MIN_BATCH_BYTES: u64 = 8 * 1024 * 1024;
const EXTENT_STORE_SEGMENT_PREALLOCATE_ENV: &str = "MC_STORE_RS_EXTENT_SEGMENT_PREALLOCATE";
const EXTENT_STORE_IO_RING_DEPTH_ENV: &str = "MC_STORE_RS_EXTENT_IO_RING_DEPTH";
const EXTENT_STORE_READ_IO_RING_DEPTH_ENV: &str = "MC_STORE_RS_EXTENT_READ_IO_RING_DEPTH";
const EXTENT_STORE_WRITE_IO_RING_DEPTH_ENV: &str = "MC_STORE_RS_EXTENT_WRITE_IO_RING_DEPTH";
const EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_OPS_ENV: &str =
    "MC_STORE_RS_EXTENT_IO_WORKER_READ_MAX_INFLIGHT_OPS";
const EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_OPS_ENV: &str =
    "MC_STORE_RS_EXTENT_IO_WORKER_WRITE_MAX_INFLIGHT_OPS";
const EXTENT_STORE_IOURING_READS_ENV: &str = "MC_STORE_RS_EXTENT_IOURING_READS";
const EXTENT_STORE_DELETE_JOURNAL_MAGIC: u32 = 0x4d45_5344; // MESD
const EXTENT_STORE_DELETE_JOURNAL_VERSION: u16 = 1;
const EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN: usize = 64;
const EXTENT_STORE_ALIGNMENT: u64 = 4096;
const DEFAULT_EXTENT_STORE_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;
const DEFAULT_EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_OPS: usize = 32;
const EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_BYTES: usize = 128 * 1024 * 1024;
const EXTENT_STORE_IO_WORKER_READ_PIPELINE_WAIT_MIN: usize = 1;
const EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_OPS: usize = 32;
const EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_BYTES: usize = 64 * 1024 * 1024;
const EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_WAIT_MIN: usize = 1;
const EXTENT_STORE_IO_WORKER_READ_STARVATION: std::time::Duration =
    std::time::Duration::from_micros(200);
const EXTENT_STORE_IO_WORKER_WRITE_STARVATION: std::time::Duration =
    std::time::Duration::from_secs(1);
/// Maximum time a write may wait behind pending reads before being force-admitted.
/// Prevents write starvation under sustained restore read load while keeping reads
/// as the priority path.
const EXTENT_STORE_IO_PRIORITY_WRITE_MAX_WAIT: std::time::Duration =
    std::time::Duration::from_secs(2);
const EXTENT_STORE_IO_WORKER_FORCE_ASYNC_READ_BYTES: u32 = 1024 * 1024;
const EXTENT_STORE_IO_WORKER_FORCE_ASYNC_WRITE_BYTES: u32 = 1024 * 1024;
const EXTENT_STORE_IO_WORKER_MAX_WRITE_BATCH_BYTES: usize = 128 * 1024 * 1024;
const EXTENT_STORE_COALESCE_MAX_GAP: u64 = 4 * 1024;
const EXTENT_STORE_COALESCE_MAX_READ_BYTES: u64 = 1024 * 1024;
const EXTENT_STORE_COALESCE_MAX_READ_AMPLIFICATION_NUM: u64 = 2;
const EXTENT_STORE_COALESCE_MIN_READS: usize = 2;
const EXTENT_STORE_COALESCE_DIRECT_COMPATIBLE_MAX_VALUE_LEN: u64 = 64 * 1024;
const EXTENT_STORE_PACKED_SPAN_MAX_GAP: u64 = 0;
const EXTENT_STORE_PACKED_SPAN_MAX_READ_BYTES: u64 = 4 * 1024 * 1024;
const EXTENT_STORE_PACKED_SPAN_TARGET_READ_BYTES: u64 = 256 * 1024;
const EXTENT_STORE_PACKED_SPAN_MAX_READ_BYTES_ENV: &str =
    "MC_STORE_RS_EXTENT_PACKED_SPAN_MAX_READ_BYTES";
const EXTENT_STORE_PACKED_SPAN_MAX_READ_AMPLIFICATION_NUM: u64 = 2;
const EXTENT_STORE_PACKED_SPAN_MIN_BLOCKS: usize = 2;
const EXTENT_STORE_PACKED_BLOCK_READ_MAX_ENTRY_BYTES: u64 = 16 * 1024;
const EXTENT_STORE_PACKED_BLOCK_CHECKSUM_MAX_READ_BYTES: u64 = 64 * 1024;
const EXTENT_STORE_GENERIC_SPAN_ENABLED_ENV: &str = "MC_STORE_RS_EXTENT_GENERIC_SPAN_READS";
const EXTENT_STORE_GENERIC_SPAN_MAX_GAP: u64 = 4 * 1024;
const EXTENT_STORE_GENERIC_SPAN_MAX_READ_BYTES: u64 = 4 * 1024 * 1024;
const EXTENT_STORE_GENERIC_SPAN_MAX_VALUE_LEN: u64 = 4 * 1024 * 1024;
const EXTENT_STORE_GENERIC_SPAN_MAX_READ_AMPLIFICATION_NUM: u64 = 2;
const EXTENT_STORE_GENERIC_SPAN_MIN_READS: usize = 2;
const EXTENT_STORE_IO_WORKER_FIXED_FILE_SLOTS: u32 = 1024;
const EXTENT_STORE_IO_WORKER_FIXED_BUFFER_SLOTS: u32 = 128;
const EXTENT_STORE_GDS_ENABLED_ENV: &str = "MC_STORE_RS_EXTENT_GDS_ENABLED";
const EXTENT_STORE_GDS_MIN_READ_SIZE_ENV: &str = "MC_STORE_RS_EXTENT_GDS_MIN_READ_SIZE";
const EXTENT_STORE_GDS_ALLOW_HOST_DST_ENV: &str = "MC_STORE_RS_EXTENT_GDS_ALLOW_HOST_DST";
const DEFAULT_EXTENT_STORE_GDS_MIN_READ_SIZE: usize = 1024 * 1024;
const IORING_REGISTER_FILES_UPDATE: libc::c_uint = 6;
const IORING_REGISTER_FILES2: libc::c_uint = 13;
const IORING_REGISTER_BUFFERS2: libc::c_uint = 15;
const IORING_REGISTER_BUFFERS_UPDATE: libc::c_uint = 16;
const EXTENT_STORE_FIXED_FILE_SKIP: RawFd = -1;

#[repr(C)]
#[derive(Default)]
struct ExtentStoreIoUringRsrcRegister {
    nr: u32,
    flags: u32,
    resv2: u64,
    data: u64,
    tags: u64,
}

#[repr(C)]
#[derive(Default)]
struct ExtentStoreIoUringRsrcUpdate {
    offset: u32,
    resv: u32,
    data: u64,
}

#[repr(C)]
#[derive(Default)]
struct ExtentStoreIoUringRsrcUpdate2 {
    offset: u32,
    resv: u32,
    data: u64,
    tags: u64,
    nr: u32,
    resv2: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExtentStoreLocator {
    segment_id: u64,
    offset: u64,
    record_len: u64,
    value_offset: u64,
    value_len: u64,
    generation: u64,
}

impl ExtentStoreLocator {
    fn block_key(&self) -> (u64, u64, u64) {
        (self.segment_id, self.offset, self.record_len)
    }
}

impl ExtentStoreLocator {
    fn encode(&self) -> String {
        format!(
            "{EXTENT_STORE_LOCATOR_PREFIX}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}",
            self.segment_id,
            self.offset,
            self.record_len,
            self.value_offset,
            self.value_len,
            self.generation
        )
    }

    fn decode(value: &str) -> Result<Self> {
        let mut parts = value.split(':');
        let Some(prefix) = parts.next() else {
            return Err(StoreError::InvalidState(
                "empty extent store locator".to_string(),
            ));
        };
        if prefix != EXTENT_STORE_LOCATOR_PREFIX {
            return Err(StoreError::InvalidState(format!(
                "invalid extent store locator prefix {prefix:?}"
            )));
        }
        let segment_id = parse_locator_hex(parts.next(), "segment_id")?;
        let offset = parse_locator_hex(parts.next(), "offset")?;
        let record_len = parse_locator_hex(parts.next(), "record_len")?;
        let value_offset = parse_locator_hex(parts.next(), "value_offset")?;
        let value_len = parse_locator_hex(parts.next(), "value_len")?;
        let generation = parse_locator_hex(parts.next(), "generation")?;
        if parts.next().is_some() {
            return Err(StoreError::InvalidState(format!(
                "extent store locator has too many fields: {value}"
            )));
        }
        Ok(Self {
            segment_id,
            offset,
            record_len,
            value_offset,
            value_len,
            generation,
        })
    }
}

fn parse_locator_hex(value: Option<&str>, field: &str) -> Result<u64> {
    let Some(value) = value else {
        return Err(StoreError::InvalidState(format!(
            "extent store locator missing {field}"
        )));
    };
    u64::from_str_radix(value, 16).map_err(|error| {
        StoreError::InvalidState(format!(
            "extent store locator field {field}={value:?} is not hex: {error}"
        ))
    })
}

struct ExtentStoreEngine {
    root: ExtentStorePathBuf,
    io: ExtentStoreIo,
    io_priority: ExtentStoreIoPriority,
    local_read_lanes: ExtentStoreLocalReadLanes,
    buffer_pool: std::sync::Arc<ExtentStoreBufferPool>,
    pinned_read_policy: ExtentStorePinnedReadPolicy,
    mmap_cache: std::sync::Arc<ExtentStoreMmapCache>,
    parallel_buffered_reads: ExtentStoreParallelBufferedReadPool,
    inner: Mutex<ExtentStoreEngineInner>,
    io_counters: std::sync::Arc<ExtentStoreIoCounters>,
}

enum ExtentStoreIo {
    WorkerPair(ExtentStoreIoWorkerPair),
    Blocking(Mutex<ExtentStoreBlockingIo>),
}

struct ExtentStoreBlockingIo;

struct ExtentStoreLocalReadLanes {
    gds: ExtentStoreGdsLane,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtentStoreWriteLane {
    Direct,
    DirectScratch,
    Buffered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExtentStoreLocalReadLane {
    GdsCuFile,
    IoUringDirect,
    IoUringDirectScratch,
    Buffered,
}

struct ExtentStoreGdsLane {
    config: ExtentStoreGdsConfig,
    backend: ExtentStoreGdsBackend,
    driver: Option<ExtentStoreCuFileDriver>,
}

#[cfg(feature = "gds")]
struct ExtentStoreCuFileDriver;

#[cfg(feature = "gds")]
#[repr(C)]
#[derive(Clone, Copy)]
struct ExtentStoreCuFileError {
    err: i32,
    cu_err: i32,
}

#[cfg(feature = "gds")]
#[repr(C)]
struct ExtentStoreCuFileDescr {
    handle_type: i32,
    fd: RawFd,
    fs_ops: *mut c_void,
}

#[cfg(feature = "gds")]
type ExtentStoreCuFileHandle = *mut c_void;

#[derive(Clone, Copy)]
struct ExtentStoreGdsConfig {
    enabled: bool,
    min_read_size: usize,
    allow_host_destination: bool,
}

#[cfg(feature = "gds")]
struct ExtentStoreGdsBackend;

#[cfg(not(feature = "gds"))]
struct ExtentStoreGdsBackend;

#[cfg_attr(not(feature = "gds"), allow(dead_code))]
struct ExtentStoreGdsRead<'a> {
    file: &'a ExtentStoreFile,
    segment_id: u64,
    offset: u64,
    dst: &'a mut [u8],
}

#[derive(Clone, Copy)]
enum ExtentStoreGdsRejectReason {
    Disabled,
    Size,
    Alignment,
    HostDestination,
}

struct ExtentStoreIoWorker {
    queue: std::sync::Arc<ExtentStoreIoWorkerQueue>,
    scratch_arena: std::sync::Arc<ExtentStoreScratchArena>,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
    handle: Mutex<Option<JoinHandle<()>>>,
    is_read: bool,
}

struct ExtentStoreIoWorkerPair {
    read_worker: ExtentStoreReadWorker,
    write_worker: ExtentStoreIoWorker,
}

struct ExtentStoreParallelBufferedReadPool {
    workers: Vec<ExtentStoreParallelBufferedReadWorker>,
}

struct ExtentStoreParallelBufferedReadWorker {
    queue: std::sync::Arc<ExtentStoreParallelBufferedReadQueue>,
    handle: Option<JoinHandle<()>>,
}

struct ExtentStoreParallelBufferedReadQueue {
    state: Mutex<ExtentStoreParallelBufferedReadState>,
    changed: Condvar,
}

#[derive(Default)]
struct ExtentStoreParallelBufferedReadState {
    request: Option<ExtentStoreParallelBufferedReadRequest>,
    shutdown: bool,
}

struct ExtentStoreParallelBufferedReadRequest {
    reads: Vec<ExtentStoreParallelBufferedRead>,
    completion: Sender<Vec<Result<()>>>,
}

struct ExtentStoreParallelBufferedRead {
    file: Arc<ExtentStoreFile>,
    offset: u64,
    dst: *mut u8,
    len: usize,
}

unsafe impl Send for ExtentStoreParallelBufferedRead {}

struct ExtentStoreIoWorkerQueue {
    state: Mutex<ExtentStoreIoWorkerState>,
    changed: Condvar,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
}

#[derive(Default)]
struct ExtentStoreIoWorkerState {
    reads: VecDeque<ExtentStoreIoRequest>,
    writes: VecDeque<ExtentStoreIoRequest>,
    first_read_wait: Option<std::time::Instant>,
    first_write_wait: Option<std::time::Instant>,
    shutdown: bool,
}

struct ExtentStoreIoRequest {
    op: ExtentStoreIoOp,
    len: usize,
    completion: Sender<Result<usize>>,
    enqueue_time: std::time::Instant,
}

struct ExtentStoreIoSubmission {
    op: ExtentStoreIoOp,
    len: usize,
}

enum ExtentStoreReadBatchEntry {
    Single {
        index: usize,
    },
    Coalesced {
        group: ExtentStoreCoalescedReadGroup,
    },
}

struct ExtentStoreCoalescedReadGroup {
    span_start: u64,
    span_len: usize,
    indices: Vec<usize>,
    scratch: ExtentStoreScratchLease,
}

struct PackedExtentStoreReadBlock {
    segment_id: u64,
    offset: u64,
    record_len: u64,
    indices: Vec<usize>,
    packed_entry_count: usize,
}

struct PackedExtentStoreReadEntry {
    index: usize,
    value_offset: u64,
}

enum ExtentStoreIoOp {
    Read {
        fd: RawFd,
        buf: *mut u8,
        len: u32,
        offset: u64,
        fixed_file_key: Option<ExtentStoreFixedFileKey>,
        fixed_buffer: Option<u16>,
    },
    Write {
        fd: RawFd,
        buf: *const u8,
        len: u32,
        offset: u64,
        fixed_file_key: Option<ExtentStoreFixedFileKey>,
        fixed_buffer: Option<u16>,
    },
    Writev {
        fd: RawFd,
        iovecs: *const libc::iovec,
        len: u32,
        offset: u64,
        fixed_file_key: Option<ExtentStoreFixedFileKey>,
    },
    RegisterBuffer {
        buffer: *mut u8,
        len: usize,
        completion: Sender<Option<u16>>,
    },
    ReleaseBuffer {
        slot: u16,
    },
    UnregisterFile {
        key: ExtentStoreFixedFileKey,
    },
}

unsafe impl Send for ExtentStoreIoOp {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ExtentStoreFixedFileKey {
    Buffered(u64),
    Direct(u64),
}

impl ExtentStoreFixedFileKey {
    fn for_buffered(segment_id: u64) -> Self {
        Self::Buffered(segment_id)
    }

    fn for_direct(segment_id: u64) -> Self {
        Self::Direct(segment_id)
    }
}

struct ExtentStoreFixedBufferRegistry {
    free_slots: Vec<u16>,
    legacy_sparse_buffers: bool,
    disabled: bool,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
}

struct ExtentStoreFixedFileRegistry {
    slots_by_key: BTreeMap<ExtentStoreFixedFileKey, u32>,
    free_slots: Vec<u32>,
    sparse_files: Option<Vec<RawFd>>,
    disabled: bool,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
}

struct ExtentStoreFixedFileGuard<'a> {
    ring: &'a mut IoUring,
    registry: Option<&'a mut ExtentStoreFixedFileRegistry>,
    fd: RawFd,
    fixed_file_key: Option<ExtentStoreFixedFileKey>,
}

include!("extent_store_io_worker.rs");

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExtentStoreMaintenanceStats {
    pub(crate) live_bytes: u64,
    pub(crate) dead_bytes: u64,
    pub(crate) segment_count: u64,
}

/// Generates `ExtentStoreIoCounters` (atomic) and `ExtentStoreIoStats` (snapshot) from a single
/// field list.  Each field is either a `counter` (cumulative, delta uses saturating_sub) or a
/// `gauge` (point-in-time, delta copies self).
macro_rules! extent_store_io_counters {
    ($(($field:ident, $kind:ident)),* $(,)?) => {
        #[derive(Default)]
        struct ExtentStoreIoCounters {
            $($field: AtomicU64,)*
        }

        #[cfg_attr(not(test), allow(dead_code))]
        #[derive(Clone, Copy, Default)]
        struct ExtentStoreIoStats {
            $($field: u64,)*
        }

        impl ExtentStoreIoCounters {
            fn snapshot(&self) -> ExtentStoreIoStats {
                ExtentStoreIoStats {
                    $($field: self.$field.load(AtomicOrdering::Relaxed),)*
                }
            }
        }

        #[cfg_attr(not(test), allow(dead_code))]
        impl ExtentStoreIoStats {
            fn delta_since(self, previous: Self) -> Self {
                Self {
                    $($field: extent_store_io_counters!(@delta_val $kind, self, previous, $field),)*
                }
            }

            fn to_profiling_pairs(self) -> Vec<(&'static str, u64)> {
                vec![
                    $((stringify!($field), self.$field),)*
                ]
            }
        }
    };
    (@delta_val counter, $this:expr, $prev:expr, $field:ident) => {
        $this.$field.saturating_sub($prev.$field)
    };
    (@delta_val gauge, $this:expr, $prev:expr, $field:ident) => {
        $this.$field
    };
}

extent_store_io_counters! {
    (segment_rollover_ops, counter),
    (segment_rollover_us, counter),
    (segment_rollover_max_us, gauge),
    (buffered_write_blocking_batches, counter),
    (buffered_write_blocking_bytes, counter),
    (direct_write_ops, counter),
    (direct_scratch_write_ops, counter),
    (buffered_write_ops, counter),
    (direct_read_ops, counter),
    (direct_read_bytes, counter),
    (direct_scratch_read_ops, counter),
    (direct_scratch_read_bytes, counter),
    (buffered_read_ops, counter),
    (buffered_read_bytes, counter),
    (scratch_alloc_ops, counter),
    (scratch_alloc_bytes, counter),
    (scratch_reuse_ops, counter),
    (scratch_reuse_bytes, counter),
    (scratch_pool_buffers, gauge),
    (scratch_pool_bytes, gauge),
    (worker_read_batch_ops, counter),
    (worker_read_batch_bytes, counter),
    (worker_write_batch_ops, counter),
    (worker_write_batch_bytes, counter),
    (worker_write_byte_limited_turns, counter),
    (worker_write_byte_limited_ops, counter),
    (worker_write_byte_limited_bytes, counter),
    (worker_read_preempted_for_write, counter),
    (worker_write_preempted_for_read, counter),
    (worker_read_starvation_turns, counter),
    (worker_write_starvation_turns, counter),
    (worker_read_queue_depth, gauge),
    (worker_write_queue_depth, gauge),
    (fixed_file_table_init_success, gauge),
    (fixed_file_table_init_failure, gauge),
    (fixed_file_sparse_table_init_success, gauge),
    (fixed_file_sparse_table_init_failure, gauge),
    (fixed_file_array_table_init_success, gauge),
    (fixed_file_array_table_init_failure, gauge),
    (fixed_file_register_success, counter),
    (fixed_file_register_failure, counter),
    (fixed_file_slot_exhaustions, counter),
    (fixed_file_hit_ops, counter),
    (fixed_file_last_errno, gauge),
    (fixed_file_update_last_errno, gauge),
    (fixed_buffer_table_init_success, gauge),
    (fixed_buffer_table_init_failure, gauge),
    (fixed_buffer_sparse_table_init_success, gauge),
    (fixed_buffer_sparse_table_init_failure, gauge),
    (fixed_buffer_array_table_init_success, gauge),
    (fixed_buffer_array_table_init_failure, gauge),
    (fixed_buffer_register_success, counter),
    (fixed_buffer_register_failure, counter),
    (fixed_buffer_slot_exhaustions, counter),
    (fixed_buffer_last_errno, gauge),
    (fixed_buffer_update_last_errno, gauge),
    (read_fixed_ops, counter),
    (write_fixed_ops, counter),
    (raw_fd_fallback_ops, counter),
    (coalesced_read_groups, counter),
    (coalesced_logical_reads, counter),
    (coalesced_requested_bytes, counter),
    (coalesced_physical_bytes, counter),
    (coalesced_gap_bytes, counter),
    (gds_read_ops, counter),
    (gds_read_bytes, counter),
    (gds_submit_error_ops, counter),
    (gds_alignment_reject_ops, counter),
    (gds_size_reject_ops, counter),
    (gds_disabled_reject_ops, counter),
    (gds_host_destination_reject_ops, counter),
    (gds_fallback_to_iouring_ops, counter),
    (gds_lane_degraded_ops, counter),
    (pinned_read_ops, counter),
    (pinned_read_bytes, counter),
    (pinned_fallback_disabled_ops, counter),
    (pinned_fallback_below_min_value_ops, counter),
    (pinned_fallback_quota_exceeded_ops, counter),
    (pinned_fallback_mapping_too_short_ops, counter),
    (pinned_fallback_mmap_failed_ops, counter),
    (pinned_fallback_unsupported_locator_ops, counter),
    (pinned_fallback_cold_ordinary_ops, counter),
    (mmap_cache_hit_ops, counter),
    (mmap_cache_miss_ops, counter),
    (mmap_active_segments, gauge),
    (mmap_cache_bytes, gauge),
    (mmap_create_ops, counter),
    (mmap_create_failures, counter),
    (pinned_payload_active_refs, gauge),
    (pinned_payload_active_bytes, gauge),
    (pinned_payload_ref_created_ops, counter),
    (pinned_payload_ref_dropped_ops, counter),
}

impl ExtentStoreIoPriority {
    fn new() -> Self {
        Self {
            state: Mutex::new(ExtentStoreIoPriorityState::default()),
            changed: Condvar::new(),
        }
    }

    fn enter_read(&self) -> ExtentStoreIoPriorityPermit<'_> {
        let mut state = self.state.lock();
        state.waiting_reads = state.waiting_reads.saturating_add(1);
        // Read-priority: reads only wait for an active write to finish.
        // They do NOT wait for waiting_writes, so offload writes cannot block
        // restore reads from reaching the io_uring worker.
        let needs_wait = state.active_write;
        let t_wait = if needs_wait {
            Some(std::time::Instant::now())
        } else {
            None
        };
        while state.active_write {
            self.changed.wait(&mut state);
        }
        state.waiting_reads = state.waiting_reads.saturating_sub(1);
        state.active_reads = state.active_reads.saturating_add(1);
        drop(state);
        if let Some(t_wait) = t_wait {
            let elapsed = t_wait.elapsed();
            crate::observability::registry::record_io_priority_wait("read", elapsed);
            let wait_ms = elapsed.as_secs_f64() * 1000.0;
            if wait_ms > 1.0 {
                warn!("io_priority_read_wait: wait_ms={wait_ms:.3}",);
            }
        }
        ExtentStoreIoPriorityPermit::Read(self)
    }

    fn enter_write(&self) -> ExtentStoreIoPriorityPermit<'_> {
        let mut state = self.state.lock();
        state.waiting_writes = state.waiting_writes.saturating_add(1);
        // Write yields to waiting reads: writes wait until no active_write,
        // no active_reads, AND no waiting_reads.  This ensures restore reads
        // are never blocked by queued offload writes.
        //
        // Anti-starvation: if the write waits longer than WRITE_MAX_WAIT,
        // it falls back to only waiting for active_write (same as a read),
        // so offload writes are guaranteed forward progress.
        let needs_wait = state.active_write || state.active_reads != 0 || state.waiting_reads != 0;
        let t_wait = if needs_wait {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let deadline = std::time::Instant::now() + EXTENT_STORE_IO_PRIORITY_WRITE_MAX_WAIT;
        while state.active_write || state.active_reads != 0 || state.waiting_reads != 0 {
            let now = std::time::Instant::now();
            if now >= deadline {
                // Write starvation timeout: fall back to waiting for active_write only,
                // granting the write the same priority as a read.
                while state.active_write {
                    self.changed.wait(&mut state);
                }
                break;
            }
            let remaining = deadline - now;
            self.changed.wait_for(&mut state, remaining);
        }
        state.waiting_writes = state.waiting_writes.saturating_sub(1);
        state.active_write = true;
        drop(state);
        if let Some(t_wait) = t_wait {
            let elapsed = t_wait.elapsed();
            crate::observability::registry::record_io_priority_wait("write", elapsed);
            let wait_ms = elapsed.as_secs_f64() * 1000.0;
            if wait_ms > 1.0 {
                warn!("io_priority_write_wait: wait_ms={wait_ms:.3}",);
            }
        }
        ExtentStoreIoPriorityPermit::Write(self)
    }

    fn exit_read(&self) {
        let mut state = self.state.lock();
        state.active_reads = state.active_reads.saturating_sub(1);
        if state.active_reads == 0 {
            self.changed.notify_all();
        }
    }

    fn exit_write(&self) {
        let mut state = self.state.lock();
        state.active_write = false;
        self.changed.notify_all();
    }
}

impl Drop for ExtentStoreIoPriorityPermit<'_> {
    fn drop(&mut self) {
        match self {
            Self::Read(priority) => priority.exit_read(),
            Self::Write(priority) => priority.exit_write(),
        }
    }
}

impl ExtentStoreBufferPool {
    fn new(counters: std::sync::Arc<ExtentStoreIoCounters>) -> Self {
        Self {
            buffers: Mutex::new(Vec::new()),
            counters,
        }
    }

    fn lease(self: &std::sync::Arc<Self>, len: usize) -> Result<ExtentStoreBufferLease> {
        let buffer = {
            let mut buffers = self.buffers.lock();
            let buffer = buffers
                .iter()
                .position(|buffer| buffer.len() >= len)
                .map(|index| buffers.swap_remove(index));
            let pool_bytes = buffers.iter().map(|buffer| buffer.len()).sum::<usize>();
            self.counters.record_scratch_pool(buffers.len(), pool_bytes);
            buffer
        };
        let buffer = match buffer {
            Some(buffer) => {
                self.counters.record_scratch_reuse(len);
                buffer
            }
            None => {
                self.counters.record_scratch_alloc(len);
                AlignedExtentStoreBuffer::uninitialized(len)?
            }
        };
        Ok(ExtentStoreBufferLease {
            buffer: Some(buffer),
            len,
            pool: self.clone(),
        })
    }

    fn release(&self, buffer: AlignedExtentStoreBuffer) {
        let mut buffers = self.buffers.lock();
        buffers.push(buffer);
        let pool_bytes = buffers.iter().map(|buffer| buffer.len()).sum::<usize>();
        self.counters.record_scratch_pool(buffers.len(), pool_bytes);
    }
}

impl Deref for ExtentStoreScratchLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer
            .as_ref()
            .expect("leased extent store scratch buffer should be present")
    }
}

impl DerefMut for ExtentStoreScratchLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer
            .as_mut()
            .expect("leased extent store scratch buffer should be present")
    }
}

impl Drop for ExtentStoreScratchLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.arena.release(buffer, self.fixed_slot.take());
        }
    }
}

impl ExtentStoreBufferLease {
    fn set_len(&mut self, len: usize) -> Result<()> {
        let capacity = self
            .buffer
            .as_ref()
            .expect("leased extent store pooled buffer should be present")
            .len();
        if len > capacity {
            return Err(StoreError::InvalidState(format!(
                "extent store pooled buffer length {len} exceeds capacity {capacity}"
            )));
        }
        self.len = len;
        Ok(())
    }
}

impl Deref for ExtentStoreBufferLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let buffer = self
            .buffer
            .as_ref()
            .expect("leased extent store pooled buffer should be present");
        &buffer[..self.len]
    }
}

impl DerefMut for ExtentStoreBufferLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let buffer = self
            .buffer
            .as_mut()
            .expect("leased extent store pooled buffer should be present");
        &mut buffer[..self.len]
    }
}

impl Drop for ExtentStoreBufferLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

struct ExtentStoreEngineInner {
    segment_size: u64,
    active_segment_id: u64,
    active_offset: u64,
    generation: u64,
    segments: BTreeMap<u64, ExtentStoreSegment>,
    delete_journal: ExtentStoreDeleteJournal,
}

struct ExtentStoreSegment {
    file: Arc<ExtentStoreFile>,
    direct_file: Option<Arc<ExtentStoreFile>>,
    path: ExtentStorePathBuf,
    live_bytes: u64,
    dead_bytes: u64,
    deleted_extents: BTreeSet<(u64, u64)>,
    packed_blocks: BTreeMap<(u64, u64), PackedBlockLiveState>,
}

#[derive(Clone, Copy)]
struct ExtentStorePinnedReadPolicy {
    enabled: bool,
    min_value_bytes: usize,
    max_mmap_segments: usize,
    max_mmap_bytes: u64,
    max_active_refs: u64,
    max_active_bytes: u64,
}

impl ExtentStorePinnedReadPolicy {
    fn from_env() -> Self {
        Self {
            enabled: env_bool_default(EXTENT_STORE_PINNED_READS_ENV, true),
            min_value_bytes: env_u64(EXTENT_STORE_PINNED_MIN_VALUE_BYTES_ENV, 128 * 1024) as usize,
            max_mmap_segments: env_u64(EXTENT_STORE_PINNED_MAX_MMAP_SEGMENTS_ENV, 256) as usize,
            max_mmap_bytes: env_u64(
                EXTENT_STORE_PINNED_MAX_MMAP_BYTES_ENV,
                32 * 1024 * 1024 * 1024,
            ),
            max_active_refs: env_u64(EXTENT_STORE_PINNED_MAX_ACTIVE_REFS_ENV, 8192),
            max_active_bytes: env_u64(
                EXTENT_STORE_PINNED_MAX_ACTIVE_BYTES_ENV,
                8 * 1024 * 1024 * 1024,
            ),
        }
    }
}

#[derive(Default)]
struct ExtentStoreMmapCache {
    mappings: Mutex<BTreeMap<u64, std::sync::Arc<ExtentStoreMmap>>>,
}

struct ExtentStoreMmap {
    ptr: NonNull<u8>,
    len: usize,
    _file: Arc<ExtentStoreFile>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExtentStorePinnedPrefetchMode {
    Disabled,
    Fadvise,
    Readahead,
    Madvise,
    Touch,
}

unsafe impl Send for ExtentStoreMmap {}
unsafe impl Sync for ExtentStoreMmap {}

impl ExtentStoreMmap {
    fn map(file: Arc<ExtentStoreFile>, len: usize) -> Result<Self> {
        if len == 0 {
            return Err(StoreError::InvalidState(
                "extent store segment mmap length is zero".to_string(),
            ));
        }
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(StoreError::Transport(format!(
                "extent store segment mmap failed: {}",
                io::Error::last_os_error()
            )));
        }
        Ok(Self {
            ptr: NonNull::new(ptr.cast::<u8>()).expect("mmap should not return null"),
            len,
            _file: file,
        })
    }

    fn slice(&self, offset: usize, len: usize) -> Result<&[u8]> {
        if offset > self.len || len > self.len - offset {
            return Err(StoreError::InvalidState(
                "extent store mmap slice exceeds segment bounds".to_string(),
            ));
        }
        Ok(unsafe { std::slice::from_raw_parts(self.ptr.as_ptr().add(offset), len) })
    }
}

impl Drop for ExtentStoreMmap {
    fn drop(&mut self) {
        let _ = unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

struct ExtentStoreMmapPayloadRef {
    mapping: std::sync::Arc<ExtentStoreMmap>,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
    offset: usize,
    len: usize,
}

struct ExtentStorePooledPayloadRef {
    buffer: ExtentStoreBufferLease,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
}

unsafe impl Sync for ExtentStorePooledPayloadRef {}

impl ColdPayloadRefData for ExtentStoreMmapPayloadRef {
    fn as_slice(&self) -> &[u8] {
        self.mapping
            .slice(self.offset, self.len)
            .expect("validated mmap payload slice should remain in bounds")
    }
}

impl ColdPayloadRefData for ExtentStorePooledPayloadRef {
    fn as_slice(&self) -> &[u8] {
        &self.buffer
    }
}

impl Drop for ExtentStoreMmapPayloadRef {
    fn drop(&mut self) {
        self.counters.record_pinned_payload_drop(self.len);
    }
}

impl Drop for ExtentStorePooledPayloadRef {
    fn drop(&mut self) {
        self.counters.record_pinned_payload_drop(self.buffer.len());
    }
}

struct PackedBlockLiveState {
    entry_count: u32,
    live_count: u32,
    live_value_bytes: u64,
    dead_value_bytes: u64,
}

struct ExtentStoreDeleteJournal {
    file: ExtentStoreFile,
}

struct ExtentStoreWrite<'a> {
    logical_locator: &'a str,
    payload: &'a [u8],
    checksum: Option<u64>,
}

struct EncodedExtentStoreRecord<'a> {
    value_offset: u64,
    value_len: u64,
    record_len: u64,
    entry_count: u32,
    prefix: ExtentStoreBufferLease,
    payload: EncodedExtentStorePayload<'a>,
    suffix_padding: ExtentStoreBufferLease,
}

enum EncodedExtentStorePayload<'a> {
    Borrowed(&'a [u8]),
    BorrowedSlices(Vec<&'a [u8]>),
}

#[derive(Clone, Copy)]
enum ExtentStorePinnedFallbackReason {
    Disabled,
    BelowMinValue,
    QuotaExceeded,
    MappingTooShort,
    MmapFailed,
    UnsupportedLocator,
    ColdOrdinary,
}

enum ExtentStoreWriteBuffers {
    Iovecs(Vec<libc::iovec>),
    Direct(ExtentStoreScratchRecord),
}

#[cfg_attr(not(test), allow(dead_code))]
struct ExtentStoreScratchRecord {
    buffer: ExtentStoreScratchLease,
    record_len: usize,
}

struct ReservedExtentStoreWrite<'a> {
    locator: ExtentStoreLocator,
    result_locators: Vec<(usize, ExtentStoreLocator)>,
    file: Arc<ExtentStoreFile>,
    direct_file: Option<Arc<ExtentStoreFile>>,
    record: EncodedExtentStoreRecord<'a>,
}

struct ExtentStoreRead<'a> {
    locator: &'a str,
    expected_len: u64,
    dst: &'a mut [u8],
}

struct ReservedExtentStoreRead<'a> {
    locator: ExtentStoreLocator,
    file: Arc<ExtentStoreFile>,
    direct_file: Option<Arc<ExtentStoreFile>>,
    dst: &'a mut [u8],
}

struct ReservedExtentStorePinnedRead {
    index: usize,
    locator: ExtentStoreLocator,
    file: Arc<ExtentStoreFile>,
    segment_len: usize,
    packed: bool,
}

type ExtentStoreReadFiles = (Arc<ExtentStoreFile>, Option<Arc<ExtentStoreFile>>);
type ReservedExtentStoreReadWithFiles = (
    usize,
    ExtentStoreLocator,
    Arc<ExtentStoreFile>,
    Option<Arc<ExtentStoreFile>>,
);

impl ExtentStoreEngine {
    #[cfg(test)]
    fn new(root: impl Into<ExtentStorePathBuf>) -> Result<Self> {
        Self::new_with_segment_size(root, DEFAULT_EXTENT_STORE_SEGMENT_SIZE)
    }

    fn new_with_segment_size(
        root: impl Into<ExtentStorePathBuf>,
        segment_size: u64,
    ) -> Result<Self> {
        let root = root.into();
        let segments_dir = root.join("segments");
        std::fs::create_dir_all(&segments_dir).map_err(|error| {
            StoreError::Transport(format!(
                "failed to create extent store segment directory {}: {error}",
                segments_dir.display()
            ))
        })?;
        let inner = recover_extent_store_segments(&root, segment_size.max(EXTENT_STORE_ALIGNMENT))?;
        let io_counters = std::sync::Arc::new(ExtentStoreIoCounters::default());
        let buffer_pool = std::sync::Arc::new(ExtentStoreBufferPool::new(io_counters.clone()));
        let default_ring_depth =
            env_u64(EXTENT_STORE_IO_RING_DEPTH_ENV, 256).clamp(32, 4096) as u32;
        let read_ring_depth = env_u64(
            EXTENT_STORE_READ_IO_RING_DEPTH_ENV,
            default_ring_depth as u64,
        )
        .clamp(32, 4096) as u32;
        let write_ring_depth = env_u64(
            EXTENT_STORE_WRITE_IO_RING_DEPTH_ENV,
            default_ring_depth as u64,
        )
        .clamp(32, 4096) as u32;
        let read_pool_threads = read_io_worker_thread_count();
        let io = match (
            IoUring::new(read_ring_depth),
            IoUring::new(write_ring_depth),
        ) {
            (Ok(read_ring), Ok(write_ring)) => {
                let read_worker = if read_pool_threads > 1 {
                    // Pool creates its own rings; drop the probe ring.
                    drop(read_ring);
                    ExtentStoreReadWorker::Pool(ExtentStoreIoWorkerPool::new(
                        read_ring_depth,
                        read_pool_threads,
                        io_counters.clone(),
                        "extent-store-io-read-pool",
                    ))
                } else {
                    ExtentStoreReadWorker::Single(ExtentStoreIoWorker::new(
                        read_ring,
                        io_counters.clone(),
                        "extent-store-io-read",
                        true,
                    ))
                };
                let write_worker = ExtentStoreIoWorker::new(
                    write_ring,
                    io_counters.clone(),
                    "extent-store-io-write",
                    false,
                );
                ExtentStoreIo::WorkerPair(ExtentStoreIoWorkerPair {
                    read_worker,
                    write_worker,
                })
            }
            (Err(error), _) | (_, Err(error)) if io_uring_unavailable(&error) => {
                warn!(
                    error = %error,
                    "extent store io_uring unavailable; falling back to blocking pread/pwrite"
                );
                ExtentStoreIo::Blocking(Mutex::new(ExtentStoreBlockingIo))
            }
            (Err(error), _) | (_, Err(error)) => {
                return Err(StoreError::Transport(format!(
                    "failed to create extent store io_uring: {error}"
                )))
            }
        };
        Ok(Self {
            root,
            io,
            io_priority: ExtentStoreIoPriority::new(),
            local_read_lanes: ExtentStoreLocalReadLanes::from_env(&io_counters),
            buffer_pool,
            pinned_read_policy: ExtentStorePinnedReadPolicy::from_env(),
            mmap_cache: std::sync::Arc::new(ExtentStoreMmapCache::default()),
            parallel_buffered_reads: ExtentStoreParallelBufferedReadPool::from_env(),
            inner: Mutex::new(inner),
            io_counters,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn io_stats(&self) -> ExtentStoreIoStats {
        self.io_counters.snapshot()
    }

    pub(crate) fn maintenance_stats(&self) -> ExtentStoreMaintenanceStats {
        let inner = self.inner.lock();
        inner.segments.values().fold(
            ExtentStoreMaintenanceStats::default(),
            |mut stats, segment| {
                stats.live_bytes = stats.live_bytes.saturating_add(segment.live_bytes);
                stats.dead_bytes = stats.dead_bytes.saturating_add(segment.dead_bytes);
                stats.segment_count = stats.segment_count.saturating_add(1);
                stats
            },
        )
    }

    fn put(&self, logical_locator: &str, payload: &[u8]) -> Result<String> {
        self.put_batch(&[ExtentStoreWrite {
            logical_locator,
            payload,
            checksum: None,
        }])
        .into_iter()
        .next()
        .unwrap_or_else(|| {
            Err(StoreError::InvalidState(
                "empty extent store put result".to_string(),
            ))
        })
    }

    fn put_batch(&self, writes: &[ExtentStoreWrite<'_>]) -> Vec<Result<String>> {
        self.put_batch_profiled(writes, &mut |_, _| {})
    }

    fn put_batch_profiled(
        &self,
        writes: &[ExtentStoreWrite<'_>],
        record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<String>> {
        if writes.is_empty() {
            return Vec::new();
        }
        let init_started = std::time::Instant::now();
        let mut results = (0..writes.len())
            .map(|_| {
                Err(StoreError::InvalidState(
                    "extent store write not submitted".to_string(),
                ))
            })
            .collect::<Vec<_>>();
        record_stage("engine_write_init", init_started.elapsed());

        let encode_packed_started = std::time::Instant::now();
        let packed_records = encode_packed_extent_store_records(writes, &self.buffer_pool);
        let packed_indices = packed_records
            .iter()
            .flat_map(|record| record.result_locators.iter().map(|(index, _)| *index))
            .collect::<BTreeSet<_>>();
        record_stage(
            "engine_encode_packed_records",
            encode_packed_started.elapsed(),
        );

        let encode_single_started = std::time::Instant::now();
        let single_records = writes
            .iter()
            .enumerate()
            .filter(|(index, _)| !packed_indices.contains(index))
            .map(|(index, write)| {
                (
                    index,
                    encode_extent_store_record(
                        write.logical_locator,
                        write.payload,
                        write.checksum,
                        &self.buffer_pool,
                    ),
                )
            })
            .collect::<Vec<_>>();
        record_stage(
            "engine_encode_single_records",
            encode_single_started.elapsed(),
        );

        let reserve_started = std::time::Instant::now();
        let mut reserved = Vec::with_capacity(writes.len());
        {
            let mut inner = self.inner.lock();
            for packed in packed_records {
                let index = packed.result_locators[0].0;
                let encoded = match packed.record {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        for (result_index, _) in packed.result_locators {
                            results[result_index] = Err(error.clone());
                        }
                        continue;
                    }
                };
                let mut reserve_context = ExtentStoreRecordReservationContext {
                    inner: &mut inner,
                    root: &self.root,
                    counters: &self.io_counters,
                    results: &mut results,
                    reserved: &mut reserved,
                    record_stage,
                };
                reserve_encoded_extent_store_record(
                    &mut reserve_context,
                    index,
                    EncodedExtentStoreRecordReservation {
                        encoded,
                        result_locators: packed.result_locators,
                    },
                );
            }
            for (index, encoded) in single_records {
                let encoded = match encoded {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        results[index] = Err(error);
                        continue;
                    }
                };
                let mut reserve_context = ExtentStoreRecordReservationContext {
                    inner: &mut inner,
                    root: &self.root,
                    counters: &self.io_counters,
                    results: &mut results,
                    reserved: &mut reserved,
                    record_stage,
                };
                reserve_encoded_extent_store_record(
                    &mut reserve_context,
                    index,
                    EncodedExtentStoreRecordReservation {
                        encoded,
                        result_locators: Vec::new(),
                    },
                );
            }
        }
        record_stage("engine_reserve_writes", reserve_started.elapsed());

        let write_started = std::time::Instant::now();
        let mut reserved_writes = reserved.into_iter().collect::<Vec<_>>();
        // Safety: data writes do not need io_priority exclusion.
        // Metadata is already committed under `inner` Mutex (dropped above),
        // and the locator is not returned until after write completes, so no
        // concurrent read can reference the in-flight offset.  enter_write()
        // is only retained in rollback_uncommitted_write / delete_decoded
        // where segment file removal requires exclusive access.
        let write_results = self.write_all_batch_at(&mut reserved_writes);
        record_stage("engine_write_all_batch", write_started.elapsed());

        let merge_started = std::time::Instant::now();
        for ((index, write), write_result) in reserved_writes.into_iter().zip(write_results) {
            match write_result {
                Ok(()) => {
                    for (result_index, locator) in write.result_locators {
                        results[result_index] = Ok(locator.encode());
                    }
                }
                Err(error) => {
                    self.rollback_uncommitted_write(&write.locator);
                    results[index] = Err(error.clone());
                    for (result_index, _) in write.result_locators {
                        results[result_index] = Err(error.clone());
                    }
                }
            }
        }
        record_stage("engine_merge_write_results", merge_started.elapsed());
        results
    }

    fn contains(&self, locator: &str, expected_len: u64) -> Result<bool> {
        let locator = ExtentStoreLocator::decode(locator)?;
        if locator.value_len != expected_len {
            return Err(StoreError::InvalidState(format!(
                "extent store locator length mismatch: expected {expected_len} actual {}",
                locator.value_len
            )));
        }
        let inner = self.inner.lock();
        let Some(segment) = inner.segments.get(&locator.segment_id) else {
            return Ok(false);
        };
        validate_extent_store_locator(segment, &locator)?;
        Ok(true)
    }

    fn get(&self, locator: &str, expected_len: u64) -> Result<Option<Vec<u8>>> {
        let locator = ExtentStoreLocator::decode(locator)?;
        if locator.value_len != expected_len {
            return Err(StoreError::InvalidState(format!(
                "extent store locator length mismatch: expected {expected_len} actual {}",
                locator.value_len
            )));
        }
        let value_len = usize::try_from(locator.value_len).map_err(|_| {
            StoreError::InvalidState(format!(
                "extent store locator length {} exceeds addressable memory",
                locator.value_len
            ))
        })?;
        let mut payload = vec![0u8; value_len];
        self.get_decoded_into(&locator, expected_len, &mut payload)?;
        Ok(Some(payload))
    }

    fn get_into(&self, locator: &str, expected_len: u64, dst: &mut [u8]) -> Result<Option<usize>> {
        let read = ExtentStoreRead {
            locator,
            expected_len,
            dst,
        };
        self.get_batch_into(&mut [read])
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                Err(StoreError::InvalidState(
                    "empty extent store get result".to_string(),
                ))
            })
    }

    fn get_batch_into(&self, reads: &mut [ExtentStoreRead<'_>]) -> Vec<Result<Option<usize>>> {
        self.get_batch_into_profiled(reads, &mut |_, _| {})
    }

    fn get_batch_into_profiled(
        &self,
        reads: &mut [ExtentStoreRead<'_>],
        record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<Option<usize>>> {
        if reads.is_empty() {
            return Vec::new();
        }
        let decode_started = std::time::Instant::now();
        let mut decoded = reads
            .iter()
            .map(|read| ExtentStoreLocator::decode(read.locator))
            .collect::<Vec<_>>();
        record_stage("engine_decode_locators", decode_started.elapsed());

        let validate_started = std::time::Instant::now();
        let mut results = (0..reads.len())
            .map(|_| {
                Err(StoreError::InvalidState(
                    "extent store read not submitted".to_string(),
                ))
            })
            .collect::<Vec<_>>();
        let mut sortable = Vec::new();
        for index in 0..reads.len() {
            let locator = match std::mem::replace(
                &mut decoded[index],
                Err(StoreError::InvalidState(
                    "extent store read locator already consumed".to_string(),
                )),
            ) {
                Ok(locator) => locator,
                Err(error) => {
                    results[index] = Err(error);
                    continue;
                }
            };
            if locator.value_len != reads[index].expected_len {
                results[index] = Err(StoreError::InvalidState(format!(
                    "extent store locator length mismatch: expected {} actual {}",
                    reads[index].expected_len, locator.value_len
                )));
                continue;
            }
            if locator.value_len as usize > reads[index].dst.len() {
                results[index] = Err(StoreError::InvalidState(format!(
                    "extent store destination too small: need {} actual {}",
                    locator.value_len,
                    reads[index].dst.len()
                )));
                continue;
            }
            sortable.push((locator.segment_id, locator.offset, index, locator));
        }
        if sortable
            .windows(2)
            .any(|pair| pair[0].0 > pair[1].0 || pair[0].0 == pair[1].0 && pair[0].1 > pair[1].1)
        {
            sortable.sort_by_key(|(segment_id, offset, _, _)| (*segment_id, *offset));
        }
        let ordered = sortable
            .into_iter()
            .map(|(_, _, index, locator)| (index, locator))
            .collect::<Vec<_>>();
        record_stage("engine_validate_sort", validate_started.elapsed());

        let permit_started = std::time::Instant::now();
        let _permit = self.io_priority.enter_read();
        record_stage("engine_enter_read_permit", permit_started.elapsed());

        let reserve_started = std::time::Instant::now();
        let reserved_results = self.reserve_reads(&ordered);
        let mut reserved = Vec::new();
        for ((index, locator), reserve_result) in ordered.iter().copied().zip(reserved_results) {
            let (file, direct_file) = match reserve_result {
                Ok(files) => files,
                Err(error) => {
                    results[index] = Err(error);
                    continue;
                }
            };
            reserved.push((index, locator, file, direct_file));
        }
        record_stage("engine_reserve_reads", reserve_started.elapsed());

        let disjoint_started = std::time::Instant::now();
        let read_results = self.with_disjoint_read_buffers(reads, reserved, |reserved_reads| {
            record_stage("engine_build_reserved_reads", disjoint_started.elapsed());
            let read_started = std::time::Instant::now();
            let results = self.read_batch_with_packed_blocks_at(reserved_reads, record_stage);
            record_stage(
                "engine_read_batch_with_packed_blocks",
                read_started.elapsed(),
            );
            results
        });
        let fill_started = std::time::Instant::now();
        for ((index, locator), read_result) in read_results {
            match read_result {
                Ok(()) => results[index] = Ok(Some(locator.value_len as usize)),
                Err(error) => results[index] = Err(error),
            }
        }
        record_stage("engine_fill_results", fill_started.elapsed());
        results
    }

    fn get_pinned(&self, locator: &str, expected_len: u64) -> Result<Option<ColdObjectPayload>> {
        let mut results = self.get_pinned_batch(&[(locator, expected_len)]);
        results.pop().unwrap_or_else(|| {
            Err(StoreError::InvalidState(
                "empty extent store pinned get result".to_string(),
            ))
        })
    }

    fn get_pinned_batch(&self, reads: &[(&str, u64)]) -> Vec<Result<Option<ColdObjectPayload>>> {
        if reads.is_empty() {
            return Vec::new();
        }
        let mut results = (0..reads.len())
            .map(|_| {
                Err(StoreError::InvalidState(
                    "extent store pinned read not submitted".to_string(),
                ))
            })
            .collect::<Vec<_>>();
        let policy = self.pinned_read_policy;
        let mut locators = Vec::with_capacity(reads.len());
        for (index, (locator, expected_len)) in reads.iter().copied().enumerate() {
            if !policy.enabled {
                self.io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::Disabled);
                continue;
            }
            if expected_len < policy.min_value_bytes as u64 {
                self.io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::BelowMinValue);
                continue;
            }
            match ExtentStoreLocator::decode(locator) {
                Ok(locator) if locator.value_len == expected_len => locators.push((index, locator)),
                Ok(locator) => {
                    results[index] = Err(StoreError::InvalidState(format!(
                        "extent store locator length mismatch: expected {expected_len} actual {}",
                        locator.value_len
                    )));
                }
                Err(error) => results[index] = Err(error),
            }
        }
        let _permit = self.io_priority.enter_read();
        let mut reserved = Vec::with_capacity(locators.len());
        {
            let inner = self.inner.lock();
            for (index, locator) in locators {
                let segment = match inner.segments.get(&locator.segment_id) {
                    Some(segment) => segment,
                    None => {
                        results[index] = Err(StoreError::NotFound(format!(
                            "extent store segment {} is not open",
                            locator.segment_id
                        )));
                        continue;
                    }
                };
                if extent_store_locator_deleted(segment, &locator) {
                    results[index] = Err(StoreError::NotFound(format!(
                        "extent store locator in segment {} was deleted",
                        locator.segment_id
                    )));
                    continue;
                }
                let segment_len = segment.live_bytes.saturating_add(segment.dead_bytes) as usize;
                let packed = segment
                    .packed_blocks
                    .contains_key(&(locator.offset, locator.record_len));
                reserved.push(ReservedExtentStorePinnedRead {
                    index,
                    locator,
                    file: segment.file.clone(),
                    segment_len,
                    packed,
                });
            }
        }
        let prefetch_mode = pinned_prefetch_mode();
        if matches!(
            prefetch_mode,
            ExtentStorePinnedPrefetchMode::Fadvise | ExtentStorePinnedPrefetchMode::Readahead
        ) {
            prefetch_pinned_read_files(&reserved, prefetch_mode);
        }
        let mut mappings = BTreeMap::<u64, Result<std::sync::Arc<ExtentStoreMmap>>>::new();
        for read in reserved {
            let mapping = match mappings.entry(read.locator.segment_id) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.get().clone(),
                std::collections::btree_map::Entry::Vacant(entry) => entry
                    .insert(self.mmap_segment(
                        read.locator.segment_id,
                        read.file.clone(),
                        read.segment_len,
                        policy,
                    ))
                    .clone(),
            };
            results[read.index] = mapping.and_then(|mapping| {
                if matches!(
                    prefetch_mode,
                    ExtentStorePinnedPrefetchMode::Madvise | ExtentStorePinnedPrefetchMode::Touch
                ) {
                    prefetch_pinned_read_mapping(&mapping, &read, prefetch_mode)?;
                }
                self.pinned_payload_from_mapping(mapping, &read)
            });
        }
        results
    }

    fn pinned_payload_from_mapping(
        &self,
        mapping: std::sync::Arc<ExtentStoreMmap>,
        read: &ReservedExtentStorePinnedRead,
    ) -> Result<Option<ColdObjectPayload>> {
        let record_start = read.locator.offset as usize;
        let record_len = read.locator.record_len as usize;
        let record = mapping.slice(record_start, record_len)?;
        validate_pinned_record(&read.locator, record, read.packed)?;
        let value_offset = record_start
            .checked_add(read.locator.value_offset as usize)
            .ok_or_else(|| {
                StoreError::InvalidState("extent store pinned value offset overflow".to_string())
            })?;
        let value_len = read.locator.value_len as usize;
        self.io_counters.record_pinned_payload_create(value_len);
        self.io_counters.record_pinned_read(value_len);
        let payload = ColdPayloadRef::new(std::sync::Arc::new(ExtentStoreMmapPayloadRef {
            mapping,
            counters: self.io_counters.clone(),
            offset: value_offset,
            len: value_len,
        }));
        Ok(Some(ColdObjectPayload::Borrowed(payload)))
    }

    fn mmap_segment(
        &self,
        segment_id: u64,
        file: Arc<ExtentStoreFile>,
        len: usize,
        policy: ExtentStorePinnedReadPolicy,
    ) -> Result<std::sync::Arc<ExtentStoreMmap>> {
        {
            let mappings = self.mmap_cache.mappings.lock();
            if let Some(mapping) = mappings.get(&segment_id) {
                if mapping.len >= len {
                    self.io_counters.record_mmap_cache_hit();
                    return Ok(mapping.clone());
                }
                self.io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::MappingTooShort);
            }
        }
        self.io_counters.record_mmap_cache_miss();
        let active_refs = self.io_counters.pinned_payload_active_refs();
        let active_bytes = self.io_counters.pinned_payload_active_bytes();
        if active_refs >= policy.max_active_refs
            || active_bytes.saturating_add(len as u64) > policy.max_active_bytes
        {
            self.io_counters
                .record_pinned_fallback(ExtentStorePinnedFallbackReason::QuotaExceeded);
            return Err(StoreError::QuotaExceeded {
                kind: mooncake_store_core::error::QuotaKind::Bytes,
                message: "extent store pinned payload quota exceeded".to_string(),
            });
        }
        {
            let mappings = self.mmap_cache.mappings.lock();
            let current_bytes = mappings
                .values()
                .map(|mapping| mapping.len as u64)
                .sum::<u64>();
            let existing_len = mappings
                .get(&segment_id)
                .map(|mapping| mapping.len as u64)
                .unwrap_or(0);
            let new_bytes = current_bytes
                .saturating_sub(existing_len)
                .saturating_add(len as u64);
            if (existing_len == 0 && mappings.len() >= policy.max_mmap_segments)
                || new_bytes > policy.max_mmap_bytes
            {
                self.io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::QuotaExceeded);
                return Err(StoreError::QuotaExceeded {
                    kind: mooncake_store_core::error::QuotaKind::Bytes,
                    message: "extent store mmap cache quota exceeded".to_string(),
                });
            }
        }
        let mapping = match ExtentStoreMmap::map(file, len) {
            Ok(mapping) => {
                self.io_counters.record_mmap_create(true);
                std::sync::Arc::new(mapping)
            }
            Err(error) => {
                self.io_counters.record_mmap_create(false);
                self.io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::MmapFailed);
                return Err(error);
            }
        };
        let (segments, bytes) = {
            let mut mappings = self.mmap_cache.mappings.lock();
            mappings.insert(segment_id, mapping.clone());
            let bytes = mappings
                .values()
                .map(|mapping| mapping.len as u64)
                .sum::<u64>();
            (mappings.len(), bytes)
        };
        self.io_counters.record_mmap_cache_state(segments, bytes);
        Ok(mapping)
    }

    fn get_decoded_into(
        &self,
        locator: &ExtentStoreLocator,
        expected_len: u64,
        dst: &mut [u8],
    ) -> Result<()> {
        if locator.value_len != expected_len {
            return Err(StoreError::InvalidState(format!(
                "extent store locator length mismatch: expected {expected_len} actual {}",
                locator.value_len
            )));
        }
        if locator.value_len as usize > dst.len() {
            return Err(StoreError::InvalidState(format!(
                "extent store destination too small: need {} actual {}",
                locator.value_len,
                dst.len()
            )));
        }
        let _permit = self.io_priority.enter_read();
        let (file, direct_file) = {
            let inner = self.inner.lock();
            let segment = inner.segments.get(&locator.segment_id).ok_or_else(|| {
                StoreError::NotFound(format!(
                    "extent store segment {} is not open",
                    locator.segment_id
                ))
            })?;
            validate_extent_store_locator(segment, locator)?;
            (segment.file.clone(), segment.direct_file.clone())
        };
        let value_len = locator.value_len as usize;
        let (file, fixed_file_key) = direct_file
            .as_ref()
            .filter(|_| {
                direct_reads_enabled()
                    && direct_payload_io_compatible(
                        locator.offset + locator.value_offset,
                        &dst[..value_len],
                    )
            })
            .map(|file| {
                (
                    file,
                    ExtentStoreFixedFileKey::for_direct(locator.segment_id),
                )
            })
            .unwrap_or((
                &file,
                ExtentStoreFixedFileKey::for_buffered(locator.segment_id),
            ));
        self.read_exact_at(
            file,
            fixed_file_key,
            locator.offset + locator.value_offset,
            &mut dst[..value_len],
        )?;
        Ok(())
    }

    fn reserve_reads(
        &self,
        reads: &[(usize, ExtentStoreLocator)],
    ) -> Vec<Result<ExtentStoreReadFiles>> {
        let inner = self.inner.lock();
        let mut files = BTreeMap::<u64, Result<ExtentStoreReadFiles>>::new();
        reads
            .iter()
            .map(|(_, locator)| -> Result<ExtentStoreReadFiles> {
                if let Some(files) = files.get(&locator.segment_id) {
                    return files.clone();
                }
                let segment = inner.segments.get(&locator.segment_id).ok_or_else(|| {
                    StoreError::NotFound(format!(
                        "extent store segment {} is not open",
                        locator.segment_id
                    ))
                })?;
                validate_extent_store_locator(segment, locator)?;
                let files_for_segment = Ok((segment.file.clone(), segment.direct_file.clone()));
                files.insert(locator.segment_id, files_for_segment.clone());
                files_for_segment
            })
            .collect()
    }

    fn with_disjoint_read_buffers(
        &self,
        reads: &mut [ExtentStoreRead<'_>],
        reserved: Vec<ReservedExtentStoreReadWithFiles>,
        f: impl FnOnce(&mut [(usize, ReservedExtentStoreRead<'_>)]) -> Vec<Result<()>>,
    ) -> Vec<((usize, ExtentStoreLocator), Result<()>)> {
        let mut slots = reads
            .iter_mut()
            .map(|read| Some(&mut *read.dst))
            .collect::<Vec<_>>();
        let mut reserved_reads = reserved
            .into_iter()
            .map(|(index, locator, file, direct_file)| {
                let dst = slots[index]
                    .take()
                    .expect("each extent store batch read index should be unique");
                (
                    index,
                    ReservedExtentStoreRead {
                        locator,
                        file,
                        direct_file,
                        dst,
                    },
                )
            })
            .collect::<Vec<_>>();
        let read_results = f(&mut reserved_reads);
        reserved_reads
            .into_iter()
            .zip(read_results)
            .map(|((index, read), result)| ((index, read.locator), result))
            .collect()
    }

    fn delete(&self, locator: &str) -> Result<bool> {
        let locator = match ExtentStoreLocator::decode(locator) {
            Ok(locator) => locator,
            Err(_) => return Ok(false),
        };
        self.delete_decoded(&locator)
    }

    fn rollback_uncommitted_write(&self, locator: &ExtentStoreLocator) {
        let _permit = self.io_priority.enter_write();
        let mut removed_segment = None;
        {
            let mut inner = self.inner.lock();
            let active_segment_id = inner.active_segment_id;
            let Some(segment) = inner.segments.get_mut(&locator.segment_id) else {
                return;
            };
            if !apply_delete_to_segment(segment, locator) {
                return;
            }
            if segment.live_bytes == 0 && locator.segment_id != active_segment_id {
                removed_segment = inner.segments.remove(&locator.segment_id);
            }
        }
        if let Some(segment) = removed_segment {
            let _ = self.remove_segment_file(locator.segment_id, segment);
        }
    }

    fn delete_decoded(&self, locator: &ExtentStoreLocator) -> Result<bool> {
        let _permit = self.io_priority.enter_write();
        let mut removed_segment = None;
        {
            let mut inner = self.inner.lock();
            let active_segment_id = inner.active_segment_id;
            let Some(segment) = inner.segments.get(&locator.segment_id) else {
                return Ok(false);
            };
            if extent_store_locator_deleted(segment, locator) {
                return Ok(false);
            }
            if !extent_store_locator_within_segment(segment, locator)? {
                return Ok(false);
            }
            append_delete_journal_record(&mut inner.delete_journal, locator)?;
            let Some(segment) = inner.segments.get_mut(&locator.segment_id) else {
                return Ok(false);
            };
            if !apply_delete_to_segment(segment, locator) {
                return Ok(false);
            }
            if segment.live_bytes == 0 && locator.segment_id != active_segment_id {
                removed_segment = inner.segments.remove(&locator.segment_id);
            }
        }
        if let Some(segment) = removed_segment {
            self.remove_segment_file(locator.segment_id, segment)?;
        }
        Ok(true)
    }

    fn is_extent_store_locator(value: &str) -> bool {
        value.starts_with(EXTENT_STORE_LOCATOR_PREFIX)
            && value.as_bytes().get(EXTENT_STORE_LOCATOR_PREFIX.len()) == Some(&b':')
    }

    fn remove_segment_file(&self, segment_id: u64, segment: ExtentStoreSegment) -> Result<()> {
        if let ExtentStoreIo::WorkerPair(pair) = &self.io {
            pair.read_worker.unregister_segment_files(segment_id)?;
            pair.write_worker.unregister_segment_files(segment_id)?;
        }
        let (segments, bytes) = {
            let mut mappings = self.mmap_cache.mappings.lock();
            mappings.remove(&segment_id);
            let bytes = mappings
                .values()
                .map(|mapping| mapping.len as u64)
                .sum::<u64>();
            (mappings.len(), bytes)
        };
        self.io_counters.record_mmap_cache_state(segments, bytes);
        let path = segment.path.clone();
        drop(segment);
        remove_extent_store_segment_file(&path)?;
        Ok(())
    }

    fn read_exact_at(
        &self,
        file: &ExtentStoreFile,
        fixed_file_key: ExtentStoreFixedFileKey,
        mut offset: u64,
        mut payload: &mut [u8],
    ) -> Result<()> {
        while !payload.is_empty() {
            let read = self.submit_read(file, fixed_file_key, offset, payload)?;
            if read == 0 {
                return Err(StoreError::NotFound(
                    "extent store read reached EOF".to_string(),
                ));
            }
            offset = offset.saturating_add(read as u64);
            let (_, rest) = payload.split_at_mut(read);
            payload = rest;
        }
        Ok(())
    }

    fn write_all_batch_at(
        &self,
        writes: &mut [(usize, ReservedExtentStoreWrite<'_>)],
    ) -> Vec<Result<()>> {
        match &self.io {
            ExtentStoreIo::WorkerPair(pair) => {
                let lanes = writes
                    .iter()
                    .map(|(_, write)| write_lane(write))
                    .collect::<Vec<_>>();
                let has_buffered = lanes
                    .iter()
                    .any(|lane| matches!(lane, ExtentStoreWriteLane::Buffered));
                if has_buffered {
                    let bytes = writes
                        .iter()
                        .map(|(_, write)| write.record.record_len as usize)
                        .sum::<usize>();
                    self.io_counters
                        .buffered_write_ops
                        .fetch_add(writes.len() as u64, AtomicOrdering::Relaxed);
                    self.io_counters
                        .record_buffered_write_blocking_batch(writes.len(), bytes);
                    write_buffered_batch_blocking(writes)
                } else {
                    for lane in lanes {
                        match lane {
                            ExtentStoreWriteLane::Direct => self
                                .io_counters
                                .direct_write_ops
                                .fetch_add(1, AtomicOrdering::Relaxed),
                            ExtentStoreWriteLane::DirectScratch => self
                                .io_counters
                                .direct_scratch_write_ops
                                .fetch_add(1, AtomicOrdering::Relaxed),
                            ExtentStoreWriteLane::Buffered => unreachable!(
                                "buffered writes are handled by the blocking batch path"
                            ),
                        };
                    }
                    submit_write_batch_io_worker(&pair.write_worker, writes)
                }
            }
            ExtentStoreIo::Blocking(lock) => {
                let _blocking = lock.lock();
                let mut scratch = match ExtentStoreScratchWorkspace::new(&self.io_counters) {
                    Ok(scratch) => scratch,
                    Err(error) => {
                        return writes
                            .iter()
                            .map(|_| Err(StoreError::Transport(format!("{error}"))))
                            .collect();
                    }
                };
                writes
                    .iter()
                    .map(|(_, write)| match write_lane(write) {
                        ExtentStoreWriteLane::Direct | ExtentStoreWriteLane::Buffered => {
                            self.io_counters
                                .buffered_write_ops
                                .fetch_add(1, AtomicOrdering::Relaxed);
                            write_vectored_all_at_blocking(
                                &write.file,
                                write.locator.offset,
                                &write.record,
                            )
                        }
                        ExtentStoreWriteLane::DirectScratch => {
                            let file = write
                                .direct_file
                                .as_ref()
                                .expect("direct scratch lane requires direct file");
                            self.io_counters
                                .direct_scratch_write_ops
                                .fetch_add(1, AtomicOrdering::Relaxed);
                            write_direct_record_blocking(
                                file,
                                write.locator.offset,
                                &write.record,
                                &mut scratch,
                            )
                        }
                    })
                    .collect()
            }
        }
    }

    fn read_batch_with_packed_blocks_at(
        &self,
        reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
        record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<()>> {
        let packed_group_started = std::time::Instant::now();
        let packed_blocks = reads
            .iter()
            .enumerate()
            .filter(|(_, (_, read))| {
                read.locator.value_len <= EXTENT_STORE_PACKED_BLOCK_READ_MAX_ENTRY_BYTES
            })
            .fold(
                BTreeMap::<(u64, u64, u64), Vec<PackedExtentStoreReadEntry>>::new(),
                |mut blocks, (index, (_, read))| {
                    blocks.entry(read.locator.block_key()).or_default().push(
                        PackedExtentStoreReadEntry {
                            index,
                            value_offset: read.locator.value_offset,
                        },
                    );
                    blocks
                },
            )
            .into_iter()
            .filter(|(_, entries)| entries.len() > 1)
            .map(|(block, mut entries)| {
                entries.sort_by_key(|entry| entry.value_offset);
                let indices = entries
                    .into_iter()
                    .map(|entry| entry.index)
                    .collect::<Vec<_>>();
                (block, indices)
            })
            .collect::<Vec<_>>();
        record_stage(
            "engine_read_build_packed_groups",
            packed_group_started.elapsed(),
        );
        if packed_blocks.is_empty() {
            let fallback_started = std::time::Instant::now();
            let results = self.read_batch_at(reads);
            record_stage("engine_read_fallback_values", fallback_started.elapsed());
            return results;
        }
        let span_read_limit = packed_span_max_read_bytes(reads, &packed_blocks);
        let mut completed = vec![false; reads.len()];
        let mut results = (0..reads.len()).map(|_| Ok(())).collect::<Vec<_>>();
        let packed_read_started = std::time::Instant::now();
        self.read_packed_block_spans(
            reads,
            packed_blocks,
            span_read_limit,
            &mut completed,
            &mut results,
        );
        record_stage("engine_read_packed_blocks", packed_read_started.elapsed());
        let fallback_started = std::time::Instant::now();
        for (index, (_, read)) in reads.iter_mut().enumerate() {
            if completed.get(index).copied().unwrap_or(false) {
                continue;
            }
            let value_len = read.locator.value_len as usize;
            let (file, fixed_file_key) = match read.direct_file.as_ref().filter(|_| {
                direct_reads_enabled()
                    && direct_payload_io_compatible(
                        read.locator.offset + read.locator.value_offset,
                        &read.dst[..value_len],
                    )
            }) {
                Some(file) => (
                    file,
                    ExtentStoreFixedFileKey::for_direct(read.locator.segment_id),
                ),
                None => (
                    &read.file,
                    ExtentStoreFixedFileKey::for_buffered(read.locator.segment_id),
                ),
            };
            results[index] = self.read_exact_at(
                file,
                fixed_file_key,
                read.locator.offset + read.locator.value_offset,
                &mut read.dst[..value_len],
            );
        }
        record_stage("engine_read_fallback_values", fallback_started.elapsed());
        results
    }

    fn is_live_packed_block(&self, segment_id: u64, offset: u64, record_len: u64) -> bool {
        let inner = self.inner.lock();
        inner
            .segments
            .get(&segment_id)
            .and_then(|segment| segment.packed_blocks.get(&(offset, record_len)))
            .is_some_and(|state| state.live_count > 0 && state.live_count <= state.entry_count)
    }

    fn read_packed_block_spans(
        &self,
        reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
        packed_blocks: Vec<((u64, u64, u64), Vec<usize>)>,
        span_read_limit: u64,
        completed: &mut [bool],
        results: &mut [Result<()>],
    ) {
        let mut blocks = packed_blocks
            .into_iter()
            .filter_map(|((segment_id, offset, record_len), indices)| {
                if !self.is_live_packed_block(segment_id, offset, record_len) {
                    return None;
                }
                let packed_entry_count =
                    packed_block_entry_count(reads, &indices).unwrap_or(indices.len());
                Some(PackedExtentStoreReadBlock {
                    segment_id,
                    offset,
                    record_len,
                    indices,
                    packed_entry_count,
                })
            })
            .collect::<Vec<_>>();
        blocks.sort_by_key(|block| (block.segment_id, block.offset));
        let mut cursor = 0usize;
        while cursor < blocks.len() {
            let start = cursor;
            let segment_id = blocks[cursor].segment_id;
            let span_start = blocks[cursor].offset;
            let mut span_end = blocks[cursor].offset + blocks[cursor].record_len;
            let mut requested_bytes = blocks[cursor].record_len;
            cursor += 1;
            while cursor < blocks.len() {
                let next = &blocks[cursor];
                if next.segment_id != segment_id {
                    break;
                }
                let gap = next.offset.saturating_sub(span_end);
                let next_span_end = span_end.max(next.offset + next.record_len);
                let next_span_len = next_span_end - span_start;
                let next_requested = requested_bytes.saturating_add(next.record_len);
                if gap > EXTENT_STORE_PACKED_SPAN_MAX_GAP
                    || next_span_len > span_read_limit
                    || next_span_len
                        > next_requested
                            .saturating_mul(EXTENT_STORE_PACKED_SPAN_MAX_READ_AMPLIFICATION_NUM)
                {
                    break;
                }
                span_end = next_span_end;
                requested_bytes = next_requested;
                cursor += 1;
            }
            let span_blocks = &blocks[start..cursor];
            if span_blocks.len() >= EXTENT_STORE_PACKED_SPAN_MIN_BLOCKS {
                match self.read_packed_block_span(reads, span_blocks, span_start, span_end) {
                    Ok(()) => {
                        for block in span_blocks {
                            for index in &block.indices {
                                completed[*index] = true;
                            }
                        }
                    }
                    Err(error) => {
                        for block in span_blocks {
                            for index in &block.indices {
                                completed[*index] = true;
                                results[*index] = Err(error.clone());
                            }
                        }
                    }
                }
                continue;
            }
            let block = &span_blocks[0];
            match self.read_packed_block_group(reads, &block.indices) {
                Ok(()) => {
                    for index in &block.indices {
                        completed[*index] = true;
                    }
                }
                Err(error) => {
                    for index in &block.indices {
                        completed[*index] = true;
                        results[*index] = Err(error.clone());
                    }
                }
            }
        }
    }

    fn read_packed_block_span(
        &self,
        reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
        blocks: &[PackedExtentStoreReadBlock],
        span_start: u64,
        span_end: u64,
    ) -> Result<()> {
        let first = &reads[blocks[0].indices[0]].1;
        let span_len = (span_end - span_start) as usize;
        let mut buffer = self.buffer_pool.lease(span_len)?;
        let (file, fixed_file_key) = first
            .direct_file
            .as_ref()
            .filter(|_| {
                is_aligned_u64(span_start)
                    && is_aligned_u64(span_end - span_start)
                    && direct_payload_io_compatible(span_start, &buffer[..span_len])
            })
            .map(|file| {
                (
                    file,
                    ExtentStoreFixedFileKey::for_direct(first.locator.segment_id),
                )
            })
            .unwrap_or((
                &first.file,
                ExtentStoreFixedFileKey::for_buffered(first.locator.segment_id),
            ));
        self.read_exact_at(file, fixed_file_key, span_start, &mut buffer[..span_len])?;
        let buffer = &buffer[..span_len];
        let logical_reads = blocks
            .iter()
            .map(|block| block.packed_entry_count)
            .sum::<usize>();
        let requested_bytes = blocks.iter().map(|block| block.record_len).sum::<u64>() as usize;
        self.io_counters
            .record_coalesced_read_group(logical_reads, requested_bytes, span_len);
        let mut verified_blocks = BTreeSet::new();
        for block in blocks {
            let block_start = (block.offset - span_start) as usize;
            let block_end = block_start + block.record_len as usize;
            if block_end > buffer.len() {
                return Err(StoreError::NotFound(
                    "extent store packed span reached EOF".to_string(),
                ));
            }
            let block_buffer = &buffer[block_start..block_end];
            let first_locator = &reads[block.indices[0]].1.locator;
            validate_packed_block_buffer(block_buffer, first_locator)?;
            if verified_blocks.insert((block.offset, block.record_len)) {
                maybe_validate_packed_block_payload(block_buffer, first_locator)?;
            }
            read_packed_block_entries(block_buffer, reads, &block.indices)?;
        }
        Ok(())
    }

    fn read_packed_block_group(
        &self,
        reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
        indices: &[usize],
    ) -> Result<()> {
        let first = &reads[indices[0]].1;
        let record_len = first.locator.record_len as usize;
        let mut buffer = self.buffer_pool.lease(record_len)?;
        let (file, fixed_file_key) = first
            .direct_file
            .as_ref()
            .filter(|_| {
                is_aligned_u64(first.locator.offset)
                    && is_aligned_u64(first.locator.record_len)
                    && direct_payload_io_compatible(first.locator.offset, &buffer[..record_len])
            })
            .map(|file| {
                (
                    file,
                    ExtentStoreFixedFileKey::for_direct(first.locator.segment_id),
                )
            })
            .unwrap_or((
                &first.file,
                ExtentStoreFixedFileKey::for_buffered(first.locator.segment_id),
            ));
        self.read_exact_at(
            file,
            fixed_file_key,
            first.locator.offset,
            &mut buffer[..record_len],
        )?;
        let buffer = &buffer[..record_len];
        validate_packed_block_buffer(buffer, &first.locator)?;
        maybe_validate_packed_block_payload(buffer, &first.locator)?;
        self.io_counters.record_coalesced_read_group(
            indices.len(),
            indices
                .iter()
                .map(|index| reads[*index].1.locator.value_len as usize)
                .sum::<usize>(),
            record_len,
        );
        read_packed_block_entries(buffer, reads, indices)?;
        Ok(())
    }

    fn read_batch_at(&self, reads: &mut [(usize, ReservedExtentStoreRead<'_>)]) -> Vec<Result<()>> {
        if !self.local_read_lanes.gds.enabled() && matches!(&self.io, ExtentStoreIo::WorkerPair(_))
        {
            if iouring_reads_enabled() {
                let worker = match &self.io {
                    ExtentStoreIo::WorkerPair(p) => &p.read_worker,
                    _ => unreachable!(),
                };
                let no_completed = vec![false; reads.len()];
                return submit_read_batch_io_worker(worker, reads, &no_completed);
            }
            return self
                .parallel_buffered_reads
                .read_batch(reads, &[], &self.io_counters);
        }

        let gds_completed = self.try_gds_reads(reads);
        match &self.io {
            ExtentStoreIo::WorkerPair(pair) => {
                if iouring_reads_enabled() {
                    return submit_read_batch_io_worker(&pair.read_worker, reads, &gds_completed);
                }
                let all_buffered = reads.iter().enumerate().all(|(index, (_, read))| {
                    gds_completed.get(index).copied().unwrap_or(false)
                        || matches!(iouring_read_lane(read), ExtentStoreLocalReadLane::Buffered)
                });
                if all_buffered {
                    self.parallel_buffered_reads.read_batch(
                        reads,
                        &gds_completed,
                        &self.io_counters,
                    )
                } else {
                    submit_read_batch_io_worker(&pair.read_worker, reads, &gds_completed)
                }
            }
            ExtentStoreIo::Blocking(lock) => {
                let _blocking = lock.lock();
                let mut scratch = match ExtentStoreScratchWorkspace::new(&self.io_counters) {
                    Ok(scratch) => scratch,
                    Err(error) => {
                        return reads
                            .iter()
                            .enumerate()
                            .map(|(index, _)| {
                                if gds_completed.get(index).copied().unwrap_or(false) {
                                    Ok(())
                                } else {
                                    Err(StoreError::Transport(format!("{error}")))
                                }
                            })
                            .collect();
                    }
                };
                reads
                    .iter_mut()
                    .enumerate()
                    .map(|(index, (_, read))| {
                        if gds_completed.get(index).copied().unwrap_or(false) {
                            return Ok(());
                        }
                        let value_len = read.locator.value_len as usize;
                        match self.local_read_lanes.choose_read_lane(read) {
                            ExtentStoreLocalReadLane::IoUringDirect => {
                                let file = read
                                    .direct_file
                                    .as_ref()
                                    .expect("direct read lane requires direct file");
                                read_exact_at_blocking(
                                    file,
                                    read.locator.offset + read.locator.value_offset,
                                    &mut read.dst[..value_len],
                                )
                            }
                            ExtentStoreLocalReadLane::IoUringDirectScratch => {
                                let file = read
                                    .direct_file
                                    .as_ref()
                                    .expect("direct scratch read lane requires direct file");
                                read_exact_with_scratch_at_blocking(
                                    file,
                                    read.locator.offset + read.locator.value_offset,
                                    &mut read.dst[..value_len],
                                    &mut scratch,
                                )
                            }
                            ExtentStoreLocalReadLane::Buffered
                            | ExtentStoreLocalReadLane::GdsCuFile => read_exact_at_blocking(
                                &read.file,
                                read.locator.offset + read.locator.value_offset,
                                &mut read.dst[..value_len],
                            ),
                        }
                    })
                    .collect()
            }
        }
    }

    fn try_gds_reads(&self, reads: &mut [(usize, ReservedExtentStoreRead<'_>)]) -> Vec<bool> {
        let mut completed = vec![false; reads.len()];
        for (index, (_, read)) in reads.iter_mut().enumerate() {
            match self.local_read_lanes.gds.eligible(read) {
                Ok(()) => {
                    let value_len = read.locator.value_len as usize;
                    let file = read
                        .direct_file
                        .as_ref()
                        .expect("GDS eligibility requires direct file");
                    self.io_counters.record_gds_read(value_len);
                    let result = self.local_read_lanes.gds.try_read(ExtentStoreGdsRead {
                        file,
                        segment_id: read.locator.segment_id,
                        offset: read.locator.offset + read.locator.value_offset,
                        dst: &mut read.dst[..value_len],
                    });
                    match result {
                        Ok(()) => {
                            completed[index] = true;
                        }
                        Err(error) => {
                            self.io_counters.record_gds_submit_error();
                            self.io_counters.record_gds_fallback_to_iouring();
                            warn!(
                                error = %error,
                                segment_id = read.locator.segment_id,
                                "extent store GDS read failed; falling back to io_uring lane"
                            );
                            self.record_iouring_read_lane(read);
                        }
                    }
                }
                Err(reason) => {
                    self.io_counters.record_gds_reject(reason);
                    self.record_iouring_read_lane(read);
                }
            }
        }
        completed
    }

    fn record_iouring_read_lane(&self, read: &ReservedExtentStoreRead<'_>) {
        self.io_counters
            .record_read_lane(iouring_read_lane(read), read.locator.value_len as usize);
    }

    fn submit_read(
        &self,
        file: &ExtentStoreFile,
        fixed_file_key: ExtentStoreFixedFileKey,
        offset: u64,
        payload: &mut [u8],
    ) -> Result<usize> {
        match &self.io {
            ExtentStoreIo::WorkerPair(pair) => {
                let op = ExtentStoreIoOp::Read {
                    fd: file.as_raw_fd(),
                    buf: payload.as_mut_ptr(),
                    len: payload.len() as u32,
                    offset,
                    fixed_file_key: Some(fixed_file_key),
                    fixed_buffer: None,
                };
                pair.read_worker.submit_read(op, payload.len())
            }
            ExtentStoreIo::Blocking(lock) => {
                let _blocking = lock.lock();
                file.read_at(payload, offset).map_err(|error| {
                    StoreError::Transport(format!("extent store blocking pread failed: {error}"))
                })
            }
        }
    }
}

fn validate_extent_store_locator(
    segment: &ExtentStoreSegment,
    locator: &ExtentStoreLocator,
) -> Result<()> {
    if extent_store_locator_deleted(segment, locator) {
        return Err(StoreError::NotFound(format!(
            "extent store locator in segment {} was deleted",
            locator.segment_id
        )));
    }
    if !extent_store_locator_within_segment(segment, locator)? {
        let locator_end = locator
            .offset
            .checked_add(locator.record_len)
            .ok_or_else(|| {
                StoreError::InvalidState("extent store locator bounds overflow".to_string())
            })?;
        let segment_len = segment.live_bytes.saturating_add(segment.dead_bytes);
        return Err(StoreError::InvalidState(format!(
            "extent store locator exceeds segment bounds: end {locator_end} segment_len {segment_len}"
        )));
    }
    Ok(())
}

fn extent_store_locator_within_segment(
    segment: &ExtentStoreSegment,
    locator: &ExtentStoreLocator,
) -> Result<bool> {
    let locator_end = locator
        .offset
        .checked_add(locator.record_len)
        .ok_or_else(|| {
            StoreError::InvalidState("extent store locator bounds overflow".to_string())
        })?;
    Ok(locator_end <= segment.live_bytes.saturating_add(segment.dead_bytes))
}

fn extent_store_locator_deleted(
    segment: &ExtentStoreSegment,
    locator: &ExtentStoreLocator,
) -> bool {
    if segment
        .deleted_extents
        .contains(&(locator.value_offset, locator.value_len))
    {
        return true;
    }
    segment
        .deleted_extents
        .contains(&(locator.offset, locator.record_len))
}

fn apply_delete_to_segment(segment: &mut ExtentStoreSegment, locator: &ExtentStoreLocator) -> bool {
    if segment
        .packed_blocks
        .contains_key(&(locator.offset, locator.record_len))
    {
        let entry_key = (locator.value_offset, locator.value_len);
        if !segment.deleted_extents.insert(entry_key) {
            return false;
        }
        let state = segment
            .packed_blocks
            .get_mut(&(locator.offset, locator.record_len))
            .expect("packed block state exists");
        state.live_count = state.live_count.saturating_sub(1);
        state.live_value_bytes = state.live_value_bytes.saturating_sub(locator.value_len);
        state.dead_value_bytes = state.dead_value_bytes.saturating_add(locator.value_len);
        if state.live_count == 0 {
            segment.live_bytes = segment.live_bytes.saturating_sub(locator.record_len);
            segment.dead_bytes = segment.dead_bytes.saturating_add(locator.record_len);
        }
        return true;
    }
    if !segment
        .deleted_extents
        .insert((locator.offset, locator.record_len))
    {
        return false;
    }
    segment.live_bytes = segment.live_bytes.saturating_sub(locator.record_len);
    segment.dead_bytes = segment.dead_bytes.saturating_add(locator.record_len);
    true
}

struct EncodedExtentStoreRecordReservation<'a> {
    encoded: EncodedExtentStoreRecord<'a>,
    result_locators: Vec<(usize, ExtentStoreLocator)>,
}

struct RecoveredPackedRecordPayload<'a> {
    file: &'a ExtentStoreFile,
    segment_id: u64,
    offset: u64,
    record_len: u64,
    value_offset: u64,
    value_len: u64,
    checksum: Option<u64>,
    entry_count: u64,
}

struct ExtentStoreRecordReservationContext<'a, 'b> {
    inner: &'a mut ExtentStoreEngineInner,
    root: &'a ExtentStorePath,
    counters: &'a ExtentStoreIoCounters,
    results: &'a mut [Result<String>],
    reserved: &'a mut Vec<(usize, ReservedExtentStoreWrite<'b>)>,
    record_stage: &'a mut dyn FnMut(&'static str, std::time::Duration),
}

fn reserve_encoded_extent_store_record<'record>(
    context: &mut ExtentStoreRecordReservationContext<'_, 'record>,
    index: usize,
    reservation: EncodedExtentStoreRecordReservation<'record>,
) {
    let EncodedExtentStoreRecordReservation {
        encoded,
        result_locators,
    } = reservation;
    let inner = &mut *context.inner;
    if inner.active_offset + encoded.record_len > inner.segment_size {
        let next_segment_id = inner.active_segment_id.saturating_add(1);
        inner.active_segment_id = next_segment_id;
        inner.active_offset = 0;
        let rollover_started = std::time::Instant::now();
        let segment = match open_segment(context.root, next_segment_id, inner.segment_size) {
            Ok(segment) => segment,
            Err(error) => {
                context.results[index] = Err(error);
                return;
            }
        };
        let rollover_elapsed = rollover_started.elapsed();
        context.counters.record_segment_rollover(rollover_elapsed);
        (context.record_stage)("engine_reserve_rollover_open", rollover_elapsed);
        inner.segments.insert(next_segment_id, segment);
    }
    let segment_id = inner.active_segment_id;
    let offset = inner.active_offset;
    inner.active_offset = inner.active_offset.saturating_add(encoded.record_len);
    let generation = inner.generation;
    let Some(segment) = inner.segments.get_mut(&segment_id) else {
        context.results[index] = Err(StoreError::InvalidState(format!(
            "active extent store segment {segment_id} is not open"
        )));
        return;
    };
    segment.live_bytes = segment.live_bytes.saturating_add(encoded.record_len);
    if encoded.entry_count > 1 {
        segment.packed_blocks.insert(
            (offset, encoded.record_len),
            PackedBlockLiveState {
                entry_count: encoded.entry_count,
                live_count: encoded.entry_count,
                live_value_bytes: encoded.value_len,
                dead_value_bytes: 0,
            },
        );
    }
    let file = segment.file.clone();
    let direct_file = segment.direct_file.clone();
    let block_locator = ExtentStoreLocator {
        segment_id,
        offset,
        record_len: encoded.record_len,
        value_offset: encoded.value_offset,
        value_len: encoded.value_len,
        generation,
    };
    let locators = if result_locators.is_empty() {
        vec![(index, block_locator)]
    } else {
        result_locators
            .into_iter()
            .map(|(result_index, mut locator)| {
                locator.segment_id = segment_id;
                locator.offset = offset;
                locator.record_len = encoded.record_len;
                locator.generation = generation;
                (result_index, locator)
            })
            .collect()
    };
    context.reserved.push((
        index,
        ReservedExtentStoreWrite {
            locator: block_locator,
            result_locators: locators,
            file,
            direct_file,
            record: encoded,
        },
    ));
}

fn with_disjoint_cold_read_buffers(
    reads: &mut [ColdObjectRead<'_, '_>],
    indices: Vec<usize>,
    f: impl FnOnce(&mut [ExtentStoreRead<'_>]) -> Vec<Result<Option<usize>>>,
) -> Vec<(usize, Result<Option<usize>>)> {
    let metadata = indices
        .iter()
        .map(|index| {
            (
                *index,
                reads[*index].cold_backing.object_locator.clone(),
                reads[*index].cold_backing.length,
            )
        })
        .collect::<Vec<_>>();
    let mut slots = reads
        .iter_mut()
        .map(|read| Some(&mut *read.dst))
        .collect::<Vec<_>>();
    let mut read_requests = metadata
        .iter()
        .map(|(index, locator, expected_len)| {
            let dst = slots[*index]
                .take()
                .expect("each cold backend batch read index should be unique");
            ExtentStoreRead {
                locator,
                expected_len: *expected_len,
                dst,
            }
        })
        .collect::<Vec<_>>();
    let results = f(&mut read_requests);
    metadata
        .into_iter()
        .zip(results)
        .map(|((index, _, _), result)| (index, result))
        .collect()
}

struct AlignedExtentStoreBuffer {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for AlignedExtentStoreBuffer {}

impl AlignedExtentStoreBuffer {
    fn zeroed(len: usize) -> Result<Self> {
        let buffer = Self::uninitialized(len)?;
        unsafe {
            std::ptr::write_bytes(buffer.ptr, 0, buffer.len);
        }
        Ok(buffer)
    }

    fn uninitialized(len: usize) -> Result<Self> {
        if len == 0 {
            return Ok(Self {
                ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(),
                len,
            });
        }
        let mut ptr = std::ptr::null_mut();
        let result =
            unsafe { libc::posix_memalign(&mut ptr, EXTENT_STORE_ALIGNMENT as usize, len) };
        if result != 0 {
            return Err(StoreError::Transport(format!(
                "failed to allocate aligned extent store buffer: {}",
                io::Error::from_raw_os_error(result)
            )));
        }
        Ok(Self {
            ptr: ptr.cast::<u8>(),
            len,
        })
    }

    fn ensure_uninitialized_len(&mut self, len: usize) -> Result<bool> {
        if self.len >= len {
            return Ok(false);
        }
        *self = Self::uninitialized(len)?;
        Ok(len != 0)
    }
}

struct ExtentStoreScratchWorkspace<'a> {
    write: AlignedExtentStoreBuffer,
    read: AlignedExtentStoreBuffer,
    counters: &'a ExtentStoreIoCounters,
}

impl<'a> ExtentStoreScratchWorkspace<'a> {
    fn new(counters: &'a ExtentStoreIoCounters) -> Result<Self> {
        Ok(Self {
            write: AlignedExtentStoreBuffer::zeroed(0)?,
            read: AlignedExtentStoreBuffer::zeroed(0)?,
            counters,
        })
    }

    fn materialize_record<'b>(
        &'b mut self,
        record: &EncodedExtentStoreRecord<'_>,
    ) -> Result<&'b [u8]> {
        let len = record.record_len as usize;
        let allocated = self.write.ensure_uninitialized_len(len)?;
        if allocated {
            self.counters.record_scratch_alloc(len);
        }
        materialize_direct_record_into(record, &mut self.write)?;
        Ok(&self.write[..len])
    }

    fn read_buffer(&mut self, len: usize) -> Result<&mut [u8]> {
        let allocated = self.read.ensure_uninitialized_len(len)?;
        if allocated {
            self.counters.record_scratch_alloc(len);
        }
        Ok(&mut self.read[..len])
    }
}

impl Deref for AlignedExtentStoreBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for AlignedExtentStoreBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedExtentStoreBuffer {
    fn drop(&mut self) {
        if self.len != 0 {
            unsafe {
                libc::free(self.ptr.cast::<c_void>());
            }
        }
    }
}

#[cfg_attr(test, allow(dead_code))]
fn direct_file_for_write<'a>(
    write: &'a ReservedExtentStoreWrite<'_>,
) -> Option<&'a ExtentStoreFile> {
    let max_scratch_record_len = env_u64(
        EXTENT_STORE_DIRECT_SCRATCH_MAX_WRITE_BYTES_ENV,
        EXTENT_STORE_DIRECT_SCRATCH_DEFAULT_MAX_RECORD_LEN,
    )
    .min(EXTENT_STORE_DIRECT_SCRATCH_MAX_RECORD_LEN);
    write.direct_file.as_deref().filter(|_| {
        is_aligned_u64(write.locator.offset)
            && is_aligned_u64(write.record.record_len)
            && write.record.record_len <= max_scratch_record_len
    })
}

fn write_lane(write: &ReservedExtentStoreWrite<'_>) -> ExtentStoreWriteLane {
    if write.record.payload.as_slice().is_none() {
        return ExtentStoreWriteLane::Buffered;
    }
    if direct_record_io_compatible(write.locator.offset, &write.record) {
        ExtentStoreWriteLane::Direct
    } else if direct_file_for_write(write).is_some() {
        ExtentStoreWriteLane::DirectScratch
    } else {
        ExtentStoreWriteLane::Buffered
    }
}

#[cfg_attr(test, allow(dead_code))]
fn direct_record_io_compatible(offset: u64, record: &EncodedExtentStoreRecord<'_>) -> bool {
    is_aligned_u64(offset)
        && is_aligned_u64(record.record_len)
        && direct_iovec_compatible(&record.prefix)
        && record
            .payload
            .as_slice()
            .is_some_and(direct_iovec_compatible)
        && direct_iovec_compatible(&record.suffix_padding)
}

fn direct_payload_io_compatible(offset: u64, payload: &[u8]) -> bool {
    is_aligned_u64(offset) && direct_iovec_compatible(payload)
}

fn direct_iovec_compatible(buffer: &[u8]) -> bool {
    buffer.is_empty()
        || (is_aligned_usize(buffer.as_ptr() as usize) && is_aligned_usize(buffer.len()))
}

fn is_aligned_u64(value: u64) -> bool {
    value.is_multiple_of(EXTENT_STORE_ALIGNMENT)
}

fn is_aligned_usize(value: usize) -> bool {
    value.is_multiple_of(EXTENT_STORE_ALIGNMENT as usize)
}

#[cfg_attr(not(test), allow(dead_code))]
fn materialize_direct_record(
    arena: &std::sync::Arc<ExtentStoreScratchArena>,
    record: &EncodedExtentStoreRecord<'_>,
) -> Result<ExtentStoreScratchRecord> {
    let record_len = record.record_len as usize;
    let mut buffer = arena.lease(record_len)?;
    materialize_direct_record_into(record, &mut buffer)?;
    Ok(ExtentStoreScratchRecord { buffer, record_len })
}

fn materialize_direct_record_into(
    record: &EncodedExtentStoreRecord<'_>,
    buffer: &mut [u8],
) -> Result<()> {
    let record_len = record.record_len as usize;
    if buffer.len() < record_len {
        return Err(StoreError::InvalidState(format!(
            "extent store direct scratch too small: need {record_len} actual {}",
            buffer.len()
        )));
    }
    let value_offset = record.value_offset as usize;
    let Some(payload) = record.payload.as_slice() else {
        return Err(StoreError::InvalidState(
            "extent store borrowed-slice record requires vectored write".to_string(),
        ));
    };
    buffer[..record.prefix.len()].copy_from_slice(&record.prefix);
    buffer[value_offset..value_offset + payload.len()].copy_from_slice(payload);
    let suffix_start = value_offset + payload.len();
    buffer[suffix_start..suffix_start + record.suffix_padding.len()]
        .copy_from_slice(&record.suffix_padding);
    Ok(())
}

fn write_direct_record_blocking(
    file: &ExtentStoreFile,
    offset: u64,
    record: &EncodedExtentStoreRecord<'_>,
    scratch: &mut ExtentStoreScratchWorkspace,
) -> Result<()> {
    let direct_record = scratch.materialize_record(record)?;
    write_all_at_blocking(file, offset, direct_record)
}

fn write_buffered_batch_blocking(
    writes: &mut [(usize, ReservedExtentStoreWrite<'_>)],
) -> Vec<Result<()>> {
    writes
        .iter()
        .map(|(_, write)| {
            write_vectored_all_at_blocking(&write.file, write.locator.offset, &write.record)
        })
        .collect()
}

fn should_use_pinned_mmap_for_pinned_reads(
    reads: &[ColdObjectPinnedRead<'_>],
    access_tracker: &Mutex<ExtentStorePinnedAccessTracker>,
) -> bool {
    if !pinned_cold_ordinary_fallback_enabled() || reads.len() < 2 {
        record_pinned_access_batch(reads, access_tracker);
        return false;
    }
    let mut locators = Vec::with_capacity(reads.len());
    let mut total_value_bytes = 0u64;
    for read in reads {
        if !ExtentStoreEngine::is_extent_store_locator(&read.cold_backing.object_locator) {
            record_pinned_access_batch(reads, access_tracker);
            return false;
        }
        let Ok(locator) = ExtentStoreLocator::decode(&read.cold_backing.object_locator) else {
            record_pinned_access_batch(reads, access_tracker);
            return false;
        };
        if locator.value_len != read.cold_backing.length {
            record_pinned_access_batch(reads, access_tracker);
            return false;
        }
        total_value_bytes = total_value_bytes.saturating_add(locator.value_len);
        locators.push((read.cold_backing.object_locator.as_str(), locator));
    }
    if total_value_bytes < EXTENT_STORE_PINNED_COLD_ORDINARY_MIN_BATCH_BYTES {
        record_pinned_access_batch(reads, access_tracker);
        return false;
    }
    let mut sorted = locators
        .iter()
        .map(|(_, locator)| *locator)
        .collect::<Vec<_>>();
    sorted.sort_by_key(|locator| (locator.segment_id, locator.offset));
    let mut previous_segment_id = sorted[0].segment_id;
    let mut previous_end = sorted[0].offset.saturating_add(sorted[0].record_len);
    for locator in sorted.iter().skip(1) {
        if locator.segment_id != previous_segment_id {
            previous_segment_id = locator.segment_id;
            previous_end = locator.offset.saturating_add(locator.record_len);
            continue;
        }
        if locator.offset < previous_end {
            record_pinned_access_batch(reads, access_tracker);
            return false;
        }
        if locator.offset.saturating_sub(previous_end)
            > EXTENT_STORE_PINNED_COLD_ORDINARY_MAX_GAP_BYTES
        {
            record_pinned_access_batch(reads, access_tracker);
            return false;
        }
        previous_end = locator.offset.saturating_add(locator.record_len);
    }
    let mut tracker = access_tracker.lock();
    let repeated_count = locators
        .iter()
        .filter(|(locator, _)| tracker.last_batch.contains(*locator))
        .count();
    tracker.last_batch.clear();
    for (locator, _) in locators {
        tracker.last_batch.insert(locator.to_string());
    }
    repeated_count * 2 >= reads.len()
}

fn record_pinned_access_batch(
    reads: &[ColdObjectPinnedRead<'_>],
    access_tracker: &Mutex<ExtentStorePinnedAccessTracker>,
) {
    let mut tracker = access_tracker.lock();
    tracker.last_batch.clear();
    for read in reads {
        tracker
            .last_batch
            .insert(read.cold_backing.object_locator.clone());
    }
}

fn prefetch_pinned_read_files(
    reads: &[ReservedExtentStorePinnedRead],
    mode: ExtentStorePinnedPrefetchMode,
) {
    let mut ranges = reads
        .iter()
        .filter_map(|read| {
            pinned_value_range(read)
                .map(|(offset, len)| (read.file.clone(), read.locator.segment_id, offset, len))
        })
        .collect::<Vec<_>>();
    ranges.sort_by_key(|(_, segment_id, offset, _)| (*segment_id, *offset));
    let mut cursor = 0usize;
    while cursor < ranges.len() {
        let (file, segment_id, offset, mut len) = ranges[cursor].clone();
        cursor += 1;
        while cursor < ranges.len()
            && ranges[cursor].1 == segment_id
            && Arc::ptr_eq(&ranges[cursor].0, &file)
            && ranges[cursor].2 >= offset
            && ranges[cursor].2.saturating_sub(offset.saturating_add(len))
                <= EXTENT_STORE_PINNED_PREFETCH_MAX_GAP_BYTES
        {
            let end = ranges[cursor].2.saturating_add(ranges[cursor].3);
            len = end.saturating_sub(offset);
            cursor += 1;
        }
        match mode {
            ExtentStorePinnedPrefetchMode::Fadvise => {
                let _ = unsafe {
                    libc::posix_fadvise(
                        file.as_raw_fd(),
                        offset as libc::off_t,
                        len as libc::off_t,
                        libc::POSIX_FADV_WILLNEED,
                    )
                };
            }
            ExtentStorePinnedPrefetchMode::Readahead => {
                let _ = unsafe {
                    libc::readahead(
                        file.as_raw_fd(),
                        offset as libc::off64_t,
                        len as libc::size_t,
                    )
                };
            }
            _ => {}
        }
    }
}

fn prefetch_pinned_read_mapping(
    mapping: &ExtentStoreMmap,
    read: &ReservedExtentStorePinnedRead,
    mode: ExtentStorePinnedPrefetchMode,
) -> Result<()> {
    let Some((offset, len)) = pinned_value_range(read) else {
        return Ok(());
    };
    let payload = mapping.slice(offset as usize, len as usize)?;
    match mode {
        ExtentStorePinnedPrefetchMode::Madvise => {
            let _ = unsafe {
                libc::madvise(
                    payload.as_ptr().cast_mut().cast(),
                    payload.len(),
                    libc::MADV_WILLNEED,
                )
            };
        }
        ExtentStorePinnedPrefetchMode::Touch => {
            let mut checksum = 0u8;
            for byte in payload.iter().step_by(4096) {
                checksum ^= *byte;
            }
            std::hint::black_box(checksum);
        }
        _ => {}
    }
    Ok(())
}

fn pinned_value_range(read: &ReservedExtentStorePinnedRead) -> Option<(u64, u64)> {
    let offset = read.locator.offset.checked_add(read.locator.value_offset)?;
    let len = read.locator.value_len;
    if len == 0 || offset > libc::off_t::MAX as u64 || len > libc::off_t::MAX as u64 {
        return None;
    }
    Some((offset, len))
}

fn read_buffered_batch_blocking_serial(
    reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
    completed: &[bool],
    counters: &ExtentStoreIoCounters,
) -> Vec<Result<()>> {
    reads
        .iter_mut()
        .enumerate()
        .map(|(index, (_, read))| {
            read_buffered_one(
                read,
                completed.get(index).copied().unwrap_or(false),
                counters,
            )
        })
        .collect()
}

fn read_buffered_one(
    read: &mut ReservedExtentStoreRead<'_>,
    completed: bool,
    counters: &ExtentStoreIoCounters,
) -> Result<()> {
    if completed {
        return Ok(());
    }
    let value_len = read.locator.value_len as usize;
    counters.record_buffered_read(value_len);
    read_exact_at_blocking(
        &read.file,
        read.locator.offset + read.locator.value_offset,
        &mut read.dst[..value_len],
    )
}

fn write_all_at_blocking(
    file: &ExtentStoreFile,
    mut offset: u64,
    mut payload: &[u8],
) -> Result<()> {
    while !payload.is_empty() {
        let written = file.write_at(payload, offset).map_err(|error| {
            StoreError::Transport(format!("extent store blocking pwrite failed: {error}"))
        })?;
        if written == 0 {
            return Err(StoreError::Transport(
                "extent store write completed with zero bytes".to_string(),
            ));
        }
        offset = offset.saturating_add(written as u64);
        payload = &payload[written..];
    }
    Ok(())
}

fn write_vectored_all_at_blocking(
    file: &ExtentStoreFile,
    mut offset: u64,
    record: &EncodedExtentStoreRecord<'_>,
) -> Result<()> {
    let mut written = 0usize;
    while written < record.record_len as usize {
        let iovecs = record.iovecs_from(written);
        let result = unsafe {
            libc::pwritev(
                file.as_raw_fd(),
                iovecs.as_ptr(),
                iovecs.len() as i32,
                offset as libc::off_t,
            )
        };
        if result < 0 {
            return Err(StoreError::Transport(format!(
                "extent store blocking pwritev failed: {}",
                io::Error::last_os_error()
            )));
        }
        if result == 0 {
            return Err(StoreError::Transport(
                "extent store write completed with zero bytes".to_string(),
            ));
        }
        let result = result as usize;
        written = written.saturating_add(result);
        offset = offset.saturating_add(result as u64);
    }
    Ok(())
}

fn read_exact_with_scratch_at_blocking(
    file: &ExtentStoreFile,
    offset: u64,
    payload: &mut [u8],
    scratch: &mut ExtentStoreScratchWorkspace,
) -> Result<()> {
    let scratch = scratch.read_buffer(payload.len())?;
    read_exact_at_blocking(file, offset, scratch)?;
    payload.copy_from_slice(scratch);
    Ok(())
}

fn read_exact_at_blocking(
    file: &ExtentStoreFile,
    mut offset: u64,
    mut payload: &mut [u8],
) -> Result<()> {
    while !payload.is_empty() {
        let read = file.read_at(payload, offset).map_err(|error| {
            StoreError::Transport(format!("extent store blocking pread failed: {error}"))
        })?;
        if read == 0 {
            return Err(StoreError::NotFound(
                "extent store read reached EOF".to_string(),
            ));
        }
        offset = offset.saturating_add(read as u64);
        let (_, rest) = payload.split_at_mut(read);
        payload = rest;
    }
    Ok(())
}

impl<'a> EncodedExtentStoreRecord<'a> {
    fn iovecs_from(&self, written: usize) -> Vec<libc::iovec> {
        let mut remaining_skip = written;
        let mut iovecs = Vec::with_capacity(2 + self.payload.slice_count());
        push_iovec_after_skip(&mut iovecs, &self.prefix, &mut remaining_skip);
        self.payload
            .push_iovecs_after_skip(&mut iovecs, &mut remaining_skip);
        push_iovec_after_skip(&mut iovecs, &self.suffix_padding, &mut remaining_skip);
        iovecs
    }
}

impl<'a> EncodedExtentStorePayload<'a> {
    fn as_slice(&self) -> Option<&[u8]> {
        match self {
            Self::Borrowed(payload) => Some(payload),
            Self::BorrowedSlices(_) => None,
        }
    }

    fn slice_count(&self) -> usize {
        match self {
            Self::Borrowed(_) => 1,
            Self::BorrowedSlices(slices) => slices.len(),
        }
    }

    fn push_iovecs_after_skip(&self, iovecs: &mut Vec<libc::iovec>, remaining_skip: &mut usize) {
        match self {
            Self::Borrowed(payload) => push_iovec_after_skip(iovecs, payload, remaining_skip),
            Self::BorrowedSlices(slices) => {
                for payload in slices {
                    push_iovec_after_skip(iovecs, payload, remaining_skip);
                }
            }
        }
    }
}

struct PackedExtentStoreRecord<'a> {
    record: Result<EncodedExtentStoreRecord<'a>>,
    result_locators: Vec<(usize, ExtentStoreLocator)>,
}

fn encode_packed_extent_store_records<'a>(
    writes: &[ExtentStoreWrite<'a>],
    buffer_pool: &std::sync::Arc<ExtentStoreBufferPool>,
) -> Vec<PackedExtentStoreRecord<'a>> {
    let mut records = Vec::new();
    let mut cursor = 0usize;
    while cursor < writes.len() {
        if !packed_extent_store_write_eligible(&writes[cursor]) {
            cursor += 1;
            continue;
        }
        let start = cursor;
        let mut total_value_len = 0u64;
        while cursor < writes.len() && packed_extent_store_write_eligible(&writes[cursor]) {
            let next_len = writes[cursor].payload.len() as u64;
            let entry_count = (cursor - start + 1) as u64;
            let index_len = entry_count.saturating_mul(EXTENT_STORE_PACKED_ENTRY_INDEX_LEN);
            let value_offset = align_up(
                EXTENT_STORE_HEADER_LEN as u64 + index_len,
                EXTENT_STORE_ALIGNMENT,
            );
            let record_len = align_up(
                value_offset + total_value_len + next_len,
                EXTENT_STORE_ALIGNMENT,
            );
            if cursor > start
                && (entry_count > EXTENT_STORE_PACKED_BLOCK_MAX_ENTRIES
                    || record_len > EXTENT_STORE_PACKED_BLOCK_TARGET_BYTES)
            {
                break;
            }
            total_value_len = total_value_len.saturating_add(next_len);
            cursor += 1;
        }
        if cursor - start < 2 {
            continue;
        }
        records.push(encode_packed_extent_store_record(
            writes,
            start,
            cursor,
            buffer_pool,
        ));
    }
    records
}

fn packed_extent_store_write_eligible(write: &ExtentStoreWrite<'_>) -> bool {
    !write.payload.is_empty()
        && write.payload.len() as u64 <= EXTENT_STORE_PACKED_BLOCK_MAX_PAYLOAD_BYTES
}

fn encode_packed_extent_store_record<'a>(
    writes: &[ExtentStoreWrite<'a>],
    start: usize,
    end: usize,
    buffer_pool: &std::sync::Arc<ExtentStoreBufferPool>,
) -> PackedExtentStoreRecord<'a> {
    let entry_count = (end - start) as u32;
    let index_len = entry_count as u64 * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN;
    let value_offset = align_up(
        EXTENT_STORE_HEADER_LEN as u64 + index_len,
        EXTENT_STORE_ALIGNMENT,
    );
    let value_len = writes[start..end]
        .iter()
        .map(|write| write.payload.len() as u64)
        .sum::<u64>();
    let record_len = align_up(value_offset + value_len, EXTENT_STORE_ALIGNMENT);
    let mut result_locators = Vec::with_capacity(entry_count as usize);
    let record = (|| {
        if record_len > usize::MAX as u64 {
            return Err(StoreError::InvalidState(format!(
                "extent store packed record length {record_len} exceeds addressable memory"
            )));
        }
        let mut prefix = buffer_pool.lease(value_offset as usize)?;
        prefix[..value_offset as usize].fill(0);
        let mut payload_cursor = 0usize;
        let mut payload_slices = Vec::with_capacity(end - start);
        let mut all_entry_checksums = true;
        for (entry_index, write) in writes[start..end].iter().enumerate() {
            let entry_offset = payload_cursor as u64;
            let entry_len = write.payload.len() as u64;
            let index_offset = EXTENT_STORE_HEADER_LEN
                + entry_index * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize;
            prefix[index_offset..index_offset + 8].copy_from_slice(&entry_offset.to_le_bytes());
            prefix[index_offset + 8..index_offset + 16].copy_from_slice(&entry_len.to_le_bytes());
            let entry_checksum = write.checksum.unwrap_or_else(|| {
                all_entry_checksums = false;
                payload_checksum(write.payload)
            });
            prefix[index_offset + 16..index_offset + 24]
                .copy_from_slice(&entry_checksum.to_le_bytes());
            payload_slices.push(write.payload);
            result_locators.push((
                start + entry_index,
                ExtentStoreLocator {
                    segment_id: 0,
                    offset: 0,
                    record_len,
                    value_offset: value_offset + entry_offset,
                    value_len: entry_len,
                    generation: 0,
                },
            ));
            payload_cursor += write.payload.len();
        }
        let checksum = if all_entry_checksums {
            packed_entry_checksum(&prefix, entry_count as usize)
        } else {
            packed_payload_checksum_from_payloads(&prefix, &payload_slices, entry_count as usize)
        };
        encode_record_header(
            &mut prefix[..EXTENT_STORE_HEADER_LEN],
            EXTENT_STORE_RECORD_KIND_PACKED,
            entry_count as u64,
            value_len,
            checksum,
            value_offset,
            record_len,
        );
        let suffix_len = record_len as usize - value_offset as usize - value_len as usize;
        let suffix_padding = zeroed_extent_store_pool_buffer(buffer_pool, suffix_len)?;
        Ok(EncodedExtentStoreRecord {
            value_offset,
            value_len,
            record_len,
            entry_count,
            prefix,
            payload: EncodedExtentStorePayload::BorrowedSlices(payload_slices),
            suffix_padding,
        })
    })();
    PackedExtentStoreRecord {
        record,
        result_locators,
    }
}

fn packed_entry_checksum(prefix: &[u8], entry_count: usize) -> u64 {
    let index_start = EXTENT_STORE_HEADER_LEN;
    let index_end = index_start + entry_count * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize;
    packed_entry_checksum_from_index(&prefix[index_start..index_end])
}

fn packed_payload_checksum_from_payloads(
    prefix: &[u8],
    payloads: &[&[u8]],
    entry_count: usize,
) -> u64 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let index_start = EXTENT_STORE_HEADER_LEN;
    for (entry_index, payload) in payloads.iter().enumerate().take(entry_count) {
        let index_offset = index_start + entry_index * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize;
        hasher.update(&prefix[index_offset..index_offset + 16]);
        hasher.update(&payload_checksum(payload).to_le_bytes());
    }
    hasher.digest()
}

fn encode_extent_store_record<'a>(
    logical_locator: &str,
    payload: &'a [u8],
    checksum: Option<u64>,
    buffer_pool: &std::sync::Arc<ExtentStoreBufferPool>,
) -> Result<EncodedExtentStoreRecord<'a>> {
    let logical_locator = logical_locator.as_bytes();
    let value_len = payload.len() as u64;
    let minimum_value_offset = EXTENT_STORE_HEADER_LEN as u64 + logical_locator.len() as u64;
    let dense = value_len <= dense_record_max_value_len()
        && value_len.saturating_add(minimum_value_offset) > EXTENT_STORE_ALIGNMENT;
    let value_offset = if dense {
        minimum_value_offset
    } else {
        align_up(minimum_value_offset, EXTENT_STORE_ALIGNMENT)
    };
    let record_len = if dense {
        value_offset + value_len
    } else {
        align_up(value_offset + value_len, EXTENT_STORE_ALIGNMENT)
    };
    if record_len > usize::MAX as u64 {
        return Err(StoreError::InvalidState(format!(
            "extent store record length {record_len} exceeds addressable memory"
        )));
    }
    let value_offset_usize = value_offset as usize;
    let mut prefix = buffer_pool.lease(value_offset_usize)?;
    prefix[..value_offset_usize].fill(0);
    encode_record_header(
        &mut prefix[..EXTENT_STORE_HEADER_LEN],
        EXTENT_STORE_RECORD_KIND_SINGLE,
        logical_locator.len() as u64,
        value_len,
        checksum.unwrap_or_else(|| payload_checksum(payload)),
        value_offset,
        record_len,
    );
    let key_start = EXTENT_STORE_HEADER_LEN;
    let key_end = key_start + logical_locator.len();
    prefix[key_start..key_end].copy_from_slice(logical_locator);
    let suffix_len = record_len as usize - value_offset_usize - payload.len();
    Ok(EncodedExtentStoreRecord {
        value_offset,
        value_len,
        record_len,
        entry_count: 1,
        prefix,
        payload: EncodedExtentStorePayload::Borrowed(payload),
        suffix_padding: zeroed_extent_store_pool_buffer(buffer_pool, suffix_len)?,
    })
}

fn zeroed_extent_store_pool_buffer(
    buffer_pool: &std::sync::Arc<ExtentStoreBufferPool>,
    len: usize,
) -> Result<ExtentStoreBufferLease> {
    let mut buffer = buffer_pool.lease(len)?;
    buffer[..len].fill(0);
    Ok(buffer)
}

fn push_iovec_after_skip(iovecs: &mut Vec<libc::iovec>, buffer: &[u8], remaining_skip: &mut usize) {
    if buffer.is_empty() {
        return;
    }
    if *remaining_skip >= buffer.len() {
        *remaining_skip -= buffer.len();
        return;
    }
    let start = *remaining_skip;
    *remaining_skip = 0;
    iovecs.push(libc::iovec {
        iov_base: buffer[start..].as_ptr() as *mut c_void,
        iov_len: buffer.len() - start,
    });
}

include!("extent_store_recovery.rs");

fn encode_record_header(
    header: &mut [u8],
    record_kind: u16,
    key_len: u64,
    value_len: u64,
    checksum: u64,
    value_offset: u64,
    record_len: u64,
) {
    encode_record_header_without_checksum(
        header,
        record_kind,
        key_len,
        value_len,
        value_offset,
        record_len,
    );
    header[24..32].copy_from_slice(&checksum.to_le_bytes());
}

fn encode_record_header_without_checksum(
    header: &mut [u8],
    record_kind: u16,
    key_len: u64,
    value_len: u64,
    value_offset: u64,
    record_len: u64,
) {
    debug_assert!(header.len() >= EXTENT_STORE_HEADER_LEN);
    header[0..4].copy_from_slice(&EXTENT_STORE_MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&(EXTENT_STORE_HEADER_LEN as u16).to_le_bytes());
    header[6..8].copy_from_slice(&record_kind.to_le_bytes());
    header[8..16].copy_from_slice(&key_len.to_le_bytes());
    header[16..24].copy_from_slice(&value_len.to_le_bytes());
    header[24..32].fill(0);
    header[32..40].copy_from_slice(&value_offset.to_le_bytes());
    header[40..48].copy_from_slice(&record_len.to_le_bytes());
}

include!("extent_store_cold_backend.rs");

fn align_up(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    (value + alignment - 1) & !(alignment - 1)
}

fn align_down(value: u64, alignment: u64) -> u64 {
    debug_assert!(alignment.is_power_of_two());
    value & !(alignment - 1)
}

#[cfg(test)]
mod extent_store_engine_tests {
    use super::*;

    struct TestExtentStoreRoot {
        root: ExtentStorePathBuf,
    }

    impl TestExtentStoreRoot {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "moon-store-extent-store-{name}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock should be after UNIX epoch")
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).expect("test extent store root should be created");
            Self { root }
        }

        fn path(&self) -> &ExtentStorePath {
            &self.root
        }
    }

    impl Drop for TestExtentStoreRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn test_extent_store_root(name: &str) -> TestExtentStoreRoot {
        TestExtentStoreRoot::new(name)
    }

    #[test]
    fn extent_store_default_open_does_not_preallocate_segment() {
        let root = test_extent_store_root("default-no-preallocate");
        let engine = ExtentStoreEngine::new(root.path()).expect("extent store should start");
        let locator = engine
            .put("logical/default-no-preallocate", b"payload")
            .expect("extent store write should succeed");
        drop(engine);

        let segment_id = ExtentStoreLocator::decode(&locator)
            .expect("locator should decode")
            .segment_id;
        let segment_path = root
            .path()
            .join("segments")
            .join(format!("{segment_id:016x}.seg"));
        let metadata = std::fs::metadata(&segment_path).expect("active segment should exist");
        assert!(metadata.len() < 1024 * 1024);
        assert!(
            std::os::unix::fs::MetadataExt::blocks(&metadata) * 512 < 1024 * 1024,
            "default extent store open unexpectedly allocated {} bytes",
            std::os::unix::fs::MetadataExt::blocks(&metadata) * 512
        );
    }

    include!("extent_store_recovery_tests.rs");
    include!("extent_store_io_worker_tests.rs");

    #[test]
    fn extent_store_pinned_read_requires_policy_enable() {
        let root = test_extent_store_root("pinned-disabled");
        let mut engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 8)
                .expect("extent store should start");
        engine.pinned_read_policy.enabled = false;
        let payload = vec![3u8; 128 * 1024];
        let locator = engine
            .put("logical/pinned-disabled", &payload)
            .expect("extent store write should succeed");

        let disabled_result = engine.get_pinned(&locator, payload.len() as u64);
        assert!(disabled_result
            .as_ref()
            .err()
            .expect("disabled pinned read should not return borrowed payload")
            .to_string()
            .contains("not submitted"));
        assert_eq!(engine.io_stats().pinned_fallback_disabled_ops, 1);

        engine.pinned_read_policy.enabled = true;
        let pinned = engine
            .get_pinned(&locator, payload.len() as u64)
            .expect("enabled pinned read should succeed")
            .expect("pinned payload should exist");
        assert_eq!(pinned.as_slice(), payload.as_slice());
        let stats = engine.io_stats();
        assert_eq!(stats.pinned_read_ops, 1);
        assert_eq!(stats.pinned_read_bytes, payload.len() as u64);
    }

    #[test]
    fn extent_store_pinned_payload_survives_delete_and_unlink() {
        let root = test_extent_store_root("pinned-lifetime");
        let segment_size = EXTENT_STORE_ALIGNMENT * 2;
        let mut engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("extent store should start");
        engine.pinned_read_policy.enabled = true;
        engine.pinned_read_policy.min_value_bytes = 1;
        let first_payload = vec![5u8; EXTENT_STORE_ALIGNMENT as usize];
        let second_payload = vec![7u8; EXTENT_STORE_ALIGNMENT as usize];
        let first_locator = engine
            .put("logical/pinned-first", &first_payload)
            .expect("first write should succeed");
        let second_locator = engine
            .put("logical/pinned-second", &second_payload)
            .expect("second write should succeed");
        let first_segment = ExtentStoreLocator::decode(&first_locator)
            .expect("first locator should decode")
            .segment_id;
        let second_segment = ExtentStoreLocator::decode(&second_locator)
            .expect("second locator should decode")
            .segment_id;
        assert!(second_segment > first_segment);
        let first_segment_path = root
            .path()
            .join("segments")
            .join(format!("{first_segment:016x}.seg"));

        let pinned = engine
            .get_pinned(&first_locator, first_payload.len() as u64)
            .expect("pinned read should succeed")
            .expect("pinned payload should exist");
        assert_eq!(engine.io_stats().pinned_payload_active_refs, 1);
        assert!(engine
            .delete(&first_locator)
            .expect("delete should succeed"));
        assert!(!first_segment_path.exists());
        assert_eq!(pinned.as_slice(), first_payload.as_slice());
        assert_eq!(engine.io_stats().pinned_payload_active_refs, 1);
        drop(pinned);
        assert_eq!(engine.io_stats().pinned_payload_active_refs, 0);
    }

    #[test]
    fn extent_store_pinned_quota_falls_back_without_mapping() {
        let root = test_extent_store_root("pinned-quota");
        let mut engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 8)
                .expect("extent store should start");
        engine.pinned_read_policy.enabled = true;
        engine.pinned_read_policy.max_mmap_bytes = 1;
        let payload = vec![9u8; 128 * 1024];
        let locator = engine
            .put("logical/pinned-quota", &payload)
            .expect("extent store write should succeed");

        let quota_result = engine.get_pinned(&locator, payload.len() as u64);
        let error = quota_result
            .as_ref()
            .err()
            .expect("quota should prevent pinned read");
        assert!(matches!(error, StoreError::QuotaExceeded { .. }));
        let stats = engine.io_stats();
        assert_eq!(stats.pinned_fallback_quota_exceeded_ops, 1);
        assert_eq!(stats.mmap_active_segments, 0);
        assert_eq!(stats.pinned_read_ops, 0);
    }

    #[test]
    fn extent_store_kvcache_batch_exercises_io_paths() {
        let root = test_extent_store_root("kvcache-io-paths");
        let engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 512)
                .expect("extent store should start");
        let payloads = kvcache_payloads();
        let writes = payloads
            .iter()
            .map(|case| ExtentStoreWrite {
                logical_locator: case.key.as_str(),
                payload: case.payload.as_slice(),
                checksum: None,
            })
            .collect::<Vec<_>>();

        let before_write = engine.io_stats();
        let locators = engine
            .put_batch(&writes)
            .into_iter()
            .map(|result| result.expect("kvcache batch write should succeed"))
            .collect::<Vec<_>>();
        let write_stats = engine.io_stats().delta_since(before_write);
        assert!(
            write_stats.direct_write_ops > 0
                || write_stats.direct_scratch_write_ops > 0
                || write_stats.buffered_write_ops > 0
        );
        assert!(
            write_stats.write_fixed_ops > 0
                || write_stats.raw_fd_fallback_ops > 0
                || write_stats.buffered_write_ops > 0
        );

        let mut buffers = payloads
            .iter()
            .enumerate()
            .map(|(index, case)| kvcache_read_buffer(index, case.payload.len()))
            .collect::<Vec<_>>();
        let mut reads = locators
            .iter()
            .zip(payloads.iter())
            .zip(buffers.iter_mut())
            .map(|((locator, case), buffer)| ExtentStoreRead {
                locator,
                expected_len: case.payload.len() as u64,
                dst: buffer.as_mut_slice(),
            })
            .collect::<Vec<_>>();

        let before_read = engine.io_stats();
        let results = engine.get_batch_into(&mut reads);
        let read_stats = engine.io_stats().delta_since(before_read);
        for (result, case) in results.into_iter().zip(payloads.iter()) {
            assert_eq!(
                result.expect("kvcache batch read should succeed"),
                Some(case.payload.len())
            );
        }
        for (buffer, case) in buffers.iter().zip(payloads.iter()) {
            assert_eq!(buffer.as_slice(), case.payload.as_slice());
        }
        assert!(
            read_stats.direct_read_ops > 0
                || read_stats.coalesced_read_groups > 0
                || read_stats.buffered_read_ops > 0
        );

        let scratch_payload = aligned_kvcache_payload((EXTENT_STORE_ALIGNMENT * 8) as usize, 37);
        let scratch_locator = engine
            .put(
                "kv/layer-05/block-0005/scratch-read",
                scratch_payload.as_slice(),
            )
            .expect("scratch-read kvcache write should succeed");
        let mut scratch_dst = vec![0u8; scratch_payload.len()];
        let before_scratch_read = engine.io_stats();
        assert_eq!(
            engine
                .get_into(
                    &scratch_locator,
                    scratch_payload.len() as u64,
                    &mut scratch_dst
                )
                .expect("scratch kvcache read should succeed"),
            Some(scratch_payload.len())
        );
        let scratch_read_stats = engine.io_stats().delta_since(before_scratch_read);
        assert_eq!(scratch_dst, scratch_payload.as_slice());
        assert!(
            scratch_read_stats.direct_scratch_read_ops > 0
                || scratch_read_stats.buffered_read_ops > 0
        );
        assert!(scratch_read_stats.read_fixed_ops > 0 || scratch_read_stats.buffered_read_ops > 0);

        let coalesced_payloads = (0..8)
            .map(|index| kvcache_payload(EXTENT_STORE_ALIGNMENT as usize, index as u8 + 51))
            .collect::<Vec<_>>();
        let coalesced_writes = coalesced_payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| ExtentStoreWrite {
                logical_locator: match index {
                    0 => "kv/coalesce/00",
                    1 => "kv/coalesce/01",
                    2 => "kv/coalesce/02",
                    3 => "kv/coalesce/03",
                    4 => "kv/coalesce/04",
                    5 => "kv/coalesce/05",
                    6 => "kv/coalesce/06",
                    _ => "kv/coalesce/07",
                },
                payload: payload.as_slice(),
                checksum: None,
            })
            .collect::<Vec<_>>();
        let coalesced_locators = engine
            .put_batch(&coalesced_writes)
            .into_iter()
            .map(|result| result.expect("coalesced write should succeed"))
            .collect::<Vec<_>>();
        let mut coalesced_buffers = coalesced_payloads
            .iter()
            .map(|payload| vec![0u8; payload.len() + 1])
            .collect::<Vec<_>>();
        let mut coalesced_reads = coalesced_locators
            .iter()
            .zip(coalesced_payloads.iter())
            .zip(coalesced_buffers.iter_mut())
            .map(|((locator, payload), buffer)| ExtentStoreRead {
                locator,
                expected_len: payload.len() as u64,
                dst: buffer.as_mut_slice(),
            })
            .collect::<Vec<_>>();
        let before_coalesced_read = engine.io_stats();
        let coalesced_results = engine.get_batch_into(&mut coalesced_reads);
        let coalesced_stats = engine.io_stats().delta_since(before_coalesced_read);
        for (result, payload) in coalesced_results.into_iter().zip(coalesced_payloads.iter()) {
            assert_eq!(
                result.expect("coalesced read should succeed"),
                Some(payload.len())
            );
        }
        for (buffer, payload) in coalesced_buffers.iter().zip(coalesced_payloads.iter()) {
            assert_eq!(&buffer[..payload.len()], payload.as_slice());
        }
        assert!(coalesced_stats.coalesced_read_groups > 0 || coalesced_stats.buffered_read_ops > 0);
        if coalesced_stats.coalesced_read_groups > 0 {
            assert_eq!(
                coalesced_stats.coalesced_logical_reads,
                coalesced_payloads.len() as u64
            );
            assert!(
                coalesced_stats.coalesced_physical_bytes
                    >= coalesced_stats.coalesced_requested_bytes
            );
            assert!(coalesced_stats.read_fixed_ops < coalesced_payloads.len() as u64);
        }

        let packed_payloads = (0..4)
            .map(|index| kvcache_payload(4096, index as u8 + 71))
            .collect::<Vec<_>>();
        let packed_writes = packed_payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| ExtentStoreWrite {
                logical_locator: match index {
                    0 => "kv/packed/00",
                    1 => "kv/packed/01",
                    2 => "kv/packed/02",
                    _ => "kv/packed/03",
                },
                payload: payload.as_slice(),
                checksum: None,
            })
            .collect::<Vec<_>>();
        let packed_locators = engine
            .put_batch(&packed_writes)
            .into_iter()
            .map(|result| result.expect("packed write should succeed"))
            .collect::<Vec<_>>();
        let decoded_packed = packed_locators
            .iter()
            .map(|locator| {
                ExtentStoreLocator::decode(locator).expect("packed locator should decode")
            })
            .collect::<Vec<_>>();
        assert!(decoded_packed
            .iter()
            .all(|locator| locator.block_key() == decoded_packed[0].block_key()));
        let packed_segment_path = root
            .path()
            .join("segments")
            .join(format!("{:016x}.seg", decoded_packed[0].segment_id));
        let packed_file = OpenOptions::new()
            .write(true)
            .open(&packed_segment_path)
            .expect("packed segment should open for corruption");
        let corrupt_offset = decoded_packed[0].offset + decoded_packed[0].value_offset;
        packed_file
            .write_at(&[packed_payloads[0].as_slice()[0] ^ 0xFF], corrupt_offset)
            .expect("packed payload corruption should succeed");
        let mut packed_buffers = packed_payloads
            .iter()
            .map(|payload| vec![0u8; payload.len()])
            .collect::<Vec<_>>();
        let mut packed_reads = packed_locators
            .iter()
            .zip(packed_payloads.iter())
            .zip(packed_buffers.iter_mut())
            .map(|((locator, payload), buffer)| ExtentStoreRead {
                locator,
                expected_len: payload.len() as u64,
                dst: buffer.as_mut_slice(),
            })
            .collect::<Vec<_>>();
        let packed_results = engine.get_batch_into(&mut packed_reads);
        assert!(packed_results.into_iter().all(|result| result
            .expect_err("corrupt packed read should fail")
            .to_string()
            .contains("checksum mismatch")));

        disable_direct_io(&engine);
        let buffered_payload = kvcache_payload(2049, 41);
        let before_buffered_write = engine.io_stats();
        let buffered_locator = engine
            .put(
                "kv/layer-05/block-0005/buffered",
                buffered_payload.as_slice(),
            )
            .expect("buffered kvcache write should succeed");
        let buffered_write_stats = engine.io_stats().delta_since(before_buffered_write);
        assert!(buffered_write_stats.buffered_write_ops > 0);
        let mut buffered_dst = vec![0u8; buffered_payload.len()];
        assert_eq!(
            engine
                .get_into(
                    &buffered_locator,
                    buffered_payload.len() as u64,
                    &mut buffered_dst
                )
                .expect("buffered kvcache read should succeed"),
            Some(buffered_payload.len())
        );
        assert_eq!(buffered_dst, buffered_payload.as_slice());
    }

    #[cfg(feature = "gds")]
    #[test]
    fn extent_store_gds_lane_reads_eligible_aligned_payload() {
        let root = test_extent_store_root("gds-lane");
        let mut engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 8)
                .expect("extent store should start");
        engine.local_read_lanes.gds.config = ExtentStoreGdsConfig {
            enabled: true,
            min_read_size: EXTENT_STORE_ALIGNMENT as usize,
            allow_host_destination: true,
        };
        engine.local_read_lanes.gds.driver = Some(ExtentStoreCuFileDriver);
        let payload = aligned_kvcache_payload((EXTENT_STORE_ALIGNMENT * 2) as usize, 53);
        let locator = engine
            .put("kv/gds/aligned", payload.as_slice())
            .expect("GDS candidate write should succeed");
        let mut dst = AlignedExtentStoreBuffer::zeroed(payload.len())
            .expect("aligned GDS destination should allocate");

        let before_read = engine.io_stats();
        assert_eq!(
            engine
                .get_into(&locator, payload.len() as u64, &mut dst)
                .expect("GDS candidate read should succeed"),
            Some(payload.len())
        );
        let read_stats = engine.io_stats().delta_since(before_read);
        assert_eq!(&dst[..payload.len()], payload.as_slice());
        assert!(read_stats.gds_read_ops > 0);
        assert!(read_stats.gds_read_bytes >= payload.len() as u64);
    }

    #[test]
    fn extent_store_backend_kvcache_batch_restart_and_delete() {
        let root = test_extent_store_root("kvcache-backend");
        let payloads = kvcache_payloads();
        let segment_size = 1024 * 1024;
        let materialized = {
            let backend =
                ExtentStoreStorageBackend::new_with_segment_size(root.path(), segment_size)
                    .expect("backend should start");
            let cold_backings = payloads
                .iter()
                .map(|case| kvcache_cold_backing(case.key.as_str(), case.payload.as_slice()))
                .collect::<Vec<_>>();
            let writes = cold_backings
                .iter()
                .zip(payloads.iter())
                .map(|(cold_backing, case)| ColdObjectWrite {
                    route: None,
                    cold_backing,
                    payload: case.payload.as_slice(),
                })
                .collect::<Vec<_>>();
            backend
                .put_objects_batch(&writes)
                .into_iter()
                .map(|result| result.expect("backend batch write should succeed"))
                .collect::<Vec<_>>()
        };

        let backend = ExtentStoreStorageBackend::new_with_segment_size(root.path(), segment_size)
            .expect("backend should restart");
        let mut buffers = payloads
            .iter()
            .map(|case| vec![0u8; case.payload.len()])
            .collect::<Vec<_>>();
        let mut reads = materialized
            .iter()
            .zip(buffers.iter_mut())
            .map(|(cold_backing, buffer)| ColdObjectRead {
                cold_backing,
                dst: buffer,
            })
            .collect::<Vec<_>>();
        let read_results = backend.get_objects_into_batch(&mut reads);
        for (result, case) in read_results.into_iter().zip(payloads.iter()) {
            assert_eq!(
                result.expect("backend batch read should succeed"),
                Some(case.payload.len())
            );
        }
        for (buffer, case) in buffers.iter().zip(payloads.iter()) {
            assert_eq!(buffer, case.payload.as_slice());
        }
        for cold_backing in &materialized {
            assert!(backend
                .delete_object(cold_backing)
                .expect("backend delete should succeed"));
        }
        let stats_after_delete = backend
            .maintenance_stats()
            .expect("backend maintenance stats should load");
        assert_eq!(stats_after_delete.extent_live_bytes, 0);
        for cold_backing in &materialized {
            assert!(!backend
                .delete_object(cold_backing)
                .expect("second backend delete should succeed"));
        }
    }

    struct KvcachePayloadCase {
        key: String,
        payload: KvcachePayload,
    }

    enum KvcachePayload {
        Vec(Vec<u8>),
        Aligned(AlignedExtentStoreBuffer),
    }

    impl KvcachePayload {
        fn as_slice(&self) -> &[u8] {
            match self {
                Self::Vec(payload) => payload,
                Self::Aligned(payload) => payload,
            }
        }

        fn as_mut_slice(&mut self) -> &mut [u8] {
            match self {
                Self::Vec(payload) => payload,
                Self::Aligned(payload) => payload,
            }
        }

        fn len(&self) -> usize {
            self.as_slice().len()
        }
    }

    fn kvcache_payloads() -> Vec<KvcachePayloadCase> {
        vec![
            KvcachePayloadCase {
                key: "kv/layer-00/block-0000/aligned-small".to_string(),
                payload: aligned_kvcache_payload(EXTENT_STORE_ALIGNMENT as usize, 11),
            },
            KvcachePayloadCase {
                key: "kv/layer-01/block-0001/unaligned-small".to_string(),
                payload: kvcache_payload(1536, 17),
            },
            KvcachePayloadCase {
                key: "kv/layer-02/block-0002/aligned-large".to_string(),
                payload: aligned_kvcache_payload((EXTENT_STORE_ALIGNMENT * 2) as usize, 23),
            },
            KvcachePayloadCase {
                key: "kv/layer-03/block-0003/unaligned-large".to_string(),
                payload: kvcache_payload(EXTENT_STORE_ALIGNMENT as usize + 777, 29),
            },
            KvcachePayloadCase {
                key: "kv/layer-04/block-0004/tiny".to_string(),
                payload: kvcache_payload(97, 31),
            },
            KvcachePayloadCase {
                key: "kv/layer-05/block-0005/aligned-bulk".to_string(),
                payload: aligned_kvcache_payload((EXTENT_STORE_ALIGNMENT * 8) as usize, 33),
            },
        ]
    }

    fn aligned_kvcache_payload(len: usize, seed: u8) -> KvcachePayload {
        let mut payload =
            AlignedExtentStoreBuffer::zeroed(len).expect("aligned kvcache payload should allocate");
        fill_kvcache_payload(&mut payload, seed);
        KvcachePayload::Aligned(payload)
    }

    fn kvcache_payload(len: usize, seed: u8) -> KvcachePayload {
        let mut payload = vec![0u8; len];
        fill_kvcache_payload(&mut payload, seed);
        KvcachePayload::Vec(payload)
    }

    fn kvcache_read_buffer(index: usize, len: usize) -> KvcachePayload {
        if index.is_multiple_of(2) {
            KvcachePayload::Aligned(
                AlignedExtentStoreBuffer::zeroed(len)
                    .expect("aligned kvcache read buffer should allocate"),
            )
        } else {
            KvcachePayload::Vec(vec![0u8; len])
        }
    }

    fn disable_direct_io(engine: &ExtentStoreEngine) {
        let mut inner = engine.inner.lock();
        for segment in inner.segments.values_mut() {
            segment.direct_file = None;
        }
    }

    fn fill_kvcache_payload(payload: &mut [u8], seed: u8) {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte = seed
                .wrapping_add((index / 128) as u8)
                .wrapping_add(index as u8);
        }
    }

    fn kvcache_cold_backing(
        logical_locator: &str,
        payload: &[u8],
    ) -> mooncake_store_core::ColdBackingRoute {
        mooncake_store_core::ColdBackingRoute {
            owner: ClientRuntimeId::new("kvcache-extent-test", ClientEpoch(1)),
            cold_tier_id: "kvcache-extent".to_string(),
            object_locator: logical_locator.to_string(),
            length: payload.len() as u64,
            checksum: Some(payload_checksum(payload)),
            state: mooncake_store_core::ColdBackingState::PendingOffload,
            replicas: Vec::new(),
        }
    }

    // --- Locator encode/decode tests ---

    #[test]
    fn locator_encode_decode_roundtrip() {
        let locator = ExtentStoreLocator {
            segment_id: 42,
            offset: 4096,
            record_len: 8192,
            value_offset: 128,
            value_len: 7000,
            generation: 1,
        };
        let encoded = locator.encode();
        assert!(encoded.starts_with(EXTENT_STORE_LOCATOR_PREFIX));
        let decoded = ExtentStoreLocator::decode(&encoded).expect("decode should succeed");
        assert_eq!(decoded, locator);
    }

    #[test]
    fn locator_decode_rejects_wrong_prefix() {
        let result = ExtentStoreLocator::decode("wrong-prefix:1:2:3:4:5:6");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("prefix"));
    }

    #[test]
    fn locator_decode_rejects_missing_fields() {
        let result = ExtentStoreLocator::decode("extent-store-v1:1:2:3");
        assert!(result.is_err());
    }

    #[test]
    fn locator_decode_rejects_extra_fields() {
        let result = ExtentStoreLocator::decode("extent-store-v1:1:2:3:4:5:6:7");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too many"));
    }

    #[test]
    fn locator_decode_rejects_invalid_hex() {
        let result = ExtentStoreLocator::decode("extent-store-v1:zz:2:3:4:5:6");
        assert!(result.is_err());
    }

    #[test]
    fn locator_block_key_groups_same_record() {
        let a = ExtentStoreLocator {
            segment_id: 1,
            offset: 4096,
            record_len: 8192,
            value_offset: 64,
            value_len: 100,
            generation: 1,
        };
        let b = ExtentStoreLocator {
            segment_id: 1,
            offset: 4096,
            record_len: 8192,
            value_offset: 128,
            value_len: 200,
            generation: 2,
        };
        assert_eq!(a.block_key(), b.block_key());
    }

    #[test]
    fn locator_block_key_distinguishes_different_records() {
        let a = ExtentStoreLocator {
            segment_id: 1,
            offset: 4096,
            record_len: 8192,
            value_offset: 64,
            value_len: 100,
            generation: 1,
        };
        let b = ExtentStoreLocator {
            segment_id: 1,
            offset: 8192,
            record_len: 8192,
            value_offset: 64,
            value_len: 100,
            generation: 1,
        };
        assert_ne!(a.block_key(), b.block_key());
    }

    #[test]
    fn is_extent_store_locator_accepts_valid() {
        let locator = ExtentStoreLocator {
            segment_id: 1,
            offset: 0,
            record_len: 4096,
            value_offset: 64,
            value_len: 10,
            generation: 0,
        };
        assert!(ExtentStoreEngine::is_extent_store_locator(
            &locator.encode()
        ));
    }

    #[test]
    fn is_extent_store_locator_rejects_other_strings() {
        assert!(!ExtentStoreEngine::is_extent_store_locator(
            "some/path/object"
        ));
        assert!(!ExtentStoreEngine::is_extent_store_locator(""));
        assert!(!ExtentStoreEngine::is_extent_store_locator(
            "extent-store-v1"
        ));
        assert!(!ExtentStoreEngine::is_extent_store_locator(
            "extent-store-v1/bad"
        ));
    }

    // --- Alignment helpers ---

    #[test]
    fn align_up_rounds_to_boundary() {
        assert_eq!(align_up(0, 4096), 0);
        assert_eq!(align_up(1, 4096), 4096);
        assert_eq!(align_up(4095, 4096), 4096);
        assert_eq!(align_up(4096, 4096), 4096);
        assert_eq!(align_up(4097, 4096), 8192);
    }

    #[test]
    fn align_down_rounds_to_boundary() {
        assert_eq!(align_down(0, 4096), 0);
        assert_eq!(align_down(1, 4096), 0);
        assert_eq!(align_down(4095, 4096), 0);
        assert_eq!(align_down(4096, 4096), 4096);
        assert_eq!(align_down(8191, 4096), 4096);
        assert_eq!(align_down(8192, 4096), 8192);
    }

    #[test]
    fn is_aligned_u64_checks_alignment() {
        assert!(is_aligned_u64(0));
        assert!(is_aligned_u64(4096));
        assert!(is_aligned_u64(8192));
        assert!(!is_aligned_u64(1));
        assert!(!is_aligned_u64(4097));
    }

    #[test]
    fn is_aligned_usize_checks_alignment() {
        assert!(is_aligned_usize(0));
        assert!(is_aligned_usize(4096));
        assert!(!is_aligned_usize(1));
        assert!(!is_aligned_usize(100));
    }

    // --- Record header encoding ---

    #[test]
    fn encode_record_header_writes_magic_and_fields() {
        let mut header = [0u8; EXTENT_STORE_HEADER_LEN];
        encode_record_header(
            &mut header,
            EXTENT_STORE_RECORD_KIND_SINGLE,
            10,         // key_len
            100,        // value_len
            0xdeadbeef, // checksum
            128,        // value_offset
            4096,       // record_len
        );
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        assert_eq!(magic, EXTENT_STORE_MAGIC);
        let header_len = u16::from_le_bytes(header[4..6].try_into().unwrap());
        assert_eq!(header_len, EXTENT_STORE_HEADER_LEN as u16);
        let kind = u16::from_le_bytes(header[6..8].try_into().unwrap());
        assert_eq!(kind, EXTENT_STORE_RECORD_KIND_SINGLE);
        let key_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
        assert_eq!(key_len, 10);
        let value_len = u64::from_le_bytes(header[16..24].try_into().unwrap());
        assert_eq!(value_len, 100);
        let checksum = u64::from_le_bytes(header[24..32].try_into().unwrap());
        assert_eq!(checksum, 0xdeadbeef);
        let value_offset = u64::from_le_bytes(header[32..40].try_into().unwrap());
        assert_eq!(value_offset, 128);
        let record_len = u64::from_le_bytes(header[40..48].try_into().unwrap());
        assert_eq!(record_len, 4096);
    }

    #[test]
    fn encode_record_header_without_checksum_zeroes_checksum_field() {
        let mut header = [0xffu8; EXTENT_STORE_HEADER_LEN];
        encode_record_header_without_checksum(
            &mut header,
            EXTENT_STORE_RECORD_KIND_PACKED,
            5,
            50,
            64,
            4096,
        );
        let checksum_bytes = &header[24..32];
        assert_eq!(checksum_bytes, &[0u8; 8]);
        let kind = u16::from_le_bytes(header[6..8].try_into().unwrap());
        assert_eq!(kind, EXTENT_STORE_RECORD_KIND_PACKED);
    }

    // --- Packed write eligibility ---

    #[test]
    fn packed_write_eligible_rejects_empty_payload() {
        let write = ExtentStoreWrite {
            logical_locator: "key",
            payload: &[],
            checksum: None,
        };
        assert!(!packed_extent_store_write_eligible(&write));
    }

    #[test]
    fn packed_write_eligible_accepts_small_payload() {
        let payload = vec![1u8; 1024];
        let write = ExtentStoreWrite {
            logical_locator: "key",
            payload: &payload,
            checksum: None,
        };
        assert!(packed_extent_store_write_eligible(&write));
    }

    #[test]
    fn packed_write_eligible_rejects_oversized_payload() {
        let payload = vec![1u8; EXTENT_STORE_PACKED_BLOCK_MAX_PAYLOAD_BYTES as usize + 1];
        let write = ExtentStoreWrite {
            logical_locator: "key",
            payload: &payload,
            checksum: None,
        };
        assert!(!packed_extent_store_write_eligible(&write));
    }

    // --- direct_iovec_compatible ---

    #[test]
    fn direct_iovec_compatible_empty_is_always_compatible() {
        assert!(direct_iovec_compatible(&[]));
    }

    // --- Engine put/get/delete lifecycle ---

    #[test]
    fn engine_put_get_delete_lifecycle() {
        let root = test_extent_store_root("lifecycle");
        let engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 8)
                .expect("engine should start");
        let payload = b"hello extent store";
        let locator = engine
            .put("logical/lifecycle-key", payload)
            .expect("put should succeed");
        assert!(ExtentStoreEngine::is_extent_store_locator(&locator));

        let data = engine
            .get(&locator, payload.len() as u64)
            .expect("get should succeed")
            .expect("data should exist");
        assert_eq!(data, payload);

        let deleted = engine.delete(&locator).expect("delete should succeed");
        assert!(deleted);

        let after_delete = engine.get(&locator, payload.len() as u64);
        assert!(
            after_delete.is_err(),
            "get after delete should return NotFound error"
        );
    }

    #[test]
    fn engine_put_batch_writes_multiple_records() {
        let root = test_extent_store_root("batch-write");
        let engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 64)
                .expect("engine should start");
        let writes = vec![
            ExtentStoreWrite {
                logical_locator: "batch/a",
                payload: b"alpha",
                checksum: None,
            },
            ExtentStoreWrite {
                logical_locator: "batch/b",
                payload: b"bravo",
                checksum: None,
            },
            ExtentStoreWrite {
                logical_locator: "batch/c",
                payload: b"charlie",
                checksum: None,
            },
        ];
        let results = engine.put_batch(&writes);
        assert_eq!(results.len(), 3);
        for (i, result) in results.iter().enumerate() {
            let locator = result.as_ref().expect("batch write should succeed");
            let data = engine
                .get(locator, writes[i].payload.len() as u64)
                .expect("get should succeed")
                .expect("data should exist");
            assert_eq!(data, writes[i].payload);
        }
    }

    #[test]
    fn engine_get_into_writes_to_caller_buffer() {
        let root = test_extent_store_root("get-into");
        let engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 8)
                .expect("engine should start");
        let payload = b"get-into-payload-data";
        let locator = engine
            .put("logical/get-into", payload)
            .expect("put should succeed");
        let mut buffer = vec![0u8; payload.len()];
        let bytes = engine
            .get_into(&locator, payload.len() as u64, &mut buffer)
            .expect("get_into should succeed")
            .expect("data should exist");
        assert_eq!(bytes, payload.len());
        assert_eq!(&buffer, payload);
    }

    #[test]
    fn engine_delete_nonexistent_returns_false() {
        let root = test_extent_store_root("delete-missing");
        let engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 8)
                .expect("engine should start");
        let locator = engine
            .put("logical/to-delete", b"data")
            .expect("put should succeed");
        engine
            .delete(&locator)
            .expect("first delete should succeed");
        let second = engine
            .delete(&locator)
            .expect("second delete should not error");
        assert!(!second);
    }

    #[test]
    fn engine_maintenance_stats_tracks_live_and_dead() {
        let root = test_extent_store_root("maintenance-stats");
        let engine =
            ExtentStoreEngine::new_with_segment_size(root.path(), EXTENT_STORE_ALIGNMENT * 64)
                .expect("engine should start");
        let stats_before = engine.maintenance_stats();
        assert_eq!(stats_before.live_bytes, 0);

        let payload = vec![0u8; 128];
        let loc1 = engine.put("stats/a", &payload).expect("put a");
        let _loc2 = engine.put("stats/b", &payload).expect("put b");
        let stats_after_put = engine.maintenance_stats();
        assert!(stats_after_put.live_bytes > 0);

        engine.delete(&loc1).expect("delete a");
        let stats_after_del = engine.maintenance_stats();
        assert!(stats_after_del.dead_bytes > 0);
        assert!(stats_after_del.live_bytes < stats_after_put.live_bytes);
    }

    #[test]
    fn engine_segment_rollover_on_full() {
        let root = test_extent_store_root("segment-rollover");
        let segment_size = EXTENT_STORE_ALIGNMENT * 2;
        let engine = ExtentStoreEngine::new_with_segment_size(root.path(), segment_size)
            .expect("engine should start");
        let payload = vec![1u8; EXTENT_STORE_ALIGNMENT as usize];
        let loc1 = engine.put("rollover/a", &payload).expect("first put");
        let loc2 = engine
            .put("rollover/b", &payload)
            .expect("second put triggers rollover");
        let decoded1 = ExtentStoreLocator::decode(&loc1).expect("decode loc1");
        let decoded2 = ExtentStoreLocator::decode(&loc2).expect("decode loc2");
        assert!(
            decoded2.segment_id > decoded1.segment_id,
            "second write should be in new segment"
        );
    }

    // --- parse_locator_hex edge case ---

    #[test]
    fn parse_locator_hex_none_returns_error() {
        let result = parse_locator_hex(None, "test_field");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("test_field"));
    }

    #[test]
    fn parse_locator_hex_empty_string_returns_error() {
        let result = parse_locator_hex(Some(""), "offset");
        assert!(result.is_err());
    }

    #[test]
    fn parse_locator_hex_valid_values() {
        assert_eq!(parse_locator_hex(Some("0"), "f").unwrap(), 0);
        assert_eq!(parse_locator_hex(Some("ff"), "f").unwrap(), 255);
        assert_eq!(parse_locator_hex(Some("1000"), "f").unwrap(), 4096);
        assert_eq!(
            parse_locator_hex(Some("ffffffffffffffff"), "f").unwrap(),
            u64::MAX
        );
    }
}
