use std::ffi::{c_char, c_int, c_void};

pub const DEFAULT_UPSTREAM_DIR: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../third_party/Mooncake");
pub const DEFAULT_UPSTREAM_BUILD_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../third_party/Mooncake/build-rust"
);

pub mod classic {
    use super::{c_char, c_int, c_void};

    pub type TransferEngineHandle = *mut c_void;
    pub type SegmentId = i32;
    pub type BatchId = u64;

    pub const OPCODE_READ: c_int = 0;
    pub const OPCODE_WRITE: c_int = 1;

    pub const STATUS_WAITING: c_int = 0;
    pub const STATUS_PENDING: c_int = 1;
    pub const STATUS_INVALID: c_int = 2;
    pub const STATUS_CANCELED: c_int = 3;
    pub const STATUS_COMPLETED: c_int = 4;
    pub const STATUS_TIMEOUT: c_int = 5;
    pub const STATUS_FAILED: c_int = 6;

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct TransferRequest {
        pub opcode: c_int,
        pub source: *mut c_void,
        pub target_id: SegmentId,
        pub target_offset: u64,
        pub length: u64,
    }

    #[repr(C)]
    #[derive(Copy, Clone, Default)]
    pub struct TransferStatus {
        pub status: c_int,
        pub transferred_bytes: u64,
    }

    unsafe extern "C" {
        pub fn createTransferEngine(
            metadata_conn_string: *const c_char,
            local_server_name: *const c_char,
            ip_or_host_name: *const c_char,
            rpc_port: u64,
            auto_discover: c_int,
        ) -> TransferEngineHandle;
        pub fn destroyTransferEngine(engine: TransferEngineHandle);
        pub fn registerLocalMemory(
            engine: TransferEngineHandle,
            addr: *mut c_void,
            length: usize,
            location: *const c_char,
            remote_accessible: c_int,
        ) -> c_int;
        pub fn unregisterLocalMemory(engine: TransferEngineHandle, addr: *mut c_void) -> c_int;
        pub fn openSegment(engine: TransferEngineHandle, segment_name: *const c_char) -> SegmentId;
        pub fn closeSegment(engine: TransferEngineHandle, segment_id: SegmentId) -> c_int;
        pub fn allocateBatchID(engine: TransferEngineHandle, batch_size: usize) -> BatchId;
        pub fn submitTransfer(
            engine: TransferEngineHandle,
            batch_id: BatchId,
            entries: *mut TransferRequest,
            count: usize,
        ) -> c_int;
        pub fn getTransferStatus(
            engine: TransferEngineHandle,
            batch_id: BatchId,
            task_id: usize,
            status: *mut TransferStatus,
        ) -> c_int;
        pub fn freeBatchID(engine: TransferEngineHandle, batch_id: BatchId) -> c_int;
        pub fn syncSegmentCache(engine: TransferEngineHandle) -> c_int;
    }
}

pub mod tent {
    use super::{c_char, c_int, c_void};

    pub type TentEngineHandle = *mut c_void;
    pub type SegmentId = u64;
    pub type BatchId = u64;

    pub const OPCODE_READ: c_int = 0;
    pub const OPCODE_WRITE: c_int = 1;

    pub const STATUS_WAITING: c_int = 0;
    pub const STATUS_PENDING: c_int = 1;
    pub const STATUS_INVALID: c_int = 2;
    pub const STATUS_CANCELED: c_int = 3;
    pub const STATUS_COMPLETED: c_int = 4;
    pub const STATUS_TIMEOUT: c_int = 5;
    pub const STATUS_FAILED: c_int = 6;

    pub const TYPE_MEMORY: c_int = 0;
    pub const TYPE_FILE: c_int = 1;

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct TentRequest {
        pub opcode: c_int,
        pub source: *mut c_void,
        pub target_id: SegmentId,
        pub target_offset: u64,
        pub length: u64,
    }

    #[repr(C)]
    #[derive(Copy, Clone, Default)]
    pub struct TentStatus {
        pub status: c_int,
        pub transferred_bytes: u64,
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct TentBufferInfo {
        pub base: u64,
        pub length: u64,
        pub location: [c_char; 64],
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct TentSegmentInfo {
        pub kind: c_int,
        pub num_buffers: c_int,
        pub buffers: *mut TentBufferInfo,
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct TentNotificationRecord {
        pub handle: SegmentId,
        pub content: [c_char; 4096],
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct TentNotificationInfo {
        pub num_records: c_int,
        pub records: *mut TentNotificationRecord,
    }

    unsafe extern "C" {
        pub fn tent_load_config_from_file(path: *const c_char);
        pub fn tent_set_config(key: *const c_char, value: *const c_char);
        pub fn tent_create_engine() -> TentEngineHandle;
        pub fn tent_destroy_engine(engine: TentEngineHandle);
        pub fn tent_segment_name(
            engine: TentEngineHandle,
            buf: *mut c_char,
            buf_len: usize,
        ) -> c_int;
        pub fn tent_rpc_server_addr_port(
            engine: TentEngineHandle,
            addr_buf: *mut c_char,
            buf_len: usize,
            port: *mut u16,
        ) -> c_int;
        pub fn tent_open_segment(
            engine: TentEngineHandle,
            handle: *mut SegmentId,
            segment_name: *const c_char,
        ) -> c_int;
        pub fn tent_close_segment(engine: TentEngineHandle, handle: SegmentId) -> c_int;
        pub fn tent_get_segment_info(
            engine: TentEngineHandle,
            handle: SegmentId,
            info: *mut TentSegmentInfo,
        ) -> c_int;
        pub fn tent_free_segment_info(info: *mut TentSegmentInfo);
        pub fn tent_allocate_memory(
            engine: TentEngineHandle,
            addr: *mut *mut c_void,
            size: usize,
            location: *const c_char,
        ) -> c_int;
        pub fn tent_free_memory(engine: TentEngineHandle, addr: *mut c_void) -> c_int;
        pub fn tent_register_memory(
            engine: TentEngineHandle,
            addr: *mut c_void,
            size: usize,
        ) -> c_int;
        pub fn tent_unregister_memory(
            engine: TentEngineHandle,
            addr: *mut c_void,
            size: usize,
        ) -> c_int;
        pub fn tent_allocate_batch(engine: TentEngineHandle, batch_size: usize) -> BatchId;
        pub fn tent_free_batch(engine: TentEngineHandle, batch_id: BatchId) -> c_int;
        pub fn tent_submit(
            engine: TentEngineHandle,
            batch_id: BatchId,
            entries: *mut TentRequest,
            count: usize,
        ) -> c_int;
        pub fn tent_task_status(
            engine: TentEngineHandle,
            batch_id: BatchId,
            task_id: usize,
            status: *mut TentStatus,
        ) -> c_int;
        pub fn tent_overall_status(
            engine: TentEngineHandle,
            batch_id: BatchId,
            status: *mut TentStatus,
        ) -> c_int;
    }
}
