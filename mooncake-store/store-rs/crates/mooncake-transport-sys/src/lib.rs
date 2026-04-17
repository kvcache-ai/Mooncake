use std::ffi::{c_char, c_int, c_void, CStr, CString};

const CLASSIC_TE_LIB_PATH: &str = env!("MOONCAKE_CLASSIC_TE_LIB_PATH");
const TENT_SHARED_LIB_PATH: &str = env!("MOONCAKE_TENT_SHARED_LIB_PATH");
const CLASSIC_SHIM_LIB_PATH: &str = env!("MOONCAKE_CLASSIC_SHIM_LIB_PATH");
const TENT_SHIM_LIB_PATH: &str = env!("MOONCAKE_TENT_SHIM_LIB_PATH");

pub const DEFAULT_UPSTREAM_DIR: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../third_party/Mooncake");
pub const DEFAULT_UPSTREAM_BUILD_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../third_party/Mooncake/build-rust"
);

#[derive(Copy, Clone)]
struct DynamicLibrary(*mut c_void);

unsafe impl Send for DynamicLibrary {}
unsafe impl Sync for DynamicLibrary {}

fn load_library(path: &str) -> Option<DynamicLibrary> {
    let path_cstr = CString::new(path).ok()?;
    let handle = unsafe { libc::dlopen(path_cstr.as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL) };
    if handle.is_null() {
        eprintln!("failed to dlopen {path}: {}", dlerror_string());
        None
    } else {
        Some(DynamicLibrary(handle))
    }
}

fn load_symbol<T: Copy>(library: DynamicLibrary, symbol: &str) -> Option<T> {
    let symbol_cstr = CString::new(symbol).ok()?;
    unsafe {
        libc::dlerror();
        let raw = libc::dlsym(library.0, symbol_cstr.as_ptr());
        if raw.is_null() {
            eprintln!("failed to dlsym {symbol}: {}", dlerror_string());
            None
        } else {
            Some(std::mem::transmute_copy(&raw))
        }
    }
}

fn dlerror_string() -> String {
    unsafe {
        let error = libc::dlerror();
        if error.is_null() {
            "unknown dynamic loader error".to_string()
        } else {
            CStr::from_ptr(error).to_string_lossy().into_owned()
        }
    }
}

pub mod classic {
    #![allow(non_snake_case)]

    use super::{
        c_char, c_int, c_void, load_library, load_symbol, DynamicLibrary, CLASSIC_SHIM_LIB_PATH,
        CLASSIC_TE_LIB_PATH,
    };
    use std::ptr;
    use std::sync::OnceLock;

    pub type TransferEngineHandle = *mut c_void;
    pub type SegmentId = i32;
    pub type BatchId = u64;
    pub type TransportHandle = *mut c_void;

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

    struct ClassicApi {
        _transfer_engine: DynamicLibrary,
        _shim: DynamicLibrary,
        create_transfer_engine: unsafe extern "C" fn(
            *const c_char,
            *const c_char,
            *const c_char,
            u64,
            c_int,
        ) -> TransferEngineHandle,
        get_local_ip_and_port:
            unsafe extern "C" fn(TransferEngineHandle, *mut c_char, usize) -> c_int,
        install_transport: unsafe extern "C" fn(
            TransferEngineHandle,
            *const c_char,
            *mut *mut c_void,
        ) -> TransportHandle,
        destroy_transfer_engine: unsafe extern "C" fn(TransferEngineHandle),
        register_local_memory: unsafe extern "C" fn(
            TransferEngineHandle,
            *mut c_void,
            usize,
            *const c_char,
            c_int,
        ) -> c_int,
        unregister_local_memory: unsafe extern "C" fn(TransferEngineHandle, *mut c_void) -> c_int,
        open_segment: unsafe extern "C" fn(TransferEngineHandle, *const c_char) -> SegmentId,
        open_segment_no_cache:
            unsafe extern "C" fn(TransferEngineHandle, *const c_char) -> SegmentId,
        close_segment: unsafe extern "C" fn(TransferEngineHandle, SegmentId) -> c_int,
        allocate_batch_id: unsafe extern "C" fn(TransferEngineHandle, usize) -> BatchId,
        submit_transfer: unsafe extern "C" fn(
            TransferEngineHandle,
            BatchId,
            *mut TransferRequest,
            usize,
        ) -> c_int,
        get_transfer_status: unsafe extern "C" fn(
            TransferEngineHandle,
            BatchId,
            usize,
            *mut TransferStatus,
        ) -> c_int,
        free_batch_id: unsafe extern "C" fn(TransferEngineHandle, BatchId) -> c_int,
        sync_segment_cache: unsafe extern "C" fn(TransferEngineHandle) -> c_int,
        republish_local_metadata: unsafe extern "C" fn(TransferEngineHandle) -> c_int,
        get_batch_transfer_status:
            unsafe extern "C" fn(TransferEngineHandle, BatchId, *mut TransferStatus) -> c_int,
        get_segment_first_buffer:
            unsafe extern "C" fn(TransferEngineHandle, SegmentId, *mut u64, *mut u64) -> c_int,
        get_segment_buffer_count:
            unsafe extern "C" fn(TransferEngineHandle, SegmentId, *mut usize) -> c_int,
        get_segment_buffer: unsafe extern "C" fn(
            TransferEngineHandle,
            SegmentId,
            usize,
            *mut u64,
            *mut u64,
        ) -> c_int,
        get_max_mr_size: unsafe extern "C" fn() -> u64,
    }

    unsafe impl Send for ClassicApi {}
    unsafe impl Sync for ClassicApi {}

    fn api() -> Option<&'static ClassicApi> {
        static API: OnceLock<Option<ClassicApi>> = OnceLock::new();
        API.get_or_init(load_api).as_ref()
    }

    fn load_api() -> Option<ClassicApi> {
        let transfer_engine = load_library(CLASSIC_TE_LIB_PATH)?;
        let shim = load_library(CLASSIC_SHIM_LIB_PATH)?;
        Some(ClassicApi {
            create_transfer_engine: load_symbol(transfer_engine, "createTransferEngine")?,
            get_local_ip_and_port: load_symbol(transfer_engine, "getLocalIpAndPort")?,
            install_transport: load_symbol(transfer_engine, "installTransport")?,
            destroy_transfer_engine: load_symbol(transfer_engine, "destroyTransferEngine")?,
            register_local_memory: load_symbol(transfer_engine, "registerLocalMemory")?,
            unregister_local_memory: load_symbol(transfer_engine, "unregisterLocalMemory")?,
            open_segment: load_symbol(transfer_engine, "openSegment")?,
            open_segment_no_cache: load_symbol(transfer_engine, "openSegmentNoCache")?,
            close_segment: load_symbol(transfer_engine, "closeSegment")?,
            allocate_batch_id: load_symbol(transfer_engine, "allocateBatchID")?,
            submit_transfer: load_symbol(transfer_engine, "submitTransfer")?,
            get_transfer_status: load_symbol(transfer_engine, "getTransferStatus")?,
            free_batch_id: load_symbol(transfer_engine, "freeBatchID")?,
            sync_segment_cache: load_symbol(transfer_engine, "syncSegmentCache")?,
            republish_local_metadata: load_symbol(
                shim,
                "mooncake_classic_republish_local_metadata",
            )?,
            get_batch_transfer_status: load_symbol(
                shim,
                "mooncake_classic_get_batch_transfer_status",
            )?,
            get_segment_first_buffer: load_symbol(
                shim,
                "mooncake_classic_get_segment_first_buffer",
            )?,
            get_segment_buffer_count: load_symbol(
                shim,
                "mooncake_classic_get_segment_buffer_count",
            )?,
            get_segment_buffer: load_symbol(shim, "mooncake_classic_get_segment_buffer")?,
            get_max_mr_size: load_symbol(shim, "mooncake_classic_get_max_mr_size")?,
            _transfer_engine: transfer_engine,
            _shim: shim,
        })
    }

    pub unsafe extern "C" fn createTransferEngine(
        metadata_conn_string: *const c_char,
        local_server_name: *const c_char,
        ip_or_host_name: *const c_char,
        rpc_port: u64,
        auto_discover: c_int,
    ) -> TransferEngineHandle {
        api().map_or(ptr::null_mut(), |api| {
            (api.create_transfer_engine)(
                metadata_conn_string,
                local_server_name,
                ip_or_host_name,
                rpc_port,
                auto_discover,
            )
        })
    }

    pub unsafe extern "C" fn getLocalIpAndPort(
        engine: TransferEngineHandle,
        buf_out: *mut c_char,
        buf_len: usize,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.get_local_ip_and_port)(engine, buf_out, buf_len)
        })
    }

    pub unsafe extern "C" fn installTransport(
        engine: TransferEngineHandle,
        proto: *const c_char,
        args: *mut *mut c_void,
    ) -> TransportHandle {
        api().map_or(ptr::null_mut(), |api| {
            (api.install_transport)(engine, proto, args)
        })
    }

    pub unsafe extern "C" fn destroyTransferEngine(engine: TransferEngineHandle) {
        if let Some(api) = api() {
            (api.destroy_transfer_engine)(engine);
        }
    }

    pub unsafe extern "C" fn registerLocalMemory(
        engine: TransferEngineHandle,
        addr: *mut c_void,
        length: usize,
        location: *const c_char,
        remote_accessible: c_int,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.register_local_memory)(engine, addr, length, location, remote_accessible)
        })
    }

    pub unsafe extern "C" fn unregisterLocalMemory(
        engine: TransferEngineHandle,
        addr: *mut c_void,
    ) -> c_int {
        api().map_or(-1, |api| (api.unregister_local_memory)(engine, addr))
    }

    pub unsafe extern "C" fn openSegment(
        engine: TransferEngineHandle,
        segment_name: *const c_char,
    ) -> SegmentId {
        api().map_or(-1, |api| (api.open_segment)(engine, segment_name))
    }

    pub unsafe extern "C" fn openSegmentNoCache(
        engine: TransferEngineHandle,
        segment_name: *const c_char,
    ) -> SegmentId {
        api().map_or(-1, |api| (api.open_segment_no_cache)(engine, segment_name))
    }

    pub unsafe extern "C" fn closeSegment(
        engine: TransferEngineHandle,
        segment_id: SegmentId,
    ) -> c_int {
        api().map_or(-1, |api| (api.close_segment)(engine, segment_id))
    }

    pub unsafe extern "C" fn allocateBatchID(
        engine: TransferEngineHandle,
        batch_size: usize,
    ) -> BatchId {
        api().map_or(0, |api| (api.allocate_batch_id)(engine, batch_size))
    }

    pub unsafe extern "C" fn submitTransfer(
        engine: TransferEngineHandle,
        batch_id: BatchId,
        entries: *mut TransferRequest,
        count: usize,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.submit_transfer)(engine, batch_id, entries, count)
        })
    }

    pub unsafe extern "C" fn getTransferStatus(
        engine: TransferEngineHandle,
        batch_id: BatchId,
        task_id: usize,
        status: *mut TransferStatus,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.get_transfer_status)(engine, batch_id, task_id, status)
        })
    }

    pub unsafe extern "C" fn freeBatchID(engine: TransferEngineHandle, batch_id: BatchId) -> c_int {
        api().map_or(-1, |api| (api.free_batch_id)(engine, batch_id))
    }

    pub unsafe extern "C" fn syncSegmentCache(engine: TransferEngineHandle) -> c_int {
        api().map_or(-1, |api| (api.sync_segment_cache)(engine))
    }

    pub unsafe extern "C" fn mooncake_classic_republish_local_metadata(
        engine: TransferEngineHandle,
    ) -> c_int {
        api().map_or(-1, |api| (api.republish_local_metadata)(engine))
    }

    pub unsafe extern "C" fn mooncake_classic_get_batch_transfer_status(
        engine: TransferEngineHandle,
        batch_id: BatchId,
        status: *mut TransferStatus,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.get_batch_transfer_status)(engine, batch_id, status)
        })
    }

    pub unsafe extern "C" fn mooncake_classic_get_segment_first_buffer(
        engine: TransferEngineHandle,
        segment_id: SegmentId,
        addr_out: *mut u64,
        length_out: *mut u64,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.get_segment_first_buffer)(engine, segment_id, addr_out, length_out)
        })
    }

    pub unsafe extern "C" fn mooncake_classic_get_segment_buffer_count(
        engine: TransferEngineHandle,
        segment_id: SegmentId,
        count_out: *mut usize,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.get_segment_buffer_count)(engine, segment_id, count_out)
        })
    }

    pub unsafe extern "C" fn mooncake_classic_get_segment_buffer(
        engine: TransferEngineHandle,
        segment_id: SegmentId,
        index: usize,
        addr_out: *mut u64,
        length_out: *mut u64,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.get_segment_buffer)(engine, segment_id, index, addr_out, length_out)
        })
    }

    pub unsafe extern "C" fn mooncake_classic_get_max_mr_size() -> u64 {
        api().map_or(0, |api| (api.get_max_mr_size)())
    }
}

pub mod tent {
    #![allow(non_snake_case)]

    use super::{
        c_char, c_int, c_void, load_library, load_symbol, DynamicLibrary, TENT_SHARED_LIB_PATH,
        TENT_SHIM_LIB_PATH,
    };
    use std::ptr;
    use std::sync::OnceLock;

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

    struct TentApi {
        _tent_shared: DynamicLibrary,
        _shim: DynamicLibrary,
        load_config_from_file: unsafe extern "C" fn(*const c_char),
        set_config: unsafe extern "C" fn(*const c_char, *const c_char),
        create_engine: unsafe extern "C" fn() -> TentEngineHandle,
        destroy_engine: unsafe extern "C" fn(TentEngineHandle),
        segment_name: unsafe extern "C" fn(TentEngineHandle, *mut c_char, usize) -> c_int,
        rpc_server_addr_port:
            unsafe extern "C" fn(TentEngineHandle, *mut c_char, usize, *mut u16) -> c_int,
        open_segment:
            unsafe extern "C" fn(TentEngineHandle, *mut SegmentId, *const c_char) -> c_int,
        close_segment: unsafe extern "C" fn(TentEngineHandle, SegmentId) -> c_int,
        get_segment_info:
            unsafe extern "C" fn(TentEngineHandle, SegmentId, *mut TentSegmentInfo) -> c_int,
        free_segment_info: unsafe extern "C" fn(*mut TentSegmentInfo),
        allocate_memory:
            unsafe extern "C" fn(TentEngineHandle, *mut *mut c_void, usize, *const c_char) -> c_int,
        free_memory: unsafe extern "C" fn(TentEngineHandle, *mut c_void) -> c_int,
        register_memory: unsafe extern "C" fn(TentEngineHandle, *mut c_void, usize) -> c_int,
        unregister_memory: unsafe extern "C" fn(TentEngineHandle, *mut c_void, usize) -> c_int,
        platform_free_memory: unsafe extern "C" fn(*mut c_void, usize) -> c_int,
        probe_rdma_max_mr_size: unsafe extern "C" fn() -> u64,
        allocate_batch: unsafe extern "C" fn(TentEngineHandle, usize) -> BatchId,
        free_batch: unsafe extern "C" fn(TentEngineHandle, BatchId) -> c_int,
        submit: unsafe extern "C" fn(TentEngineHandle, BatchId, *mut TentRequest, usize) -> c_int,
        task_status:
            unsafe extern "C" fn(TentEngineHandle, BatchId, usize, *mut TentStatus) -> c_int,
        overall_status: unsafe extern "C" fn(TentEngineHandle, BatchId, *mut TentStatus) -> c_int,
    }

    unsafe impl Send for TentApi {}
    unsafe impl Sync for TentApi {}

    fn api() -> Option<&'static TentApi> {
        static API: OnceLock<Option<TentApi>> = OnceLock::new();
        API.get_or_init(load_api).as_ref()
    }

    fn load_api() -> Option<TentApi> {
        let tent_shared = load_library(TENT_SHARED_LIB_PATH)?;
        let shim = load_library(TENT_SHIM_LIB_PATH)?;
        Some(TentApi {
            load_config_from_file: load_symbol(tent_shared, "tent_load_config_from_file")?,
            set_config: load_symbol(tent_shared, "tent_set_config")?,
            create_engine: load_symbol(tent_shared, "tent_create_engine")?,
            destroy_engine: load_symbol(tent_shared, "tent_destroy_engine")?,
            segment_name: load_symbol(tent_shared, "tent_segment_name")?,
            rpc_server_addr_port: load_symbol(tent_shared, "tent_rpc_server_addr_port")?,
            open_segment: load_symbol(tent_shared, "tent_open_segment")?,
            close_segment: load_symbol(tent_shared, "tent_close_segment")?,
            get_segment_info: load_symbol(tent_shared, "tent_get_segment_info")?,
            free_segment_info: load_symbol(tent_shared, "tent_free_segment_info")?,
            allocate_memory: load_symbol(tent_shared, "tent_allocate_memory")?,
            free_memory: load_symbol(tent_shared, "tent_free_memory")?,
            register_memory: load_symbol(tent_shared, "tent_register_memory")?,
            unregister_memory: load_symbol(tent_shared, "tent_unregister_memory")?,
            platform_free_memory: load_symbol(shim, "mooncake_tent_platform_free_memory")?,
            probe_rdma_max_mr_size: load_symbol(shim, "mooncake_tent_probe_rdma_max_mr_size")?,
            allocate_batch: load_symbol(tent_shared, "tent_allocate_batch")?,
            free_batch: load_symbol(tent_shared, "tent_free_batch")?,
            submit: load_symbol(tent_shared, "tent_submit")?,
            task_status: load_symbol(tent_shared, "tent_task_status")?,
            overall_status: load_symbol(tent_shared, "tent_overall_status")?,
            _tent_shared: tent_shared,
            _shim: shim,
        })
    }

    pub unsafe extern "C" fn tent_load_config_from_file(path: *const c_char) {
        if let Some(api) = api() {
            (api.load_config_from_file)(path);
        }
    }

    pub unsafe extern "C" fn tent_set_config(key: *const c_char, value: *const c_char) {
        if let Some(api) = api() {
            (api.set_config)(key, value);
        }
    }

    pub unsafe extern "C" fn tent_create_engine() -> TentEngineHandle {
        api().map_or(ptr::null_mut(), |api| (api.create_engine)())
    }

    pub unsafe extern "C" fn tent_destroy_engine(engine: TentEngineHandle) {
        if let Some(api) = api() {
            (api.destroy_engine)(engine);
        }
    }

    pub unsafe extern "C" fn tent_segment_name(
        engine: TentEngineHandle,
        buf: *mut c_char,
        buf_len: usize,
    ) -> c_int {
        api().map_or(-1, |api| (api.segment_name)(engine, buf, buf_len))
    }

    pub unsafe extern "C" fn tent_rpc_server_addr_port(
        engine: TentEngineHandle,
        addr_buf: *mut c_char,
        buf_len: usize,
        port: *mut u16,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.rpc_server_addr_port)(engine, addr_buf, buf_len, port)
        })
    }

    pub unsafe extern "C" fn tent_open_segment(
        engine: TentEngineHandle,
        handle: *mut SegmentId,
        segment_name: *const c_char,
    ) -> c_int {
        api().map_or(-1, |api| (api.open_segment)(engine, handle, segment_name))
    }

    pub unsafe extern "C" fn tent_close_segment(
        engine: TentEngineHandle,
        handle: SegmentId,
    ) -> c_int {
        api().map_or(-1, |api| (api.close_segment)(engine, handle))
    }

    pub unsafe extern "C" fn tent_get_segment_info(
        engine: TentEngineHandle,
        handle: SegmentId,
        info: *mut TentSegmentInfo,
    ) -> c_int {
        api().map_or(-1, |api| (api.get_segment_info)(engine, handle, info))
    }

    pub unsafe extern "C" fn tent_free_segment_info(info: *mut TentSegmentInfo) {
        if let Some(api) = api() {
            (api.free_segment_info)(info);
        }
    }

    pub unsafe extern "C" fn tent_allocate_memory(
        engine: TentEngineHandle,
        addr: *mut *mut c_void,
        size: usize,
        location: *const c_char,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.allocate_memory)(engine, addr, size, location)
        })
    }

    pub unsafe extern "C" fn tent_free_memory(
        engine: TentEngineHandle,
        addr: *mut c_void,
    ) -> c_int {
        api().map_or(-1, |api| (api.free_memory)(engine, addr))
    }

    pub unsafe extern "C" fn tent_register_memory(
        engine: TentEngineHandle,
        addr: *mut c_void,
        size: usize,
    ) -> c_int {
        api().map_or(-1, |api| (api.register_memory)(engine, addr, size))
    }

    pub unsafe extern "C" fn tent_unregister_memory(
        engine: TentEngineHandle,
        addr: *mut c_void,
        size: usize,
    ) -> c_int {
        api().map_or(-1, |api| (api.unregister_memory)(engine, addr, size))
    }

    pub unsafe extern "C" fn mooncake_tent_platform_free_memory(
        addr: *mut c_void,
        size: usize,
    ) -> c_int {
        api().map_or(-1, |api| (api.platform_free_memory)(addr, size))
    }

    pub unsafe extern "C" fn mooncake_tent_probe_rdma_max_mr_size() -> u64 {
        api().map_or(0, |api| (api.probe_rdma_max_mr_size)())
    }

    pub unsafe extern "C" fn tent_allocate_batch(
        engine: TentEngineHandle,
        batch_size: usize,
    ) -> BatchId {
        api().map_or(0, |api| (api.allocate_batch)(engine, batch_size))
    }

    pub unsafe extern "C" fn tent_free_batch(engine: TentEngineHandle, batch_id: BatchId) -> c_int {
        api().map_or(-1, |api| (api.free_batch)(engine, batch_id))
    }

    pub unsafe extern "C" fn tent_submit(
        engine: TentEngineHandle,
        batch_id: BatchId,
        entries: *mut TentRequest,
        count: usize,
    ) -> c_int {
        api().map_or(-1, |api| (api.submit)(engine, batch_id, entries, count))
    }

    pub unsafe extern "C" fn tent_task_status(
        engine: TentEngineHandle,
        batch_id: BatchId,
        task_id: usize,
        status: *mut TentStatus,
    ) -> c_int {
        api().map_or(-1, |api| {
            (api.task_status)(engine, batch_id, task_id, status)
        })
    }

    pub unsafe extern "C" fn tent_overall_status(
        engine: TentEngineHandle,
        batch_id: BatchId,
        status: *mut TentStatus,
    ) -> c_int {
        api().map_or(-1, |api| (api.overall_status)(engine, batch_id, status))
    }
}
