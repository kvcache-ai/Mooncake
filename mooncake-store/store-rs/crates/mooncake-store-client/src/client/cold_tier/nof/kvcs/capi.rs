use std::ffi::{c_char, c_int, c_longlong, c_void, CString};
use std::sync::Mutex;

use libc::size_t;
use mooncake_store_core::error::QuotaKind;

const DEFAULT_MAX_VALUE_SIZE: u64 = 4 * 1024 * 1024;
const DEFAULT_MAX_KEY_SIZE: usize = 256;
const DEFAULT_MAX_BATCH_ITEMS: usize = 256;
const KVCS_NAMESPACE_MAX_BYTES: usize = 127;

#[repr(C)]
struct KvcsClient {
    _private: [u8; 0],
}

#[repr(C)]
struct KvcsMetaEntry {
    key: *const c_char,
    value: *const c_char,
}

#[repr(C)]
struct KvcsPutResult {
    location: [c_char; 256],
    status: c_int,
}

#[repr(C)]
struct KvcsPutItem {
    key: *const c_char,
    value_segs: *const *const c_void,
    seg_lens: *const size_t,
    seg_count: c_int,
    shard_id: i32,
    total_shard: i32,
    location: *const c_char,
    meta: *const KvcsMetaEntry,
    meta_count: c_int,
}

#[repr(C)]
struct KvcsGetItem {
    key: *const c_char,
    shard_id: i32,
    location: *const c_char,
    expected_value_size: size_t,
    meta: *const KvcsMetaEntry,
    meta_count: c_int,
}

#[repr(C)]
struct KvcsDeleteItem {
    key: *const c_char,
    shard_id: i32,
    location: *const c_char,
}

#[repr(C)]
struct KvcsDeleteResult {
    location: [c_char; 256],
    status: c_int,
}

#[repr(C)]
struct KvcsQueryOptions {
    renew: c_int,
    with_shards: c_int,
}

#[repr(C)]
struct KvcsShardResult {
    shard_id: i32,
    size: i64,
    accessed_at: i64,
}

#[repr(C)]
struct KvcsQueryResult {
    key: [c_char; 256],
    status: [c_char; 32],
    total_shard: i32,
    total_size: i64,
    meta: *mut KvcsMetaEntry,
    meta_count: c_int,
    shards: *mut KvcsShardResult,
    shard_count: c_int,
}

#[repr(C)]
struct KvcsCreateNamespaceOptions {
    space_limit: i64,
    eviction_policy: *const c_char,
    gc_threshold: f64,
}

#[repr(C)]
struct KvcsClientConfig {
    efc_socket: *const c_char,
    redis_endpoints: *const *const c_char,
    redis_count: c_int,
    redis_password: *const c_char,
    redis_pool_size: c_int,
    get_workers: c_int,
    set_workers: c_int,
    simple_workers: c_int,
    max_keys_per_batch: c_int,
    iov_size: i64,
    max_value_size: i64,
    max_key_size: c_int,
    perf_report_interval_sec: c_int,
    log_path: *const c_char,
    enable_metrics: c_int,
}

#[repr(C)]
struct KvcsLlConfig {
    efc_socket: *const c_char,
    max_keys_per_batch: c_int,
    iov_size: c_longlong,
    max_value_size: c_longlong,
    max_key_size: c_int,
    log_path: *const c_char,
    get_workers: c_int,
    set_workers: c_int,
    simple_workers: c_int,
    perf_report_interval_sec: c_int,
    enable_metrics: c_int,
}

#[repr(C)]
struct KvcsLlPutItem {
    key: *const c_char,
    value_segs: *const *const c_void,
    seg_lens: *const size_t,
    seg_count: c_int,
}

#[repr(C)]
struct KvcsLlGetItem {
    key: *const c_char,
}

#[repr(C)]
struct KvcsLlBatchOptions {
    mountpoint_index: u32,
}

unsafe extern "C" {
    fn kvcs_client_create(config: *const KvcsClientConfig) -> *mut KvcsClient;
    fn kvcs_client_destroy(client: *mut KvcsClient);
    fn kvcs_create_namespace(
        client: *mut KvcsClient,
        namespace: *const c_char,
        options: *const KvcsCreateNamespaceOptions,
    ) -> c_int;
    fn kvcs_batch_put(
        client: *mut KvcsClient,
        namespace: *const c_char,
        items: *const KvcsPutItem,
        count: c_int,
        results: *mut KvcsPutResult,
        capacity: c_int,
        deadline_ns: c_longlong,
    ) -> c_int;
    fn kvcs_batch_get_into(
        client: *mut KvcsClient,
        namespace: *const c_char,
        items: *const KvcsGetItem,
        count: c_int,
        buffers: *mut *mut c_void,
        capacities: *const size_t,
        statuses: *mut c_int,
        lengths: *mut size_t,
        deadline_ns: c_longlong,
    ) -> c_int;
    fn kvcs_batch_delete_items(
        client: *mut KvcsClient,
        namespace: *const c_char,
        items: *const KvcsDeleteItem,
        count: c_int,
        results: *mut KvcsDeleteResult,
        capacity: c_int,
        deadline_ns: c_longlong,
    ) -> c_int;
    fn kvcs_batch_query(
        client: *mut KvcsClient,
        namespace: *const c_char,
        keys: *const *const c_char,
        count: c_int,
        options: *const KvcsQueryOptions,
        results: *mut KvcsQueryResult,
        capacity: c_int,
    ) -> c_int;
    fn kvcs_query_result_free(results: *mut KvcsQueryResult, count: c_int);

    fn kvcs_ll_create(config: *const KvcsLlConfig) -> *mut KvcsClient;
    fn kvcs_ll_client_destroy(client: *mut KvcsClient);
    fn kvcs_ll_batch_put(
        client: *mut KvcsClient,
        items: *const KvcsLlPutItem,
        count: c_int,
        results: *mut KvcsPutResult,
        capacity: c_int,
        deadline_ns: c_longlong,
        options: *const KvcsLlBatchOptions,
    ) -> c_int;
    fn kvcs_ll_batch_get_into(
        client: *mut KvcsClient,
        items: *const KvcsLlGetItem,
        count: c_int,
        buffers: *mut *mut c_void,
        capacities: *const size_t,
        statuses: *mut c_int,
        lengths: *mut size_t,
        deadline_ns: c_longlong,
        options: *const KvcsLlBatchOptions,
    ) -> c_int;
    fn kvcs_ll_batch_delete(
        client: *mut KvcsClient,
        keys: *const *const c_char,
        count: c_int,
        statuses: *mut c_int,
        capacity: c_int,
        deadline_ns: c_longlong,
        options: *const KvcsLlBatchOptions,
    ) -> c_int;
    fn kvcs_ll_batch_query(
        client: *mut KvcsClient,
        keys: *const *const c_char,
        count: c_int,
        results: *mut KvcsQueryResult,
        capacity: c_int,
        options: *const KvcsLlBatchOptions,
    ) -> c_int;
}

struct ClientHandle {
    client: *mut KvcsClient,
    destroy: unsafe extern "C" fn(*mut KvcsClient),
}

// KVCS documents one client as safe for concurrent synchronous batch operations. Mutexes below
// protect only lazy construction and destruction.
unsafe impl Send for ClientHandle {}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        unsafe { (self.destroy)(self.client) };
    }
}

#[derive(Default)]
struct ClientSlot(Mutex<Option<ClientHandle>>);

impl ClientSlot {
    fn get_or_create(
        &self,
        mode: &str,
        create: impl FnOnce() -> *mut KvcsClient,
        destroy: unsafe extern "C" fn(*mut KvcsClient),
    ) -> Result<*mut KvcsClient> {
        let mut slot = self.0.lock().map_err(|_| {
            StoreError::InvalidState(format!("KVCS {mode} client lock poisoned"))
        })?;
        if let Some(handle) = slot.as_ref() {
            return Ok(handle.client);
        }
        let client = create();
        if client.is_null() {
            return Err(StoreError::Transport(format!(
                "KVCS {mode} client creation failed"
            )));
        }
        slot.replace(ClientHandle { client, destroy });
        Ok(client)
    }
}

#[derive(Clone, Copy)]
struct CommonLimits {
    max_value_size: u64,
    max_key_size: usize,
    max_batch_items: usize,
}

impl Default for CommonLimits {
    fn default() -> Self {
        Self {
            max_value_size: DEFAULT_MAX_VALUE_SIZE,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_batch_items: DEFAULT_MAX_BATCH_ITEMS,
        }
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32> {
    match std::env::var(name) {
        Ok(value) => value.parse().map_err(|_| {
            StoreError::InvalidState(format!("{name} must be a non-negative integer"))
        }),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => Err(StoreError::InvalidState(format!(
            "{name} is not valid UTF-8"
        ))),
    }
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(StoreError::InvalidState(format!(
            "{name} is not valid UTF-8"
        ))),
    }
}

fn optional_cstring(name: &str) -> Result<Option<CString>> {
    optional_env(name)?
        .filter(|value| !value.is_empty())
        .map(|value| {
            CString::new(value)
                .map_err(|_| StoreError::InvalidState(format!("{name} contains NUL")))
        })
        .transpose()
}

fn checked_batch_len(len: usize) -> Result<c_int> {
    c_int::try_from(len).map_err(|_| {
        StoreError::InvalidState(format!("KVCS batch size {len} exceeds the C ABI limit"))
    })
}

fn map_call_status(status: c_int, operation: &str) -> StoreError {
    match status.unsigned_abs() {
        1 => StoreError::NotFound(format!("KVCS {operation}")),
        2 => StoreError::Backpressure(format!("KVCS {operation} is incomplete")),
        3 => StoreError::Transport(format!("KVCS {operation} provider is unavailable")),
        6 => StoreError::NotFound(format!("KVCS {operation} namespace")),
        4 => StoreError::Backpressure(format!("KVCS {operation} resource exhausted")),
        5 => StoreError::InvalidState(format!("KVCS {operation} invalid argument")),
        7 => StoreError::Backpressure(format!("KVCS {operation} buffer too small")),
        8 => StoreError::Conflict(format!("KVCS {operation} already exists")),
        _ => StoreError::Transport(format!("KVCS {operation} failed: status={status}")),
    }
}

fn map_item_status(status: c_int, operation: &str, write: bool) -> StoreError {
    let code = status.unsigned_abs();
    if code == libc::ENOENT as u32 {
        StoreError::NotFound(format!("KVCS {operation}"))
    } else if code == libc::EEXIST as u32 {
        StoreError::Conflict(format!("KVCS {operation} already exists"))
    } else if code == libc::ENOSPC as u32 {
        if write {
            StoreError::QuotaExceeded {
                kind: QuotaKind::Bytes,
                message: format!("KVCS {operation} space exhausted"),
            }
        } else {
            StoreError::Backpressure(format!("KVCS {operation} destination buffer too small"))
        }
    } else if code == libc::EINVAL as u32
        || code == libc::E2BIG as u32
        || code == libc::ENAMETOOLONG as u32
    {
        StoreError::InvalidState(format!("KVCS {operation} invalid argument"))
    } else if code == libc::ENODEV as u32 {
        StoreError::Transport(format!("KVCS {operation} mountpoint unavailable"))
    } else if code == libc::ENOMEM as u32 {
        StoreError::Backpressure(format!("KVCS {operation} memory exhausted"))
    } else {
        StoreError::Transport(format!("KVCS {operation} failed: errno={status}"))
    }
}

fn fill_missing<T>(results: &mut [Option<Result<T>>], error: &StoreError) {
    for result in results {
        if result.is_none() {
            *result = Some(Err(error.clone()));
        }
    }
}

fn fail_positional_results<T>(
    mut results: Vec<Option<Result<T>>>,
    error: StoreError,
) -> Vec<Result<T>> {
    fill_missing(&mut results, &error);
    finish_positional_results(results)
}

fn validate_string(value: &str, limit: usize, label: &str) -> Result<CString> {
    if value.len() > limit {
        return Err(StoreError::InvalidState(format!(
            "KVCS {label} is {} bytes, exceeding provider limit {limit}",
            value.len()
        )));
    }
    CString::new(value).map_err(|_| StoreError::InvalidState(format!("KVCS {label} contains NUL")))
}

fn encode_physical_key(key: &OpaquePhysicalKey, key_limit: usize) -> Result<CString> {
    let encoded = key.to_hex();
    validate_string(&encoded, key_limit, "encoded low-level key")
}

fn char_array_string(value: &[c_char]) -> String {
    let bytes = value
        .iter()
        .take_while(|byte| **byte != 0)
        .map(|byte| *byte as u8)
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn finish_positional_results<T>(results: Vec<Option<Result<T>>>) -> Vec<Result<T>> {
    results
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|| {
                Err(StoreError::Transport(
                    "KVCS executor omitted a positional batch result".to_string(),
                ))
            })
        })
        .collect()
}
