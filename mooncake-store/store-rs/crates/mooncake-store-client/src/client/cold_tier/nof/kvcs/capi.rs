use std::ffi::{c_char, c_int, c_void, CString};
use std::sync::Mutex;

use libc::size_t;
use mooncake_store_core::error::QuotaKind;

const SDK_DEFAULT_MAX_VALUE_SIZE: u64 = 4 * 1024 * 1024;
const SDK_MAX_VALUE_SIZE: u64 = 4 * 1024 * 1024 * 1024;
const DEFAULT_MAX_KEY_SIZE: usize = 256;
const KVCS_NAMESPACE_MAX_BYTES: usize = 127;

#[allow(dead_code, non_camel_case_types)]
mod sdk_ffi {
    include!(env!("KVCS_SDK_RUST_FFI"));
}

use sdk_ffi::{
    kvcs_batch_delete_items, kvcs_batch_get_into, kvcs_batch_put, kvcs_batch_query,
    kvcs_client_create, kvcs_client_destroy, kvcs_client_t as KvcsClient, kvcs_create_namespace,
    kvcs_create_ns_opts_t as KvcsCreateNamespaceOptions,
    kvcs_delete_item_t as KvcsDeleteItem, kvcs_delete_result_t as KvcsDeleteResult,
    kvcs_get_item_t as KvcsGetItem, kvcs_ll_batch_delete, kvcs_ll_batch_get,
    kvcs_ll_batch_get_into,
    kvcs_ll_batch_opts_t as KvcsLlBatchOptions, kvcs_ll_batch_put, kvcs_ll_batch_query,
    kvcs_ll_config_t as KvcsLlConfig, kvcs_ll_create, kvcs_ll_get_item_t as KvcsLlGetItem,
    kvcs_ll_put_item_t as KvcsLlPutItem, kvcs_put_item_t as KvcsPutItem,
    kvcs_put_result_t as KvcsPutResult, kvcs_query_opts_t as KvcsQueryOptions,
    kvcs_query_result_free, kvcs_query_result_t as KvcsQueryResult,
};

impl Default for sdk_ffi::kvcs_client_config_t {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

impl Default for sdk_ffi::kvcs_ll_config_t {
    fn default() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

type KvcsClientConfig = sdk_ffi::kvcs_client_config_t;

struct QueryResults {
    raw: Vec<KvcsQueryResult>,
    initialized: c_int,
}

impl QueryResults {
    fn new(count: c_int) -> Self {
        let len = usize::try_from(count).expect("KVCS query result count is non-negative");
        Self {
            raw: (0..len)
                .map(|_| unsafe { std::mem::zeroed() })
                .collect(),
            initialized: 0,
        }
    }

    fn as_mut_ptr(&mut self) -> *mut KvcsQueryResult {
        self.raw.as_mut_ptr()
    }

    fn as_slice(&self) -> &[KvcsQueryResult] {
        &self.raw[..self.initialized as usize]
    }

    fn complete(&mut self, returned: c_int, operation: &str) -> Result<()> {
        if returned < 0 {
            return Err(map_call_status(returned, operation));
        }
        let requested = self.raw.len();
        let requested_c = c_int::try_from(requested)
            .expect("query result allocation count came from a C integer");
        self.initialized = if returned == 0 {
            requested_c
        } else {
            returned.min(requested_c)
        };
        if returned != 0 && returned != requested_c {
            return Err(StoreError::Transport(format!(
                "KVCS {operation} returned {returned} results for {requested} requests"
            )));
        }
        Ok(())
    }
}

impl Drop for QueryResults {
    fn drop(&mut self) {
        if !self.raw.is_empty() {
            unsafe { kvcs_query_result_free(self.raw.as_mut_ptr(), self.initialized) };
        }
    }
}

#[cfg(test)]
mod query_result_tests {
    use super::*;

    #[test]
    fn query_completion_accepts_zero_as_the_full_requested_count() {
        let mut results = QueryResults::new(2);
        results.complete(0, "test query").unwrap();
        assert_eq!(results.as_slice().len(), 2);
    }

    #[test]
    fn query_completion_tracks_only_the_initialized_prefix() {
        let mut results = QueryResults::new(2);
        assert!(results.complete(1, "test query").is_err());
        assert_eq!(results.as_slice().len(), 1);
    }
}

unsafe extern "C" {
    fn kvcs_ll_client_destroy(client: *mut KvcsClient);
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

fn input_segments<'a>(
    values: impl IntoIterator<Item = &'a [u8]>,
) -> (Vec<*const c_void>, Vec<size_t>) {
    values
        .into_iter()
        .map(|value| {
            let pointer = if value.is_empty() {
                std::ptr::null()
            } else {
                value.as_ptr().cast()
            };
            (pointer, value.len())
        })
        .unzip()
}

fn put_results(len: usize) -> Vec<KvcsPutResult> {
    (0..len)
        .map(|_| KvcsPutResult {
            location: [0; 256],
            status: 0,
        })
        .collect()
}

fn output_buffer_parts(
    buffer: &mut [u8],
    capacities: &[size_t],
) -> Result<(Vec<*mut c_void>, Vec<c_int>, Vec<size_t>)> {
    let total_capacity = capacities.iter().try_fold(0usize, |total, capacity| {
        total.checked_add(*capacity).ok_or_else(|| {
            StoreError::InvalidState("KVCS output buffer capacity overflow".to_string())
        })
    })?;
    if total_capacity != buffer.len() {
        return Err(StoreError::InvalidState(format!(
            "KVCS output buffer length {} differs from requested capacity {total_capacity}",
            buffer.len()
        )));
    }
    let mut offset = 0usize;
    let pointers = capacities
        .iter()
        .map(|capacity| {
            let pointer = if *capacity == 0 {
                std::ptr::null_mut()
            } else {
                unsafe { buffer.as_mut_ptr().add(offset).cast() }
            };
            offset += capacity;
            pointer
        })
        .collect::<Vec<_>>();
    let count = capacities.len();
    Ok((pointers, vec![0; count], vec![0; count]))
}

fn validate_get_results(
    statuses: &[c_int],
    lengths: &[size_t],
    expected: &[size_t],
    operation: &str,
) -> Result<usize> {
    let mut found = 0;
    for (index, ((status, length), expected)) in statuses
        .iter()
        .zip(lengths)
        .zip(expected)
        .enumerate()
    {
        if *status > 0 {
            found += 1;
            if length != expected {
                return Err(StoreError::InvalidState(format!(
                    "KVCS {operation} item {index} returned {length} bytes, expected {expected}"
                )));
            }
        } else if *status < 0 {
            return Err(map_item_status(*status, operation, false));
        }
    }
    Ok(found)
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

type PreparedBatch<U, V> = (Vec<Option<Result<V>>>, Vec<(usize, U)>);

fn prepare_positional<'a, T, U, V>(
    requests: &'a [T],
    validate: impl Fn(&'a T) -> Result<U>,
) -> PreparedBatch<U, V> {
    let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
    let valid = requests
        .iter()
        .enumerate()
        .filter_map(|(index, request)| match validate(request) {
            Ok(value) => Some((index, value)),
            Err(error) => {
                results[index] = Some(Err(error));
                None
            }
        })
        .collect();
    (results, valid)
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

fn finish_unit_statuses<U>(
    mut results: Vec<Option<Result<()>>>,
    valid: &[(usize, U)],
    statuses: impl IntoIterator<Item = c_int>,
    operation: &str,
    write: bool,
) -> Vec<Result<()>> {
    for ((index, _), status) in valid.iter().zip(statuses) {
        results[*index] = Some(if status == 0 {
            Ok(())
        } else {
            Err(map_item_status(status, operation, write))
        });
    }
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

fn char_array_str(value: &[c_char]) -> std::result::Result<&str, std::str::Utf8Error> {
    let len = value
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(value.len());
    let bytes = unsafe { std::slice::from_raw_parts(value.as_ptr().cast::<u8>(), len) };
    std::str::from_utf8(bytes)
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
