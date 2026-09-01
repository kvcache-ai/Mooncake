//! KVCS Standard SDK executor for the NoF high-level object contract.

use std::ffi::{c_char, c_int, c_void, CString};
use std::ptr;
use std::sync::Mutex;

use libc::size_t;
use mooncake_store_core::{NamespaceScope, Result, StoreError};

use super::super::*;
use super::{
    encode_namespace, repeated_error, NofHighLevelCapabilities, NofHighLevelExecutor,
    NofHighLevelObject, NofHighLevelObjectMetadata, NofHighLevelRead, NofHighLevelShardPut,
};

struct RawQuery {
    metadata: NofHighLevelObjectMetadata,
    shards: Vec<(u32, usize)>,
}

struct QueryResultGuard {
    raw: KvcsQueryResult,
}

impl Drop for QueryResultGuard {
    fn drop(&mut self) {
        unsafe { kvcs_query_result_free(&mut self.raw, 1) };
    }
}

fn query_shards(
    raw: &KvcsQueryResult,
    with_shards: bool,
    length: u64,
    total_shards: u32,
    max_value_size: u64,
) -> Result<Vec<(u32, usize)>> {
    if !with_shards {
        return Ok(Vec::new());
    }
    if total_shards == 1 {
        if length > max_value_size {
            return Err(StoreError::InvalidState(format!(
                "KVCS unsharded object size {length} exceeds provider limit {max_value_size}"
            )));
        }
        return Ok(vec![(
            0,
            usize::try_from(length).map_err(|_| {
                StoreError::InvalidState(
                    "KVCS object length does not fit this platform".to_string(),
                )
            })?,
        )]);
    }
    if raw.shard_count != raw.total_shard || raw.shards.is_null() {
        return Err(StoreError::InvalidState(
            "KVCS complete manifest omitted shard details".to_string(),
        ));
    }
    let raw_shards = unsafe { std::slice::from_raw_parts(raw.shards, raw.shard_count as usize) };
    let mut shard_bytes = 0u64;
    let mut shards = Vec::with_capacity(raw_shards.len());
    for shard in raw_shards {
        let shard_id = u32::try_from(shard.shard_id).map_err(|_| {
            StoreError::InvalidState("KVCS query returned a negative shard ID".to_string())
        })?;
        let size = usize::try_from(shard.size).map_err(|_| {
            StoreError::InvalidState("KVCS query returned an invalid shard size".to_string())
        })?;
        if size == 0 {
            return Err(StoreError::InvalidState(
                "KVCS complete query returned an empty shard".to_string(),
            ));
        }
        if size as u64 > max_value_size {
            return Err(StoreError::InvalidState(format!(
                "KVCS shard size {size} exceeds provider limit {max_value_size}"
            )));
        }
        shard_bytes = shard_bytes.checked_add(size as u64).ok_or_else(|| {
            StoreError::InvalidState("KVCS query shard byte count overflow".to_string())
        })?;
        shards.push((shard_id, size));
    }
    if shard_bytes != length {
        return Err(StoreError::InvalidState(
            "KVCS query shard lengths do not match total size".to_string(),
        ));
    }
    shards.sort_by_key(|(shard_id, _)| *shard_id);
    if shards
        .iter()
        .enumerate()
        .any(|(index, (shard_id, _))| *shard_id as usize != index)
    {
        return Err(StoreError::InvalidState(
            "KVCS query returned non-contiguous shard IDs".to_string(),
        ));
    }
    Ok(shards)
}

pub struct KvcsCapiStandardExecutor {
    config: KvcsClientConfig,
    _efc_socket: Option<CString>,
    _redis_endpoints: Vec<CString>,
    _redis_endpoint_ptrs: Vec<*const c_char>,
    _redis_password: Option<CString>,
    _log_path: Option<CString>,
    client: Mutex<Option<ClientHandle>>,
    limits: CommonLimits,
}

unsafe impl Send for KvcsCapiStandardExecutor {}
unsafe impl Sync for KvcsCapiStandardExecutor {}

impl KvcsCapiStandardExecutor {
    pub fn new() -> Result<Self> {
        let limits = CommonLimits::from_env()?;
        let efc_socket = optional_cstring("MOONCAKE_KVCS_EFC_SOCKET")?;
        let redis_password = optional_cstring("MOONCAKE_KVCS_REDIS_PASSWORD")?;
        let log_path = optional_cstring("MOONCAKE_KVCS_LOG_PATH")?;
        let redis_endpoints = std::env::var("MOONCAKE_KVCS_REDIS_ENDPOINTS")
            .ok()
            .into_iter()
            .flat_map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|endpoint| !endpoint.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .map(|endpoint| {
                CString::new(endpoint).map_err(|_| {
                    StoreError::InvalidState("KVCS Redis endpoint contains NUL".to_string())
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let redis_endpoint_ptrs = redis_endpoints
            .iter()
            .map(|endpoint| endpoint.as_ptr())
            .collect::<Vec<_>>();
        let configured_value = env_u64("MOONCAKE_KVCS_MAX_VALUE_SIZE", 0)?;
        let configured_key = env_u32("MOONCAKE_KVCS_MAX_KEY_SIZE", 0)?;
        let configured_batch = env_u32("MOONCAKE_KVCS_MAX_KEYS_PER_BATCH", 0)?;
        let config = KvcsClientConfig {
            efc_socket: efc_socket
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            redis_endpoints: if redis_endpoint_ptrs.is_empty() {
                ptr::null()
            } else {
                redis_endpoint_ptrs.as_ptr()
            },
            redis_count: checked_batch_len(redis_endpoint_ptrs.len())?,
            redis_password: redis_password
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            redis_pool_size: env_u32("MOONCAKE_KVCS_REDIS_POOL_SIZE", 0)?.min(c_int::MAX as u32)
                as c_int,
            get_workers: env_u32("MOONCAKE_KVCS_GET_WORKERS", 0)?.min(c_int::MAX as u32) as c_int,
            set_workers: env_u32("MOONCAKE_KVCS_SET_WORKERS", 0)?.min(c_int::MAX as u32) as c_int,
            simple_workers: env_u32("MOONCAKE_KVCS_SIMPLE_WORKERS", 0)?.min(c_int::MAX as u32)
                as c_int,
            max_keys_per_batch: configured_batch.min(c_int::MAX as u32) as c_int,
            iov_size: 0,
            max_value_size: if configured_value == 0 {
                0
            } else {
                limits.max_value_size as i64
            },
            max_key_size: if configured_key == 0 {
                0
            } else {
                limits.max_key_size.min(c_int::MAX as usize) as c_int
            },
            perf_report_interval_sec: env_u32("MOONCAKE_KVCS_PERF_REPORT_INTERVAL_SEC", 0)?
                .min(c_int::MAX as u32) as c_int,
            log_path: log_path
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            enable_metrics: env_u32("MOONCAKE_KVCS_ENABLE_METRICS", 0)?.min(c_int::MAX as u32)
                as c_int,
        };
        Ok(Self {
            config,
            _efc_socket: efc_socket,
            _redis_endpoints: redis_endpoints,
            _redis_endpoint_ptrs: redis_endpoint_ptrs,
            _redis_password: redis_password,
            _log_path: log_path,
            client: Mutex::new(None),
            limits,
        })
    }

    fn ensure_client(&self) -> Result<*mut KvcsClient> {
        let mut slot = self.client.lock().map_err(|_| {
            StoreError::InvalidState("KVCS Standard client lock poisoned".to_string())
        })?;
        if let Some(handle) = slot.as_ref() {
            return Ok(handle.0);
        }
        let client = unsafe { kvcs_client_create(&self.config) };
        if client.is_null() {
            return Err(StoreError::Transport(
                "KVCS Standard client creation failed".to_string(),
            ));
        }
        slot.replace(ClientHandle(client));
        Ok(client)
    }

    fn namespace(&self, scope: &NamespaceScope) -> Result<CString> {
        let encoded = encode_namespace(scope)?;
        validate_string(&encoded, KVCS_NAMESPACE_MAX_BYTES, "namespace")
    }

    fn key(&self, key: &str) -> Result<CString> {
        validate_string(key, self.limits.max_key_size, "object key")
    }

    fn query_raw(
        &self,
        namespace: &NamespaceScope,
        key: &str,
        with_shards: bool,
    ) -> Result<NofHighLevelRead<RawQuery>> {
        let client = self.ensure_client()?;
        let namespace = self.namespace(namespace)?;
        let key = self.key(key)?;
        let key_ptr = key.as_ptr();
        let options = KvcsQueryOptions {
            renew: 0,
            with_shards: c_int::from(with_shards),
        };
        let mut raw: KvcsQueryResult = unsafe { std::mem::zeroed() };
        let count = unsafe {
            kvcs_batch_query(
                client,
                namespace.as_ptr(),
                &key_ptr,
                1,
                &options,
                &mut raw,
                1,
            )
        };
        if count < 0 {
            return Err(map_call_status(count, "Standard query"));
        }
        if count != 1 {
            return Err(StoreError::Transport(format!(
                "KVCS Standard query returned {count} results for one request"
            )));
        }
        let raw = QueryResultGuard { raw };
        let status = char_array_string(&raw.raw.status);
        let parsed = match status.as_str() {
            "not_found" => Ok(NofHighLevelRead::Missing),
            "incomplete" => Ok(NofHighLevelRead::Incomplete),
            "ok" => {
                let length = u64::try_from(raw.raw.total_size).map_err(|_| {
                    StoreError::InvalidState("KVCS query returned a negative size".to_string())
                })?;
                if raw.raw.total_shard <= 0 {
                    return Err(StoreError::InvalidState(
                        "KVCS complete query returned a non-positive shard count".to_string(),
                    ));
                }
                if length == 0 {
                    return Err(StoreError::InvalidState(
                        "KVCS complete query returned an empty object".to_string(),
                    ));
                }
                let total_shards = u32::try_from(raw.raw.total_shard).map_err(|_| {
                    StoreError::InvalidState("KVCS query shard count exceeds u32".to_string())
                })?;
                let shards = query_shards(
                    &raw.raw,
                    with_shards,
                    length,
                    total_shards,
                    self.limits.max_value_size,
                )?;
                Ok(NofHighLevelRead::Found(RawQuery {
                    metadata: NofHighLevelObjectMetadata {
                        length,
                        total_shards,
                    },
                    shards,
                }))
            }
            _ => Err(StoreError::Transport(format!(
                "KVCS Standard query returned status {status:?}"
            ))),
        };
        parsed
    }
}

impl Drop for KvcsCapiStandardExecutor {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.client.lock() {
            if let Some(handle) = slot.take() {
                unsafe { kvcs_client_destroy(handle.0) };
            }
        }
    }
}

impl NofHighLevelExecutor for KvcsCapiStandardExecutor {
    fn capabilities(&self) -> NofHighLevelCapabilities {
        NofHighLevelCapabilities {
            max_value_size: self.limits.max_value_size,
            max_key_size: self.limits.max_key_size as u64,
        }
    }

    fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()> {
        let client = self.ensure_client()?;
        let namespace = self.namespace(namespace)?;
        let options = KvcsCreateNamespaceOptions {
            space_limit: 0,
            eviction_policy: ptr::null(),
            gc_threshold: 0.0,
        };
        let status = unsafe { kvcs_create_namespace(client, namespace.as_ptr(), &options) };
        if status == 0 || status == -8 {
            Ok(())
        } else {
            Err(map_call_status(status, "create namespace"))
        }
    }

    fn put_shards(&self, requests: &[NofHighLevelShardPut<'_>]) -> Vec<Result<()>> {
        if requests.is_empty() {
            return Vec::new();
        }
        let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
        let namespace = match self.namespace(requests[0].namespace) {
            Ok(namespace) => namespace,
            Err(error) => return repeated_error(requests.len(), error),
        };
        if requests
            .iter()
            .any(|request| request.namespace != requests[0].namespace)
        {
            return repeated_error(
                requests.len(),
                StoreError::InvalidState(
                    "KVCS Standard native batch must use one namespace".to_string(),
                ),
            );
        }
        let valid = requests
            .iter()
            .enumerate()
            .filter_map(|(index, request)| {
                let validation = self.key(request.key).and_then(|key| {
                    if request.value.len() as u64 > self.limits.max_value_size {
                        return Err(StoreError::InvalidState(format!(
                            "KVCS Standard shard size {} exceeds provider limit {}",
                            request.value.len(),
                            self.limits.max_value_size
                        )));
                    }
                    if request.total_shards == 0 || request.shard_id >= request.total_shards {
                        return Err(StoreError::InvalidState(
                            "KVCS Standard shard ID/count is invalid".to_string(),
                        ));
                    }
                    if request.total_shards > c_int::MAX as u32 {
                        return Err(StoreError::InvalidState(
                            "KVCS Standard shard count exceeds the C ABI limit".to_string(),
                        ));
                    }
                    Ok(key)
                });
                match validation {
                    Ok(key) => Some((index, key, request)),
                    Err(error) => {
                        results[index] = Some(Err(error));
                        None
                    }
                }
            })
            .collect::<Vec<_>>();
        if valid.is_empty() {
            return finish_positional_results(results);
        }
        let count = match checked_batch_len(valid.len()) {
            Ok(count) => count,
            Err(error) => {
                fill_missing(&mut results, &error);
                return finish_positional_results(results);
            }
        };
        let client = match self.ensure_client() {
            Ok(client) => client,
            Err(error) => {
                fill_missing(&mut results, &error);
                return finish_positional_results(results);
            }
        };
        let value_ptrs = valid
            .iter()
            .map(|(_, _, request)| {
                if request.value.is_empty() {
                    ptr::null()
                } else {
                    request.value.as_ptr().cast()
                }
            })
            .collect::<Vec<*const c_void>>();
        let value_lens = valid
            .iter()
            .map(|(_, _, request)| request.value.len())
            .collect::<Vec<size_t>>();
        let items = valid
            .iter()
            .enumerate()
            .map(|(offset, (_, key, request))| KvcsPutItem {
                key: key.as_ptr(),
                value_segs: &value_ptrs[offset],
                seg_lens: &value_lens[offset],
                seg_count: 1,
                shard_id: request.shard_id as i32,
                total_shard: request.total_shards as i32,
                location: ptr::null(),
                meta: ptr::null(),
                meta_count: 0,
            })
            .collect::<Vec<_>>();
        let mut native_results = (0..valid.len())
            .map(|_| KvcsPutResult {
                location: [0; 256],
                status: 0,
            })
            .collect::<Vec<_>>();
        let status = unsafe {
            kvcs_batch_put(
                client,
                namespace.as_ptr(),
                items.as_ptr(),
                count,
                native_results.as_mut_ptr(),
                count,
                0,
            )
        };
        if status < 0 {
            let error = map_call_status(status, "Standard put");
            fill_missing(&mut results, &error);
            return finish_positional_results(results);
        }
        for ((index, _, _), native) in valid.iter().zip(native_results) {
            results[*index] = Some(if native.status == 0 {
                Ok(())
            } else {
                Err(map_item_status(native.status, "Standard put item", true))
            });
        }
        finish_positional_results(results)
    }

    fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObjectMetadata>> {
        Ok(self
            .query_raw(namespace, key, false)?
            .map(|query| query.metadata))
    }

    fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObject>> {
        let query = match self.query_raw(namespace, key, true)? {
            NofHighLevelRead::Found(query) => query,
            NofHighLevelRead::Missing => return Ok(NofHighLevelRead::Missing),
            NofHighLevelRead::Incomplete => return Ok(NofHighLevelRead::Incomplete),
        };
        let client = self.ensure_client()?;
        let namespace = self.namespace(namespace)?;
        let key = self.key(key)?;
        let items = query
            .shards
            .iter()
            .map(|(shard_id, size)| KvcsGetItem {
                key: key.as_ptr(),
                shard_id: *shard_id as i32,
                location: ptr::null(),
                expected_value_size: *size,
                meta: ptr::null(),
                meta_count: 0,
            })
            .collect::<Vec<_>>();
        let mut buffers = query
            .shards
            .iter()
            .map(|(_, size)| vec![0; *size])
            .collect::<Vec<_>>();
        let mut buffer_ptrs = buffers
            .iter_mut()
            .map(|buffer| {
                if buffer.is_empty() {
                    ptr::null_mut()
                } else {
                    buffer.as_mut_ptr().cast()
                }
            })
            .collect::<Vec<*mut c_void>>();
        let capacities = buffers.iter().map(Vec::len).collect::<Vec<size_t>>();
        let mut statuses = vec![0; items.len()];
        let mut lengths = vec![0; items.len()];
        let count = checked_batch_len(items.len())?;
        let status = unsafe {
            kvcs_batch_get_into(
                client,
                namespace.as_ptr(),
                items.as_ptr(),
                count,
                buffer_ptrs.as_mut_ptr(),
                capacities.as_ptr(),
                statuses.as_mut_ptr(),
                lengths.as_mut_ptr(),
                0,
            )
        };
        if status < 0 {
            return Err(map_call_status(status, "Standard get"));
        }
        let capacity = usize::try_from(query.metadata.length).map_err(|_| {
            StoreError::InvalidState("KVCS object length does not fit this platform".to_string())
        })?;
        let mut value = Vec::with_capacity(capacity);
        for (index, ((_, expected), status)) in query.shards.iter().zip(statuses).enumerate() {
            if status == 0 {
                return Ok(NofHighLevelRead::Incomplete);
            }
            if status < 0 {
                return Err(map_item_status(status, "Standard get item", false));
            }
            if lengths[index] != *expected {
                return Err(StoreError::InvalidState(format!(
                    "KVCS shard {index} length {} differs from manifest length {expected}",
                    lengths[index]
                )));
            }
            buffers[index].truncate(lengths[index]);
            value.extend_from_slice(&buffers[index]);
        }
        if value.len() != capacity {
            return Err(StoreError::InvalidState(
                "KVCS Standard object length differs from complete manifest".to_string(),
            ));
        }
        Ok(NofHighLevelRead::Found(NofHighLevelObject {
            metadata: query.metadata,
            value,
        }))
    }

    fn delete_object(&self, namespace: &NamespaceScope, key: &str) -> Result<NofHighLevelRead<()>> {
        let query = match self.query_raw(namespace, key, true)? {
            NofHighLevelRead::Found(query) => query,
            NofHighLevelRead::Missing => return Ok(NofHighLevelRead::Missing),
            NofHighLevelRead::Incomplete => return Ok(NofHighLevelRead::Incomplete),
        };
        let client = self.ensure_client()?;
        let namespace = self.namespace(namespace)?;
        let key = self.key(key)?;
        let items = query
            .shards
            .iter()
            .map(|(shard_id, _)| KvcsDeleteItem {
                key: key.as_ptr(),
                shard_id: *shard_id as i32,
                location: ptr::null(),
            })
            .collect::<Vec<_>>();
        let mut results = (0..items.len())
            .map(|_| KvcsDeleteResult {
                location: [0; 256],
                status: 0,
            })
            .collect::<Vec<_>>();
        let count = checked_batch_len(items.len())?;
        let status = unsafe {
            kvcs_batch_delete_items(
                client,
                namespace.as_ptr(),
                items.as_ptr(),
                count,
                results.as_mut_ptr(),
                count,
                0,
            )
        };
        if status < 0 {
            return Err(map_call_status(status, "Standard delete"));
        }
        for result in results {
            if result.status != 0 {
                return Err(map_item_status(result.status, "Standard delete item", true));
            }
        }
        Ok(NofHighLevelRead::Found(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_only_query_accepts_sharded_total_above_single_value_limit() {
        let mut raw: KvcsQueryResult = unsafe { std::mem::zeroed() };
        raw.total_shard = 2;
        assert!(query_shards(&raw, false, 9, 2, 8).unwrap().is_empty());
    }

    #[test]
    #[ignore = "requires a live KVCS EFC and Redis, or the official SDK mock"]
    fn live_standard_round_trip_smoke() {
        let executor = std::sync::Arc::new(KvcsCapiStandardExecutor::new().unwrap());
        let backend = crate::NofHighLevelBackend::new(executor).unwrap();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let namespace = NamespaceScope::new(format!("smoke-{nonce}"), "nof", "standard");
        let key = "round-trip";
        let value = b"kvcs-capi-standard-smoke";

        backend.init_namespace(&namespace).unwrap();
        let metadata = backend.put_object(&namespace, key, value).unwrap();
        assert_eq!(metadata.length, value.len() as u64);
        assert_eq!(metadata.total_shards, 1);
        assert!(matches!(
            backend.get_object(&namespace, key).unwrap(),
            NofHighLevelRead::Found(NofHighLevelObject { value: got, .. }) if got == value
        ));
        assert!(matches!(
            backend.delete_object(&namespace, key).unwrap(),
            NofHighLevelRead::Found(())
        ));
        assert!(matches!(
            backend.query_object(&namespace, key).unwrap(),
            NofHighLevelRead::Missing
        ));
    }
}
