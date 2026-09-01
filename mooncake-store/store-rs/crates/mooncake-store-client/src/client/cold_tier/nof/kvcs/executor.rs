use mooncake_store_core::{NamespaceScope, Result, StoreError};

use super::super::backend::encode_namespace;
use super::super::object::repeated_error;
use super::super::{
    NofBacking, NofHealth, NofObject, NofObjectDelete, NofObjectLimits, NofObjectMetadata,
    NofObjectQuery, NofObjectRead, NofObjectShardWrite, NofObjectState, NofObjectWrite,
    NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalLimits, NofPhysicalQuery,
    NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalWrite, NofStorageHealth, OpaquePhysicalKey,
};
const KVCS_MODE_ENV: &str = "MOONCAKE_KVCS_MODE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvcsMode {
    Standard,
    LowLevel,
}

impl KvcsMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "standard" => Ok(Self::Standard),
            "low-level" => Ok(Self::LowLevel),
            _ => Err(StoreError::InvalidState(format!(
                "{KVCS_MODE_ENV} must be 'standard' or 'low-level', got {value:?}"
            ))),
        }
    }

    fn from_env() -> Result<Self> {
        match std::env::var(KVCS_MODE_ENV) {
            Ok(value) => Self::parse(value.trim()),
            Err(std::env::VarError::NotPresent) => Ok(Self::LowLevel),
            Err(std::env::VarError::NotUnicode(_)) => Err(StoreError::InvalidState(format!(
                "{KVCS_MODE_ENV} is not valid UTF-8"
            ))),
        }
    }
}

enum KvcsExecutor {
    Standard(standard::StandardExecutor),
    LowLevel(low_level::LowLevelExecutor),
}

/// KVCS C API executor whose exposed NoF traits are selected by `MOONCAKE_KVCS_MODE`.
pub struct KvcsCapiExecutor {
    executor: KvcsExecutor,
}

impl KvcsCapiExecutor {
    pub fn new() -> Result<Self> {
        Self::with_mode(KvcsMode::from_env()?)
    }

    pub fn with_mode(mode: KvcsMode) -> Result<Self> {
        let executor = match mode {
            KvcsMode::Standard => KvcsExecutor::Standard(standard::StandardExecutor::new()?),
            KvcsMode::LowLevel => KvcsExecutor::LowLevel(low_level::LowLevelExecutor::new()?),
        };
        Ok(Self { executor })
    }

    pub fn mode(&self) -> KvcsMode {
        match &self.executor {
            KvcsExecutor::Standard(_) => KvcsMode::Standard,
            KvcsExecutor::LowLevel(_) => KvcsMode::LowLevel,
        }
    }
}

impl NofBacking for KvcsCapiExecutor {
    fn object_limits(&self) -> Option<NofObjectLimits> {
        match &self.executor {
            KvcsExecutor::Standard(executor) => Some(executor.capabilities()),
            KvcsExecutor::LowLevel(_) => None,
        }
    }

    fn object_write(&self) -> Option<&dyn NofObjectWrite> {
        (self.mode() == KvcsMode::Standard).then_some(self)
    }

    fn object_read(&self) -> Option<&dyn NofObjectRead> {
        (self.mode() == KvcsMode::Standard).then_some(self)
    }

    fn object_query(&self) -> Option<&dyn NofObjectQuery> {
        (self.mode() == KvcsMode::Standard).then_some(self)
    }

    fn object_delete(&self) -> Option<&dyn NofObjectDelete> {
        (self.mode() == KvcsMode::Standard).then_some(self)
    }

    fn physical_limits(&self) -> Option<NofPhysicalLimits> {
        match &self.executor {
            KvcsExecutor::Standard(_) => None,
            KvcsExecutor::LowLevel(executor) => Some(executor.capabilities()),
        }
    }

    fn physical_write(&self) -> Option<&dyn NofPhysicalWrite> {
        (self.mode() == KvcsMode::LowLevel).then_some(self)
    }

    fn physical_read(&self) -> Option<&dyn NofPhysicalRead> {
        (self.mode() == KvcsMode::LowLevel).then_some(self)
    }

    fn physical_query(&self) -> Option<&dyn NofPhysicalQuery> {
        (self.mode() == KvcsMode::LowLevel).then_some(self)
    }

    fn physical_delete(&self) -> Option<&dyn NofPhysicalDelete> {
        (self.mode() == KvcsMode::LowLevel).then_some(self)
    }

    fn health_capability(&self) -> Option<&dyn NofHealth> {
        (self.mode() == KvcsMode::LowLevel).then_some(self)
    }
}

impl NofObjectWrite for KvcsCapiExecutor {
    fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()> {
        match &self.executor {
            KvcsExecutor::Standard(executor) => executor.init_namespace(namespace),
            KvcsExecutor::LowLevel(_) => Err(wrong_mode(self.mode(), "logical-object writes")),
        }
    }

    fn put_shards(&self, requests: &[NofObjectShardWrite<'_>]) -> Vec<Result<()>> {
        match &self.executor {
            KvcsExecutor::Standard(executor) => executor.put_shards(requests),
            KvcsExecutor::LowLevel(_) => repeated_error(
                requests.len(),
                wrong_mode(self.mode(), "logical-object writes"),
            ),
        }
    }
}

impl NofObjectRead for KvcsCapiExecutor {
    fn get_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObject>> {
        match &self.executor {
            KvcsExecutor::Standard(executor) => executor.get_object(namespace, key),
            KvcsExecutor::LowLevel(_) => Err(wrong_mode(self.mode(), "logical-object reads")),
        }
    }
}

impl NofObjectQuery for KvcsCapiExecutor {
    fn query_object(
        &self,
        namespace: &NamespaceScope,
        key: &str,
    ) -> Result<NofObjectState<NofObjectMetadata>> {
        match &self.executor {
            KvcsExecutor::Standard(executor) => executor.query_object(namespace, key),
            KvcsExecutor::LowLevel(_) => Err(wrong_mode(self.mode(), "logical-object query")),
        }
    }
}

impl NofObjectDelete for KvcsCapiExecutor {
    fn delete_object(&self, namespace: &NamespaceScope, key: &str) -> Result<NofObjectState<()>> {
        match &self.executor {
            KvcsExecutor::Standard(executor) => executor.delete_object(namespace, key),
            KvcsExecutor::LowLevel(_) => Err(wrong_mode(self.mode(), "logical-object deletion")),
        }
    }
}

impl NofPhysicalWrite for KvcsCapiExecutor {
    fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
        match &self.executor {
            KvcsExecutor::Standard(_) => {
                repeated_error(requests.len(), wrong_mode(self.mode(), "physical writes"))
            }
            KvcsExecutor::LowLevel(executor) => executor.put_batch(requests),
        }
    }
}

impl NofPhysicalRead for KvcsCapiExecutor {
    fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
        match &self.executor {
            KvcsExecutor::Standard(_) => {
                repeated_error(requests.len(), wrong_mode(self.mode(), "physical reads"))
            }
            KvcsExecutor::LowLevel(executor) => executor.get_batch(requests),
        }
    }
}

impl NofPhysicalQuery for KvcsCapiExecutor {
    fn query_batch(&self, keys: &[OpaquePhysicalKey]) -> Vec<Result<bool>> {
        match &self.executor {
            KvcsExecutor::Standard(_) => {
                repeated_error(keys.len(), wrong_mode(self.mode(), "physical query"))
            }
            KvcsExecutor::LowLevel(executor) => executor.query_batch(keys),
        }
    }
}

impl NofPhysicalDelete for KvcsCapiExecutor {
    fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>> {
        match &self.executor {
            KvcsExecutor::Standard(_) => {
                repeated_error(requests.len(), wrong_mode(self.mode(), "physical deletion"))
            }
            KvcsExecutor::LowLevel(executor) => executor.delete_batch(requests),
        }
    }
}

impl NofHealth for KvcsCapiExecutor {
    fn health(&self) -> Result<NofStorageHealth> {
        let KvcsExecutor::LowLevel(executor) = &self.executor else {
            return Err(wrong_mode(self.mode(), "backing health"));
        };
        let probe = OpaquePhysicalKey::new(b"mooncake:nof:health:v1".to_vec());
        executor.query_batch(&[probe]).pop().ok_or_else(|| {
            StoreError::Transport("KVCS health query returned no result".to_string())
        })??;
        Ok(NofStorageHealth::default())
    }
}

fn wrong_mode(mode: KvcsMode, capability: &str) -> StoreError {
    StoreError::Unsupported(format!("KVCS mode {mode:?} does not expose {capability}"))
}

mod standard {
    //! KVCS Standard SDK implementation of the NoF logical-object capabilities.

    use std::ffi::{c_char, c_int, c_void, CString};
    use std::ptr;
    use std::sync::Mutex;

    use libc::size_t;
    use mooncake_store_core::{NamespaceScope, Result, StoreError};

    use super::super::*;
    use super::{
        encode_namespace, repeated_error, NofObject, NofObjectLimits, NofObjectMetadata,
        NofObjectShardWrite, NofObjectState,
    };

    struct RawQuery {
        metadata: NofObjectMetadata,
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
        let raw_shards =
            unsafe { std::slice::from_raw_parts(raw.shards, raw.shard_count as usize) };
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

    pub(super) struct StandardExecutor {
        config: KvcsClientConfig,
        _efc_socket: Option<CString>,
        _redis_endpoints: Vec<CString>,
        _redis_endpoint_ptrs: Vec<*const c_char>,
        _redis_password: Option<CString>,
        _log_path: Option<CString>,
        client: Mutex<Option<ClientHandle>>,
        limits: CommonLimits,
    }

    unsafe impl Send for StandardExecutor {}
    unsafe impl Sync for StandardExecutor {}

    impl StandardExecutor {
        pub(super) fn new() -> Result<Self> {
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
                get_workers: env_u32("MOONCAKE_KVCS_GET_WORKERS", 0)?.min(c_int::MAX as u32)
                    as c_int,
                set_workers: env_u32("MOONCAKE_KVCS_SET_WORKERS", 0)?.min(c_int::MAX as u32)
                    as c_int,
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
        ) -> Result<NofObjectState<RawQuery>> {
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
                "not_found" => Ok(NofObjectState::Missing),
                "incomplete" => Ok(NofObjectState::Incomplete),
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
                    Ok(NofObjectState::Found(RawQuery {
                        metadata: NofObjectMetadata {
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

    impl Drop for StandardExecutor {
        fn drop(&mut self) {
            if let Ok(mut slot) = self.client.lock() {
                if let Some(handle) = slot.take() {
                    unsafe { kvcs_client_destroy(handle.0) };
                }
            }
        }
    }

    impl StandardExecutor {
        pub(super) fn capabilities(&self) -> NofObjectLimits {
            NofObjectLimits {
                max_value_size: self.limits.max_value_size,
                max_key_size: self.limits.max_key_size as u64,
            }
        }

        pub(super) fn init_namespace(&self, namespace: &NamespaceScope) -> Result<()> {
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

        pub(super) fn put_shards(&self, requests: &[NofObjectShardWrite<'_>]) -> Vec<Result<()>> {
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

        pub(super) fn query_object(
            &self,
            namespace: &NamespaceScope,
            key: &str,
        ) -> Result<NofObjectState<NofObjectMetadata>> {
            Ok(self
                .query_raw(namespace, key, false)?
                .map(|query| query.metadata))
        }

        pub(super) fn get_object(
            &self,
            namespace: &NamespaceScope,
            key: &str,
        ) -> Result<NofObjectState<NofObject>> {
            let query = match self.query_raw(namespace, key, true)? {
                NofObjectState::Found(query) => query,
                NofObjectState::Missing => return Ok(NofObjectState::Missing),
                NofObjectState::Incomplete => return Ok(NofObjectState::Incomplete),
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
                StoreError::InvalidState(
                    "KVCS object length does not fit this platform".to_string(),
                )
            })?;
            let mut value = Vec::with_capacity(capacity);
            for (index, ((_, expected), status)) in query.shards.iter().zip(statuses).enumerate() {
                if status == 0 {
                    return Ok(NofObjectState::Incomplete);
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
            Ok(NofObjectState::Found(NofObject {
                metadata: query.metadata,
                value,
            }))
        }

        pub(super) fn delete_object(
            &self,
            namespace: &NamespaceScope,
            key: &str,
        ) -> Result<NofObjectState<()>> {
            let query = match self.query_raw(namespace, key, true)? {
                NofObjectState::Found(query) => query,
                NofObjectState::Missing => return Ok(NofObjectState::Missing),
                NofObjectState::Incomplete => return Ok(NofObjectState::Incomplete),
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
            Ok(NofObjectState::Found(()))
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
            let executor = std::sync::Arc::new(
                super::KvcsCapiExecutor::with_mode(super::KvcsMode::Standard).unwrap(),
            );
            let backend = crate::NofBackend::new(executor).unwrap();
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
                NofObjectState::Found(NofObject { value: got, .. }) if got == value
            ));
            assert!(matches!(
                backend.delete_object(&namespace, key).unwrap(),
                NofObjectState::Found(())
            ));
            assert!(matches!(
                backend.query_object(&namespace, key).unwrap(),
                NofObjectState::Missing
            ));
        }
    }
}

mod low_level {
    //! KVCS Low-Level SDK executor for the NoF physical KV contract.

    use std::ffi::{c_int, c_longlong, c_void, CString};
    use std::ptr;
    use std::sync::Mutex;

    use libc::size_t;
    use mooncake_store_core::{Result, StoreError};

    use super::super::*;
    use super::{
        NofPhysicalDeleteRequest, NofPhysicalLimits, NofPhysicalReadRequest, OpaquePhysicalKey,
    };

    pub(super) struct LowLevelExecutor {
        config: KvcsLlConfig,
        _efc_socket: CString,
        _log_path: Option<CString>,
        mountpoint_index: u32,
        client: Mutex<Option<ClientHandle>>,
        limits: CommonLimits,
    }

    unsafe impl Send for LowLevelExecutor {}
    unsafe impl Sync for LowLevelExecutor {}

    impl LowLevelExecutor {
        pub(super) fn new() -> Result<Self> {
            let limits = CommonLimits::from_env()?;
            let efc_socket = match optional_cstring("MOONCAKE_KVCS_EFC_SOCKET")? {
                Some(socket) => socket,
                None => CString::new("/var/run/kvcs/efc-grpc.sock").map_err(|_| {
                    StoreError::InvalidState("KVCS default EFC socket contains NUL".to_string())
                })?,
            };
            let log_path = optional_cstring("MOONCAKE_KVCS_LOG_PATH")?;
            let configured_value = env_u64("MOONCAKE_KVCS_MAX_VALUE_SIZE", 0)?;
            let configured_key = env_u32("MOONCAKE_KVCS_MAX_KEY_SIZE", 0)?;
            let configured_batch = env_u32("MOONCAKE_KVCS_MAX_KEYS_PER_BATCH", 0)?;
            let config = KvcsLlConfig {
                // Public SDK 0.4.0 rejects NULL/empty for the low-level client.
                efc_socket: efc_socket.as_ptr(),
                max_keys_per_batch: configured_batch.min(c_int::MAX as u32) as c_int,
                iov_size: 0,
                max_value_size: if configured_value == 0 {
                    0
                } else {
                    limits.max_value_size as c_longlong
                },
                max_key_size: if configured_key == 0 {
                    0
                } else {
                    limits.max_key_size.min(c_int::MAX as usize) as c_int
                },
                log_path: log_path
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                get_workers: env_u32("MOONCAKE_KVCS_GET_WORKERS", 0)?.min(c_int::MAX as u32)
                    as c_int,
                set_workers: env_u32("MOONCAKE_KVCS_SET_WORKERS", 0)?.min(c_int::MAX as u32)
                    as c_int,
                simple_workers: env_u32("MOONCAKE_KVCS_SIMPLE_WORKERS", 0)?.min(c_int::MAX as u32)
                    as c_int,
                perf_report_interval_sec: env_u32("MOONCAKE_KVCS_PERF_REPORT_INTERVAL_SEC", 0)?
                    .min(c_int::MAX as u32) as c_int,
                enable_metrics: env_u32("MOONCAKE_KVCS_ENABLE_METRICS", 0)?.min(c_int::MAX as u32)
                    as c_int,
            };
            Ok(Self {
                config,
                _efc_socket: efc_socket,
                _log_path: log_path,
                mountpoint_index: env_u32("MOONCAKE_KVCS_MOUNTPOINT_INDEX", 0)?,
                client: Mutex::new(None),
                limits,
            })
        }

        fn ensure_client(&self) -> Result<*mut KvcsClient> {
            let mut slot = self.client.lock().map_err(|_| {
                StoreError::InvalidState("KVCS low-level client lock poisoned".to_string())
            })?;
            if let Some(handle) = slot.as_ref() {
                return Ok(handle.0);
            }
            let client = unsafe { kvcs_ll_create(&self.config) };
            if client.is_null() {
                return Err(StoreError::Transport(
                    "KVCS low-level client creation failed".to_string(),
                ));
            }
            slot.replace(ClientHandle(client));
            Ok(client)
        }

        fn options(&self) -> Option<KvcsLlBatchOptions> {
            (self.mountpoint_index != 0).then_some(KvcsLlBatchOptions {
                mountpoint_index: self.mountpoint_index,
            })
        }

        fn call_get_into(
            &self,
            client: *mut KvcsClient,
            items: &[KvcsLlGetItem],
            buffers: &mut [Vec<u8>],
        ) -> Result<(Vec<c_int>, Vec<size_t>)> {
            let count = checked_batch_len(items.len())?;
            let mut pointers = buffers
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
            let options = self.options();
            let options_ptr = options.as_ref().map_or(ptr::null(), |options| options);
            let status = unsafe {
                kvcs_ll_batch_get_into(
                    client,
                    items.as_ptr(),
                    count,
                    pointers.as_mut_ptr(),
                    capacities.as_ptr(),
                    statuses.as_mut_ptr(),
                    lengths.as_mut_ptr(),
                    0,
                    options_ptr,
                )
            };
            if status < 0 {
                Err(map_call_status(status, "low-level get"))
            } else {
                Ok((statuses, lengths))
            }
        }
    }

    impl Drop for LowLevelExecutor {
        fn drop(&mut self) {
            if let Ok(mut slot) = self.client.lock() {
                if let Some(handle) = slot.take() {
                    unsafe { kvcs_ll_client_destroy(handle.0) };
                }
            }
        }
    }

    impl LowLevelExecutor {
        pub(super) fn capabilities(&self) -> NofPhysicalLimits {
            NofPhysicalLimits {
                max_value_size: self.limits.max_value_size,
                max_batch_items: self.limits.max_batch_items,
                // KVCS documents max_value_size per value and splits large batches internally.
                max_batch_bytes: u64::MAX,
            }
        }

        pub(super) fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
            if requests.is_empty() {
                return Vec::new();
            }
            let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
            let valid = requests
                .iter()
                .enumerate()
                .filter_map(|(index, (key, value))| {
                    let validation =
                        encode_physical_key(key, self.limits.max_key_size).and_then(|key| {
                            if value.len() as u64 > self.limits.max_value_size {
                                Err(StoreError::InvalidState(format!(
                                    "KVCS low-level value size {} exceeds provider limit {}",
                                    value.len(),
                                    self.limits.max_value_size
                                )))
                            } else {
                                Ok(key)
                            }
                        });
                    match validation {
                        Ok(key) => Some((index, key, *value)),
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
            let pointers = valid
                .iter()
                .map(|(_, _, value)| {
                    if value.is_empty() {
                        ptr::null()
                    } else {
                        value.as_ptr().cast()
                    }
                })
                .collect::<Vec<*const c_void>>();
            let lengths = valid
                .iter()
                .map(|(_, _, value)| value.len())
                .collect::<Vec<size_t>>();
            let items = valid
                .iter()
                .enumerate()
                .map(|(offset, (_, key, _))| KvcsLlPutItem {
                    key: key.as_ptr(),
                    value_segs: &pointers[offset],
                    seg_lens: &lengths[offset],
                    seg_count: 1,
                })
                .collect::<Vec<_>>();
            let mut native_results = (0..valid.len())
                .map(|_| KvcsPutResult {
                    location: [0; 256],
                    status: 0,
                })
                .collect::<Vec<_>>();
            let options = self.options();
            let options_ptr = options.as_ref().map_or(ptr::null(), |options| options);
            let status = unsafe {
                kvcs_ll_batch_put(
                    client,
                    items.as_ptr(),
                    count,
                    native_results.as_mut_ptr(),
                    count,
                    0,
                    options_ptr,
                )
            };
            if status < 0 {
                let error = map_call_status(status, "low-level put");
                fill_missing(&mut results, &error);
                return finish_positional_results(results);
            }
            for ((index, _, _), native) in valid.iter().zip(native_results) {
                results[*index] = Some(if native.status == 0 {
                    Ok(())
                } else {
                    Err(map_item_status(native.status, "low-level put item", true))
                });
            }
            finish_positional_results(results)
        }

        pub(super) fn get_batch(
            &self,
            requests: &[NofPhysicalReadRequest],
        ) -> Vec<Result<Option<Vec<u8>>>> {
            if requests.is_empty() {
                return Vec::new();
            }
            let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
            let valid = requests
                .iter()
                .enumerate()
                .filter_map(|(index, request)| {
                    let validation = encode_physical_key(&request.key, self.limits.max_key_size)
                        .and_then(|key| {
                            let hint =
                                u64::try_from(request.expected_value_size).map_err(|_| {
                                    StoreError::InvalidState(
                                        "KVCS low-level read size does not fit u64".to_string(),
                                    )
                                })?;
                            if hint > self.limits.max_value_size {
                                return Err(StoreError::InvalidState(format!(
                                    "KVCS low-level read size {hint} exceeds provider limit {}",
                                    self.limits.max_value_size
                                )));
                            }
                            Ok(key)
                        });
                    match validation {
                        Ok(key) => Some((index, key, request.expected_value_size)),
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
            let client = match self.ensure_client() {
                Ok(client) => client,
                Err(error) => {
                    fill_missing(&mut results, &error);
                    return finish_positional_results(results);
                }
            };
            let items = valid
                .iter()
                .map(|(_, key, _)| KvcsLlGetItem { key: key.as_ptr() })
                .collect::<Vec<_>>();
            let mut buffers = valid
                .iter()
                .map(|(_, _, hint)| vec![0; *hint])
                .collect::<Vec<_>>();
            let (mut statuses, mut lengths) = match self.call_get_into(client, &items, &mut buffers)
            {
                Ok(output) => output,
                Err(error) => {
                    fill_missing(&mut results, &error);
                    return finish_positional_results(results);
                }
            };
            for index in 0..valid.len() {
                if statuses[index] == -(libc::ENOSPC as c_int)
                    && lengths[index] > buffers[index].len()
                {
                    let actual_len = match u64::try_from(lengths[index]) {
                        Ok(actual_len) if actual_len <= self.limits.max_value_size => actual_len,
                        Ok(actual_len) => {
                            results[valid[index].0] = Some(Err(StoreError::InvalidState(format!(
                                "KVCS low-level value size {actual_len} exceeds provider limit {}",
                                self.limits.max_value_size
                            ))));
                            continue;
                        }
                        Err(_) => {
                            results[valid[index].0] = Some(Err(StoreError::InvalidState(
                                "KVCS low-level value length does not fit u64".to_string(),
                            )));
                            continue;
                        }
                    };
                    debug_assert_eq!(actual_len as usize, lengths[index]);
                    buffers[index].resize(lengths[index], 0);
                    match self.call_get_into(
                        client,
                        &items[index..=index],
                        std::slice::from_mut(&mut buffers[index]),
                    ) {
                        Ok((status, length)) => {
                            statuses[index] = status[0];
                            lengths[index] = length[0];
                        }
                        Err(error) => {
                            results[valid[index].0] = Some(Err(error));
                        }
                    }
                }
            }
            for (offset, ((request_index, _, _), status)) in valid.iter().zip(statuses).enumerate()
            {
                if results[*request_index].is_some() {
                    continue;
                }
                results[*request_index] = Some(if status > 0 {
                    if lengths[offset] > buffers[offset].len() {
                        Err(StoreError::Backpressure(
                            "KVCS low-level value exceeds destination buffer".to_string(),
                        ))
                    } else {
                        buffers[offset].truncate(lengths[offset]);
                        Ok(Some(std::mem::take(&mut buffers[offset])))
                    }
                } else if status == 0 {
                    Ok(None)
                } else {
                    Err(map_item_status(status, "low-level get item", false))
                });
            }
            finish_positional_results(results)
        }

        pub(super) fn delete_batch(
            &self,
            requests: &[NofPhysicalDeleteRequest],
        ) -> Vec<Result<()>> {
            if requests.is_empty() {
                return Vec::new();
            }
            let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
            let valid = requests
                .iter()
                .enumerate()
                .filter_map(|(index, request)| {
                    let validation = encode_physical_key(&request.key, self.limits.max_key_size);
                    match validation {
                        Ok(key) => Some((index, key)),
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
            let pointers = valid
                .iter()
                .map(|(_, key)| key.as_ptr())
                .collect::<Vec<_>>();
            let mut statuses = vec![0; valid.len()];
            let options = self.options();
            let options_ptr = options.as_ref().map_or(ptr::null(), |options| options);
            let status = unsafe {
                kvcs_ll_batch_delete(
                    client,
                    pointers.as_ptr(),
                    count,
                    statuses.as_mut_ptr(),
                    count,
                    0,
                    options_ptr,
                )
            };
            if status < 0 {
                let error = map_call_status(status, "low-level delete");
                fill_missing(&mut results, &error);
                return finish_positional_results(results);
            }
            for ((request_index, _), status) in valid.iter().zip(statuses) {
                results[*request_index] = Some(if status == 0 {
                    Ok(())
                } else {
                    Err(map_item_status(status, "low-level delete item", true))
                });
            }
            finish_positional_results(results)
        }

        pub(super) fn query_batch(&self, keys: &[OpaquePhysicalKey]) -> Vec<Result<bool>> {
            if keys.is_empty() {
                return Vec::new();
            }
            let mut results = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
            let valid = keys
                .iter()
                .enumerate()
                .filter_map(|(index, key)| {
                    match encode_physical_key(key, self.limits.max_key_size) {
                        Ok(key) => Some((index, key)),
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
            let pointers = valid
                .iter()
                .map(|(_, key)| key.as_ptr())
                .collect::<Vec<_>>();
            let mut native_results = (0..valid.len())
                .map(|_| KvcsQueryResult {
                    key: [0; 256],
                    status: [0; 32],
                    total_shard: 0,
                    total_size: 0,
                    meta: ptr::null_mut(),
                    meta_count: 0,
                    shards: ptr::null_mut(),
                    shard_count: 0,
                })
                .collect::<Vec<_>>();
            let options = self.options();
            let options_ptr = options.as_ref().map_or(ptr::null(), |options| options);
            let written = unsafe {
                kvcs_ll_batch_query(
                    client,
                    pointers.as_ptr(),
                    count,
                    native_results.as_mut_ptr(),
                    count,
                    options_ptr,
                )
            };
            if written < 0 {
                let error = map_call_status(written, "low-level query");
                fill_missing(&mut results, &error);
                return finish_positional_results(results);
            }
            if written != count {
                let error = StoreError::Transport(format!(
                    "KVCS low-level query returned {written} results for {count} keys"
                ));
                fill_missing(&mut results, &error);
            } else {
                for ((request_index, _), native) in valid.iter().zip(&native_results) {
                    results[*request_index] = Some(kvcs_low_level_query_status(&native.status));
                }
            }
            unsafe { kvcs_query_result_free(native_results.as_mut_ptr(), count) };
            finish_positional_results(results)
        }
    }

    fn kvcs_low_level_query_status(status: &[std::ffi::c_char; 32]) -> Result<bool> {
        let len = status
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(status.len());
        let bytes = unsafe { std::slice::from_raw_parts(status.as_ptr().cast::<u8>(), len) };
        match std::str::from_utf8(bytes) {
            Ok("ok") => Ok(true),
            Ok("not_found") => Ok(false),
            Ok("incomplete") => Err(StoreError::Backpressure(
                "KVCS low-level query returned incomplete".to_string(),
            )),
            Ok("unavailable") => Err(StoreError::Transport(
                "KVCS low-level mountpoint is unavailable".to_string(),
            )),
            Ok(other) => Err(StoreError::Transport(format!(
                "KVCS low-level query returned status {other}"
            ))),
            Err(error) => Err(StoreError::Transport(format!(
                "KVCS low-level query returned invalid status: {error}"
            ))),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::{
            NofBacking, NofHealth, NofPhysicalDelete, NofPhysicalQuery, NofPhysicalRead,
            NofPhysicalWrite, NofStorageHealth,
        };
        use super::*;

        #[test]
        #[ignore = "requires a live KVCS EFC and configured mountpoint"]
        fn live_low_level_round_trip_smoke() {
            let executor = super::KvcsCapiExecutor::with_mode(super::KvcsMode::LowLevel).unwrap();
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let key = OpaquePhysicalKey::new(format!("mooncake-smoke-{nonce}").into_bytes());
            let value = b"kvcs-capi-smoke";
            executor
                .put_batch(&[(key.clone(), value)])
                .remove(0)
                .unwrap();
            assert!(executor
                .query_batch(std::slice::from_ref(&key))
                .remove(0)
                .unwrap());
            assert_eq!(
                executor
                    .get_batch(&[NofPhysicalReadRequest {
                        key: key.clone(),
                        expected_value_size: value.len(),
                    }])
                    .remove(0)
                    .unwrap(),
                Some(value.to_vec())
            );
            executor
                .delete_batch(&[NofPhysicalDeleteRequest { key: key.clone() }])
                .remove(0)
                .unwrap();
            assert!(!executor.query_batch(&[key]).remove(0).unwrap());
            assert!(executor.physical_read().is_some());
            assert!(executor.physical_write().is_some());
            assert!(executor.physical_query().is_some());
            assert!(executor.health_capability().is_some());
            assert_eq!(
                NofHealth::health(&executor).unwrap(),
                NofStorageHealth::default()
            );
            assert!(executor.object_read().is_none());
            assert!(executor.metadata().is_none());
            assert!(executor.storage_management().is_none());
            assert!(executor.device_management().is_none());
        }
    }
}

#[cfg(test)]
mod mode_tests {
    use super::*;

    #[test]
    fn mode_parser_accepts_only_documented_values() {
        assert_eq!(KvcsMode::parse("standard").unwrap(), KvcsMode::Standard);
        assert_eq!(KvcsMode::parse("low-level").unwrap(), KvcsMode::LowLevel);
        assert!(KvcsMode::parse("lowlevel").is_err());
    }

    #[test]
    fn selected_mode_exposes_only_its_data_plane_traits() {
        let standard = KvcsCapiExecutor::with_mode(KvcsMode::Standard).unwrap();
        assert!(standard.object_read().is_some());
        assert!(standard.object_write().is_some());
        assert!(standard.object_query().is_some());
        assert!(standard.physical_read().is_none());
        assert!(standard.health_capability().is_none());

        let low_level = KvcsCapiExecutor::with_mode(KvcsMode::LowLevel).unwrap();
        assert!(low_level.object_read().is_none());
        assert!(low_level.physical_read().is_some());
        assert!(low_level.physical_write().is_some());
        assert!(low_level.physical_query().is_some());
        assert!(low_level.health_capability().is_some());
        assert!(low_level.metadata().is_none());
        assert!(low_level.storage_management().is_none());
        assert!(low_level.device_management().is_none());
    }
}
