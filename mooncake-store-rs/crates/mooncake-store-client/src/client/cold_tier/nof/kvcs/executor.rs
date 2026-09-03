use std::ffi::{c_int, CString};
use std::ptr;
use std::sync::Arc;

use mooncake_store_core::{Result, StoreError};

use super::super::backend::encode_namespace;
use super::super::object::repeated_error;
use super::super::{
    NofBacking, NofHealth, NofObjectDelete, NofObjectLimits, NofObjectQuery, NofObjectRead,
    NofObjectShardWrite, NofObjectState, NofObjectWrite, NofPhysicalDelete,
    NofPhysicalDeleteRequest, NofPhysicalQuery, NofPhysicalQueryRequest, NofPhysicalRead,
    NofPhysicalReadRequest, NofPhysicalWrite, NofPhysicalWriteRequest, NofStorageHealth,
    OpaquePhysicalKey,
};
use super::*;
const KVCS_MODE_ENV: &str = "MOONCAKE_KVCS_MODE";
const KVCS_MAX_VALUE_SIZE_ENV: &str = "MOONCAKE_KVCS_MAX_VALUE_SIZE";

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
        optional_env(KVCS_MODE_ENV)?
            .map(|value| Self::parse(value.trim()))
            .unwrap_or(Ok(Self::LowLevel))
    }
}

enum KvcsConfig {
    Standard {
        efc_socket: Option<CString>,
        redis_endpoints: Vec<CString>,
        redis_password: Option<CString>,
        client: ClientSlot,
    },
    LowLevel {
        client: KvcsLowLevelClient,
        mountpoint_index: u32,
    },
}

/// One shareable KVCS Low-Level SDK client.
///
/// The SDK selects a filesystem with the per-batch `mountpoint_index`, so callers should create
/// one client per EFC socket and bind all statically configured targets through [`Self::executor`].
#[derive(Clone)]
pub struct KvcsLowLevelClient {
    inner: Arc<KvcsLowLevelClientInner>,
}

struct KvcsLowLevelClientInner {
    efc_socket: CString,
    client: ClientSlot,
    max_value_size: u64,
}

impl KvcsLowLevelClient {
    pub fn new(efc_socket: String, max_value_size: u64) -> Result<Self> {
        validate_max_value_size(max_value_size)?;
        let efc_socket = CString::new(efc_socket)
            .map_err(|_| StoreError::InvalidState("KVCS EFC socket contains NUL".to_string()))?;
        Ok(Self {
            inner: Arc::new(KvcsLowLevelClientInner {
                efc_socket,
                client: ClientSlot::default(),
                max_value_size,
            }),
        })
    }

    /// Binds one static Mooncake target to an SDK mountpoint index.
    pub fn executor(&self, mountpoint_index: u32) -> KvcsCapiExecutor {
        KvcsCapiExecutor {
            config: KvcsConfig::LowLevel {
                client: self.clone(),
                mountpoint_index,
            },
            max_value_size: self.inner.max_value_size,
        }
    }

    fn ensure_client(&self) -> Result<*mut KvcsClient> {
        self.inner.client.get_or_create(
            "low-level",
            || {
                let raw = KvcsLlConfig {
                    // Public SDK 0.4.0 rejects NULL/empty for the low-level client.
                    efc_socket: self.inner.efc_socket.as_ptr(),
                    max_value_size: self.inner.max_value_size as i64,
                    ..KvcsLlConfig::default()
                };
                unsafe { kvcs_ll_create(&raw) }
            },
            kvcs_ll_client_destroy,
        )
    }
}

/// KVCS C API executor whose exposed NoF traits are selected by `MOONCAKE_KVCS_MODE`.
pub struct KvcsCapiExecutor {
    config: KvcsConfig,
    max_value_size: u64,
}

impl KvcsCapiExecutor {
    pub fn new() -> Result<Self> {
        Self::with_mode(KvcsMode::from_env()?)
    }

    pub fn with_mode(mode: KvcsMode) -> Result<Self> {
        let max_value_size = optional_env(KVCS_MAX_VALUE_SIZE_ENV)?
            .map(|value| {
                value.parse::<u64>().map_err(|_| {
                    StoreError::InvalidState(format!(
                        "{KVCS_MAX_VALUE_SIZE_ENV} must be an integer byte count"
                    ))
                })
            })
            .transpose()?
            .unwrap_or(SDK_DEFAULT_MAX_VALUE_SIZE);
        let efc_socket =
            optional_env("MOONCAKE_KVCS_EFC_SOCKET")?.filter(|value| !value.is_empty());
        match mode {
            KvcsMode::Standard => Self::with_standard_config(
                efc_socket,
                optional_env("MOONCAKE_KVCS_REDIS_ENDPOINTS")?
                    .into_iter()
                    .flat_map(|value| {
                        value
                            .split(',')
                            .map(str::trim)
                            .filter(|endpoint| !endpoint.is_empty())
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .collect(),
                optional_env("MOONCAKE_KVCS_REDIS_PASSWORD")?.filter(|value| !value.is_empty()),
                max_value_size,
            ),
            KvcsMode::LowLevel => Self::with_low_level_config(
                efc_socket.unwrap_or_else(|| "/var/run/kvcs/efc-grpc.sock".to_string()),
                env_u32("MOONCAKE_KVCS_MOUNTPOINT_INDEX", 0)?,
                max_value_size,
            ),
        }
    }

    pub fn with_standard_config(
        efc_socket: Option<String>,
        redis_endpoints: Vec<String>,
        redis_password: Option<String>,
        max_value_size: u64,
    ) -> Result<Self> {
        checked_batch_len(redis_endpoints.len())?;
        let to_cstring = |value: String, name: &str| {
            CString::new(value)
                .map_err(|_| StoreError::InvalidState(format!("KVCS {name} contains NUL")))
        };
        Self::from_config(
            KvcsConfig::Standard {
                efc_socket: efc_socket
                    .map(|value| to_cstring(value, "EFC socket"))
                    .transpose()?,
                redis_endpoints: redis_endpoints
                    .into_iter()
                    .map(|value| to_cstring(value, "Redis endpoint"))
                    .collect::<Result<Vec<_>>>()?,
                redis_password: redis_password
                    .map(|value| to_cstring(value, "Redis password"))
                    .transpose()?,
                client: ClientSlot::default(),
            },
            max_value_size,
        )
    }

    pub fn with_low_level_config(
        efc_socket: String,
        mountpoint_index: u32,
        max_value_size: u64,
    ) -> Result<Self> {
        Ok(KvcsLowLevelClient::new(efc_socket, max_value_size)?.executor(mountpoint_index))
    }

    fn from_config(config: KvcsConfig, max_value_size: u64) -> Result<Self> {
        validate_max_value_size(max_value_size)?;
        Ok(Self {
            config,
            max_value_size,
        })
    }

    fn mode(&self) -> KvcsMode {
        match &self.config {
            KvcsConfig::Standard { .. } => KvcsMode::Standard,
            KvcsConfig::LowLevel { .. } => KvcsMode::LowLevel,
        }
    }

    fn ensure_client(&self) -> Result<*mut KvcsClient> {
        match &self.config {
            KvcsConfig::Standard {
                efc_socket,
                redis_endpoints,
                redis_password,
                client,
            } => client.get_or_create(
                "Standard",
                || {
                    let endpoint_ptrs = redis_endpoints
                        .iter()
                        .map(|endpoint| endpoint.as_ptr())
                        .collect::<Vec<_>>();
                    let raw = KvcsClientConfig {
                        efc_socket: efc_socket
                            .as_ref()
                            .map_or(ptr::null(), |value| value.as_ptr()),
                        redis_endpoints: endpoint_ptrs
                            .first()
                            .map_or(ptr::null(), |_| endpoint_ptrs.as_ptr()),
                        redis_count: endpoint_ptrs.len() as c_int,
                        redis_password: redis_password
                            .as_ref()
                            .map_or(ptr::null(), |value| value.as_ptr()),
                        max_value_size: self.max_value_size as i64,
                        ..KvcsClientConfig::default()
                    };
                    unsafe { kvcs_client_create(&raw) }
                },
                kvcs_client_destroy,
            ),
            KvcsConfig::LowLevel { client, .. } => client.ensure_client(),
        }
    }

    fn prepare_batch(&self, len: usize) -> Result<(*mut KvcsClient, c_int)> {
        Ok((self.ensure_client()?, checked_batch_len(len)?))
    }

    fn low_level_options(&self) -> KvcsLlBatchOptions {
        let KvcsConfig::LowLevel {
            mountpoint_index, ..
        } = &self.config
        else {
            unreachable!("low-level operation is not exposed in Standard mode")
        };
        KvcsLlBatchOptions {
            mountpoint_index: *mountpoint_index,
        }
    }
}

fn validate_max_value_size(max_value_size: u64) -> Result<()> {
    if !(1..=SDK_MAX_VALUE_SIZE).contains(&max_value_size) {
        return Err(StoreError::InvalidState(format!(
            "KVCS max value size must be in 1..={SDK_MAX_VALUE_SIZE}, got {max_value_size}"
        )));
    }
    Ok(())
}

impl NofBacking for KvcsCapiExecutor {
    fn object_limits(&self) -> Option<NofObjectLimits> {
        (self.mode() == KvcsMode::Standard).then_some(NofObjectLimits {
            max_value_size: self.max_value_size,
            max_key_size: DEFAULT_MAX_KEY_SIZE as u64,
        })
    }

    fn object_write(&self) -> Option<&dyn NofObjectWrite> {
        (self.mode() == KvcsMode::Standard).then_some(self as &dyn NofObjectWrite)
    }

    fn object_read(&self) -> Option<&dyn NofObjectRead> {
        (self.mode() == KvcsMode::Standard).then_some(self as &dyn NofObjectRead)
    }

    fn object_query(&self) -> Option<&dyn NofObjectQuery> {
        (self.mode() == KvcsMode::Standard).then_some(self as &dyn NofObjectQuery)
    }

    fn object_delete(&self) -> Option<&dyn NofObjectDelete> {
        (self.mode() == KvcsMode::Standard).then_some(self as &dyn NofObjectDelete)
    }

    fn physical_write(&self) -> Option<&dyn NofPhysicalWrite> {
        (self.mode() == KvcsMode::LowLevel).then_some(self as &dyn NofPhysicalWrite)
    }

    fn physical_read(&self) -> Option<&dyn NofPhysicalRead> {
        (self.mode() == KvcsMode::LowLevel).then_some(self as &dyn NofPhysicalRead)
    }

    fn physical_query(&self) -> Option<&dyn NofPhysicalQuery> {
        (self.mode() == KvcsMode::LowLevel).then_some(self as &dyn NofPhysicalQuery)
    }

    fn physical_delete(&self) -> Option<&dyn NofPhysicalDelete> {
        (self.mode() == KvcsMode::LowLevel).then_some(self as &dyn NofPhysicalDelete)
    }

    fn health_capability(&self) -> Option<&dyn NofHealth> {
        (self.mode() == KvcsMode::LowLevel).then_some(self as &dyn NofHealth)
    }
}

mod standard {
    //! KVCS Standard SDK implementation of the NoF logical-object capabilities.

    use std::ffi::{c_int, CString};
    use std::ptr;

    use mooncake_store_core::{NamespaceScope, Result, StoreError};

    use super::super::*;
    use super::{
        encode_namespace, repeated_error, NofObjectDelete, NofObjectQuery, NofObjectRead,
        NofObjectShardWrite, NofObjectState, NofObjectWrite,
    };

    struct RawQuery {
        namespace: CString,
        key: CString,
        length: u64,
        shards: Vec<(u32, usize)>,
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
            return Err(StoreError::InvalidState(format!(
                "KVCS query shard byte count {shard_bytes} differs from reported total size {length}"
            )));
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

    impl KvcsCapiExecutor {
        fn namespace(&self, scope: &NamespaceScope) -> Result<CString> {
            let encoded = encode_namespace(scope)?;
            validate_string(&encoded, KVCS_NAMESPACE_MAX_BYTES, "namespace")
        }

        fn key(&self, key: &str) -> Result<CString> {
            validate_string(key, DEFAULT_MAX_KEY_SIZE, "object key")
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
            let mut results = QueryResults::new(1);
            let count = unsafe {
                kvcs_batch_query(
                    client,
                    namespace.as_ptr(),
                    &key_ptr,
                    1,
                    &options,
                    results.as_mut_ptr(),
                    1,
                )
            };
            results.complete(count, "Standard query")?;
            let raw = &results.as_slice()[0];
            let status = char_array_str(&raw.status).map_err(|error| {
                StoreError::Transport(format!(
                    "KVCS Standard query returned invalid status: {error}"
                ))
            })?;
            match status {
                "not_found" => Ok(NofObjectState::Missing),
                "incomplete" => Ok(NofObjectState::Incomplete),
                "ok" => {
                    let length = u64::try_from(raw.total_size).map_err(|_| {
                        StoreError::InvalidState("KVCS query returned a negative size".to_string())
                    })?;
                    if raw.total_shard <= 0 {
                        return Err(StoreError::InvalidState(
                            "KVCS complete query returned a non-positive shard count".to_string(),
                        ));
                    }
                    if length == 0 {
                        return Err(StoreError::InvalidState(
                            "KVCS complete query returned an empty object".to_string(),
                        ));
                    }
                    let total_shards = u32::try_from(raw.total_shard).map_err(|_| {
                        StoreError::InvalidState("KVCS query shard count exceeds u32".to_string())
                    })?;
                    let shards =
                        query_shards(raw, with_shards, length, total_shards, self.max_value_size)?;
                    Ok(NofObjectState::Found(RawQuery {
                        namespace,
                        key,
                        length,
                        shards,
                    }))
                }
                _ => Err(StoreError::Transport(format!(
                    "KVCS Standard query returned status {status:?}"
                ))),
            }
        }
    }

    impl NofObjectWrite for KvcsCapiExecutor {
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

        fn put_shards(&self, requests: &[NofObjectShardWrite<'_>]) -> Vec<Result<()>> {
            if requests.is_empty() {
                return Vec::new();
            }
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
            let (results, valid) = prepare_positional(requests, |request| {
                let key = self.key(request.key)?;
                if request.value.len() as u64 > self.max_value_size {
                    return Err(StoreError::InvalidState(format!(
                        "KVCS Standard shard size {} exceeds provider limit {}",
                        request.value.len(),
                        self.max_value_size
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
                Ok((key, request))
            });
            if valid.is_empty() {
                return finish_positional_results(results);
            }
            let (client, count) = match self.prepare_batch(valid.len()) {
                Ok(prepared) => prepared,
                Err(error) => return fail_positional_results(results, error),
            };
            let (value_ptrs, value_lens) =
                input_segments(valid.iter().map(|(_, (_, request))| request.value));
            let items = valid
                .iter()
                .enumerate()
                .map(|(offset, (_, (key, request)))| KvcsPutItem {
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
            let mut native_results = put_results(valid.len());
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
                return fail_positional_results(results, map_call_status(status, "Standard put"));
            }
            finish_unit_statuses(
                results,
                &valid,
                native_results.into_iter().map(|result| result.status),
                "Standard put item",
                true,
            )
        }
    }

    impl NofObjectQuery for KvcsCapiExecutor {
        fn query_object(
            &self,
            namespace: &NamespaceScope,
            key: &str,
        ) -> Result<NofObjectState<u64>> {
            Ok(match self.query_raw(namespace, key, false)? {
                NofObjectState::Found(query) => NofObjectState::Found(query.length),
                NofObjectState::Missing => NofObjectState::Missing,
                NofObjectState::Incomplete => NofObjectState::Incomplete,
            })
        }
    }

    impl NofObjectRead for KvcsCapiExecutor {
        fn get_object(
            &self,
            namespace: &NamespaceScope,
            key: &str,
            known_length: Option<u64>,
        ) -> Result<NofObjectState<Vec<u8>>> {
            let query = match known_length {
                Some(length) if length > 0 && length <= self.max_value_size => RawQuery {
                    namespace: self.namespace(namespace)?,
                    key: self.key(key)?,
                    length,
                    shards: vec![(
                        0,
                        usize::try_from(length).map_err(|_| {
                            StoreError::InvalidState(
                                "KVCS object length does not fit this platform".to_string(),
                            )
                        })?,
                    )],
                },
                _ => match self.query_raw(namespace, key, true)? {
                    NofObjectState::Found(query) => query,
                    NofObjectState::Missing => return Ok(NofObjectState::Missing),
                    NofObjectState::Incomplete => return Ok(NofObjectState::Incomplete),
                },
            };
            let client = self.ensure_client()?;
            let items = query
                .shards
                .iter()
                .map(|(shard_id, size)| KvcsGetItem {
                    key: query.key.as_ptr(),
                    shard_id: *shard_id as i32,
                    location: ptr::null(),
                    expected_value_size: *size,
                    meta: ptr::null(),
                    meta_count: 0,
                })
                .collect::<Vec<_>>();
            let capacity = usize::try_from(query.length).map_err(|_| {
                StoreError::InvalidState(
                    "KVCS object length does not fit this platform".to_string(),
                )
            })?;
            let mut value = vec![0; capacity];
            let expected = query
                .shards
                .iter()
                .map(|(_, size)| *size)
                .collect::<Vec<_>>();
            let (mut buffer_ptrs, mut statuses, mut lengths) =
                output_buffer_parts(&mut value, &expected)?;
            let count = checked_batch_len(items.len())?;
            let status = unsafe {
                kvcs_batch_get_into(
                    client,
                    query.namespace.as_ptr(),
                    items.as_ptr(),
                    count,
                    buffer_ptrs.as_mut_ptr(),
                    expected.as_ptr(),
                    statuses.as_mut_ptr(),
                    lengths.as_mut_ptr(),
                    0,
                )
            };
            if status < 0 {
                return Err(map_call_status(status, "Standard get"));
            }
            if validate_get_results(&statuses, &lengths, &expected, "Standard get")?
                != expected.len()
            {
                return Ok(NofObjectState::Incomplete);
            }
            Ok(NofObjectState::Found(value))
        }
    }

    impl NofObjectDelete for KvcsCapiExecutor {
        fn delete_object(
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
            let items = query
                .shards
                .iter()
                .map(|(shard_id, _)| KvcsDeleteItem {
                    key: query.key.as_ptr(),
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
                    query.namespace.as_ptr(),
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
            let backend = crate::NofBackend::new(executor.clone()).unwrap();
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let namespace = NamespaceScope::new(format!("smoke-{nonce}"), "nof", "standard");
            let key = "round-trip";
            let value = b"kvcs-capi-standard-smoke";

            backend.init_namespace(&namespace).unwrap();
            backend.put_object(&namespace, key, value).unwrap();
            let NofObjectState::Found(length) = backend.query_object(&namespace, key).unwrap()
            else {
                panic!("KVCS Standard put did not publish its manifest")
            };
            assert_eq!(length, value.len() as u64);
            assert!(matches!(
                backend
                    .get_object_with_known_length(&namespace, key, length)
                    .unwrap(),
                NofObjectState::Found(got) if got == value
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

    use mooncake_store_core::{Result, StoreError};

    use super::super::physical_layout::{
        build_write_layout, manifest_key, read_records, KvcsReadRecord, MANIFEST_SIZE,
    };
    use super::super::*;
    use super::{
        NofHealth, NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalQuery,
        NofPhysicalQueryRequest, NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalWrite,
        NofPhysicalWriteRequest, NofStorageHealth, OpaquePhysicalKey,
    };

    impl KvcsCapiExecutor {
        fn put_records(&self, requests: &[(&OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
            let (results, valid) = prepare_positional(requests, |(key, value)| {
                Ok((encode_physical_key(key, DEFAULT_MAX_KEY_SIZE)?, *value))
            });
            if valid.is_empty() {
                return finish_positional_results(results);
            }
            let (client, count) = match self.prepare_batch(valid.len()) {
                Ok(prepared) => prepared,
                Err(error) => return fail_positional_results(results, error),
            };
            let (pointers, lengths) = input_segments(valid.iter().map(|(_, (_, value))| *value));
            let items = valid
                .iter()
                .enumerate()
                .map(|(offset, (_, (key, _)))| KvcsLlPutItem {
                    key: key.as_ptr(),
                    value_segs: &pointers[offset],
                    seg_lens: &lengths[offset],
                    seg_count: 1,
                })
                .collect::<Vec<_>>();
            let mut native_results = put_results(valid.len());
            let options = self.low_level_options();
            let status = unsafe {
                kvcs_ll_batch_put(
                    client,
                    items.as_ptr(),
                    count,
                    native_results.as_mut_ptr(),
                    count,
                    0,
                    &options,
                )
            };
            if status < 0 {
                return fail_positional_results(results, map_call_status(status, "low-level put"));
            }
            finish_unit_statuses(
                results,
                &valid,
                native_results.into_iter().map(|result| result.status),
                "low-level put item",
                true,
            )
        }

        fn get_records(&self, records: &[KvcsReadRecord]) -> Result<Option<Vec<u8>>> {
            let keys = records
                .iter()
                .map(|record| encode_physical_key(&record.key, DEFAULT_MAX_KEY_SIZE))
                .collect::<Result<Vec<_>>>()?;
            let (client, count) = self.prepare_batch(records.len())?;
            let items = keys
                .iter()
                .map(|key| KvcsLlGetItem { key: key.as_ptr() })
                .collect::<Vec<_>>();
            let expected = records
                .iter()
                .map(|record| record.expected_value_size)
                .collect::<Vec<_>>();
            let capacity = expected.iter().try_fold(0usize, |total, length| {
                total.checked_add(*length).ok_or_else(|| {
                    StoreError::InvalidState(
                        "KVCS low-level read buffer length overflow".to_string(),
                    )
                })
            })?;
            let mut value = vec![0; capacity];
            let (mut pointers, mut statuses, mut lengths) =
                output_buffer_parts(&mut value, &expected)?;
            let options = self.low_level_options();
            let status = unsafe {
                kvcs_ll_batch_get_into(
                    client,
                    items.as_ptr(),
                    count,
                    pointers.as_mut_ptr(),
                    expected.as_ptr(),
                    statuses.as_mut_ptr(),
                    lengths.as_mut_ptr(),
                    0,
                    &options,
                )
            };
            if status < 0 {
                return Err(map_call_status(status, "low-level get"));
            }
            let found = validate_get_results(&statuses, &lengths, &expected, "low-level get")?;
            match found {
                0 => Ok(None),
                count if count == records.len() => Ok(Some(value)),
                _ => Err(StoreError::InvalidState(
                    "KVCS low-level physical object is incomplete".to_string(),
                )),
            }
        }

        fn get_records_dynamic(&self, keys: &[&OpaquePhysicalKey]) -> Vec<Result<Option<Vec<u8>>>> {
            struct DynamicGets {
                values: Vec<Option<Vec<u8>>>,
                statuses: Vec<c_int>,
                malformed: bool,
            }

            unsafe extern "C" fn capture(
                count: c_int,
                data_ptrs: *const *const std::ffi::c_void,
                data_lens: *const libc::size_t,
                statuses: *const c_int,
                context: *mut std::ffi::c_void,
            ) {
                if count < 0 || context.is_null() || statuses.is_null() {
                    return;
                }
                let output = unsafe { &mut *(context.cast::<DynamicGets>()) };
                let count = count as usize;
                if count != output.values.len() {
                    output.malformed = true;
                }
                for index in 0..count.min(output.values.len()) {
                    let status = unsafe { *statuses.add(index) };
                    output.statuses[index] = status;
                    if status <= 0 {
                        continue;
                    }
                    if data_lens.is_null() || data_ptrs.is_null() {
                        output.statuses[index] = -libc::EIO;
                        continue;
                    }
                    let length = unsafe { *data_lens.add(index) };
                    let data = unsafe { *data_ptrs.add(index) };
                    if data.is_null() {
                        output.statuses[index] = -libc::EIO;
                        continue;
                    }
                    output.values[index] = Some(
                        unsafe { std::slice::from_raw_parts(data.cast::<u8>(), length) }.to_vec(),
                    );
                }
            }

            let (mut results, valid) =
                prepare_positional(keys, |key| encode_physical_key(key, DEFAULT_MAX_KEY_SIZE));
            if valid.is_empty() {
                return finish_positional_results(results);
            }
            let (client, count) = match self.prepare_batch(valid.len()) {
                Ok(prepared) => prepared,
                Err(error) => return fail_positional_results(results, error),
            };
            let items = valid
                .iter()
                .map(|(_, key)| KvcsLlGetItem { key: key.as_ptr() })
                .collect::<Vec<_>>();
            let mut output = DynamicGets {
                values: vec![None; valid.len()],
                statuses: vec![0; valid.len()],
                malformed: false,
            };
            let options = self.low_level_options();
            let status = unsafe {
                kvcs_ll_batch_get(
                    client,
                    items.as_ptr(),
                    count,
                    Some(capture),
                    (&mut output as *mut DynamicGets).cast(),
                    0,
                    &options,
                )
            };
            if status < 0 {
                return fail_positional_results(results, map_call_status(status, "low-level get"));
            }
            if output.malformed {
                return fail_positional_results(
                    results,
                    StoreError::InvalidState(
                        "KVCS low-level get callback returned the wrong item count".to_string(),
                    ),
                );
            }
            for ((request_index, _), (status, value)) in valid
                .iter()
                .zip(output.statuses.into_iter().zip(output.values))
            {
                results[*request_index] = Some(if status < 0 {
                    Err(map_item_status(status, "low-level get item", false))
                } else if status == 0 {
                    Ok(None)
                } else {
                    value.map(Some).ok_or_else(|| {
                        StoreError::InvalidState(
                            "KVCS low-level get callback omitted a value".to_string(),
                        )
                    })
                });
            }
            finish_positional_results(results)
        }

        fn put_retryable_records(
            &self,
            requests: &[(&OpaquePhysicalKey, &[u8])],
        ) -> Vec<Result<()>> {
            requests
                .iter()
                .zip(self.put_records(requests))
                .map(|((key, value), result)| match result {
                    Err(StoreError::Conflict(_)) => {
                        let record = KvcsReadRecord {
                            key: (*key).clone(),
                            expected_value_size: value.len(),
                        };
                        match self.get_records(&[record]) {
                            Ok(Some(existing)) if existing == *value => Ok(()),
                            Ok(_) => Err(StoreError::Conflict(
                                "KVCS low-level retry found a different value".to_string(),
                            )),
                            Err(error) => Err(error),
                        }
                    }
                    result => result,
                })
                .collect()
        }

        fn read_chunked_object(&self, request: &NofPhysicalReadRequest) -> Result<Option<Vec<u8>>> {
            let Some((_, chunks, value_size)) = self.read_chunk_manifest(&request.key)? else {
                return Ok(None);
            };
            match self.get_records(&chunks)? {
                Some(value) if value.len() == value_size => Ok(Some(value)),
                Some(_) => Err(StoreError::InvalidState(
                    "KVCS low-level chunks do not match the manifest length".to_string(),
                )),
                None => Err(StoreError::InvalidState(
                    "KVCS low-level chunk manifest references missing data".to_string(),
                )),
            }
        }

        fn delete_records(&self, keys: &[&OpaquePhysicalKey]) -> Vec<Result<()>> {
            let (results, valid) =
                prepare_positional(keys, |key| encode_physical_key(key, DEFAULT_MAX_KEY_SIZE));
            if valid.is_empty() {
                return finish_positional_results(results);
            }
            let (client, count) = match self.prepare_batch(valid.len()) {
                Ok(prepared) => prepared,
                Err(error) => return fail_positional_results(results, error),
            };
            let pointers = valid
                .iter()
                .map(|(_, key)| key.as_ptr())
                .collect::<Vec<_>>();
            let mut statuses = vec![0; valid.len()];
            let options = self.low_level_options();
            let status = unsafe {
                kvcs_ll_batch_delete(
                    client,
                    pointers.as_ptr(),
                    count,
                    statuses.as_mut_ptr(),
                    count,
                    0,
                    &options,
                )
            };
            if status < 0 {
                return fail_positional_results(
                    results,
                    map_call_status(status, "low-level delete"),
                );
            }
            finish_unit_statuses(results, &valid, statuses, "low-level delete item", true)
        }

        fn delete_existing_records(&self, keys: &[&OpaquePhysicalKey]) -> Result<bool> {
            let mut deleted = false;
            for result in self.delete_records(keys) {
                match result {
                    Ok(()) => deleted = true,
                    Err(StoreError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            Ok(deleted)
        }

        fn read_chunk_manifest(
            &self,
            root: &OpaquePhysicalKey,
        ) -> Result<Option<(OpaquePhysicalKey, Vec<KvcsReadRecord>, usize)>> {
            let key = manifest_key(root)?;
            let record = KvcsReadRecord {
                key: key.clone(),
                expected_value_size: MANIFEST_SIZE,
            };
            let Some(manifest) = self.get_records(&[record])? else {
                return Ok(None);
            };
            let (chunks, value_size) = read_records(root, &manifest)?;
            Ok(Some((key, chunks, value_size)))
        }

        fn query_records(&self, keys: &[&OpaquePhysicalKey]) -> Vec<Result<Option<u64>>> {
            let (mut results, valid) =
                prepare_positional(keys, |key| encode_physical_key(key, DEFAULT_MAX_KEY_SIZE));
            if valid.is_empty() {
                return finish_positional_results(results);
            }
            let (client, count) = match self.prepare_batch(valid.len()) {
                Ok(prepared) => prepared,
                Err(error) => return fail_positional_results(results, error),
            };
            let pointers = valid
                .iter()
                .map(|(_, key)| key.as_ptr())
                .collect::<Vec<_>>();
            let mut native_results = QueryResults::new(count);
            let options = self.low_level_options();
            let written = unsafe {
                kvcs_ll_batch_query(
                    client,
                    pointers.as_ptr(),
                    count,
                    native_results.as_mut_ptr(),
                    count,
                    &options,
                )
            };
            if let Err(error) = native_results.complete(written, "low-level query") {
                fill_missing(&mut results, &error);
            } else {
                for ((request_index, _), native) in valid.iter().zip(native_results.as_slice()) {
                    results[*request_index] = Some(kvcs_low_level_query_status(native));
                }
            }
            finish_positional_results(results)
        }

        fn delete_object(&self, request: &NofPhysicalDeleteRequest) -> Result<()> {
            if self.delete_existing_records(&[&request.key])? {
                return Ok(());
            }

            let Some((manifest_key, chunks, _)) = self.read_chunk_manifest(&request.key)? else {
                return Err(StoreError::NotFound(
                    "KVCS low-level physical object".to_string(),
                ));
            };
            self.delete_existing_records(
                &chunks.iter().map(|chunk| &chunk.key).collect::<Vec<_>>(),
            )?;
            self.delete_existing_records(&[&manifest_key])?
                .then_some(())
                .ok_or_else(|| StoreError::NotFound("KVCS chunk manifest".to_string()))
        }

        fn query_object(&self, request: &NofPhysicalQueryRequest) -> Result<Option<u64>> {
            if let Some(length) = self.query_records(&[&request.key]).remove(0)? {
                return Ok(Some(length));
            }
            let Some((_, _, value_size)) = self.read_chunk_manifest(&request.key)? else {
                return Ok(None);
            };
            Ok(Some(value_size as u64))
        }
    }

    impl NofPhysicalWrite for KvcsCapiExecutor {
        fn put_batch(&self, requests: &[NofPhysicalWriteRequest<'_>]) -> Vec<Result<()>> {
            let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
            let mut layouts = Vec::with_capacity(requests.len());
            for (index, request) in requests.iter().enumerate() {
                match build_write_layout(&request.key, request.value, self.max_value_size) {
                    Ok(layout) => layouts.push((index, layout)),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }

            let data_records = layouts
                .iter()
                .flat_map(|(request_index, layout)| {
                    layout
                        .data_records
                        .iter()
                        .map(move |(key, value)| (*request_index, key, *value))
                })
                .collect::<Vec<_>>();
            let writes = data_records
                .iter()
                .map(|(_, key, value)| (*key, *value))
                .collect::<Vec<_>>();
            for ((request_index, _, _), status) in
                data_records.iter().zip(self.put_retryable_records(&writes))
            {
                if let Err(error) = status {
                    results[*request_index] = Some(Err(error));
                }
            }

            let manifests = layouts
                .iter()
                .filter(|(request_index, layout)| {
                    results[*request_index].is_none() && layout.manifest.is_some()
                })
                .map(|(request_index, layout)| {
                    let (key, value) = layout
                        .manifest
                        .as_ref()
                        .expect("manifest presence was filtered above");
                    (*request_index, key, value.as_slice())
                })
                .collect::<Vec<_>>();
            let writes = manifests
                .iter()
                .map(|(_, key, value)| (*key, *value))
                .collect::<Vec<_>>();
            for ((request_index, _, _), status) in
                manifests.iter().zip(self.put_retryable_records(&writes))
            {
                if let Err(error) = status {
                    results[*request_index].get_or_insert(Err(error));
                }
            }
            for result in &mut results {
                if result.is_none() {
                    *result = Some(Ok(()));
                }
            }
            finish_positional_results(results)
        }
    }

    impl NofPhysicalRead for KvcsCapiExecutor {
        fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
            let keys = requests
                .iter()
                .map(|request| &request.key)
                .collect::<Vec<_>>();
            requests
                .iter()
                .zip(self.get_records_dynamic(&keys))
                .map(|(request, root)| match root? {
                    Some(value) => Ok(Some(value)),
                    None => self.read_chunked_object(request),
                })
                .collect()
        }
    }

    impl NofPhysicalDelete for KvcsCapiExecutor {
        fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>> {
            requests
                .iter()
                .map(|request| self.delete_object(request))
                .collect()
        }
    }

    impl NofPhysicalQuery for KvcsCapiExecutor {
        fn query_batch(&self, requests: &[NofPhysicalQueryRequest]) -> Vec<Result<Option<u64>>> {
            requests
                .iter()
                .map(|request| self.query_object(request))
                .collect()
        }
    }

    impl NofHealth for KvcsCapiExecutor {
        fn health(&self) -> Result<NofStorageHealth> {
            let probe = OpaquePhysicalKey::new(b"mooncake:nof:health".to_vec());
            self.query_records(&[&probe]).pop().ok_or_else(|| {
                StoreError::Transport("KVCS health query returned no result".to_string())
            })??;
            Ok(NofStorageHealth::default())
        }
    }

    fn kvcs_low_level_query_status(raw: &KvcsQueryResult) -> Result<Option<u64>> {
        match char_array_str(&raw.status).map_err(|error| {
            StoreError::Transport(format!(
                "KVCS low-level query returned invalid status: {error}"
            ))
        })? {
            "ok" => u64::try_from(raw.total_size).map(Some).map_err(|_| {
                StoreError::InvalidState("KVCS query returned a negative size".to_string())
            }),
            "not_found" => Ok(None),
            "incomplete" => Err(StoreError::Backpressure(
                "KVCS low-level query returned incomplete".to_string(),
            )),
            "unavailable" => Err(StoreError::Transport(
                "KVCS low-level mountpoint is unavailable".to_string(),
            )),
            other => Err(StoreError::Transport(format!(
                "KVCS low-level query returned status {other}"
            ))),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::{NofBacking, NofStorageHealth};
        use super::*;

        fn round_trip(value: &[u8]) {
            let executor = super::KvcsCapiExecutor::with_mode(super::KvcsMode::LowLevel).unwrap();
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let key = OpaquePhysicalKey::new(format!("mooncake-smoke-{nonce}").into_bytes());
            executor
                .physical_write()
                .unwrap()
                .put_batch(&[NofPhysicalWriteRequest {
                    key: key.clone(),
                    value,
                }])
                .remove(0)
                .unwrap();
            assert_eq!(
                executor
                    .physical_query()
                    .unwrap()
                    .query_batch(&[NofPhysicalQueryRequest { key: key.clone() }])
                    .remove(0)
                    .unwrap(),
                Some(value.len() as u64)
            );
            assert_eq!(
                executor
                    .physical_read()
                    .unwrap()
                    .get_batch(&[NofPhysicalReadRequest { key: key.clone() }])
                    .remove(0)
                    .unwrap(),
                Some(value.to_vec())
            );
            executor
                .physical_delete()
                .unwrap()
                .delete_batch(&[NofPhysicalDeleteRequest { key: key.clone() }])
                .remove(0)
                .unwrap();
            assert_eq!(
                executor
                    .physical_query()
                    .unwrap()
                    .query_batch(&[NofPhysicalQueryRequest { key }])
                    .remove(0)
                    .unwrap(),
                None
            );
            assert!(executor.physical_read().is_some());
            assert!(executor.physical_write().is_some());
            assert!(executor.physical_query().is_some());
            assert!(executor.health_capability().is_some());
            assert_eq!(
                executor.health_capability().unwrap().health().unwrap(),
                NofStorageHealth::default()
            );
            assert!(executor.object_read().is_none());
        }

        #[test]
        #[ignore = "requires a live KVCS EFC and configured mountpoint, or the official SDK mock"]
        fn live_low_level_round_trip_smoke() {
            round_trip(b"kvcs-capi-smoke");
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
    }

    #[test]
    fn low_level_client_binds_static_targets_to_one_sdk_client() {
        let client =
            KvcsLowLevelClient::new("/run/kvcs.sock".to_string(), SDK_DEFAULT_MAX_VALUE_SIZE)
                .unwrap();
        let first = client.executor(3);
        let second = client.executor(7);
        let KvcsConfig::LowLevel {
            client: first_client,
            mountpoint_index: first_index,
        } = &first.config
        else {
            unreachable!()
        };
        let KvcsConfig::LowLevel {
            client: second_client,
            mountpoint_index: second_index,
        } = &second.config
        else {
            unreachable!()
        };
        assert_eq!((*first_index, *second_index), (3, 7));
        assert!(Arc::ptr_eq(&first_client.inner, &second_client.inner));
    }
}
