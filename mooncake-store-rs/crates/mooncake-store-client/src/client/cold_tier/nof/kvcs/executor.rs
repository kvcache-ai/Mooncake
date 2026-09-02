use std::ffi::{c_int, CString};
use std::ptr;

use mooncake_store_core::{Result, StoreError};

use super::super::backend::encode_namespace;
use super::super::object::repeated_error;
use super::super::{
    NofBacking, NofHealth, NofObject, NofObjectDelete, NofObjectLimits, NofObjectMetadata,
    NofObjectQuery, NofObjectRead, NofObjectShardWrite, NofObjectState, NofObjectWrite,
    NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalLocator, NofPhysicalQuery,
    NofPhysicalQueryRequest, NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalWrite,
    NofPhysicalWriteRequest, NofStorageHealth, OpaquePhysicalKey,
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
    },
    LowLevel {
        efc_socket: CString,
        mountpoint_index: u32,
    },
}

/// KVCS C API executor whose exposed NoF traits are selected by `MOONCAKE_KVCS_MODE`.
pub struct KvcsCapiExecutor {
    config: KvcsConfig,
    client: ClientSlot,
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
        if !(1..=SDK_MAX_VALUE_SIZE).contains(&max_value_size) {
            return Err(StoreError::InvalidState(format!(
                "{KVCS_MAX_VALUE_SIZE_ENV} must be in 1..={SDK_MAX_VALUE_SIZE}, got {max_value_size}"
            )));
        }
        let config = match mode {
            KvcsMode::Standard => {
                let efc_socket = optional_cstring("MOONCAKE_KVCS_EFC_SOCKET")?;
                let redis_password = optional_cstring("MOONCAKE_KVCS_REDIS_PASSWORD")?;
                let redis_endpoints = optional_env("MOONCAKE_KVCS_REDIS_ENDPOINTS")?
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
                checked_batch_len(redis_endpoints.len())?;
                KvcsConfig::Standard {
                    efc_socket,
                    redis_endpoints,
                    redis_password,
                }
            }
            KvcsMode::LowLevel => {
                let efc_socket = optional_cstring("MOONCAKE_KVCS_EFC_SOCKET")?
                    .unwrap_or(CString::new("/var/run/kvcs/efc-grpc.sock").expect("static path"));
                KvcsConfig::LowLevel {
                    efc_socket,
                    mountpoint_index: env_u32("MOONCAKE_KVCS_MOUNTPOINT_INDEX", 0)?,
                }
            }
        };
        Ok(Self {
            config,
            client: ClientSlot::default(),
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
            } => self.client.get_or_create(
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
            KvcsConfig::LowLevel { efc_socket, .. } => self.client.get_or_create(
                "low-level",
                || {
                    let raw = KvcsLlConfig {
                        // Public SDK 0.4.0 rejects NULL/empty for the low-level client.
                        efc_socket: efc_socket.as_ptr(),
                        max_value_size: self.max_value_size as i64,
                        ..KvcsLlConfig::default()
                    };
                    unsafe { kvcs_ll_create(&raw) }
                },
                kvcs_ll_client_destroy,
            ),
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
        encode_namespace, repeated_error, NofObject, NofObjectDelete, NofObjectMetadata,
        NofObjectQuery, NofObjectRead, NofObjectShardWrite, NofObjectState, NofObjectWrite,
    };

    struct RawQuery {
        namespace: CString,
        key: CString,
        metadata: NofObjectMetadata,
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
            if count < 0 {
                return Err(map_call_status(count, "Standard query"));
            }
            if count != 1 {
                return Err(StoreError::Transport(format!(
                    "KVCS Standard query returned {count} results for one request"
                )));
            }
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
        ) -> Result<NofObjectState<NofObjectMetadata>> {
            Ok(self
                .query_raw(namespace, key, false)?
                .map(|query| query.metadata))
        }
    }

    impl NofObjectRead for KvcsCapiExecutor {
        fn get_object(
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
            let mut buffers = query
                .shards
                .iter()
                .map(|(_, size)| vec![0; *size])
                .collect::<Vec<_>>();
            let (mut buffer_ptrs, capacities, mut statuses, mut lengths) =
                output_buffers(&mut buffers);
            let count = checked_batch_len(items.len())?;
            let status = unsafe {
                kvcs_batch_get_into(
                    client,
                    query.namespace.as_ptr(),
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

    use std::collections::HashSet;

    use mooncake_store_core::{Result, StoreError};

    use super::super::physical_layout::{
        assemble_read, build_write_layout, object_keys, read_records, KvcsReadRecord,
    };
    use super::super::*;
    use super::{
        NofHealth, NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalLocator,
        NofPhysicalQuery, NofPhysicalQueryRequest, NofPhysicalRead, NofPhysicalReadRequest,
        NofPhysicalWrite, NofPhysicalWriteRequest, NofStorageHealth, OpaquePhysicalKey,
    };

    impl KvcsCapiExecutor {
        fn put_records(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
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

        fn get_records(&self, requests: &[KvcsReadRecord]) -> Vec<Result<Option<Vec<u8>>>> {
            let (mut results, valid) = prepare_positional(requests, |request| {
                Ok((
                    encode_physical_key(&request.key, DEFAULT_MAX_KEY_SIZE)?,
                    request.expected_value_size,
                ))
            });
            if valid.is_empty() {
                return finish_positional_results(results);
            }
            let (client, count) = match self.prepare_batch(valid.len()) {
                Ok(prepared) => prepared,
                Err(error) => return fail_positional_results(results, error),
            };
            let items = valid
                .iter()
                .map(|(_, (key, _))| KvcsLlGetItem { key: key.as_ptr() })
                .collect::<Vec<_>>();
            let mut buffers = valid
                .iter()
                .map(|(_, (_, hint))| vec![0; *hint])
                .collect::<Vec<_>>();
            let (mut pointers, capacities, mut statuses, mut lengths) =
                output_buffers(&mut buffers);
            let options = self.low_level_options();
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
                    &options,
                )
            };
            if status < 0 {
                return fail_positional_results(results, map_call_status(status, "low-level get"));
            }
            for (offset, ((request_index, _), status)) in valid.iter().zip(statuses).enumerate() {
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

        fn delete_records(&self, keys: &[OpaquePhysicalKey]) -> Vec<Result<()>> {
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

        fn query_records(&self, keys: &[OpaquePhysicalKey]) -> Vec<Result<bool>> {
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
            if written < 0 {
                return fail_positional_results(
                    results,
                    map_call_status(written, "low-level query"),
                );
            }
            if written != count {
                let error = StoreError::Transport(format!(
                    "KVCS low-level query returned {written} results for {count} keys"
                ));
                fill_missing(&mut results, &error);
            } else {
                for ((request_index, _), native) in valid.iter().zip(native_results.as_slice()) {
                    results[*request_index] = Some(kvcs_low_level_query_status(&native.status));
                }
            }
            finish_positional_results(results)
        }

        fn read_object(&self, request: &NofPhysicalReadRequest) -> Result<Option<Vec<u8>>> {
            let records =
                read_records(&request.key, &request.locator, request.expected_value_size)?;
            assemble_read(request.expected_value_size, self.get_records(&records))
        }

        fn delete_object(&self, request: &NofPhysicalDeleteRequest) -> Result<()> {
            let keys = object_keys(&request.key, &request.locator, request.expected_value_size)?;
            let mut deleted = false;
            for result in self.delete_records(&keys) {
                match result {
                    Ok(()) => deleted = true,
                    Err(StoreError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            if deleted {
                Ok(())
            } else {
                Err(StoreError::NotFound(
                    "KVCS low-level physical object".to_string(),
                ))
            }
        }

        fn query_object(&self, request: &NofPhysicalQueryRequest) -> Result<bool> {
            let keys = object_keys(&request.key, &request.locator, request.expected_value_size)?;
            for result in self.query_records(&keys) {
                if !result? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }

    impl NofPhysicalWrite for KvcsCapiExecutor {
        fn put_batch(
            &self,
            requests: &[NofPhysicalWriteRequest<'_>],
        ) -> Vec<Result<NofPhysicalLocator>> {
            let mut results = (0..requests.len()).map(|_| None).collect::<Vec<_>>();
            let mut layouts = Vec::with_capacity(requests.len());
            for (index, request) in requests.iter().enumerate() {
                match build_write_layout(&request.key, request.value, self.max_value_size) {
                    Ok(layout) => layouts.push((index, layout)),
                    Err(error) => results[index] = Some(Err(error)),
                }
            }

            let mut seen = HashSet::new();
            let mut duplicates = HashSet::new();
            for (_, layout) in &layouts {
                for (key, _) in &layout.records {
                    if !seen.insert(key.clone()) {
                        duplicates.insert(key.clone());
                    }
                }
            }
            layouts.retain(|(index, layout)| {
                if layout
                    .records
                    .iter()
                    .any(|(key, _)| duplicates.contains(key))
                {
                    results[*index] = Some(Err(StoreError::Conflict(
                        "KVCS physical write contains duplicate record keys".to_string(),
                    )));
                    false
                } else {
                    true
                }
            });

            let records = layouts
                .iter()
                .flat_map(|(request_index, layout)| {
                    layout
                        .records
                        .iter()
                        .map(move |(key, value)| (*request_index, key.clone(), *value))
                })
                .collect::<Vec<_>>();
            let writes = records
                .iter()
                .map(|(_, key, value)| (key.clone(), *value))
                .collect::<Vec<_>>();
            let record_results = self.put_records(&writes);
            for ((request_index, _, _), status) in records.into_iter().zip(record_results) {
                if let Err(error) = status {
                    if results[request_index].is_none() {
                        results[request_index] = Some(Err(error));
                    }
                }
            }
            for (request_index, layout) in layouts {
                if results[request_index].is_none() {
                    results[request_index] = Some(Ok(layout.locator));
                }
            }
            finish_positional_results(results)
        }
    }

    impl NofPhysicalRead for KvcsCapiExecutor {
        fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
            requests
                .iter()
                .map(|request| self.read_object(request))
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
        fn query_batch(&self, requests: &[NofPhysicalQueryRequest]) -> Vec<Result<bool>> {
            requests
                .iter()
                .map(|request| self.query_object(request))
                .collect()
        }
    }

    impl NofHealth for KvcsCapiExecutor {
        fn health(&self) -> Result<NofStorageHealth> {
            let probe = OpaquePhysicalKey::new(b"mooncake:nof:health:v1".to_vec());
            self.query_records(&[probe]).pop().ok_or_else(|| {
                StoreError::Transport("KVCS health query returned no result".to_string())
            })??;
            Ok(NofStorageHealth::default())
        }
    }

    fn kvcs_low_level_query_status(status: &[std::ffi::c_char; 32]) -> Result<bool> {
        match char_array_str(status).map_err(|error| {
            StoreError::Transport(format!(
                "KVCS low-level query returned invalid status: {error}"
            ))
        })? {
            "ok" => Ok(true),
            "not_found" => Ok(false),
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
            let locator = executor
                .physical_write()
                .unwrap()
                .put_batch(&[NofPhysicalWriteRequest {
                    key: key.clone(),
                    value,
                }])
                .remove(0)
                .unwrap();
            assert!(executor
                .physical_query()
                .unwrap()
                .query_batch(&[NofPhysicalQueryRequest {
                    key: key.clone(),
                    locator: locator.clone(),
                    expected_value_size: value.len(),
                }])
                .remove(0)
                .unwrap());
            assert_eq!(
                executor
                    .physical_read()
                    .unwrap()
                    .get_batch(&[NofPhysicalReadRequest {
                        key: key.clone(),
                        locator: locator.clone(),
                        expected_value_size: value.len(),
                    }])
                    .remove(0)
                    .unwrap(),
                Some(value.to_vec())
            );
            executor
                .physical_delete()
                .unwrap()
                .delete_batch(&[NofPhysicalDeleteRequest {
                    key: key.clone(),
                    locator: locator.clone(),
                    expected_value_size: value.len(),
                }])
                .remove(0)
                .unwrap();
            assert!(!executor
                .physical_query()
                .unwrap()
                .query_batch(&[NofPhysicalQueryRequest {
                    key,
                    locator,
                    expected_value_size: value.len(),
                }])
                .remove(0)
                .unwrap());
            assert!(executor.physical_read().is_some());
            assert!(executor.physical_write().is_some());
            assert!(executor.physical_query().is_some());
            assert!(executor.health_capability().is_some());
            assert_eq!(
                executor.health_capability().unwrap().health().unwrap(),
                NofStorageHealth::default()
            );
            assert!(executor.object_read().is_none());
            assert!(executor.storage_management().is_none());
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
        assert!(low_level.storage_management().is_none());
    }
}
