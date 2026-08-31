//! KVCS Low-Level SDK executor for the NoF physical KV contract.

use std::ffi::{c_int, c_longlong, c_void, CString};
use std::ptr;
use std::sync::Mutex;

use libc::size_t;
use mooncake_store_core::{Result, StoreError};

use super::super::*;
use super::{
    NofLowLevelCapabilities, NofLowLevelDeleteRequest, NofLowLevelExecutor, NofLowLevelGetRequest,
    OpaquePhysicalKey,
};

pub struct KvcsCapiLowLevelExecutor {
    config: KvcsLlConfig,
    _efc_socket: Option<CString>,
    _log_path: Option<CString>,
    mountpoint_index: u32,
    client: Mutex<Option<ClientHandle>>,
    limits: CommonLimits,
}

unsafe impl Send for KvcsCapiLowLevelExecutor {}
unsafe impl Sync for KvcsCapiLowLevelExecutor {}

impl KvcsCapiLowLevelExecutor {
    pub fn new() -> Result<Self> {
        let limits = CommonLimits::from_env()?;
        let efc_socket = optional_cstring("MOONCAKE_KVCS_EFC_SOCKET")?;
        let log_path = optional_cstring("MOONCAKE_KVCS_LOG_PATH")?;
        let configured_value = env_u64("MOONCAKE_KVCS_MAX_VALUE_SIZE", 0)?;
        let configured_key = env_u32("MOONCAKE_KVCS_MAX_KEY_SIZE", 0)?;
        let configured_batch = env_u32("MOONCAKE_KVCS_MAX_KEYS_PER_BATCH", 0)?;
        let config = KvcsLlConfig {
            efc_socket: efc_socket
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
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
            get_workers: env_u32("MOONCAKE_KVCS_GET_WORKERS", 0)?.min(c_int::MAX as u32) as c_int,
            set_workers: env_u32("MOONCAKE_KVCS_SET_WORKERS", 0)?.min(c_int::MAX as u32) as c_int,
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

impl Drop for KvcsCapiLowLevelExecutor {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.client.lock() {
            if let Some(handle) = slot.take() {
                unsafe { kvcs_ll_client_destroy(handle.0) };
            }
        }
    }
}

impl NofLowLevelExecutor for KvcsCapiLowLevelExecutor {
    fn capabilities(&self) -> NofLowLevelCapabilities {
        NofLowLevelCapabilities {
            max_value_size: self.limits.max_value_size,
            max_batch_items: self.limits.max_batch_items,
            // KVCS documents max_value_size per value and splits large batches internally.
            max_batch_bytes: u64::MAX,
        }
    }

    fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
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

    fn get_batch(&self, requests: &[NofLowLevelGetRequest]) -> Vec<Result<Option<Vec<u8>>>> {
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
                        let hint = u64::try_from(request.expected_value_size).map_err(|_| {
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
        let (mut statuses, mut lengths) = match self.call_get_into(client, &items, &mut buffers) {
            Ok(output) => output,
            Err(error) => {
                fill_missing(&mut results, &error);
                return finish_positional_results(results);
            }
        };
        for index in 0..valid.len() {
            if statuses[index] == -(libc::ENOSPC as c_int) && lengths[index] > buffers[index].len()
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
        for (offset, ((request_index, _, _), status)) in valid.iter().zip(statuses).enumerate() {
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

    fn delete_batch(&self, requests: &[NofLowLevelDeleteRequest]) -> Vec<Result<()>> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires a live KVCS EFC and configured mountpoint"]
    fn live_low_level_round_trip_smoke() {
        let executor = KvcsCapiLowLevelExecutor::new().unwrap();
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
        assert_eq!(
            executor
                .get_batch(&[NofLowLevelGetRequest {
                    key: key.clone(),
                    expected_value_size: value.len(),
                }])
                .remove(0)
                .unwrap(),
            Some(value.to_vec())
        );
        executor
            .delete_batch(&[NofLowLevelDeleteRequest { key }])
            .remove(0)
            .unwrap();
    }
}
