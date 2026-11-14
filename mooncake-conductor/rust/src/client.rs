// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ffi::{c_char, CStr, CString};
use std::time::Duration;

use crate::error::{check, ConductorError};
use crate::ffi;
use crate::types::{
    CacheHit, ClientConfig, ContextView, GlobalView, HashProfile, HealthStatus, PublisherKind,
    QueryRequest, QueryResult, RankMatch, RegisterResult, ServiceConfig, ServiceInfo,
};

// Values of `enum mooncake_conductor_health`.
const HEALTH_OK: i32 = 0;
const HEALTH_NOT_INITIALIZED: i32 = 1;

/// Client for the Mooncake Conductor RPC service.
///
/// Wraps the C++ `ConductorClient` through the `conductor_c.h` C ABI. Every
/// call is a blocking RPC. The connection is closed and the handle destroyed
/// on drop.
pub struct ConductorClient {
    handle: ffi::mooncake_conductor_t,
}

// The C++ client serializes every call on an internal mutex, so the handle can
// be shared across threads. Drop takes exclusive ownership, so destroy never
// races another call (the only threading restriction of the C ABI).
unsafe impl Send for ConductorClient {}
unsafe impl Sync for ConductorClient {}

/// A C output struct released by its `*_free` function on drop, including on
/// early return. Outputs start zeroed, and freeing a zeroed (failed) output is
/// a no-op on the C side.
struct Output<T> {
    raw: T,
    free: unsafe extern "C" fn(*mut T),
}

impl<T: Default> Output<T> {
    fn new(free: unsafe extern "C" fn(*mut T)) -> Self {
        Self {
            raw: T::default(),
            free,
        }
    }
}

impl<T> Drop for Output<T> {
    fn drop(&mut self) {
        unsafe { (self.free)(&mut self.raw) };
    }
}

/// Borrow a C array. The C layer uses NULL for empty arrays, which
/// `slice::from_raw_parts` does not accept.
unsafe fn c_slice<'a, T>(data: *const T, count: usize) -> &'a [T] {
    if data.is_null() || count == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(data, count)
    }
}

/// Copy a C string. Output strings are never NULL on success; NULL maps to "".
unsafe fn c_string(s: *const c_char) -> String {
    if s.is_null() {
        String::new()
    } else {
        CStr::from_ptr(s).to_string_lossy().into_owned()
    }
}

unsafe fn hash_profile(raw: &ffi::mooncake_conductor_hash_profile_t) -> HashProfile {
    HashProfile {
        strategy: c_string(raw.strategy),
        algorithm: c_string(raw.algorithm),
        python_hash_seed: c_string(raw.python_hash_seed),
        root_digest: c_string(raw.root_digest),
        index_projection: c_string(raw.index_projection),
    }
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

impl ConductorClient {
    /// Create a client. Call [`setup`](Self::setup) before any operation.
    pub fn new() -> Result<Self, ConductorError> {
        let handle = unsafe { ffi::mooncake_conductor_create() };
        if handle.is_null() {
            return Err(ConductorError::NullHandle);
        }
        Ok(Self { handle })
    }

    /// Connect to the conductor RPC address (`host:port`). May be called
    /// again after [`close`](Self::close).
    pub fn setup(&self, config: &ClientConfig) -> Result<(), ConductorError> {
        let addr = CString::new(config.conductor_addr.as_str())?;
        check(unsafe {
            ffi::mooncake_conductor_setup(
                self.handle,
                addr.as_ptr(),
                millis(config.connect_timeout),
                millis(config.request_timeout),
            )
        })
    }

    /// Close the connection. Idempotent.
    pub fn close(&self) {
        unsafe { ffi::mooncake_conductor_close(self.handle) };
    }

    /// `NotInitialized` until a setup has succeeded, then `Healthy` if a
    /// `ListServices` round trip succeeds and `Unreachable` otherwise.
    pub fn health_check(&self) -> HealthStatus {
        match unsafe { ffi::mooncake_conductor_health_check(self.handle) } {
            HEALTH_OK => HealthStatus::Healthy,
            HEALTH_NOT_INITIALIZED => HealthStatus::NotInitialized,
            _ => HealthStatus::Unreachable,
        }
    }

    /// Query prefix-cache hits for `request.token_ids`, keyed by instance id.
    pub fn query(&self, request: &QueryRequest<'_>) -> Result<QueryResult, ConductorError> {
        let model_name = CString::new(request.model_name)?;
        let lora_name = CString::new(request.lora_name)?;
        let tenant_id = CString::new(request.tenant_id)?;
        let cache_salt = CString::new(request.cache_salt)?;
        let instance_filter = CString::new(request.instance_filter)?;
        let raw_request = ffi::mooncake_conductor_query_request_t {
            model_name: model_name.as_ptr(),
            lora_name: lora_name.as_ptr(),
            block_size: request.block_size,
            tenant_id: tenant_id.as_ptr(),
            token_ids: request.token_ids.as_ptr(),
            token_count: request.token_ids.len(),
            cache_salt: cache_salt.as_ptr(),
            instance_filter: instance_filter.as_ptr(),
        };

        let mut out = Output::new(ffi::mooncake_conductor_query_result_free);
        check(unsafe { ffi::mooncake_conductor_query(self.handle, &raw_request, &mut out.raw) })?;

        let mut result = QueryResult::default();
        for hit in unsafe { c_slice(out.raw.hits, out.raw.hit_count) } {
            let dp = unsafe { c_slice(hit.dp, hit.dp_count) }
                .iter()
                .map(|entry| (entry.rank, entry.tokens))
                .collect();
            let rank_matches = unsafe { c_slice(hit.rank_matches, hit.rank_match_count) }
                .iter()
                .map(|m| {
                    let tiers = RankMatch {
                        device: m.device,
                        cpu_local: m.cpu_local,
                        cpu_share: m.cpu_share,
                        disk: m.disk,
                    };
                    (m.rank, tiers)
                })
                .collect();
            let cache_hit = CacheHit {
                longest_matched: hit.longest_matched,
                dp,
                rank_matches,
                device: hit.device,
                cpu_local: hit.cpu_local,
                cpu_share: hit.cpu_share,
                disk: hit.disk,
            };
            result
                .hits
                .insert(unsafe { c_string(hit.instance_id) }, cache_hit);
        }
        Ok(result)
    }

    /// Register a KV-event publisher service. `is_new` is false when the same
    /// service was already registered.
    pub fn register(&self, config: &ServiceConfig) -> Result<RegisterResult, ConductorError> {
        let instance_id = CString::new(config.instance_id.as_str())?;
        let endpoint = CString::new(config.endpoint.as_str())?;
        let replay_endpoint = CString::new(config.replay_endpoint.as_str())?;
        let model_name = CString::new(config.model_name.as_str())?;
        let lora_name = CString::new(config.lora_name.as_str())?;
        let tenant_id = CString::new(config.tenant_id.as_str())?;
        let profile = &config.hash_profile;
        let strategy = CString::new(profile.strategy.as_str())?;
        let algorithm = CString::new(profile.algorithm.as_str())?;
        let python_hash_seed = CString::new(profile.python_hash_seed.as_str())?;
        let index_projection = CString::new(profile.index_projection.as_str())?;
        let raw_config = ffi::mooncake_conductor_service_config_t {
            instance_id: instance_id.as_ptr(),
            endpoint: endpoint.as_ptr(),
            replay_endpoint: replay_endpoint.as_ptr(),
            publisher_kind: config.publisher_kind.to_raw(),
            model_name: model_name.as_ptr(),
            lora_name: lora_name.as_ptr(),
            tenant_id: tenant_id.as_ptr(),
            block_size: config.block_size,
            dp_rank: config.dp_rank,
            has_cache_group: i32::from(config.cache_group.is_some()),
            cache_group: config.cache_group.unwrap_or(0),
            hash_profile: ffi::mooncake_conductor_hash_profile_config_t {
                strategy: strategy.as_ptr(),
                algorithm: algorithm.as_ptr(),
                python_hash_seed: python_hash_seed.as_ptr(),
                index_projection: index_projection.as_ptr(),
            },
        };

        let mut is_new: i32 = 0;
        check(unsafe { ffi::mooncake_conductor_register(self.handle, &raw_config, &mut is_new) })?;
        Ok(RegisterResult {
            is_new: is_new != 0,
        })
    }

    /// Unregister a service. An empty `tenant_id` means `"default"`.
    pub fn unregister(
        &self,
        instance_id: &str,
        tenant_id: &str,
        dp_rank: i32,
    ) -> Result<(), ConductorError> {
        let instance_id = CString::new(instance_id)?;
        let tenant_id = CString::new(tenant_id)?;
        check(unsafe {
            ffi::mooncake_conductor_unregister(
                self.handle,
                instance_id.as_ptr(),
                tenant_id.as_ptr(),
                dp_rank,
            )
        })
    }

    /// Snapshot of every indexed context and the instances serving it.
    pub fn get_global_view(&self) -> Result<GlobalView, ConductorError> {
        let mut out = Output::new(ffi::mooncake_conductor_global_view_free);
        check(unsafe { ffi::mooncake_conductor_get_global_view(self.handle, &mut out.raw) })?;

        let mut view = GlobalView::default();
        for context in unsafe { c_slice(out.raw.contexts, out.raw.context_count) } {
            let instances = unsafe { c_slice(context.instances, context.instance_count) }
                .iter()
                .map(|instance| unsafe {
                    let ranks = c_slice(instance.ranks, instance.rank_count);
                    (
                        c_string(instance.instance_id),
                        ranks.iter().copied().collect(),
                    )
                })
                .collect();
            view.contexts.push(unsafe {
                ContextView {
                    model_name: c_string(context.model_name),
                    lora_name: c_string(context.lora_name),
                    block_size: context.block_size,
                    tenant_id: c_string(context.tenant_id),
                    prefix_count: context.prefix_count,
                    hash_profile: hash_profile(&context.hash_profile),
                    instances,
                }
            });
        }
        Ok(view)
    }

    /// Every service registered with the conductor.
    pub fn list_services(&self) -> Result<Vec<ServiceInfo>, ConductorError> {
        let mut out = Output::new(ffi::mooncake_conductor_service_list_free);
        check(unsafe { ffi::mooncake_conductor_list_services(self.handle, &mut out.raw) })?;

        unsafe { c_slice(out.raw.services, out.raw.service_count) }
            .iter()
            .map(|service| {
                // The C layer only emits values of the publisher-kind enum.
                let publisher_kind = PublisherKind::from_raw(service.publisher_kind)
                    .ok_or(ConductorError::Internal)?;
                Ok(unsafe {
                    ServiceInfo {
                        instance_id: c_string(service.instance_id),
                        endpoint: c_string(service.endpoint),
                        replay_endpoint: c_string(service.replay_endpoint),
                        publisher_kind,
                        model_name: c_string(service.model_name),
                        lora_name: c_string(service.lora_name),
                        tenant_id: c_string(service.tenant_id),
                        block_size: service.block_size,
                        dp_rank: service.dp_rank,
                        cache_group: (service.has_cache_group != 0).then_some(service.cache_group),
                        hash_profile: hash_profile(&service.hash_profile),
                    }
                })
            })
            .collect()
    }
}

impl Drop for ConductorClient {
    fn drop(&mut self) {
        unsafe { ffi::mooncake_conductor_destroy(self.handle) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_values_match_c_header() {
        assert_eq!(
            i64::from(ffi::MOONCAKE_CONDUCTOR_HEALTH_OK),
            i64::from(HEALTH_OK)
        );
        assert_eq!(
            i64::from(ffi::MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED),
            i64::from(HEALTH_NOT_INITIALIZED)
        );
    }

    #[test]
    fn millis_saturates() {
        assert_eq!(millis(Duration::from_millis(1500)), 1500);
        assert_eq!(millis(Duration::MAX), i64::MAX);
    }

    #[test]
    fn client_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ConductorClient>();
    }
}
