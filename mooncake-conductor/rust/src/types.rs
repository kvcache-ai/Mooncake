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

//! Request and result types. Field names follow the Python API
//! (`mooncake.conductor`); results are owned copies of the C output.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// Connection settings for [`crate::ConductorClient::setup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// RPC server address, `"host:port"`.
    pub conductor_addr: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl ClientConfig {
    /// Timeouts default to the Python client's: 1 s connect, 3 s per request.
    pub fn new(conductor_addr: impl Into<String>) -> Self {
        Self {
            conductor_addr: conductor_addr.into(),
            connect_timeout: Duration::from_millis(1000),
            request_timeout: Duration::from_millis(3000),
        }
    }
}

/// A prefix-cache hit query. Fields are borrowed so token arrays are not
/// copied before crossing the C boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRequest<'a> {
    pub model_name: &'a str,
    pub lora_name: &'a str,
    pub block_size: i64,
    /// Empty normalizes to `"default"`.
    pub tenant_id: &'a str,
    pub token_ids: &'a [i32],
    /// Empty means unset.
    pub cache_salt: &'a str,
    /// Restrict hits to one instance; empty means all instances.
    pub instance_filter: &'a str,
}

impl<'a> QueryRequest<'a> {
    /// The remaining fields default as in the Python API: no LoRA, tenant
    /// `"default"`, no cache salt, no instance filter.
    pub fn new(model_name: &'a str, block_size: i64, token_ids: &'a [i32]) -> Self {
        Self {
            model_name,
            lora_name: "",
            block_size,
            tenant_id: "default",
            token_ids,
            cache_salt: "",
            instance_filter: "",
        }
    }

    pub fn with_lora_name(mut self, lora_name: &'a str) -> Self {
        self.lora_name = lora_name;
        self
    }

    pub fn with_tenant_id(mut self, tenant_id: &'a str) -> Self {
        self.tenant_id = tenant_id;
        self
    }

    pub fn with_cache_salt(mut self, cache_salt: &'a str) -> Self {
        self.cache_salt = cache_salt;
        self
    }

    pub fn with_instance_filter(mut self, instance_filter: &'a str) -> Self {
        self.instance_filter = instance_filter;
        self
    }
}

/// Per-rank hit lengths (in tokens) on each storage tier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RankMatch {
    pub device: i64,
    pub cpu_local: i64,
    pub cpu_share: i64,
    pub disk: i64,
}

/// Hits for one instance. Tier fields are the maximum over ranks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheHit {
    pub longest_matched: i64,
    /// DP rank -> tokens matched on that rank's device tier.
    pub dp: BTreeMap<i64, i64>,
    pub rank_matches: BTreeMap<i64, RankMatch>,
    pub device: i64,
    pub cpu_local: i64,
    pub cpu_share: i64,
    pub disk: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryResult {
    /// instance_id -> hit.
    pub hits: BTreeMap<String, CacheHit>,
}

/// KV-event publisher type of a registered service.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum PublisherKind {
    #[default]
    Vllm,
    Mooncake,
    Sglang,
}

impl PublisherKind {
    /// The `/services` wire name (`"vLLM"`, `"Mooncake"`, `"SGLang"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vllm => "vLLM",
            Self::Mooncake => "Mooncake",
            Self::Sglang => "SGLang",
        }
    }

    // Values of `enum mooncake_conductor_publisher_kind`.
    pub(crate) fn to_raw(self) -> i32 {
        match self {
            Self::Vllm => 0,
            Self::Mooncake => 1,
            Self::Sglang => 2,
        }
    }

    pub(crate) fn from_raw(raw: i32) -> Option<Self> {
        match raw {
            0 => Some(Self::Vllm),
            1 => Some(Self::Mooncake),
            2 => Some(Self::Sglang),
            _ => None,
        }
    }
}

/// Hash recipe a service registers with; the server derives `root_digest`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashProfileConfig {
    pub strategy: String,
    pub algorithm: String,
    pub python_hash_seed: String,
    pub index_projection: String,
}

/// A resolved hash profile as reported by the server.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HashProfile {
    pub strategy: String,
    pub algorithm: String,
    pub python_hash_seed: String,
    pub root_digest: String,
    pub index_projection: String,
}

/// A serving instance to register. Required: instance_id, endpoint,
/// model_name, block_size, hash_profile (as in the Python API).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceConfig {
    pub instance_id: String,
    /// ZMQ endpoint the conductor subscribes to.
    pub endpoint: String,
    pub replay_endpoint: String,
    pub publisher_kind: PublisherKind,
    pub model_name: String,
    pub lora_name: String,
    /// Empty normalizes to `"default"` on the server.
    pub tenant_id: String,
    pub block_size: i64,
    pub dp_rank: i32,
    /// `None` keeps the server default (only cache group 0 is supported).
    pub cache_group: Option<i64>,
    pub hash_profile: HashProfileConfig,
}

impl ServiceConfig {
    pub fn new(
        instance_id: impl Into<String>,
        endpoint: impl Into<String>,
        model_name: impl Into<String>,
        block_size: i64,
        hash_profile: HashProfileConfig,
    ) -> Self {
        Self {
            instance_id: instance_id.into(),
            endpoint: endpoint.into(),
            model_name: model_name.into(),
            block_size,
            hash_profile,
            ..Self::default()
        }
    }
}

/// A registered service as reported by `list_services`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServiceInfo {
    pub instance_id: String,
    pub endpoint: String,
    pub replay_endpoint: String,
    pub publisher_kind: PublisherKind,
    pub model_name: String,
    pub lora_name: String,
    pub tenant_id: String,
    pub block_size: i64,
    pub dp_rank: i32,
    pub cache_group: Option<i64>,
    pub hash_profile: HashProfile,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegisterResult {
    /// False when an identical service was already registered.
    pub is_new: bool,
}

/// One indexed context (model, LoRA, block size, tenant).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContextView {
    pub model_name: String,
    pub lora_name: String,
    pub block_size: i64,
    pub tenant_id: String,
    pub prefix_count: u64,
    pub hash_profile: HashProfile,
    /// instance_id -> DP ranks.
    pub instances: BTreeMap<String, BTreeSet<i64>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GlobalView {
    pub contexts: Vec<ContextView>,
}

/// Result of [`crate::ConductorClient::health_check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// The conductor answered a `list_services` probe.
    Healthy,
    /// `setup` has never succeeded on this client.
    NotInitialized,
    /// `setup` succeeded once, but the conductor does not answer now
    /// (including after `close`).
    Unreachable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi;

    #[test]
    fn publisher_kind_matches_c_header() {
        for (kind, c_value) in [
            (
                PublisherKind::Vllm,
                i64::from(ffi::MOONCAKE_CONDUCTOR_PUBLISHER_VLLM),
            ),
            (
                PublisherKind::Mooncake,
                i64::from(ffi::MOONCAKE_CONDUCTOR_PUBLISHER_MOONCAKE),
            ),
            (
                PublisherKind::Sglang,
                i64::from(ffi::MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG),
            ),
        ] {
            assert_eq!(i64::from(kind.to_raw()), c_value);
            assert_eq!(PublisherKind::from_raw(kind.to_raw()), Some(kind));
        }
        assert_eq!(PublisherKind::from_raw(42), None);
        assert_eq!(PublisherKind::Sglang.as_str(), "SGLang");
    }

    #[test]
    fn query_request_defaults_follow_python_api() {
        let tokens = [1, 2, 3];
        let request = QueryRequest::new("m", 16, &tokens).with_instance_filter("i");
        assert_eq!(request.tenant_id, "default");
        assert_eq!(request.lora_name, "");
        assert_eq!(request.cache_salt, "");
        assert_eq!(request.instance_filter, "i");
    }
}
