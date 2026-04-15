use mooncake_store_client::RouteControlMode;
use mooncake_store_core::{RoutePolicy, TenantPolicy, TenantPolicyScope, TenantPolicySpec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyPatchInput {
    pub route_topk: Option<u32>,
    pub route_control: Option<RouteControlMode>,
    pub max_bytes: Option<u64>,
    pub max_objects: Option<usize>,
    pub max_remote_batch_items_per_tenant: Option<usize>,
    pub max_remote_batch_bytes: Option<usize>,
    pub max_remote_batch_burst_items: Option<usize>,
    pub max_inflight_bytes_per_batch: Option<u64>,
    pub default_replica_count: Option<usize>,
    pub prefer_local: Option<bool>,
    pub prefer_alloc_in_same_node: Option<bool>,
    pub preferred_storage_owners: Option<Vec<String>>,
    pub preferred_segments: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetTenantPolicyResponse {
    pub scope: TenantPolicyScope,
    pub effective: bool,
    pub found: bool,
    pub policy: Option<TenantPolicy>,
    pub effective_spec: Option<TenantPolicySpec>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeleteTenantPolicyResponse {
    pub scope: TenantPolicyScope,
    pub removed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PutTenantPolicyRequest {
    #[serde(flatten)]
    pub patch: PolicyPatchInput,
    pub expected_version: Option<u64>,
    #[serde(default = "default_updated_by")]
    pub updated_by: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdminCleanupReport {
    pub live_clients: usize,
    pub inspected_segment_keys: usize,
    pub removed_segment_keys: usize,
    pub removed_segment_index_entries: usize,
    pub removed_owner_segment_index_entries: usize,
    pub stale_missing_segment_index_entries: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RoutePolicyResponse {
    pub domain: String,
    pub found: bool,
    pub policy: Option<RoutePolicy>,
}

fn default_updated_by() -> String {
    "admin".to_string()
}
