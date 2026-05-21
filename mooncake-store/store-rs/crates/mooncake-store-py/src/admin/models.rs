use mooncake_store_core::{
    ClientRuntimeId, RoutePolicy, TenantObjectAccounting, TenantPolicy, TenantPolicyScope,
    TenantPolicySpec, TenantQuotaReservation, TenantQuotaReservationState, TenantQuotaState,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaReconcileAction {
    pub reservation_id: String,
    pub key: String,
    pub action: String,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaReconcileReport {
    pub scope: TenantPolicyScope,
    pub dry_run: bool,
    pub inspected: usize,
    pub finalized: usize,
    pub aborted: usize,
    pub skipped: usize,
    pub actions: Vec<TenantQuotaReconcileAction>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaReconcileRequest {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaAbortRequest {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaAbortResponse {
    pub scope: TenantPolicyScope,
    pub reservation_id: String,
    pub dry_run: bool,
    pub aborted: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyPatchInput {
    pub route_topk: Option<u32>,
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
pub enum AdminOwnerCleanupState {
    SkippedLive,
    CleanedMissingLease,
    CleanedExpiredLease,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdminOwnerCleanupReport {
    pub owner: ClientRuntimeId,
    pub state: AdminOwnerCleanupState,
    pub cleanup: AdminCleanupReport,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AdminMaintenanceReport {
    pub due_entries: usize,
    pub invalid_entries: usize,
    pub cleaned_missing_lease: usize,
    pub cleaned_expired_lease: usize,
    pub skipped_live: usize,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetTenantQuotaStateResponse {
    pub scope: TenantPolicyScope,
    pub found: bool,
    pub state: Option<TenantQuotaState>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetTenantObjectAccountingResponse {
    pub scope: TenantPolicyScope,
    pub key: String,
    pub found: bool,
    pub accounting: Option<TenantObjectAccounting>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReservationFilterInput {
    pub state: Option<TenantQuotaReservationState>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListTenantQuotaReservationsResponse {
    pub scope: TenantPolicyScope,
    pub count: usize,
    pub reservations: Vec<TenantQuotaReservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteMigrationMode {
    Copy,
    Move,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteMigrationTaskState {
    Pending,
    Dispatching,
    Running,
    RetryWait,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteMigrationTaskSubmitRequest {
    pub authority: String,
    pub tenant: String,
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub object_set: Option<String>,
    pub key: String,
    pub source_segment: String,
    pub target_segments: Vec<String>,
    pub task_executor: String,
    pub max_retries: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteMigrationTaskStatusResponse {
    pub task_id: String,
    pub namespace: String,
    pub authority: String,
    pub tenant: String,
    pub domain: Option<String>,
    pub object_set: Option<String>,
    pub key: String,
    pub mode: RouteMigrationMode,
    pub source_segment: String,
    pub target_segments: Vec<String>,
    pub task_executor: String,
    pub state: RouteMigrationTaskState,
    pub attempts: u32,
    pub max_retries: u32,
    pub execution_id: Option<String>,
    pub next_retry_at_ms: Option<u64>,
    pub last_error: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteMigrationTaskListResponse {
    pub count: usize,
    pub tasks: Vec<RouteMigrationTaskStatusResponse>,
}

fn default_updated_by() -> String {
    "admin".to_string()
}
