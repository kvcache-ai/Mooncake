use mooncake_store_core::{
    ClientRuntimeId, RoutePolicy, TenantObjectAccounting, TenantPolicy, TenantPolicyScope,
    TenantPolicySpec, TenantQuotaReservation, TenantQuotaReservationState, TenantQuotaState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TracingUpdateRequest {
    pub endpoint: Option<String>,
    pub file: Option<String>,
    pub clear_file: Option<bool>,
    pub sample_ratio: Option<f64>,
    pub timeout_ms: Option<u64>,
    pub max_targets: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TracingAction {
    Status,
    On,
    Off,
    Flush,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TracingNodeResponse {
    pub runtime: ClientRuntimeId,
    pub metrics_url: String,
    pub ok: bool,
    pub status: Option<Value>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TracingClusterResponse {
    pub action: TracingAction,
    pub total: usize,
    pub ok: usize,
    pub failed: usize,
    pub nodes: Vec<TracingNodeResponse>,
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

pub use mooncake_store_core::{ColdTierDeviceState, ColdTierTargetSpec};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateColdTierDeviceRequest {
    pub stable_id: String,
    pub cold_tier_id: String,
    pub kind: String,
    pub target: ColdTierTargetSpec,
    pub capacity_override_bytes: Option<u64>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierDeviceResponse {
    pub device_id: String,
    pub stable_id: String,
    pub epoch: Option<u64>,
    pub cold_tier_id: String,
    pub kind: String,
    pub target: ColdTierTargetSpec,
    pub root_dir: Option<String>,
    pub state: ColdTierDeviceState,
    pub schedulable: bool,
    pub capacity_bytes: Option<u64>,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub free_bytes: Option<u64>,
    pub failure_count: u64,
    pub last_error: Option<String>,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CreateColdTierDeviceResponse {
    #[serde(flatten)]
    pub device: ColdTierDeviceResponse,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListColdTierDevicesResponse {
    pub devices: Vec<ColdTierDeviceResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TriggerColdTierOffloadRequest {
    pub stable_id: String,
    pub max_tasks: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierOffloadTaskState {
    Pending,
    Running,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TriggerColdTierOffloadResponse {
    pub task_id: String,
    pub stable_id: String,
    pub epoch: Option<u64>,
    pub max_tasks: u64,
    pub batch_size: u64,
    pub state: ColdTierOffloadTaskState,
    pub materialized: u64,
    pub batches: u64,
    pub last_error: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListColdTierOffloadTasksResponse {
    pub count: usize,
    pub tasks: Vec<TriggerColdTierOffloadResponse>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdTierRegisterRequest {
    #[serde(default = "default_true")]
    pub scan_existing: bool,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierRegisterResponse {
    pub device_id: String,
    pub stable_id: String,
    pub epoch: Option<u64>,
    pub cold_tier_id: String,
    pub state: ColdTierDeviceState,
    pub schedulable: bool,
    pub dry_run: bool,
    pub scanned_objects: u64,
    pub used_bytes: u64,
    pub message: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdTierUnregisterRequest {
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierUnregisterResponse {
    pub device_id: String,
    pub stable_id: String,
    pub cold_tier_id: String,
    pub state: ColdTierDeviceState,
    pub schedulable: bool,
    pub dry_run: bool,
    pub blocked_objects: u64,
    pub inflight_operations: u64,
    pub message: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdTierDisableRequest {
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdTierEnableRequest {}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierStateChangeResponse {
    pub device_id: String,
    pub state: ColdTierDeviceState,
    pub schedulable: bool,
    pub message: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColdTierDrainRequest {
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub migration_task_executor: Option<String>,
    #[serde(default)]
    pub migration_target_segments: Vec<String>,
    #[serde(default)]
    pub migration_max_retries: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierBlockersResponse {
    pub device_id: String,
    pub state: ColdTierDeviceState,
    pub schedulable: bool,
    pub blocked_objects: u64,
    pub reclaimable_objects: u64,
    pub pending_delete_objects: u64,
    pub inflight_operations: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierDrainResponse {
    pub device_id: String,
    pub state: ColdTierDeviceState,
    pub schedulable: bool,
    pub dry_run: bool,
    pub blocked_objects: u64,
    pub reclaimable_objects: u64,
    pub marked_pending_delete: u64,
    pub collected_pending_delete: u64,
    pub migration_tasks_submitted: u64,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierManualGcResponse {
    pub device_id: String,
    pub pending_delete_objects: u64,
    pub collected_objects: u64,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierManualFreeResponse {
    pub device_id: String,
    pub inflight_operations: u64,
    pub attempted_victims: u64,
    pub freed_backings: u64,
    pub collected_backings: u64,
    pub skipped_backings: u64,
    pub reached_low_watermark: bool,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierObjectBackingOwnerResponse {
    pub stable_id: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierObjectBackingResponse {
    pub device_id: String,
    pub cold_tier_id: String,
    pub owner: ColdTierObjectBackingOwnerResponse,
    pub state: String,
    pub locator: String,
    pub length: u64,
    pub checksum: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetColdTierObjectResponse {
    pub key: String,
    pub cold_backing: Option<ColdTierObjectBackingResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DebugRouteReplicaResponse {
    pub owner_stable_id: String,
    pub owner_epoch: u64,
    pub segment_name: String,
    pub segment_offset: u64,
    pub length: u64,
    pub checksum: Option<u64>,
    pub tier: String,
    pub priority: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DebugRouteNamespaceResponse {
    pub tenant: String,
    pub domain: String,
    pub object_set: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DebugRouteResponse {
    pub key: String,
    pub version: u64,
    pub state: String,
    pub namespace: Option<DebugRouteNamespaceResponse>,
    pub logical_key: Option<String>,
    pub canonical_key: Option<String>,
    pub sharing_scope: Option<String>,
    pub qos_tier: Option<String>,
    pub replicas: Vec<DebugRouteReplicaResponse>,
    pub cold_backing: Option<ColdTierObjectBackingResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DebugRouteListResponse {
    pub count: usize,
    pub routes: Vec<DebugRouteResponse>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierQuarantineFile {
    pub area: String,
    pub file_name: String,
    pub encoded_path: String,
    pub size_bytes: u64,
    pub modified_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierQuarantineReport {
    pub device_id: String,
    pub cold_tier_id: String,
    pub root_dir: String,
    pub count: usize,
    pub total_bytes: u64,
    pub files: Vec<ColdTierQuarantineFile>,
}

fn default_true() -> bool {
    true
}

fn default_updated_by() -> String {
    "admin".to_string()
}
