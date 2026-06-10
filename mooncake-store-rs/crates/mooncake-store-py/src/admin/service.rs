use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_metadata::{
    ClientLeaseLiveness, EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace,
    RedisMetadataBackend, RedisMetadataCleanupReport, RedisMetadataConfig,
};
use mooncake_store_client::{
    control_plane_pb, record_tenant_quota_reconcile, MigrationControlClient,
};
use mooncake_store_core::{
    route_logical_object_id, ClientEpoch, ClientLease, ClientRuntimeId, ClientStableId,
    LogicalObjectId, MetadataBackend, NamespaceScope, ObjectKey, ObjectRoute, RoutePolicyDomain,
    StoreError, TenantBandwidthShapingPolicy, TenantExecutionFairnessPolicy,
    TenantObjectAccountingState, TenantPlacementPolicy, TenantPolicy, TenantPolicyScope,
    TenantPolicySpec, TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservationState,
    TenantRoutePolicy, CONTROL_ADDR_LABEL, DEFAULT_DOMAIN, DEFAULT_OBJECT_SET, METRICS_PORT_LABEL,
};
use parking_lot::Mutex;
use url::Url;

use crate::config::build_store_metadata_backend;

use super::models::{
    AdminCleanupReport, AdminMaintenanceReport, AdminOwnerCleanupReport, AdminOwnerCleanupState,
    DeleteTenantPolicyResponse, GetTenantObjectAccountingResponse, GetTenantPolicyResponse,
    GetTenantQuotaStateResponse, ListTenantQuotaReservationsResponse, PolicyPatchInput,
    RouteMigrationMode, RouteMigrationTaskListResponse, RouteMigrationTaskState,
    RouteMigrationTaskStatusResponse, RouteMigrationTaskSubmitRequest, RoutePolicyResponse,
    TenantQuotaAbortResponse, TenantQuotaReconcileAction, TenantQuotaReconcileReport,
    TracingAction, TracingClusterResponse, TracingNodeResponse, TracingUpdateRequest,
};

pub type AdminResult<T> = mooncake_store_core::Result<T>;

const DEFAULT_MIGRATION_MAX_RETRIES: u32 = 5;
const DEFAULT_MIGRATION_RETRY_BASE_DELAY: Duration = Duration::from_secs(3);
const DEFAULT_MIGRATION_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const DEFAULT_MIGRATION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const MIGRATION_MAX_RETRIES_ENV: &str = "MC_STORE_ADMIN_MIGRATION_MAX_RETRIES";
const MIGRATION_RETRY_BASE_DELAY_MS_ENV: &str = "MC_STORE_ADMIN_MIGRATION_RETRY_BASE_DELAY_MS";
const MIGRATION_RETRY_MAX_DELAY_MS_ENV: &str = "MC_STORE_ADMIN_MIGRATION_RETRY_MAX_DELAY_MS";
const MIGRATION_POLL_INTERVAL_MS_ENV: &str = "MC_STORE_ADMIN_MIGRATION_POLL_INTERVAL_MS";
const DEFAULT_STORE_METRICS_PORT: u16 = 9300;
const DEFAULT_TRACING_FANOUT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_TRACING_MAX_TARGETS: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TracingTarget {
    pub(crate) runtime: ClientRuntimeId,
    pub(crate) metrics_url: String,
    host: String,
    port: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationQueueConfig {
    pub default_max_retries: u32,
    pub retry_base_delay: Duration,
    pub retry_max_delay: Duration,
    pub poll_interval: Duration,
}

impl Default for MigrationQueueConfig {
    fn default() -> Self {
        Self {
            default_max_retries: DEFAULT_MIGRATION_MAX_RETRIES,
            retry_base_delay: DEFAULT_MIGRATION_RETRY_BASE_DELAY,
            retry_max_delay: DEFAULT_MIGRATION_RETRY_MAX_DELAY,
            poll_interval: DEFAULT_MIGRATION_POLL_INTERVAL,
        }
    }
}

impl MigrationQueueConfig {
    fn from_env() -> AdminResult<Self> {
        let mut config = Self::default();
        if let Some(value) = parse_env_u32(MIGRATION_MAX_RETRIES_ENV)? {
            config.default_max_retries = value.max(1);
        }
        if let Some(value) = parse_env_duration_ms(MIGRATION_RETRY_BASE_DELAY_MS_ENV)? {
            config.retry_base_delay = value.max(Duration::from_millis(1));
        }
        if let Some(value) = parse_env_duration_ms(MIGRATION_RETRY_MAX_DELAY_MS_ENV)? {
            config.retry_max_delay = value.max(config.retry_base_delay);
        }
        if let Some(value) = parse_env_duration_ms(MIGRATION_POLL_INTERVAL_MS_ENV)? {
            config.poll_interval = value.max(Duration::from_millis(1));
        }
        Ok(config)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MigrationExecutionProbe {
    pub(crate) state: control_plane_pb::MigrationExecutionState,
    pub(crate) attempts: u32,
    pub(crate) last_error: String,
}

pub(crate) trait MigrationRpc: Send + Sync {
    fn submit_migration_task(
        &self,
        lease: &ClientLease,
        request: control_plane_pb::SubmitMigrationTaskRequest,
    ) -> AdminResult<String>;

    fn get_migration_execution_status(
        &self,
        lease: &ClientLease,
        request: control_plane_pb::GetMigrationExecutionStatusRequest,
    ) -> AdminResult<MigrationExecutionProbe>;

    fn get_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> AdminResult<Option<ObjectRoute>>;
}

struct ControlPlaneMigrationRpc;

impl MigrationRpc for ControlPlaneMigrationRpc {
    fn submit_migration_task(
        &self,
        lease: &ClientLease,
        request: control_plane_pb::SubmitMigrationTaskRequest,
    ) -> AdminResult<String> {
        MigrationControlClient::new()?.submit_migration_task(lease, request)
    }

    fn get_migration_execution_status(
        &self,
        lease: &ClientLease,
        request: control_plane_pb::GetMigrationExecutionStatusRequest,
    ) -> AdminResult<MigrationExecutionProbe> {
        let reply =
            MigrationControlClient::new()?.get_migration_execution_status_detail(lease, request)?;
        let state =
            control_plane_pb::MigrationExecutionState::try_from(reply.state).map_err(|_| {
                StoreError::Transport(format!(
                    "admin migration status reply has invalid execution state: {}",
                    reply.state
                ))
            })?;
        Ok(MigrationExecutionProbe {
            state,
            attempts: reply.attempts,
            last_error: reply.last_error,
        })
    }

    fn get_route(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> AdminResult<Option<ObjectRoute>> {
        MigrationControlClient::new()?.get_route(lease, namespace, authority, key)
    }
}

#[derive(Clone, Debug)]
struct RouteMigrationTaskRecord {
    task_id: String,
    namespace: String,
    authority: String,
    tenant: String,
    domain: Option<String>,
    object_set: Option<String>,
    key: String,
    mode: RouteMigrationMode,
    source_segment: String,
    target_segments: Vec<String>,
    task_executor: String,
    state: RouteMigrationTaskState,
    attempts: u32,
    status_failures: u32,
    max_retries: u32,
    execution_id: Option<String>,
    next_retry_at_ms: Option<u64>,
    last_error: String,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl RouteMigrationTaskRecord {
    fn to_response(&self) -> RouteMigrationTaskStatusResponse {
        RouteMigrationTaskStatusResponse {
            task_id: self.task_id.clone(),
            namespace: self.namespace.clone(),
            authority: self.authority.clone(),
            tenant: self.tenant.clone(),
            domain: self.domain.clone(),
            object_set: self.object_set.clone(),
            key: self.key.clone(),
            mode: self.mode,
            source_segment: self.source_segment.clone(),
            target_segments: self.target_segments.clone(),
            task_executor: self.task_executor.clone(),
            state: self.state,
            attempts: self.attempts,
            max_retries: self.max_retries,
            execution_id: self.execution_id.clone(),
            next_retry_at_ms: self.next_retry_at_ms,
            last_error: self.last_error.clone(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

#[derive(Clone)]
struct MigrationTaskManager {
    state: Arc<MigrationTaskManagerState>,
}

struct MigrationTaskManagerState {
    backend: Arc<dyn MetadataBackend>,
    rpc: Arc<dyn MigrationRpc>,
    config: MigrationQueueConfig,
    next_task_id: AtomicU64,
    tasks: Mutex<BTreeMap<String, RouteMigrationTaskRecord>>,
}

#[derive(Clone, Debug)]
enum RouteCompletionCheck {
    Pending,
    Completed,
    Conflict(String),
}

impl MigrationTaskManager {
    fn new(
        backend: Arc<dyn MetadataBackend>,
        rpc: Arc<dyn MigrationRpc>,
        config: MigrationQueueConfig,
    ) -> Self {
        let state = Arc::new(MigrationTaskManagerState {
            backend,
            rpc,
            config,
            next_task_id: AtomicU64::new(1),
            tasks: Mutex::new(BTreeMap::new()),
        });
        let weak = Arc::downgrade(&state);
        let poll_interval = state.config.poll_interval.max(Duration::from_millis(1));
        let _ = thread::Builder::new()
            .name("mooncake-admin-migration".to_string())
            .spawn(move || {
                while let Some(state) = weak.upgrade() {
                    state.tick();
                    thread::sleep(poll_interval);
                }
            });
        Self { state }
    }

    fn submit(
        &self,
        mode: RouteMigrationMode,
        request: RouteMigrationTaskSubmitRequest,
    ) -> AdminResult<RouteMigrationTaskStatusResponse> {
        validate_route_migration_request(mode, &request)?;
        let now = now_ms();
        let sequence = self.state.next_task_id.fetch_add(1, Ordering::SeqCst);
        let task_id = format!("route-migration-{sequence}");
        let max_retries = request
            .max_retries
            .unwrap_or(self.state.config.default_max_retries)
            .max(1);
        let record = RouteMigrationTaskRecord {
            task_id: task_id.clone(),
            namespace: self.state.backend.route_namespace(),
            authority: request.authority,
            tenant: request.tenant,
            domain: request.domain,
            object_set: request.object_set,
            key: request.key,
            mode,
            source_segment: request.source_segment,
            target_segments: request.target_segments,
            task_executor: request.task_executor,
            state: RouteMigrationTaskState::Pending,
            attempts: 0,
            status_failures: 0,
            max_retries,
            execution_id: None,
            next_retry_at_ms: None,
            last_error: String::new(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let response = record.to_response();
        self.state.tasks.lock().insert(task_id, record);
        Ok(response)
    }

    fn get(&self, task_id: &str) -> AdminResult<RouteMigrationTaskStatusResponse> {
        self.state
            .tasks
            .lock()
            .get(task_id)
            .cloned()
            .map(|record| record.to_response())
            .ok_or_else(|| {
                StoreError::NotFound(format!("route migration task {task_id} was not found"))
            })
    }

    fn list(&self) -> RouteMigrationTaskListResponse {
        let mut tasks = self
            .state
            .tasks
            .lock()
            .values()
            .map(|record| record.to_response())
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.task_id.cmp(&right.task_id))
        });
        RouteMigrationTaskListResponse {
            count: tasks.len(),
            tasks,
        }
    }
}

impl MigrationTaskManagerState {
    fn tick(&self) {
        let now = now_ms();
        let task_ids = self.tasks.lock().keys().cloned().collect::<Vec<_>>();
        for task_id in task_ids {
            let action = {
                let tasks = self.tasks.lock();
                let Some(record) = tasks.get(&task_id).cloned() else {
                    continue;
                };
                match record.state {
                    RouteMigrationTaskState::Pending => Some(MigrationTaskAction::Dispatch(record)),
                    RouteMigrationTaskState::RetryWait
                        if record
                            .next_retry_at_ms
                            .is_some_and(|retry_at| retry_at <= now) =>
                    {
                        if record.execution_id.is_some() {
                            Some(MigrationTaskAction::Poll(record))
                        } else {
                            Some(MigrationTaskAction::Dispatch(record))
                        }
                    }
                    RouteMigrationTaskState::Dispatching | RouteMigrationTaskState::Running => {
                        Some(MigrationTaskAction::Poll(record))
                    }
                    _ => None,
                }
            };
            match action {
                Some(MigrationTaskAction::Dispatch(record)) => self.dispatch(record),
                Some(MigrationTaskAction::Poll(record)) => self.poll(record),
                None => {}
            }
        }
    }

    fn dispatch(&self, record: RouteMigrationTaskRecord) {
        let attempted_dispatches = record.attempts.saturating_add(1);
        let result = self
            .resolve_live_lease(&record.task_executor)
            .and_then(|lease| {
                self.rpc.submit_migration_task(
                    &lease,
                    control_plane_pb::SubmitMigrationTaskRequest {
                        namespace: record.namespace.clone(),
                        authority: record.authority.clone(),
                        tenant: record.tenant.clone(),
                        domain: record.domain.clone().unwrap_or_default(),
                        object_set: record.object_set.clone().unwrap_or_default(),
                        key: record.key.clone(),
                        mode: migration_mode_to_proto(record.mode) as i32,
                        source_segment: record.source_segment.clone(),
                        target_segments: record.target_segments.clone(),
                        task_executor: record.task_executor.clone(),
                        max_retries: record.max_retries as u64,
                    },
                )
            });
        match result {
            Ok(execution_id) => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Dispatching,
                    attempted_dispatches,
                    0,
                    Some(execution_id),
                    None,
                    String::new(),
                ),
            ),
            Err(error) => {
                self.handle_attempt_failure(record, attempted_dispatches, error.to_string())
            }
        }
    }

    fn poll(&self, record: RouteMigrationTaskRecord) {
        let Some(execution_id) = record.execution_id.clone() else {
            self.handle_execution_failure(record, "route migration task is missing execution_id");
            return;
        };
        let lease = match self.resolve_live_lease(&record.task_executor) {
            Ok(lease) => lease,
            Err(error) => {
                self.handle_execution_failure(record, &error.to_string());
                return;
            }
        };
        let result = self.rpc.get_migration_execution_status(
            &lease,
            control_plane_pb::GetMigrationExecutionStatusRequest {
                namespace: record.namespace.clone(),
                authority: record.authority.clone(),
                execution_id: execution_id.clone(),
            },
        );
        match result {
            Ok(status) => match status.state {
                control_plane_pb::MigrationExecutionState::Pending
                | control_plane_pb::MigrationExecutionState::Dispatching => self.update_task(
                    &record.task_id,
                    task_update(
                        RouteMigrationTaskState::Dispatching,
                        record.attempts.max(status.attempts),
                        0,
                        Some(execution_id),
                        None,
                        status.last_error,
                    ),
                ),
                control_plane_pb::MigrationExecutionState::Running
                | control_plane_pb::MigrationExecutionState::RetryWait => self.update_task(
                    &record.task_id,
                    task_update(
                        RouteMigrationTaskState::Running,
                        record.attempts.max(status.attempts),
                        0,
                        Some(execution_id),
                        None,
                        status.last_error,
                    ),
                ),
                control_plane_pb::MigrationExecutionState::Succeeded => self.update_task(
                    &record.task_id,
                    task_update(
                        RouteMigrationTaskState::Succeeded,
                        record.attempts.max(status.attempts),
                        0,
                        Some(execution_id),
                        None,
                        status.last_error,
                    ),
                ),
                control_plane_pb::MigrationExecutionState::Failed
                | control_plane_pb::MigrationExecutionState::Cancelled
                | control_plane_pb::MigrationExecutionState::Unspecified => {
                    let last_error = if status.last_error.is_empty() {
                        format!("task executor reported {:?}", status.state)
                    } else {
                        status.last_error
                    };
                    self.handle_execution_failure(record, &last_error);
                }
            },
            Err(error) => self.handle_status_query_failure(record, &error.to_string()),
        }
    }

    fn handle_status_query_failure(&self, record: RouteMigrationTaskRecord, error: &str) {
        let status_failures = record.status_failures.saturating_add(1);
        match self.check_route_completion(&record) {
            RouteCompletionCheck::Completed => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Succeeded,
                    record.attempts,
                    status_failures,
                    record.execution_id.clone(),
                    None,
                    String::new(),
                ),
            ),
            RouteCompletionCheck::Conflict(message) => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Failed,
                    record.attempts,
                    status_failures,
                    record.execution_id.clone(),
                    None,
                    format!("{error}; {message}"),
                ),
            ),
            RouteCompletionCheck::Pending => {
                if status_failures < record.max_retries {
                    self.update_task(
                        &record.task_id,
                        task_update(
                            RouteMigrationTaskState::RetryWait,
                            record.attempts,
                            status_failures,
                            record.execution_id.clone(),
                            Some(self.next_retry_at_ms(status_failures)),
                            error.to_string(),
                        ),
                    );
                } else {
                    self.update_task(
                        &record.task_id,
                        task_update(
                            RouteMigrationTaskState::Failed,
                            record.attempts,
                            status_failures,
                            record.execution_id.clone(),
                            None,
                            error.to_string(),
                        ),
                    );
                }
            }
        }
    }

    fn handle_execution_failure(&self, record: RouteMigrationTaskRecord, error: &str) {
        match self.check_route_completion(&record) {
            RouteCompletionCheck::Completed => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Succeeded,
                    record.attempts,
                    record.status_failures,
                    record.execution_id.clone(),
                    None,
                    String::new(),
                ),
            ),
            RouteCompletionCheck::Conflict(message) => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Failed,
                    record.attempts,
                    record.status_failures,
                    record.execution_id.clone(),
                    None,
                    format!("{error}; {message}"),
                ),
            ),
            RouteCompletionCheck::Pending => {
                if record.attempts < record.max_retries {
                    self.update_task(
                        &record.task_id,
                        task_update(
                            RouteMigrationTaskState::RetryWait,
                            record.attempts,
                            0,
                            None,
                            Some(self.next_retry_at_ms(record.attempts)),
                            error.to_string(),
                        ),
                    );
                } else {
                    self.update_task(
                        &record.task_id,
                        task_update(
                            RouteMigrationTaskState::Failed,
                            record.attempts,
                            0,
                            record.execution_id.clone(),
                            None,
                            error.to_string(),
                        ),
                    );
                }
            }
        }
    }

    fn handle_attempt_failure(
        &self,
        record: RouteMigrationTaskRecord,
        attempted_dispatches: u32,
        error: String,
    ) {
        match self.check_route_completion(&record) {
            RouteCompletionCheck::Completed => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Succeeded,
                    attempted_dispatches,
                    record.status_failures,
                    record.execution_id.clone(),
                    None,
                    String::new(),
                ),
            ),
            RouteCompletionCheck::Conflict(message) => self.update_task(
                &record.task_id,
                task_update(
                    RouteMigrationTaskState::Failed,
                    attempted_dispatches,
                    record.status_failures,
                    record.execution_id.clone(),
                    None,
                    format!("{error}; {message}"),
                ),
            ),
            RouteCompletionCheck::Pending => {
                if attempted_dispatches < record.max_retries {
                    self.update_task(
                        &record.task_id,
                        task_update(
                            RouteMigrationTaskState::RetryWait,
                            attempted_dispatches,
                            0,
                            None,
                            Some(self.next_retry_at_ms(attempted_dispatches)),
                            error,
                        ),
                    );
                } else {
                    self.update_task(
                        &record.task_id,
                        task_update(
                            RouteMigrationTaskState::Failed,
                            attempted_dispatches,
                            0,
                            record.execution_id.clone(),
                            None,
                            error,
                        ),
                    );
                }
            }
        }
    }

    fn check_route_completion(&self, record: &RouteMigrationTaskRecord) -> RouteCompletionCheck {
        let object_key = ObjectKey::from_logical_id(&LogicalObjectId::new(
            NamespaceScope::with_defaults(
                Some(record.tenant.as_str()),
                record.domain.as_deref(),
                record.object_set.as_deref(),
            ),
            record.key.clone(),
        ));
        if let Ok(authorities) = self.route_authority_leases(&record.authority) {
            for authority in authorities {
                let authority_id = authority.runtime.stable_id.clone();
                if let Ok(route) =
                    self.rpc
                        .get_route(&authority, &record.namespace, &authority_id, &object_key)
                {
                    let completion = evaluate_route_completion(record, route.as_ref());
                    if !matches!(completion, RouteCompletionCheck::Pending) {
                        return completion;
                    }
                }
            }
        }
        let route = match self.backend.get_object_route(&object_key) {
            Ok(route) => route,
            Err(_) => return RouteCompletionCheck::Pending,
        };
        evaluate_route_completion(record, route.as_ref())
    }

    fn route_authority_leases(&self, preferred: &str) -> AdminResult<Vec<ClientLease>> {
        let mut leases = self
            .backend
            .list_live_clients()?
            .into_iter()
            .filter(|lease| lease.endpoints.labels.get("route").map(String::as_str) == Some("true"))
            .collect::<Vec<_>>();
        leases.sort_by(|left, right| {
            let left_key = (
                left.runtime.stable_id.0 != preferred,
                left.runtime.stable_id.0.clone(),
                u64::MAX - left.runtime.epoch.0,
                u64::MAX - left.expires_at_ms,
            );
            let right_key = (
                right.runtime.stable_id.0 != preferred,
                right.runtime.stable_id.0.clone(),
                u64::MAX - right.runtime.epoch.0,
                u64::MAX - right.expires_at_ms,
            );
            left_key.cmp(&right_key)
        });
        let mut seen = BTreeSet::new();
        let mut authorities = Vec::new();
        for lease in leases {
            if seen.insert(lease.runtime.stable_id.0.clone()) {
                authorities.push(lease);
            }
        }
        if authorities.is_empty() {
            return Err(StoreError::NotFound(
                "no live route-capable client lease was found".to_string(),
            ));
        }
        Ok(authorities)
    }

    fn resolve_live_lease(&self, stable_id: &str) -> AdminResult<ClientLease> {
        let mut matches = self
            .backend
            .list_live_clients()?
            .into_iter()
            .filter(|lease| lease.runtime.stable_id.0 == stable_id)
            .collect::<Vec<_>>();
        matches.sort_by(|left, right| {
            right
                .runtime
                .epoch
                .0
                .cmp(&left.runtime.epoch.0)
                .then_with(|| right.expires_at_ms.cmp(&left.expires_at_ms))
        });
        matches.into_iter().next().ok_or_else(|| {
            StoreError::NotFound(format!("live client lease for {stable_id} was not found"))
        })
    }

    fn next_retry_at_ms(&self, attempts: u32) -> u64 {
        let exponent = attempts.saturating_sub(1).min(16);
        let multiplier = 1u32.checked_shl(exponent).unwrap_or(u32::MAX);
        let delay = self
            .config
            .retry_base_delay
            .saturating_mul(multiplier)
            .min(self.config.retry_max_delay);
        now_ms().saturating_add(delay.as_millis() as u64)
    }

    fn update_task(&self, task_id: &str, update: RouteMigrationTaskUpdate) {
        if let Some(record) = self.tasks.lock().get_mut(task_id) {
            record.state = update.state;
            record.attempts = update.attempts;
            record.status_failures = update.status_failures;
            record.execution_id = update.execution_id;
            record.next_retry_at_ms = update.next_retry_at_ms;
            record.last_error = update.last_error;
            record.updated_at_ms = now_ms();
        }
    }
}

struct RouteMigrationTaskUpdate {
    state: RouteMigrationTaskState,
    attempts: u32,
    status_failures: u32,
    execution_id: Option<String>,
    next_retry_at_ms: Option<u64>,
    last_error: String,
}

fn task_update(
    state: RouteMigrationTaskState,
    attempts: u32,
    status_failures: u32,
    execution_id: Option<String>,
    next_retry_at_ms: Option<u64>,
    last_error: String,
) -> RouteMigrationTaskUpdate {
    RouteMigrationTaskUpdate {
        state,
        attempts,
        status_failures,
        execution_id,
        next_retry_at_ms,
        last_error,
    }
}

enum MigrationTaskAction {
    Dispatch(RouteMigrationTaskRecord),
    Poll(RouteMigrationTaskRecord),
}

fn migration_mode_to_proto(mode: RouteMigrationMode) -> control_plane_pb::MigrationMode {
    match mode {
        RouteMigrationMode::Copy => control_plane_pb::MigrationMode::Copy,
        RouteMigrationMode::Move => control_plane_pb::MigrationMode::Move,
    }
}

fn parse_env_u32(name: &str) -> AdminResult<Option<u32>> {
    match env::var(name) {
        Ok(value) => value.parse::<u32>().map(Some).map_err(|error| {
            StoreError::InvalidState(format!(
                "invalid {name} environment variable value {value:?}: {error}"
            ))
        }),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(StoreError::InvalidState(format!(
            "failed to read {name} environment variable: {error}"
        ))),
    }
}

fn parse_env_duration_ms(name: &str) -> AdminResult<Option<Duration>> {
    Ok(parse_env_u32(name)?.map(|value| Duration::from_millis(value as u64)))
}

fn validate_route_migration_request(
    mode: RouteMigrationMode,
    request: &RouteMigrationTaskSubmitRequest,
) -> AdminResult<()> {
    if request.authority.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "route migration task is missing authority".to_string(),
        ));
    }
    if request.tenant.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "route migration task is missing tenant".to_string(),
        ));
    }
    if request.key.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "route migration task is missing key".to_string(),
        ));
    }
    if request.source_segment.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "route migration task is missing source_segment".to_string(),
        ));
    }
    if request.task_executor.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "route migration task is missing task_executor".to_string(),
        ));
    }
    if request.max_retries == Some(0) {
        return Err(StoreError::InvalidState(
            "route migration task max_retries must be greater than zero".to_string(),
        ));
    }
    match mode {
        RouteMigrationMode::Copy if request.target_segments.is_empty() => {
            Err(StoreError::InvalidState(
                "route migration copy tasks require at least one target_segment".to_string(),
            ))
        }
        RouteMigrationMode::Move if request.target_segments.len() != 1 => {
            Err(StoreError::InvalidState(
                "route migration move tasks require exactly one target_segment".to_string(),
            ))
        }
        _ => Ok(()),
    }
}

fn evaluate_route_completion(
    record: &RouteMigrationTaskRecord,
    route: Option<&ObjectRoute>,
) -> RouteCompletionCheck {
    let Some(route) = route else {
        return RouteCompletionCheck::Pending;
    };
    let has_source = route
        .replicas
        .iter()
        .any(|replica| replica.segment_name.0 == record.source_segment);
    let target_matches = record
        .target_segments
        .iter()
        .map(|target| {
            route
                .replicas
                .iter()
                .any(|replica| replica.segment_name.0 == *target)
        })
        .collect::<Vec<_>>();
    match record.mode {
        RouteMigrationMode::Copy => {
            if has_source && target_matches.iter().all(|present| *present) {
                RouteCompletionCheck::Completed
            } else if target_matches.iter().any(|present| *present) || !has_source {
                RouteCompletionCheck::Conflict(
                    "route migration copy requires source visibility plus all requested targets"
                        .to_string(),
                )
            } else {
                RouteCompletionCheck::Pending
            }
        }
        RouteMigrationMode::Move => {
            let has_target = target_matches.first().copied().unwrap_or(false);
            if has_target && !has_source {
                RouteCompletionCheck::Completed
            } else if has_target && has_source {
                RouteCompletionCheck::Conflict(
                    "route migration move left both source and target replicas visible".to_string(),
                )
            } else if !has_target && !has_source {
                RouteCompletionCheck::Conflict(
                    "route migration move lost the source replica before the target became visible"
                        .to_string(),
                )
            } else {
                RouteCompletionCheck::Pending
            }
        }
    }
}

#[derive(Clone)]
pub struct AdminService {
    backend: Arc<dyn MetadataBackend>,
    metadata_url: String,
    keyspace: MetadataKeyspace,
    migrations: MigrationTaskManager,
}

trait AdminMaintenanceBackend {
    fn cleanup_stale_segments_for_owner(
        &self,
        runtime: &ClientRuntimeId,
    ) -> AdminResult<RedisMetadataCleanupReport>;
    fn client_lease_liveness(&self, runtime: &ClientRuntimeId) -> AdminResult<ClientLeaseLiveness>;
    fn list_due_client_lease_expiries(
        &self,
        expires_before_ms: u64,
        limit: usize,
    ) -> AdminResult<Vec<String>>;
    fn refresh_client_lease_expiry(
        &self,
        runtime: &ClientRuntimeId,
        expires_at_ms: u64,
    ) -> AdminResult<()>;
    fn remove_client_lease_expiry(&self, runtime: &ClientRuntimeId) -> AdminResult<bool>;
    fn remove_client_lease_expiry_entry(&self, lease_key: &str) -> AdminResult<bool>;
}

impl AdminMaintenanceBackend for RedisMetadataBackend {
    fn cleanup_stale_segments_for_owner(
        &self,
        runtime: &ClientRuntimeId,
    ) -> AdminResult<RedisMetadataCleanupReport> {
        Self::cleanup_stale_segments_for_owner(self, runtime)
    }

    fn client_lease_liveness(&self, runtime: &ClientRuntimeId) -> AdminResult<ClientLeaseLiveness> {
        Self::client_lease_liveness(self, runtime)
    }

    fn list_due_client_lease_expiries(
        &self,
        expires_before_ms: u64,
        limit: usize,
    ) -> AdminResult<Vec<String>> {
        Self::list_due_client_lease_expiries(self, expires_before_ms, limit)
    }

    fn refresh_client_lease_expiry(
        &self,
        runtime: &ClientRuntimeId,
        expires_at_ms: u64,
    ) -> AdminResult<()> {
        Self::refresh_client_lease_expiry(self, runtime, expires_at_ms)
    }

    fn remove_client_lease_expiry(&self, runtime: &ClientRuntimeId) -> AdminResult<bool> {
        Self::remove_client_lease_expiry(self, runtime)
    }

    fn remove_client_lease_expiry_entry(&self, lease_key: &str) -> AdminResult<bool> {
        Self::remove_client_lease_expiry_entry(self, lease_key)
    }
}

impl AdminMaintenanceBackend for EtcdMetadataBackend {
    fn cleanup_stale_segments_for_owner(
        &self,
        runtime: &ClientRuntimeId,
    ) -> AdminResult<RedisMetadataCleanupReport> {
        Self::cleanup_stale_segments_for_owner(self, runtime)
    }

    fn client_lease_liveness(&self, runtime: &ClientRuntimeId) -> AdminResult<ClientLeaseLiveness> {
        Self::client_lease_liveness(self, runtime)
    }

    fn list_due_client_lease_expiries(
        &self,
        expires_before_ms: u64,
        limit: usize,
    ) -> AdminResult<Vec<String>> {
        Self::list_due_client_lease_expiries(self, expires_before_ms, limit)
    }

    fn refresh_client_lease_expiry(
        &self,
        runtime: &ClientRuntimeId,
        expires_at_ms: u64,
    ) -> AdminResult<()> {
        Self::refresh_client_lease_expiry(self, runtime, expires_at_ms)
    }

    fn remove_client_lease_expiry(&self, runtime: &ClientRuntimeId) -> AdminResult<bool> {
        Self::remove_client_lease_expiry(self, runtime)
    }

    fn remove_client_lease_expiry_entry(&self, lease_key: &str) -> AdminResult<bool> {
        Self::remove_client_lease_expiry_entry(self, lease_key)
    }
}

enum MaintenanceBackend {
    Redis(RedisMetadataBackend),
    Etcd(EtcdMetadataBackend),
}

fn normalize_etcd_endpoint(endpoint: &str) -> String {
    if endpoint.contains("://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn admin_cleanup_report(report: &RedisMetadataCleanupReport) -> AdminCleanupReport {
    AdminCleanupReport {
        live_clients: report.live_clients,
        inspected_segment_keys: report.inspected_segment_keys,
        removed_segment_keys: report.removed_segment_keys,
        removed_segment_index_entries: report.removed_segment_index_entries,
        removed_owner_segment_index_entries: report.removed_owner_segment_index_entries,
        stale_missing_segment_index_entries: report.stale_missing_segment_index_entries,
    }
}

fn store_lease_can_host_tracing(lease: &ClientLease) -> bool {
    lease.state.serves_reads()
        && (lease.endpoints.labels.get("route").map(String::as_str) == Some("true")
            || lease.endpoints.labels.get("storage").map(String::as_str) == Some("true"))
}

fn tracing_target_from_lease(lease: &ClientLease) -> Option<TracingTarget> {
    let host = lease
        .endpoints
        .labels
        .get(CONTROL_ADDR_LABEL)
        .and_then(|value| endpoint_host(value))
        .or_else(|| endpoint_host(&lease.endpoints.rpc_address))?;
    let port = lease
        .endpoints
        .labels
        .get(METRICS_PORT_LABEL)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(DEFAULT_STORE_METRICS_PORT);
    let metrics_url = format!("http://{}", format_host_port(&host, port));
    Some(TracingTarget {
        runtime: lease.runtime.clone(),
        metrics_url,
        host,
        port,
    })
}

fn endpoint_host(address: &str) -> Option<String> {
    let address = address.trim();
    if address.is_empty() {
        return None;
    }
    let normalized = if address.contains("://") {
        address.to_string()
    } else {
        format!("http://{address}")
    };
    Url::parse(&normalized)
        .ok()
        .and_then(|url| url.host_str().map(str::to_string))
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn tracing_action_path(
    action: TracingAction,
    request: &TracingUpdateRequest,
) -> AdminResult<String> {
    match action {
        TracingAction::Status => Ok("/tracing".to_string()),
        TracingAction::Off => Ok("/tracing/off".to_string()),
        TracingAction::Flush => Ok("/tracing/flush".to_string()),
        TracingAction::On => {
            if request.endpoint.is_none()
                && request.file.is_none()
                && request.clear_file != Some(true)
                && request.sample_ratio.is_none()
            {
                return Ok("/tracing/on".to_string());
            }
            let mut query = url::form_urlencoded::Serializer::new(String::new());
            query.append_pair("enabled", "on");
            if let Some(endpoint) = request.endpoint.as_deref() {
                query.append_pair("endpoint", endpoint);
            }
            if let Some(file) = request.file.as_deref() {
                query.append_pair("file", file);
            }
            if request.clear_file == Some(true) {
                query.append_pair("clear_file", "true");
            }
            if let Some(sample_ratio) = request.sample_ratio {
                if !sample_ratio.is_finite() || !(0.0..=1.0).contains(&sample_ratio) {
                    return Err(StoreError::Unsupported(format!(
                        "tracing sample_ratio must be between 0.0 and 1.0, got {sample_ratio}"
                    )));
                }
                query.append_pair("sample_ratio", &sample_ratio.to_string());
            }
            Ok(format!("/tracing?{}", query.finish()))
        }
    }
}

fn fetch_tracing_status(
    target: &TracingTarget,
    path: &str,
    timeout: Duration,
) -> AdminResult<serde_json::Value> {
    let socket_addr = (target.host.as_str(), target.port)
        .to_socket_addrs()
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to resolve tracing target {}: {error}",
                target.metrics_url
            ))
        })?
        .next()
        .ok_or_else(|| {
            StoreError::Transport(format!(
                "failed to resolve tracing target {}",
                target.metrics_url
            ))
        })?;
    let mut stream = TcpStream::connect_timeout(&socket_addr, timeout).map_err(|error| {
        StoreError::Transport(format!(
            "failed to connect tracing target {}: {error}",
            target.metrics_url
        ))
    })?;
    stream.set_read_timeout(Some(timeout)).map_err(|error| {
        StoreError::Transport(format!(
            "failed to set tracing read timeout for {}: {error}",
            target.metrics_url
        ))
    })?;
    stream.set_write_timeout(Some(timeout)).map_err(|error| {
        StoreError::Transport(format!(
            "failed to set tracing write timeout for {}: {error}",
            target.metrics_url
        ))
    })?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: application/json\r\n\r\n",
        format_host_port(&target.host, target.port)
    );
    stream.write_all(request.as_bytes()).map_err(|error| {
        StoreError::Transport(format!(
            "failed to write tracing request to {}: {error}",
            target.metrics_url
        ))
    })?;
    stream.flush().map_err(|error| {
        StoreError::Transport(format!(
            "failed to flush tracing request to {}: {error}",
            target.metrics_url
        ))
    })?;
    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(|error| {
        StoreError::Transport(format!(
            "failed to read tracing response from {}: {error}",
            target.metrics_url
        ))
    })?;
    let (headers, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        StoreError::Transport(format!(
            "invalid tracing http response from {}",
            target.metrics_url
        ))
    })?;
    let status_line = headers.lines().next().ok_or_else(|| {
        StoreError::Transport(format!(
            "missing tracing http status from {}",
            target.metrics_url
        ))
    })?;
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| {
            StoreError::Transport(format!(
                "invalid tracing http status from {}: {status_line}",
                target.metrics_url
            ))
        })?;
    if !(200..300).contains(&status_code) {
        return Err(StoreError::Transport(format!(
            "tracing request to {} failed: {status_line}: {}",
            target.metrics_url,
            body.trim()
        )));
    }
    Ok(serde_json::from_str(body)
        .unwrap_or_else(|_| serde_json::Value::String(body.trim().to_string())))
}

impl AdminService {
    fn maintenance_backend(&self) -> AdminResult<MaintenanceBackend> {
        if self.metadata_url.starts_with("redis://") || self.metadata_url.starts_with("rediss://") {
            return RedisMetadataBackend::new(
                RedisMetadataConfig::new(self.metadata_url.clone()).keyspace(self.keyspace.clone()),
            )
            .map(MaintenanceBackend::Redis);
        }
        if let Some(rest) = self.metadata_url.strip_prefix("etcd://") {
            let endpoints = rest
                .split(',')
                .filter(|entry| !entry.trim().is_empty())
                .map(|entry| normalize_etcd_endpoint(entry.trim()))
                .collect::<Vec<_>>();
            if endpoints.is_empty() {
                return Err(StoreError::Metadata(
                    "etcd metadata url must contain at least one endpoint".to_string(),
                ));
            }
            return EtcdMetadataBackend::from_config(
                EtcdMetadataConfig::new(endpoints).keyspace(self.keyspace.clone()),
            )
            .map(MaintenanceBackend::Etcd);
        }
        Err(StoreError::Unsupported(
            "stale segment maintenance currently supports redis://, rediss://, and etcd:// metadata only"
                .to_string(),
        ))
    }

    pub fn supports_stale_segment_maintenance(&self) -> bool {
        self.maintenance_backend().is_ok()
    }

    pub fn from_config(metadata_url: &str, keyspace: Option<String>) -> AdminResult<Self> {
        let keyspace = keyspace.map(MetadataKeyspace::new).unwrap_or_default();
        let backend = build_store_metadata_backend(metadata_url, keyspace.clone())?;
        Ok(Self::new_with_migration_support(
            backend,
            metadata_url.to_string(),
            keyspace,
            MigrationQueueConfig::from_env()?,
            Arc::new(ControlPlaneMigrationRpc),
        ))
    }

    pub fn new(
        backend: Arc<dyn MetadataBackend>,
        metadata_url: impl Into<String>,
        keyspace: MetadataKeyspace,
    ) -> Self {
        Self::new_with_migration_support(
            backend,
            metadata_url,
            keyspace,
            MigrationQueueConfig::default(),
            Arc::new(ControlPlaneMigrationRpc),
        )
    }

    pub(crate) fn new_with_migration_support(
        backend: Arc<dyn MetadataBackend>,
        metadata_url: impl Into<String>,
        keyspace: MetadataKeyspace,
        migration_config: MigrationQueueConfig,
        migration_rpc: Arc<dyn MigrationRpc>,
    ) -> Self {
        Self {
            migrations: MigrationTaskManager::new(backend.clone(), migration_rpc, migration_config),
            backend,
            metadata_url: metadata_url.into(),
            keyspace,
        }
    }

    pub fn backend(&self) -> &dyn MetadataBackend {
        self.backend.as_ref()
    }

    pub fn metadata_url(&self) -> &str {
        &self.metadata_url
    }

    pub fn keyspace(&self) -> &MetadataKeyspace {
        &self.keyspace
    }

    pub fn redacted_metadata_url(&self) -> String {
        redact_redis_url(&self.metadata_url)
    }

    pub(crate) fn discover_tracing_targets(&self) -> AdminResult<Vec<TracingTarget>> {
        let mut targets = self
            .backend
            .list_live_clients()?
            .into_iter()
            .filter(store_lease_can_host_tracing)
            .filter_map(|lease| tracing_target_from_lease(&lease))
            .collect::<Vec<_>>();
        targets.sort_by(|left, right| left.runtime.cmp(&right.runtime));
        Ok(targets)
    }

    pub fn update_tracing(
        &self,
        action: TracingAction,
        request: TracingUpdateRequest,
    ) -> AdminResult<TracingClusterResponse> {
        let targets = self.discover_tracing_targets()?;
        let max_targets = request
            .max_targets
            .unwrap_or(DEFAULT_TRACING_MAX_TARGETS)
            .max(1);
        if targets.len() > max_targets {
            return Err(StoreError::Unsupported(format!(
                "tracing fanout target count {} exceeds max_targets {max_targets}",
                targets.len()
            )));
        }
        let timeout = request
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_TRACING_FANOUT_TIMEOUT)
            .max(Duration::from_millis(1));
        let path = tracing_action_path(action.clone(), &request)?;
        let mut nodes = Vec::with_capacity(targets.len());
        for target in targets {
            nodes.push(match fetch_tracing_status(&target, &path, timeout) {
                Ok(status) => TracingNodeResponse {
                    runtime: target.runtime,
                    metrics_url: target.metrics_url,
                    ok: true,
                    status: Some(status),
                    error: None,
                },
                Err(error) => TracingNodeResponse {
                    runtime: target.runtime,
                    metrics_url: target.metrics_url,
                    ok: false,
                    status: None,
                    error: Some(error.to_string()),
                },
            });
        }
        let ok = nodes.iter().filter(|node| node.ok).count();
        Ok(TracingClusterResponse {
            action,
            total: nodes.len(),
            ok,
            failed: nodes.len().saturating_sub(ok),
            nodes,
        })
    }

    fn backend_for_tenant(&self, tenant: Option<&str>) -> AdminResult<Arc<dyn MetadataBackend>> {
        match tenant.map(str::trim).filter(|tenant| !tenant.is_empty()) {
            Some(tenant) => self.backend.for_tenant(tenant).ok_or_else(|| {
                StoreError::Unsupported(format!(
                    "metadata backend {} does not support tenant-scoped admin queries",
                    self.backend.route_namespace()
                ))
            }),
            None => Ok(self.backend.clone()),
        }
    }

    pub fn submit_route_migration_task(
        &self,
        mode: RouteMigrationMode,
        request: RouteMigrationTaskSubmitRequest,
    ) -> AdminResult<RouteMigrationTaskStatusResponse> {
        self.migrations.submit(mode, request)
    }

    pub fn get_route_migration_task(
        &self,
        task_id: &str,
    ) -> AdminResult<RouteMigrationTaskStatusResponse> {
        self.migrations.get(task_id)
    }

    pub fn list_route_migration_tasks(&self) -> RouteMigrationTaskListResponse {
        self.migrations.list()
    }

    pub fn get_route_policy(&self, tenant: Option<&str>) -> AdminResult<RoutePolicyResponse> {
        let backend = self.backend_for_tenant(tenant)?;
        let domain = route_policy_domain(tenant);
        let policy = backend.get_route_policy(&domain)?;
        Ok(RoutePolicyResponse {
            domain: format_route_policy_domain(&domain),
            found: policy.is_some(),
            policy,
        })
    }

    pub fn get_tenant_policy(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        effective: bool,
    ) -> AdminResult<GetTenantPolicyResponse> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        if effective {
            let namespace = NamespaceScope::with_defaults(Some(tenant), domain, object_set);
            let effective_spec = resolve_effective_tenant_policy(backend.as_ref(), &namespace)?;
            return Ok(GetTenantPolicyResponse {
                scope,
                effective: true,
                found: effective_spec.is_some(),
                policy: None,
                effective_spec,
            });
        }

        let policy = backend.get_tenant_policy(&scope)?;
        Ok(GetTenantPolicyResponse {
            scope,
            effective: false,
            found: policy.is_some(),
            policy,
            effective_spec: None,
        })
    }

    pub fn set_tenant_policy(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        patch: PolicyPatchInput,
        expected_version: Option<u64>,
        updated_by: &str,
    ) -> AdminResult<TenantPolicy> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let patch = tenant_policy_patch(&patch)?;
        if policy_patch_is_empty(&patch) {
            return Err(StoreError::InvalidState(
                "at least one policy flag must be provided".to_string(),
            ));
        }
        let current = backend.get_tenant_policy(&scope)?;
        let policy = merge_tenant_policy(current.as_ref(), scope.clone(), patch, updated_by);
        let expected = expected_version.or_else(|| current.as_ref().map(|policy| policy.version));
        let stored = backend.put_tenant_policy(&policy, expected)?;
        Ok(stored)
    }

    pub fn delete_tenant_policy(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        expected_version: Option<u64>,
    ) -> AdminResult<DeleteTenantPolicyResponse> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let removed = backend.delete_tenant_policy(&scope, expected_version)?;
        if removed && is_root_tenant_scope(&scope) {
            backend.delete_route_policy(&RoutePolicyDomain::Tenant(scope.tenant.clone()))?;
        }
        Ok(DeleteTenantPolicyResponse { scope, removed })
    }

    pub fn list_tenant_policies(&self, tenant: Option<&str>) -> AdminResult<Vec<TenantPolicy>> {
        let backend = self.backend_for_tenant(tenant)?;
        let mut policies = backend.list_tenant_policies(tenant)?;
        policies.sort_by(|left, right| left.scope.cmp(&right.scope));
        Ok(policies)
    }

    pub fn get_tenant_quota_state(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
    ) -> AdminResult<GetTenantQuotaStateResponse> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let state = backend.get_tenant_quota_state(&root_scope)?;
        Ok(GetTenantQuotaStateResponse {
            scope: root_scope,
            found: state.is_some(),
            state,
        })
    }

    pub fn get_tenant_object_accounting(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        key: &str,
    ) -> AdminResult<GetTenantObjectAccountingResponse> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let object_id = LogicalObjectId::new(
            NamespaceScope::with_defaults(Some(tenant), domain, object_set),
            key.to_string(),
        );
        let scoped_key = ObjectKey::from_logical_id(&object_id);
        let accounting = backend.get_tenant_object_accounting(&scoped_key)?;
        Ok(GetTenantObjectAccountingResponse {
            scope: root_scope,
            key: scoped_key.0,
            found: accounting.is_some(),
            accounting,
        })
    }

    pub fn list_tenant_quota_reservations(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        state: Option<TenantQuotaReservationState>,
    ) -> AdminResult<ListTenantQuotaReservationsResponse> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let mut reservations = backend.list_tenant_quota_reservations(&root_scope)?;
        if let Some(state) = state {
            reservations.retain(|reservation| reservation.state == state);
        }
        reservations.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.reservation_id.cmp(&right.reservation_id))
        });
        Ok(ListTenantQuotaReservationsResponse {
            scope: root_scope,
            count: reservations.len(),
            reservations,
        })
    }

    pub fn abort_tenant_quota_reservation(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        reservation_id: &str,
        dry_run: bool,
    ) -> AdminResult<TenantQuotaAbortResponse> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let reservation = backend
            .get_tenant_quota_reservation(reservation_id)?
            .filter(|entry| entry.scope == root_scope)
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "tenant quota reservation {reservation_id} was not found in {}",
                    format_policy_scope(&root_scope)
                ))
            })?;
        if reservation.state != TenantQuotaReservationState::Pending {
            return Ok(TenantQuotaAbortResponse {
                scope: root_scope,
                reservation_id: reservation_id.to_string(),
                dry_run,
                aborted: false,
            });
        }
        if !dry_run {
            let abort_result = backend.abort_tenant_quota(reservation_id);
            record_tenant_quota_reconcile(match &abort_result {
                Ok(_) => "aborted",
                Err(_) => "error",
            });
            abort_result?;
        }
        Ok(TenantQuotaAbortResponse {
            scope: root_scope,
            reservation_id: reservation_id.to_string(),
            dry_run,
            aborted: true,
        })
    }

    pub fn reconcile_tenant_quota_reservations(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        dry_run: bool,
    ) -> AdminResult<TenantQuotaReconcileReport> {
        let backend = self.backend_for_tenant(Some(tenant))?;
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let now = now_ms();
        let mut reservations = backend.list_tenant_quota_reservations(&root_scope)?;
        reservations.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.reservation_id.cmp(&right.reservation_id))
        });
        let mut report = TenantQuotaReconcileReport {
            scope: root_scope.clone(),
            dry_run,
            inspected: reservations.len(),
            finalized: 0,
            aborted: 0,
            skipped: 0,
            actions: Vec::new(),
        };
        for reservation in reservations {
            if reservation.state != TenantQuotaReservationState::Pending {
                report.skipped = report.skipped.saturating_add(1);
                continue;
            }
            if reservation.expires_at_ms <= now {
                if !dry_run {
                    let abort_result = backend.abort_tenant_quota(&reservation.reservation_id);
                    record_tenant_quota_reconcile(match &abort_result {
                        Ok(_) => "aborted",
                        Err(_) => "error",
                    });
                    abort_result?;
                }
                report.aborted = report.aborted.saturating_add(1);
                report.actions.push(TenantQuotaReconcileAction {
                    reservation_id: reservation.reservation_id,
                    key: reservation.key.0,
                    action: "abort".to_string(),
                    reason: "expired_pending_reservation".to_string(),
                });
                continue;
            }
            let route = backend.get_object_route(&reservation.key)?;
            let accounting = backend.get_tenant_object_accounting(&reservation.key)?;
            let route_matches_accounting = route
                .as_ref()
                .and_then(|route| {
                    let object_id = route_logical_object_id(route).ok()?;
                    (object_id.scope.tenant == root_scope.tenant).then_some(route)
                })
                .zip(accounting.as_ref())
                .is_some_and(|(route, accounting)| {
                    route.state == mooncake_store_core::RouteState::Active
                        && accounting.state == TenantObjectAccountingState::Active
                        && accounting.route_version == Some(route.version)
                        && accounting.committed_length
                            == route
                                .replicas
                                .iter()
                                .min_by_key(|replica| replica.priority)
                                .map(|replica| replica.length)
                                .unwrap_or(0)
                });
            if route_matches_accounting {
                if !dry_run {
                    let committed_length = route
                        .as_ref()
                        .and_then(|route| {
                            route
                                .replicas
                                .iter()
                                .min_by_key(|replica| replica.priority)
                                .map(|replica| replica.length)
                        })
                        .unwrap_or(0);
                    let finalize_result =
                        backend.finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                            reservation_id: reservation.reservation_id.clone(),
                            expected_object_version: reservation.expected_object_version,
                            committed_length: Some(committed_length),
                            route_version: accounting
                                .as_ref()
                                .and_then(|accounting| accounting.route_version),
                            state: TenantObjectAccountingState::Active,
                            updated_at_ms: now,
                            updated_by: "admin-reconcile".to_string(),
                        });
                    record_tenant_quota_reconcile(match &finalize_result {
                        Ok(_) => "finalized",
                        Err(_) => "error",
                    });
                    finalize_result?;
                }
                report.finalized = report.finalized.saturating_add(1);
                report.actions.push(TenantQuotaReconcileAction {
                    reservation_id: reservation.reservation_id,
                    key: reservation.key.0,
                    action: "finalize-visible-route".to_string(),
                    reason: "route_and_accounting_are_authoritative".to_string(),
                });
            } else {
                report.skipped = report.skipped.saturating_add(1);
            }
        }
        Ok(report)
    }

    pub fn cleanup_stale_segments(&self) -> AdminResult<AdminCleanupReport> {
        match self.maintenance_backend()? {
            MaintenanceBackend::Redis(backend) => {
                Ok(admin_cleanup_report(&backend.cleanup_stale_segments()?))
            }
            MaintenanceBackend::Etcd(backend) => {
                Ok(admin_cleanup_report(&backend.cleanup_stale_segments()?))
            }
        }
    }

    pub fn cleanup_stale_segments_for_owner(
        &self,
        runtime: &ClientRuntimeId,
    ) -> AdminResult<AdminOwnerCleanupReport> {
        match self.maintenance_backend()? {
            MaintenanceBackend::Redis(backend) => {
                Self::cleanup_stale_segments_for_owner_with_backend(&backend, runtime)
            }
            MaintenanceBackend::Etcd(backend) => {
                Self::cleanup_stale_segments_for_owner_with_backend(&backend, runtime)
            }
        }
    }

    fn cleanup_stale_segments_for_owner_with_backend<B: AdminMaintenanceBackend>(
        backend: &B,
        runtime: &ClientRuntimeId,
    ) -> AdminResult<AdminOwnerCleanupReport> {
        match backend.client_lease_liveness(runtime)? {
            ClientLeaseLiveness::Live(lease) => {
                backend.refresh_client_lease_expiry(&lease.runtime, lease.expires_at_ms)?;
                Ok(AdminOwnerCleanupReport {
                    owner: lease.runtime,
                    state: AdminOwnerCleanupState::SkippedLive,
                    cleanup: AdminCleanupReport {
                        live_clients: 0,
                        inspected_segment_keys: 0,
                        removed_segment_keys: 0,
                        removed_segment_index_entries: 0,
                        removed_owner_segment_index_entries: 0,
                        stale_missing_segment_index_entries: 0,
                    },
                })
            }
            ClientLeaseLiveness::Missing => {
                let cleanup = backend.cleanup_stale_segments_for_owner(runtime)?;
                let _ = backend.remove_client_lease_expiry(runtime)?;
                Ok(AdminOwnerCleanupReport {
                    owner: runtime.clone(),
                    state: AdminOwnerCleanupState::CleanedMissingLease,
                    cleanup: admin_cleanup_report(&cleanup),
                })
            }
            ClientLeaseLiveness::Expired(lease) => {
                let cleanup = backend.cleanup_stale_segments_for_owner(&lease.runtime)?;
                let _ = backend.remove_client_lease_expiry(&lease.runtime)?;
                Ok(AdminOwnerCleanupReport {
                    owner: lease.runtime,
                    state: AdminOwnerCleanupState::CleanedExpiredLease,
                    cleanup: admin_cleanup_report(&cleanup),
                })
            }
        }
    }

    pub fn reconcile_due_stale_segments(
        &self,
        limit: usize,
    ) -> AdminResult<AdminMaintenanceReport> {
        match self.maintenance_backend()? {
            MaintenanceBackend::Redis(backend) => {
                self.reconcile_due_stale_segments_with_backend(&backend, limit)
            }
            MaintenanceBackend::Etcd(backend) => {
                self.reconcile_due_stale_segments_with_backend(&backend, limit)
            }
        }
    }

    fn reconcile_due_stale_segments_with_backend<B: AdminMaintenanceBackend>(
        &self,
        backend: &B,
        limit: usize,
    ) -> AdminResult<AdminMaintenanceReport> {
        let due_entries = backend.list_due_client_lease_expiries(now_ms(), limit)?;
        let mut report = AdminMaintenanceReport {
            due_entries: due_entries.len(),
            ..AdminMaintenanceReport::default()
        };

        for lease_key in due_entries {
            let Some((stable_id, epoch)) = self.keyspace.parse_client_key(&lease_key) else {
                let _ = backend.remove_client_lease_expiry_entry(&lease_key)?;
                report.invalid_entries = report.invalid_entries.saturating_add(1);
                continue;
            };
            let runtime = ClientRuntimeId::new(stable_id, ClientEpoch(epoch));
            let outcome = Self::cleanup_stale_segments_for_owner_with_backend(backend, &runtime)?;
            match outcome.state {
                AdminOwnerCleanupState::SkippedLive => {
                    report.skipped_live = report.skipped_live.saturating_add(1);
                }
                AdminOwnerCleanupState::CleanedMissingLease => {
                    report.cleaned_missing_lease = report.cleaned_missing_lease.saturating_add(1);
                }
                AdminOwnerCleanupState::CleanedExpiredLease => {
                    report.cleaned_expired_lease = report.cleaned_expired_lease.saturating_add(1);
                }
            }
            report.removed_segment_keys = report
                .removed_segment_keys
                .saturating_add(outcome.cleanup.removed_segment_keys);
            report.removed_segment_index_entries = report
                .removed_segment_index_entries
                .saturating_add(outcome.cleanup.removed_segment_index_entries);
            report.removed_owner_segment_index_entries = report
                .removed_owner_segment_index_entries
                .saturating_add(outcome.cleanup.removed_owner_segment_index_entries);
            report.stale_missing_segment_index_entries = report
                .stale_missing_segment_index_entries
                .saturating_add(outcome.cleanup.stale_missing_segment_index_entries);
        }

        Ok(report)
    }
}

pub fn route_policy_domain(tenant: Option<&str>) -> RoutePolicyDomain {
    tenant
        .map(|tenant| RoutePolicyDomain::Tenant(tenant.to_string()))
        .unwrap_or(RoutePolicyDomain::Default)
}

pub fn format_route_policy_domain(domain: &RoutePolicyDomain) -> String {
    match domain {
        RoutePolicyDomain::Default => "default".to_string(),
        RoutePolicyDomain::Tenant(tenant) => format!("tenant:{tenant}"),
    }
}

fn resolve_effective_tenant_policy(
    backend: &dyn MetadataBackend,
    scope: &NamespaceScope,
) -> AdminResult<Option<TenantPolicySpec>> {
    let mut resolved = TenantPolicySpec::default();
    let mut found = false;
    for policy_scope in TenantPolicyScope::ancestors(scope) {
        if let Some(policy) = backend.get_tenant_policy(&policy_scope)? {
            resolved = resolved.merged_with(&policy.spec);
            found = true;
        }
    }
    Ok(found.then_some(resolved))
}

pub fn tenant_policy_scope(
    tenant: &str,
    domain: Option<&str>,
    object_set: Option<&str>,
) -> AdminResult<TenantPolicyScope> {
    let scope = TenantPolicyScope::new(tenant, domain, object_set);
    scope.validate()?;
    Ok(scope)
}

pub fn tenant_policy_patch(values: &PolicyPatchInput) -> AdminResult<TenantPolicySpec> {
    if let Some(route_topk) = values.route_topk {
        if route_topk < 2 {
            return Err(StoreError::InvalidState(
                "route_topk must be greater than or equal to 2".to_string(),
            ));
        }
    }

    Ok(TenantPolicySpec {
        routing: Some(TenantRoutePolicy {
            route_topk: values.route_topk,
        })
        .filter(|policy| policy.route_topk.is_some()),
        quota: Some(TenantQuotaPolicy {
            max_bytes: values.max_bytes,
            max_objects: values.max_objects,
        })
        .filter(|policy| policy.max_bytes.is_some() || policy.max_objects.is_some()),
        fairness: Some(TenantExecutionFairnessPolicy {
            max_remote_batch_items_per_tenant: values.max_remote_batch_items_per_tenant,
        })
        .filter(|policy| policy.max_remote_batch_items_per_tenant.is_some()),
        shaping: Some(TenantBandwidthShapingPolicy {
            max_remote_batch_bytes: values.max_remote_batch_bytes,
            max_remote_batch_burst_items: values.max_remote_batch_burst_items,
            max_inflight_bytes_per_batch: values.max_inflight_bytes_per_batch,
        })
        .filter(|policy| {
            policy.max_remote_batch_bytes.is_some()
                || policy.max_remote_batch_burst_items.is_some()
                || policy.max_inflight_bytes_per_batch.is_some()
        }),
        placement: Some(TenantPlacementPolicy {
            default_replica_count: values.default_replica_count,
            prefer_local: values.prefer_local,
            prefer_alloc_in_same_node: values.prefer_alloc_in_same_node,
            preferred_storage_owners: values.preferred_storage_owners.clone(),
            preferred_segments: values.preferred_segments.clone(),
        })
        .filter(|policy| {
            policy.default_replica_count.is_some()
                || policy.prefer_local.is_some()
                || policy.prefer_alloc_in_same_node.is_some()
                || policy.preferred_storage_owners.is_some()
                || policy.preferred_segments.is_some()
        }),
    })
}

pub fn policy_patch_is_empty(spec: &TenantPolicySpec) -> bool {
    spec.routing.is_none()
        && spec.quota.is_none()
        && spec.fairness.is_none()
        && spec.shaping.is_none()
        && spec.placement.is_none()
}

pub fn merge_tenant_policy(
    current: Option<&TenantPolicy>,
    scope: TenantPolicyScope,
    patch: TenantPolicySpec,
    updated_by: &str,
) -> TenantPolicy {
    let spec = current
        .map(|policy| policy.spec.merged_with(&patch))
        .unwrap_or(patch);
    TenantPolicy {
        scope,
        spec,
        version: current
            .map(|policy| policy.version.saturating_add(1))
            .unwrap_or(1),
        updated_at_ms: now_ms(),
        updated_by: updated_by.to_string(),
    }
}

pub fn is_root_tenant_scope(scope: &TenantPolicyScope) -> bool {
    scope.domain.is_none() && scope.object_set.is_none()
}

pub fn format_policy_scope(scope: &TenantPolicyScope) -> String {
    let mut formatted = format!("tenant={}", scope.tenant);
    if let Some(domain) = scope.domain.as_deref() {
        formatted.push_str(&format!(", domain={domain}"));
    }
    if let Some(object_set) = scope.object_set.as_deref() {
        formatted.push_str(&format!(", object_set={object_set}"));
    }
    formatted
}

pub fn root_tenant_scope(scope: &TenantPolicyScope) -> AdminResult<TenantPolicyScope> {
    let root_scope = TenantPolicyScope::new(scope.tenant.clone(), None::<String>, None::<String>);
    root_scope.validate()?;
    Ok(root_scope)
}

pub fn format_tenant_object_accounting_state(state: TenantObjectAccountingState) -> &'static str {
    match state {
        TenantObjectAccountingState::Active => "active",
        TenantObjectAccountingState::Deleted => "deleted",
    }
}

pub fn format_tenant_quota_reservation_state(state: TenantQuotaReservationState) -> &'static str {
    match state {
        TenantQuotaReservationState::Pending => "pending",
        TenantQuotaReservationState::Finalized => "finalized",
        TenantQuotaReservationState::Aborted => "aborted",
    }
}

pub fn default_namespace() -> NamespaceScope {
    NamespaceScope::with_defaults(Some("default"), None::<&str>, None::<&str>)
}

pub fn default_domain_name() -> &'static str {
    DEFAULT_DOMAIN
}

pub fn default_object_set_name() -> &'static str {
    DEFAULT_OBJECT_SET
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn redact_redis_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.to_string()
        }
        Err(_) => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::env;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use mooncake_metadata::{
        EtcdMetadataBackend, EtcdMetadataConfig, InMemoryMetadataBackend, RedisMetadataBackend,
        RedisMetadataConfig,
    };
    use mooncake_store_client::control_plane_pb;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        ClientStableId, CompatibilityDescriptor, MetadataBackend, ObjectKey, ObjectRoute,
        ReplicaRoute, ReplicaTier, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
        SegmentAnnouncement, SegmentLifecycleState, SegmentName, TenantObjectAccounting,
        TenantObjectAccountingState, TenantPolicy, TenantQuotaAbortOutcome,
        TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest, TenantQuotaPolicy,
        TenantQuotaReservation, TenantQuotaReservationOutcome, TenantQuotaReservationRequest,
        TenantQuotaReservationState, TenantQuotaState, METRICS_PORT_LABEL,
    };
    use parking_lot::Mutex;

    use super::*;

    struct NoTenantPolicyListBackend {
        inner: Arc<InMemoryMetadataBackend>,
    }

    impl NoTenantPolicyListBackend {
        fn new(inner: Arc<InMemoryMetadataBackend>) -> Self {
            Self { inner }
        }
    }

    impl MetadataBackend for NoTenantPolicyListBackend {
        fn route_namespace(&self) -> String {
            self.inner.route_namespace()
        }

        fn for_tenant(&self, tenant: &str) -> Option<Arc<dyn MetadataBackend>> {
            self.inner.for_tenant(tenant)
        }

        fn upsert_client_lease(
            &self,
            lease: &mooncake_store_core::ClientLease,
        ) -> mooncake_store_core::Result<()> {
            self.inner.upsert_client_lease(lease)
        }

        fn allocate_client_lease(
            &self,
            template: &mooncake_store_core::ClientLease,
        ) -> mooncake_store_core::Result<ClientRuntimeId> {
            self.inner.allocate_client_lease(template)
        }

        fn update_client_state(
            &self,
            runtime: &ClientRuntimeId,
            next: mooncake_store_core::ClientLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_client_state(runtime, next)
        }

        fn get_client_lease(
            &self,
            runtime: &ClientRuntimeId,
        ) -> mooncake_store_core::Result<Option<mooncake_store_core::ClientLease>> {
            self.inner.get_client_lease(runtime)
        }

        fn get_live_runtime_by_stable_id(
            &self,
            stable_id: &mooncake_store_core::ClientStableId,
        ) -> mooncake_store_core::Result<Option<mooncake_store_core::ClientLease>> {
            self.inner.get_live_runtime_by_stable_id(stable_id)
        }

        fn list_live_clients(
            &self,
        ) -> mooncake_store_core::Result<Vec<mooncake_store_core::ClientLease>> {
            self.inner.list_live_clients()
        }

        fn publish_segment(
            &self,
            segment: &mooncake_store_core::SegmentAnnouncement,
        ) -> mooncake_store_core::Result<()> {
            self.inner.publish_segment(segment)
        }

        fn unpublish_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &mooncake_store_core::SegmentName,
        ) -> mooncake_store_core::Result<()> {
            self.inner.unpublish_segment(owner, segment)
        }

        fn get_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &mooncake_store_core::SegmentName,
        ) -> mooncake_store_core::Result<Option<mooncake_store_core::SegmentAnnouncement>> {
            self.inner.get_segment(owner, segment)
        }

        fn list_segments(
            &self,
            owner: Option<&ClientRuntimeId>,
        ) -> mooncake_store_core::Result<Vec<mooncake_store_core::SegmentAnnouncement>> {
            self.inner.list_segments(owner)
        }

        fn update_segment_state(
            &self,
            owner: &ClientRuntimeId,
            segment: &mooncake_store_core::SegmentName,
            next: mooncake_store_core::SegmentLifecycleState,
        ) -> mooncake_store_core::Result<()> {
            self.inner.update_segment_state(owner, segment, next)
        }

        fn reserve_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &mooncake_store_core::SegmentName,
            length_bytes: u64,
        ) -> mooncake_store_core::Result<mooncake_store_core::SegmentReservation> {
            self.inner.reserve_segment(owner, segment, length_bytes)
        }

        fn release_segment(
            &self,
            owner: &ClientRuntimeId,
            segment: &mooncake_store_core::SegmentName,
            offset_bytes: u64,
            length_bytes: u64,
        ) -> mooncake_store_core::Result<()> {
            self.inner
                .release_segment(owner, segment, offset_bytes, length_bytes)
        }

        fn get_object_route(
            &self,
            key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
            self.inner.get_object_route(key)
        }

        fn list_object_routes(&self) -> mooncake_store_core::Result<Vec<ObjectRoute>> {
            self.inner.list_object_routes()
        }

        fn compare_and_swap_object_route(
            &self,
            key: &ObjectKey,
            expected: Option<mooncake_store_core::RouteVersion>,
            next: Option<&ObjectRoute>,
        ) -> mooncake_store_core::Result<mooncake_store_core::CasResult> {
            self.inner
                .compare_and_swap_object_route(key, expected, next)
        }

        fn get_route_policy(
            &self,
            domain: &RoutePolicyDomain,
        ) -> mooncake_store_core::Result<Option<RoutePolicy>> {
            self.inner.get_route_policy(domain)
        }

        fn put_route_policy_if_absent(
            &self,
            domain: &RoutePolicyDomain,
            policy: &RoutePolicy,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.put_route_policy_if_absent(domain, policy)
        }

        fn put_route_policy(
            &self,
            domain: &RoutePolicyDomain,
            policy: &RoutePolicy,
        ) -> mooncake_store_core::Result<()> {
            self.inner.put_route_policy(domain, policy)
        }

        fn delete_route_policy(
            &self,
            domain: &RoutePolicyDomain,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.delete_route_policy(domain)
        }

        fn list_route_policies(
            &self,
        ) -> mooncake_store_core::Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
            self.inner.list_route_policies()
        }

        fn get_tenant_policy(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Option<TenantPolicy>> {
            self.inner.get_tenant_policy(scope)
        }

        fn list_tenant_policies(
            &self,
            _tenant: Option<&str>,
        ) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
            Err(StoreError::Unsupported(
                "tenant policy listing is disabled in this test".to_string(),
            ))
        }

        fn put_tenant_policy(
            &self,
            policy: &TenantPolicy,
            expected_version: Option<u64>,
        ) -> mooncake_store_core::Result<TenantPolicy> {
            self.inner.put_tenant_policy(policy, expected_version)
        }

        fn delete_tenant_policy(
            &self,
            scope: &TenantPolicyScope,
            expected_version: Option<u64>,
        ) -> mooncake_store_core::Result<bool> {
            self.inner.delete_tenant_policy(scope, expected_version)
        }

        fn get_tenant_quota_state(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Option<TenantQuotaState>> {
            self.inner.get_tenant_quota_state(scope)
        }

        fn get_tenant_object_accounting(
            &self,
            key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<TenantObjectAccounting>> {
            self.inner.get_tenant_object_accounting(key)
        }

        fn get_tenant_quota_reservation(
            &self,
            reservation_id: &str,
        ) -> mooncake_store_core::Result<Option<TenantQuotaReservation>> {
            self.inner.get_tenant_quota_reservation(reservation_id)
        }

        fn list_tenant_eviction_candidates(
            &self,
            scope: &TenantPolicyScope,
            limit: usize,
        ) -> mooncake_store_core::Result<Vec<TenantObjectAccounting>> {
            self.inner.list_tenant_eviction_candidates(scope, limit)
        }

        fn list_tenant_quota_reservations(
            &self,
            scope: &TenantPolicyScope,
        ) -> mooncake_store_core::Result<Vec<TenantQuotaReservation>> {
            self.inner.list_tenant_quota_reservations(scope)
        }

        fn reserve_tenant_quota(
            &self,
            request: &TenantQuotaReservationRequest,
        ) -> mooncake_store_core::Result<TenantQuotaReservationOutcome> {
            self.inner.reserve_tenant_quota(request)
        }

        fn finalize_tenant_quota(
            &self,
            request: &TenantQuotaFinalizeRequest,
        ) -> mooncake_store_core::Result<TenantQuotaFinalizeOutcome> {
            self.inner.finalize_tenant_quota(request)
        }

        fn abort_tenant_quota(
            &self,
            reservation_id: &str,
        ) -> mooncake_store_core::Result<TenantQuotaAbortOutcome> {
            self.inner.abort_tenant_quota(reservation_id)
        }

        fn put_handoff(
            &self,
            handoff: &mooncake_store_core::HandoffPlan,
        ) -> mooncake_store_core::Result<()> {
            self.inner.put_handoff(handoff)
        }

        fn get_handoff(
            &self,
            stable_id: &mooncake_store_core::ClientStableId,
        ) -> mooncake_store_core::Result<Option<mooncake_store_core::HandoffPlan>> {
            self.inner.get_handoff(stable_id)
        }
    }

    fn test_service() -> AdminService {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        AdminService::new(backend, "memory://test", MetadataKeyspace::default())
    }

    struct RedisTestServer {
        child: Child,
        url: String,
        dir: PathBuf,
    }

    struct RedisEnvGuard {
        username: Option<String>,
        password: Option<String>,
    }

    impl RedisEnvGuard {
        fn clear_auth() -> Self {
            let guard = Self {
                username: env::var("MC_REDIS_USERNAME").ok(),
                password: env::var("MC_REDIS_PASSWORD").ok(),
            };
            env::remove_var("MC_REDIS_USERNAME");
            env::remove_var("MC_REDIS_PASSWORD");
            guard
        }
    }

    impl Drop for RedisEnvGuard {
        fn drop(&mut self) {
            if let Some(username) = &self.username {
                env::set_var("MC_REDIS_USERNAME", username);
            } else {
                env::remove_var("MC_REDIS_USERNAME");
            }
            if let Some(password) = &self.password {
                env::set_var("MC_REDIS_PASSWORD", password);
            } else {
                env::remove_var("MC_REDIS_PASSWORD");
            }
        }
    }

    impl RedisTestServer {
        fn start() -> Option<Self> {
            let _env_guard = RedisEnvGuard::clear_auth();
            if Command::new("redis-server")
                .arg("--version")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .ok()?
                .success()
                == false
            {
                return None;
            }

            let listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let port = listener.local_addr().ok()?.port();
            drop(listener);

            let dir = std::env::temp_dir().join(format!(
                "mooncake-admin-redis-test-{}-{port}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).ok()?;

            let child = Command::new("redis-server")
                .arg("--save")
                .arg("")
                .arg("--appendonly")
                .arg("no")
                .arg("--port")
                .arg(port.to_string())
                .arg("--bind")
                .arg("127.0.0.1")
                .arg("--dir")
                .arg(&dir)
                .arg("--dbfilename")
                .arg("dump.rdb")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;

            let url = format!("redis://127.0.0.1:{port}/0");
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if TcpStream::connect(("127.0.0.1", port)).is_ok() {
                    return Some(Self { child, url, dir });
                }
                sleep(Duration::from_millis(50));
            }
            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_dir_all(&dir);
            None
        }
    }

    impl Drop for RedisTestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    struct EtcdTestServer {
        child: Child,
        endpoint: String,
        dir: PathBuf,
    }

    fn etcd_ready(endpoint: &str) -> bool {
        let Some(port) = endpoint
            .rsplit(':')
            .next()
            .and_then(|value| value.parse::<u16>().ok())
        else {
            return false;
        };
        TcpStream::connect(("127.0.0.1", port)).is_ok()
    }

    impl EtcdTestServer {
        fn start() -> Option<Self> {
            let client_listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let client_port = client_listener.local_addr().ok()?.port();
            drop(client_listener);

            let peer_listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let peer_port = peer_listener.local_addr().ok()?.port();
            drop(peer_listener);

            let dir = std::env::temp_dir().join(format!(
                "mooncake-admin-etcd-test-{}-{client_port}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).ok()?;
            let endpoint = format!("http://127.0.0.1:{client_port}");
            let peer_url = format!("http://127.0.0.1:{peer_port}");
            let child = Command::new("etcd")
                .arg("--name")
                .arg("default")
                .arg("--data-dir")
                .arg(&dir)
                .arg("--listen-client-urls")
                .arg(&endpoint)
                .arg("--advertise-client-urls")
                .arg(&endpoint)
                .arg("--listen-peer-urls")
                .arg(&peer_url)
                .arg("--initial-advertise-peer-urls")
                .arg(&peer_url)
                .arg("--initial-cluster")
                .arg(format!("default={peer_url}"))
                .arg("--initial-cluster-state")
                .arg("new")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;

            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if etcd_ready(&endpoint) {
                    sleep(Duration::from_millis(100));
                    return Some(Self {
                        child,
                        endpoint,
                        dir,
                    });
                }
                sleep(Duration::from_millis(25));
            }
            let mut child = child;
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_dir_all(&dir);
            None
        }
    }

    impl Drop for EtcdTestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn put_quota_policy(service: &AdminService) {
        service
            .backend()
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                    spec: TenantPolicySpec {
                        quota: Some(TenantQuotaPolicy {
                            max_bytes: Some(1024),
                            max_objects: Some(16),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 1,
                    updated_by: "tester".to_string(),
                },
                None,
            )
            .expect("tenant policy should store");
    }

    fn reserve_quota(service: &AdminService, reservation_id: &str, key: &str, expires_at_ms: u64) {
        service
            .backend()
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: reservation_id.to_string(),
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                key: ObjectKey::new(key),
                expected_object_version: None,
                delta_bytes: 12,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms,
                created_at_ms: 5,
                writer_runtime: ClientRuntimeId::new("writer-a", ClientEpoch(1)),
            })
            .expect("reservation should store");
    }

    #[derive(Clone, Default)]
    struct FakeMigrationRpc {
        submit_results: Arc<Mutex<VecDeque<AdminResult<String>>>>,
        status_results: Arc<Mutex<VecDeque<AdminResult<MigrationExecutionProbe>>>>,
        route_results: Arc<Mutex<VecDeque<AdminResult<Option<ObjectRoute>>>>>,
        submit_calls: Arc<AtomicUsize>,
        status_calls: Arc<AtomicUsize>,
    }

    impl FakeMigrationRpc {
        fn submit_calls(&self) -> usize {
            self.submit_calls.load(Ordering::Relaxed)
        }

        fn status_calls(&self) -> usize {
            self.status_calls.load(Ordering::Relaxed)
        }
    }

    impl MigrationRpc for FakeMigrationRpc {
        fn submit_migration_task(
            &self,
            _lease: &ClientLease,
            _request: control_plane_pb::SubmitMigrationTaskRequest,
        ) -> AdminResult<String> {
            self.submit_calls.fetch_add(1, Ordering::Relaxed);
            self.submit_results
                .lock()
                .pop_front()
                .unwrap_or_else(|| Ok("execution-1".to_string()))
        }

        fn get_migration_execution_status(
            &self,
            _lease: &ClientLease,
            _request: control_plane_pb::GetMigrationExecutionStatusRequest,
        ) -> AdminResult<MigrationExecutionProbe> {
            self.status_calls.fetch_add(1, Ordering::Relaxed);
            self.status_results.lock().pop_front().unwrap_or_else(|| {
                Ok(MigrationExecutionProbe {
                    state: control_plane_pb::MigrationExecutionState::Succeeded,
                    attempts: 1,
                    last_error: String::new(),
                })
            })
        }

        fn get_route(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _key: &ObjectKey,
        ) -> AdminResult<Option<ObjectRoute>> {
            self.route_results.lock().pop_front().unwrap_or(Ok(None))
        }
    }

    fn test_migration_service_with_rpc(
        backend: Arc<dyn MetadataBackend>,
        rpc: Arc<dyn MigrationRpc>,
        config: MigrationQueueConfig,
    ) -> AdminService {
        AdminService::new_with_migration_support(
            backend,
            "memory://test",
            MetadataKeyspace::default(),
            config,
            rpc,
        )
    }

    fn live_lease(stable_id: &str, epoch: u64) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(epoch)),
            compatibility: CompatibilityDescriptor::default(),
            state: ClientLifecycleState::Active,
            endpoints: ClientEndpointSet {
                rpc_address: format!("127.0.0.1:{}", 18_000 + epoch),
                segment_name: Some(SegmentName::new(format!("{stable_id}-segment"))),
                labels: [
                    (
                        "control_addr".to_string(),
                        format!("http://127.0.0.1:{}", 19_000 + epoch),
                    ),
                    ("route".to_string(), "true".to_string()),
                    ("storage".to_string(), "true".to_string()),
                ]
                .into_iter()
                .collect(),
            },
            expires_at_ms: u64::MAX,
        }
    }

    #[test]
    fn tracing_target_discovery_uses_control_host_and_metrics_port_label() {
        let service = test_service();
        let mut lease = live_lease("store-a", 1);
        lease.endpoints.rpc_address = "192.0.2.10:17000".to_string();
        lease.endpoints.labels.insert(
            "control_addr".to_string(),
            "http://10.1.2.3:19001".to_string(),
        );
        lease
            .endpoints
            .labels
            .insert(METRICS_PORT_LABEL.to_string(), "19300".to_string());
        service
            .backend()
            .upsert_client_lease(&lease)
            .expect("lease should store");

        let targets = service
            .discover_tracing_targets()
            .expect("tracing targets should discover");

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].runtime, lease.runtime);
        assert_eq!(targets[0].metrics_url, "http://10.1.2.3:19300");
    }

    #[test]
    fn tracing_target_discovery_falls_back_to_rpc_host_and_default_metrics_port() {
        let service = test_service();
        let mut lease = live_lease("store-a", 1);
        lease.endpoints.rpc_address = "10.9.8.7:17000".to_string();
        lease.endpoints.labels.remove("control_addr");
        lease.endpoints.labels.remove(METRICS_PORT_LABEL);
        service
            .backend()
            .upsert_client_lease(&lease)
            .expect("lease should store");

        let targets = service
            .discover_tracing_targets()
            .expect("tracing targets should discover");

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].metrics_url, "http://10.9.8.7:9300");
    }

    #[test]
    fn tracing_target_discovery_ignores_non_store_client_leases() {
        let service = test_service();
        let mut lease = live_lease("bench-a", 1);
        lease
            .endpoints
            .labels
            .insert("route".to_string(), "false".to_string());
        lease
            .endpoints
            .labels
            .insert("storage".to_string(), "false".to_string());
        lease
            .endpoints
            .labels
            .insert(METRICS_PORT_LABEL.to_string(), "19300".to_string());
        service
            .backend()
            .upsert_client_lease(&lease)
            .expect("lease should store");

        let targets = service
            .discover_tracing_targets()
            .expect("tracing targets should discover");

        assert!(targets.is_empty());
    }

    #[test]
    fn tracing_fanout_calls_discovered_metrics_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test tracing listener should bind");
        let port = listener
            .local_addr()
            .expect("test tracing listener should expose address")
            .port();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener
                .accept()
                .expect("test tracing server should accept request");
            let mut request = [0; 512];
            let read = stream
                .read(&mut request)
                .expect("test tracing server should read request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /tracing/off HTTP/1.1"));
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 17\r\nConnection: close\r\n\r\n{\"enabled\":false}",
                )
                .expect("test tracing server should reply");
        });
        let service = test_service();
        let mut lease = live_lease("store-a", 1);
        lease.endpoints.labels.insert(
            "control_addr".to_string(),
            "http://127.0.0.1:19001".to_string(),
        );
        lease
            .endpoints
            .labels
            .insert(METRICS_PORT_LABEL.to_string(), port.to_string());
        service
            .backend()
            .upsert_client_lease(&lease)
            .expect("lease should store");

        let response = service
            .update_tracing(TracingAction::Off, TracingUpdateRequest::default())
            .expect("tracing fanout should succeed");

        server.join().expect("test tracing server should finish");
        assert_eq!(response.total, 1);
        assert_eq!(response.ok, 1);
        assert_eq!(response.failed, 0);
        assert_eq!(
            response.nodes[0]
                .status
                .as_ref()
                .and_then(|value| value.get("enabled")),
            Some(&serde_json::Value::Bool(false))
        );
    }

    fn wait_for_task_state(
        service: &AdminService,
        task_id: &str,
        expected: RouteMigrationTaskState,
    ) -> RouteMigrationTaskStatusResponse {
        let started = now_ms();
        loop {
            let response = service
                .get_route_migration_task(task_id)
                .expect("task status should read");
            if response.state == expected {
                return response;
            }
            assert!(
                now_ms().saturating_sub(started) < 5_000,
                "task {task_id} should reach {expected:?}, current={:?}",
                response.state
            );
            sleep(Duration::from_millis(10));
        }
    }

    fn sample_move_request() -> RouteMigrationTaskSubmitRequest {
        RouteMigrationTaskSubmitRequest {
            authority: "authority-a".to_string(),
            tenant: "tenant-a".to_string(),
            domain: None,
            object_set: None,
            key: "object-a".to_string(),
            source_segment: "segment-a".to_string(),
            target_segments: vec!["segment-b".to_string()],
            task_executor: "executor-a".to_string(),
            max_retries: None,
        }
    }

    fn sample_copy_request() -> RouteMigrationTaskSubmitRequest {
        RouteMigrationTaskSubmitRequest {
            authority: "authority-a".to_string(),
            tenant: "tenant-a".to_string(),
            domain: None,
            object_set: None,
            key: "object-a".to_string(),
            source_segment: "segment-a".to_string(),
            target_segments: vec!["segment-b".to_string()],
            task_executor: "executor-a".to_string(),
            max_retries: None,
        }
    }

    fn sample_copy_request_with_targets(
        target_segments: &[&str],
    ) -> RouteMigrationTaskSubmitRequest {
        let mut request = sample_copy_request();
        request.target_segments = target_segments
            .iter()
            .map(|segment| (*segment).to_string())
            .collect();
        request
    }

    fn sample_move_request_with_max_retries(max_retries: u32) -> RouteMigrationTaskSubmitRequest {
        let mut request = sample_move_request();
        request.max_retries = Some(max_retries);
        request
    }

    fn sample_move_request_in_scope(
        domain: &str,
        object_set: &str,
    ) -> RouteMigrationTaskSubmitRequest {
        let mut request = sample_move_request();
        request.domain = Some(domain.to_string());
        request.object_set = Some(object_set.to_string());
        request
    }

    fn sample_task_record(
        mode: RouteMigrationMode,
        source_segment: &str,
        target_segments: &[&str],
    ) -> RouteMigrationTaskRecord {
        RouteMigrationTaskRecord {
            task_id: "route-migration-test".to_string(),
            namespace: "mooncake/routes".to_string(),
            authority: "authority-a".to_string(),
            tenant: "tenant-a".to_string(),
            domain: None,
            object_set: None,
            key: "object-a".to_string(),
            mode,
            source_segment: source_segment.to_string(),
            target_segments: target_segments
                .iter()
                .map(|segment| (*segment).to_string())
                .collect(),
            task_executor: "executor-a".to_string(),
            state: RouteMigrationTaskState::Pending,
            attempts: 0,
            status_failures: 0,
            max_retries: 3,
            execution_id: None,
            next_retry_at_ms: None,
            last_error: String::new(),
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }

    fn sample_active_route(
        key: &str,
        source_segment: &str,
        target_segments: &[&str],
    ) -> ObjectRoute {
        let mut replicas = Vec::new();
        if !source_segment.is_empty() {
            replicas.push(ReplicaRoute {
                owner: ClientRuntimeId::new("storage-a", ClientEpoch(1)),
                segment_name: SegmentName::new(source_segment),
                offset: Some(0),
                segment_offset: 0,
                length: 12,
                checksum: None,
                tier: ReplicaTier::Nvme,
                priority: 0,
            });
        }
        for (index, segment) in target_segments.iter().enumerate() {
            replicas.push(ReplicaRoute {
                owner: ClientRuntimeId::new("storage-b", ClientEpoch(1)),
                segment_name: SegmentName::new(*segment),
                offset: Some(64 * (index as u64 + 1)),
                segment_offset: 64 * (index as u64 + 1),
                length: 12,
                checksum: None,
                tier: ReplicaTier::Nvme,
                priority: (index + 1) as u16,
            });
        }
        ObjectRoute {
            key: ObjectKey::new(format!("tenant-a::{key}")),
            namespace: Some(NamespaceScope::with_defaults(
                Some("tenant-a"),
                None::<&str>,
                None::<&str>,
            )),
            logical_key: Some(key.to_string()),
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas,
            cold_backing: None,
        }
    }

    fn sample_active_route_in_scope(
        tenant: &str,
        domain: &str,
        object_set: &str,
        key: &str,
        source_segment: &str,
        target_segments: &[&str],
    ) -> ObjectRoute {
        let mut route = sample_active_route(key, source_segment, target_segments);
        route.key = ObjectKey::from_logical_id(&LogicalObjectId::new(
            NamespaceScope::with_defaults(Some(tenant), Some(domain), Some(object_set)),
            key.to_string(),
        ));
        route.namespace = Some(NamespaceScope::with_defaults(
            Some(tenant),
            Some(domain),
            Some(object_set),
        ));
        route.sharing_scope = Some(tenant.to_string());
        route
    }

    #[test]
    fn root_scope_helpers_collapse_nested_scope() {
        let nested = TenantPolicyScope::new("tenant-a", Some("domain-a"), Some("set-a"));
        let root = root_tenant_scope(&nested).expect("root scope should build");
        assert_eq!(root.tenant, "tenant-a");
        assert!(root.domain.is_none());
        assert!(root.object_set.is_none());
    }

    #[test]
    fn admin_service_lists_tenant_policies_with_backend_filter() {
        let service = test_service();
        let tenant_a_root = TenantPolicy {
            scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 10,
            updated_by: "admin".to_string(),
        };
        let tenant_a_domain = TenantPolicy {
            scope: TenantPolicyScope::new("tenant-a", Some("domain-a"), None::<String>),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 20,
            updated_by: "admin".to_string(),
        };
        let tenant_b = TenantPolicy {
            scope: TenantPolicyScope::new("tenant-b", None::<String>, None::<String>),
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 30,
            updated_by: "admin".to_string(),
        };
        service
            .backend()
            .put_tenant_policy(&tenant_a_domain, None)
            .expect("tenant-a domain policy should store");
        service
            .backend()
            .put_tenant_policy(&tenant_b, None)
            .expect("tenant-b policy should store");
        service
            .backend()
            .put_tenant_policy(&tenant_a_root, None)
            .expect("tenant-a root policy should store");

        assert_eq!(
            service
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy list should succeed"),
            vec![tenant_a_root.clone(), tenant_a_domain.clone()]
        );
        assert_eq!(
            service
                .list_tenant_policies(Some("tenant-b"))
                .expect("tenant-b policy list should succeed"),
            vec![tenant_b.clone()]
        );
        assert!(service
            .list_tenant_policies(Some("missing"))
            .expect("missing tenant policy list should succeed")
            .is_empty());
        assert_eq!(
            service
                .list_tenant_policies(None)
                .expect("full policy list should succeed"),
            vec![tenant_a_root, tenant_a_domain, tenant_b]
        );
    }

    #[test]
    fn admin_service_resolves_effective_policy_without_listing_all_policies() {
        let inner = Arc::new(InMemoryMetadataBackend::new());
        inner
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                    spec: TenantPolicySpec {
                        routing: Some(TenantRoutePolicy {
                            route_topk: Some(3),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 10,
                    updated_by: "admin".to_string(),
                },
                None,
            )
            .expect("root tenant policy should be stored");
        inner
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new("tenant-a", Some("default"), Some("default")),
                    spec: TenantPolicySpec {
                        routing: Some(TenantRoutePolicy {
                            route_topk: Some(4),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 20,
                    updated_by: "admin".to_string(),
                },
                None,
            )
            .expect("object-set tenant policy should be stored");

        let backend: Arc<dyn MetadataBackend> = Arc::new(NoTenantPolicyListBackend::new(inner));
        let service = AdminService::new(backend, "memory://test", MetadataKeyspace::default());
        let effective = service
            .get_tenant_policy("tenant-a", None, None, true)
            .expect("effective policy read should succeed without listing all policies");

        assert!(effective.found);
        assert_eq!(
            effective
                .effective_spec
                .expect("effective spec should exist")
                .routing,
            Some(TenantRoutePolicy {
                route_topk: Some(4),
            })
        );
    }

    #[test]
    fn admin_service_reads_quota_state_and_reservations_at_root_scope() {
        let service = test_service();
        put_quota_policy(&service);
        service
            .backend()
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "res-a".to_string(),
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                key: ObjectKey::new("tenant-a::object-a"),
                expected_object_version: None,
                delta_bytes: 10,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms: 10,
                created_at_ms: 5,
                writer_runtime: ClientRuntimeId::new("writer-a", ClientEpoch(1)),
            })
            .expect("reservation should store");

        let quota = service
            .get_tenant_quota_state("tenant-a", Some("domain-a"), Some("set-a"))
            .expect("quota state should read");
        assert_eq!(
            quota.scope,
            TenantPolicyScope::new("tenant-a", None::<String>, None::<String>)
        );
        assert_eq!(
            quota
                .state
                .expect("quota state should exist")
                .pending_reserved_bytes,
            10
        );

        let reservations = service
            .list_tenant_quota_reservations(
                "tenant-a",
                Some("domain-a"),
                Some("set-a"),
                Some(TenantQuotaReservationState::Pending),
            )
            .expect("reservations should read");
        assert_eq!(reservations.scope.tenant, "tenant-a");
        assert_eq!(reservations.count, 1);
        assert_eq!(reservations.reservations[0].reservation_id, "res-a");
    }

    #[test]
    fn admin_service_reads_object_accounting_from_scoped_key() {
        let service = test_service();
        put_quota_policy(&service);
        reserve_quota(
            &service,
            "res-a",
            "tenant-a::ns/domain-a/set-a/object-a",
            10,
        );
        service
            .backend()
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "res-a".to_string(),
                expected_object_version: None,
                committed_length: Some(12),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 6,
                updated_by: "writer-a".to_string(),
            })
            .expect("finalize should succeed");

        let accounting = service
            .get_tenant_object_accounting("tenant-a", Some("domain-a"), Some("set-a"), "object-a")
            .expect("object accounting should read");
        assert_eq!(accounting.scope.tenant, "tenant-a");
        assert_eq!(accounting.key, "tenant-a::ns/domain-a/set-a/object-a");
        assert_eq!(
            accounting
                .accounting
                .expect("accounting should exist")
                .committed_length,
            12
        );
    }

    #[test]
    fn admin_service_abort_returns_true_for_pending_reservation() {
        let service = test_service();
        put_quota_policy(&service);
        reserve_quota(&service, "res-pending", "tenant-a::object-a", u64::MAX);

        let response = service
            .abort_tenant_quota_reservation("tenant-a", None, None, "res-pending", false)
            .expect("abort should succeed");
        assert!(response.aborted);

        let reservations = service
            .list_tenant_quota_reservations("tenant-a", None, None, None)
            .expect("reservations should read");
        assert_eq!(
            reservations.reservations[0].state,
            TenantQuotaReservationState::Aborted
        );
    }

    #[test]
    fn admin_service_reconcile_aborts_expired_pending_reservations() {
        let service = test_service();
        put_quota_policy(&service);
        reserve_quota(&service, "res-expired", "tenant-a::object-expired", 0);

        let report = service
            .reconcile_tenant_quota_reservations("tenant-a", None, None, false)
            .expect("reconcile should succeed");
        assert_eq!(report.aborted, 1);
        assert_eq!(report.finalized, 0);
        assert_eq!(report.skipped, 0);
        assert_eq!(report.actions[0].reservation_id, "res-expired");
        assert_eq!(report.actions[0].action, "abort");

        let reservations = service
            .list_tenant_quota_reservations("tenant-a", None, None, None)
            .expect("reservations should read");
        assert_eq!(
            reservations.reservations[0].state,
            TenantQuotaReservationState::Aborted
        );
    }

    #[test]
    fn admin_service_reconcile_finalizes_visible_route_reservations() {
        let service = test_service();
        put_quota_policy(&service);
        reserve_quota(&service, "res-seed", "tenant-a::object-visible", u64::MAX);
        service
            .backend()
            .compare_and_swap_object_route(
                &ObjectKey::new("tenant-a::object-visible"),
                None,
                Some(&ObjectRoute {
                    key: ObjectKey::new("tenant-a::object-visible"),
                    namespace: Some(NamespaceScope::with_defaults(
                        Some("tenant-a"),
                        None::<&str>,
                        None::<&str>,
                    )),
                    logical_key: Some("object-visible".to_string()),
                    canonical_key: None,
                    sharing_scope: None,
                    qos_tier: None,
                    version: RouteVersion(1),
                    state: RouteState::Active,
                    compatibility: CompatibilityDescriptor::default(),
                    replicas: vec![ReplicaRoute {
                        owner: ClientRuntimeId::new("storage-a", ClientEpoch(1)),
                        segment_name: SegmentName::new("segment-a"),
                        offset: Some(0),
                        segment_offset: 0,
                        length: 12,
                        checksum: None,
                        tier: ReplicaTier::Nvme,
                        priority: 0,
                    }],
                    cold_backing: None,
                }),
            )
            .expect("route cas should succeed");
        service
            .backend()
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "res-seed".to_string(),
                expected_object_version: None,
                committed_length: Some(12),
                route_version: Some(RouteVersion(1)),
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 6,
                updated_by: "writer-a".to_string(),
            })
            .expect("seed finalize should succeed");
        service
            .backend()
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "res-visible".to_string(),
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                key: ObjectKey::new("tenant-a::object-visible"),
                expected_object_version: Some(1),
                delta_bytes: 0,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms: u64::MAX,
                created_at_ms: 7,
                writer_runtime: ClientRuntimeId::new("writer-a", ClientEpoch(1)),
            })
            .expect("visible reservation should store");

        let report = service
            .reconcile_tenant_quota_reservations("tenant-a", None, None, false)
            .expect("reconcile should succeed");
        assert_eq!(report.finalized, 1);
        assert_eq!(report.aborted, 0);
        assert_eq!(report.actions[0].reservation_id, "res-visible");
        assert_eq!(report.actions[0].action, "finalize-visible-route");

        let reservations = service
            .list_tenant_quota_reservations("tenant-a", None, None, None)
            .expect("reservations should read");
        let visible = reservations
            .reservations
            .into_iter()
            .find(|reservation| reservation.reservation_id == "res-visible")
            .expect("visible reservation should exist");
        assert_eq!(visible.state, TenantQuotaReservationState::Finalized);
    }

    #[test]
    fn admin_service_reconcile_due_stale_segments_cleans_missing_lease_owner() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/admin-stale-maint-missing");
        let backend = Arc::new(
            RedisMetadataBackend::new(
                RedisMetadataConfig::new(server.url.clone()).keyspace(keyspace.clone()),
            )
            .expect("redis backend should initialize"),
        );
        let service = AdminService::new(backend.clone(), server.url.clone(), keyspace.clone());

        let dead_runtime = ClientRuntimeId::new("dead-owner", ClientEpoch(9));
        backend
            .upsert_client_lease(&ClientLease {
                runtime: dead_runtime.clone(),
                state: ClientLifecycleState::Active,
                compatibility: CompatibilityDescriptor::default(),
                endpoints: ClientEndpointSet::default(),
                expires_at_ms: super::now_ms() + 60_000,
            })
            .expect("dead owner lease should publish before segment");
        backend
            .publish_segment(&SegmentAnnouncement {
                owner: dead_runtime.clone(),
                segment_name: SegmentName::new("dead-segment"),
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 256,
                used_bytes: 64,
                target_chunks: Vec::new(),
                state: SegmentLifecycleState::Active,
                alignment_bytes: 16,
                tags: vec!["dram".to_string()],
            })
            .expect("dead segment publish should succeed");
        let mut connection = redis::Client::open(server.url.clone())
            .expect("redis client should open")
            .get_connection()
            .expect("redis connection should open");
        redis::cmd("HDEL")
            .arg(keyspace.client(&dead_runtime))
            .arg(keyspace.client_lease_field())
            .query::<()>(&mut connection)
            .expect("lease field delete should simulate missing lease owner");
        backend
            .refresh_client_lease_expiry(&dead_runtime, super::now_ms().saturating_sub(1))
            .expect("expiry queue seed should succeed");

        let report = service
            .reconcile_due_stale_segments(8)
            .expect("maintenance reconcile should succeed");
        assert_eq!(report.due_entries, 1);
        assert_eq!(report.cleaned_missing_lease, 1);
        assert_eq!(report.removed_segment_keys, 1);
        assert!(backend
            .list_segments(None)
            .expect("segment listing should succeed")
            .is_empty());
        assert!(backend
            .list_due_client_lease_expiries(super::now_ms(), 8)
            .expect("expiry queue should be queryable")
            .is_empty());
    }

    #[test]
    fn admin_service_reconcile_due_stale_segments_skips_live_owner() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/admin-stale-maint-live");
        let backend = Arc::new(
            RedisMetadataBackend::new(
                RedisMetadataConfig::new(server.url.clone()).keyspace(keyspace.clone()),
            )
            .expect("redis backend should initialize"),
        );
        let service = AdminService::new(backend.clone(), server.url.clone(), keyspace);

        let live_runtime = ClientRuntimeId::new("live-owner", ClientEpoch(3));
        backend
            .upsert_client_lease(&mooncake_store_core::ClientLease {
                runtime: live_runtime.clone(),
                state: mooncake_store_core::ClientLifecycleState::Active,
                compatibility: CompatibilityDescriptor::default(),
                endpoints: mooncake_store_core::ClientEndpointSet::default(),
                expires_at_ms: super::now_ms().saturating_add(60_000),
            })
            .expect("live lease publish should succeed");
        let segment = SegmentAnnouncement {
            owner: live_runtime.clone(),
            segment_name: SegmentName::new("live-segment"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        backend
            .publish_segment(&segment)
            .expect("live segment publish should succeed");
        backend
            .refresh_client_lease_expiry(&live_runtime, super::now_ms().saturating_sub(1))
            .expect("expiry queue seed should succeed");

        let report = service
            .reconcile_due_stale_segments(8)
            .expect("maintenance reconcile should succeed");
        assert_eq!(report.due_entries, 1);
        assert_eq!(report.skipped_live, 1);
        assert_eq!(report.removed_segment_keys, 0);
        assert_eq!(
            backend
                .list_segments(None)
                .expect("segment listing should succeed"),
            vec![segment]
        );
        assert!(backend
            .list_due_client_lease_expiries(super::now_ms(), 8)
            .expect("expiry queue should be queryable")
            .is_empty());
    }

    #[test]
    fn admin_service_reconcile_due_stale_segments_cleans_missing_lease_owner_in_etcd() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/admin-stale-maint-missing-etcd");
        let backend = Arc::new(
            EtcdMetadataBackend::from_config(
                EtcdMetadataConfig::new([server.endpoint.clone()]).keyspace(keyspace.clone()),
            )
            .expect("etcd backend should initialize"),
        );
        let service = AdminService::new(
            backend.clone(),
            format!("etcd://{}", server.endpoint),
            keyspace,
        );

        let dead_runtime = ClientRuntimeId::new("dead-owner", ClientEpoch(9));
        backend
            .publish_segment(&SegmentAnnouncement {
                owner: dead_runtime.clone(),
                segment_name: SegmentName::new("dead-segment"),
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 256,
                used_bytes: 64,
                target_chunks: Vec::new(),
                state: SegmentLifecycleState::Active,
                alignment_bytes: 16,
                tags: vec!["dram".to_string()],
            })
            .expect("dead segment publish should succeed");
        backend
            .refresh_client_lease_expiry(&dead_runtime, super::now_ms().saturating_sub(1))
            .expect("expiry queue seed should succeed");

        let report = service
            .reconcile_due_stale_segments(8)
            .expect("maintenance reconcile should succeed");
        assert_eq!(report.due_entries, 1);
        assert_eq!(report.cleaned_missing_lease, 1);
        assert_eq!(report.removed_segment_keys, 1);
        assert!(backend
            .list_segments(None)
            .expect("segment listing should succeed")
            .is_empty());
        assert!(backend
            .list_due_client_lease_expiries(super::now_ms(), 8)
            .expect("expiry queue should be queryable")
            .is_empty());
    }

    #[test]
    fn admin_service_reconcile_due_stale_segments_skips_live_owner_in_etcd() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/admin-stale-maint-live-etcd");
        let backend = Arc::new(
            EtcdMetadataBackend::from_config(
                EtcdMetadataConfig::new([server.endpoint.clone()]).keyspace(keyspace.clone()),
            )
            .expect("etcd backend should initialize"),
        );
        let service = AdminService::new(
            backend.clone(),
            format!("etcd://{}", server.endpoint),
            keyspace,
        );

        let live_runtime = ClientRuntimeId::new("live-owner", ClientEpoch(3));
        backend
            .upsert_client_lease(&mooncake_store_core::ClientLease {
                runtime: live_runtime.clone(),
                state: mooncake_store_core::ClientLifecycleState::Active,
                compatibility: CompatibilityDescriptor::default(),
                endpoints: mooncake_store_core::ClientEndpointSet::default(),
                expires_at_ms: super::now_ms().saturating_add(60_000),
            })
            .expect("live lease publish should succeed");
        let segment = SegmentAnnouncement {
            owner: live_runtime.clone(),
            segment_name: SegmentName::new("live-segment"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        backend
            .publish_segment(&segment)
            .expect("live segment publish should succeed");
        backend
            .refresh_client_lease_expiry(&live_runtime, super::now_ms().saturating_sub(1))
            .expect("expiry queue seed should succeed");

        let report = service
            .reconcile_due_stale_segments(8)
            .expect("maintenance reconcile should succeed");
        assert_eq!(report.due_entries, 1);
        assert_eq!(report.skipped_live, 1);
        assert_eq!(report.removed_segment_keys, 0);
        assert_eq!(
            backend
                .list_segments(None)
                .expect("segment listing should succeed"),
            vec![segment]
        );
        assert!(backend
            .list_due_client_lease_expiries(super::now_ms(), 8)
            .expect("expiry queue should be queryable")
            .is_empty());
    }

    #[test]
    fn admin_service_route_migration_succeeds_after_executor_reports_success() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![
                    Ok(MigrationExecutionProbe {
                        state: control_plane_pb::MigrationExecutionState::Running,
                        attempts: 1,
                        last_error: String::new(),
                    }),
                    Ok(MigrationExecutionProbe {
                        state: control_plane_pb::MigrationExecutionState::Succeeded,
                        attempts: 1,
                        last_error: String::new(),
                    }),
                ]
                .into(),
            )),
            route_results: Arc::new(Mutex::new(VecDeque::new())),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 3,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.attempts, 1);
        assert_eq!(status.execution_id.as_deref(), Some("execution-1"));
        assert!(status.last_error.is_empty());
    }

    #[test]
    fn admin_service_route_migration_marks_succeeded_when_route_is_already_visible() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(
                vec![Ok(Some(sample_active_route(
                    "object-a",
                    "",
                    &["segment-b"],
                )))]
                .into(),
            )),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.attempts, 1);
        assert_eq!(status.execution_id.as_deref(), Some("execution-1"));
    }

    #[test]
    fn admin_service_route_migration_retries_and_fails_after_budget() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(
                vec![
                    Err(StoreError::Transport("executor down".to_string())),
                    Err(StoreError::Transport("executor down".to_string())),
                ]
                .into(),
            )),
            status_results: Arc::new(Mutex::new(VecDeque::new())),
            route_results: Arc::new(Mutex::new(vec![Ok(None), Ok(None)].into())),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Failed,
        );
        assert_eq!(status.attempts, 2);
        assert!(status.last_error.contains("executor down"));
    }

    #[test]
    fn admin_service_route_migration_uses_metadata_fallback_when_authority_is_missing() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        backend
            .compare_and_swap_object_route(
                &ObjectKey::new("tenant-a::object-a"),
                None,
                Some(&sample_active_route("object-a", "", &["segment-b"])),
            )
            .expect("route should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(VecDeque::new())),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.attempts, 1);
    }

    #[test]
    fn admin_service_route_migration_checks_other_live_authorities_before_failing() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("preferred authority lease should store");
        backend
            .upsert_client_lease(&live_lease("authority-b", 1))
            .expect("secondary authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(
                vec![
                    Ok(None),
                    Ok(Some(sample_active_route("object-a", "", &["segment-b"]))),
                ]
                .into(),
            )),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.attempts, 1);
    }

    #[test]
    fn admin_service_route_migration_copy_requires_source_visibility_for_success() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(
                vec![Ok(Some(sample_active_route(
                    "object-a",
                    "",
                    &["segment-b"],
                )))]
                .into(),
            )),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Copy, sample_copy_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Failed,
        );
        assert!(status.last_error.contains("requires source visibility"));
    }

    #[test]
    fn admin_service_route_migration_fails_on_route_conflict_after_executor_loss() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(
                vec![Ok(Some(sample_active_route(
                    "object-a",
                    "segment-a",
                    &["segment-b"],
                )))]
                .into(),
            )),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc.clone(),
            MigrationQueueConfig {
                default_max_retries: 3,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(
                RouteMigrationMode::Copy,
                sample_copy_request_with_targets(&["segment-b", "segment-c"]),
            )
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Failed,
        );
        assert_eq!(status.attempts, 1);
        assert_eq!(status.execution_id.as_deref(), Some("execution-1"));
        assert!(status.last_error.contains("all requested targets"));
        assert_eq!(rpc.submit_calls(), 1);
        assert_eq!(rpc.status_calls(), 1);
    }

    #[test]
    fn admin_service_route_migration_evaluates_terminal_route_states() {
        let copy_record = sample_task_record(
            RouteMigrationMode::Copy,
            "segment-a",
            &["segment-b", "segment-c"],
        );
        assert!(matches!(
            evaluate_route_completion(
                &copy_record,
                Some(&sample_active_route(
                    "object-a",
                    "segment-a",
                    &["segment-b", "segment-c"]
                )),
            ),
            RouteCompletionCheck::Completed
        ));
        let copy_conflict = evaluate_route_completion(
            &copy_record,
            Some(&sample_active_route(
                "object-a",
                "segment-a",
                &["segment-b"],
            )),
        );
        assert!(matches!(copy_conflict, RouteCompletionCheck::Conflict(_)));
        assert!(format!("{copy_conflict:?}").contains("Conflict"));

        let move_record = sample_task_record(RouteMigrationMode::Move, "segment-a", &["segment-b"]);
        assert!(matches!(
            evaluate_route_completion(
                &move_record,
                Some(&sample_active_route("object-a", "", &["segment-b"])),
            ),
            RouteCompletionCheck::Completed
        ));
        let move_conflict_both_visible = evaluate_route_completion(
            &move_record,
            Some(&sample_active_route(
                "object-a",
                "segment-a",
                &["segment-b"],
            )),
        );
        assert!(matches!(
            move_conflict_both_visible,
            RouteCompletionCheck::Conflict(_)
        ));
        let move_conflict_lost_source = evaluate_route_completion(
            &move_record,
            Some(&sample_active_route("object-a", "", &[])),
        );
        assert!(matches!(
            move_conflict_lost_source,
            RouteCompletionCheck::Conflict(_)
        ));
    }

    #[test]
    fn admin_service_route_migration_status_retry_does_not_resubmit_task() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![
                    Err(StoreError::Transport("temporary status miss".to_string())),
                    Ok(MigrationExecutionProbe {
                        state: control_plane_pb::MigrationExecutionState::Succeeded,
                        attempts: 1,
                        last_error: String::new(),
                    }),
                ]
                .into(),
            )),
            route_results: Arc::new(Mutex::new(vec![Ok(None)].into())),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc.clone(),
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.attempts, 1);
        assert_eq!(rpc.submit_calls(), 1);
    }

    #[test]
    fn admin_service_route_migration_status_failures_exhaust_retry_budget() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![
                    Err(StoreError::Transport("status miss 1".to_string())),
                    Err(StoreError::Transport("status miss 2".to_string())),
                ]
                .into(),
            )),
            route_results: Arc::new(Mutex::new(vec![Ok(None), Ok(None)].into())),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc.clone(),
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(RouteMigrationMode::Move, sample_move_request())
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Failed,
        );
        assert_eq!(status.attempts, 1);
        assert!(status.last_error.contains("status miss 2"));
        assert_eq!(rpc.submit_calls(), 1);
        assert_eq!(rpc.status_calls(), 2);
    }

    #[test]
    fn admin_service_route_migration_rejects_zero_max_retries() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let service = test_migration_service_with_rpc(
            backend,
            Arc::new(FakeMigrationRpc::default()),
            MigrationQueueConfig::default(),
        );

        let error = service
            .submit_route_migration_task(
                RouteMigrationMode::Move,
                sample_move_request_with_max_retries(0),
            )
            .expect_err("zero max_retries should be rejected");
        assert!(matches!(error, StoreError::InvalidState(_)));
        assert!(error.to_string().contains("max_retries"));
    }

    #[test]
    fn admin_service_route_migration_rejects_missing_required_fields() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let service = test_migration_service_with_rpc(
            backend,
            Arc::new(FakeMigrationRpc::default()),
            MigrationQueueConfig::default(),
        );

        let mut missing_authority = sample_move_request();
        missing_authority.authority.clear();
        assert!(service
            .submit_route_migration_task(RouteMigrationMode::Move, missing_authority)
            .expect_err("missing authority should fail")
            .to_string()
            .contains("authority"));

        let mut missing_tenant = sample_move_request();
        missing_tenant.tenant.clear();
        assert!(service
            .submit_route_migration_task(RouteMigrationMode::Move, missing_tenant)
            .expect_err("missing tenant should fail")
            .to_string()
            .contains("tenant"));

        let mut missing_key = sample_move_request();
        missing_key.key.clear();
        assert!(service
            .submit_route_migration_task(RouteMigrationMode::Move, missing_key)
            .expect_err("missing key should fail")
            .to_string()
            .contains("key"));

        let mut missing_source = sample_move_request();
        missing_source.source_segment.clear();
        assert!(service
            .submit_route_migration_task(RouteMigrationMode::Move, missing_source)
            .expect_err("missing source_segment should fail")
            .to_string()
            .contains("source_segment"));

        let mut missing_executor = sample_move_request();
        missing_executor.task_executor.clear();
        assert!(service
            .submit_route_migration_task(RouteMigrationMode::Move, missing_executor)
            .expect_err("missing task_executor should fail")
            .to_string()
            .contains("task_executor"));
    }

    #[test]
    fn admin_service_route_migration_accepts_copy_with_multiple_targets() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(
                vec![Ok(Some(sample_active_route(
                    "object-a",
                    "segment-a",
                    &["segment-b", "segment-c"],
                )))]
                .into(),
            )),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(
                RouteMigrationMode::Copy,
                sample_copy_request_with_targets(&["segment-b", "segment-c"]),
            )
            .expect("migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.attempts, 1);
        assert_eq!(status.target_segments, vec!["segment-b", "segment-c"]);
    }

    #[test]
    fn admin_service_route_migration_respects_domain_and_object_set() {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        backend
            .compare_and_swap_object_route(
                &ObjectKey::from_logical_id(&LogicalObjectId::new(
                    NamespaceScope::with_defaults(
                        Some("tenant-a"),
                        Some("domain-a"),
                        Some("set-a"),
                    ),
                    "object-a".to_string(),
                )),
                None,
                Some(&sample_active_route_in_scope(
                    "tenant-a",
                    "domain-a",
                    "set-a",
                    "object-a",
                    "",
                    &["segment-b"],
                )),
            )
            .expect("scoped route should store");
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(VecDeque::new())),
            submit_calls: Arc::new(AtomicUsize::new(0)),
            status_calls: Arc::new(AtomicUsize::new(0)),
        });
        let service = test_migration_service_with_rpc(
            backend,
            rpc,
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
        );

        let submitted = service
            .submit_route_migration_task(
                RouteMigrationMode::Move,
                sample_move_request_in_scope("domain-a", "set-a"),
            )
            .expect("scoped migration task submit should succeed");
        let status = wait_for_task_state(
            &service,
            &submitted.task_id,
            RouteMigrationTaskState::Succeeded,
        );
        assert_eq!(status.domain.as_deref(), Some("domain-a"));
        assert_eq!(status.object_set.as_deref(), Some("set-a"));
    }

    fn hard_isolated_service() -> AdminService {
        let backend: Arc<dyn MetadataBackend> =
            Arc::new(InMemoryMetadataBackend::new_hard_isolated());
        AdminService::new(
            backend,
            "memory://hard-isolated",
            MetadataKeyspace::default(),
        )
    }

    #[test]
    fn hard_isolated_tenant_policies_are_isolated() {
        let service = hard_isolated_service();
        let patch_a = PolicyPatchInput {
            route_topk: Some(3),
            ..PolicyPatchInput::default()
        };
        let patch_b = PolicyPatchInput {
            route_topk: Some(5),
            ..PolicyPatchInput::default()
        };

        // Write policy for tenant-a via service (goes through backend_for_tenant)
        service
            .set_tenant_policy("tenant-a", None, None, patch_a, None, "admin")
            .expect("set tenant-a policy should succeed");

        // Write policy for tenant-b
        service
            .set_tenant_policy("tenant-b", None, None, patch_b, None, "admin")
            .expect("set tenant-b policy should succeed");

        // tenant-a can read its own policy
        let resp = service
            .get_tenant_policy("tenant-a", None, None, false)
            .expect("get tenant-a policy should succeed");
        assert!(resp.found);
        assert_eq!(
            resp.policy
                .as_ref()
                .unwrap()
                .spec
                .routing
                .as_ref()
                .unwrap()
                .route_topk,
            Some(3)
        );

        // tenant-b can read its own policy
        let resp = service
            .get_tenant_policy("tenant-b", None, None, false)
            .expect("get tenant-b policy should succeed");
        assert!(resp.found);
        assert_eq!(
            resp.policy
                .as_ref()
                .unwrap()
                .spec
                .routing
                .as_ref()
                .unwrap()
                .route_topk,
            Some(5)
        );

        // Directly reading from tenant-a's backend should NOT see tenant-b's data
        let backend_a = service.backend_for_tenant(Some("tenant-a")).unwrap();
        let policies_a = backend_a
            .list_tenant_policies(Some("tenant-a"))
            .expect("list from tenant-a backend should succeed");
        assert_eq!(policies_a.len(), 1);
        assert_eq!(policies_a[0].scope.tenant, "tenant-a");

        let backend_b = service.backend_for_tenant(Some("tenant-b")).unwrap();
        let policies_b = backend_b
            .list_tenant_policies(Some("tenant-b"))
            .expect("list from tenant-b backend should succeed");
        assert_eq!(policies_b.len(), 1);
        assert_eq!(policies_b[0].scope.tenant, "tenant-b");
    }

    #[test]
    fn hard_isolated_delete_policy_does_not_affect_other_tenant() {
        let service = hard_isolated_service();
        let patch_a = PolicyPatchInput {
            route_topk: Some(3),
            ..PolicyPatchInput::default()
        };
        let patch_b = PolicyPatchInput {
            route_topk: Some(5),
            ..PolicyPatchInput::default()
        };

        service
            .set_tenant_policy("tenant-a", None, None, patch_a, None, "admin")
            .expect("set tenant-a policy");
        service
            .set_tenant_policy("tenant-b", None, None, patch_b, None, "admin")
            .expect("set tenant-b policy");

        // Delete tenant-a's policy
        let del = service
            .delete_tenant_policy("tenant-a", None, None, None)
            .expect("delete tenant-a policy should succeed");
        assert!(del.removed);

        // tenant-b's policy still exists
        let resp = service
            .get_tenant_policy("tenant-b", None, None, false)
            .expect("get tenant-b policy should still succeed");
        assert!(resp.found);
        assert_eq!(
            resp.policy.unwrap().spec.routing.unwrap().route_topk,
            Some(5)
        );
    }

    #[test]
    fn hard_isolated_quota_state_is_per_tenant() {
        let service = hard_isolated_service();

        // Set up quota policies for both tenants (write directly to each tenant backend)
        let backend_a = service.backend_for_tenant(Some("tenant-a")).unwrap();
        backend_a
            .put_tenant_policy(
                &TenantPolicy {
                    scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                    spec: TenantPolicySpec {
                        quota: Some(TenantQuotaPolicy {
                            max_bytes: Some(1024),
                            max_objects: Some(16),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 1,
                    updated_by: "tester".to_string(),
                },
                None,
            )
            .expect("tenant-a quota policy should store");

        // Reserve quota for tenant-a via its backend
        backend_a
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "res-a".to_string(),
                scope: TenantPolicyScope::new("tenant-a", None::<String>, None::<String>),
                key: ObjectKey::new("tenant-a::object-a"),
                expected_object_version: None,
                delta_bytes: 100,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms: u64::MAX,
                created_at_ms: 5,
                writer_runtime: ClientRuntimeId::new("writer-a", ClientEpoch(1)),
            })
            .expect("reservation for tenant-a should store");

        // Query quota state via service
        let state_a = service
            .get_tenant_quota_state("tenant-a", None, None)
            .expect("tenant-a quota state should read");
        assert!(state_a.state.is_some());
        assert_eq!(state_a.state.unwrap().pending_reserved_bytes, 100);

        // tenant-b has no quota state
        let state_b = service
            .get_tenant_quota_state("tenant-b", None, None)
            .expect("tenant-b quota state should read");
        assert!(state_b.state.is_none());

        // tenant-b reservations list is empty
        let reservations_b = service
            .list_tenant_quota_reservations("tenant-b", None, None, None)
            .expect("tenant-b reservations should read");
        assert_eq!(reservations_b.count, 0);
    }

    #[test]
    fn hard_isolated_backend_for_tenant_none_returns_root() {
        let service = hard_isolated_service();

        // None tenant returns root backend (same arc as service.backend())
        let root = service
            .backend_for_tenant(None)
            .expect("None tenant should return root backend");
        assert_eq!(root.route_namespace(), service.backend().route_namespace());

        // Empty string also returns root backend
        let root_empty = service
            .backend_for_tenant(Some(""))
            .expect("empty tenant should return root backend");
        assert_eq!(
            root_empty.route_namespace(),
            service.backend().route_namespace()
        );

        // Whitespace-only also returns root
        let root_ws = service
            .backend_for_tenant(Some("  "))
            .expect("whitespace tenant should return root backend");
        assert_eq!(
            root_ws.route_namespace(),
            service.backend().route_namespace()
        );
    }
}
