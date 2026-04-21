use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_metadata::{
    ClientLeaseLiveness, EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace,
    RedisMetadataBackend, RedisMetadataCleanupReport, RedisMetadataConfig,
};
use mooncake_store_client::record_tenant_quota_reconcile;
use mooncake_store_core::{
    route_logical_object_id, ClientEpoch, ClientRuntimeId, LogicalObjectId, MetadataBackend,
    NamespaceScope, ObjectKey, RoutePolicy, RoutePolicyDomain, StoreError,
    TenantBandwidthShapingPolicy, TenantExecutionFairnessPolicy, TenantObjectAccountingState,
    TenantPlacementPolicy, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
    TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservationState, TenantRoutePolicy,
    DEFAULT_DOMAIN, DEFAULT_OBJECT_SET,
};
use url::Url;

use crate::config::build_metadata_backend;

use super::models::{
    AdminCleanupReport, AdminMaintenanceReport, AdminOwnerCleanupReport, AdminOwnerCleanupState,
    DeleteTenantPolicyResponse, GetTenantObjectAccountingResponse, GetTenantPolicyResponse,
    GetTenantQuotaStateResponse, ListTenantQuotaReservationsResponse, PolicyPatchInput,
    RoutePolicyResponse, TenantQuotaAbortResponse, TenantQuotaReconcileAction,
    TenantQuotaReconcileReport,
};

pub type AdminResult<T> = mooncake_store_core::Result<T>;

#[derive(Clone)]
pub struct AdminService {
    backend: Arc<dyn MetadataBackend>,
    metadata_url: String,
    keyspace: MetadataKeyspace,
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
        let (backend, _) = build_metadata_backend(metadata_url, None, keyspace.clone())?;
        Ok(Self {
            backend,
            metadata_url: metadata_url.to_string(),
            keyspace,
        })
    }

    pub fn new(
        backend: Arc<dyn MetadataBackend>,
        metadata_url: impl Into<String>,
        keyspace: MetadataKeyspace,
    ) -> Self {
        Self {
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

    pub fn get_route_policy(&self, tenant: Option<&str>) -> AdminResult<RoutePolicyResponse> {
        let domain = route_policy_domain(tenant);
        let policy = self.backend.get_route_policy(&domain)?;
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
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        if effective {
            let namespace = NamespaceScope::with_defaults(Some(tenant), domain, object_set);
            let effective_spec =
                resolve_effective_tenant_policy(self.backend.as_ref(), &namespace)?;
            return Ok(GetTenantPolicyResponse {
                scope,
                effective: true,
                found: effective_spec.is_some(),
                policy: None,
                effective_spec,
            });
        }

        let policy = self.backend.get_tenant_policy(&scope)?;
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
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let patch = tenant_policy_patch(&patch)?;
        if policy_patch_is_empty(&patch) {
            return Err(StoreError::InvalidState(
                "at least one policy flag must be provided".to_string(),
            ));
        }
        let current = self.backend.get_tenant_policy(&scope)?;
        let policy = merge_tenant_policy(current.as_ref(), scope.clone(), patch, updated_by);
        let expected = expected_version.or_else(|| current.as_ref().map(|policy| policy.version));
        let stored = self.backend.put_tenant_policy(&policy, expected)?;
        sync_legacy_route_policy(self.backend.as_ref(), &stored)?;
        Ok(stored)
    }

    pub fn delete_tenant_policy(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
        expected_version: Option<u64>,
    ) -> AdminResult<DeleteTenantPolicyResponse> {
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let removed = self
            .backend
            .delete_tenant_policy(&scope, expected_version)?;
        if removed && is_root_tenant_scope(&scope) {
            self.backend
                .delete_route_policy(&RoutePolicyDomain::Tenant(scope.tenant.clone()))?;
        }
        Ok(DeleteTenantPolicyResponse { scope, removed })
    }

    pub fn list_tenant_policies(&self, tenant: Option<&str>) -> AdminResult<Vec<TenantPolicy>> {
        let mut policies = self.backend.list_tenant_policies()?;
        if let Some(tenant) = tenant {
            policies.retain(|policy| policy.scope.tenant == tenant);
        }
        policies.sort_by(|left, right| left.scope.cmp(&right.scope));
        Ok(policies)
    }

    pub fn get_tenant_quota_state(
        &self,
        tenant: &str,
        domain: Option<&str>,
        object_set: Option<&str>,
    ) -> AdminResult<GetTenantQuotaStateResponse> {
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let state = self.backend.get_tenant_quota_state(&root_scope)?;
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
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let object_id = LogicalObjectId::new(
            NamespaceScope::with_defaults(Some(tenant), domain, object_set),
            key.to_string(),
        );
        let scoped_key = ObjectKey::from_logical_id(&object_id);
        let accounting = self.backend.get_tenant_object_accounting(&scoped_key)?;
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
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let mut reservations = self.backend.list_tenant_quota_reservations(&root_scope)?;
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
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let reservations = self.backend.list_tenant_quota_reservations(&root_scope)?;
        let reservation = reservations
            .into_iter()
            .find(|entry| entry.reservation_id == reservation_id)
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
            let abort_result = self.backend.abort_tenant_quota(reservation_id);
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
        let scope = tenant_policy_scope(tenant, domain, object_set)?;
        let root_scope = root_tenant_scope(&scope)?;
        let now = now_ms();
        let mut reservations = self.backend.list_tenant_quota_reservations(&root_scope)?;
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
                    let abort_result = self.backend.abort_tenant_quota(&reservation.reservation_id);
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
            let route = self.backend.get_object_route(&reservation.key)?;
            let accounting = self
                .backend
                .get_tenant_object_accounting(&reservation.key)?;
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
                        self.backend
                            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
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
            route_control: values.route_control,
        })
        .filter(|policy| policy.route_topk.is_some() || policy.route_control.is_some()),
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

pub fn sync_legacy_route_policy(
    backend: &dyn MetadataBackend,
    policy: &TenantPolicy,
) -> AdminResult<()> {
    if !is_root_tenant_scope(&policy.scope) {
        return Ok(());
    }
    let domain = RoutePolicyDomain::Tenant(policy.scope.tenant.clone());
    if let Some(routing) = policy.spec.routing.as_ref() {
        if let Some(route_policy) = route_policy_from_tenant_policy(policy, routing) {
            backend.put_route_policy(&domain, &route_policy)?;
            return Ok(());
        }
    }
    backend.delete_route_policy(&domain)?;
    Ok(())
}

pub fn route_policy_from_tenant_policy(
    policy: &TenantPolicy,
    routing: &TenantRoutePolicy,
) -> Option<RoutePolicy> {
    Some(RoutePolicy {
        route_topk: routing.route_topk?,
        route_control: routing.route_control?,
        created_by: ClientRuntimeId::new(policy.updated_by.clone(), ClientEpoch(0)),
        created_at_ms: policy.updated_at_ms,
    })
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
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use mooncake_metadata::{
        EtcdMetadataBackend, EtcdMetadataConfig, InMemoryMetadataBackend, RedisMetadataBackend,
        RedisMetadataConfig,
    };
    use mooncake_store_core::{
        ClientEpoch, ClientRuntimeId, CompatibilityDescriptor, MetadataBackend, ObjectRoute,
        ReplicaRoute, ReplicaTier, RoutePolicy, RoutePolicyDomain, RouteState, RouteVersion,
        SegmentAnnouncement, SegmentLifecycleState, SegmentName, TenantObjectAccounting,
        TenantObjectAccountingState, TenantPolicy, TenantQuotaAbortOutcome,
        TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest, TenantQuotaPolicy,
        TenantQuotaReservation, TenantQuotaReservationOutcome, TenantQuotaReservationRequest,
        TenantQuotaReservationState, TenantQuotaState,
    };

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

        fn upsert_client_lease(
            &self,
            lease: &mooncake_store_core::ClientLease,
        ) -> mooncake_store_core::Result<()> {
            self.inner.upsert_client_lease(lease)
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

        fn get_segment_owner(
            &self,
            segment: &mooncake_store_core::SegmentName,
        ) -> mooncake_store_core::Result<Option<ClientRuntimeId>> {
            self.inner.get_segment_owner(segment)
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

        fn list_tenant_policies(&self) -> mooncake_store_core::Result<Vec<TenantPolicy>> {
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

    impl RedisTestServer {
        fn start() -> Option<Self> {
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

    #[test]
    fn root_scope_helpers_collapse_nested_scope() {
        let nested = TenantPolicyScope::new("tenant-a", Some("domain-a"), Some("set-a"));
        let root = root_tenant_scope(&nested).expect("root scope should build");
        assert_eq!(root.tenant, "tenant-a");
        assert!(root.domain.is_none());
        assert!(root.object_set.is_none());
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
                            route_topk: None,
                            route_control: Some(
                                mooncake_store_client::RouteControlMode::MetadataOnly,
                            ),
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
                            route_control: None,
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
                route_control: Some(mooncake_store_client::RouteControlMode::MetadataOnly),
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
        reserve_quota(&service, "res-a", "tenant-a::object-a", 10);
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
        assert_eq!(accounting.key, "tenant-a::object-a");
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
                        offset: 0,
                        segment_offset: 0,
                        length: 12,
                        checksum: None,
                        tier: ReplicaTier::Nvme,
                        priority: 0,
                    }],
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
        let service = AdminService::new(backend.clone(), server.url.clone(), keyspace);

        let dead_runtime = ClientRuntimeId::new("dead-owner", ClientEpoch(9));
        backend
            .publish_segment(&SegmentAnnouncement {
                owner: dead_runtime.clone(),
                segment_name: SegmentName::new("dead-segment"),
                capacity_bytes: 256,
                used_bytes: 64,
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
            capacity_bytes: 128,
            used_bytes: 0,
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
                capacity_bytes: 256,
                used_bytes: 64,
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
            capacity_bytes: 128,
            used_bytes: 0,
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
}
