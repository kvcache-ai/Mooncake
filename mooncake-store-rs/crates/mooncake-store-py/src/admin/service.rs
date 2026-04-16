use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_core::{
    ClientEpoch, ClientRuntimeId, MetadataBackend, NamespaceScope, RoutePolicy, RoutePolicyDomain,
    StoreError, TenantBandwidthShapingPolicy, TenantExecutionFairnessPolicy, TenantPlacementPolicy,
    TenantPolicy, TenantPolicyScope, TenantPolicySpec, TenantQuotaPolicy, TenantRoutePolicy,
    DEFAULT_DOMAIN, DEFAULT_OBJECT_SET,
};
use url::Url;

use crate::config::build_metadata_backend;

use super::models::{
    AdminCleanupReport, DeleteTenantPolicyResponse, GetTenantPolicyResponse, PolicyPatchInput,
    RoutePolicyResponse,
};

pub type AdminResult<T> = mooncake_store_core::Result<T>;

#[derive(Clone)]
pub struct AdminService {
    backend: Arc<dyn MetadataBackend>,
    metadata_url: String,
    keyspace: MetadataKeyspace,
}

impl AdminService {
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
            let policies = self.backend.list_tenant_policies()?;
            let namespace = NamespaceScope::with_defaults(Some(tenant), domain, object_set);
            let matching = policies
                .iter()
                .filter(|policy| policy.scope.matches_namespace(&namespace))
                .collect::<Vec<_>>();
            let effective_spec = (!matching.is_empty())
                .then(|| TenantPolicySpec::resolve_for_scope(matching, &namespace));
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

    pub fn cleanup_stale_segments(&self) -> AdminResult<AdminCleanupReport> {
        if !self.metadata_url.starts_with("redis://") && !self.metadata_url.starts_with("rediss://")
        {
            return Err(StoreError::Unsupported(
                "cleanup-stale-segments currently supports redis:// and rediss:// metadata only"
                    .to_string(),
            ));
        }
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(self.metadata_url.clone()).keyspace(self.keyspace.clone()),
        )?;
        let report = backend.cleanup_stale_segments()?;
        Ok(AdminCleanupReport {
            live_clients: report.live_clients,
            inspected_segment_keys: report.inspected_segment_keys,
            removed_segment_keys: report.removed_segment_keys,
            removed_segment_index_entries: report.removed_segment_index_entries,
            removed_owner_segment_index_entries: report.removed_owner_segment_index_entries,
            stale_missing_segment_index_entries: report.stale_missing_segment_index_entries,
        })
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
