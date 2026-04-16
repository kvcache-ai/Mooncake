use serde::{Deserialize, Serialize};

use crate::compat::CompatibilityDescriptor;
use crate::identity::{ClientEndpointSet, ClientRuntimeId, LogicalObjectId, NamespaceScope};
use crate::lifecycle::ClientLifecycleState;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ObjectKey(pub String);

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn from_scope(scope: &NamespaceScope, logical_key: &str) -> Self {
        Self::new(format!("{}::{}", scope.tenant, logical_key))
    }

    pub fn from_logical_id(id: &LogicalObjectId) -> Self {
        Self::from_scope(&id.scope, &id.logical_key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SegmentName(pub String);

impl SegmentName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct RouteVersion(pub u64);

impl RouteVersion {
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RouteState {
    Active,
    Deleting,
    Tombstone,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicaTier {
    Dram,
    Nvme,
    File,
    Unknown,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum SegmentLifecycleState {
    #[default]
    Active,
    Draining,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaRoute {
    pub owner: ClientRuntimeId,
    pub segment_name: SegmentName,
    pub offset: u64,
    #[serde(default)]
    pub segment_offset: u64,
    pub length: u64,
    pub checksum: Option<u64>,
    pub tier: ReplicaTier,
    pub priority: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRoute {
    pub key: ObjectKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<NamespaceScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sharing_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos_tier: Option<String>,
    pub version: RouteVersion,
    pub state: RouteState,
    pub compatibility: CompatibilityDescriptor,
    pub replicas: Vec<ReplicaRoute>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientLease {
    pub runtime: ClientRuntimeId,
    pub state: ClientLifecycleState,
    pub compatibility: CompatibilityDescriptor,
    pub endpoints: ClientEndpointSet,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentAnnouncement {
    pub owner: ClientRuntimeId,
    pub segment_name: SegmentName,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    #[serde(default)]
    pub state: SegmentLifecycleState,
    #[serde(default = "default_segment_alignment_bytes")]
    pub alignment_bytes: u64,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentReservation {
    pub owner: ClientRuntimeId,
    pub segment_name: SegmentName,
    pub offset_bytes: u64,
    pub length_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CasResult {
    pub applied: bool,
    pub current: Option<ObjectRoute>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteCasRequest {
    pub key: ObjectKey,
    pub expected: Option<RouteVersion>,
    pub next: Option<ObjectRoute>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum RouteControlMode {
    MetadataOnly,
    #[default]
    EmbeddedWrh,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum RoutePolicyDomain {
    Default,
    Tenant(String),
}

impl Default for RoutePolicyDomain {
    fn default() -> Self {
        Self::Default
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RoutePolicy {
    pub route_topk: u32,
    pub route_control: RouteControlMode,
    pub created_by: ClientRuntimeId,
    pub created_at_ms: u64,
}

impl RoutePolicy {
    pub fn semantically_matches(&self, other: &Self) -> bool {
        self.route_topk == other.route_topk && self.route_control == other.route_control
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct TenantPolicyScope {
    pub tenant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_set: Option<String>,
}

impl TenantPolicyScope {
    pub fn new(
        tenant: impl Into<String>,
        domain: Option<impl Into<String>>,
        object_set: Option<impl Into<String>>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            domain: domain.map(Into::into),
            object_set: object_set.map(Into::into),
        }
    }

    pub fn validate(&self) -> crate::Result<()> {
        if self.tenant.is_empty() {
            return Err(crate::StoreError::InvalidState(
                "tenant policy scope tenant must not be empty".to_string(),
            ));
        }
        if self.object_set.is_some() && self.domain.is_none() {
            return Err(crate::StoreError::InvalidState(
                "tenant policy scope object_set requires domain".to_string(),
            ));
        }
        Ok(())
    }

    pub fn validate_root_only(&self, context: &str) -> crate::Result<()> {
        self.validate()?;
        if self.domain.is_some() || self.object_set.is_some() {
            return Err(crate::StoreError::InvalidState(format!(
                "{context} currently supports tenant root scope only"
            )));
        }
        Ok(())
    }

    pub fn matches_namespace(&self, scope: &crate::NamespaceScope) -> bool {
        if self.tenant != scope.tenant {
            return false;
        }
        if self
            .domain
            .as_deref()
            .is_some_and(|domain| domain != scope.domain)
        {
            return false;
        }
        if self
            .object_set
            .as_deref()
            .is_some_and(|object_set| object_set != scope.object_set)
        {
            return false;
        }
        true
    }

    pub fn specificity(&self) -> usize {
        1 + usize::from(self.domain.is_some()) + usize::from(self.object_set.is_some())
    }

    pub fn ancestors(scope: &crate::NamespaceScope) -> [Self; 3] {
        [
            Self::new(scope.tenant.clone(), None::<String>, None::<String>),
            Self::new(
                scope.tenant.clone(),
                Some(scope.domain.clone()),
                None::<String>,
            ),
            Self::new(
                scope.tenant.clone(),
                Some(scope.domain.clone()),
                Some(scope.object_set.clone()),
            ),
        ]
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantRoutePolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_topk: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_control: Option<RouteControlMode>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_objects: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TenantObjectAccountingState {
    #[default]
    Active,
    Deleted,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum TenantQuotaReservationState {
    #[default]
    Pending,
    Finalized,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaState {
    pub scope: TenantPolicyScope,
    pub version: u64,
    pub used_bytes: u64,
    pub used_objects: u64,
    pub pending_reserved_bytes: u64,
    pub pending_reserved_objects: u64,
    pub updated_at_ms: u64,
    pub updated_by: String,
}

impl TenantQuotaState {
    pub fn validate(&self) -> crate::Result<()> {
        self.scope.validate_root_only("tenant quota state")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantObjectAccounting {
    pub key: ObjectKey,
    pub scope: TenantPolicyScope,
    pub version: u64,
    pub committed_length: u64,
    pub route_version: Option<RouteVersion>,
    pub state: TenantObjectAccountingState,
    pub last_writer: String,
    pub updated_at_ms: u64,
}

impl TenantObjectAccounting {
    pub fn validate(&self) -> crate::Result<()> {
        self.scope.validate_root_only("tenant object accounting")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaReservation {
    pub reservation_id: String,
    pub scope: TenantPolicyScope,
    pub key: ObjectKey,
    pub version: u64,
    pub expected_object_version: Option<u64>,
    pub delta_bytes: i64,
    pub delta_objects: i64,
    pub state: TenantQuotaReservationState,
    pub expires_at_ms: u64,
    pub created_at_ms: u64,
    pub writer_runtime: ClientRuntimeId,
}

impl TenantQuotaReservation {
    pub fn validate(&self) -> crate::Result<()> {
        if self.reservation_id.is_empty() {
            return Err(crate::StoreError::InvalidState(
                "tenant quota reservation_id must not be empty".to_string(),
            ));
        }
        self.scope.validate_root_only("tenant quota reservation")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaReservationRequest {
    pub reservation_id: String,
    pub scope: TenantPolicyScope,
    pub key: ObjectKey,
    pub expected_object_version: Option<u64>,
    pub delta_bytes: i64,
    pub delta_objects: i64,
    pub limit: TenantQuotaPolicy,
    pub expires_at_ms: u64,
    pub created_at_ms: u64,
    pub writer_runtime: ClientRuntimeId,
}

impl TenantQuotaReservationRequest {
    pub fn validate(&self) -> crate::Result<()> {
        if self.reservation_id.is_empty() {
            return Err(crate::StoreError::InvalidState(
                "tenant quota reservation request_id must not be empty".to_string(),
            ));
        }
        self.scope
            .validate_root_only("tenant quota reservation request")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaReservationOutcome {
    pub quota: TenantQuotaState,
    pub object: Option<TenantObjectAccounting>,
    pub reservation: TenantQuotaReservation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaFinalizeRequest {
    pub reservation_id: String,
    pub expected_object_version: Option<u64>,
    pub committed_length: Option<u64>,
    pub route_version: Option<RouteVersion>,
    pub state: TenantObjectAccountingState,
    pub updated_at_ms: u64,
    pub updated_by: String,
}

impl TenantQuotaFinalizeRequest {
    pub fn validate(&self) -> crate::Result<()> {
        if self.reservation_id.is_empty() {
            return Err(crate::StoreError::InvalidState(
                "tenant quota finalize reservation_id must not be empty".to_string(),
            ));
        }
        match (self.state, self.committed_length) {
            (TenantObjectAccountingState::Active, Some(_)) => Ok(()),
            (TenantObjectAccountingState::Deleted, None) => Ok(()),
            (TenantObjectAccountingState::Active, None) => Err(crate::StoreError::InvalidState(
                "tenant quota finalize active object requires committed_length".to_string(),
            )),
            (TenantObjectAccountingState::Deleted, Some(_)) => {
                Err(crate::StoreError::InvalidState(
                    "tenant quota finalize deleted object must not carry committed_length"
                        .to_string(),
                ))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaFinalizeOutcome {
    pub quota: TenantQuotaState,
    pub object: Option<TenantObjectAccounting>,
    pub reservation: TenantQuotaReservation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantQuotaAbortOutcome {
    pub quota: TenantQuotaState,
    pub reservation: TenantQuotaReservation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantExecutionFairnessPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_remote_batch_items_per_tenant: Option<usize>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantBandwidthShapingPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_remote_batch_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_remote_batch_burst_items: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_inflight_bytes_per_batch: Option<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantPlacementPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_replica_count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefer_local: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefer_alloc_in_same_node: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_storage_owners: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_segments: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantPolicySpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<TenantRoutePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota: Option<TenantQuotaPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fairness: Option<TenantExecutionFairnessPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shaping: Option<TenantBandwidthShapingPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<TenantPlacementPolicy>,
}

impl TenantPolicySpec {
    pub fn merged_with(&self, overlay: &Self) -> Self {
        Self {
            routing: Some(merge_routing_policy(
                self.routing.as_ref(),
                overlay.routing.as_ref(),
            ))
            .filter(|policy| policy.route_topk.is_some() || policy.route_control.is_some()),
            quota: Some(merge_quota_policy(
                self.quota.as_ref(),
                overlay.quota.as_ref(),
            ))
            .filter(|policy| policy.max_bytes.is_some() || policy.max_objects.is_some()),
            fairness: Some(merge_fairness_policy(
                self.fairness.as_ref(),
                overlay.fairness.as_ref(),
            ))
            .filter(|policy| policy.max_remote_batch_items_per_tenant.is_some()),
            shaping: Some(merge_shaping_policy(
                self.shaping.as_ref(),
                overlay.shaping.as_ref(),
            ))
            .filter(|policy| {
                policy.max_remote_batch_bytes.is_some()
                    || policy.max_remote_batch_burst_items.is_some()
                    || policy.max_inflight_bytes_per_batch.is_some()
            }),
            placement: Some(merge_placement_policy(
                self.placement.as_ref(),
                overlay.placement.as_ref(),
            ))
            .filter(|policy| {
                policy.default_replica_count.is_some()
                    || policy.prefer_local.is_some()
                    || policy.prefer_alloc_in_same_node.is_some()
                    || policy.preferred_storage_owners.is_some()
                    || policy.preferred_segments.is_some()
            }),
        }
    }

    pub fn resolve_for_scope<'a>(
        policies: impl IntoIterator<Item = &'a TenantPolicy>,
        scope: &crate::NamespaceScope,
    ) -> Self {
        let mut resolved = Self::default();
        let mut matching = policies
            .into_iter()
            .filter(|policy| policy.scope.matches_namespace(scope))
            .collect::<Vec<_>>();
        matching.sort_by(|left, right| left.scope.specificity().cmp(&right.scope.specificity()));
        for policy in matching {
            resolved = resolved.merged_with(&policy.spec);
        }
        resolved
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TenantPolicy {
    pub scope: TenantPolicyScope,
    pub spec: TenantPolicySpec,
    pub version: u64,
    pub updated_at_ms: u64,
    pub updated_by: String,
}

impl TenantPolicy {
    pub fn validate(&self) -> crate::Result<()> {
        self.scope.validate()
    }
}

fn merge_routing_policy(
    base: Option<&TenantRoutePolicy>,
    overlay: Option<&TenantRoutePolicy>,
) -> TenantRoutePolicy {
    TenantRoutePolicy {
        route_topk: overlay
            .and_then(|policy| policy.route_topk)
            .or(base.and_then(|policy| policy.route_topk)),
        route_control: overlay
            .and_then(|policy| policy.route_control)
            .or(base.and_then(|policy| policy.route_control)),
    }
}

fn merge_quota_policy(
    base: Option<&TenantQuotaPolicy>,
    overlay: Option<&TenantQuotaPolicy>,
) -> TenantQuotaPolicy {
    TenantQuotaPolicy {
        max_bytes: overlay
            .and_then(|policy| policy.max_bytes)
            .or(base.and_then(|policy| policy.max_bytes)),
        max_objects: overlay
            .and_then(|policy| policy.max_objects)
            .or(base.and_then(|policy| policy.max_objects)),
    }
}

fn merge_fairness_policy(
    base: Option<&TenantExecutionFairnessPolicy>,
    overlay: Option<&TenantExecutionFairnessPolicy>,
) -> TenantExecutionFairnessPolicy {
    TenantExecutionFairnessPolicy {
        max_remote_batch_items_per_tenant: overlay
            .and_then(|policy| policy.max_remote_batch_items_per_tenant)
            .or(base.and_then(|policy| policy.max_remote_batch_items_per_tenant)),
    }
}

fn merge_shaping_policy(
    base: Option<&TenantBandwidthShapingPolicy>,
    overlay: Option<&TenantBandwidthShapingPolicy>,
) -> TenantBandwidthShapingPolicy {
    TenantBandwidthShapingPolicy {
        max_remote_batch_bytes: overlay
            .and_then(|policy| policy.max_remote_batch_bytes)
            .or(base.and_then(|policy| policy.max_remote_batch_bytes)),
        max_remote_batch_burst_items: overlay
            .and_then(|policy| policy.max_remote_batch_burst_items)
            .or(base.and_then(|policy| policy.max_remote_batch_burst_items)),
        max_inflight_bytes_per_batch: overlay
            .and_then(|policy| policy.max_inflight_bytes_per_batch)
            .or(base.and_then(|policy| policy.max_inflight_bytes_per_batch)),
    }
}

fn merge_placement_policy(
    base: Option<&TenantPlacementPolicy>,
    overlay: Option<&TenantPlacementPolicy>,
) -> TenantPlacementPolicy {
    TenantPlacementPolicy {
        default_replica_count: overlay
            .and_then(|policy| policy.default_replica_count)
            .or(base.and_then(|policy| policy.default_replica_count)),
        prefer_local: overlay
            .and_then(|policy| policy.prefer_local)
            .or(base.and_then(|policy| policy.prefer_local)),
        prefer_alloc_in_same_node: overlay
            .and_then(|policy| policy.prefer_alloc_in_same_node)
            .or(base.and_then(|policy| policy.prefer_alloc_in_same_node)),
        preferred_storage_owners: overlay
            .and_then(|policy| policy.preferred_storage_owners.clone())
            .or(base.and_then(|policy| policy.preferred_storage_owners.clone())),
        preferred_segments: overlay
            .and_then(|policy| policy.preferred_segments.clone())
            .or(base.and_then(|policy| policy.preferred_segments.clone())),
    }
}

fn default_segment_alignment_bytes() -> u64 {
    1
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        default_segment_alignment_bytes, ObjectKey, RouteVersion, SegmentAnnouncement,
        SegmentLifecycleState, SegmentName,
    };
    use crate::NamespaceScope;
    use crate::{
        scoped_logical_object_id, scoped_object_key, ClientEndpointSet, ClientEpoch, ClientLease,
        ClientLifecycleState, ClientRuntimeId, CompatibilityDescriptor,
    };

    #[test]
    fn object_and_segment_name_helpers_wrap_strings_directly() {
        assert_eq!(ObjectKey::new("alpha").0, "alpha");
        assert_eq!(SegmentName::new("segment-a").0, "segment-a");
    }

    #[test]
    fn object_key_can_be_derived_from_namespace_scope() {
        let scope = NamespaceScope::new("tenant-a", "domain-a", "set-a");
        assert_eq!(
            ObjectKey::from_scope(&scope, "logical-a").0,
            "tenant-a::logical-a"
        );
        assert_eq!(
            ObjectKey::from_logical_id(&scoped_logical_object_id("tenant-a", "logical-a")),
            scoped_object_key("tenant-a", "logical-a")
        );
    }

    #[test]
    fn route_version_next_saturates_at_max_value() {
        assert_eq!(RouteVersion(9).next(), RouteVersion(10));
        assert_eq!(RouteVersion(u64::MAX).next(), RouteVersion(u64::MAX));
    }

    #[test]
    fn segment_announcement_defaults_state_and_alignment_for_serde() {
        let announcement: SegmentAnnouncement = serde_json::from_value(json!({
            "owner": {
                "stable_id": "runtime-a",
                "epoch": 1
            },
            "segment_name": "segment-a",
            "capacity_bytes": 4096,
            "used_bytes": 1024,
            "tags": ["storage"]
        }))
        .expect("announcement should deserialize");
        assert_eq!(announcement.state, SegmentLifecycleState::Active);
        assert_eq!(
            announcement.alignment_bytes,
            default_segment_alignment_bytes()
        );
    }

    #[test]
    fn client_lease_round_trip_keeps_runtime_contract() {
        let lease = ClientLease {
            runtime: ClientRuntimeId::new("runtime-a", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: 42,
        };
        let encoded = serde_json::to_value(&lease).expect("lease should serialize");
        let decoded: ClientLease =
            serde_json::from_value(encoded).expect("lease should deserialize");
        assert_eq!(
            decoded.runtime,
            ClientRuntimeId::new("runtime-a", ClientEpoch(1))
        );
        assert_eq!(decoded.state, ClientLifecycleState::Active);
        assert_eq!(decoded.expires_at_ms, 42);
    }
}
