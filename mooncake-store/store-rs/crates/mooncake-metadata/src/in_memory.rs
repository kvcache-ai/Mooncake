use std::collections::BTreeMap;

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy, RoutePolicyDomain, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
    TenantPolicy, TenantPolicyScope,
};
use parking_lot::RwLock;

use crate::segment_state::StoredSegmentState;

#[derive(Default)]
struct InMemoryState {
    clients: BTreeMap<String, ClientLease>,
    handoffs: BTreeMap<String, HandoffPlan>,
    objects: BTreeMap<String, ObjectRoute>,
    route_policies: BTreeMap<RoutePolicyDomain, RoutePolicy>,
    tenant_policies: BTreeMap<TenantPolicyScope, TenantPolicy>,
    segments: BTreeMap<String, StoredSegmentState>,
}

#[derive(Default)]
pub struct InMemoryMetadataBackend {
    state: RwLock<InMemoryState>,
}

impl InMemoryMetadataBackend {
    pub fn new() -> Self {
        Self::default()
    }

    fn segment_key(owner: &ClientRuntimeId, segment: &SegmentName) -> String {
        format!("{}:{}", owner.storage_key(), segment.0)
    }
}

impl MetadataBackend for InMemoryMetadataBackend {
    fn route_namespace(&self) -> String {
        format!("inmemory://{:p}", self)
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        self.state
            .write()
            .clients
            .insert(lease.runtime.storage_key(), lease.clone());
        Ok(())
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        let mut state = self.state.write();
        let Some(lease) = state.clients.get_mut(&runtime.storage_key()) else {
            return Err(StoreError::NotFound(runtime.storage_key()));
        };
        lease.state = next;
        Ok(())
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        Ok(self.state.read().clients.values().cloned().collect())
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        let key = Self::segment_key(&segment.owner, &segment.segment_name);
        let mut state = self.state.write();
        match state.segments.get_mut(&key) {
            Some(current) => current.merge_announcement(segment),
            None => {
                state
                    .segments
                    .insert(key, StoredSegmentState::new(segment.clone()));
            }
        }
        Ok(())
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        self.state
            .write()
            .segments
            .remove(&Self::segment_key(owner, segment));
        Ok(())
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        Ok(self
            .state
            .read()
            .segments
            .values()
            .filter(|segment| owner.is_none_or(|owner| &segment.announcement.owner == owner))
            .map(|segment| segment.announcement.clone())
            .collect())
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        let key = Self::segment_key(owner, segment);
        let mut state = self.state.write();
        let segment_state = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        segment_state.announcement.state = next;
        Ok(())
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let mut state = self.state.write();
        let key = Self::segment_key(owner, segment);
        let segment_state = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        segment_state.reserve(owner, segment, length_bytes)
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let mut state = self.state.write();
        let key = Self::segment_key(owner, segment);
        let segment_state = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        segment_state.release(owner, segment, offset_bytes, length_bytes)
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        Ok(self.state.read().objects.get(&key.0).cloned())
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        Ok(self.state.read().objects.values().cloned().collect())
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let mut state = self.state.write();
        let current = state.objects.get(&key.0).cloned();
        let matches = match (expected, current.as_ref()) {
            (None, None) => true,
            (Some(version), Some(route)) => route.version == version,
            _ => false,
        };

        if !matches {
            return Ok(CasResult {
                applied: false,
                current,
            });
        }

        match next {
            Some(route) => {
                state.objects.insert(key.0.clone(), route.clone());
            }
            None => {
                state.objects.remove(&key.0);
            }
        }

        Ok(CasResult {
            applied: true,
            current: next.cloned(),
        })
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        Ok(self.state.read().route_policies.get(domain).cloned())
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        let mut state = self.state.write();
        if state.route_policies.contains_key(domain) {
            return Ok(false);
        }
        state.route_policies.insert(domain.clone(), policy.clone());
        Ok(true)
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        self.state
            .write()
            .route_policies
            .insert(domain.clone(), policy.clone());
        Ok(())
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        Ok(self.state.write().route_policies.remove(domain).is_some())
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        let mut policies = self
            .state
            .read()
            .route_policies
            .iter()
            .map(|(domain, policy)| (domain.clone(), policy.clone()))
            .collect::<Vec<_>>();
        policies.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(policies)
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        Ok(self.state.read().tenant_policies.get(scope).cloned())
    }

    fn list_tenant_policies(&self) -> Result<Vec<TenantPolicy>> {
        let mut policies = self
            .state
            .read()
            .tenant_policies
            .values()
            .cloned()
            .collect::<Vec<_>>();
        policies.sort_by(|left, right| left.scope.cmp(&right.scope));
        Ok(policies)
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        policy.validate()?;
        let mut state = self.state.write();
        let current = state.tenant_policies.get(&policy.scope).cloned();
        match (expected_version, current.as_ref()) {
            (None, None) => {}
            (Some(expected), Some(current)) if current.version == expected => {}
            (None, Some(_)) => {
                return Err(StoreError::Conflict(format!(
                    "tenant policy already exists for {}",
                    policy.scope.tenant
                )))
            }
            (Some(expected), Some(current)) => {
                return Err(StoreError::Conflict(format!(
                    "tenant policy version mismatch for {}: expected={} actual={}",
                    policy.scope.tenant, expected, current.version
                )))
            }
            (Some(expected), None) => {
                return Err(StoreError::Conflict(format!(
                    "tenant policy missing for {} at expected version {}",
                    policy.scope.tenant, expected
                )))
            }
        }
        state
            .tenant_policies
            .insert(policy.scope.clone(), policy.clone());
        Ok(policy.clone())
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        let mut state = self.state.write();
        let Some(current) = state.tenant_policies.get(scope) else {
            return Ok(false);
        };
        if let Some(expected_version) = expected_version {
            if current.version != expected_version {
                return Err(StoreError::Conflict(format!(
                    "tenant policy version mismatch for {}: expected={} actual={}",
                    scope.tenant, expected_version, current.version
                )));
            }
        }
        state.tenant_policies.remove(scope);
        Ok(true)
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        self.state
            .write()
            .handoffs
            .insert(handoff.stable_id.0.clone(), handoff.clone());
        Ok(())
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        Ok(self.state.read().handoffs.get(&stable_id.0).cloned())
    }
}

#[cfg(test)]
mod tests {
    use mooncake_store_core::{
        ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
        CompatibilityDescriptor, MetadataBackend, RouteControlMode, RoutePolicy, RoutePolicyDomain,
        SegmentAnnouncement, SegmentLifecycleState, SegmentName, StoreError, TenantPolicy,
        TenantPolicyScope, TenantPolicySpec, TenantQuotaPolicy,
    };

    use super::InMemoryMetadataBackend;

    #[test]
    fn segment_reservation_is_aligned_and_monotonic() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node-a", ClientEpoch(1));
        metadata
            .upsert_client_lease(&ClientLease {
                runtime: owner.clone(),
                state: ClientLifecycleState::Active,
                compatibility: CompatibilityDescriptor::default(),
                endpoints: Default::default(),
                expires_at_ms: 10_000,
            })
            .expect("lease should upsert");
        metadata
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                capacity_bytes: 1024,
                used_bytes: 0,
                state: SegmentLifecycleState::Active,
                alignment_bytes: 64,
                tags: vec![],
            })
            .expect("segment should publish");

        let first = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-a"), 17)
            .expect("first reserve should work");
        let second = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-a"), 17)
            .expect("second reserve should work");

        assert_eq!(first.offset_bytes, 0);
        assert_eq!(second.offset_bytes, 64);
    }

    #[test]
    fn segment_reservation_rejects_exhaustion() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId {
            stable_id: ClientStableId::new("node-b"),
            epoch: ClientEpoch(1),
        };
        metadata
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("seg-b"),
                capacity_bytes: 32,
                used_bytes: 0,
                state: SegmentLifecycleState::Active,
                alignment_bytes: 1,
                tags: vec![],
            })
            .expect("segment should publish");

        let error = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-b"), 64)
            .expect_err("reserve should fail");
        assert!(error.to_string().contains("segment capacity exhausted"));
    }

    #[test]
    fn released_segment_space_is_reused() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node-c", ClientEpoch(1));
        metadata
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("seg-c"),
                capacity_bytes: 256,
                used_bytes: 0,
                state: SegmentLifecycleState::Active,
                alignment_bytes: 64,
                tags: vec![],
            })
            .expect("segment should publish");

        let first = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-c"), 33)
            .expect("first reserve should work");
        metadata
            .release_segment(
                &owner,
                &SegmentName::new("seg-c"),
                first.offset_bytes,
                first.length_bytes,
            )
            .expect("release should work");
        let second = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-c"), 17)
            .expect("second reserve should work");

        assert_eq!(second.offset_bytes, first.offset_bytes);
        let segments = metadata
            .list_segments(Some(&owner))
            .expect("list should work");
        assert_eq!(segments[0].used_bytes, 64);
    }

    #[test]
    fn route_policy_put_if_absent_is_domain_scoped() {
        let metadata = InMemoryMetadataBackend::new();
        let creator = ClientRuntimeId::new("route-owner", ClientEpoch(7));
        let default_policy = RoutePolicy {
            route_topk: 2,
            route_control: RouteControlMode::EmbeddedWrh,
            created_by: creator.clone(),
            created_at_ms: 11,
        };
        let tenant_policy = RoutePolicy {
            route_topk: 4,
            route_control: RouteControlMode::MetadataOnly,
            created_by: creator,
            created_at_ms: 22,
        };

        assert!(metadata
            .put_route_policy_if_absent(&RoutePolicyDomain::Default, &default_policy)
            .expect("default route policy bootstrap should succeed"));
        assert!(!metadata
            .put_route_policy_if_absent(&RoutePolicyDomain::Default, &tenant_policy)
            .expect("second default route policy bootstrap should be rejected"));
        assert!(metadata
            .put_route_policy_if_absent(
                &RoutePolicyDomain::Tenant("tenant-a".to_string()),
                &tenant_policy,
            )
            .expect("tenant-scoped route policy bootstrap should succeed"));

        assert_eq!(
            metadata
                .get_route_policy(&RoutePolicyDomain::Default)
                .expect("default route policy read should succeed"),
            Some(default_policy),
        );
        assert_eq!(
            metadata
                .get_route_policy(&RoutePolicyDomain::Tenant("tenant-a".to_string()))
                .expect("tenant route policy read should succeed"),
            Some(tenant_policy.clone()),
        );

        let replacement = RoutePolicy {
            route_topk: 6,
            route_control: RouteControlMode::EmbeddedWrh,
            created_by: ClientRuntimeId::new("admin", ClientEpoch(0)),
            created_at_ms: 33,
        };
        metadata
            .put_route_policy(
                &RoutePolicyDomain::Tenant("tenant-a".to_string()),
                &replacement,
            )
            .expect("tenant route policy overwrite should succeed");
        assert_eq!(
            metadata
                .get_route_policy(&RoutePolicyDomain::Tenant("tenant-a".to_string()))
                .expect("tenant route policy read should succeed"),
            Some(replacement.clone()),
        );
        assert_eq!(
            metadata
                .list_route_policies()
                .expect("route policy listing should succeed")
                .len(),
            2
        );
        assert!(metadata
            .delete_route_policy(&RoutePolicyDomain::Tenant("tenant-a".to_string()))
            .expect("tenant route policy delete should succeed"));
        assert_eq!(
            metadata
                .get_route_policy(&RoutePolicyDomain::Tenant("tenant-a".to_string()))
                .expect("tenant route policy read should succeed"),
            None,
        );
    }

    #[test]
    fn tenant_policy_put_requires_matching_version() {
        let metadata = InMemoryMetadataBackend::new();
        let scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let policy = TenantPolicy {
            scope: scope.clone(),
            spec: TenantPolicySpec {
                quota: Some(TenantQuotaPolicy {
                    max_bytes: Some(128),
                    max_objects: Some(8),
                }),
                ..TenantPolicySpec::default()
            },
            version: 1,
            updated_at_ms: 10,
            updated_by: "admin".to_string(),
        };
        metadata
            .put_tenant_policy(&policy, None)
            .expect("first tenant policy insert should succeed");
        assert_eq!(
            metadata
                .get_tenant_policy(&scope)
                .expect("tenant policy read should succeed"),
            Some(policy.clone())
        );

        let mut updated = policy.clone();
        updated.version = 2;
        updated.updated_at_ms = 20;
        updated.updated_by = "admin-2".to_string();
        updated.spec.quota.as_mut().unwrap().max_objects = Some(16);
        let error = metadata
            .put_tenant_policy(&updated, Some(3))
            .expect_err("mismatched version should fail");
        assert!(matches!(error, StoreError::Conflict(_)));

        metadata
            .put_tenant_policy(&updated, Some(1))
            .expect("matching version should succeed");
        assert_eq!(
            metadata
                .list_tenant_policies()
                .expect("tenant policy listing should succeed"),
            vec![updated.clone()]
        );
        assert!(metadata
            .delete_tenant_policy(&scope, Some(2))
            .expect("delete should succeed"));
        assert_eq!(
            metadata
                .get_tenant_policy(&scope)
                .expect("tenant policy read after delete should succeed"),
            None
        );
    }
}
