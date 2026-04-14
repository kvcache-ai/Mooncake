use crate::error::Result;
use crate::identity::{
    ClientRuntimeId, ClientStableId, LogicalObjectId, NamespaceScope, ReuseIdentity,
};
use crate::lifecycle::{ClientLifecycleState, HandoffPlan};
use crate::route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, RouteCasRequest, RoutePolicy,
    RoutePolicyDomain, RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
    SegmentReservation, TenantPolicy, TenantPolicyScope,
};

pub trait MetadataBackend: Send + Sync {
    fn route_namespace(&self) -> String;

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()>;

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()>;

    fn list_live_clients(&self) -> Result<Vec<ClientLease>>;

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()>;

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()>;

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>>;

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()>;

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation>;

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()>;

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>>;

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>>;

    fn list_object_routes_in_scope(&self, scope: &NamespaceScope) -> Result<Vec<ObjectRoute>> {
        Ok(self
            .list_object_routes()?
            .into_iter()
            .filter(|route| {
                crate::route_logical_object_id(route)
                    .map(|object_id| object_id.scope == *scope)
                    .unwrap_or(false)
            })
            .collect())
    }

    fn get_object_route_by_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>> {
        self.get_object_route(&ObjectKey::from_logical_id(object_id))
    }

    fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Result<Vec<ObjectRoute>> {
        Ok(self
            .list_object_routes()?
            .into_iter()
            .filter_map(|route| {
                let candidate = crate::route_reuse_identity(&route).ok()?;
                (candidate == *reuse).then_some(route)
            })
            .collect())
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>>;

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool>;

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()>;

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool>;

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>>;

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>>;

    fn list_tenant_policies(&self) -> Result<Vec<TenantPolicy>>;

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy>;

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool>;

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()>;

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>>;
}

pub trait RouteDirectory: Send + Sync {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>>;

    fn get_object_routes(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        keys.iter()
            .map(|key| self.get_object_route(observer, key))
            .collect()
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>> {
        Ok(requests
            .iter()
            .map(|request| {
                self.compare_and_swap_object_route(
                    observer,
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                )
            })
            .collect())
    }

    fn list_routes_by_replica_owner(
        &self,
        _observer: &ClientLease,
        _owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        Err(crate::error::StoreError::Unsupported(
            "route directory does not support list_routes_by_replica_owner".to_string(),
        ))
    }
}

pub trait PlacementStrategy: Send + Sync {
    fn select_write_targets(
        &self,
        local: &ClientRuntimeId,
        live_clients: &[ClientLease],
        replica_count: usize,
    ) -> Result<Vec<ClientRuntimeId>>;
}

#[cfg(test)]
mod tests {
    use super::{RouteDirectory, *};
    use crate::compat::CompatibilityDescriptor;
    use crate::identity::{ClientEndpointSet, ClientEpoch};
    use crate::route::{ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteState};

    struct TestRouteDirectory;

    impl RouteDirectory for TestRouteDirectory {
        fn get_object_route(
            &self,
            _observer: &ClientLease,
            key: &ObjectKey,
        ) -> Result<Option<ObjectRoute>> {
            Ok((key.0 != "missing").then(|| sample_route(key.0.as_str())))
        }

        fn compare_and_swap_object_route(
            &self,
            _observer: &ClientLease,
            key: &ObjectKey,
            expected: Option<RouteVersion>,
            next: Option<&ObjectRoute>,
        ) -> Result<CasResult> {
            Ok(CasResult {
                applied: expected == Some(RouteVersion(7)),
                current: next.cloned().or_else(|| Some(sample_route(key.0.as_str()))),
            })
        }
    }

    fn sample_observer() -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new("observer", ClientEpoch(3)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: 1_000,
        }
    }

    fn sample_route(key: &str) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(7),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("owner", ClientEpoch(1)),
                segment_name: SegmentName::new("seg-a"),
                offset: 64,
                segment_offset: 64,
                length: 8,
                checksum: Some(9),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
        }
    }

    #[test]
    fn route_directory_default_batch_get_uses_single_lookup() {
        let observer = sample_observer();
        let directory = TestRouteDirectory;
        let routes = directory
            .get_object_routes(
                &observer,
                &[ObjectKey::new("alpha"), ObjectKey::new("missing")],
            )
            .expect("batch get should succeed");

        assert_eq!(routes.len(), 2);
        assert_eq!(
            routes[0].as_ref().map(|route| route.key.0.as_str()),
            Some("alpha")
        );
        assert!(routes[1].is_none());
    }

    #[test]
    fn route_directory_default_batch_cas_preserves_per_item_results() {
        let observer = sample_observer();
        let directory = TestRouteDirectory;
        let replacement = sample_route("beta");
        let results = directory
            .compare_and_swap_object_routes(
                &observer,
                &[
                    RouteCasRequest {
                        key: ObjectKey::new("alpha"),
                        expected: Some(RouteVersion(7)),
                        next: Some(sample_route("alpha")),
                    },
                    RouteCasRequest {
                        key: ObjectKey::new("beta"),
                        expected: Some(RouteVersion(1)),
                        next: Some(replacement.clone()),
                    },
                ],
            )
            .expect("batch cas should succeed");

        assert_eq!(results.len(), 2);
        assert!(
            results[0]
                .as_ref()
                .expect("first cas should succeed")
                .applied
        );
        assert!(
            !results[1]
                .as_ref()
                .expect("second cas should succeed")
                .applied
        );
        assert_eq!(
            results[1]
                .as_ref()
                .expect("second cas should succeed")
                .current
                .as_ref()
                .map(|route| route.key.0.as_str()),
            Some("beta")
        );
    }
}
