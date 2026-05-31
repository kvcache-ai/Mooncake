use crate::error::Result;
use crate::identity::{
    ClientRuntimeId, ClientStableId, LogicalObjectId, NamespaceScope, ReuseIdentity,
};
use crate::lifecycle::{ClientLifecycleState, HandoffPlan};
use crate::route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, RouteCasRequest, RoutePolicy,
    RoutePolicyDomain, RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
    SegmentReservation, TenantObjectAccounting, TenantPolicy, TenantPolicyScope,
    TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest,
    TenantQuotaReservation, TenantQuotaReservationOutcome, TenantQuotaReservationRequest,
    TenantQuotaState,
};
use std::sync::Arc;

pub trait MetadataBackend: Send + Sync {
    fn route_namespace(&self) -> String;

    fn for_tenant(&self, _tenant: &str) -> Option<Arc<dyn MetadataBackend>> {
        None
    }

    /// Low-cardinality backend label for Store-RS metadata metrics.
    fn backend_kind(&self) -> &'static str {
        "unknown"
    }

    #[doc(hidden)]
    fn metrics_observed(&self) -> bool {
        false
    }

    /// Republishes a client lease at an already-assigned `(stable_id, epoch)`.
    ///
    /// This path is for heartbeat refresh, lifecycle-state updates, and low-level
    /// test setup. Implementations MUST reject writes whose `lease.runtime.epoch`
    /// is not strictly greater than every epoch ever observed for the same
    /// `lease.runtime.stable_id` unless the lease key already exists (refresh).
    ///
    /// If the exact lease key disappeared after expiry, implementations MAY also
    /// accept a same-epoch reclaim for the current historical high-water-mark
    /// epoch when no higher live epoch for that stable id exists. This keeps
    /// heartbeat repair and predecessor drain pinning from spuriously failing
    /// after a lease TTL gap while preserving monotonic epoch assignment.
    ///
    /// Other stale writes must be rejected with `StoreError::StaleEpoch`.
    ///
    /// Normal registration goes through [`Self::allocate_client_lease`]; callers
    /// SHOULD NOT invent new epoch values here.
    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()>;

    /// Atomically allocates the next epoch for `template.runtime.stable_id` and
    /// publishes the lease under that epoch.
    ///
    /// The epoch field of `template.runtime` is ignored. Implementations compute
    /// `new_epoch = max(active epochs for stable_id, historical HWM) + 1`, mutate
    /// the serialized lease to carry `new_epoch`, and commit the lease key, the
    /// per-stable-id active-epoch index, and the HWM in a single atomic step.
    ///
    /// Returns the assigned `ClientRuntimeId`. Callers should use the returned
    /// runtime for subsequent [`Self::update_client_state`] and refresh calls.
    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId>;

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()>;

    /// Looks up a live client lease by runtime id.
    ///
    /// # Performance
    /// The default implementation scans `list_live_clients()` in memory and is
    /// intended for tests or small datasets. Production backends should provide
    /// an indexed implementation.
    fn get_client_lease(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLease>> {
        Ok(self
            .list_live_clients()?
            .into_iter()
            .find(|lease| lease.runtime == *runtime))
    }

    /// Looks up a live client lease by stable id.
    ///
    /// # Performance
    /// The default implementation scans `list_live_clients()` in memory and is
    /// intended for tests or small datasets. Production backends should provide
    /// an indexed implementation.
    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> Result<Option<ClientLease>> {
        Ok(self
            .list_live_clients()?
            .into_iter()
            .find(|lease| lease.runtime.stable_id == *stable_id))
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>>;

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()>;

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()>;

    /// Looks up a segment by owner and segment name.
    ///
    /// # Performance
    /// The default implementation scans `list_segments()` in memory and is
    /// intended for tests or small datasets. Production backends should provide
    /// an indexed implementation.
    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<Option<SegmentAnnouncement>> {
        Ok(self
            .list_segments(Some(owner))?
            .into_iter()
            .find(|entry| entry.segment_name == *segment))
    }

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

    /// Lists object routes in the given namespace scope.
    ///
    /// # Performance
    /// The default implementation filters the full route set in memory and is
    /// intended for tests or small datasets. Production backends should provide
    /// a scope-aware implementation.
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

    /// Looks up an object route by logical object id.
    fn get_object_route_by_id(&self, object_id: &LogicalObjectId) -> Result<Option<ObjectRoute>> {
        self.get_object_route(&ObjectKey::from_logical_id(object_id))
    }

    /// Lists object routes that share the same reuse identity.
    ///
    /// # Performance
    /// The default implementation filters the full route set in memory and is
    /// intended for tests or small datasets. Production backends should provide
    /// an indexed implementation when available.
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

    /// Looks up tenant policies by exact scopes.
    ///
    /// # Performance
    /// The default implementation performs one exact lookup per scope and is
    /// intended for tests or small batches. Production backends should
    /// override this when they can batch exact reads without falling back to
    /// scans.
    fn get_tenant_policies(
        &self,
        scopes: &[TenantPolicyScope],
    ) -> Result<Vec<Option<TenantPolicy>>> {
        scopes
            .iter()
            .map(|scope| self.get_tenant_policy(scope))
            .collect()
    }

    fn list_tenant_policies(&self, tenant: Option<&str>) -> Result<Vec<TenantPolicy>>;

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

    fn get_tenant_quota_state(&self, scope: &TenantPolicyScope)
        -> Result<Option<TenantQuotaState>>;

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>>;

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> Result<Option<TenantQuotaReservation>>;

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> Result<Vec<TenantObjectAccounting>>;

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>>;

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome>;

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome>;

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome>;

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

    /// Looks up routes without exhaustive fallback probing.
    ///
    /// Hot existence checks use this path because a miss is common and must not
    /// fan out to every route authority in the cluster.
    fn get_object_routes_bounded(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        self.get_object_routes(observer, keys)
    }

    fn contains_object_route(&self, observer: &ClientLease, key: &ObjectKey) -> Result<bool> {
        Ok(self.get_object_route(observer, key)?.is_some())
    }

    fn contains_object_routes_bounded(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<bool>> {
        Ok(self
            .get_object_routes_bounded(observer, keys)?
            .into_iter()
            .map(|r| r.is_some())
            .collect())
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

    fn list_routes_in_scope(
        &self,
        _observer: &ClientLease,
        _scope: &NamespaceScope,
    ) -> Result<Vec<ObjectRoute>> {
        Err(crate::error::StoreError::Unsupported(
            "route directory does not support list_routes_in_scope".to_string(),
        ))
    }

    fn list_reuse_candidates(
        &self,
        _observer: &ClientLease,
        _reuse: &ReuseIdentity,
    ) -> Result<Vec<ObjectRoute>> {
        Err(crate::error::StoreError::Unsupported(
            "route directory does not support list_reuse_candidates".to_string(),
        ))
    }

    fn get_version_floor(&self, _observer: &ClientLease, _key: &ObjectKey) -> Option<RouteVersion> {
        None
    }

    fn get_version_floors(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Vec<Option<RouteVersion>> {
        keys.iter()
            .map(|key| self.get_version_floor(observer, key))
            .collect()
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
                version_floor: None,
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
                offset: Some(64),
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
