use std::collections::BTreeMap;

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy, RoutePolicyDomain, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
    TenantObjectAccounting, TenantObjectAccountingState, TenantPolicy, TenantPolicyScope,
    TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest,
    TenantQuotaReservation, TenantQuotaReservationOutcome, TenantQuotaReservationRequest,
    TenantQuotaReservationState, TenantQuotaState,
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
    tenant_quota_states: BTreeMap<TenantPolicyScope, TenantQuotaState>,
    tenant_object_accounting: BTreeMap<ObjectKey, TenantObjectAccounting>,
    tenant_quota_reservations: BTreeMap<String, TenantQuotaReservation>,
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

    fn root_scope(scope: &TenantPolicyScope) -> Result<TenantPolicyScope> {
        scope.validate_root_only("tenant quota metadata")?;
        Ok(TenantPolicyScope::new(
            scope.tenant.clone(),
            None::<String>,
            None::<String>,
        ))
    }
}

fn version_conflict(
    entity: &str,
    scope: &TenantPolicyScope,
    expected: Option<u64>,
    actual: Option<u64>,
) -> StoreError {
    match (expected, actual) {
        (Some(expected), Some(actual)) => StoreError::Conflict(format!(
            "{entity} version mismatch for {}: expected={} actual={}",
            scope.tenant, expected, actual
        )),
        (Some(expected), None) => StoreError::Conflict(format!(
            "{entity} missing for {} at expected version {}",
            scope.tenant, expected
        )),
        (None, Some(actual)) => StoreError::Conflict(format!(
            "{entity} already exists for {} at version {}",
            scope.tenant, actual
        )),
        (None, None) => {
            StoreError::Conflict(format!("{entity} write rejected for {}", scope.tenant))
        }
    }
}

fn non_negative_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value)
        .map_err(|_| StoreError::InvalidState(format!("{field} must not be negative, got {value}")))
}

fn magnitude_u64(value: i64, field: &str) -> Result<u64> {
    value
        .checked_abs()
        .ok_or_else(|| StoreError::InvalidState(format!("{field} magnitude overflow: {value}")))
        .and_then(|magnitude| {
            u64::try_from(magnitude).map_err(|_| {
                StoreError::InvalidState(format!("{field} magnitude overflow: {value}"))
            })
        })
}

fn apply_positive_delta(base: u64, delta: i64, field: &str) -> Result<u64> {
    if delta <= 0 {
        return Ok(base);
    }
    base.checked_add(non_negative_i64_to_u64(delta, field)?)
        .ok_or_else(|| {
            StoreError::InvalidState(format!("{field} overflow while applying delta {delta}"))
        })
}

fn apply_signed_delta(base: u64, delta: i64, field: &str) -> Result<u64> {
    if delta >= 0 {
        return apply_positive_delta(base, delta, field);
    }
    let magnitude = magnitude_u64(delta, field)?;
    base.checked_sub(magnitude).ok_or_else(|| {
        StoreError::InvalidState(format!("{field} underflow while applying delta {delta}"))
    })
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

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        let scope = Self::root_scope(scope)?;
        Ok(self.state.read().tenant_quota_states.get(&scope).cloned())
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        Ok(self.state.read().tenant_object_accounting.get(key).cloned())
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        let scope = Self::root_scope(scope)?;
        let mut reservations = self
            .state
            .read()
            .tenant_quota_reservations
            .values()
            .filter(|reservation| reservation.scope == scope)
            .cloned()
            .collect::<Vec<_>>();
        reservations.sort_by(|left, right| left.reservation_id.cmp(&right.reservation_id));
        Ok(reservations)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        request.validate()?;
        let scope = Self::root_scope(&request.scope)?;
        let mut state = self.state.write();
        if let Some(existing) = state
            .tenant_quota_reservations
            .get(&request.reservation_id)
            .cloned()
        {
            return match existing.state {
                TenantQuotaReservationState::Pending
                    if existing.scope == scope
                        && existing.key == request.key
                        && existing.expected_object_version == request.expected_object_version
                        && existing.delta_bytes == request.delta_bytes
                        && existing.delta_objects == request.delta_objects =>
                {
                    let quota =
                        state
                            .tenant_quota_states
                            .get(&scope)
                            .cloned()
                            .ok_or_else(|| {
                                StoreError::InvalidState(format!(
                                    "tenant quota state missing for {} while reservation {} exists",
                                    scope.tenant, request.reservation_id
                                ))
                            })?;
                    let object = state.tenant_object_accounting.get(&request.key).cloned();
                    Ok(TenantQuotaReservationOutcome {
                        quota,
                        object,
                        reservation: existing,
                    })
                }
                TenantQuotaReservationState::Pending => Err(StoreError::Conflict(format!(
                    "tenant quota reservation {} already exists with different parameters",
                    request.reservation_id
                ))),
                _ => Err(StoreError::Conflict(format!(
                    "tenant quota reservation {} is already {:?}",
                    request.reservation_id, existing.state
                ))),
            };
        }

        let current_object = state.tenant_object_accounting.get(&request.key).cloned();
        let actual_object_version = current_object.as_ref().map(|object| object.version);
        if actual_object_version != request.expected_object_version {
            return Err(version_conflict(
                "tenant object accounting",
                &scope,
                request.expected_object_version,
                actual_object_version,
            ));
        }
        if let Some(object) = current_object.as_ref() {
            object.validate()?;
            if object.scope != scope {
                return Err(StoreError::InvalidState(format!(
                    "tenant object accounting scope mismatch for key {}",
                    request.key.0
                )));
            }
        }

        let quota = state
            .tenant_quota_states
            .entry(scope.clone())
            .or_insert_with(|| TenantQuotaState {
                scope: scope.clone(),
                version: 0,
                used_bytes: 0,
                used_objects: 0,
                pending_reserved_bytes: 0,
                pending_reserved_objects: 0,
                updated_at_ms: request.created_at_ms,
                updated_by: request.writer_runtime.to_string(),
            });
        quota.validate()?;

        let positive_bytes = non_negative_i64_to_u64(request.delta_bytes.max(0), "delta_bytes")?;
        let positive_objects =
            non_negative_i64_to_u64(request.delta_objects.max(0), "delta_objects")?;
        if let Some(limit) = request.limit.max_bytes {
            let admitted = quota
                .used_bytes
                .checked_add(quota.pending_reserved_bytes)
                .and_then(|value| value.checked_add(positive_bytes))
                .ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "tenant quota admission overflow for {} bytes",
                        scope.tenant
                    ))
                })?;
            if admitted > limit {
                return Err(StoreError::Conflict(format!(
                    "tenant quota bytes exceeded for {}: used={} pending={} requested={} limit={}",
                    scope.tenant,
                    quota.used_bytes,
                    quota.pending_reserved_bytes,
                    positive_bytes,
                    limit
                )));
            }
        }
        if let Some(limit) = request.limit.max_objects {
            let limit = u64::try_from(limit).map_err(|_| {
                StoreError::InvalidState(format!(
                    "tenant object limit overflow for {}",
                    scope.tenant
                ))
            })?;
            let admitted = quota
                .used_objects
                .checked_add(quota.pending_reserved_objects)
                .and_then(|value| value.checked_add(positive_objects))
                .ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "tenant quota admission overflow for {} objects",
                        scope.tenant
                    ))
                })?;
            if admitted > limit {
                return Err(StoreError::Conflict(format!(
                    "tenant quota objects exceeded for {}: used={} pending={} requested={} limit={}",
                    scope.tenant,
                    quota.used_objects,
                    quota.pending_reserved_objects,
                    positive_objects,
                    limit
                )));
            }
        }

        quota.pending_reserved_bytes = apply_positive_delta(
            quota.pending_reserved_bytes,
            request.delta_bytes,
            "pending_reserved_bytes",
        )?;
        quota.pending_reserved_objects = apply_positive_delta(
            quota.pending_reserved_objects,
            request.delta_objects,
            "pending_reserved_objects",
        )?;
        quota.version = quota.version.saturating_add(1);
        quota.updated_at_ms = request.created_at_ms;
        quota.updated_by = request.writer_runtime.to_string();
        let updated_quota = quota.clone();

        let reservation = TenantQuotaReservation {
            reservation_id: request.reservation_id.clone(),
            scope: scope.clone(),
            key: request.key.clone(),
            version: 1,
            expected_object_version: request.expected_object_version,
            delta_bytes: request.delta_bytes,
            delta_objects: request.delta_objects,
            state: TenantQuotaReservationState::Pending,
            expires_at_ms: request.expires_at_ms,
            created_at_ms: request.created_at_ms,
            writer_runtime: request.writer_runtime.clone(),
        };
        reservation.validate()?;
        state
            .tenant_quota_reservations
            .insert(request.reservation_id.clone(), reservation.clone());

        Ok(TenantQuotaReservationOutcome {
            quota: updated_quota,
            object: current_object,
            reservation,
        })
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        request.validate()?;
        let mut state = self.state.write();
        let reservation = state
            .tenant_quota_reservations
            .get(&request.reservation_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(request.reservation_id.clone()))?;
        match reservation.state {
            TenantQuotaReservationState::Finalized => {
                let quota = state
                    .tenant_quota_states
                    .get(&reservation.scope)
                    .cloned()
                    .ok_or_else(|| {
                        StoreError::InvalidState(format!(
                            "tenant quota state missing for {} while reservation {} is finalized",
                            reservation.scope.tenant, reservation.reservation_id
                        ))
                    })?;
                let object = state
                    .tenant_object_accounting
                    .get(&reservation.key)
                    .cloned();
                return Ok(TenantQuotaFinalizeOutcome {
                    quota,
                    object,
                    reservation,
                });
            }
            TenantQuotaReservationState::Aborted => {
                return Err(StoreError::Conflict(format!(
                    "tenant quota reservation {} is already aborted",
                    reservation.reservation_id
                )));
            }
            TenantQuotaReservationState::Pending => {}
        }

        let scope = reservation.scope.clone();
        let current_object = state
            .tenant_object_accounting
            .get(&reservation.key)
            .cloned();
        let actual_object_version = current_object.as_ref().map(|object| object.version);
        let expected_object_version = request
            .expected_object_version
            .or(reservation.expected_object_version);
        if actual_object_version != expected_object_version {
            return Err(version_conflict(
                "tenant object accounting",
                &scope,
                expected_object_version,
                actual_object_version,
            ));
        }
        if let Some(object) = current_object.as_ref() {
            object.validate()?;
            if object.scope != scope {
                return Err(StoreError::InvalidState(format!(
                    "tenant object accounting scope mismatch for key {}",
                    reservation.key.0
                )));
            }
        }

        let updated_quota = {
            let quota = state.tenant_quota_states.get_mut(&scope).ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "tenant quota state missing for {} while finalizing reservation {}",
                    scope.tenant, reservation.reservation_id
                ))
            })?;
            quota.pending_reserved_bytes = apply_signed_delta(
                quota.pending_reserved_bytes,
                -reservation.delta_bytes.max(0),
                "pending_reserved_bytes",
            )?;
            quota.pending_reserved_objects = apply_signed_delta(
                quota.pending_reserved_objects,
                -reservation.delta_objects.max(0),
                "pending_reserved_objects",
            )?;
            quota.used_bytes =
                apply_signed_delta(quota.used_bytes, reservation.delta_bytes, "used_bytes")?;
            quota.used_objects = apply_signed_delta(
                quota.used_objects,
                reservation.delta_objects,
                "used_objects",
            )?;
            quota.version = quota.version.saturating_add(1);
            quota.updated_at_ms = request.updated_at_ms;
            quota.updated_by = request.updated_by.clone();
            quota.clone()
        };

        let next_object = match request.state {
            TenantObjectAccountingState::Active => Some(TenantObjectAccounting {
                key: reservation.key.clone(),
                scope: scope.clone(),
                version: expected_object_version.unwrap_or(0).saturating_add(1),
                committed_length: request
                    .committed_length
                    .expect("validated active finalize request"),
                route_version: request.route_version,
                state: TenantObjectAccountingState::Active,
                last_writer: request.updated_by.clone(),
                updated_at_ms: request.updated_at_ms,
            }),
            TenantObjectAccountingState::Deleted => None,
        };
        match next_object.as_ref() {
            Some(object) => {
                object.validate()?;
                state
                    .tenant_object_accounting
                    .insert(reservation.key.clone(), object.clone());
            }
            None => {
                state.tenant_object_accounting.remove(&reservation.key);
            }
        }

        let mut updated_reservation = reservation.clone();
        updated_reservation.state = TenantQuotaReservationState::Finalized;
        updated_reservation.version = updated_reservation.version.saturating_add(1);
        state.tenant_quota_reservations.insert(
            updated_reservation.reservation_id.clone(),
            updated_reservation.clone(),
        );

        Ok(TenantQuotaFinalizeOutcome {
            quota: updated_quota,
            object: next_object,
            reservation: updated_reservation,
        })
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        let mut state = self.state.write();
        let reservation = state
            .tenant_quota_reservations
            .get(reservation_id)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(reservation_id.to_string()))?;
        match reservation.state {
            TenantQuotaReservationState::Aborted => {
                let quota = state
                    .tenant_quota_states
                    .get(&reservation.scope)
                    .cloned()
                    .ok_or_else(|| {
                        StoreError::InvalidState(format!(
                            "tenant quota state missing for {} while reservation {} is aborted",
                            reservation.scope.tenant, reservation.reservation_id
                        ))
                    })?;
                return Ok(TenantQuotaAbortOutcome { quota, reservation });
            }
            TenantQuotaReservationState::Finalized => {
                return Err(StoreError::Conflict(format!(
                    "tenant quota reservation {} is already finalized",
                    reservation.reservation_id
                )));
            }
            TenantQuotaReservationState::Pending => {}
        }

        let updated_quota = {
            let quota = state
                .tenant_quota_states
                .get_mut(&reservation.scope)
                .ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "tenant quota state missing for {} while aborting reservation {}",
                        reservation.scope.tenant, reservation.reservation_id
                    ))
                })?;
            quota.pending_reserved_bytes = apply_signed_delta(
                quota.pending_reserved_bytes,
                -reservation.delta_bytes.max(0),
                "pending_reserved_bytes",
            )?;
            quota.pending_reserved_objects = apply_signed_delta(
                quota.pending_reserved_objects,
                -reservation.delta_objects.max(0),
                "pending_reserved_objects",
            )?;
            quota.version = quota.version.saturating_add(1);
            quota.updated_at_ms = reservation.created_at_ms;
            quota.updated_by = reservation.writer_runtime.to_string();
            quota.clone()
        };

        let mut updated_reservation = reservation.clone();
        updated_reservation.state = TenantQuotaReservationState::Aborted;
        updated_reservation.version = updated_reservation.version.saturating_add(1);
        state.tenant_quota_reservations.insert(
            updated_reservation.reservation_id.clone(),
            updated_reservation.clone(),
        );
        Ok(TenantQuotaAbortOutcome {
            quota: updated_quota,
            reservation: updated_reservation,
        })
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
        CompatibilityDescriptor, MetadataBackend, ObjectKey, RouteControlMode, RoutePolicy,
        RoutePolicyDomain, SegmentAnnouncement, SegmentLifecycleState, SegmentName, StoreError,
        TenantObjectAccountingState, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
        TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservationRequest,
        TenantQuotaReservationState,
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

    #[test]
    fn tenant_quota_reservation_finalize_and_abort_follow_versioned_state_machine() {
        let metadata = InMemoryMetadataBackend::new();
        let scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let key = ObjectKey::new("tenant-a::alpha");
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        let reserved = metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-create".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 200,
                created_at_ms: 100,
                writer_runtime: writer.clone(),
            })
            .expect("reservation should succeed");
        assert_eq!(reserved.quota.pending_reserved_bytes, 32);
        assert_eq!(reserved.quota.pending_reserved_objects, 1);
        assert!(reserved.object.is_none());
        assert_eq!(
            reserved.reservation.state,
            TenantQuotaReservationState::Pending
        );

        let finalized = metadata
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-create".to_string(),
                expected_object_version: None,
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 120,
                updated_by: "writer".to_string(),
            })
            .expect("finalize should succeed");
        assert_eq!(finalized.quota.used_bytes, 32);
        assert_eq!(finalized.quota.used_objects, 1);
        assert_eq!(finalized.quota.pending_reserved_bytes, 0);
        assert_eq!(finalized.quota.pending_reserved_objects, 0);
        assert_eq!(
            finalized
                .object
                .as_ref()
                .expect("object accounting should exist")
                .committed_length,
            32
        );
        assert_eq!(
            finalized.reservation.state,
            TenantQuotaReservationState::Finalized
        );

        let duplicate_finalize = metadata
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-create".to_string(),
                expected_object_version: Some(1),
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 121,
                updated_by: "writer".to_string(),
            })
            .expect("duplicate finalize should be idempotent");
        assert_eq!(duplicate_finalize.quota.used_bytes, 32);
        assert_eq!(duplicate_finalize.reservation.version, 2);

        let overwrite = metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-overwrite".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: 8,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 260,
                created_at_ms: 140,
                writer_runtime: writer.clone(),
            })
            .expect("overwrite reservation should succeed");
        assert_eq!(
            overwrite
                .object
                .expect("object accounting should exist")
                .version,
            1
        );
        assert_eq!(overwrite.quota.pending_reserved_bytes, 8);

        let conflict = metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-conflict".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(99),
                delta_bytes: 1,
                delta_objects: 0,
                limit: TenantQuotaPolicy::default(),
                expires_at_ms: 300,
                created_at_ms: 150,
                writer_runtime: writer.clone(),
            })
            .expect_err("stale object version should fail");
        assert!(matches!(conflict, StoreError::Conflict(_)));

        let aborted = metadata
            .abort_tenant_quota("resv-overwrite")
            .expect("abort should succeed");
        assert_eq!(aborted.quota.pending_reserved_bytes, 0);
        assert_eq!(aborted.quota.used_bytes, 32);
        assert_eq!(
            aborted.reservation.state,
            TenantQuotaReservationState::Aborted
        );

        let duplicate_abort = metadata
            .abort_tenant_quota("resv-overwrite")
            .expect("duplicate abort should be idempotent");
        assert_eq!(duplicate_abort.quota.pending_reserved_bytes, 0);
        assert_eq!(duplicate_abort.reservation.version, 2);

        let delete_reserved = metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-delete".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: -32,
                delta_objects: -1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 400,
                created_at_ms: 200,
                writer_runtime: writer.clone(),
            })
            .expect("delete reservation should succeed");
        assert_eq!(delete_reserved.quota.pending_reserved_bytes, 0);
        assert_eq!(delete_reserved.quota.used_bytes, 32);

        let deleted = metadata
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-delete".to_string(),
                expected_object_version: Some(1),
                committed_length: None,
                route_version: None,
                state: TenantObjectAccountingState::Deleted,
                updated_at_ms: 220,
                updated_by: "writer".to_string(),
            })
            .expect("delete finalize should succeed");
        assert_eq!(deleted.quota.used_bytes, 0);
        assert_eq!(deleted.quota.used_objects, 0);
        assert!(deleted.object.is_none());
        assert!(metadata
            .get_tenant_object_accounting(&key)
            .expect("object accounting lookup should succeed")
            .is_none());
    }

    #[test]
    fn tenant_quota_reservation_enforces_limits_and_lists_by_tenant() {
        let metadata = InMemoryMetadataBackend::new();
        let scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-1".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::one"),
                expected_object_version: None,
                delta_bytes: 40,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(1),
                },
                expires_at_ms: 100,
                created_at_ms: 10,
                writer_runtime: writer.clone(),
            })
            .expect("first reservation should succeed");

        let byte_limit = metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-2".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::two"),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 110,
                created_at_ms: 11,
                writer_runtime: writer.clone(),
            })
            .expect_err("bytes over limit should fail");
        assert!(matches!(byte_limit, StoreError::Conflict(_)));

        let object_limit = metadata
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-3".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::three"),
                expected_object_version: None,
                delta_bytes: 1,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(128),
                    max_objects: Some(1),
                },
                expires_at_ms: 120,
                created_at_ms: 12,
                writer_runtime: writer,
            })
            .expect_err("objects over limit should fail");
        assert!(matches!(object_limit, StoreError::Conflict(_)));

        let reservations = metadata
            .list_tenant_quota_reservations(&scope)
            .expect("reservation listing should succeed");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].reservation_id, "resv-1");
        let quota = metadata
            .get_tenant_quota_state(&scope)
            .expect("quota state lookup should succeed")
            .expect("quota state should exist");
        assert_eq!(quota.pending_reserved_bytes, 40);
        assert_eq!(quota.pending_reserved_objects, 1);
    }
}
