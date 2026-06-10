use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_store_core::error::QuotaKind;
use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
    ColdTierDeviceFilter, ColdTierDeviceRecord, ColdTierDeviceUpdate, ColdTierPutDeviceResult,
    ColdTierUsageDelta, HandoffPlan, MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy,
    RoutePolicyDomain, RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
    SegmentReservation, StoreError, TenantObjectAccounting, TenantObjectAccountingState,
    TenantPolicy, TenantPolicyScope, TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome,
    TenantQuotaFinalizeRequest, TenantQuotaReservation, TenantQuotaReservationOutcome,
    TenantQuotaReservationRequest, TenantQuotaReservationState, TenantQuotaState,
};
use parking_lot::RwLock;

use crate::cold_tier::{apply_cold_tier_usage_delta_to_record, cold_tier_device_matches_filter};
use crate::keyspace::encode_key_component;
use crate::segment_state::StoredSegmentState;

static NEXT_NAMESPACE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct InMemoryState {
    clients: BTreeMap<String, ClientLease>,
    client_by_stable: BTreeMap<String, BTreeSet<u64>>,
    client_epoch_hwm: BTreeMap<String, u64>,
    stable_runtimes: BTreeMap<ClientStableId, String>,
    handoffs: BTreeMap<String, HandoffPlan>,
    objects: BTreeMap<String, ObjectRoute>,
    cold_tier_devices: BTreeMap<String, ColdTierDeviceRecord>,
    route_policies: BTreeMap<RoutePolicyDomain, RoutePolicy>,
    tenant_policies: BTreeMap<TenantPolicyScope, TenantPolicy>,
    tenant_quota_states: BTreeMap<TenantPolicyScope, TenantQuotaState>,
    tenant_object_accounting: BTreeMap<ObjectKey, TenantObjectAccounting>,
    tenant_quota_reservations: BTreeMap<String, TenantQuotaReservation>,
    segments: BTreeMap<String, StoredSegmentState>,
}

pub struct InMemoryMetadataBackend {
    state: Arc<RwLock<InMemoryState>>,
    namespace_id: u64,
    tenant_prefix: Option<String>,
    default_tenant_legacy_mode: bool,
}

impl Default for InMemoryMetadataBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryMetadataBackend {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(InMemoryState::default())),
            namespace_id: NEXT_NAMESPACE_ID.fetch_add(1, Ordering::Relaxed),
            tenant_prefix: None,
            default_tenant_legacy_mode: true,
        }
    }

    pub fn new_hard_isolated() -> Self {
        Self {
            state: Arc::new(RwLock::new(InMemoryState::default())),
            namespace_id: NEXT_NAMESPACE_ID.fetch_add(1, Ordering::Relaxed),
            tenant_prefix: None,
            default_tenant_legacy_mode: false,
        }
    }

    fn effective_tenant_prefix(&self) -> Option<&str> {
        if self.default_tenant_legacy_mode {
            None
        } else {
            self.tenant_prefix.as_deref()
        }
    }

    fn tenant_storage_prefix(&self) -> Option<String> {
        self.effective_tenant_prefix()
            .map(|tenant| format!("tenant:{}", encode_key_component(tenant)))
    }

    fn scoped_key(&self, key: impl AsRef<str>) -> String {
        match self.tenant_storage_prefix() {
            Some(prefix) => format!("{prefix}/{}", key.as_ref()),
            None => key.as_ref().to_string(),
        }
    }

    fn scoped_stable_id_key(&self, stable_id: &ClientStableId) -> ClientStableId {
        match self.tenant_storage_prefix() {
            Some(prefix) => ClientStableId(format!("{prefix}/{}", stable_id.0)),
            None => stable_id.clone(),
        }
    }

    fn scoped_runtime(&self, runtime: &ClientRuntimeId) -> ClientRuntimeId {
        ClientRuntimeId::new(self.scoped_key(&runtime.stable_id.0), runtime.epoch)
    }

    fn unscoped_runtime(&self, runtime: ClientRuntimeId) -> ClientRuntimeId {
        match self.tenant_storage_prefix() {
            Some(prefix) => {
                let stable_id = runtime
                    .stable_id
                    .0
                    .strip_prefix(&prefix)
                    .and_then(|rest| rest.strip_prefix('/'))
                    .unwrap_or(runtime.stable_id.0.as_str());
                ClientRuntimeId::new(stable_id, runtime.epoch)
            }
            None => runtime,
        }
    }

    fn scope_lease(&self, lease: &ClientLease) -> ClientLease {
        let mut lease = lease.clone();
        lease.runtime = self.scoped_runtime(&lease.runtime);
        lease
    }

    fn unscope_lease(&self, lease: ClientLease) -> ClientLease {
        let mut lease = lease;
        lease.runtime = self.unscoped_runtime(lease.runtime);
        lease
    }

    fn segment_key(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> String {
        let owner = self.scoped_runtime(owner);
        format!("{}:{}", owner.storage_key(), segment.0)
    }

    fn is_present_lease(lease: &ClientLease) -> bool {
        lease.expires_at_ms >= now_ms()
    }

    fn is_readable_lease(lease: &ClientLease) -> bool {
        lease.state.serves_reads() && Self::is_present_lease(lease)
    }

    fn root_scope(scope: &TenantPolicyScope) -> Result<TenantPolicyScope> {
        scope.validate_root_only("tenant quota metadata")?;
        Ok(TenantPolicyScope::new(
            scope.tenant.clone(),
            None::<String>,
            None::<String>,
        ))
    }

    fn prune_stale_epochs(state: &mut InMemoryState, stable_id: &str) -> u64 {
        if !state.client_by_stable.contains_key(stable_id) {
            return 0;
        }
        let active_epochs = state
            .clients
            .values()
            .filter(|lease| lease.runtime.stable_id.0 == stable_id)
            .map(|lease| lease.runtime.epoch.0)
            .collect::<BTreeSet<_>>();
        let active_max = active_epochs.iter().next_back().copied().unwrap_or(0);
        if active_epochs.is_empty() {
            state.client_by_stable.remove(stable_id);
        } else {
            state
                .client_by_stable
                .insert(stable_id.to_string(), active_epochs);
        }
        active_max
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl MetadataBackend for InMemoryMetadataBackend {
    fn route_namespace(&self) -> String {
        match self.effective_tenant_prefix() {
            Some(prefix) => format!("inmemory://{}/tenants/{}", self.namespace_id, prefix),
            None => format!("inmemory://{}", self.namespace_id),
        }
    }

    fn for_tenant(&self, tenant: &str) -> Option<Arc<dyn MetadataBackend>> {
        Some(Arc::new(Self {
            state: self.state.clone(),
            namespace_id: self.namespace_id,
            tenant_prefix: Some(tenant.to_string()),
            default_tenant_legacy_mode: self.default_tenant_legacy_mode,
        }))
    }

    fn backend_kind(&self) -> &'static str {
        "in_memory"
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        use std::collections::btree_map::Entry;

        let lease = self.scope_lease(lease);
        let storage_key = lease.runtime.storage_key();
        let stable_id = lease.runtime.stable_id.clone();
        let stable_id_key = stable_id.0.clone();
        let new_epoch = lease.runtime.epoch.0;
        let mut state = self.state.write();

        if let Entry::Occupied(mut entry) = state.clients.entry(storage_key.clone()) {
            entry.insert(lease.clone());
            if lease.state == ClientLifecycleState::Active {
                state
                    .stable_runtimes
                    .insert(stable_id.clone(), storage_key.clone());
            } else if state.stable_runtimes.get(&stable_id) == Some(&storage_key) {
                state.stable_runtimes.remove(&stable_id);
            }
            return Ok(());
        }

        let active_max = Self::prune_stale_epochs(&mut state, &stable_id_key);
        let hwm = state
            .client_epoch_hwm
            .get(&stable_id_key)
            .copied()
            .unwrap_or(0);
        let floor = active_max.max(hwm);
        let allow_same_epoch_reclaim = new_epoch == hwm && new_epoch > active_max;
        if new_epoch <= floor && !allow_same_epoch_reclaim {
            return Err(StoreError::StaleEpoch(format!(
                "lease rejected: proposed epoch {new_epoch} <= floor {floor} for stable_id {stable_id_key}"
            )));
        }

        if lease.state == ClientLifecycleState::Active {
            state
                .stable_runtimes
                .insert(stable_id.clone(), storage_key.clone());
        }
        state.clients.insert(storage_key, lease.clone());
        state
            .client_by_stable
            .entry(stable_id_key.clone())
            .or_default()
            .insert(new_epoch);
        if new_epoch > hwm {
            state.client_epoch_hwm.insert(stable_id_key, new_epoch);
        }
        Ok(())
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        let template = self.scope_lease(template);
        let stable_id = template.runtime.stable_id.0.clone();
        let mut state = self.state.write();

        let active_max = Self::prune_stale_epochs(&mut state, &stable_id);
        let hwm = state.client_epoch_hwm.get(&stable_id).copied().unwrap_or(0);
        let new_epoch = active_max.max(hwm).saturating_add(1);

        let mut lease = template.clone();
        lease.runtime.epoch = ClientEpoch(new_epoch);
        let storage_key = lease.runtime.storage_key();
        let runtime = lease.runtime.clone();

        state.clients.insert(storage_key, lease);
        state
            .client_by_stable
            .entry(stable_id.clone())
            .or_default()
            .insert(new_epoch);
        state.client_epoch_hwm.insert(stable_id, new_epoch);
        Ok(self.unscoped_runtime(runtime))
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        let mut state = self.state.write();
        let runtime = self.scoped_runtime(runtime);
        let runtime_key = runtime.storage_key();
        {
            let Some(lease) = state.clients.get_mut(&runtime_key) else {
                return Err(StoreError::NotFound(runtime_key));
            };
            lease.state = next;
        }
        if next != ClientLifecycleState::Active {
            state.stable_runtimes.remove(&runtime.stable_id);
        }
        Ok(())
    }

    fn get_client_lease(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLease>> {
        Ok(self
            .state
            .read()
            .clients
            .get(&self.scoped_runtime(runtime).storage_key())
            .filter(|lease| Self::is_present_lease(lease))
            .cloned()
            .map(|lease| self.unscope_lease(lease)))
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> Result<Option<ClientLease>> {
        let state = self.state.read();
        let stable_id = self.scoped_stable_id_key(stable_id);
        let Some(runtime_key) = state.stable_runtimes.get(&stable_id) else {
            return Ok(None);
        };
        Ok(state
            .clients
            .get(runtime_key)
            .filter(|lease| Self::is_readable_lease(lease))
            .cloned()
            .map(|lease| self.unscope_lease(lease)))
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        Ok(self
            .state
            .read()
            .clients
            .values()
            .filter(|lease| {
                self.tenant_storage_prefix()
                    .as_deref()
                    .is_none_or(|prefix| {
                        lease.runtime.stable_id.0.starts_with(&format!("{prefix}/"))
                    })
            })
            .cloned()
            .map(|lease| self.unscope_lease(lease))
            .collect())
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        let key = self.segment_key(&segment.owner, &segment.segment_name);
        let mut segment = segment.clone();
        segment.owner = self.scoped_runtime(&segment.owner);
        let mut state = self.state.write();
        match state.segments.get_mut(&key) {
            Some(current) => current.merge_announcement(&segment),
            None => {
                state.segments.insert(key, StoredSegmentState::new(segment));
            }
        }
        Ok(())
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        let mut state = self.state.write();
        state.segments.remove(&self.segment_key(owner, segment));
        Ok(())
    }

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<Option<SegmentAnnouncement>> {
        Ok(self
            .state
            .read()
            .segments
            .get(&self.segment_key(owner, segment))
            .map(|segment| {
                let mut announcement = segment.announcement.clone();
                announcement.owner = self.unscoped_runtime(announcement.owner);
                announcement
            }))
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        let scoped_owner = owner.map(|owner| self.scoped_runtime(owner));
        Ok(self
            .state
            .read()
            .segments
            .values()
            .filter(|segment| {
                scoped_owner
                    .as_ref()
                    .is_none_or(|owner| &segment.announcement.owner == owner)
                    && self
                        .tenant_storage_prefix()
                        .as_deref()
                        .is_none_or(|prefix| {
                            segment
                                .announcement
                                .owner
                                .stable_id
                                .0
                                .starts_with(&format!("{prefix}/"))
                        })
            })
            .map(|segment| {
                let mut announcement = segment.announcement.clone();
                announcement.owner = self.unscoped_runtime(announcement.owner);
                announcement
            })
            .collect())
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        let key = self.segment_key(owner, segment);
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
        let key = self.segment_key(owner, segment);
        let scoped_owner = self.scoped_runtime(owner);
        let segment_state = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        segment_state.reserve(&scoped_owner, segment, length_bytes)
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let mut state = self.state.write();
        let key = self.segment_key(owner, segment);
        let scoped_owner = self.scoped_runtime(owner);
        let segment_state = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        segment_state.release(&scoped_owner, segment, offset_bytes, length_bytes)
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        Ok(self
            .state
            .read()
            .objects
            .get(&self.scoped_key(&key.0))
            .cloned())
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        Ok(self
            .state
            .read()
            .objects
            .iter()
            .filter(|(key, _)| {
                self.tenant_storage_prefix()
                    .as_deref()
                    .is_none_or(|prefix| key.starts_with(&format!("{prefix}/")))
            })
            .map(|(_, route)| route.clone())
            .collect())
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let mut state = self.state.write();
        let storage_key = self.scoped_key(&key.0);
        let current = state.objects.get(&storage_key).cloned();
        let matches = match (expected, current.as_ref()) {
            (None, None) => true,
            (Some(version), Some(route)) => route.version == version,
            _ => false,
        };

        if !matches {
            return Ok(CasResult {
                applied: false,
                current,
                version_floor: None,
            });
        }

        match next {
            Some(route) => {
                state.objects.insert(storage_key.clone(), route.clone());
            }
            None => {
                state.objects.remove(&storage_key);
            }
        }

        Ok(CasResult {
            applied: true,
            current: next.cloned(),
            version_floor: None,
        })
    }

    fn put_cold_tier_device_if_absent(
        &self,
        device: &ColdTierDeviceRecord,
    ) -> Result<ColdTierPutDeviceResult> {
        let key = self.scoped_key(&device.device_id);
        let mut state = self.state.write();
        if let Some(existing) = state.cold_tier_devices.get(&key) {
            return Ok(ColdTierPutDeviceResult::Existing(existing.clone()));
        }
        state.cold_tier_devices.insert(key, device.clone());
        Ok(ColdTierPutDeviceResult::Created(device.clone()))
    }

    fn get_cold_tier_device(&self, device_id: &str) -> Result<Option<ColdTierDeviceRecord>> {
        Ok(self
            .state
            .read()
            .cold_tier_devices
            .get(&self.scoped_key(device_id))
            .cloned())
    }

    fn list_cold_tier_devices(
        &self,
        filter: &ColdTierDeviceFilter,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        let storage_prefix = self.tenant_storage_prefix();
        let mut devices = self
            .state
            .read()
            .cold_tier_devices
            .iter()
            .filter(|(key, _)| match storage_prefix.as_deref() {
                Some(prefix) => key
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('/')),
                None if self.default_tenant_legacy_mode => true,
                None => !key.starts_with("tenant:"),
            })
            .map(|(_, device)| device)
            .filter(|device| cold_tier_device_matches_filter(device, filter))
            .cloned()
            .collect::<Vec<_>>();
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        Ok(devices)
    }

    fn update_cold_tier_device(
        &self,
        device_id: &str,
        update: ColdTierDeviceUpdate,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.scoped_key(device_id);
        let mut state = self.state.write();
        let device = state
            .cold_tier_devices
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(device_id.to_string()))?;
        if update
            .expected_updated_at_ms
            .is_some_and(|expected| device.updated_at_ms != expected)
        {
            return Err(StoreError::Conflict(format!(
                "cold tier device {device_id} update timestamp mismatch"
            )));
        }
        update.apply(device);
        Ok(device.clone())
    }

    fn apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        delta: ColdTierUsageDelta,
        updated_at_ms: u64,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.scoped_key(device_id);
        let mut state = self.state.write();
        let device = state
            .cold_tier_devices
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(device_id.to_string()))?;
        apply_cold_tier_usage_delta_to_record(device, &delta, updated_at_ms)?;
        Ok(device.clone())
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

    fn list_tenant_policies(&self, tenant: Option<&str>) -> Result<Vec<TenantPolicy>> {
        let mut policies = self
            .state
            .read()
            .tenant_policies
            .values()
            .filter(|policy| tenant.is_none_or(|tenant| policy.scope.tenant == tenant))
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

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> Result<Option<TenantQuotaReservation>> {
        Ok(self
            .state
            .read()
            .tenant_quota_reservations
            .get(reservation_id)
            .cloned())
    }

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> Result<Vec<TenantObjectAccounting>> {
        let scope = Self::root_scope(scope)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut candidates = self
            .state
            .read()
            .tenant_object_accounting
            .values()
            .filter(|object| {
                object.scope == scope && object.state == TenantObjectAccountingState::Active
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.updated_at_ms
                .cmp(&right.updated_at_ms)
                .then_with(|| right.committed_length.cmp(&left.committed_length))
                .then_with(|| left.key.cmp(&right.key))
        });
        candidates.truncate(limit);
        Ok(candidates)
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
                return Err(StoreError::QuotaExceeded {
                    kind: QuotaKind::Bytes,
                    message: format!(
                        "tenant quota bytes exceeded for {}: used={} pending={} requested={} limit={}",
                        scope.tenant,
                        quota.used_bytes,
                        quota.pending_reserved_bytes,
                        positive_bytes,
                        limit
                    ),
                });
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
                return Err(StoreError::QuotaExceeded {
                    kind: QuotaKind::Objects,
                    message: format!(
                        "tenant quota objects exceeded for {}: used={} pending={} requested={} limit={}",
                        scope.tenant,
                        quota.used_objects,
                        quota.pending_reserved_objects,
                        positive_objects,
                        limit
                    ),
                });
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
        let mut handoff = handoff.clone();
        handoff.stable_id = self.scoped_stable_id_key(&handoff.stable_id);
        handoff.from = self.scoped_runtime(&handoff.from);
        handoff.to = self.scoped_runtime(&handoff.to);
        self.state
            .write()
            .handoffs
            .insert(handoff.stable_id.0.clone(), handoff);
        Ok(())
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        Ok(self
            .state
            .read()
            .handoffs
            .get(&self.scoped_key(&stable_id.0))
            .cloned()
            .map(|mut handoff| {
                handoff.stable_id = self
                    .unscoped_runtime(ClientRuntimeId::new(
                        handoff.stable_id.0.as_str(),
                        ClientEpoch(0),
                    ))
                    .stable_id;
                handoff.from = self.unscoped_runtime(handoff.from);
                handoff.to = self.unscoped_runtime(handoff.to);
                handoff
            }))
    }
}

#[cfg(test)]
mod tests {
    use mooncake_store_core::error::QuotaKind;
    use mooncake_store_core::{
        ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
        CompatibilityDescriptor, HandoffKind, MetadataBackend, ObjectKey, RouteControlMode,
        RoutePolicy, RoutePolicyDomain, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
        StoreError, TenantObjectAccountingState, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
        TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservationRequest,
        TenantQuotaReservationState,
    };

    use super::{now_ms, InMemoryMetadataBackend};

    use mooncake_store_core::{ObjectRoute, ReplicaRoute, ReplicaTier, RouteState, RouteVersion};

    fn active_lease(stable_id: &str, epoch: u64) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(epoch)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: now_ms() + 10_000,
        }
    }

    fn sample_segment(owner: &ClientRuntimeId, name: &str, capacity: u64) -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: SegmentName::new(name),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: capacity,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 64,
            tags: vec![],
        }
    }

    fn sample_route(key_str: &str, owner_id: &str, seg_name: &str) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key_str),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new(owner_id, ClientEpoch(1)),
                segment_name: SegmentName::new(seg_name),
                offset: Some(0),
                segment_offset: 0,
                length: 4096,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
        }
    }

    #[test]
    fn tenant_views_isolate_runtime_segments_objects_and_handoffs() {
        let metadata = InMemoryMetadataBackend::new_hard_isolated();
        let tenant_a = metadata.for_tenant("tenant-a").expect("tenant view");
        let tenant_b = metadata.for_tenant("tenant-b").expect("tenant view");
        assert_ne!(tenant_a.route_namespace(), tenant_b.route_namespace());

        let lease = active_lease("writer", 1);
        tenant_a
            .upsert_client_lease(&lease)
            .expect("tenant-a lease should write");
        assert!(tenant_a.get_client_lease(&lease.runtime).unwrap().is_some());
        assert!(tenant_b.get_client_lease(&lease.runtime).unwrap().is_none());
        assert_eq!(tenant_a.list_live_clients().unwrap().len(), 1);
        assert!(tenant_b.list_live_clients().unwrap().is_empty());

        let segment = sample_segment(&lease.runtime, "seg-a", 4096);
        tenant_a
            .publish_segment(&segment)
            .expect("tenant-a segment should write");
        assert!(tenant_a
            .get_segment(&lease.runtime, &SegmentName::new("seg-a"))
            .unwrap()
            .is_some());
        assert!(tenant_b
            .get_segment(&lease.runtime, &SegmentName::new("seg-a"))
            .unwrap()
            .is_none());
        assert_eq!(tenant_a.list_segments(None).unwrap().len(), 1);
        assert!(tenant_b.list_segments(None).unwrap().is_empty());

        let key = ObjectKey::new("object-a");
        let route = sample_route("object-a", "writer", "seg-a");
        assert!(
            tenant_a
                .compare_and_swap_object_route(&key, None, Some(&route))
                .unwrap()
                .applied
        );
        assert!(tenant_a.get_object_route(&key).unwrap().is_some());
        assert!(tenant_b.get_object_route(&key).unwrap().is_none());
        assert_eq!(tenant_a.list_object_routes().unwrap().len(), 1);
        assert!(tenant_b.list_object_routes().unwrap().is_empty());

        let handoff = mooncake_store_core::HandoffPlan {
            stable_id: ClientStableId::new("writer"),
            from: lease.runtime.clone(),
            to: ClientRuntimeId::new("writer", ClientEpoch(2)),
            kind: HandoffKind::HotUpgrade,
            barrier_version: 1,
            created_at_ms: now_ms(),
            deadline_ms: Some(now_ms() + 10_000),
        };
        tenant_a.put_handoff(&handoff).expect("handoff write");
        let stored_handoff = tenant_a
            .get_handoff(&ClientStableId::new("writer"))
            .unwrap()
            .expect("tenant-a handoff should be visible");
        assert_eq!(stored_handoff, handoff);
        assert!(tenant_b
            .get_handoff(&ClientStableId::new("writer"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn tenant_views_do_not_overlap_when_tenant_names_share_prefixes() {
        let metadata = InMemoryMetadataBackend::new_hard_isolated();
        let tenant_a = metadata.for_tenant("a").expect("tenant view");
        let tenant_ab = metadata.for_tenant("a/b").expect("tenant view");
        let lease = active_lease("writer", 1);

        tenant_ab
            .upsert_client_lease(&lease)
            .expect("tenant a/b lease should write");
        tenant_ab
            .publish_segment(&sample_segment(&lease.runtime, "seg", 4096))
            .expect("tenant a/b segment should write");
        let key = ObjectKey::new("object");
        assert!(
            tenant_ab
                .compare_and_swap_object_route(
                    &key,
                    None,
                    Some(&sample_route("object", "writer", "seg"))
                )
                .unwrap()
                .applied
        );

        assert!(tenant_a.list_live_clients().unwrap().is_empty());
        assert!(tenant_a.list_segments(None).unwrap().is_empty());
        assert!(tenant_a.list_object_routes().unwrap().is_empty());
        assert!(tenant_a.get_client_lease(&lease.runtime).unwrap().is_none());
        assert!(tenant_a
            .get_segment(&lease.runtime, &SegmentName::new("seg"))
            .unwrap()
            .is_none());
        assert!(tenant_a.get_object_route(&key).unwrap().is_none());
    }

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
                expires_at_ms: now_ms() + 10_000,
            })
            .expect("lease should upsert");
        metadata
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 1024,
                used_bytes: 0,
                target_chunks: Vec::new(),
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
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 32,
                used_bytes: 0,
                target_chunks: Vec::new(),
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
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 256,
                used_bytes: 0,
                target_chunks: Vec::new(),
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
        let missing_scope =
            TenantPolicyScope::new("tenant-missing", None::<String>, None::<String>);
        assert_eq!(
            metadata
                .get_tenant_policies(&[scope.clone(), missing_scope.clone()])
                .expect("tenant policy batch read should succeed"),
            vec![Some(policy.clone()), None]
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
                .list_tenant_policies(None)
                .expect("tenant policy listing should succeed"),
            vec![updated.clone()]
        );
        assert_eq!(
            metadata
                .list_tenant_policies(Some("tenant-a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![updated.clone()]
        );
        assert!(metadata
            .list_tenant_policies(Some("tenant-b"))
            .expect("missing tenant policy listing should succeed")
            .is_empty());
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
        assert!(matches!(
            byte_limit,
            StoreError::QuotaExceeded {
                kind: QuotaKind::Bytes,
                ..
            }
        ));

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
        assert!(matches!(
            object_limit,
            StoreError::QuotaExceeded {
                kind: QuotaKind::Objects,
                ..
            }
        ));

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

    fn make_lease(stable_id: &str, epoch: u64) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(epoch)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: now_ms() + 10_000,
        }
    }

    #[test]
    fn upsert_client_lease_rejects_non_monotonic_epoch() {
        let metadata = InMemoryMetadataBackend::new();
        metadata
            .upsert_client_lease(&make_lease("client-a", 5))
            .expect("first lease should publish");

        let stale = metadata
            .upsert_client_lease(&make_lease("client-a", 3))
            .expect_err("older epoch should be rejected");
        assert!(matches!(stale, StoreError::StaleEpoch(_)));

        metadata
            .upsert_client_lease(&make_lease("client-a", 5))
            .expect("same (stable_id, epoch) is a refresh and must succeed");

        metadata
            .upsert_client_lease(&make_lease("client-a", 6))
            .expect("strictly greater epoch should succeed");
    }

    #[test]
    fn upsert_client_lease_reclaims_expired_same_epoch() {
        let metadata = InMemoryMetadataBackend::new();
        metadata
            .upsert_client_lease(&make_lease("client-a", 5))
            .expect("first lease should publish");

        {
            let mut state = metadata.state.write();
            state
                .clients
                .remove(&ClientRuntimeId::new("client-a", ClientEpoch(5)).storage_key());
        }

        metadata
            .upsert_client_lease(&make_lease("client-a", 5))
            .expect("same epoch should reclaim after the exact lease key disappears");
    }

    #[test]
    fn upsert_client_lease_isolates_stable_ids() {
        let metadata = InMemoryMetadataBackend::new();
        metadata
            .upsert_client_lease(&make_lease("client-a", 9))
            .expect("client-a lease should publish");
        metadata
            .upsert_client_lease(&make_lease("client-b", 1))
            .expect("distinct stable_id keeps its own monotonicity");
    }

    #[test]
    fn allocate_client_lease_returns_monotonic_epochs() {
        let metadata = InMemoryMetadataBackend::new();
        let first = metadata
            .allocate_client_lease(&make_lease("client-alloc", 0))
            .expect("first allocation should succeed");
        let second = metadata
            .allocate_client_lease(&make_lease("client-alloc", 999))
            .expect("second allocation ignores template epoch");
        let third = metadata
            .allocate_client_lease(&make_lease("client-alloc", 0))
            .expect("third allocation should succeed");
        assert_eq!(first.epoch, ClientEpoch(1));
        assert_eq!(second.epoch, ClientEpoch(2));
        assert_eq!(third.epoch, ClientEpoch(3));
    }

    #[test]
    fn allocate_client_lease_respects_hwm_after_upsert() {
        let metadata = InMemoryMetadataBackend::new();
        metadata
            .upsert_client_lease(&make_lease("client-mixed", 7))
            .expect("explicit lease at epoch 7 should publish");
        let assigned = metadata
            .allocate_client_lease(&make_lease("client-mixed", 0))
            .expect("allocate should climb above prior explicit epoch");
        assert_eq!(assigned.epoch, ClientEpoch(8));
    }

    #[test]
    fn allocate_client_lease_isolates_stable_ids() {
        let metadata = InMemoryMetadataBackend::new();
        let a = metadata
            .allocate_client_lease(&make_lease("stable-a", 0))
            .expect("first stable_id allocation");
        let b = metadata
            .allocate_client_lease(&make_lease("stable-b", 0))
            .expect("second stable_id allocation");
        assert_eq!(a.epoch, ClientEpoch(1));
        assert_eq!(b.epoch, ClientEpoch(1));
    }

    #[test]
    fn upsert_client_lease_hwm_persists_across_concurrent_epochs() {
        let metadata = InMemoryMetadataBackend::new();
        metadata
            .upsert_client_lease(&make_lease("client-a", 2))
            .expect("publish epoch 2");
        metadata
            .upsert_client_lease(&make_lease("client-a", 3))
            .expect("publish epoch 3 (handoff window)");

        metadata
            .upsert_client_lease(&make_lease("client-a", 3))
            .expect("epoch 3 refresh under handoff must succeed");

        let rejected = metadata
            .upsert_client_lease(&make_lease("client-a", 1))
            .expect_err("epoch behind active set must be rejected");
        assert!(matches!(rejected, StoreError::StaleEpoch(_)));
    }

    // -----------------------------------------------------------------------
    // Adversarial: concurrency + idempotency + error paths
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_lease_upserts_for_distinct_stable_ids_all_land() {
        use std::sync::Arc;
        use std::thread;

        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let mut handles = Vec::new();
        for i in 0..20 {
            let meta = metadata.clone();
            handles.push(thread::spawn(move || {
                meta.upsert_client_lease(&active_lease(&format!("concurrent-{i}"), 1))
                    .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let clients = metadata.list_live_clients().unwrap();
        assert_eq!(clients.len(), 20);
    }

    #[test]
    fn concurrent_cas_insert_on_same_key_exactly_one_winner() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::thread;

        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let key = ObjectKey::new("cas-race-key");
        let wins = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..10 {
            let meta = metadata.clone();
            let k = key.clone();
            let w = wins.clone();
            handles.push(thread::spawn(move || {
                let owner_id = format!("racer-{i}");
                let route = sample_route("cas-race-key", &owner_id, "seg");
                let result = meta
                    .compare_and_swap_object_route(&k, None, Some(&route))
                    .unwrap();
                if result.applied {
                    w.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            wins.load(Ordering::Relaxed),
            1,
            "exactly one CAS insert should apply"
        );
    }

    #[test]
    fn concurrent_segment_reserves_are_serialized_without_oversubscribing() {
        use std::sync::Arc;
        use std::thread;

        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let owner = ClientRuntimeId::new("reserve-node", ClientEpoch(1));
        metadata
            .upsert_client_lease(&active_lease("reserve-node", 1))
            .unwrap();
        metadata
            .publish_segment(&sample_segment(&owner, "seg", 640))
            .unwrap();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let md = Arc::clone(&metadata);
            let o = owner.clone();
            handles.push(thread::spawn(move || {
                md.reserve_segment(&o, &SegmentName::new("seg"), 64)
            }));
        }
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let ok = results.iter().filter(|r| r.is_ok()).count();
        assert_eq!(
            ok, 10,
            "capacity is exactly 10x64 — all reserves must succeed"
        );

        let no_room = metadata
            .reserve_segment(&owner, &SegmentName::new("seg"), 1)
            .expect_err("segment is exhausted");
        assert!(matches!(no_room, StoreError::Allocator(_)));
    }

    #[test]
    fn publish_segment_is_idempotent() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node", ClientEpoch(1));
        let ann = sample_segment(&owner, "seg", 1024);
        metadata.publish_segment(&ann).unwrap();
        metadata.publish_segment(&ann).unwrap();
        assert_eq!(metadata.list_segments(Some(&owner)).unwrap().len(), 1);
    }

    #[test]
    fn publish_segment_merges_later_announcement_into_existing_state() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("merge-owner", ClientEpoch(1));
        metadata
            .publish_segment(&sample_segment(&owner, "merge-seg", 1024))
            .unwrap();

        let mut updated = sample_segment(&owner, "merge-seg", 2048);
        updated.state = SegmentLifecycleState::Draining;
        metadata.publish_segment(&updated).unwrap();

        let segments = metadata.list_segments(Some(&owner)).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].capacity_bytes, 2048);
        assert_eq!(segments[0].state, SegmentLifecycleState::Draining);
    }

    #[test]
    fn unpublish_segment_is_idempotent() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node", ClientEpoch(1));
        let seg = SegmentName::new("ephemeral");
        metadata
            .publish_segment(&sample_segment(&owner, "ephemeral", 1024))
            .unwrap();
        metadata.unpublish_segment(&owner, &seg).unwrap();
        metadata
            .unpublish_segment(&owner, &seg)
            .expect("second unpublish must be silent");
    }

    #[test]
    fn unpublish_nonexistent_segment_is_silent() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node", ClientEpoch(1));
        metadata
            .unpublish_segment(&owner, &SegmentName::new("nonexistent"))
            .expect("unpublishing unknown segment is a no-op");
    }

    #[test]
    fn reserve_on_unpublished_segment_returns_not_found() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node", ClientEpoch(1));
        let err = metadata
            .reserve_segment(&owner, &SegmentName::new("missing"), 64)
            .expect_err("reserve on missing segment must fail");
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn reserve_on_retired_segment_is_rejected() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("node", ClientEpoch(1));
        let mut ann = sample_segment(&owner, "retired-seg", 1024);
        ann.state = SegmentLifecycleState::Retired;
        metadata.publish_segment(&ann).unwrap();
        let err = metadata
            .reserve_segment(&owner, &SegmentName::new("retired-seg"), 64)
            .expect_err("retired segment must reject");
        assert!(matches!(
            err,
            StoreError::InvalidState(_) | StoreError::NotFound(_)
        ));
    }

    #[test]
    fn update_client_state_on_missing_client_returns_not_found() {
        let metadata = InMemoryMetadataBackend::new();
        let runtime = ClientRuntimeId::new("phantom", ClientEpoch(1));
        let err = metadata
            .update_client_state(&runtime, ClientLifecycleState::Draining)
            .expect_err("phantom lease has no state to update");
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn cas_with_stale_expected_version_conflicts_without_mutating() {
        let metadata = InMemoryMetadataBackend::new();
        let key = ObjectKey::new("stale-cas");
        let route = sample_route("stale-cas", "owner", "seg");
        metadata
            .compare_and_swap_object_route(&key, None, Some(&route))
            .unwrap();

        let mut updated = route.clone();
        updated.version = RouteVersion(2);
        let result = metadata
            .compare_and_swap_object_route(&key, Some(RouteVersion(999)), Some(&updated))
            .unwrap();
        assert!(!result.applied);
        // Current stored version must still be 1
        let current = metadata.get_object_route(&key).unwrap().unwrap();
        assert_eq!(current.version, RouteVersion(1));
    }

    #[test]
    fn cas_delete_on_nonexistent_with_expected_none_succeeds() {
        let metadata = InMemoryMetadataBackend::new();
        let key = ObjectKey::new("ghost");
        let result = metadata
            .compare_and_swap_object_route(&key, None, None)
            .unwrap();
        assert!(
            result.applied,
            "expected=None with next=None on absent key is a no-op apply"
        );
    }

    #[test]
    fn multiple_segments_for_same_owner_are_all_listed() {
        let metadata = InMemoryMetadataBackend::new();
        let owner = ClientRuntimeId::new("multi-owner", ClientEpoch(1));
        for i in 0..5 {
            metadata
                .publish_segment(&sample_segment(&owner, &format!("seg-{i}"), 1024))
                .unwrap();
        }
        assert_eq!(metadata.list_segments(Some(&owner)).unwrap().len(), 5);
    }

    #[test]
    fn unicode_object_key_round_trips_through_cas_and_get() {
        let metadata = InMemoryMetadataBackend::new();
        let key = ObjectKey::new("α-tenant/β-set/layer-42");
        let route = sample_route("α-tenant/β-set/layer-42", "owner-1", "seg-1");
        let result = metadata
            .compare_and_swap_object_route(&key, None, Some(&route))
            .unwrap();
        assert!(result.applied);
        let found = metadata.get_object_route(&key).unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn get_object_route_returns_none_for_missing_key() {
        let metadata = InMemoryMetadataBackend::new();
        let got = metadata
            .get_object_route(&ObjectKey::new("never-stored"))
            .unwrap();
        assert!(got.is_none());
    }
}
