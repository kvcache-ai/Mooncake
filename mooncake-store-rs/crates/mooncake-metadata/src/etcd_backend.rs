use std::time::{SystemTime, UNIX_EPOCH};

use crate::keyspace::{parse_route_policy_domain, parse_tenant_policy_scope};
use crate::segment_state::StoredSegmentState;
use crate::MetadataKeyspace;
use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy, RoutePolicyDomain, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
    TenantObjectAccounting, TenantObjectAccountingState, TenantPolicy, TenantPolicyScope,
    TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome, TenantQuotaFinalizeRequest,
    TenantQuotaReservation, TenantQuotaReservationOutcome, TenantQuotaReservationRequest,
    TenantQuotaReservationState, TenantQuotaState,
};

#[derive(Clone, Debug)]
pub struct EtcdMetadataConfig {
    pub endpoints: Vec<String>,
    pub keyspace: MetadataKeyspace,
}

impl EtcdMetadataConfig {
    pub fn new(endpoints: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            endpoints: endpoints.into_iter().map(Into::into).collect(),
            keyspace: MetadataKeyspace::default(),
        }
    }

    pub fn localhost() -> Self {
        Self::new(["http://127.0.0.1:2379"])
    }

    pub fn keyspace(mut self, keyspace: MetadataKeyspace) -> Self {
        self.keyspace = keyspace;
        self
    }
}

pub struct EtcdMetadataBackend {
    runtime: tokio::runtime::Runtime,
    config: EtcdMetadataConfig,
}

impl Default for EtcdMetadataBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl EtcdMetadataBackend {
    pub fn new() -> Self {
        Self::from_config(EtcdMetadataConfig::localhost())
            .expect("localhost etcd metadata backend should construct")
    }

    pub fn from_config(config: EtcdMetadataConfig) -> Result<Self> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| StoreError::Metadata(format!("tokio runtime build: {error}")))?;
        Ok(Self { runtime, config })
    }

    fn block_on<T>(&self, work: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        self.runtime.block_on(work)
    }

    async fn client(&self) -> Result<Client> {
        Client::connect(self.config.endpoints.clone(), None)
            .await
            .map_err(etcd_error("etcd connect"))
    }
}

impl MetadataBackend for EtcdMetadataBackend {
    fn route_namespace(&self) -> String {
        format!(
            "etcd://{}#{}",
            self.config.endpoints.join(","),
            self.config.keyspace.prefix()
        )
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        let key = self.config.keyspace.client(&lease.runtime);
        let payload = serde_json::to_string(lease).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
            client
                .put(key, payload, None)
                .await
                .map_err(etcd_error("etcd put client lease"))?;
            Ok(())
        })
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        let key = self.config.keyspace.client(runtime);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key.clone(), None)
                .await
                .map_err(etcd_error("etcd get client lease"))?;
            let kv = response
                .kvs()
                .first()
                .ok_or_else(|| StoreError::NotFound(key.clone()))?;
            let mut lease: ClientLease = serde_json::from_slice(kv.value()).map_err(json_error)?;
            lease.state = next;
            let payload = serde_json::to_string(&lease).map_err(json_error)?;
            client
                .put(key, payload, None)
                .await
                .map_err(etcd_error("etcd update client lease"))?;
            Ok(())
        })
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        let prefix = self
            .config
            .keyspace
            .client_pattern()
            .trim_end_matches('*')
            .to_string();
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list client leases"))?;
            let now = now_ms();
            response
                .kvs()
                .iter()
                .map(|kv| serde_json::from_slice::<ClientLease>(kv.value()).map_err(json_error))
                .filter_map(|lease| match lease {
                    Ok(lease) if lease.expires_at_ms >= now => Some(Ok(lease)),
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect()
        })
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        let key = self
            .config
            .keyspace
            .segment(&segment.owner, &segment.segment_name);
        self.block_on(async {
            let mut client = self.client().await?;
            let current = client
                .get(key.clone(), None)
                .await
                .map_err(etcd_error("etcd get segment before publish"))?;
            let mut state = current
                .kvs()
                .first()
                .map(|kv| {
                    serde_json::from_slice::<StoredSegmentState>(kv.value()).map_err(json_error)
                })
                .transpose()?
                .unwrap_or_else(|| StoredSegmentState::new(segment.clone()));
            state.merge_announcement(segment);
            let payload = serde_json::to_string(&state).map_err(json_error)?;
            client
                .put(key, payload, None)
                .await
                .map_err(etcd_error("etcd publish segment"))?;
            Ok(())
        })
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        let key = self.config.keyspace.segment(owner, segment);
        self.block_on(async {
            let mut client = self.client().await?;
            client
                .delete(key, None)
                .await
                .map_err(etcd_error("etcd delete segment"))?;
            Ok(())
        })
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        let prefix = self.config.keyspace.segment_prefix(owner);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list segments"))?;
            response
                .kvs()
                .iter()
                .map(|kv| {
                    serde_json::from_slice::<StoredSegmentState>(kv.value())
                        .map(|state| state.announcement)
                        .map_err(json_error)
                })
                .collect()
        })
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        let key = self.config.keyspace.segment(owner, segment);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get segment"))?;
                let kv = response
                    .kvs()
                    .first()
                    .ok_or_else(|| StoreError::NotFound(key.clone()))?;
                let mut state: StoredSegmentState =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                state.announcement.state = next;
                let payload = serde_json::to_string(&state).map_err(json_error)?;
                let txn = Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then([TxnOp::put(key.clone(), payload, None)]);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd update segment state"))?;
                if txn_response.succeeded() {
                    return Ok(());
                }
            }
        })
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let key = self.config.keyspace.segment(owner, segment);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get segment"))?;
                let kv = response
                    .kvs()
                    .first()
                    .ok_or_else(|| StoreError::NotFound(key.clone()))?;
                let mut state: StoredSegmentState =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                let reservation = state.reserve(owner, segment, length_bytes)?;
                let payload = serde_json::to_string(&state).map_err(json_error)?;
                let txn = Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then([TxnOp::put(key.clone(), payload, None)]);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd reserve segment"))?;
                if txn_response.succeeded() {
                    return Ok(reservation);
                }
            }
        })
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let key = self.config.keyspace.segment(owner, segment);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get segment"))?;
                let kv = response
                    .kvs()
                    .first()
                    .ok_or_else(|| StoreError::NotFound(key.clone()))?;
                let mut state: StoredSegmentState =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                state.release(owner, segment, offset_bytes, length_bytes)?;
                let payload = serde_json::to_string(&state).map_err(json_error)?;
                let txn = Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then([TxnOp::put(key.clone(), payload, None)]);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd release segment"))?;
                if txn_response.succeeded() {
                    return Ok(());
                }
            }
        })
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        let key = self.config.keyspace.object(key);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get object route"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        let prefix = self.config.keyspace.object_prefix();
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list object routes"))?;
            let mut routes = Vec::with_capacity(response.kvs().len());
            for kv in response.kvs() {
                routes.push(serde_json::from_slice(kv.value()).map_err(json_error)?);
            }
            Ok(routes)
        })
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let key = self.config.keyspace.object(key);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get object route"))?;
                let current = response
                    .kvs()
                    .first()
                    .map(|kv| serde_json::from_slice::<ObjectRoute>(kv.value()).map_err(json_error))
                    .transpose()?;
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

                let txn = if let Some(kv) = response.kvs().first() {
                    let compare =
                        Compare::mod_revision(key.clone(), CompareOp::Equal, kv.mod_revision());
                    let op = match next {
                        Some(route) => TxnOp::put(
                            key.clone(),
                            serde_json::to_string(route).map_err(json_error)?,
                            None,
                        ),
                        None => TxnOp::delete(key.clone(), None),
                    };
                    Txn::new().when([compare]).and_then([op])
                } else {
                    if next.is_none() {
                        return Ok(CasResult {
                            applied: true,
                            current: None,
                        });
                    }
                    Txn::new()
                        .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
                        .and_then([TxnOp::put(
                            key.clone(),
                            serde_json::to_string(next.expect("checked above"))
                                .map_err(json_error)?,
                            None,
                        )])
                };
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd cas object route"))?;
                if txn_response.succeeded() {
                    return Ok(CasResult {
                        applied: true,
                        current: next.cloned(),
                    });
                }
            }
        })
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        let key = self.config.keyspace.route_policy(domain);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get route policy"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        let key = self.config.keyspace.route_policy(domain);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .txn(
                    Txn::new()
                        .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
                        .and_then([TxnOp::put(key, payload, None)]),
                )
                .await
                .map_err(etcd_error("etcd put route policy if absent"))?;
            Ok(response.succeeded())
        })
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        let key = self.config.keyspace.route_policy(domain);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
            client
                .put(key, payload, None)
                .await
                .map_err(etcd_error("etcd put route policy"))?;
            Ok(())
        })
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        let key = self.config.keyspace.route_policy(domain);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .delete(key, None)
                .await
                .map_err(etcd_error("etcd delete route policy"))?;
            Ok(response.deleted() != 0)
        })
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        let prefix = self.config.keyspace.route_policy_prefix();
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix.clone(), Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list route policies"))?;
            let mut policies = response
                .kvs()
                .iter()
                .filter_map(|kv| {
                    let key = std::str::from_utf8(kv.key()).ok()?;
                    let domain = parse_route_policy_domain(&self.config.keyspace, key)?;
                    let policy = serde_json::from_slice(kv.value()).ok()?;
                    Some((domain, policy))
                })
                .collect::<Vec<_>>();
            policies.sort_by(|left, right| left.0.cmp(&right.0));
            Ok(policies)
        })
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        let key = self.config.keyspace.tenant_policy(scope);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get tenant policy"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    fn list_tenant_policies(&self) -> Result<Vec<TenantPolicy>> {
        let prefix = self.config.keyspace.tenant_policy_prefix(None);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list tenant policies"))?;
            let mut policies = response
                .kvs()
                .iter()
                .filter_map(|kv| {
                    let key = std::str::from_utf8(kv.key()).ok()?;
                    parse_tenant_policy_scope(&self.config.keyspace, key)?;
                    let policy: TenantPolicy = serde_json::from_slice(kv.value()).ok()?;
                    Some(policy)
                })
                .collect::<Vec<_>>();
            policies.sort_by(|left, right| left.scope.cmp(&right.scope));
            Ok(policies)
        })
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        policy.validate()?;
        let key = self.config.keyspace.tenant_policy(&policy.scope);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant policy"))?;
                let current = response
                    .kvs()
                    .first()
                    .map(|kv| {
                        serde_json::from_slice::<TenantPolicy>(kv.value()).map_err(json_error)
                    })
                    .transpose()?;
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
                let txn = if let Some(kv) = response.kvs().first() {
                    Txn::new()
                        .when([Compare::mod_revision(
                            key.clone(),
                            CompareOp::Equal,
                            kv.mod_revision(),
                        )])
                        .and_then([TxnOp::put(key.clone(), payload.clone(), None)])
                } else {
                    Txn::new()
                        .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
                        .and_then([TxnOp::put(key.clone(), payload.clone(), None)])
                };
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd put tenant policy"))?;
                if txn_response.succeeded() {
                    return Ok(policy.clone());
                }
            }
        })
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        let key = self.config.keyspace.tenant_policy(scope);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant policy"))?;
                let current = response
                    .kvs()
                    .first()
                    .map(|kv| {
                        serde_json::from_slice::<TenantPolicy>(kv.value()).map_err(json_error)
                    })
                    .transpose()?;
                let Some(current) = current else {
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
                let kv = response
                    .kvs()
                    .first()
                    .expect("tenant policy current kv should exist");
                let txn = Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then([TxnOp::delete(key.clone(), None)]);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd delete tenant policy"))?;
                if txn_response.succeeded() {
                    return Ok(true);
                }
            }
        })
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        let scope = root_scope(scope)?;
        let key = self.config.keyspace.tenant_quota_state(&scope);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get tenant quota state"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        let key = self.config.keyspace.tenant_object_accounting(key);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get tenant object accounting"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        let scope = root_scope(scope)?;
        let prefix = self
            .config
            .keyspace
            .tenant_quota_reservation_prefix(Some(&scope.tenant));
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list tenant quota reservation indexes"))?;
            let mut reservations = Vec::new();
            for kv in response.kvs() {
                let reservation_key = String::from_utf8(kv.value().to_vec()).map_err(|error| {
                    StoreError::Metadata(format!(
                        "etcd list tenant quota reservation indexes: invalid utf8 value: {error}"
                    ))
                })?;
                let reservation_response = client
                    .get(reservation_key, None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota reservation"))?;
                let Some(kv) = reservation_response.kvs().first() else {
                    continue;
                };
                let reservation: TenantQuotaReservation =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                if reservation.scope == scope {
                    reservations.push(reservation);
                }
            }
            reservations.sort_by(|left, right| left.reservation_id.cmp(&right.reservation_id));
            Ok(reservations)
        })
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        request.validate()?;
        let scope = root_scope(&request.scope)?;
        let mut normalized = request.clone();
        normalized.scope = scope.clone();
        let quota_key = self.config.keyspace.tenant_quota_state(&scope);
        let object_key = self
            .config
            .keyspace
            .tenant_object_accounting(&normalized.key);
        let reservation_key = self
            .config
            .keyspace
            .tenant_quota_reservation(&normalized.reservation_id);
        let reservation_index_key = self
            .config
            .keyspace
            .tenant_quota_reservation_index(&scope, &normalized.reservation_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let reservation_response = client
                    .get(reservation_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota reservation"))?;
                if let Some(kv) = reservation_response.kvs().first() {
                    let existing: TenantQuotaReservation =
                        serde_json::from_slice(kv.value()).map_err(json_error)?;
                    return match existing.state {
                        TenantQuotaReservationState::Pending
                            if existing.scope == scope
                                && existing.key == normalized.key
                                && existing.expected_object_version
                                    == normalized.expected_object_version
                                && existing.delta_bytes == normalized.delta_bytes
                                && existing.delta_objects == normalized.delta_objects =>
                        {
                            let quota_response = client
                                .get(quota_key.clone(), None)
                                .await
                                .map_err(etcd_error("etcd get tenant quota state"))?;
                            let quota = quota_response
                                .kvs()
                                .first()
                                .ok_or_else(|| {
                                    StoreError::InvalidState(format!(
                                        "tenant quota state missing for {} while reservation {} exists",
                                        scope.tenant, normalized.reservation_id
                                    ))
                                })
                                .and_then(|kv| {
                                    serde_json::from_slice::<TenantQuotaState>(kv.value())
                                        .map_err(json_error)
                                })?;
                            let object = client
                                .get(object_key.clone(), None)
                                .await
                                .map_err(etcd_error("etcd get tenant object accounting"))?
                                .kvs()
                                .first()
                                .map(|kv| {
                                    serde_json::from_slice::<TenantObjectAccounting>(kv.value())
                                        .map_err(json_error)
                                })
                                .transpose()?;
                            Ok(TenantQuotaReservationOutcome {
                                quota,
                                object,
                                reservation: existing,
                            })
                        }
                        TenantQuotaReservationState::Pending => Err(StoreError::Conflict(format!(
                            "tenant quota reservation {} already exists with different parameters",
                            normalized.reservation_id
                        ))),
                        _ => Err(StoreError::Conflict(format!(
                            "tenant quota reservation {} is already {:?}",
                            normalized.reservation_id, existing.state
                        ))),
                    };
                }

                let object_response = client
                    .get(object_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant object accounting"))?;
                let current_object = object_response
                    .kvs()
                    .first()
                    .map(|kv| {
                        serde_json::from_slice::<TenantObjectAccounting>(kv.value()).map_err(json_error)
                    })
                    .transpose()?;
                let actual_object_version = current_object.as_ref().map(|object| object.version);
                if actual_object_version != normalized.expected_object_version {
                    return Err(version_conflict(
                        "tenant object accounting",
                        &scope,
                        normalized.expected_object_version,
                        actual_object_version,
                    ));
                }
                if let Some(object) = current_object.as_ref() {
                    object.validate()?;
                    if object.scope != scope {
                        return Err(StoreError::InvalidState(format!(
                            "tenant object accounting scope mismatch for key {}",
                            normalized.key.0
                        )));
                    }
                }

                let quota_response = client
                    .get(quota_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota state"))?;
                let current_quota = quota_response
                    .kvs()
                    .first()
                    .map(|kv| serde_json::from_slice::<TenantQuotaState>(kv.value()).map_err(json_error))
                    .transpose()?;
                let mut quota = current_quota.clone().unwrap_or_else(|| TenantQuotaState {
                    scope: scope.clone(),
                    version: 0,
                    used_bytes: 0,
                    used_objects: 0,
                    pending_reserved_bytes: 0,
                    pending_reserved_objects: 0,
                    updated_at_ms: normalized.created_at_ms,
                    updated_by: normalized.writer_runtime.to_string(),
                });
                quota.validate()?;

                let positive_bytes =
                    non_negative_i64_to_u64(normalized.delta_bytes.max(0), "delta_bytes")?;
                let positive_objects =
                    non_negative_i64_to_u64(normalized.delta_objects.max(0), "delta_objects")?;
                if let Some(limit) = normalized.limit.max_bytes {
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
                if let Some(limit) = normalized.limit.max_objects {
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
                    normalized.delta_bytes,
                    "pending_reserved_bytes",
                )?;
                quota.pending_reserved_objects = apply_positive_delta(
                    quota.pending_reserved_objects,
                    normalized.delta_objects,
                    "pending_reserved_objects",
                )?;
                quota.version = quota.version.saturating_add(1);
                quota.updated_at_ms = normalized.created_at_ms;
                quota.updated_by = normalized.writer_runtime.to_string();
                let reservation = TenantQuotaReservation {
                    reservation_id: normalized.reservation_id.clone(),
                    scope: scope.clone(),
                    key: normalized.key.clone(),
                    version: 1,
                    expected_object_version: normalized.expected_object_version,
                    delta_bytes: normalized.delta_bytes,
                    delta_objects: normalized.delta_objects,
                    state: TenantQuotaReservationState::Pending,
                    expires_at_ms: normalized.expires_at_ms,
                    created_at_ms: normalized.created_at_ms,
                    writer_runtime: normalized.writer_runtime.clone(),
                };
                reservation.validate()?;

                let quota_payload = serde_json::to_string(&quota).map_err(json_error)?;
                let reservation_payload = serde_json::to_string(&reservation).map_err(json_error)?;
                let txn = Txn::new()
                    .when([
                        if let Some(kv) = quota_response.kvs().first() {
                            Compare::mod_revision(
                                quota_key.clone(),
                                CompareOp::Equal,
                                kv.mod_revision(),
                            )
                        } else {
                            Compare::version(quota_key.clone(), CompareOp::Equal, 0)
                        },
                        if let Some(kv) = object_response.kvs().first() {
                            Compare::mod_revision(
                                object_key.clone(),
                                CompareOp::Equal,
                                kv.mod_revision(),
                            )
                        } else {
                            Compare::version(object_key.clone(), CompareOp::Equal, 0)
                        },
                        Compare::version(reservation_key.clone(), CompareOp::Equal, 0),
                    ])
                    .and_then([
                        TxnOp::put(quota_key.clone(), quota_payload, None),
                        TxnOp::put(reservation_key.clone(), reservation_payload, None),
                        TxnOp::put(reservation_index_key.clone(), reservation_key.clone(), None),
                    ]);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd reserve tenant quota"))?;
                if txn_response.succeeded() {
                    return Ok(TenantQuotaReservationOutcome {
                        quota,
                        object: current_object,
                        reservation,
                    });
                }
            }
        })
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        request.validate()?;
        let reservation_key = self
            .config
            .keyspace
            .tenant_quota_reservation(&request.reservation_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let reservation_response = client
                    .get(reservation_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota reservation"))?;
                let reservation_kv = reservation_response
                    .kvs()
                    .first()
                    .ok_or_else(|| StoreError::NotFound(request.reservation_id.clone()))?;
                let reservation: TenantQuotaReservation =
                    serde_json::from_slice(reservation_kv.value()).map_err(json_error)?;
                match reservation.state {
                    TenantQuotaReservationState::Finalized => {
                        let scope = root_scope(&reservation.scope)?;
                        let quota_key = self.config.keyspace.tenant_quota_state(&scope);
                        let quota_response = client
                            .get(quota_key, None)
                            .await
                            .map_err(etcd_error("etcd get tenant quota state"))?;
                        let quota = quota_response
                            .kvs()
                            .first()
                            .ok_or_else(|| {
                                StoreError::InvalidState(format!(
                                    "tenant quota state missing for {} while reservation {} is finalized",
                                    reservation.scope.tenant, reservation.reservation_id
                                ))
                            })
                            .and_then(|kv| {
                                serde_json::from_slice::<TenantQuotaState>(kv.value())
                                    .map_err(json_error)
                            })?;
                        let object_key = self
                            .config
                            .keyspace
                            .tenant_object_accounting(&reservation.key);
                        let object = client
                            .get(object_key, None)
                            .await
                            .map_err(etcd_error("etcd get tenant object accounting"))?
                            .kvs()
                            .first()
                            .map(|kv| {
                                serde_json::from_slice::<TenantObjectAccounting>(kv.value())
                                    .map_err(json_error)
                            })
                            .transpose()?;
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

                let scope = root_scope(&reservation.scope)?;
                let quota_key = self.config.keyspace.tenant_quota_state(&scope);
                let object_key = self.config.keyspace.tenant_object_accounting(&reservation.key);
                let object_response = client
                    .get(object_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant object accounting"))?;
                let current_object = object_response
                    .kvs()
                    .first()
                    .map(|kv| {
                        serde_json::from_slice::<TenantObjectAccounting>(kv.value()).map_err(json_error)
                    })
                    .transpose()?;
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

                let quota_response = client
                    .get(quota_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota state"))?;
                let quota_kv = quota_response.kvs().first().ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "tenant quota state missing for {} while finalizing reservation {}",
                        scope.tenant, reservation.reservation_id
                    ))
                })?;
                let mut quota: TenantQuotaState =
                    serde_json::from_slice(quota_kv.value()).map_err(json_error)?;
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
                if let Some(object) = next_object.as_ref() {
                    object.validate()?;
                }
                let mut updated_reservation = reservation.clone();
                updated_reservation.state = TenantQuotaReservationState::Finalized;
                updated_reservation.version = updated_reservation.version.saturating_add(1);

                let quota_payload = serde_json::to_string(&quota).map_err(json_error)?;
                let reservation_payload =
                    serde_json::to_string(&updated_reservation).map_err(json_error)?;
                let mut txn = Txn::new().when([
                    Compare::mod_revision(
                        reservation_key.clone(),
                        CompareOp::Equal,
                        reservation_kv.mod_revision(),
                    ),
                    Compare::mod_revision(
                        quota_key.clone(),
                        CompareOp::Equal,
                        quota_kv.mod_revision(),
                    ),
                    if let Some(kv) = object_response.kvs().first() {
                        Compare::mod_revision(
                            object_key.clone(),
                            CompareOp::Equal,
                            kv.mod_revision(),
                        )
                    } else {
                        Compare::version(object_key.clone(), CompareOp::Equal, 0)
                    },
                ]);
                let mut ops = vec![
                    TxnOp::put(quota_key.clone(), quota_payload, None),
                    TxnOp::put(reservation_key.clone(), reservation_payload, None),
                ];
                match next_object.as_ref() {
                    Some(object) => ops.push(TxnOp::put(
                        object_key.clone(),
                        serde_json::to_string(object).map_err(json_error)?,
                        None,
                    )),
                    None => ops.push(TxnOp::delete(object_key.clone(), None)),
                }
                txn = txn.and_then(ops);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd finalize tenant quota"))?;
                if txn_response.succeeded() {
                    return Ok(TenantQuotaFinalizeOutcome {
                        quota,
                        object: next_object,
                        reservation: updated_reservation,
                    });
                }
            }
        })
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        if reservation_id.is_empty() {
            return Err(StoreError::InvalidState(
                "tenant quota abort reservation_id must not be empty".to_string(),
            ));
        }
        let reservation_key = self
            .config
            .keyspace
            .tenant_quota_reservation(reservation_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let reservation_response = client
                    .get(reservation_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota reservation"))?;
                let reservation_kv = reservation_response
                    .kvs()
                    .first()
                    .ok_or_else(|| StoreError::NotFound(reservation_id.to_string()))?;
                let reservation: TenantQuotaReservation =
                    serde_json::from_slice(reservation_kv.value()).map_err(json_error)?;
                match reservation.state {
                    TenantQuotaReservationState::Aborted => {
                        let scope = root_scope(&reservation.scope)?;
                        let quota_key = self.config.keyspace.tenant_quota_state(&scope);
                        let quota_response = client
                            .get(quota_key, None)
                            .await
                            .map_err(etcd_error("etcd get tenant quota state"))?;
                        let quota = quota_response
                            .kvs()
                            .first()
                            .ok_or_else(|| {
                                StoreError::InvalidState(format!(
                                    "tenant quota state missing for {} while reservation {} is aborted",
                                    reservation.scope.tenant, reservation.reservation_id
                                ))
                            })
                            .and_then(|kv| {
                                serde_json::from_slice::<TenantQuotaState>(kv.value())
                                    .map_err(json_error)
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

                let scope = root_scope(&reservation.scope)?;
                let quota_key = self.config.keyspace.tenant_quota_state(&scope);
                let quota_response = client
                    .get(quota_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get tenant quota state"))?;
                let quota_kv = quota_response.kvs().first().ok_or_else(|| {
                    StoreError::InvalidState(format!(
                        "tenant quota state missing for {} while aborting reservation {}",
                        scope.tenant, reservation.reservation_id
                    ))
                })?;
                let mut quota: TenantQuotaState =
                    serde_json::from_slice(quota_kv.value()).map_err(json_error)?;
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
                let mut updated_reservation = reservation.clone();
                updated_reservation.state = TenantQuotaReservationState::Aborted;
                updated_reservation.version = updated_reservation.version.saturating_add(1);

                let txn = Txn::new()
                    .when([
                        Compare::mod_revision(
                            reservation_key.clone(),
                            CompareOp::Equal,
                            reservation_kv.mod_revision(),
                        ),
                        Compare::mod_revision(
                            quota_key.clone(),
                            CompareOp::Equal,
                            quota_kv.mod_revision(),
                        ),
                    ])
                    .and_then([
                        TxnOp::put(
                            quota_key.clone(),
                            serde_json::to_string(&quota).map_err(json_error)?,
                            None,
                        ),
                        TxnOp::put(
                            reservation_key.clone(),
                            serde_json::to_string(&updated_reservation).map_err(json_error)?,
                            None,
                        ),
                    ]);
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd abort tenant quota"))?;
                if txn_response.succeeded() {
                    return Ok(TenantQuotaAbortOutcome {
                        quota,
                        reservation: updated_reservation,
                    });
                }
            }
        })
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        let key = self.config.keyspace.handoff(&handoff.stable_id);
        let payload = serde_json::to_string(handoff).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
            client
                .put(key, payload, None)
                .await
                .map_err(etcd_error("etcd put handoff"))?;
            Ok(())
        })
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        let key = self.config.keyspace.handoff(stable_id);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get handoff"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn etcd_error(operation: &'static str) -> impl FnOnce(etcd_client::Error) -> StoreError {
    move |error| StoreError::Metadata(format!("{operation}: {error}"))
}

fn json_error(error: serde_json::Error) -> StoreError {
    StoreError::Metadata(format!("json serialization: {error}"))
}

fn root_scope(scope: &TenantPolicyScope) -> Result<TenantPolicyScope> {
    scope.validate_root_only("tenant quota metadata")?;
    Ok(TenantPolicyScope::new(
        scope.tenant.clone(),
        None::<String>,
        None::<String>,
    ))
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend,
        ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteState, RouteVersion,
        SegmentAnnouncement, SegmentLifecycleState, SegmentName, StoreError,
        TenantObjectAccountingState, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
        TenantQuotaFinalizeRequest, TenantQuotaPolicy, TenantQuotaReservationRequest,
        TenantQuotaReservationState,
    };

    use super::{EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace};

    struct EtcdTestServer {
        child: Child,
        endpoint: String,
        dir: PathBuf,
    }

    fn etcd_ready(endpoint: &str) -> bool {
        let Ok(runtime) = tokio::runtime::Runtime::new() else {
            return false;
        };
        runtime
            .block_on(etcd_client::Client::connect([endpoint.to_string()], None))
            .is_ok()
    }

    impl EtcdTestServer {
        fn start() -> Option<Self> {
            let client_listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let client_port = client_listener.local_addr().ok()?.port();
            drop(client_listener);

            let peer_listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let peer_port = peer_listener.local_addr().ok()?.port();
            drop(peer_listener);

            let dir = std::env::temp_dir().join(format!("mooncake-store-rs-etcd-{client_port}"));
            let _ = std::fs::create_dir_all(&dir);
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
                    return Some(Self {
                        child,
                        endpoint,
                        dir,
                    });
                }
                sleep(Duration::from_millis(25));
            }
            None
        }

        fn endpoint(&self) -> &str {
            &self.endpoint
        }
    }

    impl Drop for EtcdTestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn sample_runtime() -> ClientRuntimeId {
        ClientRuntimeId::new("writer", ClientEpoch(5))
    }

    fn sample_lease(state: ClientLifecycleState) -> ClientLease {
        ClientLease {
            runtime: sample_runtime(),
            state,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet {
                rpc_address: "127.0.0.1:7777".to_string(),
                segment_name: Some(SegmentName::new("segment-a")),
                labels: BTreeMap::from([("pool".to_string(), "local".to_string())]),
            },
            expires_at_ms: super::now_ms() + 60_000,
        }
    }

    fn sample_segment(state: SegmentLifecycleState) -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner: sample_runtime(),
            segment_name: SegmentName::new("segment-a"),
            capacity_bytes: 128,
            used_bytes: 0,
            state,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        }
    }

    fn sample_route(version: u64) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new("object-a"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(version),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: sample_runtime(),
                segment_name: SegmentName::new("segment-a"),
                offset: 32,
                segment_offset: 32,
                length: 12,
                checksum: Some(7),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
        }
    }

    #[test]
    fn etcd_config_builders_cover_default_construction() {
        let backend = EtcdMetadataBackend::default();
        assert!(backend.route_namespace().contains("127.0.0.1:2379"));

        let config = EtcdMetadataConfig::localhost().keyspace(MetadataKeyspace::new("test/etcd"));
        let backend = EtcdMetadataBackend::from_config(config).expect("etcd backend should build");
        assert!(backend.route_namespace().contains("test/etcd"));
    }

    #[test]
    fn etcd_backend_round_trips_full_metadata_surface() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd")),
        )
        .expect("etcd backend should initialize");

        assert!(backend.route_namespace().contains(server.endpoint()));
        assert!(backend.route_namespace().contains("test/etcd"));

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("client lease upsert should succeed");
        backend
            .update_client_state(&lease.runtime, ClientLifecycleState::Draining)
            .expect("client state update should succeed");
        let live_clients = backend
            .list_live_clients()
            .expect("listing live clients should succeed");
        assert_eq!(live_clients.len(), 1);
        assert_eq!(live_clients[0].state, ClientLifecycleState::Draining);
        assert!(matches!(
            backend.update_client_state(
                &ClientRuntimeId::new("missing", ClientEpoch(1)),
                ClientLifecycleState::Active
            ),
            Err(StoreError::NotFound(_))
        ));

        let segment = sample_segment(SegmentLifecycleState::Active);
        backend
            .publish_segment(&segment)
            .expect("segment publish should succeed");
        let listed = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing should succeed");
        assert_eq!(listed, vec![segment.clone()]);

        let reservation = backend
            .reserve_segment(&segment.owner, &segment.segment_name, 13)
            .expect("segment reservation should succeed");
        assert_eq!(reservation.offset_bytes, 0);
        backend
            .release_segment(
                &segment.owner,
                &segment.segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
            )
            .expect("segment release should succeed");

        backend
            .update_segment_state(
                &segment.owner,
                &segment.segment_name,
                SegmentLifecycleState::Draining,
            )
            .expect("segment state update should succeed");
        assert!(matches!(
            backend.reserve_segment(&segment.owner, &segment.segment_name, 8),
            Err(StoreError::InvalidState(_))
        ));
        backend
            .unpublish_segment(&segment.owner, &segment.segment_name)
            .expect("segment unpublish should succeed");
        assert!(backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after delete should succeed")
            .is_empty());
        assert!(matches!(
            backend.release_segment(&segment.owner, &segment.segment_name, 0, 1),
            Err(StoreError::NotFound(_))
        ));

        let route_key = ObjectKey::new("object-a");
        assert!(backend
            .get_object_route(&route_key)
            .expect("route get should succeed")
            .is_none());

        let route_v1 = sample_route(1);
        let created = backend
            .compare_and_swap_object_route(&route_key, None, Some(&route_v1))
            .expect("route create should succeed");
        assert!(created.applied);
        assert_eq!(created.current, Some(route_v1.clone()));

        let route_v2 = sample_route(2);
        let rejected = backend
            .compare_and_swap_object_route(&route_key, None, Some(&route_v2))
            .expect("route cas rejection should succeed");
        assert!(!rejected.applied);
        assert_eq!(rejected.current, Some(route_v1.clone()));

        let replaced = backend
            .compare_and_swap_object_route(&route_key, Some(RouteVersion(1)), Some(&route_v2))
            .expect("route cas update should succeed");
        assert!(replaced.applied);
        assert_eq!(replaced.current, Some(route_v2.clone()));
        assert_eq!(
            backend
                .get_object_route(&route_key)
                .expect("route get after update should succeed"),
            Some(route_v2.clone())
        );
        assert_eq!(
            backend
                .list_object_routes()
                .expect("route listing should succeed"),
            vec![route_v2.clone()]
        );

        let deleted = backend
            .compare_and_swap_object_route(&route_key, Some(RouteVersion(2)), None)
            .expect("route delete should succeed");
        assert!(deleted.applied);
        assert!(deleted.current.is_none());
        assert!(backend
            .list_object_routes()
            .expect("route listing after delete should succeed")
            .is_empty());

        let handoff = HandoffPlan {
            stable_id: ClientStableId::new("writer"),
            from: sample_runtime(),
            to: ClientRuntimeId::new("writer", ClientEpoch(6)),
            kind: HandoffKind::HotUpgrade,
            barrier_version: 9,
            created_at_ms: 10,
            deadline_ms: Some(20),
        };
        backend
            .put_handoff(&handoff)
            .expect("handoff put should succeed");
        assert_eq!(
            backend
                .get_handoff(&handoff.stable_id)
                .expect("handoff get should succeed"),
            Some(handoff)
        );
    }

    #[test]
    fn etcd_backend_tenant_policy_supports_versioned_put_list_and_delete() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-tenant-policy")),
        )
        .expect("etcd backend should initialize");
        let scope = TenantPolicyScope::new("tenant/a", Some("domain-1"), None::<String>);
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

        let stored = backend
            .put_tenant_policy(&policy, None)
            .expect("tenant policy insert should succeed");
        assert_eq!(stored, policy);
        assert_eq!(
            backend
                .get_tenant_policy(&scope)
                .expect("tenant policy read should succeed"),
            Some(policy.clone())
        );

        let mut updated = policy.clone();
        updated.version = 2;
        updated.updated_at_ms = 20;
        updated.updated_by = "admin-2".to_string();
        updated.spec.quota.as_mut().unwrap().max_objects = Some(16);
        let error = backend
            .put_tenant_policy(&updated, Some(3))
            .expect_err("mismatched version should fail");
        assert!(matches!(error, StoreError::Conflict(_)));

        backend
            .put_tenant_policy(&updated, Some(1))
            .expect("tenant policy update should succeed");
        assert_eq!(
            backend
                .list_tenant_policies()
                .expect("tenant policy listing should succeed"),
            vec![updated.clone()]
        );
        assert!(backend
            .delete_tenant_policy(&scope, Some(2))
            .expect("tenant policy delete should succeed"));
        assert_eq!(
            backend
                .get_tenant_policy(&scope)
                .expect("tenant policy read after delete should succeed"),
            None
        );
    }

    #[test]
    fn etcd_backend_tenant_quota_reservation_finalize_and_abort_follow_versioned_state_machine() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-tenant-quota")),
        )
        .expect("etcd backend should initialize");
        let scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let key = ObjectKey::new("tenant-a::alpha");
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        let reserved = backend
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

        let finalized = backend
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

        let duplicate_finalize = backend
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

        let overwrite = backend
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

        let conflict = backend
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

        let aborted = backend
            .abort_tenant_quota("resv-overwrite")
            .expect("abort should succeed");
        assert_eq!(aborted.quota.pending_reserved_bytes, 0);
        assert_eq!(aborted.quota.used_bytes, 32);
        assert_eq!(
            aborted.reservation.state,
            TenantQuotaReservationState::Aborted
        );

        let duplicate_abort = backend
            .abort_tenant_quota("resv-overwrite")
            .expect("duplicate abort should be idempotent");
        assert_eq!(duplicate_abort.quota.pending_reserved_bytes, 0);
        assert_eq!(duplicate_abort.reservation.version, 2);

        let delete_reserved = backend
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

        let deleted = backend
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
        assert!(backend
            .get_tenant_object_accounting(&key)
            .expect("object accounting lookup should succeed")
            .is_none());
    }

    #[test]
    fn etcd_backend_tenant_quota_reservation_enforces_limits_and_lists_by_tenant() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-tenant-quota-limits")),
        )
        .expect("etcd backend should initialize");
        let scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        backend
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

        let byte_limit = backend
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

        let object_limit = backend
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

        let reservations = backend
            .list_tenant_quota_reservations(&scope)
            .expect("reservation listing should succeed");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].reservation_id, "resv-1");
        let quota = backend
            .get_tenant_quota_state(&scope)
            .expect("quota state lookup should succeed")
            .expect("quota state should exist");
        assert_eq!(quota.pending_reserved_bytes, 40);
        assert_eq!(quota.pending_reserved_objects, 1);
    }
}
