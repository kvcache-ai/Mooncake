mod cold_tier;

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::keyspace::{parse_route_policy_domain, parse_tenant_policy_scope};
use crate::redis_backend::{ClientLeaseLiveness, RedisMetadataCleanupReport};
use crate::segment_state::StoredSegmentState;
use crate::MetadataKeyspace;
use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
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

    pub fn tenant(mut self, tenant: &str) -> Self {
        self.keyspace = self.keyspace.tenant_prefixed(tenant);
        self
    }
}

const EVICTION_FRONTIER_LIMIT: usize = 16;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct EvictionCandidate {
    updated_at_ms: u64,
    committed_length: Reverse<u64>,
    key: ObjectKey,
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
    fn candidate_from_object(object: &TenantObjectAccounting) -> EvictionCandidate {
        EvictionCandidate {
            updated_at_ms: object.updated_at_ms,
            committed_length: Reverse(object.committed_length),
            key: object.key.clone(),
        }
    }

    fn frontier_entries_for_candidate(
        &self,
        tenant: &str,
        candidate: &EvictionCandidate,
    ) -> (String, String) {
        let frontier_key = self.config.keyspace.tenant_eviction_candidate(
            tenant,
            candidate.updated_at_ms,
            candidate.committed_length.0,
            &candidate.key,
        );
        let object_key = self
            .config
            .keyspace
            .tenant_object_accounting(&candidate.key);
        (frontier_key, object_key)
    }

    fn frontier_entries_from_objects(
        &self,
        tenant: &str,
        objects: &[TenantObjectAccounting],
    ) -> Vec<(String, String)> {
        let mut frontier = objects
            .iter()
            .filter(|object| object.state == TenantObjectAccountingState::Active)
            .map(Self::candidate_from_object)
            .collect::<Vec<_>>();
        frontier.sort();
        frontier.truncate(EVICTION_FRONTIER_LIMIT);
        frontier
            .into_iter()
            .map(|candidate| self.frontier_entries_for_candidate(tenant, &candidate))
            .collect()
    }

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

    async fn collect_live_epochs(
        &self,
        client: &mut Client,
        stable_id: &ClientStableId,
    ) -> Result<Vec<u64>> {
        let marker_prefix = self
            .config
            .keyspace
            .client_by_stable_marker_prefix(stable_id);
        let lease_prefix = self.config.keyspace.client_prefix_for_stable(stable_id);
        let response = client
            .get(marker_prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(etcd_error("etcd scan client by-stable markers"))?;
        let mut live_epochs = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            let Some(epoch) = std::str::from_utf8(kv.key())
                .ok()
                .and_then(|key| key.rsplit('/').next())
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            let lease_key = format!("{lease_prefix}{epoch}");
            let existing = client
                .get(lease_key.clone(), None)
                .await
                .map_err(etcd_error("etcd get client lease while scanning markers"))?;
            if existing.kvs().is_empty() {
                client
                    .delete(kv.key(), None)
                    .await
                    .map_err(etcd_error("etcd delete stale by-stable marker"))?;
                continue;
            }
            live_epochs.push(epoch);
        }
        Ok(live_epochs)
    }

    async fn current_expiry_entry_key(
        &self,
        client: &mut Client,
        runtime: &ClientRuntimeId,
    ) -> Result<(Option<String>, i64)> {
        let state_key = self.config.keyspace.client_lease_expiry_runtime(runtime);
        let response = client
            .get(state_key, None)
            .await
            .map_err(etcd_error("etcd get client lease expiry runtime state"))?;
        let current = response
            .kvs()
            .first()
            .map(|kv| std::str::from_utf8(kv.value()).map(|value| value.to_string()))
            .transpose()
            .map_err(etcd_utf8_error(
                "etcd decode client lease expiry runtime state",
            ))?;
        let mod_revision = response
            .kvs()
            .first()
            .map(|kv| kv.mod_revision())
            .unwrap_or(0);
        Ok((current, mod_revision))
    }

    async fn delete_current_expiry_entry(
        &self,
        client: &mut Client,
        runtime: &ClientRuntimeId,
    ) -> Result<bool> {
        let state_key = self.config.keyspace.client_lease_expiry_runtime(runtime);
        loop {
            let (current_entry, mod_revision) =
                self.current_expiry_entry_key(client, runtime).await?;
            let Some(current_entry) = current_entry else {
                return Ok(false);
            };
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    state_key.clone(),
                    CompareOp::Equal,
                    mod_revision,
                )])
                .and_then([
                    TxnOp::delete(state_key.clone(), None),
                    TxnOp::delete(current_entry, None),
                ]);
            let response = client
                .txn(txn)
                .await
                .map_err(etcd_error("etcd delete client lease expiry entries"))?;
            if response.succeeded() {
                return Ok(true);
            }
        }
    }

    async fn update_expiry_entry(
        &self,
        client: &mut Client,
        runtime: &ClientRuntimeId,
        expires_at_ms: u64,
    ) -> Result<()> {
        let state_key = self.config.keyspace.client_lease_expiry_runtime(runtime);
        let lease_key = self.config.keyspace.client(runtime);
        let next_entry = self
            .config
            .keyspace
            .client_lease_expiry_time(expires_at_ms, runtime);
        loop {
            let (current_entry, mod_revision) =
                self.current_expiry_entry_key(client, runtime).await?;
            let mut ops = vec![
                TxnOp::put(state_key.clone(), next_entry.clone(), None),
                TxnOp::put(next_entry.clone(), lease_key.clone(), None),
            ];
            if let Some(current_entry) = current_entry.filter(|value| value != &next_entry) {
                ops.push(TxnOp::delete(current_entry, None));
            }
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    state_key.clone(),
                    CompareOp::Equal,
                    mod_revision,
                )])
                .and_then(ops);
            let response = client
                .txn(txn)
                .await
                .map_err(etcd_error("etcd update client lease expiry index"))?;
            if response.succeeded() {
                return Ok(());
            }
        }
    }

    async fn cleanup_stale_segments_for_owner_runtime(
        &self,
        client: &mut Client,
        owner: &ClientRuntimeId,
    ) -> Result<RedisMetadataCleanupReport> {
        let prefix = self.config.keyspace.segment_prefix(Some(owner));
        let response = client
            .get(prefix, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(etcd_error("etcd list owner segments for cleanup"))?;
        let segments = response
            .kvs()
            .iter()
            .map(|kv| {
                serde_json::from_slice::<StoredSegmentState>(kv.value())
                    .map(|state| state.announcement)
                    .map_err(json_error)
            })
            .collect::<Result<Vec<_>>>()?;
        for segment in &segments {
            client
                .delete(
                    self.config.keyspace.segment(owner, &segment.segment_name),
                    None,
                )
                .await
                .map_err(etcd_error("etcd delete stale segment"))?;
        }
        Ok(RedisMetadataCleanupReport {
            live_clients: 0,
            inspected_segment_keys: segments.len(),
            removed_segment_keys: segments.len(),
            removed_segment_index_entries: 0,
            removed_owner_segment_index_entries: 0,
            stale_missing_segment_index_entries: 0,
        })
    }

    pub fn cleanup_stale_segments(&self) -> Result<RedisMetadataCleanupReport> {
        let live_clients = self.list_live_clients()?;
        let due_lease_keys = self.list_due_client_lease_expiries(now_ms(), 4096)?;
        let mut removed_segment_keys = 0usize;
        let mut inspected_segment_keys = 0usize;
        self.block_on(async {
            let mut client = self.client().await?;
            for lease_key in &due_lease_keys {
                let Some((stable_id, epoch)) = self.config.keyspace.parse_client_key(lease_key)
                else {
                    let _ = self.remove_client_lease_expiry_entry(lease_key);
                    continue;
                };
                let owner = ClientRuntimeId::new(stable_id, ClientEpoch(epoch));
                if matches!(
                    self.client_lease_liveness(&owner)?,
                    ClientLeaseLiveness::Live(_)
                ) {
                    continue;
                }
                let stats = self
                    .cleanup_stale_segments_for_owner_runtime(&mut client, &owner)
                    .await?;
                inspected_segment_keys =
                    inspected_segment_keys.saturating_add(stats.inspected_segment_keys);
                removed_segment_keys =
                    removed_segment_keys.saturating_add(stats.removed_segment_keys);
                let _ = self.remove_client_lease_expiry_entry(lease_key);
            }
            Ok(())
        })?;
        Ok(RedisMetadataCleanupReport {
            live_clients: live_clients.len(),
            inspected_segment_keys,
            removed_segment_keys,
            removed_segment_index_entries: 0,
            removed_owner_segment_index_entries: 0,
            stale_missing_segment_index_entries: 0,
        })
    }

    pub fn cleanup_stale_segments_for_owner(
        &self,
        owner: &ClientRuntimeId,
    ) -> Result<RedisMetadataCleanupReport> {
        self.block_on(async {
            let mut client = self.client().await?;
            self.cleanup_stale_segments_for_owner_runtime(&mut client, owner)
                .await
        })
    }

    pub fn client_lease_liveness(&self, runtime: &ClientRuntimeId) -> Result<ClientLeaseLiveness> {
        let key = self.config.keyspace.client(runtime);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key.clone(), None)
                .await
                .map_err(etcd_error("etcd get client lease liveness"))?;
            let Some(kv) = response.kvs().first() else {
                return Ok(ClientLeaseLiveness::Missing);
            };
            let lease: ClientLease = serde_json::from_slice(kv.value()).map_err(json_error)?;
            if lease.expires_at_ms >= now_ms() {
                Ok(ClientLeaseLiveness::Live(lease))
            } else {
                Ok(ClientLeaseLiveness::Expired(lease))
            }
        })
    }

    pub fn list_due_client_lease_expiries(
        &self,
        expires_before_ms: u64,
        limit: usize,
    ) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = self.config.keyspace.client_lease_expiry_time_prefix();
        let range_end = self
            .config
            .keyspace
            .client_lease_expiry_time_range_end(expires_before_ms.saturating_add(1));
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(
                    prefix,
                    Some(
                        GetOptions::new()
                            .with_range(range_end)
                            .with_limit(limit as i64),
                    ),
                )
                .await
                .map_err(etcd_error("etcd list due client lease expiries"))?;
            response
                .kvs()
                .iter()
                .map(|kv| {
                    std::str::from_utf8(kv.value())
                        .map(|value| value.to_string())
                        .map_err(etcd_utf8_error("etcd decode client lease expiry entry"))
                })
                .collect()
        })
    }

    pub fn refresh_client_lease_expiry(
        &self,
        runtime: &ClientRuntimeId,
        expires_at_ms: u64,
    ) -> Result<()> {
        self.block_on(async {
            let mut client = self.client().await?;
            self.update_expiry_entry(&mut client, runtime, expires_at_ms)
                .await
        })
    }

    pub fn remove_client_lease_expiry(&self, runtime: &ClientRuntimeId) -> Result<bool> {
        self.block_on(async {
            let mut client = self.client().await?;
            self.delete_current_expiry_entry(&mut client, runtime).await
        })
    }

    pub fn remove_client_lease_expiry_entry(&self, lease_key: &str) -> Result<bool> {
        let Some((stable_id, epoch)) = self.config.keyspace.parse_client_key(lease_key) else {
            return Ok(false);
        };
        self.remove_client_lease_expiry(&ClientRuntimeId::new(stable_id, ClientEpoch(epoch)))
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

    fn backend_kind(&self) -> &'static str {
        "etcd"
    }

    fn for_tenant(&self, tenant: &str) -> Option<Arc<dyn MetadataBackend>> {
        EtcdMetadataBackend::from_config(self.config.clone().tenant(tenant))
            .ok()
            .map(|backend| Arc::new(backend) as Arc<dyn MetadataBackend>)
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        let stable_id = lease.runtime.stable_id.clone();
        let new_epoch = lease.runtime.epoch.0;
        let lease_key = self.config.keyspace.client(&lease.runtime);
        let marker_key = self
            .config
            .keyspace
            .client_by_stable_marker(&stable_id, new_epoch);
        let hwm_key = self.config.keyspace.client_epoch_hwm(&stable_id);
        let stable_key = self.config.keyspace.stable_runtime(&stable_id);
        let expiry_state_key = self
            .config
            .keyspace
            .client_lease_expiry_runtime(&lease.runtime);
        let expiry_entry_key = self
            .config
            .keyspace
            .client_lease_expiry_time(lease.expires_at_ms, &lease.runtime);
        let payload = serde_json::to_string(lease).map_err(json_error)?;
        let runtime_key = lease.runtime.storage_key();
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let existing = client
                    .get(lease_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get client lease"))?;
                if !existing.kvs().is_empty() {
                    let lease_mod_rev = existing
                        .kvs()
                        .first()
                        .map(|kv| kv.mod_revision())
                        .unwrap_or(0);
                    let (current_expiry_entry, expiry_mod_rev) =
                        self.current_expiry_entry_key(&mut client, &lease.runtime).await?;
                    let mut ops = vec![
                        TxnOp::put(lease_key.clone(), payload.clone(), None),
                        TxnOp::put(expiry_state_key.clone(), expiry_entry_key.clone(), None),
                        TxnOp::put(expiry_entry_key.clone(), lease_key.clone(), None),
                    ];
                    if lease.state == ClientLifecycleState::Active {
                        ops.push(TxnOp::put(stable_key.clone(), runtime_key.clone(), None));
                    } else {
                        ops.push(TxnOp::delete(stable_key.clone(), None));
                    }
                    if let Some(current_expiry_entry) =
                        current_expiry_entry.filter(|value| value != &expiry_entry_key)
                    {
                        ops.push(TxnOp::delete(current_expiry_entry, None));
                    }
                    let txn = Txn::new()
                        .when(vec![
                            Compare::mod_revision(
                                lease_key.clone(),
                                CompareOp::Equal,
                                lease_mod_rev,
                            ),
                            Compare::mod_revision(
                                expiry_state_key.clone(),
                                CompareOp::Equal,
                                expiry_mod_rev,
                            ),
                        ])
                        .and_then(ops);
                    let txn_response = client
                        .txn(txn)
                        .await
                        .map_err(etcd_error("etcd refresh client lease"))?;
                    if txn_response.succeeded() {
                        return Ok(());
                    }
                    continue;
                }

                let active_max = self
                    .collect_live_epochs(&mut client, &stable_id)
                    .await?
                    .into_iter()
                    .max()
                    .unwrap_or(0);

                let hwm_response = client
                    .get(hwm_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get client epoch hwm"))?;
                let (hwm_value, hwm_mod_rev) = hwm_response
                    .kvs()
                    .first()
                    .and_then(|kv| {
                        let value = std::str::from_utf8(kv.value()).ok()?;
                        let parsed = value.parse::<u64>().ok()?;
                        Some((parsed, kv.mod_revision()))
                    })
                    .unwrap_or((0, 0));
                let (current_expiry_entry, expiry_mod_rev) =
                    self.current_expiry_entry_key(&mut client, &lease.runtime).await?;

                let floor = active_max.max(hwm_value);
                let allow_same_epoch_reclaim = new_epoch == hwm_value && new_epoch > active_max;
                if new_epoch <= floor && !allow_same_epoch_reclaim {
                    return Err(StoreError::StaleEpoch(format!(
                        "lease rejected: proposed epoch {new_epoch} <= floor {floor} for stable_id {}",
                        stable_id.0
                    )));
                }

                let txn = Txn::new()
                    .when(vec![
                        Compare::mod_revision(lease_key.clone(), CompareOp::Equal, 0),
                        Compare::mod_revision(marker_key.clone(), CompareOp::Equal, 0),
                        Compare::mod_revision(hwm_key.clone(), CompareOp::Equal, hwm_mod_rev),
                        Compare::mod_revision(
                            expiry_state_key.clone(),
                            CompareOp::Equal,
                            expiry_mod_rev,
                        ),
                    ])
                    .and_then({
                        let mut ops = vec![
                            TxnOp::put(lease_key.clone(), payload.clone(), None),
                            TxnOp::put(marker_key.clone(), Vec::<u8>::new(), None),
                            TxnOp::put(
                                hwm_key.clone(),
                                hwm_value.max(new_epoch).to_string(),
                                None,
                            ),
                            TxnOp::put(expiry_state_key.clone(), expiry_entry_key.clone(), None),
                            TxnOp::put(expiry_entry_key.clone(), lease_key.clone(), None),
                        ];
                        if lease.state == ClientLifecycleState::Active {
                            ops.push(TxnOp::put(stable_key.clone(), runtime_key.clone(), None));
                        } else {
                            ops.push(TxnOp::delete(stable_key.clone(), None));
                        }
                        if let Some(current_expiry_entry) =
                            current_expiry_entry.filter(|value| value != &expiry_entry_key)
                        {
                            ops.push(TxnOp::delete(current_expiry_entry, None));
                        }
                        ops
                    });
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd upsert client lease"))?;
                if txn_response.succeeded() {
                    return Ok(());
                }
            }
        })
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        let stable_id = template.runtime.stable_id.clone();
        let hwm_key = self.config.keyspace.client_epoch_hwm(&stable_id);
        let lease_key_prefix = self.config.keyspace.client_prefix_for_stable(&stable_id);
        let stable_key = self.config.keyspace.stable_runtime(&stable_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let active_max = self
                    .collect_live_epochs(&mut client, &stable_id)
                    .await?
                    .into_iter()
                    .max()
                    .unwrap_or(0);

                let hwm_response = client
                    .get(hwm_key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get client epoch hwm"))?;
                let (hwm_value, hwm_mod_rev) = hwm_response
                    .kvs()
                    .first()
                    .and_then(|kv| {
                        let value = std::str::from_utf8(kv.value()).ok()?;
                        let parsed = value.parse::<u64>().ok()?;
                        Some((parsed, kv.mod_revision()))
                    })
                    .unwrap_or((0, 0));

                let floor = active_max.max(hwm_value);
                let new_epoch = floor.saturating_add(1);
                let lease_key = format!("{lease_key_prefix}{new_epoch}");
                let marker_key = self
                    .config
                    .keyspace
                    .client_by_stable_marker(&stable_id, new_epoch);
                let runtime = ClientRuntimeId {
                    stable_id: stable_id.clone(),
                    epoch: ClientEpoch(new_epoch),
                };
                let expiry_state_key = self.config.keyspace.client_lease_expiry_runtime(&runtime);
                let expiry_entry_key = self
                    .config
                    .keyspace
                    .client_lease_expiry_time(template.expires_at_ms, &runtime);

                let mut lease = template.clone();
                lease.runtime = runtime.clone();
                let payload = serde_json::to_string(&lease).map_err(json_error)?;
                let (current_expiry_entry, expiry_mod_rev) =
                    self.current_expiry_entry_key(&mut client, &runtime).await?;

                let txn = Txn::new()
                    .when(vec![
                        Compare::mod_revision(lease_key.clone(), CompareOp::Equal, 0),
                        Compare::mod_revision(marker_key.clone(), CompareOp::Equal, 0),
                        Compare::mod_revision(hwm_key.clone(), CompareOp::Equal, hwm_mod_rev),
                        Compare::mod_revision(
                            expiry_state_key.clone(),
                            CompareOp::Equal,
                            expiry_mod_rev,
                        ),
                    ])
                    .and_then({
                        let mut ops = vec![
                            TxnOp::put(lease_key.clone(), payload, None),
                            TxnOp::put(marker_key, Vec::<u8>::new(), None),
                            TxnOp::put(hwm_key.clone(), new_epoch.to_string(), None),
                            TxnOp::put(expiry_state_key.clone(), expiry_entry_key.clone(), None),
                            TxnOp::put(expiry_entry_key.clone(), lease_key.clone(), None),
                        ];
                        if lease.state == ClientLifecycleState::Active {
                            ops.push(TxnOp::put(
                                stable_key.clone(),
                                lease.runtime.storage_key(),
                                None,
                            ));
                        } else {
                            ops.push(TxnOp::delete(stable_key.clone(), None));
                        }
                        if let Some(current_expiry_entry) =
                            current_expiry_entry.filter(|value| value != &expiry_entry_key)
                        {
                            ops.push(TxnOp::delete(current_expiry_entry, None));
                        }
                        ops
                    });
                let txn_response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd allocate client lease"))?;
                if txn_response.succeeded() {
                    return Ok(runtime);
                }
            }
        })
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        let key = self.config.keyspace.client(runtime);
        let stable_key = self.config.keyspace.stable_runtime(&runtime.stable_id);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get client lease"))?;
                let kv = response
                    .kvs()
                    .first()
                    .ok_or_else(|| StoreError::NotFound(key.clone()))?;
                let mut lease: ClientLease =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                lease.state = next;
                let payload = serde_json::to_string(&lease).map_err(json_error)?;
                let mut ops = vec![TxnOp::put(key.clone(), payload, None)];
                if next == ClientLifecycleState::Active {
                    ops.push(TxnOp::put(stable_key.clone(), runtime.storage_key(), None));
                } else {
                    ops.push(TxnOp::delete(stable_key.clone(), None));
                }
                let txn = Txn::new()
                    .when([Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    )])
                    .and_then(ops);
                let response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd update client lease"))?;
                if response.succeeded() {
                    return Ok(());
                }
            }
        })
    }

    fn get_client_lease(&self, runtime: &ClientRuntimeId) -> Result<Option<ClientLease>> {
        let key = self.config.keyspace.client(runtime);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get client lease by runtime"))?;
            let lease = response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice::<ClientLease>(kv.value()).map_err(json_error))
                .transpose()?;
            Ok(lease.filter(|lease| lease.expires_at_ms >= now_ms()))
        })
    }

    fn get_live_runtime_by_stable_id(
        &self,
        stable_id: &ClientStableId,
    ) -> Result<Option<ClientLease>> {
        let stable_key = self.config.keyspace.stable_runtime(stable_id);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(stable_key, None)
                .await
                .map_err(etcd_error("etcd get stable runtime index"))?;
            let Some(kv) = response.kvs().first() else {
                return Ok(None);
            };
            let runtime_key = std::str::from_utf8(kv.value()).map_err(|error| {
                StoreError::Metadata(format!("invalid stable runtime index: {error}"))
            })?;
            let Some(runtime) = ClientRuntimeId::from_storage_key(runtime_key) else {
                return Err(StoreError::Metadata(format!(
                    "invalid runtime storage key in stable runtime index: {runtime_key}"
                )));
            };
            let lease_key = self.config.keyspace.client(&runtime);
            let response = client
                .get(lease_key, None)
                .await
                .map_err(etcd_error("etcd get client lease by stable runtime"))?;
            let lease = response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice::<ClientLease>(kv.value()).map_err(json_error))
                .transpose()?;
            Ok(lease.filter(|lease| lease.expires_at_ms >= now_ms()))
        })
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        let prefix = self
            .config
            .keyspace
            .client_pattern()
            .trim_end_matches('*')
            .to_string();
        let keyspace = self.config.keyspace.clone();
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(prefix, Some(GetOptions::new().with_prefix()))
                .await
                .map_err(etcd_error("etcd list client leases"))?;
            let now = now_ms();
            let mut live = Vec::with_capacity(response.kvs().len());
            let mut stale_markers: Vec<String> = Vec::new();
            for kv in response.kvs() {
                let lease: ClientLease = serde_json::from_slice(kv.value()).map_err(json_error)?;
                if lease.expires_at_ms >= now {
                    live.push(lease);
                } else {
                    stale_markers.push(
                        keyspace.client_by_stable_marker(
                            &lease.runtime.stable_id,
                            lease.runtime.epoch.0,
                        ),
                    );
                }
            }
            for marker in stale_markers {
                client
                    .delete(marker, None)
                    .await
                    .map_err(etcd_error("etcd delete stale by-stable marker"))?;
            }
            Ok(live)
        })
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        let key = self
            .config
            .keyspace
            .segment(&segment.owner, &segment.segment_name);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
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
                let segment_compare = if let Some(kv) = current.kvs().first() {
                    Compare::mod_revision(key.clone(), CompareOp::Equal, kv.mod_revision())
                } else {
                    Compare::version(key.clone(), CompareOp::Equal, 0)
                };
                let txn = Txn::new().when([segment_compare]).and_then([TxnOp::put(
                    key.clone(),
                    payload,
                    None,
                )]);
                let response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd publish segment"))?;
                if response.succeeded() {
                    return Ok(());
                }
            }
        })
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        let key = self.config.keyspace.segment(owner, segment);
        self.block_on(async {
            let mut client = self.client().await?;
            loop {
                let segment_response = client
                    .get(key.clone(), None)
                    .await
                    .map_err(etcd_error("etcd get segment before delete"))?;
                let Some(segment_kv) = segment_response.kvs().first() else {
                    return Ok(());
                };
                let compares = vec![Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    segment_kv.mod_revision(),
                )];
                let ops = vec![TxnOp::delete(key.clone(), None)];
                let txn = Txn::new().when(compares).and_then(ops);
                let response = client
                    .txn(txn)
                    .await
                    .map_err(etcd_error("etcd delete segment"))?;
                if response.succeeded() {
                    return Ok(());
                }
            }
        })
    }

    fn get_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<Option<SegmentAnnouncement>> {
        let key = self.config.keyspace.segment(owner, segment);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get segment by owner and name"))?;
            response
                .kvs()
                .first()
                .map(|kv| {
                    serde_json::from_slice::<StoredSegmentState>(kv.value())
                        .map(|state| state.announcement)
                        .map_err(json_error)
                })
                .transpose()
        })
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        if owner.is_none() {
            let mut segments = Vec::new();
            for lease in self.list_live_clients()? {
                segments.extend(self.list_segments(Some(&lease.runtime))?);
            }
            return Ok(segments);
        }
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
                        version_floor: None,
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
                            version_floor: None,
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
                        version_floor: None,
                    });
                }
            }
        })
    }

    fn put_cold_tier_device_if_absent(
        &self,
        device: &ColdTierDeviceRecord,
    ) -> Result<ColdTierPutDeviceResult> {
        self.etcd_put_cold_tier_device_if_absent(device)
    }

    fn get_cold_tier_device(&self, device_id: &str) -> Result<Option<ColdTierDeviceRecord>> {
        self.etcd_get_cold_tier_device(device_id)
    }

    fn list_cold_tier_devices(
        &self,
        filter: &ColdTierDeviceFilter,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        self.etcd_list_cold_tier_devices(filter)
    }

    fn update_cold_tier_device(
        &self,
        device_id: &str,
        update: ColdTierDeviceUpdate,
    ) -> Result<ColdTierDeviceRecord> {
        self.etcd_update_cold_tier_device(device_id, update)
    }

    fn apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        delta: ColdTierUsageDelta,
        updated_at_ms: u64,
    ) -> Result<ColdTierDeviceRecord> {
        self.etcd_apply_cold_tier_usage_delta(device_id, delta, updated_at_ms)
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

    fn get_tenant_policies(
        &self,
        scopes: &[TenantPolicyScope],
    ) -> Result<Vec<Option<TenantPolicy>>> {
        if scopes.is_empty() {
            return Ok(Vec::new());
        }
        let keys = scopes
            .iter()
            .map(|scope| self.config.keyspace.tenant_policy(scope))
            .collect::<Vec<_>>();
        self.block_on(async {
            let ops = keys
                .iter()
                .cloned()
                .map(|key| TxnOp::get(key, None))
                .collect::<Vec<_>>();
            let mut client = self.client().await?;
            let response = client
                .txn(Txn::new().and_then(ops))
                .await
                .map_err(etcd_error("etcd batch get tenant policies"))?;
            response
                .op_responses()
                .into_iter()
                .map(|op| match op {
                    etcd_client::TxnOpResponse::Get(get) => get
                        .kvs()
                        .first()
                        .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                        .transpose(),
                    other => Err(StoreError::Metadata(format!(
                        "etcd batch get tenant policies returned unexpected response: {other:?}"
                    ))),
                })
                .collect()
        })
    }

    fn list_tenant_policies(&self, tenant: Option<&str>) -> Result<Vec<TenantPolicy>> {
        self.block_on(async {
            let mut client = self.client().await?;
            let mut responses = Vec::new();
            if let Some(tenant) = tenant {
                let root_key = self.config.keyspace.tenant_policy(&TenantPolicyScope::new(
                    tenant,
                    None::<String>,
                    None::<String>,
                ));
                responses.push(
                    client
                        .get(root_key.clone(), None)
                        .await
                        .map_err(etcd_error("etcd get root tenant policy"))?,
                );
                responses.push(
                    client
                        .get(
                            format!("{root_key}/"),
                            Some(GetOptions::new().with_prefix()),
                        )
                        .await
                        .map_err(etcd_error("etcd list tenant policies"))?,
                );
            } else {
                let prefix = self.config.keyspace.tenant_policy_prefix(None);
                responses.push(
                    client
                        .get(prefix, Some(GetOptions::new().with_prefix()))
                        .await
                        .map_err(etcd_error("etcd list tenant policies"))?,
                );
            }
            let mut policies = responses
                .iter()
                .flat_map(|response| response.kvs().iter())
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

    fn get_tenant_quota_reservation(
        &self,
        reservation_id: &str,
    ) -> Result<Option<TenantQuotaReservation>> {
        let key = self
            .config
            .keyspace
            .tenant_quota_reservation(reservation_id);
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(key, None)
                .await
                .map_err(etcd_error("etcd get tenant quota reservation"))?;
            response
                .kvs()
                .first()
                .map(|kv| serde_json::from_slice(kv.value()).map_err(json_error))
                .transpose()
        })
    }

    fn list_tenant_eviction_candidates(
        &self,
        scope: &TenantPolicyScope,
        limit: usize,
    ) -> Result<Vec<TenantObjectAccounting>> {
        let scope = root_scope(scope)?;
        let prefix = self
            .config
            .keyspace
            .tenant_eviction_frontier_prefix(&scope.tenant);
        let read_limit = limit.min(EVICTION_FRONTIER_LIMIT);
        if read_limit == 0 {
            return Ok(Vec::new());
        }
        self.block_on(async {
            let mut client = self.client().await?;
            let response = client
                .get(
                    prefix,
                    Some(
                        GetOptions::new()
                            .with_prefix()
                            .with_limit(read_limit as i64),
                    ),
                )
                .await
                .map_err(etcd_error("etcd list tenant eviction frontier"))?;
            let mut candidates = Vec::new();
            for kv in response.kvs() {
                let object_key = String::from_utf8(kv.value().to_vec()).map_err(|error| {
                    StoreError::Metadata(format!(
                        "etcd list tenant eviction frontier: invalid object key utf8: {error}"
                    ))
                })?;
                let object_response = client
                    .get(object_key, None)
                    .await
                    .map_err(etcd_error("etcd get tenant eviction candidate object"))?;
                let Some(object_kv) = object_response.kvs().first() else {
                    continue;
                };
                let object: TenantObjectAccounting =
                    serde_json::from_slice(object_kv.value()).map_err(json_error)?;
                if object.scope == scope && object.state == TenantObjectAccountingState::Active {
                    candidates.push(object);
                }
            }
            candidates.sort_by(|left, right| {
                left.updated_at_ms
                    .cmp(&right.updated_at_ms)
                    .then_with(|| right.committed_length.cmp(&left.committed_length))
                    .then_with(|| left.key.cmp(&right.key))
            });
            candidates.truncate(read_limit);
            Ok(candidates)
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
                if let Some(limit) = normalized.limit.max_bytes.filter(|_| positive_bytes > 0) {
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
                if let Some(limit) =
                    normalized.limit.max_objects.filter(|_| positive_objects > 0)
                {
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
                let frontier_prefix = self
                    .config
                    .keyspace
                    .tenant_eviction_frontier_prefix(&scope.tenant);
                let frontier_response = client
                    .get(frontier_prefix.clone(), Some(GetOptions::new().with_prefix()))
                    .await
                    .map_err(etcd_error("etcd get tenant eviction frontier"))?;
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

                let mut frontier_objects = Vec::new();
                for kv in frontier_response.kvs() {
                    let object_key = String::from_utf8(kv.value().to_vec()).map_err(|error| {
                        StoreError::Metadata(format!(
                            "etcd get tenant eviction frontier: invalid object key utf8: {error}"
                        ))
                    })?;
                    let object_response = client
                        .get(object_key, None)
                        .await
                        .map_err(etcd_error("etcd get tenant eviction frontier object"))?;
                    let Some(object_kv) = object_response.kvs().first() else {
                        continue;
                    };
                    let object: TenantObjectAccounting =
                        serde_json::from_slice(object_kv.value()).map_err(json_error)?;
                    if object.scope == scope && object.state == TenantObjectAccountingState::Active {
                        frontier_objects.push(object);
                    }
                }
                frontier_objects.retain(|object| object.key != reservation.key);
                if let Some(object) = next_object.clone() {
                    frontier_objects.push(object);
                }
                let frontier_entries =
                    self.frontier_entries_from_objects(&scope.tenant, &frontier_objects);
                let next_frontier_keys = frontier_entries
                    .iter()
                    .map(|(frontier_key, _)| frontier_key.clone())
                    .collect::<BTreeSet<_>>();
                let stale_frontier_keys = frontier_response
                    .kvs()
                    .iter()
                    .filter_map(|kv| String::from_utf8(kv.key().to_vec()).ok())
                    .filter(|frontier_key| !next_frontier_keys.contains(frontier_key))
                    .collect::<Vec<_>>();

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
                for frontier_key in stale_frontier_keys {
                    ops.push(TxnOp::delete(frontier_key, None));
                }
                for (frontier_key, object_key) in frontier_entries {
                    ops.push(TxnOp::put(frontier_key, object_key, None));
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

fn etcd_utf8_error(operation: &'static str) -> impl FnOnce(std::str::Utf8Error) -> StoreError {
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
    use mooncake_store_core::error::QuotaKind;
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

    use super::{etcd_error, EtcdMetadataBackend, EtcdMetadataConfig, MetadataKeyspace};

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
            .block_on(async {
                let mut client = etcd_client::Client::connect([endpoint.to_string()], None)
                    .await
                    .ok()?;
                client.get("__health__", None).await.ok()?;
                Some(())
            })
            .is_some()
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
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
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
                offset: Some(32),
                segment_offset: 32,
                length: 12,
                checksum: Some(7),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
            cold_backing: None,
            nof_backing: None,
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
    fn etcd_backend_reclaims_expired_same_epoch() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/etcd-reclaim-same-epoch");
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()]).keyspace(keyspace.clone()),
        )
        .expect("etcd backend should initialize");

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("initial lease publish should succeed");

        backend
            .block_on(async {
                let mut client = backend.client().await?;
                client
                    .delete(keyspace.client(&lease.runtime), None)
                    .await
                    .map_err(etcd_error("etcd delete client lease for reclaim test"))?;
                Ok(())
            })
            .expect("lease delete should succeed");

        backend
            .upsert_client_lease(&lease)
            .expect("same epoch should reclaim after the lease key disappears");
    }

    #[test]
    fn etcd_backend_tracks_due_client_lease_expiries() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/etcd-lease-expiry");
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()]).keyspace(keyspace.clone()),
        )
        .expect("etcd backend should initialize");

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("lease publish should seed expiry index");

        assert!(backend
            .list_due_client_lease_expiries(super::now_ms(), 8)
            .expect("future lease should not be due")
            .is_empty());

        backend
            .refresh_client_lease_expiry(&lease.runtime, super::now_ms().saturating_sub(1))
            .expect("expiry queue refresh should succeed");
        assert_eq!(
            backend
                .list_due_client_lease_expiries(super::now_ms(), 8)
                .expect("due lease should be visible"),
            vec![keyspace.client(&lease.runtime)]
        );

        assert!(backend
            .remove_client_lease_expiry(&lease.runtime)
            .expect("expiry removal should succeed"));
        assert!(backend
            .list_due_client_lease_expiries(super::now_ms(), 8)
            .expect("due queue should be empty after removal")
            .is_empty());
    }

    #[test]
    fn etcd_backend_point_lookups_only_return_present_leases() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-live-point-lookups")),
        )
        .expect("etcd backend should initialize");

        let active = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&active)
            .expect("active lease should upsert");
        assert_eq!(
            backend
                .get_client_lease(&active.runtime)
                .expect("active lease lookup should succeed"),
            Some(active.clone())
        );
        assert_eq!(
            backend
                .get_live_runtime_by_stable_id(&active.runtime.stable_id)
                .expect("active stable lookup should succeed"),
            Some(active.clone())
        );

        backend
            .update_client_state(&active.runtime, ClientLifecycleState::Draining)
            .expect("client state update should succeed");
        let draining = backend
            .get_client_lease(&active.runtime)
            .expect("draining lease lookup should succeed")
            .expect("draining lease should remain present");
        assert_eq!(draining.state, ClientLifecycleState::Draining);
        assert!(backend
            .get_live_runtime_by_stable_id(&active.runtime.stable_id)
            .expect("draining stable lookup should succeed")
            .is_none());
        let live = backend
            .list_live_clients()
            .expect("live client listing should succeed");
        assert_eq!(live, vec![draining]);
    }

    #[test]
    fn etcd_backend_cleanup_stale_segments_for_owner_is_scoped() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-cleanup-scoped")),
        )
        .expect("etcd backend should initialize");

        let dead_runtime = ClientRuntimeId::new("dead-owner", ClientEpoch(7));
        let live_runtime = ClientRuntimeId::new("live-owner", ClientEpoch(1));
        backend
            .publish_segment(&SegmentAnnouncement {
                owner: dead_runtime.clone(),
                segment_name: SegmentName::new("dead-segment"),
                transport_endpoint: None,
                transport_segment_descriptor: None,
                capacity_bytes: 128,
                used_bytes: 32,
                target_chunks: Vec::new(),
                state: SegmentLifecycleState::Active,
                alignment_bytes: 16,
                tags: vec!["dram".to_string()],
            })
            .expect("dead segment publish should succeed");
        let live_segment = SegmentAnnouncement {
            owner: live_runtime.clone(),
            segment_name: SegmentName::new("live-segment"),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 16,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        backend
            .publish_segment(&live_segment)
            .expect("live segment publish should succeed");

        let report = backend
            .cleanup_stale_segments_for_owner(&dead_runtime)
            .expect("owner cleanup should succeed");
        assert_eq!(report.inspected_segment_keys, 1);
        assert_eq!(report.removed_segment_keys, 1);
        assert_eq!(
            backend
                .list_segments(Some(&live_runtime))
                .expect("segment listing should succeed"),
            vec![live_segment]
        );
    }

    #[test]
    fn etcd_backend_allows_republish_after_previous_owner_unpublished() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/etcd-stale-segment-owner");
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()]).keyspace(keyspace.clone()),
        )
        .expect("etcd backend should initialize");

        let owner_a = ClientRuntimeId::new("writer-a", ClientEpoch(1));
        let owner_b = ClientRuntimeId::new("writer-b", ClientEpoch(1));
        let segment_name = SegmentName::new("shared-segment");
        let segment_a = SegmentAnnouncement {
            owner: owner_a.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec![],
        };
        backend
            .publish_segment(&segment_a)
            .expect("first owner should publish");
        backend
            .unpublish_segment(&owner_a, &segment_name)
            .expect("first owner should unpublish");

        let segment_b = SegmentAnnouncement {
            owner: owner_b,
            segment_name,
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec![],
        };
        backend
            .publish_segment(&segment_b)
            .expect("segment should publish under the new owner");
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
        let missing_scope =
            TenantPolicyScope::new("tenant-missing", None::<String>, None::<String>);
        assert_eq!(
            backend
                .get_tenant_policies(&[scope.clone(), missing_scope.clone()])
                .expect("tenant policy batch read should succeed"),
            vec![Some(policy.clone()), None]
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
        let other_scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let other_policy = TenantPolicy {
            scope: other_scope,
            spec: TenantPolicySpec::default(),
            version: 1,
            updated_at_ms: 30,
            updated_by: "admin".to_string(),
        };
        backend
            .put_tenant_policy(&other_policy, None)
            .expect("other tenant policy insert should succeed");
        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant/a"))
                .expect("tenant-scoped policy listing should succeed"),
            vec![updated.clone()]
        );
        assert_eq!(
            backend
                .list_tenant_policies(Some("tenant-b"))
                .expect("other tenant-scoped policy listing should succeed"),
            vec![other_policy.clone()]
        );
        assert!(backend
            .list_tenant_policies(Some("missing"))
            .expect("missing tenant policy listing should succeed")
            .is_empty());
        assert_eq!(
            backend
                .list_tenant_policies(None)
                .expect("tenant policy listing should succeed"),
            vec![other_policy, updated.clone()]
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
    fn etcd_backend_tenant_eviction_frontier_orders_and_refreshes_candidates() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-tenant-eviction-frontier")),
        )
        .expect("etcd backend should initialize");
        let scope = TenantPolicyScope::new("tenant-evict", None::<String>, None::<String>);
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        let seed = |backend: &EtcdMetadataBackend,
                    reservation_id: &str,
                    key: &str,
                    expected_object_version: Option<u64>,
                    delta_bytes: i64,
                    delta_objects: i64,
                    committed_length: Option<u64>,
                    state: TenantObjectAccountingState,
                    updated_at_ms: u64| {
            backend
                .reserve_tenant_quota(&TenantQuotaReservationRequest {
                    reservation_id: reservation_id.to_string(),
                    scope: scope.clone(),
                    key: ObjectKey::new(key),
                    expected_object_version,
                    delta_bytes,
                    delta_objects,
                    limit: TenantQuotaPolicy {
                        max_bytes: Some(4096),
                        max_objects: Some(64),
                    },
                    expires_at_ms: updated_at_ms + 100,
                    created_at_ms: updated_at_ms,
                    writer_runtime: writer.clone(),
                })
                .expect("reservation should succeed");
            backend
                .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                    reservation_id: reservation_id.to_string(),
                    expected_object_version,
                    committed_length,
                    route_version: None,
                    state,
                    updated_at_ms,
                    updated_by: "writer".to_string(),
                })
                .expect("finalize should succeed");
        };

        seed(
            &backend,
            "resv-alpha",
            "tenant-evict::alpha",
            None,
            10,
            1,
            Some(10),
            TenantObjectAccountingState::Active,
            100,
        );
        seed(
            &backend,
            "resv-beta",
            "tenant-evict::beta",
            None,
            20,
            1,
            Some(20),
            TenantObjectAccountingState::Active,
            100,
        );
        seed(
            &backend,
            "resv-gamma",
            "tenant-evict::gamma",
            None,
            5,
            1,
            Some(5),
            TenantObjectAccountingState::Active,
            90,
        );

        let listed = backend
            .list_tenant_eviction_candidates(&scope, 16)
            .expect("candidate listing should succeed");
        assert_eq!(
            listed
                .iter()
                .map(|object| object.key.0.as_str())
                .collect::<Vec<_>>(),
            vec![
                "tenant-evict::gamma",
                "tenant-evict::beta",
                "tenant-evict::alpha"
            ]
        );

        seed(
            &backend,
            "resv-beta-refresh",
            "tenant-evict::beta",
            Some(1),
            5,
            0,
            Some(25),
            TenantObjectAccountingState::Active,
            130,
        );
        let refreshed = backend
            .list_tenant_eviction_candidates(&scope, 16)
            .expect("candidate listing after refresh should succeed");
        assert_eq!(
            refreshed
                .iter()
                .map(|object| (
                    object.key.0.as_str(),
                    object.updated_at_ms,
                    object.committed_length
                ))
                .collect::<Vec<_>>(),
            vec![
                ("tenant-evict::gamma", 90, 5),
                ("tenant-evict::alpha", 100, 10),
                ("tenant-evict::beta", 130, 25),
            ]
        );

        seed(
            &backend,
            "resv-gamma-delete",
            "tenant-evict::gamma",
            Some(1),
            -5,
            -1,
            None,
            TenantObjectAccountingState::Deleted,
            140,
        );
        let after_delete = backend
            .list_tenant_eviction_candidates(&scope, 16)
            .expect("candidate listing after delete should succeed");
        assert_eq!(
            after_delete
                .iter()
                .map(|object| object.key.0.as_str())
                .collect::<Vec<_>>(),
            vec!["tenant-evict::alpha", "tenant-evict::beta"]
        );
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
        assert!(matches!(
            byte_limit,
            StoreError::QuotaExceeded {
                kind: QuotaKind::Bytes,
                ..
            }
        ));

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
        assert!(matches!(
            object_limit,
            StoreError::QuotaExceeded {
                kind: QuotaKind::Objects,
                ..
            }
        ));

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

    #[test]
    fn etcd_backend_tenant_quota_allows_non_positive_deltas_when_usage_is_already_over_limit() {
        let Some(server) = EtcdTestServer::start() else {
            return;
        };
        let backend = EtcdMetadataBackend::from_config(
            EtcdMetadataConfig::new([server.endpoint()])
                .keyspace(MetadataKeyspace::new("test/etcd-tenant-quota-nonpositive")),
        )
        .expect("etcd backend should initialize");
        let scope = TenantPolicyScope::new("tenant-nonpositive", None::<String>, None::<String>);
        let key = ObjectKey::new("tenant-nonpositive::alpha");
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-seed".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 100,
                created_at_ms: 10,
                writer_runtime: writer.clone(),
            })
            .expect("seed reservation should succeed");
        backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-seed".to_string(),
                expected_object_version: None,
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 20,
                updated_by: "writer".to_string(),
            })
            .expect("seed finalize should succeed");

        let zero_delta = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-zero".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: 0,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(16),
                    max_objects: Some(0),
                },
                expires_at_ms: 200,
                created_at_ms: 30,
                writer_runtime: writer.clone(),
            })
            .expect("zero-delta reservation should stay admissible while over limit");
        assert_eq!(zero_delta.quota.used_bytes, 32);
        assert_eq!(zero_delta.quota.used_objects, 1);
        assert_eq!(zero_delta.quota.pending_reserved_bytes, 0);
        assert_eq!(zero_delta.quota.pending_reserved_objects, 0);
        backend
            .abort_tenant_quota("resv-zero")
            .expect("zero-delta abort should succeed");

        let negative_delta = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-delete".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: -32,
                delta_objects: -1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(16),
                    max_objects: Some(0),
                },
                expires_at_ms: 260,
                created_at_ms: 40,
                writer_runtime: writer,
            })
            .expect("negative-delta reservation should stay admissible while over limit");
        assert_eq!(negative_delta.quota.used_bytes, 32);
        assert_eq!(negative_delta.quota.used_objects, 1);
        assert_eq!(negative_delta.quota.pending_reserved_bytes, 0);
        assert_eq!(negative_delta.quota.pending_reserved_objects, 0);
        let deleted = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-delete".to_string(),
                expected_object_version: Some(1),
                committed_length: None,
                route_version: None,
                state: TenantObjectAccountingState::Deleted,
                updated_at_ms: 50,
                updated_by: "writer".to_string(),
            })
            .expect("negative-delta finalize should succeed");
        assert_eq!(deleted.quota.used_bytes, 0);
        assert_eq!(deleted.quota.used_objects, 0);
    }
}
