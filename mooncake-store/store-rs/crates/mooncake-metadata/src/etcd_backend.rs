use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy, RoutePolicyDomain, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
    TenantPolicy, TenantPolicyScope,
};

use crate::keyspace::{parse_route_policy_domain, parse_tenant_policy_scope};
use crate::segment_state::StoredSegmentState;
use crate::MetadataKeyspace;

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
        SegmentAnnouncement, SegmentLifecycleState, SegmentName, StoreError, TenantPolicy,
        TenantPolicyScope, TenantPolicySpec, TenantQuotaPolicy,
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
}
