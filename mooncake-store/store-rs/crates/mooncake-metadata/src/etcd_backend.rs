use std::time::{SystemTime, UNIX_EPOCH};

use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RouteVersion, SegmentAnnouncement, SegmentName,
    SegmentReservation, StoreError,
};

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

    fn block_on<T>(
        &self,
        work: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        self.runtime.block_on(work)
    }

    async fn client(&self) -> Result<Client> {
        Client::connect(self.config.endpoints.clone(), None)
            .await
            .map_err(etcd_error("etcd connect"))
    }
}

impl MetadataBackend for EtcdMetadataBackend {
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
            let mut lease: ClientLease =
                serde_json::from_slice(kv.value()).map_err(json_error)?;
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
        let prefix = self.config.keyspace.client_pattern().trim_end_matches('*').to_string();
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
        let key = self.config.keyspace.segment(&segment.owner, &segment.segment_name);
        let payload = serde_json::to_string(segment).map_err(json_error)?;
        self.block_on(async {
            let mut client = self.client().await?;
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

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
        alignment: u64,
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
                let mut announcement: SegmentAnnouncement =
                    serde_json::from_slice(kv.value()).map_err(json_error)?;
                let offset = align_up_u64(announcement.used_bytes, alignment.max(1));
                let next_used = offset.checked_add(length_bytes).ok_or_else(|| {
                    StoreError::Allocator("segment reservation overflow".to_string())
                })?;
                if next_used > announcement.capacity_bytes {
                    return Err(StoreError::Allocator(format!(
                        "segment capacity exhausted for {}:{} requested={} remaining={}",
                        owner,
                        segment.0,
                        length_bytes,
                        announcement.capacity_bytes.saturating_sub(offset)
                    )));
                }
                announcement.used_bytes = next_used;
                let payload = serde_json::to_string(&announcement).map_err(json_error)?;
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
                    return Ok(SegmentReservation {
                        owner: owner.clone(),
                        segment_name: segment.clone(),
                        offset_bytes: offset,
                        length_bytes,
                    });
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
                    let compare = Compare::mod_revision(
                        key.clone(),
                        CompareOp::Equal,
                        kv.mod_revision(),
                    );
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
                            serde_json::to_string(next.expect("checked above")).map_err(json_error)?,
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

fn align_up_u64(value: u64, alignment: u64) -> u64 {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

fn etcd_error(operation: &'static str) -> impl FnOnce(etcd_client::Error) -> StoreError {
    move |error| StoreError::Metadata(format!("{operation}: {error}"))
}

fn json_error(error: serde_json::Error) -> StoreError {
    StoreError::Metadata(format!("json serialization: {error}"))
}
