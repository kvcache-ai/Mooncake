use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend, ObjectKey,
    ObjectRoute, Result, RouteVersion, SegmentAnnouncement, SegmentName, StoreError,
};

pub struct StoreClientBuilder {
    metadata: Arc<dyn MetadataBackend>,
    stable_id: ClientStableId,
    epoch: ClientEpoch,
    compatibility: CompatibilityDescriptor,
    endpoints: ClientEndpointSet,
    initial_state: ClientLifecycleState,
}

impl StoreClientBuilder {
    pub fn new(metadata: Arc<dyn MetadataBackend>, stable_id: impl Into<String>) -> Self {
        Self {
            metadata,
            stable_id: ClientStableId::new(stable_id),
            epoch: ClientEpoch(1),
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            initial_state: ClientLifecycleState::Standby,
        }
    }

    pub fn epoch(mut self, epoch: ClientEpoch) -> Self {
        self.epoch = epoch;
        self
    }

    pub fn compatibility(mut self, compatibility: CompatibilityDescriptor) -> Self {
        self.compatibility = compatibility;
        self
    }

    pub fn rpc_address(mut self, rpc_address: impl Into<String>) -> Self {
        self.endpoints.rpc_address = rpc_address.into();
        self
    }

    pub fn segment_name(mut self, segment_name: impl Into<String>) -> Self {
        self.endpoints.segment_name = Some(SegmentName::new(segment_name));
        self
    }

    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.endpoints.labels.insert(key.into(), value.into());
        self
    }

    pub fn state(mut self, state: ClientLifecycleState) -> Self {
        self.initial_state = state;
        self
    }

    pub fn build(self, expires_at_ms: u64) -> Result<StoreClient> {
        let runtime = ClientRuntimeId {
            stable_id: self.stable_id,
            epoch: self.epoch,
        };
        let lease = ClientLease {
            runtime: runtime.clone(),
            state: self.initial_state,
            compatibility: self.compatibility,
            endpoints: self.endpoints,
            expires_at_ms,
        };
        self.metadata.upsert_client_lease(&lease)?;
        Ok(StoreClient {
            metadata: self.metadata,
            lease,
        })
    }
}

pub trait MooncakeCompatibilityFacade {
    fn heartbeat(&mut self, expires_at_ms: u64) -> Result<()>;
    fn enter_standby(&mut self) -> Result<()>;
    fn activate(&mut self) -> Result<()>;
    fn enter_draining(&mut self) -> Result<()>;
    fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan>;
    fn mount_segment(&self, capacity_bytes: u64, used_bytes: u64, tags: Vec<String>) -> Result<()>;
    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>>;
    fn cas_route(
        &self,
        key: &str,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;
    fn register_local_memory(&self) -> Result<()>;
    fn put(&self, _key: &str) -> Result<()> {
        Err(StoreError::Unsupported(
            "data path is staged for the transport phase".to_string(),
        ))
    }
    fn get(&self, _key: &str) -> Result<()> {
        Err(StoreError::Unsupported(
            "data path is staged for the transport phase".to_string(),
        ))
    }
}

pub struct StoreClient {
    metadata: Arc<dyn MetadataBackend>,
    lease: ClientLease,
}

impl StoreClient {
    pub fn runtime_id(&self) -> &ClientRuntimeId {
        &self.lease.runtime
    }

    pub fn lease(&self) -> &ClientLease {
        &self.lease
    }
}

impl MooncakeCompatibilityFacade for StoreClient {
    fn heartbeat(&mut self, expires_at_ms: u64) -> Result<()> {
        self.lease.expires_at_ms = expires_at_ms;
        self.metadata.upsert_client_lease(&self.lease)
    }

    fn enter_standby(&mut self) -> Result<()> {
        self.lease.state = ClientLifecycleState::Standby;
        self.metadata
            .update_client_state(&self.lease.runtime, self.lease.state)
    }

    fn activate(&mut self) -> Result<()> {
        self.lease.state = ClientLifecycleState::Active;
        self.metadata
            .update_client_state(&self.lease.runtime, self.lease.state)
    }

    fn enter_draining(&mut self) -> Result<()> {
        self.lease.state = ClientLifecycleState::Draining;
        self.metadata
            .update_client_state(&self.lease.runtime, self.lease.state)
    }

    fn plan_handoff(
        &self,
        successor_epoch: ClientEpoch,
        kind: HandoffKind,
        barrier_version: u64,
        created_at_ms: u64,
        deadline_ms: Option<u64>,
    ) -> Result<HandoffPlan> {
        let plan = HandoffPlan {
            stable_id: self.lease.runtime.stable_id.clone(),
            from: self.lease.runtime.clone(),
            to: ClientRuntimeId {
                stable_id: self.lease.runtime.stable_id.clone(),
                epoch: successor_epoch,
            },
            kind,
            barrier_version,
            created_at_ms,
            deadline_ms,
        };
        self.metadata.put_handoff(&plan)?;
        Ok(plan)
    }

    fn mount_segment(&self, capacity_bytes: u64, used_bytes: u64, tags: Vec<String>) -> Result<()> {
        let Some(segment_name) = self.lease.endpoints.segment_name.clone() else {
            return Err(StoreError::InvalidState(
                "segment_name must be configured before publishing a segment".to_string(),
            ));
        };
        let segment = SegmentAnnouncement {
            owner: self.lease.runtime.clone(),
            segment_name,
            capacity_bytes,
            used_bytes,
            tags,
        };
        self.metadata.publish_segment(&segment)
    }

    fn query_route(&self, key: &str) -> Result<Option<ObjectRoute>> {
        self.metadata.get_object_route(&ObjectKey::new(key))
    }

    fn cas_route(
        &self,
        key: &str,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.metadata
            .compare_and_swap_object_route(&ObjectKey::new(key), expected, next)
    }

    fn register_local_memory(&self) -> Result<()> {
        Err(StoreError::Unsupported(
            "local memory registration is staged for the transport phase".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{ClientEpoch, ClientLifecycleState, HandoffKind, MetadataBackend};

    use crate::{MooncakeCompatibilityFacade, StoreClientBuilder};

    #[test]
    fn hot_upgrade_handoff_is_published_after_draining() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let mut client = StoreClientBuilder::new(metadata.clone(), "client-a")
            .epoch(ClientEpoch(1))
            .state(ClientLifecycleState::Active)
            .rpc_address("127.0.0.1:7001")
            .segment_name("client-a-segment")
            .build(10_000)
            .expect("client build should succeed");

        client.enter_draining().expect("draining should succeed");
        let handoff = client
            .plan_handoff(ClientEpoch(2), HandoffKind::HotUpgrade, 7, 100, Some(1_000))
            .expect("handoff planning should succeed");

        let leases = metadata.list_live_clients().expect("list clients should succeed");
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].state, ClientLifecycleState::Draining);

        let stored = metadata
            .get_handoff(&handoff.stable_id)
            .expect("get handoff should succeed")
            .expect("handoff should exist");
        assert_eq!(stored.from.epoch, ClientEpoch(1));
        assert_eq!(stored.to.epoch, ClientEpoch(2));
        assert_eq!(stored.kind, HandoffKind::HotUpgrade);
    }
}
