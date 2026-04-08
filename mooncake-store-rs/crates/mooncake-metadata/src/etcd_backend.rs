use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RouteVersion, SegmentAnnouncement, SegmentName,
    StoreError,
};

#[derive(Clone, Debug, Default)]
pub struct EtcdMetadataBackend;

impl EtcdMetadataBackend {
    pub fn new() -> Self {
        Self
    }
}

impl MetadataBackend for EtcdMetadataBackend {
    fn upsert_client_lease(&self, _lease: &ClientLease) -> Result<()> {
        unsupported()
    }

    fn update_client_state(
        &self,
        _runtime: &ClientRuntimeId,
        _next: ClientLifecycleState,
    ) -> Result<()> {
        unsupported()
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        unsupported()
    }

    fn publish_segment(&self, _segment: &SegmentAnnouncement) -> Result<()> {
        unsupported()
    }

    fn unpublish_segment(&self, _owner: &ClientRuntimeId, _segment: &SegmentName) -> Result<()> {
        unsupported()
    }

    fn get_object_route(&self, _key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        unsupported()
    }

    fn compare_and_swap_object_route(
        &self,
        _key: &ObjectKey,
        _expected: Option<RouteVersion>,
        _next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        unsupported()
    }

    fn put_handoff(&self, _handoff: &HandoffPlan) -> Result<()> {
        unsupported()
    }

    fn get_handoff(&self, _stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        unsupported()
    }
}

fn unsupported<T>() -> Result<T> {
    Err(StoreError::Unsupported(
        "etcd backend is reserved for a later phase".to_string(),
    ))
}
