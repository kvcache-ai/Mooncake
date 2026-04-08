use crate::error::Result;
use crate::identity::{ClientRuntimeId, ClientStableId};
use crate::lifecycle::{ClientLifecycleState, HandoffPlan};
use crate::route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, RouteVersion, SegmentAnnouncement, SegmentName,
};

pub trait MetadataBackend: Send + Sync {
    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()>;

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()>;

    fn list_live_clients(&self) -> Result<Vec<ClientLease>>;

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()>;

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()>;

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>>;

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()>;

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>>;
}

pub trait PlacementStrategy: Send + Sync {
    fn select_write_targets(
        &self,
        local: &ClientRuntimeId,
        live_clients: &[ClientLease],
        replica_count: usize,
    ) -> Result<Vec<ClientRuntimeId>>;
}
