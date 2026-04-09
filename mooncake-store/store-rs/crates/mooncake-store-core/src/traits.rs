use crate::error::Result;
use crate::identity::{ClientRuntimeId, ClientStableId};
use crate::lifecycle::{ClientLifecycleState, HandoffPlan};
use crate::route::{
    CasResult, ClientLease, ObjectKey, ObjectRoute, RouteCasRequest, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation,
};

pub trait MetadataBackend: Send + Sync {
    fn route_namespace(&self) -> String;

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()>;

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()>;

    fn list_live_clients(&self) -> Result<Vec<ClientLease>>;

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()>;

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()>;

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

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

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
}

pub trait PlacementStrategy: Send + Sync {
    fn select_write_targets(
        &self,
        local: &ClientRuntimeId,
        live_clients: &[ClientLease],
        replica_count: usize,
    ) -> Result<Vec<ClientRuntimeId>>;
}
