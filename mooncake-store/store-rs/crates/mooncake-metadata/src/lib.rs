use std::collections::BTreeMap;

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, MetadataBackend,
    ObjectKey, ObjectRoute, Result, RouteVersion, SegmentAnnouncement, SegmentName, StoreError,
    HandoffPlan,
};
use parking_lot::RwLock;

#[derive(Default)]
struct InMemoryState {
    clients: BTreeMap<String, ClientLease>,
    handoffs: BTreeMap<String, HandoffPlan>,
    objects: BTreeMap<String, ObjectRoute>,
    segments: BTreeMap<String, SegmentAnnouncement>,
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
}

impl MetadataBackend for InMemoryMetadataBackend {
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
        self.state.write().segments.insert(
            Self::segment_key(&segment.owner, &segment.segment_name),
            segment.clone(),
        );
        Ok(())
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        self.state
            .write()
            .segments
            .remove(&Self::segment_key(owner, segment));
        Ok(())
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        Ok(self.state.read().objects.get(&key.0).cloned())
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
