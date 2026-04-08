use std::collections::BTreeMap;

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RouteVersion, SegmentAnnouncement, SegmentName,
    SegmentReservation, StoreError,
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

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
        alignment: u64,
    ) -> Result<SegmentReservation> {
        let mut state = self.state.write();
        let key = Self::segment_key(owner, segment);
        let announcement = state
            .segments
            .get_mut(&key)
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        let offset = align_up_u64(announcement.used_bytes, alignment.max(1));
        let next_used = offset
            .checked_add(length_bytes)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
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
        Ok(SegmentReservation {
            owner: owner.clone(),
            segment_name: segment.clone(),
            offset_bytes: offset,
            length_bytes,
        })
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

fn align_up_u64(value: u64, alignment: u64) -> u64 {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

#[cfg(test)]
mod tests {
    use mooncake_store_core::{
        ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
        CompatibilityDescriptor, MetadataBackend, SegmentAnnouncement, SegmentName,
    };

    use super::InMemoryMetadataBackend;

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
                expires_at_ms: 10_000,
            })
            .expect("lease should upsert");
        metadata
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                capacity_bytes: 1024,
                used_bytes: 0,
                tags: vec![],
            })
            .expect("segment should publish");

        let first = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-a"), 17, 64)
            .expect("first reserve should work");
        let second = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-a"), 17, 64)
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
                capacity_bytes: 32,
                used_bytes: 0,
                tags: vec![],
            })
            .expect("segment should publish");

        let error = metadata
            .reserve_segment(&owner, &SegmentName::new("seg-b"), 64, 1)
            .expect_err("reserve should fail");
        assert!(error.to_string().contains("segment capacity exhausted"));
    }
}
