use serde::{Deserialize, Serialize};

use crate::compat::CompatibilityDescriptor;
use crate::identity::{ClientEndpointSet, ClientRuntimeId};
use crate::lifecycle::ClientLifecycleState;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ObjectKey(pub String);

impl ObjectKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct SegmentName(pub String);

impl SegmentName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct RouteVersion(pub u64);

impl RouteVersion {
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RouteState {
    Active,
    Deleting,
    Tombstone,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicaTier {
    Dram,
    Nvme,
    File,
    Unknown,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum SegmentLifecycleState {
    #[default]
    Active,
    Draining,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaRoute {
    pub owner: ClientRuntimeId,
    pub segment_name: SegmentName,
    pub offset: u64,
    #[serde(default)]
    pub segment_offset: u64,
    pub length: u64,
    pub checksum: Option<u64>,
    pub tier: ReplicaTier,
    pub priority: u16,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectRoute {
    pub key: ObjectKey,
    pub version: RouteVersion,
    pub state: RouteState,
    pub compatibility: CompatibilityDescriptor,
    pub replicas: Vec<ReplicaRoute>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientLease {
    pub runtime: ClientRuntimeId,
    pub state: ClientLifecycleState,
    pub compatibility: CompatibilityDescriptor,
    pub endpoints: ClientEndpointSet,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentAnnouncement {
    pub owner: ClientRuntimeId,
    pub segment_name: SegmentName,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    #[serde(default)]
    pub state: SegmentLifecycleState,
    #[serde(default = "default_segment_alignment_bytes")]
    pub alignment_bytes: u64,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SegmentReservation {
    pub owner: ClientRuntimeId,
    pub segment_name: SegmentName,
    pub offset_bytes: u64,
    pub length_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CasResult {
    pub applied: bool,
    pub current: Option<ObjectRoute>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RouteCasRequest {
    pub key: ObjectKey,
    pub expected: Option<RouteVersion>,
    pub next: Option<ObjectRoute>,
}

fn default_segment_alignment_bytes() -> u64 {
    1
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        default_segment_alignment_bytes, ObjectKey, RouteVersion, SegmentAnnouncement,
        SegmentLifecycleState, SegmentName,
    };
    use crate::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor,
    };

    #[test]
    fn object_and_segment_name_helpers_wrap_strings_directly() {
        assert_eq!(ObjectKey::new("alpha").0, "alpha");
        assert_eq!(SegmentName::new("segment-a").0, "segment-a");
    }

    #[test]
    fn route_version_next_saturates_at_max_value() {
        assert_eq!(RouteVersion(9).next(), RouteVersion(10));
        assert_eq!(RouteVersion(u64::MAX).next(), RouteVersion(u64::MAX));
    }

    #[test]
    fn segment_announcement_defaults_state_and_alignment_for_serde() {
        let announcement: SegmentAnnouncement = serde_json::from_value(json!({
            "owner": {
                "stable_id": "runtime-a",
                "epoch": 1
            },
            "segment_name": "segment-a",
            "capacity_bytes": 4096,
            "used_bytes": 1024,
            "tags": ["storage"]
        }))
        .expect("announcement should deserialize");
        assert_eq!(announcement.state, SegmentLifecycleState::Active);
        assert_eq!(
            announcement.alignment_bytes,
            default_segment_alignment_bytes()
        );
    }

    #[test]
    fn client_lease_round_trip_keeps_runtime_contract() {
        let lease = ClientLease {
            runtime: ClientRuntimeId::new("runtime-a", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: 42,
        };
        let encoded = serde_json::to_value(&lease).expect("lease should serialize");
        let decoded: ClientLease =
            serde_json::from_value(encoded).expect("lease should deserialize");
        assert_eq!(
            decoded.runtime,
            ClientRuntimeId::new("runtime-a", ClientEpoch(1))
        );
        assert_eq!(decoded.state, ClientLifecycleState::Active);
        assert_eq!(decoded.expires_at_ms, 42);
    }
}
