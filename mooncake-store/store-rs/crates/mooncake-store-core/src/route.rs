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

fn default_segment_alignment_bytes() -> u64 {
    1
}
