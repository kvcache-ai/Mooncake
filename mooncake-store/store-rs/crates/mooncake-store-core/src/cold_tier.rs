use serde::{Deserialize, Serialize};

use crate::identity::ClientRuntimeId;

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ColdBackingState {
    #[default]
    PendingOffload,
    Materialized,
    PendingDelete,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColdBackingRouteFilter {
    pub device_id: Option<String>,
    pub state: Option<ColdBackingState>,
    pub owner: Option<ClientRuntimeId>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdBackingRoute {
    pub owner: ClientRuntimeId,
    /// Registered cold tier device key used for backend lookup.
    ///
    /// The field name is kept for route compatibility; bootstrap/Admin create currently set
    /// `device_id == cold_tier_id`, so this carries the device id in persisted routes.
    pub cold_tier_id: String,
    pub object_locator: String,
    pub length: u64,
    pub checksum: Option<u64>,
    #[serde(default)]
    pub state: ColdBackingState,
    /// Additional replicas on other cold-tier devices.  When non-empty, the
    /// restore path can choose among `cold_tier_id` (primary) and each replica
    /// to balance read load across devices.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<ColdBackingReplica>,
}

impl ColdBackingRoute {
    /// Returns the cold-tier device IDs for all available copies of this object
    /// (primary first, then replicas).
    pub fn all_cold_tier_ids(&self) -> Vec<&str> {
        let mut ids = vec![self.cold_tier_id.as_str()];
        for replica in &self.replicas {
            ids.push(replica.cold_tier_id.as_str());
        }
        ids
    }

    /// Returns all read targets (primary + replicas) with full routing info.
    pub fn all_targets(&self) -> Vec<ColdTierTarget<'_>> {
        let mut targets = vec![ColdTierTarget {
            owner: &self.owner,
            cold_tier_id: &self.cold_tier_id,
            object_locator: &self.object_locator,
        }];
        for replica in &self.replicas {
            targets.push(ColdTierTarget {
                owner: &replica.owner,
                cold_tier_id: &replica.cold_tier_id,
                object_locator: &replica.object_locator,
            });
        }
        targets
    }
}

/// A replica of a cold-backed object stored on a different device (possibly on
/// a different node).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdBackingReplica {
    /// The node that owns this replica's storage.  Same as the primary
    /// `ColdBackingRoute::owner` for local-only replicas; differs for
    /// cross-machine replicas.
    pub owner: ClientRuntimeId,
    pub cold_tier_id: String,
    pub object_locator: String,
}

/// A potential read target for a cold-backed object (primary or replica).
pub struct ColdTierTarget<'a> {
    pub owner: &'a ClientRuntimeId,
    pub cold_tier_id: &'a str,
    pub object_locator: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColdTierDeviceState {
    Unregistered,
    Healthy,
    Full,
    DisabledByAdmin,
    Draining,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ColdTierTargetSpec {
    Directory { path: String },
    Uuid { uuid: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ColdTierDeviceRecord {
    pub device_id: String,
    pub stable_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    pub cold_tier_id: String,
    pub kind: String,
    pub target: ColdTierTargetSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_dir: Option<String>,
    pub state: ColdTierDeviceState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_bytes: Option<u64>,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub reserved_bytes: u64,
    #[serde(default)]
    pub failure_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::route::deserialize_string_vec_or_object"
    )]
    pub tags: Vec<String>,
    #[serde(default)]
    pub updated_at_ms: u64,
}

impl ColdTierDeviceRecord {
    pub fn schedulable(&self) -> bool {
        self.state == ColdTierDeviceState::Healthy
            && self
                .capacity_bytes
                .map(|capacity| self.used_bytes.saturating_add(self.reserved_bytes) < capacity)
                .unwrap_or(true)
    }

    pub fn free_bytes(&self) -> Option<u64> {
        self.capacity_bytes.map(|capacity| {
            capacity.saturating_sub(self.used_bytes.saturating_add(self.reserved_bytes))
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColdTierDeviceUpdate {
    pub expected_updated_at_ms: Option<u64>,
    pub stable_id: Option<String>,
    pub cold_tier_id: Option<String>,
    pub kind: Option<String>,
    pub target: Option<ColdTierTargetSpec>,
    pub epoch: Option<Option<u64>>,
    pub root_dir: Option<Option<String>>,
    pub state: Option<ColdTierDeviceState>,
    pub capacity_bytes: Option<Option<u64>>,
    pub used_bytes: Option<u64>,
    pub reserved_bytes: Option<u64>,
    pub failure_count: Option<u64>,
    pub last_error: Option<Option<String>>,
    pub tags: Option<Vec<String>>,
    pub updated_at_ms: u64,
}

impl ColdTierDeviceUpdate {
    pub fn new(updated_at_ms: u64) -> Self {
        Self {
            expected_updated_at_ms: None,
            stable_id: None,
            cold_tier_id: None,
            kind: None,
            target: None,
            epoch: None,
            root_dir: None,
            state: None,
            capacity_bytes: None,
            used_bytes: None,
            reserved_bytes: None,
            failure_count: None,
            last_error: None,
            tags: None,
            updated_at_ms,
        }
    }

    pub fn apply(self, record: &mut ColdTierDeviceRecord) {
        if let Some(stable_id) = self.stable_id {
            record.stable_id = stable_id;
        }
        if let Some(cold_tier_id) = self.cold_tier_id {
            record.cold_tier_id = cold_tier_id;
        }
        if let Some(kind) = self.kind {
            record.kind = kind;
        }
        if let Some(target) = self.target {
            record.target = target;
        }
        if let Some(epoch) = self.epoch {
            record.epoch = epoch;
        }
        if let Some(root_dir) = self.root_dir {
            record.root_dir = root_dir;
        }
        if let Some(state) = self.state {
            record.state = state;
        }
        if let Some(capacity_bytes) = self.capacity_bytes {
            record.capacity_bytes = capacity_bytes;
        }
        if let Some(used_bytes) = self.used_bytes {
            record.used_bytes = used_bytes;
        }
        if let Some(reserved_bytes) = self.reserved_bytes {
            record.reserved_bytes = reserved_bytes;
        }
        if let Some(failure_count) = self.failure_count {
            record.failure_count = failure_count;
        }
        if let Some(last_error) = self.last_error {
            record.last_error = last_error;
        }
        if let Some(tags) = self.tags {
            record.tags = tags;
        }
        record.updated_at_ms = self.updated_at_ms;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColdTierPutDeviceResult {
    Created(ColdTierDeviceRecord),
    Existing(ColdTierDeviceRecord),
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ColdTierDeviceFilter {
    pub stable_id: Option<String>,
    pub state: Option<ColdTierDeviceState>,
    pub schedulable: Option<bool>,
    pub kind: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ColdTierUsageDelta {
    pub used_bytes: i64,
    pub reserved_bytes: i64,
}
