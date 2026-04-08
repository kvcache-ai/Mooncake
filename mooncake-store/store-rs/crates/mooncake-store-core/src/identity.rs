use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::route::SegmentName;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ClientStableId(pub String);

impl ClientStableId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Display for ClientStableId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(
    Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize,
)]
pub struct ClientEpoch(pub u64);

impl ClientEpoch {
    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ClientRuntimeId {
    pub stable_id: ClientStableId,
    pub epoch: ClientEpoch,
}

impl ClientRuntimeId {
    pub fn new(stable_id: impl Into<String>, epoch: ClientEpoch) -> Self {
        Self {
            stable_id: ClientStableId::new(stable_id),
            epoch,
        }
    }

    pub fn storage_key(&self) -> String {
        format!("{}:{}", self.stable_id, self.epoch.0)
    }
}

impl Display for ClientRuntimeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.storage_key())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientEndpointSet {
    pub rpc_address: String,
    pub segment_name: Option<SegmentName>,
    pub labels: BTreeMap<String, String>,
}
