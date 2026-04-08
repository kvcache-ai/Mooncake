use serde::{Deserialize, Serialize};

use crate::identity::{ClientRuntimeId, ClientStableId};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClientLifecycleState {
    Standby,
    Active,
    Draining,
    Sealed,
    Offline,
}

impl ClientLifecycleState {
    pub fn serves_reads(self) -> bool {
        matches!(self, Self::Standby | Self::Active | Self::Draining)
    }

    pub fn allows_new_writes(self) -> bool {
        matches!(self, Self::Active)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HandoffKind {
    HotUpgrade,
    HotStandbyPromotion,
    GracefulDrain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HandoffPlan {
    pub stable_id: ClientStableId,
    pub from: ClientRuntimeId,
    pub to: ClientRuntimeId,
    pub kind: HandoffKind,
    pub barrier_version: u64,
    pub created_at_ms: u64,
    pub deadline_ms: Option<u64>,
}
