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

#[cfg(test)]
mod tests {
    use super::{ClientLifecycleState, HandoffKind};

    #[test]
    fn lifecycle_state_flags_match_contract() {
        assert!(ClientLifecycleState::Standby.serves_reads());
        assert!(ClientLifecycleState::Active.serves_reads());
        assert!(ClientLifecycleState::Draining.serves_reads());
        assert!(!ClientLifecycleState::Sealed.serves_reads());
        assert!(!ClientLifecycleState::Offline.serves_reads());

        assert!(!ClientLifecycleState::Standby.allows_new_writes());
        assert!(ClientLifecycleState::Active.allows_new_writes());
        assert!(!ClientLifecycleState::Draining.allows_new_writes());
        assert!(!ClientLifecycleState::Sealed.allows_new_writes());
        assert!(!ClientLifecycleState::Offline.allows_new_writes());
    }

    #[test]
    fn handoff_kind_variants_are_distinct() {
        assert_ne!(HandoffKind::HotUpgrade, HandoffKind::HotStandbyPromotion);
        assert_ne!(HandoffKind::HotUpgrade, HandoffKind::GracefulDrain);
        assert_ne!(HandoffKind::HotStandbyPromotion, HandoffKind::GracefulDrain);
    }
}
