use serde::{Deserialize, Serialize};

use crate::identity::ClientRuntimeId;

/// Lifecycle of a NoF object reference stored in Mooncake external metadata.
///
/// This is intentionally separate from `ColdBackingState`: a NoF target is not
/// a local Cold Tier disk and must never be selected by local-disk maintenance.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum NofBackingState {
    #[default]
    PendingWrite,
    Materialized,
    PendingDelete,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NofBackingRouteFilter {
    pub target_id: Option<String>,
    pub state: Option<NofBackingState>,
    pub owner: Option<ClientRuntimeId>,
    pub limit: Option<usize>,
}

/// External logical metadata for an object stored through a NoF low-level executor.
///
/// `target_id` resolves a configured NoF target. It is deliberately not named
/// `cold_tier_id`, because provider-managed remote storage is not a local disk.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NofBackingRoute {
    pub owner: ClientRuntimeId,
    pub target_id: String,
    pub object_locator: String,
    pub length: u64,
    pub checksum: Option<u64>,
    #[serde(default)]
    pub state: NofBackingState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<NofBackingReplica>,
}

impl NofBackingRoute {
    pub fn all_target_ids(&self) -> Vec<&str> {
        let mut ids = vec![self.target_id.as_str()];
        ids.extend(
            self.replicas
                .iter()
                .map(|replica| replica.target_id.as_str()),
        );
        ids
    }

    pub fn all_targets(&self) -> Vec<NofTarget<'_>> {
        let mut targets = vec![NofTarget {
            owner: &self.owner,
            target_id: &self.target_id,
            object_locator: &self.object_locator,
        }];
        targets.extend(self.replicas.iter().map(|replica| NofTarget {
            owner: &replica.owner,
            target_id: &replica.target_id,
            object_locator: &replica.object_locator,
        }));
        targets
    }

    pub fn matches_filter(&self, filter: &NofBackingRouteFilter) -> bool {
        if filter.state.is_some_and(|state| self.state != state) {
            return false;
        }
        self.all_targets().into_iter().any(|target| {
            filter
                .target_id
                .as_deref()
                .is_none_or(|target_id| target.target_id == target_id)
                && filter
                    .owner
                    .as_ref()
                    .is_none_or(|owner| target.owner == owner)
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NofBackingReplica {
    pub owner: ClientRuntimeId,
    pub target_id: String,
    pub object_locator: String,
}

pub struct NofTarget<'a> {
    pub owner: &'a ClientRuntimeId,
    pub target_id: &'a str,
    pub object_locator: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientEpoch, ClientStableId};

    fn owner(name: &str) -> ClientRuntimeId {
        ClientRuntimeId {
            stable_id: ClientStableId::new(name),
            epoch: ClientEpoch(1),
        }
    }

    #[test]
    fn nof_targets_do_not_use_cold_tier_ids() {
        let route = NofBackingRoute {
            owner: owner("primary"),
            target_id: "nof-a".to_string(),
            object_locator: "nof-ll:v1:i:01".to_string(),
            length: 1,
            checksum: None,
            state: NofBackingState::Materialized,
            replicas: vec![NofBackingReplica {
                owner: owner("replica"),
                target_id: "nof-b".to_string(),
                object_locator: "nof-ll:v1:i:02".to_string(),
            }],
        };

        assert_eq!(route.all_target_ids(), vec!["nof-a", "nof-b"]);
        assert_eq!(route.all_targets()[1].object_locator, "nof-ll:v1:i:02");
        assert!(route.matches_filter(&NofBackingRouteFilter {
            target_id: Some("nof-b".to_string()),
            owner: Some(owner("replica")),
            ..NofBackingRouteFilter::default()
        }));
        assert!(!route.matches_filter(&NofBackingRouteFilter {
            target_id: Some("nof-b".to_string()),
            owner: Some(owner("primary")),
            ..NofBackingRouteFilter::default()
        }));
    }
}
