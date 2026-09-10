use serde::{Deserialize, Serialize};

use crate::cold_tier::{ColdBackingReplica, ColdBackingRoute, ColdBackingState};
use crate::identity::ClientRuntimeId;

/// Placement metadata for a Mooncake-managed NoF object.
///
/// The locator is opaque to Mooncake. `target_id` is the stable target identity from the shared
/// Cold Tier registration catalog; the executor decides how the locator maps to physical storage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NofBackingRoute {
    pub target_id: String,
    pub owner: ClientRuntimeId,
    pub object_locator: String,
    pub length: u64,
    pub checksum: Option<u64>,
    #[serde(default)]
    pub state: ColdBackingState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replicas: Vec<NofBackingReplica>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NofBackingReplica {
    pub target_id: String,
    pub owner: ClientRuntimeId,
    pub object_locator: String,
}

impl NofBackingRoute {
    pub fn from_cold(route: &ColdBackingRoute) -> Self {
        Self {
            target_id: route.cold_tier_id.clone(),
            owner: route.owner.clone(),
            object_locator: route.object_locator.clone(),
            length: route.length,
            checksum: route.checksum,
            state: route.state,
            replicas: route
                .replicas
                .iter()
                .map(|replica| NofBackingReplica {
                    target_id: replica.cold_tier_id.clone(),
                    owner: replica.owner.clone(),
                    object_locator: replica.object_locator.clone(),
                })
                .collect(),
        }
    }

    pub fn to_cold(&self) -> ColdBackingRoute {
        ColdBackingRoute {
            cold_tier_id: self.target_id.clone(),
            owner: self.owner.clone(),
            object_locator: self.object_locator.clone(),
            length: self.length,
            checksum: self.checksum,
            state: self.state,
            replicas: self
                .replicas
                .iter()
                .map(|replica| ColdBackingReplica {
                    cold_tier_id: replica.target_id.clone(),
                    owner: replica.owner.clone(),
                    object_locator: replica.object_locator.clone(),
                })
                .collect(),
        }
    }

    pub fn all_targets(&self) -> impl Iterator<Item = (&str, &ClientRuntimeId, &str)> {
        std::iter::once((
            self.target_id.as_str(),
            &self.owner,
            self.object_locator.as_str(),
        ))
        .chain(self.replicas.iter().map(|replica| {
            (
                replica.target_id.as_str(),
                &replica.owner,
                replica.object_locator.as_str(),
            )
        }))
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NofBackingRouteFilter {
    pub target_id: Option<String>,
    pub state: Option<ColdBackingState>,
    pub limit: Option<usize>,
}
