use serde::{Deserialize, Serialize};

use crate::cold_tier::ColdBackingState;
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
