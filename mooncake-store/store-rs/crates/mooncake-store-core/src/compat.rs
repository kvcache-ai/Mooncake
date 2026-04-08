use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompatibilityDescriptor {
    pub store_api_version: u32,
    pub metadata_schema_version: u32,
    pub transport_api_version: u32,
    pub capabilities: BTreeSet<String>,
}

impl CompatibilityDescriptor {
    pub fn mooncake_v1() -> Self {
        let capabilities = BTreeSet::from([
            "epoch-fencing".to_string(),
            "graceful-drain".to_string(),
            "hot-standby".to_string(),
            "hot-upgrade".to_string(),
            "masterless-routing".to_string(),
        ]);
        Self {
            store_api_version: 1,
            metadata_schema_version: 1,
            transport_api_version: 1,
            capabilities,
        }
    }

    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }
}

impl Default for CompatibilityDescriptor {
    fn default() -> Self {
        Self::mooncake_v1()
    }
}
