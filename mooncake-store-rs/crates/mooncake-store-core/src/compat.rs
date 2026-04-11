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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::CompatibilityDescriptor;

    #[test]
    fn default_descriptor_matches_mooncake_v1_contract() {
        let descriptor = CompatibilityDescriptor::default();
        assert_eq!(descriptor, CompatibilityDescriptor::mooncake_v1());
        assert_eq!(descriptor.store_api_version, 1);
        assert_eq!(descriptor.metadata_schema_version, 1);
        assert_eq!(descriptor.transport_api_version, 1);
        assert!(descriptor.supports("masterless-routing"));
        assert!(!descriptor.supports("imaginary-capability"));
    }

    #[test]
    fn supports_checks_capability_membership_without_side_effects() {
        let descriptor = CompatibilityDescriptor {
            store_api_version: 9,
            metadata_schema_version: 8,
            transport_api_version: 7,
            capabilities: BTreeSet::from([
                "hot-upgrade".to_string(),
                "tenant-isolation".to_string(),
            ]),
        };
        assert!(descriptor.supports("hot-upgrade"));
        assert!(descriptor.supports("tenant-isolation"));
        assert!(!descriptor.supports("graceful-drain"));
    }
}
