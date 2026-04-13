use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompatibilityDescriptor {
    pub store_api_version: u32,
    #[serde(default)]
    pub store_api_minor_version: u32,
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
            store_api_minor_version: 0,
            metadata_schema_version: 1,
            transport_api_version: 1,
            capabilities,
        }
    }

    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities.contains(capability)
    }

    /// Returns `true` when `self` and `other` are compatible for interoperation.
    ///
    /// Rules:
    /// - `store_api_version` (major) must be strictly equal.
    /// - `store_api_minor_version` is **not** checked — a node with minor=0
    ///   can freely interoperate with a node with minor=3 as long as the major
    ///   versions match.  The minor version only indicates which additive
    ///   features are available; the absence of a feature is handled at the
    ///   call-site (e.g. via `capabilities` or gRPC `UNIMPLEMENTED` fallback).
    /// - `metadata_schema_version` and `transport_api_version` must be strictly
    ///   equal.
    pub fn is_compatible_with(&self, other: &CompatibilityDescriptor) -> bool {
        self.store_api_version == other.store_api_version
            && self.metadata_schema_version == other.metadata_schema_version
            && self.transport_api_version == other.transport_api_version
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
        assert_eq!(descriptor.store_api_minor_version, 0);
        assert_eq!(descriptor.metadata_schema_version, 1);
        assert_eq!(descriptor.transport_api_version, 1);
        assert!(descriptor.supports("masterless-routing"));
        assert!(!descriptor.supports("imaginary-capability"));
    }

    #[test]
    fn supports_checks_capability_membership_without_side_effects() {
        let descriptor = CompatibilityDescriptor {
            store_api_version: 9,
            store_api_minor_version: 2,
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

    #[test]
    fn compatible_when_major_matches_and_minor_differs() {
        let base = CompatibilityDescriptor::default();
        let newer_minor = CompatibilityDescriptor {
            store_api_minor_version: 5,
            ..base.clone()
        };
        assert!(base.is_compatible_with(&newer_minor));
        assert!(newer_minor.is_compatible_with(&base));
    }

    #[test]
    fn incompatible_when_major_differs() {
        let base = CompatibilityDescriptor::default();
        let different_major = CompatibilityDescriptor {
            store_api_version: base.store_api_version + 1,
            ..base.clone()
        };
        assert!(!base.is_compatible_with(&different_major));
        assert!(!different_major.is_compatible_with(&base));
    }

    #[test]
    fn incompatible_when_metadata_schema_differs() {
        let base = CompatibilityDescriptor::default();
        let different_schema = CompatibilityDescriptor {
            metadata_schema_version: base.metadata_schema_version + 1,
            ..base.clone()
        };
        assert!(!base.is_compatible_with(&different_schema));
    }

    #[test]
    fn incompatible_when_transport_version_differs() {
        let base = CompatibilityDescriptor::default();
        let different_transport = CompatibilityDescriptor {
            transport_api_version: base.transport_api_version + 1,
            ..base.clone()
        };
        assert!(!base.is_compatible_with(&different_transport));
    }

    #[test]
    fn serde_default_fills_minor_version_for_legacy_descriptor() {
        let legacy_json = r#"{
            "store_api_version": 1,
            "metadata_schema_version": 1,
            "transport_api_version": 1,
            "capabilities": ["hot-upgrade"]
        }"#;
        let deserialized: CompatibilityDescriptor =
            serde_json::from_str(legacy_json).expect("should deserialize legacy format");
        assert_eq!(deserialized.store_api_minor_version, 0);
        assert_eq!(deserialized.store_api_version, 1);
    }
}
