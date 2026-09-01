use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub const NOF_BACKING_ROUTE_CAPABILITY: &str = "nof-backing-route-v1";

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
            NOF_BACKING_ROUTE_CAPABILITY.to_string(),
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

    use super::{CompatibilityDescriptor, NOF_BACKING_ROUTE_CAPABILITY};

    #[test]
    fn default_descriptor_matches_mooncake_v1_contract() {
        let descriptor = CompatibilityDescriptor::default();
        assert_eq!(descriptor, CompatibilityDescriptor::mooncake_v1());
        assert_eq!(descriptor.store_api_version, 1);
        assert_eq!(descriptor.store_api_minor_version, 0);
        assert_eq!(descriptor.metadata_schema_version, 1);
        assert_eq!(descriptor.transport_api_version, 1);
        assert!(descriptor.supports("masterless-routing"));
        assert!(descriptor.supports(NOF_BACKING_ROUTE_CAPABILITY));
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

    // --- Adversarial: protocol compatibility boundary conditions ---------

    #[test]
    fn compatibility_with_zero_versions_is_reflexive_but_not_equal_to_default() {
        let zero = CompatibilityDescriptor {
            store_api_version: 0,
            store_api_minor_version: 0,
            metadata_schema_version: 0,
            transport_api_version: 0,
            capabilities: BTreeSet::new(),
        };
        assert!(zero.is_compatible_with(&zero));
        assert!(!zero.is_compatible_with(&CompatibilityDescriptor::default()));
    }

    #[test]
    fn compatibility_with_max_versions_is_reflexive() {
        let max_ver = CompatibilityDescriptor {
            store_api_version: u32::MAX,
            store_api_minor_version: u32::MAX,
            metadata_schema_version: u32::MAX,
            transport_api_version: u32::MAX,
            capabilities: BTreeSet::new(),
        };
        assert!(max_ver.is_compatible_with(&max_ver));
        assert!(!max_ver.is_compatible_with(&CompatibilityDescriptor::default()));
    }

    #[test]
    fn supports_is_case_sensitive() {
        let d = CompatibilityDescriptor {
            capabilities: BTreeSet::from(["Hot-Upgrade".to_string()]),
            ..CompatibilityDescriptor::default()
        };
        assert!(d.supports("Hot-Upgrade"));
        assert!(!d.supports("hot-upgrade"));
        assert!(!d.supports("HOT-UPGRADE"));
    }

    #[test]
    fn empty_capabilities_supports_nothing_including_empty_query() {
        let d = CompatibilityDescriptor {
            capabilities: BTreeSet::new(),
            ..CompatibilityDescriptor::default()
        };
        assert!(!d.supports("anything"));
        assert!(!d.supports(""));
    }

    #[test]
    fn serde_round_trip_preserves_all_fields_and_capability_set() {
        let d = CompatibilityDescriptor {
            store_api_version: 42,
            store_api_minor_version: 7,
            metadata_schema_version: 3,
            transport_api_version: 5,
            capabilities: BTreeSet::from([
                "cap-a".to_string(),
                "cap-b".to_string(),
                "cap-c".to_string(),
            ]),
        };
        let s = serde_json::to_string(&d).unwrap();
        let back: CompatibilityDescriptor = serde_json::from_str(&s).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn compatibility_is_symmetric_across_minor_version_and_capabilities() {
        let a = CompatibilityDescriptor {
            store_api_version: 2,
            store_api_minor_version: 0,
            metadata_schema_version: 1,
            transport_api_version: 1,
            capabilities: BTreeSet::from(["cap-a".to_string()]),
        };
        let b = CompatibilityDescriptor {
            store_api_version: 2,
            store_api_minor_version: 3,
            metadata_schema_version: 1,
            transport_api_version: 1,
            capabilities: BTreeSet::from(["cap-b".to_string()]),
        };
        assert_eq!(a.is_compatible_with(&b), b.is_compatible_with(&a));
    }
}
