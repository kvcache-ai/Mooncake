use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::route::SegmentName;

pub const DEFAULT_TENANT: &str = "default";
pub const DEFAULT_DOMAIN: &str = "default";
pub const DEFAULT_OBJECT_SET: &str = "default";
pub const DEFAULT_QOS_TIER: &str = "default";

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

    pub fn from_storage_key(value: &str) -> Option<Self> {
        let (stable_id, epoch) = value.rsplit_once(':')?;
        if stable_id.is_empty() {
            return None;
        }
        let epoch = epoch.parse::<u64>().ok()?;
        Some(Self::new(stable_id, ClientEpoch(epoch)))
    }
}

impl Display for ClientRuntimeId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.storage_key())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct NamespaceScope {
    pub tenant: String,
    pub domain: String,
    pub object_set: String,
}

impl NamespaceScope {
    pub fn new(
        tenant: impl Into<String>,
        domain: impl Into<String>,
        object_set: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            domain: domain.into(),
            object_set: object_set.into(),
        }
    }

    pub fn with_defaults(
        tenant: Option<&str>,
        domain: Option<&str>,
        object_set: Option<&str>,
    ) -> Self {
        Self {
            tenant: tenant.unwrap_or(DEFAULT_TENANT).to_string(),
            domain: domain.unwrap_or(DEFAULT_DOMAIN).to_string(),
            object_set: object_set.unwrap_or(DEFAULT_OBJECT_SET).to_string(),
        }
    }

    pub fn canonical_prefix(&self) -> String {
        format!("{}/{}/{}", self.tenant, self.domain, self.object_set)
    }
}

impl Default for NamespaceScope {
    fn default() -> Self {
        Self::with_defaults(None, None, None)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct LogicalObjectId {
    pub scope: NamespaceScope,
    pub logical_key: String,
}

impl LogicalObjectId {
    pub fn new(scope: NamespaceScope, logical_key: impl Into<String>) -> Self {
        Self {
            scope,
            logical_key: logical_key.into(),
        }
    }

    pub fn canonical_key(&self) -> String {
        format!("{}/{}", self.scope.canonical_prefix(), self.logical_key)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ReuseIdentity {
    pub tenant: String,
    pub domain: String,
    pub sharing_scope: String,
    pub canonical_key: String,
}

impl ReuseIdentity {
    pub fn new(
        tenant: impl Into<String>,
        domain: impl Into<String>,
        sharing_scope: impl Into<String>,
        canonical_key: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            domain: domain.into(),
            sharing_scope: sharing_scope.into(),
            canonical_key: canonical_key.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // -----------------------------------------------------------------------
    // ClientStableId
    // -----------------------------------------------------------------------

    #[test]
    fn client_stable_id_new_from_str() {
        let id = ClientStableId::new("node-1");
        assert_eq!(id.0, "node-1");
    }

    #[test]
    fn client_stable_id_new_from_string() {
        let id = ClientStableId::new(String::from("node-2"));
        assert_eq!(id.0, "node-2");
    }

    #[test]
    fn client_stable_id_display_matches_inner() {
        let id = ClientStableId::new("display-test");
        assert_eq!(format!("{id}"), "display-test");
    }

    #[test]
    fn client_stable_id_equality_and_ordering() {
        assert_eq!(ClientStableId::new("same"), ClientStableId::new("same"));
        assert_ne!(ClientStableId::new("a"), ClientStableId::new("b"));
        assert!(ClientStableId::new("aaa") < ClientStableId::new("zzz"));
    }

    #[test]
    fn client_stable_id_clone_is_equal() {
        let id = ClientStableId::new("clone-me");
        assert_eq!(id, id.clone());
    }

    #[test]
    fn client_stable_id_hash_is_consistent() {
        let mut set = HashSet::new();
        set.insert(ClientStableId::new("unique"));
        set.insert(ClientStableId::new("unique"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn client_stable_id_with_empty_string() {
        let id = ClientStableId::new("");
        assert_eq!(id.0, "");
        assert_eq!(format!("{id}"), "");
    }

    #[test]
    fn client_stable_id_with_unicode() {
        let id = ClientStableId::new("naïve-αβγ");
        assert_eq!(format!("{id}"), "naïve-αβγ");
    }

    // -----------------------------------------------------------------------
    // ClientEpoch
    // -----------------------------------------------------------------------

    #[test]
    fn client_epoch_default_is_zero() {
        assert_eq!(ClientEpoch::default(), ClientEpoch(0));
    }

    #[test]
    fn client_epoch_next_increments() {
        assert_eq!(ClientEpoch(5).next(), ClientEpoch(6));
    }

    #[test]
    fn client_epoch_next_saturates_at_max() {
        let maxed = ClientEpoch(u64::MAX);
        assert_eq!(maxed.next(), maxed);
    }

    #[test]
    fn client_epoch_ordering_and_equality() {
        assert!(ClientEpoch(1) < ClientEpoch(2));
        assert_eq!(ClientEpoch(42), ClientEpoch(42));
    }

    #[test]
    fn client_epoch_is_copy() {
        let e = ClientEpoch(10);
        let c = e;
        assert_eq!(e.0, c.0);
    }

    // -----------------------------------------------------------------------
    // ClientRuntimeId
    // -----------------------------------------------------------------------

    #[test]
    fn client_runtime_id_new_stores_components() {
        let r = ClientRuntimeId::new("node-1", ClientEpoch(3));
        assert_eq!(r.stable_id.0, "node-1");
        assert_eq!(r.epoch.0, 3);
    }

    #[test]
    fn client_runtime_id_storage_key_uses_colon_separator() {
        let r = ClientRuntimeId::new("worker-a", ClientEpoch(7));
        assert_eq!(r.storage_key(), "worker-a:7");
    }

    #[test]
    fn client_runtime_id_display_equals_storage_key() {
        let r = ClientRuntimeId::new("display-node", ClientEpoch(99));
        assert_eq!(format!("{r}"), "display-node:99");
    }

    #[test]
    fn client_runtime_id_equality() {
        let a = ClientRuntimeId::new("same", ClientEpoch(1));
        let b = ClientRuntimeId::new("same", ClientEpoch(1));
        assert_eq!(a, b);
    }

    #[test]
    fn client_runtime_id_differs_on_epoch() {
        let a = ClientRuntimeId::new("n", ClientEpoch(1));
        let b = ClientRuntimeId::new("n", ClientEpoch(2));
        assert_ne!(a, b);
    }

    #[test]
    fn client_runtime_id_differs_on_stable_id() {
        let a = ClientRuntimeId::new("a", ClientEpoch(1));
        let b = ClientRuntimeId::new("b", ClientEpoch(1));
        assert_ne!(a, b);
    }

    #[test]
    fn client_runtime_id_ordering_is_stable_id_then_epoch() {
        let hi_epoch_same_node = ClientRuntimeId::new("a", ClientEpoch(2));
        let lo_epoch_same_node = ClientRuntimeId::new("a", ClientEpoch(1));
        let other_node_zero_epoch = ClientRuntimeId::new("b", ClientEpoch(0));
        assert!(lo_epoch_same_node < hi_epoch_same_node);
        assert!(hi_epoch_same_node < other_node_zero_epoch);
    }

    #[test]
    fn client_runtime_id_hash_is_consistent() {
        let mut set = HashSet::new();
        set.insert(ClientRuntimeId::new("node", ClientEpoch(1)));
        set.insert(ClientRuntimeId::new("node", ClientEpoch(1)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn client_runtime_id_storage_key_with_epoch_zero() {
        let r = ClientRuntimeId::new("zero", ClientEpoch(0));
        assert_eq!(r.storage_key(), "zero:0");
    }

    #[test]
    fn client_runtime_id_storage_key_with_max_epoch() {
        let r = ClientRuntimeId::new("max", ClientEpoch(u64::MAX));
        assert_eq!(r.storage_key(), format!("max:{}", u64::MAX));
    }

    #[test]
    fn client_runtime_id_storage_key_preserves_special_chars_in_stable_id() {
        let r = ClientRuntimeId::new("node/with:special@chars", ClientEpoch(42));
        assert_eq!(r.storage_key(), "node/with:special@chars:42");
    }

    // -----------------------------------------------------------------------
    // ClientEndpointSet
    // -----------------------------------------------------------------------

    #[test]
    fn client_endpoint_set_default_is_empty() {
        let ep = ClientEndpointSet::default();
        assert!(ep.rpc_address.is_empty());
        assert!(ep.segment_name.is_none());
        assert!(ep.labels.is_empty());
    }

    #[test]
    fn client_endpoint_set_with_labels_roundtrip() {
        let mut labels = BTreeMap::new();
        labels.insert("role".to_string(), "storage".to_string());
        let ep = ClientEndpointSet {
            rpc_address: "10.0.0.1:8080".to_string(),
            segment_name: Some(SegmentName::new("seg-0")),
            labels,
        };
        assert_eq!(ep.rpc_address, "10.0.0.1:8080");
        assert_eq!(ep.segment_name.as_ref().unwrap().0, "seg-0");
        assert_eq!(ep.labels.get("role").unwrap(), "storage");
    }

    #[test]
    fn client_endpoint_set_equality_and_clone() {
        let mut labels = BTreeMap::new();
        labels.insert("k".to_string(), "v".to_string());
        let a = ClientEndpointSet {
            rpc_address: "host:1234".to_string(),
            segment_name: Some(SegmentName::new("seg")),
            labels,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    // -----------------------------------------------------------------------
    // Serde roundtrips
    // -----------------------------------------------------------------------

    #[test]
    fn client_stable_id_serde_roundtrip() {
        let id = ClientStableId::new("serde-test");
        let s = serde_json::to_string(&id).unwrap();
        let back: ClientStableId = serde_json::from_str(&s).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn client_epoch_serde_roundtrip() {
        let e = ClientEpoch(42);
        let s = serde_json::to_string(&e).unwrap();
        let back: ClientEpoch = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn client_runtime_id_serde_roundtrip() {
        let r = ClientRuntimeId::new("serde-node", ClientEpoch(7));
        let s = serde_json::to_string(&r).unwrap();
        let back: ClientRuntimeId = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn client_endpoint_set_serde_roundtrip() {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        let ep = ClientEndpointSet {
            rpc_address: "10.0.0.1:9090".to_string(),
            segment_name: Some(SegmentName::new("primary")),
            labels,
        };
        let s = serde_json::to_string(&ep).unwrap();
        let back: ClientEndpointSet = serde_json::from_str(&s).unwrap();
        assert_eq!(ep, back);
    }

    // -----------------------------------------------------------------------
    // NamespaceScope boundary
    // -----------------------------------------------------------------------

    #[test]
    fn namespace_scope_canonical_prefix_with_empty_components_produces_two_slashes() {
        let s = NamespaceScope::new("", "", "");
        assert_eq!(s.canonical_prefix(), "//");
    }

    #[test]
    fn namespace_scope_canonical_prefix_passes_through_embedded_slashes() {
        let s = NamespaceScope::new("a/b", "c/d", "e/f");
        assert_eq!(s.canonical_prefix(), "a/b/c/d/e/f");
    }

    #[test]
    fn namespace_scope_with_defaults_fills_none_fields() {
        let s = NamespaceScope::with_defaults(None, None, None);
        assert_eq!(s.tenant, DEFAULT_TENANT);
        assert_eq!(s.domain, DEFAULT_DOMAIN);
        assert_eq!(s.object_set, DEFAULT_OBJECT_SET);
    }

    #[test]
    fn namespace_scope_with_defaults_respects_provided() {
        let s = NamespaceScope::with_defaults(Some("t"), Some("d"), Some("o"));
        assert_eq!(s.tenant, "t");
        assert_eq!(s.domain, "d");
        assert_eq!(s.object_set, "o");
    }

    #[test]
    fn namespace_scope_default_uses_default_constants() {
        let s = NamespaceScope::default();
        assert_eq!(s.tenant, DEFAULT_TENANT);
        assert_eq!(s.domain, DEFAULT_DOMAIN);
        assert_eq!(s.object_set, DEFAULT_OBJECT_SET);
    }

    #[test]
    fn namespace_scope_serde_roundtrip() {
        let s = NamespaceScope::new("t", "d", "o");
        let j = serde_json::to_string(&s).unwrap();
        let back: NamespaceScope = serde_json::from_str(&j).unwrap();
        assert_eq!(s, back);
    }

    // -----------------------------------------------------------------------
    // LogicalObjectId boundary
    // -----------------------------------------------------------------------

    #[test]
    fn logical_object_id_canonical_key_with_empty_logical_key() {
        let id = LogicalObjectId::new(NamespaceScope::default(), "");
        assert_eq!(
            id.canonical_key(),
            format!("{DEFAULT_TENANT}/{DEFAULT_DOMAIN}/{DEFAULT_OBJECT_SET}/")
        );
    }

    #[test]
    fn logical_object_id_canonical_key_with_unicode() {
        let id = LogicalObjectId::new(
            NamespaceScope::new("α-tenant", "β-domain", "γ-set"),
            "naïve/path",
        );
        assert_eq!(id.canonical_key(), "α-tenant/β-domain/γ-set/naïve/path");
    }

    #[test]
    fn logical_object_id_canonical_key_with_long_logical_key() {
        let long = "k".repeat(10_000);
        let id = LogicalObjectId::new(NamespaceScope::default(), long.clone());
        let key = id.canonical_key();
        assert!(key.ends_with(&long));
    }

    #[test]
    fn logical_object_id_serde_roundtrip() {
        let id = LogicalObjectId::new(NamespaceScope::new("t", "d", "s"), "key");
        let j = serde_json::to_string(&id).unwrap();
        let back: LogicalObjectId = serde_json::from_str(&j).unwrap();
        assert_eq!(id, back);
    }

    // -----------------------------------------------------------------------
    // ReuseIdentity
    // -----------------------------------------------------------------------

    #[test]
    fn reuse_identity_stores_all_fields() {
        let r = ReuseIdentity::new("t", "d", "share", "canon");
        assert_eq!(r.tenant, "t");
        assert_eq!(r.domain, "d");
        assert_eq!(r.sharing_scope, "share");
        assert_eq!(r.canonical_key, "canon");
    }

    #[test]
    fn reuse_identity_with_empty_fields_serde_roundtrips() {
        let r = ReuseIdentity::new("", "", "", "");
        assert_eq!(r.tenant, "");
        let j = serde_json::to_string(&r).unwrap();
        let back: ReuseIdentity = serde_json::from_str(&j).unwrap();
        assert_eq!(r, back);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientEndpointSet {
    pub rpc_address: String,
    pub segment_name: Option<SegmentName>,
    pub labels: BTreeMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // -----------------------------------------------------------------------
    // ClientStableId
    // -----------------------------------------------------------------------

    #[test]
    fn client_stable_id_new_from_str() {
        let id = ClientStableId::new("node-1");
        assert_eq!(id.0, "node-1");
    }

    #[test]
    fn client_stable_id_new_from_string() {
        let id = ClientStableId::new(String::from("node-2"));
        assert_eq!(id.0, "node-2");
    }

    #[test]
    fn client_stable_id_display_matches_inner() {
        let id = ClientStableId::new("display-test");
        assert_eq!(format!("{id}"), "display-test");
    }

    #[test]
    fn client_stable_id_equality_and_ordering() {
        assert_eq!(ClientStableId::new("same"), ClientStableId::new("same"));
        assert_ne!(ClientStableId::new("a"), ClientStableId::new("b"));
        assert!(ClientStableId::new("aaa") < ClientStableId::new("zzz"));
    }

    #[test]
    fn client_stable_id_clone_is_equal() {
        let id = ClientStableId::new("clone-me");
        assert_eq!(id, id.clone());
    }

    #[test]
    fn client_stable_id_hash_is_consistent() {
        let mut set = HashSet::new();
        set.insert(ClientStableId::new("unique"));
        set.insert(ClientStableId::new("unique"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn client_stable_id_with_empty_string() {
        let id = ClientStableId::new("");
        assert_eq!(id.0, "");
        assert_eq!(format!("{id}"), "");
    }

    #[test]
    fn client_stable_id_with_unicode() {
        let id = ClientStableId::new("naïve-αβγ");
        assert_eq!(format!("{id}"), "naïve-αβγ");
    }

    // -----------------------------------------------------------------------
    // ClientEpoch
    // -----------------------------------------------------------------------

    #[test]
    fn client_epoch_default_is_zero() {
        assert_eq!(ClientEpoch::default(), ClientEpoch(0));
    }

    #[test]
    fn client_epoch_next_increments() {
        assert_eq!(ClientEpoch(5).next(), ClientEpoch(6));
    }

    #[test]
    fn client_epoch_next_saturates_at_max() {
        let maxed = ClientEpoch(u64::MAX);
        assert_eq!(maxed.next(), maxed);
    }

    #[test]
    fn client_epoch_ordering_and_equality() {
        assert!(ClientEpoch(1) < ClientEpoch(2));
        assert_eq!(ClientEpoch(42), ClientEpoch(42));
    }

    #[test]
    fn client_epoch_is_copy() {
        let e = ClientEpoch(10);
        let c = e;
        assert_eq!(e.0, c.0);
    }

    // -----------------------------------------------------------------------
    // ClientRuntimeId
    // -----------------------------------------------------------------------

    #[test]
    fn client_runtime_id_new_stores_components() {
        let r = ClientRuntimeId::new("node-1", ClientEpoch(3));
        assert_eq!(r.stable_id.0, "node-1");
        assert_eq!(r.epoch.0, 3);
    }

    #[test]
    fn client_runtime_id_storage_key_uses_colon_separator() {
        let r = ClientRuntimeId::new("worker-a", ClientEpoch(7));
        assert_eq!(r.storage_key(), "worker-a:7");
    }

    #[test]
    fn client_runtime_id_display_equals_storage_key() {
        let r = ClientRuntimeId::new("display-node", ClientEpoch(99));
        assert_eq!(format!("{r}"), "display-node:99");
    }

    #[test]
    fn client_runtime_id_equality() {
        let a = ClientRuntimeId::new("same", ClientEpoch(1));
        let b = ClientRuntimeId::new("same", ClientEpoch(1));
        assert_eq!(a, b);
    }

    #[test]
    fn client_runtime_id_differs_on_epoch() {
        let a = ClientRuntimeId::new("n", ClientEpoch(1));
        let b = ClientRuntimeId::new("n", ClientEpoch(2));
        assert_ne!(a, b);
    }

    #[test]
    fn client_runtime_id_differs_on_stable_id() {
        let a = ClientRuntimeId::new("a", ClientEpoch(1));
        let b = ClientRuntimeId::new("b", ClientEpoch(1));
        assert_ne!(a, b);
    }

    #[test]
    fn client_runtime_id_ordering_is_stable_id_then_epoch() {
        let hi_epoch_same_node = ClientRuntimeId::new("a", ClientEpoch(2));
        let lo_epoch_same_node = ClientRuntimeId::new("a", ClientEpoch(1));
        let other_node_zero_epoch = ClientRuntimeId::new("b", ClientEpoch(0));
        assert!(lo_epoch_same_node < hi_epoch_same_node);
        assert!(hi_epoch_same_node < other_node_zero_epoch);
    }

    #[test]
    fn client_runtime_id_hash_is_consistent() {
        let mut set = HashSet::new();
        set.insert(ClientRuntimeId::new("node", ClientEpoch(1)));
        set.insert(ClientRuntimeId::new("node", ClientEpoch(1)));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn client_runtime_id_storage_key_with_epoch_zero() {
        let r = ClientRuntimeId::new("zero", ClientEpoch(0));
        assert_eq!(r.storage_key(), "zero:0");
    }

    #[test]
    fn client_runtime_id_storage_key_with_max_epoch() {
        let r = ClientRuntimeId::new("max", ClientEpoch(u64::MAX));
        assert_eq!(r.storage_key(), format!("max:{}", u64::MAX));
    }

    #[test]
    fn client_runtime_id_storage_key_preserves_special_chars_in_stable_id() {
        let r = ClientRuntimeId::new("node/with:special@chars", ClientEpoch(42));
        assert_eq!(r.storage_key(), "node/with:special@chars:42");
    }

    #[test]
    fn client_runtime_id_from_storage_key_round_trips_colon_stable_id() {
        let parsed = ClientRuntimeId::from_storage_key("node:with:colon:42").unwrap();
        assert_eq!(parsed.stable_id.0, "node:with:colon");
        assert_eq!(parsed.epoch, ClientEpoch(42));
    }

    #[test]
    fn client_runtime_id_from_storage_key_rejects_empty_stable_id() {
        assert!(ClientRuntimeId::from_storage_key(":42").is_none());
    }

    // -----------------------------------------------------------------------
    // ClientEndpointSet
    // -----------------------------------------------------------------------

    #[test]
    fn client_endpoint_set_default_is_empty() {
        let ep = ClientEndpointSet::default();
        assert!(ep.rpc_address.is_empty());
        assert!(ep.segment_name.is_none());
        assert!(ep.labels.is_empty());
    }

    #[test]
    fn client_endpoint_set_with_labels_roundtrip() {
        let mut labels = BTreeMap::new();
        labels.insert("role".to_string(), "storage".to_string());
        let ep = ClientEndpointSet {
            rpc_address: "10.0.0.1:8080".to_string(),
            segment_name: Some(SegmentName::new("seg-0")),
            labels,
        };
        assert_eq!(ep.rpc_address, "10.0.0.1:8080");
        assert_eq!(ep.segment_name.as_ref().unwrap().0, "seg-0");
        assert_eq!(ep.labels.get("role").unwrap(), "storage");
    }

    #[test]
    fn client_endpoint_set_equality_and_clone() {
        let mut labels = BTreeMap::new();
        labels.insert("k".to_string(), "v".to_string());
        let a = ClientEndpointSet {
            rpc_address: "host:1234".to_string(),
            segment_name: Some(SegmentName::new("seg")),
            labels,
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    // -----------------------------------------------------------------------
    // Serde roundtrips
    // -----------------------------------------------------------------------

    #[test]
    fn client_stable_id_serde_roundtrip() {
        let id = ClientStableId::new("serde-test");
        let s = serde_json::to_string(&id).unwrap();
        let back: ClientStableId = serde_json::from_str(&s).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn client_epoch_serde_roundtrip() {
        let e = ClientEpoch(42);
        let s = serde_json::to_string(&e).unwrap();
        let back: ClientEpoch = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn client_runtime_id_serde_roundtrip() {
        let r = ClientRuntimeId::new("serde-node", ClientEpoch(7));
        let s = serde_json::to_string(&r).unwrap();
        let back: ClientRuntimeId = serde_json::from_str(&s).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn client_endpoint_set_serde_roundtrip() {
        let mut labels = BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        let ep = ClientEndpointSet {
            rpc_address: "10.0.0.1:9090".to_string(),
            segment_name: Some(SegmentName::new("primary")),
            labels,
        };
        let s = serde_json::to_string(&ep).unwrap();
        let back: ClientEndpointSet = serde_json::from_str(&s).unwrap();
        assert_eq!(ep, back);
    }

    // -----------------------------------------------------------------------
    // NamespaceScope boundary
    // -----------------------------------------------------------------------

    #[test]
    fn namespace_scope_canonical_prefix_with_empty_components_produces_two_slashes() {
        let s = NamespaceScope::new("", "", "");
        assert_eq!(s.canonical_prefix(), "//");
    }

    #[test]
    fn namespace_scope_canonical_prefix_passes_through_embedded_slashes() {
        let s = NamespaceScope::new("a/b", "c/d", "e/f");
        assert_eq!(s.canonical_prefix(), "a/b/c/d/e/f");
    }

    #[test]
    fn namespace_scope_with_defaults_fills_none_fields() {
        let s = NamespaceScope::with_defaults(None, None, None);
        assert_eq!(s.tenant, DEFAULT_TENANT);
        assert_eq!(s.domain, DEFAULT_DOMAIN);
        assert_eq!(s.object_set, DEFAULT_OBJECT_SET);
    }

    #[test]
    fn namespace_scope_with_defaults_respects_provided() {
        let s = NamespaceScope::with_defaults(Some("t"), Some("d"), Some("o"));
        assert_eq!(s.tenant, "t");
        assert_eq!(s.domain, "d");
        assert_eq!(s.object_set, "o");
    }

    #[test]
    fn namespace_scope_default_uses_default_constants() {
        let s = NamespaceScope::default();
        assert_eq!(s.tenant, DEFAULT_TENANT);
        assert_eq!(s.domain, DEFAULT_DOMAIN);
        assert_eq!(s.object_set, DEFAULT_OBJECT_SET);
    }

    #[test]
    fn namespace_scope_serde_roundtrip() {
        let s = NamespaceScope::new("t", "d", "o");
        let j = serde_json::to_string(&s).unwrap();
        let back: NamespaceScope = serde_json::from_str(&j).unwrap();
        assert_eq!(s, back);
    }

    // -----------------------------------------------------------------------
    // LogicalObjectId boundary
    // -----------------------------------------------------------------------

    #[test]
    fn logical_object_id_canonical_key_with_empty_logical_key() {
        let id = LogicalObjectId::new(NamespaceScope::default(), "");
        assert_eq!(
            id.canonical_key(),
            format!("{DEFAULT_TENANT}/{DEFAULT_DOMAIN}/{DEFAULT_OBJECT_SET}/")
        );
    }

    #[test]
    fn logical_object_id_canonical_key_with_unicode() {
        let id = LogicalObjectId::new(
            NamespaceScope::new("α-tenant", "β-domain", "γ-set"),
            "naïve/path",
        );
        assert_eq!(id.canonical_key(), "α-tenant/β-domain/γ-set/naïve/path");
    }

    #[test]
    fn logical_object_id_canonical_key_with_long_logical_key() {
        let long = "k".repeat(10_000);
        let id = LogicalObjectId::new(NamespaceScope::default(), long.clone());
        let key = id.canonical_key();
        assert!(key.ends_with(&long));
    }

    #[test]
    fn logical_object_id_serde_roundtrip() {
        let id = LogicalObjectId::new(NamespaceScope::new("t", "d", "s"), "key");
        let j = serde_json::to_string(&id).unwrap();
        let back: LogicalObjectId = serde_json::from_str(&j).unwrap();
        assert_eq!(id, back);
    }

    // -----------------------------------------------------------------------
    // ReuseIdentity
    // -----------------------------------------------------------------------

    #[test]
    fn reuse_identity_stores_all_fields() {
        let r = ReuseIdentity::new("t", "d", "share", "canon");
        assert_eq!(r.tenant, "t");
        assert_eq!(r.domain, "d");
        assert_eq!(r.sharing_scope, "share");
        assert_eq!(r.canonical_key, "canon");
    }

    #[test]
    fn reuse_identity_with_empty_fields_serde_roundtrips() {
        let r = ReuseIdentity::new("", "", "", "");
        assert_eq!(r.tenant, "");
        let j = serde_json::to_string(&r).unwrap();
        let back: ReuseIdentity = serde_json::from_str(&j).unwrap();
        assert_eq!(r, back);
    }
}
