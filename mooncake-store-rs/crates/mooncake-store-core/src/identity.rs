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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientEndpointSet {
    pub rpc_address: String,
    pub segment_name: Option<SegmentName>,
    pub labels: BTreeMap<String, String>,
}
