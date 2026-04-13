use crate::error::{Result, StoreError};
use crate::identity::{LogicalObjectId, NamespaceScope, DEFAULT_QOS_TIER};
use crate::route::{ObjectKey, ObjectRoute};

pub fn scoped_logical_object_id(tenant: &str, logical_key: &str) -> LogicalObjectId {
    LogicalObjectId::new(
        NamespaceScope::with_defaults(Some(tenant), None, None),
        logical_key,
    )
}

pub fn scoped_object_key(tenant: &str, logical_key: &str) -> ObjectKey {
    ObjectKey::from_logical_id(&scoped_logical_object_id(tenant, logical_key))
}

pub fn parse_legacy_scoped_key(key: &ObjectKey) -> Result<LogicalObjectId> {
    let (tenant, logical_key) = key.0.split_once("::").ok_or_else(|| {
        StoreError::InvalidState(format!("route key {} is missing tenant scope", key.0))
    })?;
    Ok(scoped_logical_object_id(tenant, logical_key))
}

pub fn route_logical_object_id(route: &ObjectRoute) -> Result<LogicalObjectId> {
    match (&route.namespace, &route.logical_key) {
        (Some(namespace), Some(logical_key)) => {
            Ok(LogicalObjectId::new(namespace.clone(), logical_key.clone()))
        }
        _ => parse_legacy_scoped_key(&route.key),
    }
}

pub fn apply_route_identity(route: &mut ObjectRoute, id: &LogicalObjectId) {
    route.key = ObjectKey::from_logical_id(id);
    route.namespace = Some(id.scope.clone());
    route.logical_key = Some(id.logical_key.clone());
    route.canonical_key = Some(id.canonical_key());
    route.sharing_scope = Some(id.scope.tenant.clone());
    route.qos_tier = Some(DEFAULT_QOS_TIER.to_string());
}

#[cfg(test)]
mod tests {
    use crate::{
        CompatibilityDescriptor, ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteState,
        RouteVersion, StoreError,
    };

    use super::{apply_route_identity, parse_legacy_scoped_key, route_logical_object_id, scoped_logical_object_id, scoped_object_key};

    #[test]
    fn scoped_helpers_build_default_namespace_identity() {
        let id = scoped_logical_object_id("tenant-a", "key-a");
        assert_eq!(id.scope.tenant, "tenant-a");
        assert_eq!(id.scope.domain, "default");
        assert_eq!(id.scope.object_set, "default");
        assert_eq!(id.logical_key, "key-a");
        assert_eq!(scoped_object_key("tenant-a", "key-a").0, "tenant-a::key-a");
    }

    #[test]
    fn route_logical_object_id_falls_back_to_legacy_key() {
        let route = ObjectRoute {
            key: scoped_object_key("tenant-a", "key-a"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::<ReplicaRoute>::new(),
        };
        assert_eq!(
            route_logical_object_id(&route).expect("route should parse"),
            scoped_logical_object_id("tenant-a", "key-a")
        );
    }

    #[test]
    fn apply_route_identity_populates_route_metadata() {
        let mut route = ObjectRoute {
            key: scoped_object_key("tenant-a", "old"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: crate::ClientRuntimeId::new("runtime-a", crate::ClientEpoch(1)),
                segment_name: crate::SegmentName::new("segment-a"),
                offset: 0,
                segment_offset: 0,
                length: 1,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        };
        apply_route_identity(&mut route, &scoped_logical_object_id("tenant-b", "key-b"));
        assert_eq!(route.key.0, "tenant-b::key-b");
        assert_eq!(route.logical_key.as_deref(), Some("key-b"));
        assert_eq!(route.canonical_key.as_deref(), Some("tenant-b/default/default/key-b"));
        assert_eq!(route.sharing_scope.as_deref(), Some("tenant-b"));
        assert_eq!(route.qos_tier.as_deref(), Some("default"));
    }

    #[test]
    fn parse_legacy_scoped_key_rejects_unscoped_values() {
        let error = parse_legacy_scoped_key(&ObjectKey::new("plain-key")).expect_err("key should fail");
        assert!(matches!(error, StoreError::InvalidState(_)));
    }
}
