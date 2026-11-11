use crate::error::{Result, StoreError};
use crate::identity::{LogicalObjectId, NamespaceScope, ReuseIdentity, DEFAULT_QOS_TIER};
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
    if tenant.is_empty() || logical_key.is_empty() {
        return Err(StoreError::InvalidState(format!(
            "route key {} must include non-empty tenant and logical key",
            key.0
        )));
    }
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

pub fn route_reuse_identity(route: &ObjectRoute) -> Result<ReuseIdentity> {
    let object_id = route_logical_object_id(route)?;
    let tenant = object_id.scope.tenant.clone();
    let domain = object_id.scope.domain.clone();
    let object_set = object_id.scope.object_set.clone();
    let canonical_key = object_id.canonical_key();
    Ok(ReuseIdentity::new(
        tenant,
        domain,
        route.sharing_scope.clone().unwrap_or(object_set),
        route.canonical_key.clone().unwrap_or(canonical_key),
    ))
}

#[cfg(test)]
mod tests {
    use crate::{
        CompatibilityDescriptor, LogicalObjectId, NamespaceScope, ObjectKey, ObjectRoute,
        ReplicaRoute, ReplicaTier, RouteState, RouteVersion, StoreError,
    };

    use super::{
        apply_route_identity, parse_legacy_scoped_key, route_logical_object_id,
        route_reuse_identity, scoped_logical_object_id, scoped_object_key,
    };

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
            cold_backing: None,

            nof_backing: None,
        };
        assert_eq!(
            route_logical_object_id(&route).expect("route should parse"),
            scoped_logical_object_id("tenant-a", "key-a")
        );
    }

    #[test]
    fn non_default_scope_object_key_distinguishes_shared_logical_key() {
        let scope_a = NamespaceScope::new("tenant-a", "domain-a", "set-a");
        let scope_b = NamespaceScope::new("tenant-a", "domain-b", "set-b");
        let key_a = ObjectKey::from_logical_id(&LogicalObjectId::new(scope_a, "shared-key"));
        let key_b = ObjectKey::from_logical_id(&LogicalObjectId::new(scope_b, "shared-key"));
        assert_ne!(key_a, key_b);
        assert_eq!(key_a.0, "tenant-a::ns/domain-a/set-a/shared-key");
        assert_eq!(key_b.0, "tenant-a::ns/domain-b/set-b/shared-key");
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
                offset: Some(0),
                segment_offset: 0,
                length: 1,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,

            nof_backing: None,
        };
        apply_route_identity(&mut route, &scoped_logical_object_id("tenant-b", "key-b"));
        assert_eq!(route.key.0, "tenant-b::key-b");
        assert_eq!(route.logical_key.as_deref(), Some("key-b"));
        assert_eq!(
            route.canonical_key.as_deref(),
            Some("tenant-b/default/default/key-b")
        );
        assert_eq!(route.sharing_scope.as_deref(), Some("tenant-b"));
        assert_eq!(route.qos_tier.as_deref(), Some("default"));
    }

    #[test]
    fn apply_route_identity_uses_full_scope_key_for_non_default_scope() {
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
            replicas: Vec::<ReplicaRoute>::new(),
            cold_backing: None,

            nof_backing: None,
        };
        let id = LogicalObjectId::new(
            NamespaceScope::new("tenant-a", "domain-a", "set-a"),
            "shared-key",
        );
        apply_route_identity(&mut route, &id);
        assert_eq!(route.key.0, "tenant-a::ns/domain-a/set-a/shared-key");
        assert_eq!(route.namespace.as_ref(), Some(&id.scope));
        assert_eq!(route.logical_key.as_deref(), Some("shared-key"));
        assert_eq!(
            route.canonical_key.as_deref(),
            Some("tenant-a/domain-a/set-a/shared-key")
        );
    }

    #[test]
    fn parse_legacy_scoped_key_rejects_unscoped_values() {
        let error =
            parse_legacy_scoped_key(&ObjectKey::new("plain-key")).expect_err("key should fail");
        assert!(matches!(error, StoreError::InvalidState(_)));
    }

    #[test]
    fn parse_legacy_scoped_key_rejects_empty_components() {
        let missing_tenant =
            parse_legacy_scoped_key(&ObjectKey::new("::key")).expect_err("tenant should fail");
        assert!(matches!(missing_tenant, StoreError::InvalidState(_)));

        let missing_logical_key = parse_legacy_scoped_key(&ObjectKey::new("tenant::"))
            .expect_err("logical key should fail");
        assert!(matches!(missing_logical_key, StoreError::InvalidState(_)));
    }

    #[test]
    fn route_reuse_identity_uses_route_metadata_boundary() {
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
            replicas: Vec::<ReplicaRoute>::new(),
            cold_backing: None,

            nof_backing: None,
        };
        apply_route_identity(&mut route, &scoped_logical_object_id("tenant-b", "key-b"));
        route.sharing_scope = Some("domain-a".to_string());

        let reuse = route_reuse_identity(&route).expect("reuse identity should build");
        assert_eq!(reuse.tenant, "tenant-b");
        assert_eq!(reuse.domain, "default");
        assert_eq!(reuse.sharing_scope, "domain-a");
        assert_eq!(reuse.canonical_key, "tenant-b/default/default/key-b");
    }

    // --- Adversarial: boundary inputs on codec routines -------------------

    #[test]
    fn parse_legacy_scoped_key_with_multiple_double_colons_splits_on_first() {
        let id = parse_legacy_scoped_key(&ObjectKey::new("tenant::key::extra"))
            .expect("multiple :: should parse using first occurrence");
        assert_eq!(id.scope.tenant, "tenant");
        assert_eq!(id.logical_key, "key::extra");
    }

    #[test]
    fn parse_legacy_scoped_key_with_unicode_components() {
        let id = parse_legacy_scoped_key(&ObjectKey::new("tenant-αβγ::naïve-path"))
            .expect("unicode key should parse");
        assert_eq!(id.scope.tenant, "tenant-αβγ");
        assert_eq!(id.logical_key, "naïve-path");
    }

    #[test]
    fn parse_legacy_scoped_key_with_10k_char_logical_key() {
        let long = "a".repeat(10_000);
        let id = parse_legacy_scoped_key(&ObjectKey::new(format!("tenant::{long}")))
            .expect("long key should parse");
        assert_eq!(id.logical_key.len(), 10_000);
    }

    #[test]
    fn route_logical_object_id_prefers_namespace_over_legacy_key() {
        let route = ObjectRoute {
            key: ObjectKey::new("legacy-tenant::legacy-key"),
            namespace: Some(crate::NamespaceScope::new(
                "explicit-tenant",
                "explicit-domain",
                "explicit-set",
            )),
            logical_key: Some("explicit-key".to_string()),
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::<ReplicaRoute>::new(),
            cold_backing: None,

            nof_backing: None,
        };
        let id = route_logical_object_id(&route).expect("namespace path must win");
        assert_eq!(id.scope.tenant, "explicit-tenant");
        assert_eq!(id.logical_key, "explicit-key");
    }

    #[test]
    fn route_reuse_identity_defaults_sharing_scope_when_absent() {
        let route = ObjectRoute {
            key: ObjectKey::new("tenant-x::key-x"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::<ReplicaRoute>::new(),
            cold_backing: None,

            nof_backing: None,
        };
        let reuse = route_reuse_identity(&route).expect("fallback build");
        assert_eq!(reuse.sharing_scope, "default");
    }

    #[test]
    fn apply_route_identity_overwrites_all_metadata_fields() {
        let mut route = ObjectRoute {
            key: ObjectKey::new("old::key"),
            namespace: Some(crate::NamespaceScope::new("old", "old", "old")),
            logical_key: Some("old".to_string()),
            canonical_key: Some("old/old/old/old".to_string()),
            sharing_scope: Some("old".to_string()),
            qos_tier: Some("old".to_string()),
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::<ReplicaRoute>::new(),
            cold_backing: None,

            nof_backing: None,
        };
        apply_route_identity(&mut route, &scoped_logical_object_id("new-t", "new-k"));
        assert_eq!(route.key.0, "new-t::new-k");
        assert_eq!(route.namespace.as_ref().unwrap().tenant, "new-t");
        assert_eq!(route.logical_key.as_ref().unwrap(), "new-k");
    }
}
