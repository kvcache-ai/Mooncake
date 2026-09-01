use super::*;
use mooncake_store_core::{
    ClientEpoch, ClientRuntimeId, ClientStableId, ColdBackingRoute, ColdBackingState,
    CompatibilityDescriptor, NofBackingRoute, NofBackingState, ObjectRoute, ReplicaRoute,
    RouteState, StoreError,
};

fn owner() -> ClientRuntimeId {
    ClientRuntimeId {
        stable_id: ClientStableId::new("owner"),
        epoch: ClientEpoch(1),
    }
}

fn nof_route() -> ObjectRoute {
    ObjectRoute {
        key: ObjectKey::new("key"),
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
        nof_backing: Some(NofBackingRoute {
            owner: owner(),
            target_id: "nof-target".to_string(),
            object_locator: "nof-ll:v1:i:01".to_string(),
            length: 1,
            checksum: None,
            state: NofBackingState::Materialized,
            replicas: Vec::new(),
        }),
    }
}

#[test]
fn rejects_mixed_local_and_nof_backing_before_metadata_write() {
    let metadata = mooncake_metadata::InMemoryMetadataBackend::new();
    let mut route = nof_route();
    route.cold_backing = Some(ColdBackingRoute {
        owner: owner(),
        cold_tier_id: "local-disk".to_string(),
        object_locator: "file".to_string(),
        length: 1,
        checksum: None,
        state: ColdBackingState::Materialized,
        replicas: Vec::new(),
    });

    assert!(matches!(
        metadata.compare_and_swap_nof_object_route(&route.key, None, Some(&route)),
        Err(StoreError::InvalidState(message)) if message.contains("both local Cold Tier and NoF")
    ));
}

#[test]
fn rejects_nof_backing_without_route_capability() {
    let metadata = mooncake_metadata::InMemoryMetadataBackend::new();
    let mut route = nof_route();
    route.compatibility.capabilities.clear();

    assert!(matches!(
        metadata.compare_and_swap_nof_object_route(&route.key, None, Some(&route)),
        Err(StoreError::InvalidState(message)) if message.contains("nof-backing-route-v1")
    ));
}

#[test]
fn rejects_route_without_nof_backing_on_typed_cas() {
    let metadata = mooncake_metadata::InMemoryMetadataBackend::new();
    let mut route = nof_route();
    route.nof_backing = None;

    assert!(matches!(
        metadata.compare_and_swap_nof_object_route(&route.key, None, Some(&route)),
        Err(StoreError::InvalidState(message)) if message.contains("requires nof_backing")
    ));
}

#[test]
fn lists_nof_routes_without_returning_local_cold_routes() {
    let metadata = mooncake_metadata::InMemoryMetadataBackend::new();
    let route = nof_route();
    assert!(
        metadata
            .compare_and_swap_nof_object_route(&route.key, None, Some(&route))
            .expect("NoF route CAS should succeed")
            .applied
    );

    let routes = metadata
        .list_nof_object_routes(&NofBackingRouteFilter {
            target_id: Some("nof-target".to_string()),
            state: Some(NofBackingState::Materialized),
            owner: Some(owner()),
            limit: None,
        })
        .expect("NoF route list should succeed");

    assert_eq!(routes, vec![route]);
}
