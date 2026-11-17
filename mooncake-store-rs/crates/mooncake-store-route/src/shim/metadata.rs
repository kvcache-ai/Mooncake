use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, MetadataBackend, NamespaceScope, ObjectKey,
    ObjectRoute, ReplicaRoute, Result, ReuseIdentity, RouteDirectory, RouteReplicaReference,
    RouteVersion,
};

use crate::metrics::record_cas_outcome;
use crate::DEFAULT_ROUTE_OWNER_PAGE_SIZE;

pub(crate) fn build_metadata_route_directory(
    metadata: Arc<dyn MetadataBackend>,
) -> Arc<dyn RouteDirectory> {
    Arc::new(MetadataRouteDirectory { metadata })
}

struct MetadataRouteDirectory {
    metadata: Arc<dyn MetadataBackend>,
}

impl RouteDirectory for MetadataRouteDirectory {
    fn metadata(&self) -> &dyn MetadataBackend {
        self.metadata.as_ref()
    }

    fn get_object_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        self.metadata.get_object_route(key)
    }

    fn confirm_replica_reference(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
        replica: &ReplicaRoute,
    ) -> Result<RouteReplicaReference> {
        match self.metadata.get_object_route(key)? {
            Some(route)
                if route
                    .replicas
                    .iter()
                    .any(|candidate| replica_has_same_allocation_identity(candidate, replica)) =>
            {
                Ok(RouteReplicaReference::Referenced(Box::new(route)))
            }
            Some(_) | None => Ok(RouteReplicaReference::Absent),
        }
    }

    fn compare_and_swap_object_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let result = self
            .metadata
            .compare_and_swap_object_route(key, expected, next);
        if let Ok(cas) = &result {
            record_cas_outcome(cas);
        }
        result
    }

    fn list_routes_by_replica_owner(
        &self,
        _observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        let mut routes = self
            .metadata
            .list_object_routes()?
            .into_iter()
            .filter(|route| route.replicas.iter().any(|replica| replica.owner == *owner))
            .take(DEFAULT_ROUTE_OWNER_PAGE_SIZE.saturating_add(1))
            .collect::<Vec<_>>();
        if routes.len() > DEFAULT_ROUTE_OWNER_PAGE_SIZE {
            return Err(mooncake_store_core::StoreError::Unsupported(
                "MetadataOnly owner-route listing exceeds the fixed compatibility limit"
                    .to_string(),
            ));
        }
        routes.sort_by(|left, right| left.key.0.cmp(&right.key.0));
        Ok(routes)
    }

    fn list_routes_in_scope(
        &self,
        _observer: &ClientLease,
        scope: &NamespaceScope,
    ) -> Result<Vec<ObjectRoute>> {
        self.metadata.list_object_routes_in_scope(scope)
    }

    fn list_reuse_candidates(
        &self,
        _observer: &ClientLease,
        reuse: &ReuseIdentity,
    ) -> Result<Vec<ObjectRoute>> {
        self.metadata.list_reuse_candidates(reuse)
    }
}

fn replica_has_same_allocation_identity(candidate: &ReplicaRoute, expected: &ReplicaRoute) -> bool {
    candidate.owner == expected.owner
        && candidate.segment_name == expected.segment_name
        && candidate.segment_offset == expected.segment_offset
        && candidate.allocation_id == expected.allocation_id
        && expected
            .allocation_id
            .is_some_and(|allocation_id| allocation_id != 0)
}
