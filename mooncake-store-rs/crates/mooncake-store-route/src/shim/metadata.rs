use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientLease, ClientRuntimeId, MetadataBackend, NamespaceScope, ObjectKey,
    ObjectRoute, Result, ReuseIdentity, RouteDirectory, RouteVersion,
};

use crate::metrics::record_cas_outcome;

pub(crate) fn build_metadata_route_directory(
    metadata: Arc<dyn MetadataBackend>,
) -> Arc<dyn RouteDirectory> {
    Arc::new(MetadataRouteDirectory { metadata })
}

struct MetadataRouteDirectory {
    metadata: Arc<dyn MetadataBackend>,
}

impl RouteDirectory for MetadataRouteDirectory {
    fn get_object_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        self.metadata.get_object_route(key)
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
            record_cas_outcome(cas, next, key);
        }
        result
    }

    fn list_routes_by_replica_owner(
        &self,
        _observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        Ok(self
            .metadata
            .list_object_routes()?
            .into_iter()
            .filter(|route| route.replicas.iter().any(|replica| replica.owner == *owner))
            .collect())
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
