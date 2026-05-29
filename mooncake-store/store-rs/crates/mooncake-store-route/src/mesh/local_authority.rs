use std::sync::Arc;

use mooncake_store_core::{
    CasResult, ClientRuntimeId, ClientStableId, ObjectKey, ObjectRoute, Result, RouteCasRequest,
    RouteVersion,
};

use super::registry::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_contains_many,
    authority_get, authority_get_many, authority_list_routes, authority_list_routes_by_replica_owner,
    authority_replace, authority_replace_many, bind_local_authority_service,
};
use crate::shim::RouteAuthorityService;

#[derive(Clone, Debug)]
pub struct LocalRouteAuthority {
    namespace: String,
    authority: ClientStableId,
}

impl LocalRouteAuthority {
    pub fn new(namespace: impl Into<String>, authority: ClientStableId) -> Self {
        Self {
            namespace: namespace.into(),
            authority,
        }
    }

    pub fn bind_service(&self, service: Arc<dyn RouteAuthorityService>) {
        bind_local_authority_service(&self.namespace, &self.authority, service);
    }

    pub fn get_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        authority_get(&self.namespace, &self.authority, key)
    }

    pub fn get_routes(&self, keys: &[ObjectKey]) -> Result<Vec<Option<ObjectRoute>>> {
        authority_get_many(&self.namespace, &self.authority, keys)
    }

    pub fn contains_routes(&self, keys: &[ObjectKey]) -> Result<Vec<bool>> {
        authority_contains_many(&self.namespace, &self.authority, keys)
    }

    pub fn list_routes(&self) -> Result<Vec<ObjectRoute>> {
        authority_list_routes(&self.namespace, &self.authority)
    }

    pub fn list_routes_by_replica_owner(
        &self,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        authority_list_routes_by_replica_owner(&self.namespace, &self.authority, owner)
    }

    pub fn compare_and_swap_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        authority_compare_and_swap(&self.namespace, &self.authority, key, expected, next)
    }

    pub fn compare_and_swap_routes(&self, requests: &[RouteCasRequest]) -> Result<Vec<CasResult>> {
        authority_compare_and_swap_many(&self.namespace, &self.authority, requests)
    }

    pub fn replace_route(&self, key: &ObjectKey, next: Option<&ObjectRoute>) -> Result<()> {
        authority_replace(&self.namespace, &self.authority, key, next)
    }

    pub fn replace_routes(&self, requests: &[RouteCasRequest]) -> Result<()> {
        authority_replace_many(&self.namespace, &self.authority, requests)
    }
}
