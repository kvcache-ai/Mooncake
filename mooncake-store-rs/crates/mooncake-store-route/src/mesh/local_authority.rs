use std::sync::Arc;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientRuntimeId, ClientStableId, ObjectKey, ObjectRoute, Result, RouteCasRequest, RouteVersion,
};

use super::registry::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_contains_many,
    authority_get, authority_get_allocation_route_reference_keys,
    authority_get_allocation_route_references, authority_get_many, authority_get_record,
    authority_get_records, authority_list_routes, authority_list_routes_by_replica_owner,
    authority_list_routes_by_replica_owner_page, authority_replace, authority_replace_many,
    bind_local_authority_service,
};
use crate::shim::RouteAuthorityService;
use crate::{RouteAuthorityRecord, RouteOwnerPage};

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

    pub fn get_route_record(&self, key: &ObjectKey) -> Result<RouteAuthorityRecord> {
        authority_get_record(&self.namespace, &self.authority, key)
    }

    pub fn get_route_records(&self, keys: &[ObjectKey]) -> Result<Vec<RouteAuthorityRecord>> {
        authority_get_records(&self.namespace, &self.authority, keys)
    }

    pub fn contains_routes(&self, keys: &[ObjectKey]) -> Result<Vec<bool>> {
        authority_contains_many(&self.namespace, &self.authority, keys)
    }

    pub fn list_routes(&self) -> Result<Vec<ObjectRoute>> {
        authority_list_routes(&self.namespace, &self.authority)
    }

    pub fn get_allocation_route_references(
        &self,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        authority_get_allocation_route_references(&self.namespace, &self.authority, identities)
    }

    pub fn get_allocation_route_reference_keys(
        &self,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        authority_get_allocation_route_reference_keys(&self.namespace, &self.authority, identities)
    }

    pub fn list_routes_by_replica_owner(
        &self,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        authority_list_routes_by_replica_owner(&self.namespace, &self.authority, owner)
    }

    pub fn list_routes_by_replica_owner_page(
        &self,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        snapshot_epoch: Option<u64>,
        limit: usize,
    ) -> Result<RouteOwnerPage> {
        authority_list_routes_by_replica_owner_page(
            &self.namespace,
            &self.authority,
            owner,
            cursor,
            snapshot_epoch,
            limit,
        )
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
