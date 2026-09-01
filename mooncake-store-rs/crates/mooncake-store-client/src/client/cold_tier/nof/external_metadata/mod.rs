//! NoF external metadata view over Mooncake's shared metadata backend.

#[cfg(test)]
mod tests;

use mooncake_store_core::{
    CasResult, MetadataBackend, NofBackingRouteFilter, ObjectKey, ObjectRoute, Result, RouteVersion,
};

/// NoF-specific route operations backed by the existing Mooncake metadata service.
///
/// This extension adds no database, journal, or cache. It provides a typed NoF
/// boundary and prevents a route from being published as both a local Cold Tier
/// backing and a NoF backing.
pub trait NofExternalMetadata: Send + Sync {
    fn get_nof_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>>;

    fn list_nof_object_routes(&self, filter: &NofBackingRouteFilter) -> Result<Vec<ObjectRoute>>;

    fn compare_and_swap_nof_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;
}

impl<T> NofExternalMetadata for T
where
    T: MetadataBackend + ?Sized,
{
    fn get_nof_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        Ok(self
            .get_object_route(key)?
            .filter(|route| route.nof_backing.is_some()))
    }

    fn list_nof_object_routes(&self, filter: &NofBackingRouteFilter) -> Result<Vec<ObjectRoute>> {
        self.list_object_routes_by_nof_backing(filter)
    }

    fn compare_and_swap_nof_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        if let Some(route) = next {
            route.validate_backing_kind()?;
            if route.nof_backing.is_none() {
                return Err(mooncake_store_core::StoreError::InvalidState(
                    "NoF route CAS requires nof_backing metadata".to_string(),
                ));
            }
        }
        self.compare_and_swap_object_route(key, expected, next)
    }
}
