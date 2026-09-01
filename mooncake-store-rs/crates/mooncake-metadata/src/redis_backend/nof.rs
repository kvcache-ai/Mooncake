use std::collections::BTreeSet;

use mooncake_store_core::{MetadataBackend, NofBackingRouteFilter, ObjectRoute, Result};

use super::RedisMetadataBackend;

impl RedisMetadataBackend {
    pub(super) fn nof_backing_filter_index(
        &self,
        filter: &NofBackingRouteFilter,
    ) -> Option<String> {
        if let Some(target_id) = filter.target_id.as_deref() {
            return Some(self.keyspace.object_nof_target_index(target_id));
        }
        if let Some(state) = filter.state {
            return Some(self.keyspace.object_nof_backing_state_index(state));
        }
        filter
            .owner
            .as_ref()
            .map(|owner| self.keyspace.object_nof_backing_owner_index(owner))
    }

    pub(super) fn nof_backing_index_keys(&self, route: Option<&ObjectRoute>) -> Vec<String> {
        let Some(backing) = route.and_then(|route| route.nof_backing.as_ref()) else {
            return Vec::new();
        };
        let mut indexes = BTreeSet::new();
        indexes.insert(self.keyspace.object_nof_backing_state_index(backing.state));
        for target in backing.all_targets() {
            indexes.insert(self.keyspace.object_nof_target_index(target.target_id));
            indexes.insert(self.keyspace.object_nof_backing_owner_index(target.owner));
        }
        indexes.into_iter().collect()
    }

    pub(super) fn redis_list_object_routes_by_nof_backing(
        &self,
        filter: &NofBackingRouteFilter,
    ) -> Result<Vec<ObjectRoute>> {
        let Some(index) = self.nof_backing_filter_index(filter) else {
            return MetadataBackend::list_object_routes_by_nof_backing(self, filter);
        };
        Ok(self
            .load_indexed_object_routes(
                &index,
                "redis sscan NoF backing route index",
                "redis load NoF backing object routes",
                "redis srem NoF backing route index",
            )?
            .into_iter()
            .filter(|route| {
                route
                    .nof_backing
                    .as_ref()
                    .is_some_and(|backing| backing.matches_filter(filter))
            })
            .take(filter.limit.unwrap_or(usize::MAX))
            .collect())
    }
}
