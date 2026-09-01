use mooncake_store_core::{MetadataBackend, NofBackingRouteFilter, ObjectRoute, Result};
use redis::Commands;

use super::{bounded_set_members, json_error, metadata_error, RedisMetadataBackend};

fn route_matches_nof_backing_filter(route: &ObjectRoute, filter: &NofBackingRouteFilter) -> bool {
    let Some(backing) = route.nof_backing.as_ref() else {
        return false;
    };
    if filter
        .target_id
        .as_ref()
        .is_some_and(|target_id| backing.target_id != *target_id)
    {
        return false;
    }
    if filter.state.is_some_and(|state| backing.state != state) {
        return false;
    }
    if filter
        .owner
        .as_ref()
        .is_some_and(|owner| backing.owner != *owner)
    {
        return false;
    }
    true
}

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

    pub(super) fn nof_backing_index_keys(&self, route: Option<&ObjectRoute>) -> [String; 3] {
        let target_index = route
            .and_then(|route| route.nof_backing.as_ref())
            .map(|backing| self.keyspace.object_nof_target_index(&backing.target_id))
            .unwrap_or_default();
        let state_index = route
            .and_then(|route| route.nof_backing.as_ref())
            .map(|backing| self.keyspace.object_nof_backing_state_index(backing.state))
            .unwrap_or_default();
        let owner_index = route
            .and_then(|route| route.nof_backing.as_ref())
            .map(|backing| self.keyspace.object_nof_backing_owner_index(&backing.owner))
            .unwrap_or_default();
        [target_index, state_index, owner_index]
    }

    pub(super) fn redis_list_object_routes_by_nof_backing(
        &self,
        filter: &NofBackingRouteFilter,
    ) -> Result<Vec<ObjectRoute>> {
        let Some(index) = self.nof_backing_filter_index(filter) else {
            return MetadataBackend::list_object_routes_by_nof_backing(self, filter);
        };
        let keys =
            self.query_readonly("redis list object routes by NoF backing", |connection| {
                bounded_set_members(connection, &index, "redis sscan NoF backing route index")
                    .map_err(super::store_error_to_redis_error)
            })?;
        let entries =
            self.query_readonly("redis load NoF backing object routes", |connection| {
                let mut entries = Vec::with_capacity(keys.len());
                for key in &keys {
                    let payload: Option<String> = redis::cmd("HGET")
                        .arg(key.as_str())
                        .arg("payload")
                        .query(connection)?;
                    entries.push((key.clone(), payload));
                }
                Ok(entries)
            })?;
        let mut stale_keys = Vec::new();
        let mut routes = Vec::new();
        for (key, payload) in entries {
            if let Some(payload) = payload {
                routes.push(serde_json::from_str(&payload).map_err(json_error)?);
            } else {
                stale_keys.push(key);
            }
        }
        if !stale_keys.is_empty() {
            let mut connection = self.connection("redis prune stale NoF backing route index")?;
            connection
                .srem::<_, _, ()>(&index, stale_keys)
                .map_err(|error| metadata_error("redis srem NoF backing route index", error))?;
        }
        Ok(routes
            .into_iter()
            .filter(|route| route_matches_nof_backing_filter(route, filter))
            .take(filter.limit.unwrap_or(usize::MAX))
            .collect())
    }
}
