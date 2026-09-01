use std::collections::BTreeSet;

use mooncake_store_core::{MetadataBackend, NofBackingRouteFilter, ObjectRoute, Result};
use redis::Commands;

use super::{bounded_set_members, json_error, metadata_error, RedisMetadataBackend};

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
        let mut routes: Vec<ObjectRoute> = Vec::new();
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
