use mooncake_store_core::{
    CasResult, ColdBackingRouteFilter, ColdTierDeviceFilter, ColdTierDeviceRecord,
    ColdTierDeviceUpdate, ColdTierPutDeviceResult, ColdTierUsageDelta, MetadataBackend, ObjectKey,
    ObjectRoute, Result, RouteVersion, StoreError,
};
use redis::{Commands, Script};

use crate::cold_tier::cold_tier_device_matches_filter;

use super::{json_error, metadata_error, scan_keys, RedisMetadataBackend, CAS_OBJECT_ROUTE_SCRIPT};

const APPLY_COLD_TIER_USAGE_DELTA_SCRIPT: &str = r#"
local key = KEYS[1]
local used_delta = tonumber(ARGV[1])
local reserved_delta = tonumber(ARGV[2])
local updated_at_ms = tonumber(ARGV[3])

local payload = redis.call('GET', key)
if not payload then
    return {0, ''}
end
local device = cjson.decode(payload)
local tags = device['tags']
local used = tonumber(device['used_bytes'] or 0) + used_delta
local reserved = tonumber(device['reserved_bytes'] or 0) + reserved_delta
if used < 0 or reserved < 0 then
    return {1, payload}
end
device['used_bytes'] = used
device['reserved_bytes'] = reserved
device['updated_at_ms'] = updated_at_ms
if tags and next(tags) == nil then
    device['tags'] = cjson.empty_array
end
local next_payload = cjson.encode(device)
redis.call('SET', key, next_payload)
return {2, next_payload}
"#;

fn route_matches_cold_backing_filter(route: &ObjectRoute, filter: &ColdBackingRouteFilter) -> bool {
    let Some(backing) = route.cold_backing.as_ref() else {
        return false;
    };
    if filter
        .device_id
        .as_ref()
        .is_some_and(|device_id| backing.cold_tier_id != *device_id)
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
    pub(super) fn cold_backing_filter_index(
        &self,
        filter: &ColdBackingRouteFilter,
    ) -> Option<String> {
        if let Some(device_id) = filter.device_id.as_deref() {
            return Some(self.keyspace.object_cold_tier_device_index(device_id));
        }
        if let Some(state) = filter.state {
            return Some(self.keyspace.object_cold_backing_state_index(state));
        }
        filter
            .owner
            .as_ref()
            .map(|owner| self.keyspace.object_cold_backing_owner_index(owner))
    }

    pub(super) fn cold_backing_index_keys(&self, route: Option<&ObjectRoute>) -> [String; 3] {
        let device_index = route
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| {
                self.keyspace
                    .object_cold_tier_device_index(&backing.cold_tier_id)
            })
            .unwrap_or_default();
        let state_index = route
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| self.keyspace.object_cold_backing_state_index(backing.state))
            .unwrap_or_default();
        let owner_index = route
            .and_then(|route| route.cold_backing.as_ref())
            .map(|backing| {
                self.keyspace
                    .object_cold_backing_owner_index(&backing.owner)
            })
            .unwrap_or_default();
        [device_index, state_index, owner_index]
    }

    pub(super) fn redis_list_object_routes_by_cold_backing(
        &self,
        filter: &ColdBackingRouteFilter,
    ) -> Result<Vec<ObjectRoute>> {
        let Some(index) = self.cold_backing_filter_index(filter) else {
            return MetadataBackend::list_object_routes_by_cold_backing(self, filter);
        };
        let keys = self
            .query_readonly("redis list object routes by cold backing", |connection| {
                connection.smembers::<_, Vec<String>>(&index)
            })?;
        let entries =
            self.query_readonly("redis load cold backing object routes", |connection| {
                let mut entries = Vec::with_capacity(keys.len());
                for key in keys.iter() {
                    let payload: Option<String> = redis::cmd("HGET")
                        .arg(key.as_str())
                        .arg("payload")
                        .query(connection)?;
                    entries.push((key.clone(), payload));
                }
                Ok(entries)
            })?;
        let mut stale_keys = Vec::new();
        let mut live_keys = Vec::new();
        let mut routes = Vec::new();
        for (key, payload) in entries {
            if let Some(payload) = payload {
                live_keys.push(key);
                routes.push(serde_json::from_str(&payload).map_err(json_error)?);
            } else {
                stale_keys.push(key);
            }
        }
        if !stale_keys.is_empty() {
            let mut connection = self.connection("redis prune stale cold backing route index")?;
            connection
                .srem::<_, _, ()>(&index, stale_keys)
                .map_err(|error| metadata_error("redis srem cold backing route index", error))?;
        }
        if keys.is_empty() || routes.is_empty() {
            let all_routes = self.list_object_routes()?;
            routes = all_routes
                .into_iter()
                .filter(|route| route_matches_cold_backing_filter(route, filter))
                .take(filter.limit.unwrap_or(usize::MAX))
                .collect();
            if !routes.is_empty() {
                let index_keys = routes
                    .iter()
                    .map(|route| self.keyspace.object(&route.key))
                    .collect::<Vec<_>>();
                let mut connection = self.connection("redis backfill cold backing route index")?;
                connection
                    .sadd::<_, _, ()>(&index, index_keys)
                    .map_err(|error| {
                        metadata_error("redis sadd cold backing route index", error)
                    })?;
            }
            return Ok(routes);
        }
        if !live_keys.is_empty() {
            let mut connection = self.connection("redis refresh cold backing route index")?;
            connection
                .sadd::<_, _, ()>(&index, live_keys)
                .map_err(|error| metadata_error("redis sadd cold backing route index", error))?;
        }
        Ok(routes
            .into_iter()
            .filter(|route| route_matches_cold_backing_filter(route, filter))
            .take(filter.limit.unwrap_or(usize::MAX))
            .collect())
    }

    pub(super) fn redis_compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        if let Some(route) = next {
            route.validate_backing_kind()?;
        }
        let mut connection = self.connection("redis cas object route")?;
        let payload = next
            .map(|route| serde_json::to_string(route).map_err(json_error))
            .transpose()?;
        let next_version = next.map(|route| route.version.0).unwrap_or_default();
        let object_key = self.keyspace.object(key);
        let current = self.get_object_route(key)?;
        let [old_device_index, old_state_index, old_owner_index] =
            self.cold_backing_index_keys(current.as_ref());
        let [new_device_index, new_state_index, new_owner_index] =
            self.cold_backing_index_keys(next);
        let [old_nof_target_index, old_nof_state_index, old_nof_owner_index] =
            self.nof_backing_index_keys(current.as_ref());
        let [new_nof_target_index, new_nof_state_index, new_nof_owner_index] =
            self.nof_backing_index_keys(next);
        let current_payload = Script::new(CAS_OBJECT_ROUTE_SCRIPT)
            .key(&object_key)
            .key(self.keyspace.object_index())
            .key(old_device_index.as_str())
            .key(old_state_index.as_str())
            .key(old_owner_index.as_str())
            .key(new_device_index.as_str())
            .key(new_state_index.as_str())
            .key(new_owner_index.as_str())
            .key(old_nof_target_index.as_str())
            .key(old_nof_state_index.as_str())
            .key(old_nof_owner_index.as_str())
            .key(new_nof_target_index.as_str())
            .key(new_nof_state_index.as_str())
            .key(new_nof_owner_index.as_str())
            .arg(
                expected
                    .map(|version| version.0.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(next_version.to_string())
            .arg(payload.clone().unwrap_or_else(|| "__delete__".to_string()))
            .arg(if old_device_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if old_state_index.is_empty() { "0" } else { "1" })
            .arg(if old_owner_index.is_empty() { "0" } else { "1" })
            .arg(if new_device_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if new_state_index.is_empty() { "0" } else { "1" })
            .arg(if new_owner_index.is_empty() { "0" } else { "1" })
            .arg(if old_nof_target_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if old_nof_state_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if old_nof_owner_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if new_nof_target_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if new_nof_state_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .arg(if new_nof_owner_index.is_empty() {
                "0"
            } else {
                "1"
            })
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis cas object route", error))?;

        let current: Option<ObjectRoute> = if current_payload.1.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&current_payload.1).map_err(json_error)?)
        };
        Ok(CasResult {
            applied: current_payload.0 == 1,
            current,
            version_floor: None,
        })
    }

    pub(super) fn redis_put_cold_tier_device_if_absent(
        &self,
        device: &ColdTierDeviceRecord,
    ) -> Result<ColdTierPutDeviceResult> {
        let key = self.keyspace.cold_tier_device(&device.device_id);
        let payload = serde_json::to_string(device).map_err(json_error)?;
        let mut connection = self.connection("redis create cold tier device")?;
        let inserted: bool = connection
            .set_nx(key.as_str(), payload.as_str())
            .map_err(|error| metadata_error("redis setnx cold tier device", error))?;
        if inserted {
            connection
                .sadd::<_, _, ()>(self.keyspace.cold_tier_device_index(), key)
                .map_err(|error| metadata_error("redis sadd cold tier device index", error))?;
            return Ok(ColdTierPutDeviceResult::Created(device.clone()));
        }
        let payload: String = connection
            .get(key.as_str())
            .map_err(|error| metadata_error("redis get existing cold tier device", error))?;
        Ok(ColdTierPutDeviceResult::Existing(
            serde_json::from_str(&payload).map_err(json_error)?,
        ))
    }

    pub(super) fn redis_get_cold_tier_device(
        &self,
        device_id: &str,
    ) -> Result<Option<ColdTierDeviceRecord>> {
        let key = self.keyspace.cold_tier_device(device_id);
        let mut connection = self.connection("redis get cold tier device")?;
        let payload: Option<String> = connection
            .get(key)
            .map_err(|error| metadata_error("redis get cold tier device", error))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    pub(super) fn redis_list_cold_tier_devices(
        &self,
        filter: &ColdTierDeviceFilter,
    ) -> Result<Vec<ColdTierDeviceRecord>> {
        let index = self.keyspace.cold_tier_device_index();
        let prefix = self.keyspace.cold_tier_device_prefix();
        let mut connection = self.connection("redis list cold tier devices")?;
        let mut keys: Vec<String> = connection
            .smembers(&index)
            .map_err(|error| metadata_error("redis smembers cold tier device index", error))?;
        if keys.is_empty() {
            keys = scan_keys(&mut connection, &format!("{}*", prefix))?;
            if !keys.is_empty() {
                connection
                    .sadd::<_, _, ()>(&index, keys.clone())
                    .map_err(|error| metadata_error("redis sadd cold tier device index", error))?;
            }
        }
        let mut stale = Vec::new();
        let mut devices = Vec::new();
        for key in keys {
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get cold tier device list entry", error))?;
            let Some(payload) = payload else {
                stale.push(key);
                continue;
            };
            let device: ColdTierDeviceRecord =
                serde_json::from_str(&payload).map_err(json_error)?;
            if cold_tier_device_matches_filter(&device, filter) {
                devices.push(device);
            }
        }
        if !stale.is_empty() {
            connection
                .srem::<_, _, ()>(&index, stale)
                .map_err(|error| {
                    metadata_error("redis srem stale cold tier device index", error)
                })?;
        }
        devices.sort_by(|left, right| left.device_id.cmp(&right.device_id));
        Ok(devices)
    }

    pub(super) fn redis_update_cold_tier_device(
        &self,
        device_id: &str,
        update: ColdTierDeviceUpdate,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.keyspace.cold_tier_device(device_id);
        let mut connection = self.connection("redis update cold tier device")?;
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get cold tier device for update", error))?;
        let Some(payload) = payload else {
            return Err(StoreError::NotFound(format!(
                "cold tier device {device_id} not found"
            )));
        };
        let mut device: ColdTierDeviceRecord =
            serde_json::from_str(&payload).map_err(json_error)?;
        if let Some(expected) = update.expected_updated_at_ms {
            if device.updated_at_ms != expected {
                return Err(StoreError::Conflict(format!(
                    "cold tier device {device_id} changed concurrently"
                )));
            }
        }
        update.apply(&mut device);
        let payload = serde_json::to_string(&device).map_err(json_error)?;
        connection
            .set::<_, _, ()>(key, payload)
            .map_err(|error| metadata_error("redis set cold tier device", error))?;
        Ok(device)
    }

    pub(super) fn redis_apply_cold_tier_usage_delta(
        &self,
        device_id: &str,
        delta: ColdTierUsageDelta,
        updated_at_ms: u64,
    ) -> Result<ColdTierDeviceRecord> {
        let key = self.keyspace.cold_tier_device(device_id);
        let script = Script::new(APPLY_COLD_TIER_USAGE_DELTA_SCRIPT);
        let mut connection = self.connection("redis apply cold tier usage delta")?;
        let response: (u8, String) = script
            .key(key)
            .arg(delta.used_bytes)
            .arg(delta.reserved_bytes)
            .arg(updated_at_ms)
            .invoke(&mut connection)
            .map_err(|error| metadata_error("redis apply cold tier usage delta", error))?;
        match response.0 {
            0 => Err(StoreError::NotFound(format!(
                "cold tier device {device_id} not found"
            ))),
            1 => Err(StoreError::InvalidState(format!(
                "cold tier device {device_id} usage delta would make accounting negative"
            ))),
            2 => serde_json::from_str(&response.1).map_err(json_error),
            status => Err(StoreError::Transport(format!(
                "unexpected redis cold tier usage delta status {status}"
            ))),
        }
    }
}
