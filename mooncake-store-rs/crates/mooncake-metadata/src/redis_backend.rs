use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_store_core::{
    CasResult, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId,
    HandoffPlan, MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy, RoutePolicyDomain,
    RouteVersion, SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation,
    StoreError, TenantObjectAccounting, TenantObjectAccountingState, TenantPolicy,
    TenantPolicyScope, TenantQuotaAbortOutcome, TenantQuotaFinalizeOutcome,
    TenantQuotaFinalizeRequest, TenantQuotaReservation, TenantQuotaReservationOutcome,
    TenantQuotaReservationRequest, TenantQuotaState,
};
use redis::cmd;
use redis::{Commands, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo, Script};

use crate::keyspace::{parse_route_policy_domain, parse_tenant_policy_scope};
use crate::segment_state::StoredSegmentState;
use crate::MetadataKeyspace;

const CAS_OBJECT_ROUTE_SCRIPT: &str = r#"
local key = KEYS[1]
local expected = ARGV[1]
local next_version = ARGV[2]
local payload = ARGV[3]

local current_payload = redis.call('HGET', key, 'payload')
local current_version = redis.call('HGET', key, 'version')

if expected == '__none__' then
    if current_payload then
        return {0, current_payload}
    end
else
    if (not current_version) or current_version ~= expected then
        return {0, current_payload or ''}
    end
end

if payload == '__delete__' then
    redis.call('DEL', key)
    return {1, ''}
end

redis.call('HSET', key, 'version', next_version, 'payload', payload)
return {1, payload}
"#;

const RESERVE_SEGMENT_SCRIPT: &str = r#"
local key = KEYS[1]
local requested = tonumber(ARGV[1])

local payload = redis.call('HGET', key, 'state_json')
if not payload then
    return {0, -1}
end

local state = cjson.decode(payload)
local announcement = state["announcement"]
if announcement["state"] ~= "Active" then
    return {0, -2}
end

local alignment = tonumber(announcement["alignment_bytes"] or 1)
if alignment < 1 then
    alignment = 1
end

local function align_up(value, align)
    if align <= 1 then
        return value
    end
    local rem = value % align
    if rem == 0 then
        return value
    end
    return value + (align - rem)
end

local reserved = align_up(requested, alignment)
local offset = nil
local free_spans = state["free_spans"] or {}
for index, span in ipairs(free_spans) do
    if tonumber(span["length_bytes"]) >= reserved then
        offset = tonumber(span["offset_bytes"])
        local remaining = tonumber(span["length_bytes"]) - reserved
        if remaining > 0 then
            span["offset_bytes"] = offset + reserved
            span["length_bytes"] = remaining
        else
            table.remove(free_spans, index)
        end
        break
    end
end

if offset == nil then
    local cursor = tonumber(state["cursor_bytes"] or 0)
    offset = align_up(cursor, alignment)
    local next_cursor = offset + reserved
    local capacity = tonumber(announcement["capacity_bytes"])
    if next_cursor > capacity then
        return {0, capacity - offset}
    end
    state["cursor_bytes"] = next_cursor
end

announcement["used_bytes"] = tonumber(announcement["used_bytes"] or 0) + reserved
state["announcement"] = announcement
state["free_spans"] = free_spans

local next_payload = cjson.encode(state)
redis.call('HSET', key,
    'state_json', next_payload,
    'used_bytes', tostring(announcement["used_bytes"]),
    'state', announcement["state"],
    'alignment_bytes', tostring(announcement["alignment_bytes"] or 1),
    'capacity_bytes', tostring(announcement["capacity_bytes"]))
return {1, offset}
"#;

const RELEASE_SEGMENT_SCRIPT: &str = r#"
local key = KEYS[1]
local offset = tonumber(ARGV[1])
local requested = tonumber(ARGV[2])

local payload = redis.call('HGET', key, 'state_json')
if not payload then
    return {0, -1}
end

local state = cjson.decode(payload)
local announcement = state["announcement"]
local alignment = tonumber(announcement["alignment_bytes"] or 1)
if alignment < 1 then
    alignment = 1
end

local function align_up(value, align)
    if align <= 1 then
        return value
    end
    local rem = value % align
    if rem == 0 then
        return value
    end
    return value + (align - rem)
end

local reserved = align_up(requested, alignment)
local capacity = tonumber(announcement["capacity_bytes"])
if offset + reserved > capacity then
    return {0, -2}
end

local free_spans = state["free_spans"] or {}
table.insert(free_spans, { offset_bytes = offset, length_bytes = reserved })
table.sort(free_spans, function(left, right)
    return tonumber(left["offset_bytes"]) < tonumber(right["offset_bytes"])
end)

local merged = {}
for _, span in ipairs(free_spans) do
    local current_offset = tonumber(span["offset_bytes"])
    local current_length = tonumber(span["length_bytes"])
    local previous = merged[#merged]
    if previous then
        local previous_end = tonumber(previous["offset_bytes"]) + tonumber(previous["length_bytes"])
        if previous_end >= current_offset then
            local merged_end = math.max(previous_end, current_offset + current_length)
            previous["length_bytes"] = merged_end - tonumber(previous["offset_bytes"])
        else
            table.insert(merged, { offset_bytes = current_offset, length_bytes = current_length })
        end
    else
        table.insert(merged, { offset_bytes = current_offset, length_bytes = current_length })
    end
end

local cursor = tonumber(state["cursor_bytes"] or 0)
while #merged > 0 do
    local last = merged[#merged]
    local last_end = tonumber(last["offset_bytes"]) + tonumber(last["length_bytes"])
    if last_end ~= cursor then
        break
    end
    cursor = tonumber(last["offset_bytes"])
    table.remove(merged, #merged)
end

local used = tonumber(announcement["used_bytes"] or 0) - reserved
if used < 0 then
    used = 0
end
announcement["used_bytes"] = used
if used == 0 then
    cursor = 0
    merged = {}
end

state["announcement"] = announcement
state["cursor_bytes"] = cursor
state["free_spans"] = merged
local next_payload = cjson.encode(state)
redis.call('HSET', key,
    'state_json', next_payload,
    'used_bytes', tostring(announcement["used_bytes"]),
    'state', announcement["state"],
    'alignment_bytes', tostring(announcement["alignment_bytes"] or 1),
    'capacity_bytes', tostring(announcement["capacity_bytes"]))
return {1, used}
"#;

const CAS_TENANT_POLICY_SCRIPT: &str = r#"
local key = KEYS[1]
local expected = ARGV[1]
local payload = ARGV[2]

local current_payload = redis.call('GET', key)
if not current_payload then
    if expected ~= '__none__' then
        return {0, ''}
    end
else
    local current = cjson.decode(current_payload)
    local current_version = tonumber(current['version'])
    local expected_version = tonumber(expected)
    if expected == '__none__' or current_version ~= expected_version then
        return {0, current_payload}
    end
end

if payload == '__delete__' then
    redis.call('DEL', key)
    return {1, current_payload or ''}
end

redis.call('SET', key, payload)
return {1, payload}
"#;

const ALLOCATE_CLIENT_LEASE_SCRIPT: &str = r#"
local lease_key_prefix = KEYS[1]
local by_stable_index = KEYS[2]
local hwm_key = KEYS[3]
local global_index = KEYS[4]
local payload_template = ARGV[1]
local ttl_ms = tonumber(ARGV[2])

local active_max = 0
local members = redis.call('SMEMBERS', by_stable_index)
for _, member in ipairs(members) do
    local candidate = tonumber(member)
    if candidate and candidate > active_max then
        active_max = candidate
    end
end

local hwm_raw = redis.call('GET', hwm_key)
local hwm_value = 0
if hwm_raw then
    hwm_value = tonumber(hwm_raw) or 0
end

local floor = active_max
if hwm_value > floor then
    floor = hwm_value
end
local new_epoch = floor + 1

local lease = cjson.decode(payload_template)
lease['runtime']['epoch'] = new_epoch
local payload = cjson.encode(lease)

local lease_key = lease_key_prefix .. tostring(new_epoch)
redis.call('SET', lease_key, payload, 'PX', ttl_ms)
redis.call('SADD', global_index, lease_key)
redis.call('SADD', by_stable_index, tostring(new_epoch))
redis.call('SET', hwm_key, tostring(new_epoch))
return tostring(new_epoch)
"#;

const UPSERT_CLIENT_LEASE_STRICT_GREATER_SCRIPT: &str = r#"
local lease_key = KEYS[1]
local by_stable_index = KEYS[2]
local hwm_key = KEYS[3]
local global_index = KEYS[4]
local new_epoch = tonumber(ARGV[1])
local payload = ARGV[2]
local ttl_ms = tonumber(ARGV[3])

if redis.call('EXISTS', lease_key) == 1 then
    redis.call('SET', lease_key, payload, 'PX', ttl_ms)
    return {1, ''}
end

local active_max = 0
local members = redis.call('SMEMBERS', by_stable_index)
for _, member in ipairs(members) do
    local candidate = tonumber(member)
    if candidate and candidate > active_max then
        active_max = candidate
    end
end

local hwm_raw = redis.call('GET', hwm_key)
local hwm_value = 0
if hwm_raw then
    hwm_value = tonumber(hwm_raw) or 0
end

local floor = active_max
if hwm_value > floor then
    floor = hwm_value
end

if new_epoch <= floor then
    return {0, tostring(floor)}
end

redis.call('SET', lease_key, payload, 'PX', ttl_ms)
redis.call('SADD', global_index, lease_key)
redis.call('SADD', by_stable_index, tostring(new_epoch))
redis.call('SET', hwm_key, tostring(new_epoch))
return {1, ''}
"#;

const RESERVE_TENANT_QUOTA_SCRIPT: &str = r#"
local quota_key = KEYS[1]
local object_key = KEYS[2]
local reservation_key = KEYS[3]
local reservation_index_key = KEYS[4]

local reservation_id = ARGV[1]
local scope_payload = ARGV[2]
local object_storage_key = ARGV[3]
local expected_object_version = ARGV[4]
local delta_bytes = tonumber(ARGV[5])
local delta_objects = tonumber(ARGV[6])
local limit_max_bytes = ARGV[7]
local limit_max_objects = ARGV[8]
local expires_at_ms = tonumber(ARGV[9])
local created_at_ms = tonumber(ARGV[10])
local writer_runtime_payload = ARGV[11]

local function expected_version_or_nil(value)
    if value == '__none__' then
        return nil
    end
    return tonumber(value)
end

local function positive(value)
    if value > 0 then
        return value
    end
    return 0
end

local existing_reservation_payload = redis.call('GET', reservation_key)
if existing_reservation_payload then
    local reservation = cjson.decode(existing_reservation_payload)
    local existing_expected = reservation['expected_object_version']
    if reservation['state'] == 'Pending'
        and reservation['scope']['tenant'] == cjson.decode(scope_payload)['tenant']
        and reservation['key'] == object_storage_key
        and existing_expected == expected_version_or_nil(expected_object_version)
        and tonumber(reservation['delta_bytes']) == delta_bytes
        and tonumber(reservation['delta_objects']) == delta_objects then
        local quota_payload = redis.call('GET', quota_key)
        local object_payload = redis.call('GET', object_key)
        return {1, quota_payload or '', object_payload or '', existing_reservation_payload}
    end
    return {-2, existing_reservation_payload}
end

local object_payload = redis.call('GET', object_key)
local object = nil
if object_payload then
    object = cjson.decode(object_payload)
end
local actual_object_version = nil
if object then
    actual_object_version = tonumber(object['version'])
end
local expected_object = expected_version_or_nil(expected_object_version)
if actual_object_version ~= expected_object then
    return {-3, object_payload or '', '', ''}
end
if object and object['scope']['tenant'] ~= cjson.decode(scope_payload)['tenant'] then
    return {-4, object_payload or '', '', ''}
end

local quota_payload = redis.call('GET', quota_key)
local quota = nil
if quota_payload then
    quota = cjson.decode(quota_payload)
else
    quota = {
        scope = cjson.decode(scope_payload),
        version = 0,
        used_bytes = 0,
        used_objects = 0,
        pending_reserved_bytes = 0,
        pending_reserved_objects = 0,
        updated_at_ms = created_at_ms,
        updated_by = writer_runtime_payload,
    }
end

local positive_bytes = positive(delta_bytes)
local positive_objects = positive(delta_objects)
if limit_max_bytes ~= '__none__' then
    local admitted = tonumber(quota['used_bytes']) + tonumber(quota['pending_reserved_bytes']) + positive_bytes
    if admitted > tonumber(limit_max_bytes) then
        return {-5, quota_payload or cjson.encode(quota), '', ''}
    end
end
if limit_max_objects ~= '__none__' then
    local admitted = tonumber(quota['used_objects']) + tonumber(quota['pending_reserved_objects']) + positive_objects
    if admitted > tonumber(limit_max_objects) then
        return {-6, quota_payload or cjson.encode(quota), '', ''}
    end
end

quota['pending_reserved_bytes'] = tonumber(quota['pending_reserved_bytes']) + positive_bytes
quota['pending_reserved_objects'] = tonumber(quota['pending_reserved_objects']) + positive_objects
quota['version'] = tonumber(quota['version']) + 1
quota['updated_at_ms'] = created_at_ms
quota['updated_by'] = writer_runtime_payload
local next_quota_payload = cjson.encode(quota)

local reservation = {
    reservation_id = reservation_id,
    scope = cjson.decode(scope_payload),
    key = object_storage_key,
    version = 1,
    expected_object_version = expected_object,
    delta_bytes = delta_bytes,
    delta_objects = delta_objects,
    state = 'Pending',
    expires_at_ms = expires_at_ms,
    created_at_ms = created_at_ms,
    writer_runtime = cjson.decode(writer_runtime_payload),
}
local reservation_payload = cjson.encode(reservation)
redis.call('SET', quota_key, next_quota_payload)
redis.call('SET', reservation_key, reservation_payload)
redis.call('SET', reservation_index_key, reservation_key)
return {2, next_quota_payload, object_payload or '', reservation_payload}
"#;

const FINALIZE_TENANT_QUOTA_SCRIPT: &str = r#"
local quota_key = KEYS[1]
local object_key = KEYS[2]
local reservation_key = KEYS[3]

local reservation_id = ARGV[1]
local expected_object_version = ARGV[2]
local committed_length = ARGV[3]
local route_version = ARGV[4]
local object_state = ARGV[5]
local updated_at_ms = tonumber(ARGV[6])
local updated_by = ARGV[7]

local function parse_optional_number(value)
    if value == '__none__' then
        return nil
    end
    return tonumber(value)
end

local function positive(value)
    if value > 0 then
        return value
    end
    return 0
end

local function apply_signed(base, delta)
    local next = base + delta
    if next < 0 then
        return nil
    end
    return next
end

local reservation_payload = redis.call('GET', reservation_key)
if not reservation_payload then
    return {-1, ''}
end
local reservation = cjson.decode(reservation_payload)
if reservation['reservation_id'] ~= reservation_id then
    return {-2, reservation_payload}
end
if reservation['state'] == 'Finalized' then
    local quota_payload = redis.call('GET', quota_key)
    local object_payload = redis.call('GET', object_key)
    return {1, quota_payload or '', object_payload or '', reservation_payload}
end
if reservation['state'] == 'Aborted' then
    return {-3, reservation_payload}
end

local object_payload = redis.call('GET', object_key)
local object = nil
if object_payload then
    object = cjson.decode(object_payload)
end
local actual_object_version = nil
if object then
    actual_object_version = tonumber(object['version'])
end
local expected_object = parse_optional_number(expected_object_version)
if expected_object == nil then
    expected_object = reservation['expected_object_version']
end
if actual_object_version ~= expected_object then
    return {-4, object_payload or ''}
end
if object and object['scope']['tenant'] ~= reservation['scope']['tenant'] then
    return {-5, object_payload}
end

local quota_payload = redis.call('GET', quota_key)
if not quota_payload then
    return {-6, ''}
end
local quota = cjson.decode(quota_payload)
local positive_bytes = positive(tonumber(reservation['delta_bytes']))
local positive_objects = positive(tonumber(reservation['delta_objects']))
quota['pending_reserved_bytes'] = apply_signed(tonumber(quota['pending_reserved_bytes']), -positive_bytes)
quota['pending_reserved_objects'] = apply_signed(tonumber(quota['pending_reserved_objects']), -positive_objects)
quota['used_bytes'] = apply_signed(tonumber(quota['used_bytes']), tonumber(reservation['delta_bytes']))
quota['used_objects'] = apply_signed(tonumber(quota['used_objects']), tonumber(reservation['delta_objects']))
if quota['pending_reserved_bytes'] == nil or quota['pending_reserved_objects'] == nil or quota['used_bytes'] == nil or quota['used_objects'] == nil then
    return {-7, quota_payload}
end
quota['version'] = tonumber(quota['version']) + 1
quota['updated_at_ms'] = updated_at_ms
quota['updated_by'] = updated_by
local next_quota_payload = cjson.encode(quota)

local next_object_payload = ''
if object_state == 'Active' then
    local next_object = {
        key = reservation['key'],
        scope = reservation['scope'],
        version = (expected_object or 0) + 1,
        committed_length = tonumber(committed_length),
        route_version = parse_optional_number(route_version),
        state = 'Active',
        last_writer = updated_by,
        updated_at_ms = updated_at_ms,
    }
    next_object_payload = cjson.encode(next_object)
    redis.call('SET', object_key, next_object_payload)
else
    redis.call('DEL', object_key)
end

reservation['state'] = 'Finalized'
reservation['version'] = tonumber(reservation['version']) + 1
local next_reservation_payload = cjson.encode(reservation)
redis.call('SET', quota_key, next_quota_payload)
redis.call('SET', reservation_key, next_reservation_payload)
return {2, next_quota_payload, next_object_payload, next_reservation_payload}
"#;

const ABORT_TENANT_QUOTA_SCRIPT: &str = r#"
local quota_key = KEYS[1]
local reservation_key = KEYS[2]

local reservation_id = ARGV[1]

local function positive(value)
    if value > 0 then
        return value
    end
    return 0
end

local function apply_signed(base, delta)
    local next = base + delta
    if next < 0 then
        return nil
    end
    return next
end

local reservation_payload = redis.call('GET', reservation_key)
if not reservation_payload then
    return {-1, '', ''}
end
local reservation = cjson.decode(reservation_payload)
if reservation['reservation_id'] ~= reservation_id then
    return {-2, '', reservation_payload}
end
if reservation['state'] == 'Aborted' then
    local quota_payload = redis.call('GET', quota_key)
    return {1, quota_payload or '', reservation_payload}
end
if reservation['state'] == 'Finalized' then
    return {-3, '', reservation_payload}
end

local quota_payload = redis.call('GET', quota_key)
if not quota_payload then
    return {-4, '', reservation_payload}
end
local quota = cjson.decode(quota_payload)
local positive_bytes = positive(tonumber(reservation['delta_bytes']))
local positive_objects = positive(tonumber(reservation['delta_objects']))
quota['pending_reserved_bytes'] = apply_signed(tonumber(quota['pending_reserved_bytes']), -positive_bytes)
quota['pending_reserved_objects'] = apply_signed(tonumber(quota['pending_reserved_objects']), -positive_objects)
if quota['pending_reserved_bytes'] == nil or quota['pending_reserved_objects'] == nil then
    return {-5, quota_payload, reservation_payload}
end
quota['version'] = tonumber(quota['version']) + 1
quota['updated_at_ms'] = tonumber(reservation['created_at_ms'])
quota['updated_by'] = cjson.encode(reservation['writer_runtime'])
local next_quota_payload = cjson.encode(quota)

reservation['state'] = 'Aborted'
reservation['version'] = tonumber(reservation['version']) + 1
local next_reservation_payload = cjson.encode(reservation)
redis.call('SET', quota_key, next_quota_payload)
redis.call('SET', reservation_key, next_reservation_payload)
return {2, next_quota_payload, next_reservation_payload}
"#;

const REDIS_CONNECT_TIMEOUT_ENV: &str = "MC_STORE_RS_REDIS_CONNECT_TIMEOUT_MS";
const REDIS_IO_TIMEOUT_ENV: &str = "MC_STORE_RS_REDIS_IO_TIMEOUT_MS";
const REDIS_AUTH_PROBE_TIMEOUT_ENV: &str = "MC_STORE_RS_REDIS_AUTH_PROBE_TIMEOUT_MS";
const REDIS_RETRY_ATTEMPTS_ENV: &str = "MC_STORE_RS_REDIS_RETRY_ATTEMPTS";
const REDIS_RETRY_DELAY_ENV: &str = "MC_STORE_RS_REDIS_RETRY_DELAY_MS";
const DEFAULT_REDIS_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_REDIS_IO_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_REDIS_AUTH_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_REDIS_RETRY_ATTEMPTS: usize = 3;
const DEFAULT_REDIS_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone, Debug)]
pub struct RedisMetadataConfig {
    pub url: String,
    pub keyspace: MetadataKeyspace,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResolvedRedisAuth {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl ResolvedRedisAuth {
    fn from_connection_info(info: &RedisConnectionInfo) -> Self {
        Self {
            username: info.username.clone(),
            password: info.password.clone(),
        }
    }

    fn with_env_fallback(mut self) -> Self {
        if self.username.is_some() || self.password.is_some() {
            return self;
        }

        let password = std::env::var("MC_REDIS_PASSWORD")
            .ok()
            .filter(|value| !value.is_empty());
        let Some(password) = password else {
            return self;
        };

        self.password = Some(password);
        self.username = std::env::var("MC_REDIS_USERNAME")
            .ok()
            .filter(|value| !value.is_empty());
        self
    }

    fn normalize_for_endpoint(self, url: &str) -> Self {
        let Some(legacy_auth) = self.password_only_fallback() else {
            return self;
        };
        if redis_accepts_auth(url, &legacy_auth) {
            return legacy_auth;
        }
        self
    }

    fn apply_to_connection_info(&self, info: &mut RedisConnectionInfo) {
        info.username = self.username.clone();
        info.password = self.password.clone();
    }

    fn password_only_fallback(&self) -> Option<Self> {
        if self.username.is_none() || self.password.is_none() {
            return None;
        }
        Some(Self {
            username: None,
            password: self.password.clone(),
        })
    }
}

impl RedisMetadataConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            keyspace: MetadataKeyspace::default(),
        }
    }

    pub fn keyspace(mut self, keyspace: MetadataKeyspace) -> Self {
        self.keyspace = keyspace;
        self
    }
}

pub struct RedisMetadataBackend {
    client: redis::Client,
    legacy_auth_client: Option<redis::Client>,
    prefer_legacy_auth: AtomicBool,
    keyspace: MetadataKeyspace,
    route_namespace: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RedisMetadataCleanupReport {
    pub live_clients: usize,
    pub inspected_segment_keys: usize,
    pub removed_segment_keys: usize,
    pub removed_segment_index_entries: usize,
    pub removed_owner_segment_index_entries: usize,
    pub stale_missing_segment_index_entries: usize,
}

impl RedisMetadataBackend {
    pub fn new(config: RedisMetadataConfig) -> Result<Self> {
        let connection_info = redis_connection_info(&config.url)?;
        let legacy_auth_connection_info = legacy_auth_connection_info(&connection_info);
        let route_namespace = format!(
            "redis://{}#{}",
            redacted_route_namespace_source(&config.url),
            config.keyspace.prefix()
        );
        let client = redis::Client::open(connection_info)
            .map_err(|error| metadata_error("redis client open", error))?;
        let legacy_auth_client = legacy_auth_connection_info
            .map(redis::Client::open)
            .transpose()
            .map_err(|error| metadata_error("redis client open legacy auth", error))?;
        Ok(Self {
            client,
            legacy_auth_client,
            prefer_legacy_auth: AtomicBool::new(false),
            keyspace: config.keyspace,
            route_namespace,
        })
    }

    fn connection_with_client_raw(
        &self,
        client: &redis::Client,
    ) -> std::result::Result<redis::Connection, redis::RedisError> {
        let connection = client.get_connection_with_timeout(redis_connect_timeout())?;
        connection.set_read_timeout(Some(redis_io_timeout()))?;
        connection.set_write_timeout(Some(redis_io_timeout()))?;
        Ok(connection)
    }

    fn connection_raw(&self) -> std::result::Result<redis::Connection, redis::RedisError> {
        if self.prefer_legacy_auth.load(Ordering::Relaxed) {
            if let Some(client) = &self.legacy_auth_client {
                return self.connection_with_client_raw(client);
            }
        }

        match self.connection_with_client_raw(&self.client) {
            Ok(connection) => Ok(connection),
            Err(error) => {
                if !is_legacy_redis_auth_arity_error(&error.to_string()) {
                    return Err(error);
                }
                let Some(client) = &self.legacy_auth_client else {
                    return Err(error);
                };
                let connection = self.connection_with_client_raw(client)?;
                self.prefer_legacy_auth.store(true, Ordering::Relaxed);
                Ok(connection)
            }
        }
    }

    fn connection(&self) -> Result<redis::Connection> {
        self.connection_raw()
            .map_err(|error| metadata_error("redis get_connection", error))
    }

    fn query_with_retry<T, F>(&self, operation: &str, mut query: F) -> Result<T>
    where
        F: FnMut(&mut redis::Connection) -> std::result::Result<T, redis::RedisError>,
    {
        let attempts = redis_retry_attempts();
        let delay = redis_retry_delay();
        for attempt in 0..attempts {
            let mut connection = match self.connection_raw() {
                Ok(connection) => connection,
                Err(error)
                    if attempt + 1 < attempts && should_retry_transient_redis_error(&error) =>
                {
                    std::thread::sleep(delay);
                    continue;
                }
                Err(error) => return Err(metadata_error(operation, error)),
            };
            match query(&mut connection) {
                Ok(value) => return Ok(value),
                Err(error)
                    if attempt + 1 < attempts && should_retry_transient_redis_error(&error) =>
                {
                    std::thread::sleep(delay);
                }
                Err(error) => return Err(metadata_error(operation, error)),
            }
        }
        unreachable!("redis query either returns or exhausts retries");
    }

    fn query_readonly<T, F>(&self, operation: &str, mut query: F) -> Result<T>
    where
        F: FnMut(&mut redis::Connection) -> std::result::Result<T, redis::RedisError>,
    {
        self.query_with_retry(operation, |connection| query(connection))
    }

    fn query_idempotent_write<T, F>(&self, operation: &str, mut query: F) -> Result<T>
    where
        F: FnMut(&mut redis::Connection) -> std::result::Result<T, redis::RedisError>,
    {
        self.query_with_retry(operation, |connection| query(connection))
    }

    fn load_segment_state(
        &self,
        connection: &mut redis::Connection,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
    ) -> Result<StoredSegmentState> {
        let key = self.keyspace.segment(owner, segment);
        let payload: Option<String> = redis::cmd("HGET")
            .arg(&key)
            .arg("state_json")
            .query(connection)
            .map_err(|error| metadata_error("redis hget segment state", error))?;
        let payload = payload.ok_or(StoreError::NotFound(key))?;
        serde_json::from_str(&payload).map_err(json_error)
    }

    fn store_segment_state(
        &self,
        connection: &mut redis::Connection,
        state: &StoredSegmentState,
    ) -> Result<()> {
        let key = self
            .keyspace
            .segment(&state.announcement.owner, &state.announcement.segment_name);
        let payload = serde_json::to_string(state).map_err(json_error)?;
        let tags = serde_json::to_string(&state.announcement.tags).map_err(json_error)?;
        connection
            .hset_multiple::<_, _, _, ()>(
                &key,
                &[
                    ("owner", state.announcement.owner.storage_key()),
                    ("segment_name", state.announcement.segment_name.0.clone()),
                    (
                        "capacity_bytes",
                        state.announcement.capacity_bytes.to_string(),
                    ),
                    ("used_bytes", state.announcement.used_bytes.to_string()),
                    ("state", format!("{:?}", state.announcement.state)),
                    (
                        "alignment_bytes",
                        state.announcement.alignment_bytes.to_string(),
                    ),
                    ("tags_json", tags),
                    ("state_json", payload),
                ],
            )
            .map_err(|error| metadata_error("redis hset segment state", error))?;
        connection
            .sadd::<_, _, ()>(self.keyspace.segment_index(None), key.as_str())
            .map_err(|error| metadata_error("redis sadd segment index", error))?;
        connection
            .sadd::<_, _, ()>(
                self.keyspace.segment_index(Some(&state.announcement.owner)),
                key,
            )
            .map_err(|error| metadata_error("redis sadd owner segment index", error))
    }

    pub fn cleanup_stale_segments(&self) -> Result<RedisMetadataCleanupReport> {
        let live_clients = self.list_live_clients()?;
        let live_runtime_keys = live_clients
            .iter()
            .map(|lease| lease.runtime.storage_key())
            .collect::<std::collections::BTreeSet<_>>();
        let mut connection = self.connection()?;
        let global_index = self.keyspace.segment_index(None);
        let mut keys: Vec<String> = connection
            .smembers(&global_index)
            .map_err(|error| metadata_error("redis smembers segment index", error))?;
        if keys.is_empty() {
            keys = scan_keys(&mut connection, &self.keyspace.segment_pattern(None))?;
        }

        let mut report = RedisMetadataCleanupReport {
            live_clients: live_clients.len(),
            inspected_segment_keys: keys.len(),
            ..RedisMetadataCleanupReport::default()
        };
        let mut stale_global_entries = Vec::new();
        let mut stale_owner_entries = std::collections::BTreeMap::<String, Vec<String>>::new();

        for key in keys {
            let owner: Option<String> = redis::cmd("HGET")
                .arg(key.as_str())
                .arg("owner")
                .query(&mut connection)
                .map_err(|error| metadata_error("redis hget segment owner", error))?;
            let Some(owner) = owner else {
                stale_global_entries.push(key);
                report.stale_missing_segment_index_entries += 1;
                continue;
            };
            if live_runtime_keys.contains(&owner) {
                continue;
            }
            connection
                .del::<_, ()>(key.as_str())
                .map_err(|error| metadata_error("redis del stale segment", error))?;
            stale_global_entries.push(key.clone());
            stale_owner_entries.entry(owner).or_default().push(key);
            report.removed_segment_keys += 1;
        }

        if !stale_global_entries.is_empty() {
            let removed: usize = connection
                .srem(&global_index, stale_global_entries)
                .map_err(|error| metadata_error("redis srem stale segment index", error))?;
            report.removed_segment_index_entries = removed;
        }

        for (owner, keys) in stale_owner_entries {
            let owner_index = self.keyspace.segment_index_for_owner_key(&owner);
            let removed: usize = connection
                .srem(owner_index.as_str(), keys)
                .map_err(|error| metadata_error("redis srem stale owner segment index", error))?;
            report.removed_owner_segment_index_entries += removed;
            let remaining: usize = connection
                .scard(owner_index.as_str())
                .map_err(|error| metadata_error("redis scard owner segment index", error))?;
            if remaining == 0 {
                let _ = connection.del::<_, usize>(owner_index.as_str());
            }
        }

        Ok(report)
    }
}

fn redis_connection_info(url: &str) -> Result<ConnectionInfo> {
    let mut info = url
        .to_string()
        .into_connection_info()
        .map_err(|error| metadata_error("redis connection info", error))?;
    resolve_redis_auth(url)?.apply_to_connection_info(&mut info.redis);
    Ok(info)
}

fn legacy_auth_connection_info(info: &ConnectionInfo) -> Option<ConnectionInfo> {
    let legacy_auth =
        ResolvedRedisAuth::from_connection_info(&info.redis).password_only_fallback()?;
    let mut legacy = info.clone();
    legacy_auth.apply_to_connection_info(&mut legacy.redis);
    Some(legacy)
}

pub fn resolve_redis_auth(url: &str) -> Result<ResolvedRedisAuth> {
    let info = url
        .to_string()
        .into_connection_info()
        .map_err(|error| metadata_error("redis auth resolution", error))?;
    Ok(ResolvedRedisAuth::from_connection_info(&info.redis)
        .with_env_fallback()
        .normalize_for_endpoint(url))
}

fn redis_accepts_auth(url: &str, auth: &ResolvedRedisAuth) -> bool {
    let Ok(mut info) = url.to_string().into_connection_info() else {
        return false;
    };
    auth.apply_to_connection_info(&mut info.redis);
    let Ok(client) = redis::Client::open(info) else {
        return false;
    };
    client
        .get_connection_with_timeout(redis_auth_probe_timeout())
        .is_ok()
}

fn redacted_route_namespace_source(url: &str) -> String {
    match redis::parse_redis_url(url) {
        Some(mut parsed) => {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.to_string()
        }
        None => url.to_string(),
    }
}

fn redis_connect_timeout() -> Duration {
    duration_from_env_ms(REDIS_CONNECT_TIMEOUT_ENV, DEFAULT_REDIS_CONNECT_TIMEOUT)
}

fn redis_io_timeout() -> Duration {
    duration_from_env_ms(REDIS_IO_TIMEOUT_ENV, DEFAULT_REDIS_IO_TIMEOUT)
}

fn redis_auth_probe_timeout() -> Duration {
    duration_from_env_ms(
        REDIS_AUTH_PROBE_TIMEOUT_ENV,
        DEFAULT_REDIS_AUTH_PROBE_TIMEOUT,
    )
}

fn duration_from_env_ms(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn scan_keys(connection: &mut redis::Connection, pattern: &str) -> Result<Vec<String>> {
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (next, batch): (u64, Vec<String>) = cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .query(connection)
            .map_err(|error| metadata_error("redis scan keys", error))?;
        keys.extend(batch);
        if next == 0 {
            break;
        }
        cursor = next;
    }
    Ok(keys)
}

impl MetadataBackend for RedisMetadataBackend {
    fn route_namespace(&self) -> String {
        self.route_namespace.clone()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        let stable_id = lease.runtime.stable_id.clone();
        let new_epoch = lease.runtime.epoch.0;
        let lease_key = self.keyspace.client(&lease.runtime);
        let by_stable_key = self.keyspace.client_by_stable_index(&stable_id);
        let hwm_key = self.keyspace.client_epoch_hwm(&stable_id);
        let global_index_key = self.keyspace.client_index();
        let payload = serde_json::to_string(lease).map_err(json_error)?;
        let result = self.query_idempotent_write("redis upsert client lease", |connection| {
            let ttl_ms = lease.expires_at_ms.saturating_sub(now_ms()).max(1);
            Script::new(UPSERT_CLIENT_LEASE_STRICT_GREATER_SCRIPT)
                .key(&lease_key)
                .key(&by_stable_key)
                .key(&hwm_key)
                .key(&global_index_key)
                .arg(new_epoch.to_string())
                .arg(payload.as_str())
                .arg(ttl_ms.to_string())
                .invoke::<(i32, String)>(connection)
        })?;
        if result.0 == 1 {
            return Ok(());
        }
        Err(StoreError::StaleEpoch(format!(
            "lease rejected: proposed epoch {new_epoch} <= floor {} for stable_id {}",
            result.1, stable_id.0
        )))
    }

    fn allocate_client_lease(&self, template: &ClientLease) -> Result<ClientRuntimeId> {
        let stable_id = template.runtime.stable_id.clone();
        let lease_key_prefix = self.keyspace.client_prefix_for_stable(&stable_id);
        let by_stable_key = self.keyspace.client_by_stable_index(&stable_id);
        let hwm_key = self.keyspace.client_epoch_hwm(&stable_id);
        let global_index_key = self.keyspace.client_index();
        let payload_template = serde_json::to_string(template).map_err(json_error)?;
        let assigned_epoch: String =
            self.query_idempotent_write("redis allocate client lease", |connection| {
                let ttl_ms = template.expires_at_ms.saturating_sub(now_ms()).max(1);
                Script::new(ALLOCATE_CLIENT_LEASE_SCRIPT)
                    .key(&lease_key_prefix)
                    .key(&by_stable_key)
                    .key(&hwm_key)
                    .key(&global_index_key)
                    .arg(payload_template.as_str())
                    .arg(ttl_ms.to_string())
                    .invoke::<String>(connection)
            })?;
        let epoch = assigned_epoch.parse::<u64>().map_err(|error| {
            StoreError::Metadata(format!(
                "redis allocate client lease: invalid epoch payload {assigned_epoch:?}: {error}"
            ))
        })?;
        Ok(ClientRuntimeId {
            stable_id,
            epoch: ClientEpoch(epoch),
        })
    }

    fn update_client_state(
        &self,
        runtime: &ClientRuntimeId,
        next: ClientLifecycleState,
    ) -> Result<()> {
        let key = self.keyspace.client(runtime);
        let mut connection = self.connection()?;
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get client lease", error))?;
        let Some(payload) = payload else {
            return Err(StoreError::NotFound(key));
        };
        let mut lease: ClientLease = serde_json::from_str(&payload).map_err(json_error)?;
        lease.state = next;
        drop(connection);
        self.upsert_client_lease(&lease)
    }

    fn list_live_clients(&self) -> Result<Vec<ClientLease>> {
        let index = self.keyspace.client_index();
        let (keys, backfill_index) =
            self.query_readonly("redis list live clients", |connection| {
                let mut keys: Vec<String> = connection.smembers(&index)?;
                let mut backfill_index = false;
                if keys.is_empty() {
                    keys = redis::cmd("KEYS")
                        .arg(self.keyspace.client_pattern())
                        .query(connection)?;
                    backfill_index = !keys.is_empty();
                }
                Ok((keys, backfill_index))
            })?;
        if backfill_index && !keys.is_empty() {
            let mut connection = self.connection()?;
            connection
                .sadd::<_, _, ()>(&index, keys.clone())
                .map_err(|error| metadata_error("redis sadd legacy client index", error))?;
        }
        let entries = self.query_readonly("redis fetch client leases", |connection| {
            let mut entries = Vec::with_capacity(keys.len());
            for key in &keys {
                let payload: Option<String> = connection.get(key.as_str())?;
                entries.push((key.clone(), payload));
            }
            Ok(entries)
        })?;
        let now = now_ms();
        let mut stale = Vec::new();
        let mut stale_by_stable: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut leases = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            let parsed = self.keyspace.parse_client_key(&key);
            match payload {
                Some(payload) => {
                    let lease =
                        serde_json::from_str::<ClientLease>(&payload).map_err(json_error)?;
                    if lease.expires_at_ms >= now {
                        leases.push(lease);
                    } else {
                        let by_stable_key = self
                            .keyspace
                            .client_by_stable_index(&lease.runtime.stable_id);
                        stale_by_stable
                            .entry(by_stable_key)
                            .or_default()
                            .push(lease.runtime.epoch.0.to_string());
                        stale.push(key);
                    }
                }
                None => {
                    if let Some((stable_id, epoch)) = parsed {
                        let by_stable_key = self
                            .keyspace
                            .client_by_stable_index(&ClientStableId::new(stable_id));
                        stale_by_stable
                            .entry(by_stable_key)
                            .or_default()
                            .push(epoch.to_string());
                    }
                    stale.push(key);
                }
            }
        }
        if !stale.is_empty() || !stale_by_stable.is_empty() {
            let mut connection = self.connection()?;
            if !stale.is_empty() {
                connection
                    .srem::<_, _, ()>(&index, stale)
                    .map_err(|error| metadata_error("redis srem stale client index", error))?;
            }
            for (by_stable_key, epochs) in stale_by_stable {
                connection
                    .srem::<_, _, ()>(&by_stable_key, epochs)
                    .map_err(|error| {
                        metadata_error("redis srem stale by-stable client index", error)
                    })?;
            }
        }
        Ok(leases)
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        self.query_idempotent_write("redis publish segment", |connection| {
            let key = self.keyspace.segment(&segment.owner, &segment.segment_name);
            let current_payload: Option<String> = redis::cmd("HGET")
                .arg(&key)
                .arg("state_json")
                .query(connection)?;
            let mut state = match current_payload {
                Some(payload) => serde_json::from_str::<StoredSegmentState>(&payload)
                    .map_err(|error| store_error_to_redis_error(json_error(error)))?,
                None => StoredSegmentState::new(segment.clone()),
            };
            state.merge_announcement(segment);
            self.store_segment_state(connection, &state)
                .map_err(store_error_to_redis_error)
        })
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        let key = self.keyspace.segment(owner, segment);
        self.query_idempotent_write("redis unpublish segment", |connection| {
            connection.del::<_, ()>(&key)?;
            connection.srem::<_, _, ()>(self.keyspace.segment_index(None), key.as_str())?;
            connection.srem::<_, _, ()>(self.keyspace.segment_index(Some(owner)), key.as_str())?;
            Ok(())
        })
    }

    fn list_segments(&self, owner: Option<&ClientRuntimeId>) -> Result<Vec<SegmentAnnouncement>> {
        let index = self.keyspace.segment_index(owner);
        let keys = self.query_readonly("redis list segments", |connection| {
            let mut keys: Vec<String> = connection.smembers(&index)?;
            if keys.is_empty() {
                keys = redis::cmd("KEYS")
                    .arg(self.keyspace.segment_pattern(owner))
                    .query(connection)?;
            }
            Ok(keys)
        })?;
        let entries = self.query_readonly("redis fetch segment states", |connection| {
            let mut entries = Vec::with_capacity(keys.len());
            for key in &keys {
                let payload: Option<String> = redis::cmd("HGET")
                    .arg(key.as_str())
                    .arg("state_json")
                    .query(connection)?;
                entries.push((key.clone(), payload));
            }
            Ok(entries)
        })?;
        let mut stale = Vec::new();
        let mut segments = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            if let Some(payload) = payload {
                let state =
                    serde_json::from_str::<StoredSegmentState>(&payload).map_err(json_error)?;
                segments.push(state.announcement);
            } else {
                stale.push(key);
            }
        }
        if !stale.is_empty() {
            let mut connection = self.connection()?;
            connection
                .srem::<_, _, ()>(&index, stale)
                .map_err(|error| metadata_error("redis srem stale segment index", error))?;
        }
        Ok(segments)
    }

    fn update_segment_state(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        self.query_idempotent_write("redis update segment state", |connection| {
            let mut state = self
                .load_segment_state(connection, owner, segment)
                .map_err(store_error_to_redis_error)?;
            state.announcement.state = next;
            self.store_segment_state(connection, &state)
                .map_err(store_error_to_redis_error)
        })
    }

    fn reserve_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        let mut connection = self.connection()?;
        let key = self.keyspace.segment(owner, segment);
        let result = Script::new(RESERVE_SEGMENT_SCRIPT)
            .key(&key)
            .arg(length_bytes)
            .invoke::<(i32, i64)>(&mut connection)
            .map_err(|error| metadata_error("redis reserve segment", error))?;
        match result.0 {
            1 => Ok(SegmentReservation {
                owner: owner.clone(),
                segment_name: segment.clone(),
                offset_bytes: result.1 as u64,
                length_bytes,
            }),
            _ if result.1 == -1 => Err(StoreError::NotFound(key)),
            _ if result.1 == -2 => Err(StoreError::InvalidState(format!(
                "segment {}:{} is not active",
                owner, segment.0
            ))),
            _ => Err(StoreError::Allocator(format!(
                "segment capacity exhausted for {}:{} requested={} remaining={}",
                owner, segment.0, length_bytes, result.1
            ))),
        }
    }

    fn release_segment(
        &self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let mut connection = self.connection()?;
        let key = self.keyspace.segment(owner, segment);
        let result = Script::new(RELEASE_SEGMENT_SCRIPT)
            .key(&key)
            .arg(offset_bytes)
            .arg(length_bytes)
            .invoke::<(i32, i64)>(&mut connection)
            .map_err(|error| metadata_error("redis release segment", error))?;
        match result.0 {
            1 => Ok(()),
            _ if result.1 == -1 => Err(StoreError::NotFound(key)),
            _ => Err(StoreError::Allocator(format!(
                "segment release rejected for {}:{} offset={} len={}",
                owner, segment.0, offset_bytes, length_bytes
            ))),
        }
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        let key = self.keyspace.object(key);
        let payload: Option<String> =
            self.query_readonly("redis hget route payload", |connection| {
                redis::cmd("HGET")
                    .arg(key.as_str())
                    .arg("payload")
                    .query(connection)
            })?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn list_object_routes(&self) -> Result<Vec<ObjectRoute>> {
        let index = self.keyspace.object_index();
        let (entries, backfill_index) =
            self.query_readonly("redis list object routes", |connection| {
                let mut keys: Vec<String> = connection.smembers(&index)?;
                let mut backfill_index = false;
                if keys.is_empty() {
                    keys = redis::cmd("KEYS")
                        .arg(self.keyspace.object_pattern())
                        .query(connection)?;
                    backfill_index = !keys.is_empty();
                }

                let mut entries = Vec::with_capacity(keys.len());
                for key in keys {
                    let payload: Option<String> = redis::cmd("HGET")
                        .arg(key.as_str())
                        .arg("payload")
                        .query(connection)?;
                    entries.push((key, payload));
                }
                Ok((entries, backfill_index))
            })?;

        let mut stale = Vec::new();
        let mut live_keys = Vec::new();
        let mut routes = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            if let Some(payload) = payload {
                live_keys.push(key);
                routes.push(serde_json::from_str(&payload).map_err(json_error)?);
            } else {
                stale.push(key);
            }
        }
        if backfill_index && !live_keys.is_empty() {
            let mut connection = self.connection()?;
            connection
                .sadd::<_, _, ()>(&index, live_keys)
                .map_err(|error| metadata_error("redis sadd legacy object index", error))?;
        }
        if !stale.is_empty() {
            let mut connection = self.connection()?;
            connection
                .srem::<_, _, ()>(&index, stale)
                .map_err(|error| metadata_error("redis srem stale object index", error))?;
        }
        Ok(routes)
    }

    fn compare_and_swap_object_route(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let mut connection = self.connection()?;
        let payload = next
            .map(|route| serde_json::to_string(route).map_err(json_error))
            .transpose()?;
        let next_version = next.map(|route| route.version.0).unwrap_or_default();
        let object_key = self.keyspace.object(key);
        let current_payload = Script::new(CAS_OBJECT_ROUTE_SCRIPT)
            .key(&object_key)
            .arg(
                expected
                    .map(|version| version.0.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(next_version.to_string())
            .arg(payload.clone().unwrap_or_else(|| "__delete__".to_string()))
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis cas object route", error))?;
        if current_payload.0 == 1 {
            if next.is_some() {
                connection
                    .sadd::<_, _, ()>(self.keyspace.object_index(), object_key)
                    .map_err(|error| metadata_error("redis sadd object index", error))?;
            } else {
                connection
                    .srem::<_, _, ()>(self.keyspace.object_index(), object_key)
                    .map_err(|error| metadata_error("redis srem object index", error))?;
            }
        }

        let current = if current_payload.1.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&current_payload.1).map_err(json_error)?)
        };
        Ok(CasResult {
            applied: current_payload.0 == 1,
            current,
        })
    }

    fn get_route_policy(&self, domain: &RoutePolicyDomain) -> Result<Option<RoutePolicy>> {
        let key = self.keyspace.route_policy(domain);
        let payload: Option<String> =
            self.query_readonly("redis get route policy", |connection| connection.get(&key))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn put_route_policy_if_absent(
        &self,
        domain: &RoutePolicyDomain,
        policy: &RoutePolicy,
    ) -> Result<bool> {
        let key = self.keyspace.route_policy(domain);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        self.query_idempotent_write("redis setnx route policy", |connection| {
            connection.set_nx(key.as_str(), payload.as_str())
        })
    }

    fn put_route_policy(&self, domain: &RoutePolicyDomain, policy: &RoutePolicy) -> Result<()> {
        let mut connection = self.connection()?;
        let key = self.keyspace.route_policy(domain);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        connection
            .set::<_, _, ()>(key, payload)
            .map_err(|error| metadata_error("redis set route policy", error))
    }

    fn delete_route_policy(&self, domain: &RoutePolicyDomain) -> Result<bool> {
        let mut connection = self.connection()?;
        let key = self.keyspace.route_policy(domain);
        let removed: usize = connection
            .del(key)
            .map_err(|error| metadata_error("redis del route policy", error))?;
        Ok(removed != 0)
    }

    fn list_route_policies(&self) -> Result<Vec<(RoutePolicyDomain, RoutePolicy)>> {
        let mut connection = self.connection()?;
        let prefix = self.keyspace.route_policy_prefix();
        let keys = scan_keys(&mut connection, &format!("{}*", prefix))?;
        let mut policies = Vec::new();
        for key in keys {
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get route policy", error))?;
            let Some(payload) = payload else {
                continue;
            };
            let Some(domain) = parse_route_policy_domain(&self.keyspace, &key) else {
                continue;
            };
            policies.push((domain, serde_json::from_str(&payload).map_err(json_error)?));
        }
        policies.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(policies)
    }

    fn get_tenant_policy(&self, scope: &TenantPolicyScope) -> Result<Option<TenantPolicy>> {
        let mut connection = self.connection()?;
        let key = self.keyspace.tenant_policy(scope);
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get tenant policy", error))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }

    fn list_tenant_policies(&self) -> Result<Vec<TenantPolicy>> {
        let mut connection = self.connection()?;
        let prefix = self.keyspace.tenant_policy_prefix(None);
        let keys = scan_keys(&mut connection, &format!("{}*", prefix))?;
        let mut policies: Vec<TenantPolicy> = Vec::new();
        for key in keys {
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get tenant policy", error))?;
            let Some(payload) = payload else {
                continue;
            };
            let Some(_scope) = parse_tenant_policy_scope(&self.keyspace, &key) else {
                continue;
            };
            policies.push(serde_json::from_str(&payload).map_err(json_error)?);
        }
        policies.sort_by(|left, right| left.scope.cmp(&right.scope));
        Ok(policies)
    }

    fn put_tenant_policy(
        &self,
        policy: &TenantPolicy,
        expected_version: Option<u64>,
    ) -> Result<TenantPolicy> {
        policy.validate()?;
        let mut connection = self.connection()?;
        let key = self.keyspace.tenant_policy(&policy.scope);
        let payload = serde_json::to_string(policy).map_err(json_error)?;
        let result = Script::new(CAS_TENANT_POLICY_SCRIPT)
            .key(&key)
            .arg(
                expected_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(payload)
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis cas tenant policy", error))?;
        if result.0 == 1 {
            return Ok(policy.clone());
        }
        let current = if result.1.is_empty() {
            None
        } else {
            Some(parse_tenant_policy_cas_payload(
                &result.1,
                "redis cas tenant policy",
            )?)
        };
        match (expected_version, current.as_ref()) {
            (None, Some(_)) => Err(StoreError::Conflict(format!(
                "tenant policy already exists for {}",
                policy.scope.tenant
            ))),
            (Some(expected), Some(current)) => Err(StoreError::Conflict(format!(
                "tenant policy version mismatch for {}: expected={} actual={}",
                policy.scope.tenant, expected, current.version
            ))),
            (Some(expected), None) => Err(StoreError::Conflict(format!(
                "tenant policy missing for {} at expected version {}",
                policy.scope.tenant, expected
            ))),
            (None, None) => Err(StoreError::Conflict(format!(
                "tenant policy write rejected for {}",
                policy.scope.tenant
            ))),
        }
    }

    fn delete_tenant_policy(
        &self,
        scope: &TenantPolicyScope,
        expected_version: Option<u64>,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let key = self.keyspace.tenant_policy(scope);
        let result = Script::new(CAS_TENANT_POLICY_SCRIPT)
            .key(&key)
            .arg(
                expected_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg("__delete__")
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis delete tenant policy", error))?;
        if result.0 == 1 {
            return Ok(true);
        }
        let current = if result.1.is_empty() {
            None
        } else {
            Some(parse_tenant_policy_cas_payload(
                &result.1,
                "redis delete tenant policy",
            )?)
        };
        match (expected_version, current.as_ref()) {
            (None, None) => Ok(false),
            (Some(expected), Some(current)) => Err(StoreError::Conflict(format!(
                "tenant policy version mismatch for {}: expected={} actual={}",
                scope.tenant, expected, current.version
            ))),
            (Some(expected), None) => Err(StoreError::Conflict(format!(
                "tenant policy missing for {} at expected version {}",
                scope.tenant, expected
            ))),
            (None, Some(_)) => Err(StoreError::Conflict(format!(
                "tenant policy delete rejected for {}",
                scope.tenant
            ))),
        }
    }

    fn get_tenant_quota_state(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Option<TenantQuotaState>> {
        let scope = root_scope(scope)?;
        let mut connection = self.connection()?;
        let key = self.keyspace.tenant_quota_state(&scope);
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get tenant quota state", error))?;
        payload
            .map(|payload| {
                parse_tenant_quota_state_payload(&payload, "redis get tenant quota state")
            })
            .transpose()
    }

    fn get_tenant_object_accounting(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<TenantObjectAccounting>> {
        let mut connection = self.connection()?;
        let storage_key = self.keyspace.tenant_object_accounting(key);
        let payload: Option<String> = connection
            .get(&storage_key)
            .map_err(|error| metadata_error("redis get tenant object accounting", error))?;
        payload
            .map(|payload| {
                parse_tenant_object_accounting_payload(
                    &payload,
                    "redis get tenant object accounting",
                )
            })
            .transpose()
    }

    fn list_tenant_quota_reservations(
        &self,
        scope: &TenantPolicyScope,
    ) -> Result<Vec<TenantQuotaReservation>> {
        let scope = root_scope(scope)?;
        let mut connection = self.connection()?;
        let index_prefix = self
            .keyspace
            .tenant_quota_reservation_prefix(Some(&scope.tenant));
        let index_keys = scan_keys(&mut connection, &format!("{}*", index_prefix))?;
        let mut reservations = Vec::new();
        for index_key in index_keys {
            let reservation_key: Option<String> =
                connection.get(index_key.as_str()).map_err(|error| {
                    metadata_error("redis get tenant quota reservation index", error)
                })?;
            let Some(reservation_key) = reservation_key else {
                continue;
            };
            let payload: Option<String> = connection
                .get(reservation_key.as_str())
                .map_err(|error| metadata_error("redis get tenant quota reservation", error))?;
            let Some(payload) = payload else {
                continue;
            };
            let reservation = parse_tenant_quota_reservation_payload(
                &payload,
                "redis list tenant quota reservations",
            )?;
            if reservation.scope == scope {
                reservations.push(reservation);
            }
        }
        reservations.sort_by(|left, right| left.reservation_id.cmp(&right.reservation_id));
        Ok(reservations)
    }

    fn reserve_tenant_quota(
        &self,
        request: &TenantQuotaReservationRequest,
    ) -> Result<TenantQuotaReservationOutcome> {
        request.validate()?;
        let scope = root_scope(&request.scope)?;
        let mut normalized = request.clone();
        normalized.scope = scope.clone();
        let mut connection = self.connection()?;
        let quota_key = self.keyspace.tenant_quota_state(&scope);
        let object_key = self.keyspace.tenant_object_accounting(&normalized.key);
        let reservation_key = self
            .keyspace
            .tenant_quota_reservation(&normalized.reservation_id);
        let reservation_index_key = self
            .keyspace
            .tenant_quota_reservation_index(&scope, &normalized.reservation_id);
        let scope_payload = serde_json::to_string(&scope).map_err(json_error)?;
        let writer_runtime_payload =
            serde_json::to_string(&normalized.writer_runtime).map_err(json_error)?;
        let result = Script::new(RESERVE_TENANT_QUOTA_SCRIPT)
            .key(&quota_key)
            .key(&object_key)
            .key(&reservation_key)
            .key(&reservation_index_key)
            .arg(&normalized.reservation_id)
            .arg(scope_payload)
            .arg(&normalized.key.0)
            .arg(
                normalized
                    .expected_object_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(normalized.delta_bytes)
            .arg(normalized.delta_objects)
            .arg(
                normalized
                    .limit
                    .max_bytes
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(
                normalized
                    .limit
                    .max_objects
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(normalized.expires_at_ms)
            .arg(normalized.created_at_ms)
            .arg(writer_runtime_payload)
            .invoke::<(i32, String, String, String)>(&mut connection)
            .map_err(|error| metadata_error("redis reserve tenant quota", error))?;

        match result.0 {
            1 | 2 => Ok(TenantQuotaReservationOutcome {
                quota: parse_tenant_quota_state_payload(&result.1, "redis reserve tenant quota")?,
                object: parse_optional_tenant_object_accounting_payload(
                    &result.2,
                    "redis reserve tenant quota",
                )?,
                reservation: parse_tenant_quota_reservation_payload(
                    &result.3,
                    "redis reserve tenant quota",
                )?,
            }),
            -2 => Err(StoreError::Conflict(format!(
                "tenant quota reservation {} already exists with different parameters",
                normalized.reservation_id
            ))),
            -3 => Err(version_conflict(
                "tenant object accounting",
                &scope,
                normalized.expected_object_version,
                parse_optional_tenant_object_accounting_payload(
                    &result.1,
                    "redis reserve tenant quota conflict",
                )?
                .as_ref()
                .map(|object| object.version),
            )),
            -4 => Err(StoreError::InvalidState(format!(
                "tenant object accounting scope mismatch for key {}",
                normalized.key.0
            ))),
            -5 => {
                let quota =
                    parse_tenant_quota_state_payload(&result.1, "redis reserve tenant quota")?;
                Err(StoreError::Conflict(format!(
                    "tenant quota bytes exceeded for {}: used={} pending={} requested={} limit={}",
                    scope.tenant,
                    quota.used_bytes,
                    quota.pending_reserved_bytes,
                    normalized.delta_bytes.max(0),
                    normalized.limit.max_bytes.unwrap_or_default()
                )))
            }
            -6 => {
                let quota =
                    parse_tenant_quota_state_payload(&result.1, "redis reserve tenant quota")?;
                Err(StoreError::Conflict(format!(
                    "tenant quota objects exceeded for {}: used={} pending={} requested={} limit={}",
                    scope.tenant,
                    quota.used_objects,
                    quota.pending_reserved_objects,
                    normalized.delta_objects.max(0),
                    normalized.limit.max_objects.unwrap_or_default()
                )))
            }
            code => Err(StoreError::Metadata(format!(
                "redis reserve tenant quota: unexpected status code {code}"
            ))),
        }
    }

    fn finalize_tenant_quota(
        &self,
        request: &TenantQuotaFinalizeRequest,
    ) -> Result<TenantQuotaFinalizeOutcome> {
        request.validate()?;
        let mut connection = self.connection()?;
        let reservation_key = self
            .keyspace
            .tenant_quota_reservation(&request.reservation_id);
        let reservation_payload: Option<String> =
            connection.get(&reservation_key).map_err(|error| {
                metadata_error("redis get tenant quota reservation before finalize", error)
            })?;
        let reservation_payload = reservation_payload
            .ok_or_else(|| StoreError::NotFound(request.reservation_id.clone()))?;
        let reservation = parse_tenant_quota_reservation_payload(
            &reservation_payload,
            "redis finalize tenant quota prefetch",
        )?;
        let scope = root_scope(&reservation.scope)?;
        let quota_key = self.keyspace.tenant_quota_state(&scope);
        let object_key = self.keyspace.tenant_object_accounting(&reservation.key);
        let result = Script::new(FINALIZE_TENANT_QUOTA_SCRIPT)
            .key(&quota_key)
            .key(&object_key)
            .key(&reservation_key)
            .arg(&request.reservation_id)
            .arg(
                request
                    .expected_object_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(
                request
                    .committed_length
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(
                request
                    .route_version
                    .map(|version| version.0.to_string())
                    .unwrap_or_else(|| "__none__".to_string()),
            )
            .arg(match request.state {
                TenantObjectAccountingState::Active => "Active",
                TenantObjectAccountingState::Deleted => "Deleted",
            })
            .arg(request.updated_at_ms)
            .arg(&request.updated_by)
            .invoke::<(i32, String, String, String)>(&mut connection)
            .map_err(|error| metadata_error("redis finalize tenant quota", error))?;

        match result.0 {
            1 | 2 => Ok(TenantQuotaFinalizeOutcome {
                quota: parse_tenant_quota_state_payload(&result.1, "redis finalize tenant quota")?,
                object: parse_optional_tenant_object_accounting_payload(
                    &result.2,
                    "redis finalize tenant quota",
                )?,
                reservation: parse_tenant_quota_reservation_payload(
                    &result.3,
                    "redis finalize tenant quota",
                )?,
            }),
            -3 => Err(StoreError::Conflict(format!(
                "tenant quota reservation {} is already aborted",
                request.reservation_id
            ))),
            -4 => Err(version_conflict(
                "tenant object accounting",
                &scope,
                request
                    .expected_object_version
                    .or(reservation.expected_object_version),
                parse_optional_tenant_object_accounting_payload(
                    &result.1,
                    "redis finalize tenant quota conflict",
                )?
                .as_ref()
                .map(|object| object.version),
            )),
            -5 => Err(StoreError::InvalidState(format!(
                "tenant object accounting scope mismatch for key {}",
                reservation.key.0
            ))),
            -6 => Err(StoreError::InvalidState(format!(
                "tenant quota state missing for {} while finalizing reservation {}",
                scope.tenant, request.reservation_id
            ))),
            -7 => Err(StoreError::InvalidState(format!(
                "tenant quota state underflow while finalizing reservation {}",
                request.reservation_id
            ))),
            code => Err(StoreError::Metadata(format!(
                "redis finalize tenant quota: unexpected status code {code}"
            ))),
        }
    }

    fn abort_tenant_quota(&self, reservation_id: &str) -> Result<TenantQuotaAbortOutcome> {
        if reservation_id.is_empty() {
            return Err(StoreError::InvalidState(
                "tenant quota abort reservation_id must not be empty".to_string(),
            ));
        }
        let mut connection = self.connection()?;
        let reservation_key = self.keyspace.tenant_quota_reservation(reservation_id);
        let reservation_payload: Option<String> =
            connection.get(&reservation_key).map_err(|error| {
                metadata_error("redis get tenant quota reservation before abort", error)
            })?;
        let reservation_payload =
            reservation_payload.ok_or_else(|| StoreError::NotFound(reservation_id.to_string()))?;
        let reservation = parse_tenant_quota_reservation_payload(
            &reservation_payload,
            "redis abort tenant quota prefetch",
        )?;
        let scope = root_scope(&reservation.scope)?;
        let quota_key = self.keyspace.tenant_quota_state(&scope);
        let result = Script::new(ABORT_TENANT_QUOTA_SCRIPT)
            .key(&quota_key)
            .key(&reservation_key)
            .arg(reservation_id)
            .invoke::<(i32, String, String)>(&mut connection)
            .map_err(|error| metadata_error("redis abort tenant quota", error))?;
        match result.0 {
            1 | 2 => Ok(TenantQuotaAbortOutcome {
                quota: parse_tenant_quota_state_payload(&result.1, "redis abort tenant quota")?,
                reservation: parse_tenant_quota_reservation_payload(
                    &result.2,
                    "redis abort tenant quota",
                )?,
            }),
            -3 => Err(StoreError::Conflict(format!(
                "tenant quota reservation {} is already finalized",
                reservation_id
            ))),
            -4 => Err(StoreError::InvalidState(format!(
                "tenant quota state missing for {} while aborting reservation {}",
                scope.tenant, reservation_id
            ))),
            -5 => Err(StoreError::InvalidState(format!(
                "tenant quota state underflow while aborting reservation {}",
                reservation_id
            ))),
            code => Err(StoreError::Metadata(format!(
                "redis abort tenant quota: unexpected status code {code}"
            ))),
        }
    }

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        let key = self.keyspace.handoff(&handoff.stable_id);
        let payload = serde_json::to_string(handoff).map_err(json_error)?;
        self.query_idempotent_write("redis set handoff", |connection| {
            connection.set::<_, _, ()>(key.as_str(), payload.as_str())
        })
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        let key = self.keyspace.handoff(stable_id);
        let payload: Option<String> =
            self.query_readonly("redis get handoff", |connection| connection.get(&key))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn metadata_error(operation: &str, error: redis::RedisError) -> StoreError {
    StoreError::Metadata(format!("{operation}: {error}"))
}

pub fn is_legacy_redis_auth_arity_error(message: &str) -> bool {
    let detail = message.to_ascii_lowercase();
    detail.contains("wrong number of arguments for 'auth' command")
        || detail.contains("wrong number of arguments for `auth` command")
        || detail.contains("wrong number of arguments for auth command")
}

#[cfg(test)]
fn should_retry_readonly_redis_error(error: &redis::RedisError) -> bool {
    should_retry_transient_redis_error(error)
}

fn should_retry_transient_redis_error(error: &redis::RedisError) -> bool {
    let detail = error.to_string().to_ascii_lowercase();
    error.is_timeout()
        || error.is_connection_dropped()
        || error.is_io_error()
        || detail.contains("interrupted")
        || detail.contains("connection refused")
        || detail.contains("connection reset")
        || detail.contains("connection aborted")
        || detail.contains("connection closed")
        || detail.contains("not connected")
        || detail.contains("broken pipe")
        || detail.contains("temporarily unavailable")
        || detail.contains("timed out")
}

fn json_error(error: serde_json::Error) -> StoreError {
    StoreError::Metadata(format!("json serialization: {error}"))
}

fn store_error_to_redis_error(error: StoreError) -> redis::RedisError {
    redis::RedisError::from((
        redis::ErrorKind::TypeError,
        "metadata error",
        error.to_string(),
    ))
}

fn redis_retry_attempts() -> usize {
    std::env::var(REDIS_RETRY_ATTEMPTS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|attempts| *attempts > 0)
        .unwrap_or(DEFAULT_REDIS_RETRY_ATTEMPTS)
}

fn redis_retry_delay() -> Duration {
    duration_from_env_ms(REDIS_RETRY_DELAY_ENV, DEFAULT_REDIS_RETRY_DELAY)
}

fn parse_tenant_policy_cas_payload(payload: &str, operation: &str) -> Result<TenantPolicy> {
    serde_json::from_str::<TenantPolicy>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant policy payload returned from redis CAS: {error}"
        ))
    })
}

fn parse_tenant_quota_state_payload(payload: &str, operation: &str) -> Result<TenantQuotaState> {
    serde_json::from_str::<TenantQuotaState>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant quota state payload from redis: {error}"
        ))
    })
}

fn parse_tenant_object_accounting_payload(
    payload: &str,
    operation: &str,
) -> Result<TenantObjectAccounting> {
    serde_json::from_str::<TenantObjectAccounting>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant object accounting payload from redis: {error}"
        ))
    })
}

fn parse_optional_tenant_object_accounting_payload(
    payload: &str,
    operation: &str,
) -> Result<Option<TenantObjectAccounting>> {
    if payload.is_empty() {
        return Ok(None);
    }
    parse_tenant_object_accounting_payload(payload, operation).map(Some)
}

fn parse_tenant_quota_reservation_payload(
    payload: &str,
    operation: &str,
) -> Result<TenantQuotaReservation> {
    serde_json::from_str::<TenantQuotaReservation>(payload).map_err(|error| {
        StoreError::Metadata(format!(
            "{operation}: corrupted tenant quota reservation payload from redis: {error}"
        ))
    })
}

fn root_scope(scope: &TenantPolicyScope) -> Result<TenantPolicyScope> {
    scope.validate_root_only("tenant quota metadata")?;
    Ok(TenantPolicyScope::new(
        scope.tenant.clone(),
        None::<String>,
        None::<String>,
    ))
}

fn version_conflict(
    entity: &str,
    scope: &TenantPolicyScope,
    expected: Option<u64>,
    actual: Option<u64>,
) -> StoreError {
    match (expected, actual) {
        (Some(expected), Some(actual)) => StoreError::Conflict(format!(
            "{entity} version mismatch for {}: expected={} actual={}",
            scope.tenant, expected, actual
        )),
        (Some(expected), None) => StoreError::Conflict(format!(
            "{entity} missing for {} at expected version {}",
            scope.tenant, expected
        )),
        (None, Some(actual)) => StoreError::Conflict(format!(
            "{entity} already exists for {} at version {}",
            scope.tenant, actual
        )),
        (None, None) => {
            StoreError::Conflict(format!("{entity} write rejected for {}", scope.tenant))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        ClientStableId, CompatibilityDescriptor, HandoffKind, HandoffPlan, MetadataBackend,
        ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteControlMode, RoutePolicy,
        RoutePolicyDomain, RouteState, RouteVersion, SegmentAnnouncement, SegmentLifecycleState,
        SegmentName, StoreError, TenantObjectAccountingState, TenantPolicy, TenantPolicyScope,
        TenantPolicySpec, TenantQuotaFinalizeRequest, TenantQuotaPolicy,
        TenantQuotaReservationRequest, TenantQuotaReservationState,
    };
    use redis::Commands;

    use super::{
        is_legacy_redis_auth_arity_error, legacy_auth_connection_info,
        redacted_route_namespace_source, redis_auth_probe_timeout, redis_connect_timeout,
        redis_connection_info, redis_io_timeout, redis_retry_attempts, redis_retry_delay,
        resolve_redis_auth, should_retry_readonly_redis_error, MetadataKeyspace,
        RedisMetadataBackend, RedisMetadataConfig, DEFAULT_REDIS_AUTH_PROBE_TIMEOUT,
        DEFAULT_REDIS_CONNECT_TIMEOUT, DEFAULT_REDIS_IO_TIMEOUT, DEFAULT_REDIS_RETRY_ATTEMPTS,
        DEFAULT_REDIS_RETRY_DELAY, REDIS_AUTH_PROBE_TIMEOUT_ENV, REDIS_CONNECT_TIMEOUT_ENV,
        REDIS_IO_TIMEOUT_ENV, REDIS_RETRY_ATTEMPTS_ENV, REDIS_RETRY_DELAY_ENV,
    };

    struct RedisTestServer {
        child: Child,
        url: String,
        dir: PathBuf,
    }

    impl RedisTestServer {
        fn start() -> Option<Self> {
            Self::start_with_password(None)
        }

        fn start_with_password(password: Option<&str>) -> Option<Self> {
            let listener = TcpListener::bind("127.0.0.1:0").ok()?;
            let port = listener.local_addr().ok()?.port();
            drop(listener);
            Self::start_on_port(port, password)
        }

        fn start_on_port(port: u16, password: Option<&str>) -> Option<Self> {
            let dir = std::env::temp_dir().join(format!("mooncake-store-rs-redis-{port}"));
            let _ = std::fs::create_dir_all(&dir);
            let mut command = Command::new("redis-server");
            command
                .arg("--save")
                .arg("")
                .arg("--appendonly")
                .arg("no")
                .arg("--bind")
                .arg("127.0.0.1")
                .arg("--port")
                .arg(port.to_string())
                .arg("--dir")
                .arg(&dir)
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            if let Some(password) = password {
                command.arg("--requirepass").arg(password);
            }
            let child = command.spawn().ok()?;

            let url = match password {
                Some(password) => format!("redis://:{password}@127.0.0.1:{port}/0"),
                None => format!("redis://127.0.0.1:{port}/0"),
            };
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if redis::Client::open(url.as_str())
                    .ok()
                    .and_then(|client| client.get_connection().ok())
                    .is_some()
                {
                    return Some(Self { child, url, dir });
                }
                sleep(Duration::from_millis(25));
            }
            None
        }

        fn url(&self) -> &str {
            &self.url
        }
    }

    impl Drop for RedisTestServer {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn sample_runtime() -> ClientRuntimeId {
        ClientRuntimeId::new("writer", ClientEpoch(5))
    }

    fn sample_lease(state: ClientLifecycleState) -> ClientLease {
        ClientLease {
            runtime: sample_runtime(),
            state,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet {
                rpc_address: "127.0.0.1:7777".to_string(),
                segment_name: Some(SegmentName::new("segment-a")),
                labels: BTreeMap::from([("pool".to_string(), "local".to_string())]),
            },
            expires_at_ms: super::now_ms() + 60_000,
        }
    }

    fn sample_segment(state: SegmentLifecycleState) -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner: sample_runtime(),
            segment_name: SegmentName::new("segment-a"),
            capacity_bytes: 128,
            used_bytes: 0,
            state,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        }
    }

    fn sample_route(version: u64) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new("object-a"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(version),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: sample_runtime(),
                segment_name: SegmentName::new("segment-a"),
                offset: 32,
                segment_offset: 32,
                length: 12,
                checksum: Some(7),
                tier: ReplicaTier::Dram,
                priority: 1,
            }],
        }
    }

    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock should not be poisoned")
    }

    #[test]
    fn redis_backend_round_trips_full_metadata_surface() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(MetadataKeyspace::new("test/redis")),
        )
        .expect("redis backend should initialize");

        assert!(backend.route_namespace().contains(server.url()));
        assert!(backend.route_namespace().contains("test/redis"));

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("client lease upsert should succeed");
        backend
            .update_client_state(&lease.runtime, ClientLifecycleState::Draining)
            .expect("client state update should succeed");
        let live_clients = backend
            .list_live_clients()
            .expect("listing live clients should succeed");
        assert_eq!(live_clients.len(), 1);
        assert_eq!(live_clients[0].state, ClientLifecycleState::Draining);
        assert!(matches!(
            backend.update_client_state(
                &ClientRuntimeId::new("missing", ClientEpoch(1)),
                ClientLifecycleState::Active
            ),
            Err(StoreError::NotFound(_))
        ));

        let segment = sample_segment(SegmentLifecycleState::Active);
        backend
            .publish_segment(&segment)
            .expect("segment publish should succeed");
        let listed = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing should succeed");
        assert_eq!(listed, vec![segment.clone()]);

        let reservation = backend
            .reserve_segment(&segment.owner, &segment.segment_name, 13)
            .expect("segment reservation should succeed");
        assert_eq!(reservation.offset_bytes, 0);
        backend
            .release_segment(
                &segment.owner,
                &segment.segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
            )
            .expect("segment release should succeed");

        backend
            .update_segment_state(
                &segment.owner,
                &segment.segment_name,
                SegmentLifecycleState::Draining,
            )
            .expect("segment state update should succeed");
        assert!(matches!(
            backend.reserve_segment(&segment.owner, &segment.segment_name, 8),
            Err(StoreError::InvalidState(_))
        ));
        backend
            .unpublish_segment(&segment.owner, &segment.segment_name)
            .expect("segment unpublish should succeed");
        assert!(backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after delete should succeed")
            .is_empty());
        assert!(matches!(
            backend.release_segment(&segment.owner, &segment.segment_name, 0, 1),
            Err(StoreError::NotFound(_))
        ));

        let route_key = ObjectKey::new("object-a");
        assert!(backend
            .get_object_route(&route_key)
            .expect("route get should succeed")
            .is_none());

        let route_v1 = sample_route(1);
        let created = backend
            .compare_and_swap_object_route(&route_key, None, Some(&route_v1))
            .expect("route create should succeed");
        assert!(created.applied);
        assert_eq!(created.current, Some(route_v1.clone()));

        let route_v2 = sample_route(2);
        let rejected = backend
            .compare_and_swap_object_route(&route_key, None, Some(&route_v2))
            .expect("route cas rejection should succeed");
        assert!(!rejected.applied);
        assert_eq!(rejected.current, Some(route_v1.clone()));

        let replaced = backend
            .compare_and_swap_object_route(&route_key, Some(RouteVersion(1)), Some(&route_v2))
            .expect("route cas update should succeed");
        assert!(replaced.applied);
        assert_eq!(replaced.current, Some(route_v2.clone()));
        assert_eq!(
            backend
                .get_object_route(&route_key)
                .expect("route get after update should succeed"),
            Some(route_v2.clone())
        );
        assert_eq!(
            backend
                .list_object_routes()
                .expect("route listing should succeed"),
            vec![route_v2.clone()]
        );

        let deleted = backend
            .compare_and_swap_object_route(&route_key, Some(RouteVersion(2)), None)
            .expect("route delete should succeed");
        assert!(deleted.applied);
        assert!(deleted.current.is_none());
        assert!(backend
            .list_object_routes()
            .expect("route listing after delete should succeed")
            .is_empty());

        let handoff = HandoffPlan {
            stable_id: ClientStableId::new("writer"),
            from: sample_runtime(),
            to: ClientRuntimeId::new("writer", ClientEpoch(6)),
            kind: HandoffKind::HotUpgrade,
            barrier_version: 9,
            created_at_ms: 10,
            deadline_ms: Some(20),
        };
        backend
            .put_handoff(&handoff)
            .expect("handoff put should succeed");
        assert_eq!(
            backend
                .get_handoff(&handoff.stable_id)
                .expect("handoff get should succeed"),
            Some(handoff)
        );
    }

    #[test]
    fn redis_backend_list_segments_accepts_empty_tags_reencoded_by_lua() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-empty-tags")),
        )
        .expect("redis backend should initialize");

        let mut segment = sample_segment(SegmentLifecycleState::Active);
        segment.tags.clear();
        segment.capacity_bytes = 1024;
        segment.alignment_bytes = 16;
        backend
            .publish_segment(&segment)
            .expect("segment publish should succeed");

        let reservation = backend
            .reserve_segment(&segment.owner, &segment.segment_name, 13)
            .expect("segment reservation should succeed");
        assert_eq!(reservation.offset_bytes, 0);

        let reserved_segments = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after reserve should succeed");
        assert_eq!(reserved_segments.len(), 1);
        assert!(reserved_segments[0].tags.is_empty());
        assert_eq!(reserved_segments[0].used_bytes, 16);

        backend
            .release_segment(
                &segment.owner,
                &segment.segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
            )
            .expect("segment release should succeed");

        let released_segments = backend
            .list_segments(Some(&segment.owner))
            .expect("segment listing after release should succeed");
        assert_eq!(released_segments, vec![segment]);
    }

    #[test]
    fn redis_backend_backfills_and_prunes_client_index() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let lease = sample_lease(ClientLifecycleState::Active);
        let client_key = keyspace.client(&lease.runtime);
        let index_key = keyspace.client_index();
        let payload = serde_json::to_string(&lease).expect("lease should serialize");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");

        redis::cmd("SET")
            .arg(&client_key)
            .arg(payload)
            .arg("PX")
            .arg(60_000)
            .query::<()>(&mut connection)
            .expect("legacy client lease insert should succeed");

        let listed = backend
            .list_live_clients()
            .expect("legacy client listing should succeed");
        assert_eq!(listed, vec![lease.clone()]);

        let indexed: Vec<String> = connection
            .smembers(&index_key)
            .expect("client index read should succeed");
        assert_eq!(indexed, vec![client_key.clone()]);

        connection
            .del::<_, ()>(&client_key)
            .expect("client lease delete should succeed");

        let listed = backend
            .list_live_clients()
            .expect("stale client listing should succeed");
        assert!(listed.is_empty());

        let indexed: Vec<String> = connection
            .smembers(&index_key)
            .expect("client index read after prune should succeed");
        assert!(indexed.is_empty());
    }

    #[test]
    fn redis_backend_upsert_client_lease_rejects_stale_epoch() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-epoch-cas");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let make_lease = |epoch: u64| ClientLease {
            runtime: ClientRuntimeId::new("client-cas", ClientEpoch(epoch)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };

        backend
            .upsert_client_lease(&make_lease(5))
            .expect("first lease should publish");

        let rejection = backend
            .upsert_client_lease(&make_lease(3))
            .expect_err("older epoch must be rejected");
        assert!(matches!(rejection, StoreError::StaleEpoch(_)));

        let equal = backend
            .upsert_client_lease(&make_lease(5))
            .expect("same (stable_id, epoch) is a refresh");
        let _ = equal;

        backend
            .upsert_client_lease(&make_lease(6))
            .expect("strictly greater epoch must succeed");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("client-cas")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("6"));
        let members: Vec<String> = connection
            .smembers(keyspace.client_by_stable_index(&ClientStableId::new("client-cas")))
            .expect("by-stable index read should succeed");
        let mut sorted = members.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["5".to_string(), "6".to_string()]);
    }

    #[test]
    fn redis_backend_allocate_client_lease_assigns_monotonic_epochs() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-allocate");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let template = |stable_id: &str| ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(0)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };

        let first = backend
            .allocate_client_lease(&template("client-alloc"))
            .expect("first allocation should succeed");
        let second = backend
            .allocate_client_lease(&template("client-alloc"))
            .expect("second allocation should succeed");
        let isolated = backend
            .allocate_client_lease(&template("client-other"))
            .expect("independent stable_id gets its own counter");
        assert_eq!(first.epoch, ClientEpoch(1));
        assert_eq!(second.epoch, ClientEpoch(2));
        assert_eq!(isolated.epoch, ClientEpoch(1));

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("client-alloc")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("2"));

        let listed = backend
            .list_live_clients()
            .expect("list live clients should succeed");
        assert_eq!(listed.len(), 3);
        let stored_epochs: Vec<ClientEpoch> = listed
            .iter()
            .filter(|lease| lease.runtime.stable_id.0 == "client-alloc")
            .map(|lease| lease.runtime.epoch)
            .collect();
        let mut sorted = stored_epochs.clone();
        sorted.sort();
        assert_eq!(sorted, vec![ClientEpoch(1), ClientEpoch(2)]);
    }

    #[test]
    fn redis_backend_upsert_client_lease_cleans_by_stable_index_on_expiry() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-client-by-stable-prune");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let lease = ClientLease {
            runtime: ClientRuntimeId::new("prune-me", ClientEpoch(2)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        backend
            .upsert_client_lease(&lease)
            .expect("lease should publish");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        connection
            .del::<_, ()>(keyspace.client(&lease.runtime))
            .expect("simulate lease expiry by deleting key");

        let listed = backend
            .list_live_clients()
            .expect("list should succeed with stale key");
        assert!(listed.is_empty());

        let members: Vec<String> = connection
            .smembers(keyspace.client_by_stable_index(&ClientStableId::new("prune-me")))
            .expect("by-stable index read should succeed");
        assert!(
            members.is_empty(),
            "by-stable index should prune expired epoch"
        );
        let hwm: Option<String> = connection
            .get(keyspace.client_epoch_hwm(&ClientStableId::new("prune-me")))
            .expect("hwm read should succeed");
        assert_eq!(hwm.as_deref(), Some("2"), "HWM must not regress on expiry");
    }

    #[test]
    fn redis_backend_backfills_and_prunes_object_index() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-object-index");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let route = sample_route(3);
        backend
            .compare_and_swap_object_route(&route.key, None, Some(&route))
            .expect("route create should succeed");

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        connection
            .del::<_, ()>(keyspace.object_index())
            .expect("object index delete should succeed");

        let listed = backend
            .list_object_routes()
            .expect("legacy object listing should succeed");
        assert_eq!(listed, vec![route.clone()]);

        let indexed: Vec<String> = connection
            .smembers(keyspace.object_index())
            .expect("object index read should succeed");
        assert_eq!(indexed, vec![keyspace.object(&route.key)]);

        connection
            .del::<_, ()>(keyspace.object(&route.key))
            .expect("object route delete should succeed");

        let listed = backend
            .list_object_routes()
            .expect("stale object listing should succeed");
        assert!(listed.is_empty());

        let indexed: Vec<String> = connection
            .smembers(keyspace.object_index())
            .expect("object index read after prune should succeed");
        assert!(indexed.is_empty());
    }

    #[test]
    fn redis_backend_route_policy_supports_overwrite_list_and_delete() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-route-policy");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");
        let creator = ClientRuntimeId::new("route-owner", ClientEpoch(7));
        let default_policy = RoutePolicy {
            route_topk: 2,
            route_control: RouteControlMode::EmbeddedWrh,
            created_by: creator.clone(),
            created_at_ms: 11,
        };
        let tenant_domain = RoutePolicyDomain::Tenant("tenant/a".to_string());
        let tenant_policy = RoutePolicy {
            route_topk: 4,
            route_control: RouteControlMode::MetadataOnly,
            created_by: creator,
            created_at_ms: 22,
        };

        assert!(backend
            .put_route_policy_if_absent(&RoutePolicyDomain::Default, &default_policy)
            .expect("default route policy bootstrap should succeed"));
        backend
            .put_route_policy(&tenant_domain, &tenant_policy)
            .expect("tenant route policy overwrite should succeed");
        let listed = backend
            .list_route_policies()
            .expect("route policy listing should succeed");
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().any(|(domain, policy)| {
            *domain == RoutePolicyDomain::Default && *policy == default_policy
        }));
        assert!(listed
            .iter()
            .any(|(domain, policy)| *domain == tenant_domain && *policy == tenant_policy));
        assert!(backend
            .delete_route_policy(&tenant_domain)
            .expect("tenant route policy delete should succeed"));
        assert_eq!(
            backend
                .get_route_policy(&tenant_domain)
                .expect("tenant route policy read should succeed"),
            None,
        );
    }

    #[test]
    fn redis_backend_idempotent_write_retries_until_reconnected() {
        let _guard = env_lock();
        if Command::new("redis-server")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "20");
        std::env::set_var(REDIS_RETRY_DELAY_ENV, "50");

        let Ok(listener) = TcpListener::bind("127.0.0.1:0") else {
            return;
        };
        let port = listener
            .local_addr()
            .expect("free redis port should have addr")
            .port();
        drop(listener);
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(format!("redis://127.0.0.1:{port}/0"))
                .keyspace(MetadataKeyspace::new("test/redis-retry")),
        )
        .expect("redis backend should initialize before server is reachable");

        let server = std::thread::spawn(move || {
            sleep(Duration::from_millis(150));
            RedisTestServer::start_on_port(port, None).expect("delayed redis server should start")
        });

        let lease = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&lease)
            .expect("lease upsert should reconnect and retry");
        let _server = server.join().expect("redis server thread should join");
        assert_eq!(
            backend
                .list_live_clients()
                .expect("live clients should list after reconnect"),
            vec![lease]
        );

        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);
    }

    #[test]
    fn readonly_metadata_retry_classifier_accepts_transient_io() {
        let interrupted =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::Interrupted));
        let timed_out = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::TimedOut));
        let dropped = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
        let refused =
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionRefused));

        assert!(should_retry_readonly_redis_error(&interrupted));
        assert!(should_retry_readonly_redis_error(&timed_out));
        assert!(should_retry_readonly_redis_error(&dropped));
        assert!(should_retry_readonly_redis_error(&refused));
    }

    #[test]
    fn redis_backend_cleanup_stale_segments_removes_dead_owner_metadata() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-cleanup");
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url()).keyspace(keyspace.clone()),
        )
        .expect("redis backend should initialize");

        let live = sample_lease(ClientLifecycleState::Active);
        backend
            .upsert_client_lease(&live)
            .expect("live lease should upsert");

        let live_segment = sample_segment(SegmentLifecycleState::Active);
        backend
            .publish_segment(&live_segment)
            .expect("live segment publish should succeed");

        let dead_runtime = ClientRuntimeId::new("dead", ClientEpoch(8));
        let dead_segment = SegmentAnnouncement {
            owner: dead_runtime.clone(),
            segment_name: SegmentName::new("dead-segment"),
            capacity_bytes: 256,
            used_bytes: 64,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec!["dram".to_string()],
        };
        backend
            .publish_segment(&dead_segment)
            .expect("dead segment publish should succeed");

        let report = backend
            .cleanup_stale_segments()
            .expect("stale segment cleanup should succeed");
        assert_eq!(report.live_clients, 1);
        assert!(report.inspected_segment_keys >= 2);
        assert_eq!(report.removed_segment_keys, 1);

        let segments = backend
            .list_segments(None)
            .expect("segment listing after cleanup should succeed");
        assert_eq!(segments, vec![live_segment.clone()]);

        let client = redis::Client::open(server.url()).expect("redis client should open");
        let mut connection = client
            .get_connection()
            .expect("redis connection should succeed");
        let global_index: Vec<String> = connection
            .smembers(keyspace.segment_index(None))
            .expect("global segment index should load");
        assert_eq!(
            global_index,
            vec![keyspace.segment(&live_segment.owner, &live_segment.segment_name)]
        );
        let dead_owner_index: Vec<String> = connection
            .smembers(keyspace.segment_index(Some(&dead_runtime)))
            .expect("dead owner index should load");
        assert!(dead_owner_index.is_empty());
    }

    #[test]
    fn redis_backend_tenant_policy_supports_versioned_put_list_and_delete() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-policy")),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant/a", Some("domain-1"), None::<String>);
        let policy = TenantPolicy {
            scope: scope.clone(),
            spec: TenantPolicySpec {
                quota: Some(TenantQuotaPolicy {
                    max_bytes: Some(128),
                    max_objects: Some(8),
                }),
                ..TenantPolicySpec::default()
            },
            version: 1,
            updated_at_ms: 10,
            updated_by: "admin".to_string(),
        };

        let stored = backend
            .put_tenant_policy(&policy, None)
            .expect("tenant policy insert should succeed");
        assert_eq!(stored, policy);
        assert_eq!(
            backend
                .get_tenant_policy(&scope)
                .expect("tenant policy read should succeed"),
            Some(policy.clone())
        );

        let mut updated = policy.clone();
        updated.version = 2;
        updated.updated_at_ms = 20;
        updated.updated_by = "admin-2".to_string();
        updated.spec.quota.as_mut().unwrap().max_objects = Some(16);
        let error = backend
            .put_tenant_policy(&updated, Some(3))
            .expect_err("mismatched version should fail");
        assert!(matches!(error, StoreError::Conflict(_)));

        backend
            .put_tenant_policy(&updated, Some(1))
            .expect("tenant policy update should succeed");
        assert_eq!(
            backend
                .list_tenant_policies()
                .expect("tenant policy listing should succeed"),
            vec![updated.clone()]
        );
        assert!(backend
            .delete_tenant_policy(&scope, Some(2))
            .expect("tenant policy delete should succeed"));
        assert_eq!(
            backend
                .get_tenant_policy(&scope)
                .expect("tenant policy read after delete should succeed"),
            None
        );
    }

    #[test]
    fn redis_backend_tenant_quota_reservation_finalize_and_abort_follow_versioned_state_machine() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-quota")),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-a", None::<String>, None::<String>);
        let key = ObjectKey::new("tenant-a::alpha");
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        let reserved = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-create".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 200,
                created_at_ms: 100,
                writer_runtime: writer.clone(),
            })
            .expect("reservation should succeed");
        assert_eq!(reserved.quota.pending_reserved_bytes, 32);
        assert_eq!(reserved.quota.pending_reserved_objects, 1);
        assert!(reserved.object.is_none());
        assert_eq!(
            reserved.reservation.state,
            TenantQuotaReservationState::Pending
        );

        let finalized = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-create".to_string(),
                expected_object_version: None,
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 120,
                updated_by: "writer".to_string(),
            })
            .expect("finalize should succeed");
        assert_eq!(finalized.quota.used_bytes, 32);
        assert_eq!(finalized.quota.used_objects, 1);
        assert_eq!(finalized.quota.pending_reserved_bytes, 0);
        assert_eq!(finalized.quota.pending_reserved_objects, 0);
        assert_eq!(
            finalized
                .object
                .as_ref()
                .expect("object accounting should exist")
                .committed_length,
            32
        );
        assert_eq!(
            finalized.reservation.state,
            TenantQuotaReservationState::Finalized
        );

        let duplicate_finalize = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-create".to_string(),
                expected_object_version: Some(1),
                committed_length: Some(32),
                route_version: None,
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 121,
                updated_by: "writer".to_string(),
            })
            .expect("duplicate finalize should be idempotent");
        assert_eq!(duplicate_finalize.quota.used_bytes, 32);
        assert_eq!(duplicate_finalize.reservation.version, 2);

        let overwrite = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-overwrite".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: 8,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 260,
                created_at_ms: 140,
                writer_runtime: writer.clone(),
            })
            .expect("overwrite reservation should succeed");
        assert_eq!(
            overwrite
                .object
                .expect("object accounting should exist")
                .version,
            1
        );
        assert_eq!(overwrite.quota.pending_reserved_bytes, 8);

        let conflict = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-conflict".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(99),
                delta_bytes: 1,
                delta_objects: 0,
                limit: TenantQuotaPolicy::default(),
                expires_at_ms: 300,
                created_at_ms: 150,
                writer_runtime: writer.clone(),
            })
            .expect_err("stale object version should fail");
        assert!(matches!(conflict, StoreError::Conflict(_)));

        let aborted = backend
            .abort_tenant_quota("resv-overwrite")
            .expect("abort should succeed");
        assert_eq!(aborted.quota.pending_reserved_bytes, 0);
        assert_eq!(aborted.quota.used_bytes, 32);
        assert_eq!(
            aborted.reservation.state,
            TenantQuotaReservationState::Aborted
        );

        let duplicate_abort = backend
            .abort_tenant_quota("resv-overwrite")
            .expect("duplicate abort should be idempotent");
        assert_eq!(duplicate_abort.quota.pending_reserved_bytes, 0);
        assert_eq!(duplicate_abort.reservation.version, 2);

        let delete_reserved = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-delete".to_string(),
                scope: scope.clone(),
                key: key.clone(),
                expected_object_version: Some(1),
                delta_bytes: -32,
                delta_objects: -1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 400,
                created_at_ms: 200,
                writer_runtime: writer.clone(),
            })
            .expect("delete reservation should succeed");
        assert_eq!(delete_reserved.quota.pending_reserved_bytes, 0);
        assert_eq!(delete_reserved.quota.used_bytes, 32);

        let deleted = backend
            .finalize_tenant_quota(&TenantQuotaFinalizeRequest {
                reservation_id: "resv-delete".to_string(),
                expected_object_version: Some(1),
                committed_length: None,
                route_version: None,
                state: TenantObjectAccountingState::Deleted,
                updated_at_ms: 220,
                updated_by: "writer".to_string(),
            })
            .expect("delete finalize should succeed");
        assert_eq!(deleted.quota.used_bytes, 0);
        assert_eq!(deleted.quota.used_objects, 0);
        assert!(deleted.object.is_none());
        assert!(backend
            .get_tenant_object_accounting(&key)
            .expect("object accounting lookup should succeed")
            .is_none());
    }

    #[test]
    fn redis_backend_tenant_quota_reservation_enforces_limits_and_lists_by_tenant() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-tenant-quota-limits")),
        )
        .expect("redis backend should initialize");
        let scope = TenantPolicyScope::new("tenant-b", None::<String>, None::<String>);
        let writer = ClientRuntimeId::new("writer", ClientEpoch(1));

        backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-1".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::one"),
                expected_object_version: None,
                delta_bytes: 40,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(1),
                },
                expires_at_ms: 100,
                created_at_ms: 10,
                writer_runtime: writer.clone(),
            })
            .expect("first reservation should succeed");

        let byte_limit = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-2".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::two"),
                expected_object_version: None,
                delta_bytes: 32,
                delta_objects: 0,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(64),
                    max_objects: Some(2),
                },
                expires_at_ms: 110,
                created_at_ms: 11,
                writer_runtime: writer.clone(),
            })
            .expect_err("bytes over limit should fail");
        assert!(matches!(byte_limit, StoreError::Conflict(_)));

        let object_limit = backend
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "resv-3".to_string(),
                scope: scope.clone(),
                key: ObjectKey::new("tenant-b::three"),
                expected_object_version: None,
                delta_bytes: 1,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(128),
                    max_objects: Some(1),
                },
                expires_at_ms: 120,
                created_at_ms: 12,
                writer_runtime: writer,
            })
            .expect_err("objects over limit should fail");
        assert!(matches!(object_limit, StoreError::Conflict(_)));

        let reservations = backend
            .list_tenant_quota_reservations(&scope)
            .expect("reservation listing should succeed");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].reservation_id, "resv-1");
        let quota = backend
            .get_tenant_quota_state(&scope)
            .expect("quota state lookup should succeed")
            .expect("quota state should exist");
        assert_eq!(quota.pending_reserved_bytes, 40);
        assert_eq!(quota.pending_reserved_objects, 1);
    }

    #[test]
    fn redis_connection_info_injects_env_auth_when_url_has_no_credentials() {
        let _guard = env_lock();
        std::env::set_var("MC_REDIS_USERNAME", "cloud-user");
        std::env::set_var("MC_REDIS_PASSWORD", "cloud-pass");

        let info = redis_connection_info("redis://127.0.0.1:6379/7")
            .expect("redis connection info should parse");
        assert_eq!(info.redis.username.as_deref(), Some("cloud-user"));
        assert_eq!(info.redis.password.as_deref(), Some("cloud-pass"));
        assert_eq!(info.redis.db, 7);

        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
    }

    #[test]
    fn redis_connection_info_prefers_explicit_url_credentials() {
        let _guard = env_lock();
        std::env::set_var("MC_REDIS_USERNAME", "env-user");
        std::env::set_var("MC_REDIS_PASSWORD", "env-pass");

        let info = redis_connection_info("redis://url-user:url-pass@127.0.0.1:6379/3")
            .expect("redis connection info should parse");
        assert_eq!(info.redis.username.as_deref(), Some("url-user"));
        assert_eq!(info.redis.password.as_deref(), Some("url-pass"));
        assert_eq!(info.redis.db, 3);

        std::env::remove_var("MC_REDIS_USERNAME");
        std::env::remove_var("MC_REDIS_PASSWORD");
    }

    #[test]
    fn legacy_auth_connection_info_strips_username_when_password_exists() {
        let info = redis_connection_info("redis://url-user:url-pass@127.0.0.1:6379/3")
            .expect("redis connection info should parse");
        let legacy =
            legacy_auth_connection_info(&info).expect("legacy auth info should be derived");
        assert_eq!(legacy.redis.username, None);
        assert_eq!(legacy.redis.password.as_deref(), Some("url-pass"));
        assert_eq!(legacy.redis.db, 3);
    }

    #[test]
    fn redis_auth_normalization_strips_username_when_password_auth_works() {
        let _guard = env_lock();
        let Some(server) = RedisTestServer::start_with_password(Some("legacy-pass")) else {
            return;
        };
        let url = server
            .url()
            .replacen("redis://:", "redis://legacy-user:", 1);

        let auth = resolve_redis_auth(&url).expect("redis auth should resolve");
        assert_eq!(auth.username, None);
        assert_eq!(auth.password.as_deref(), Some("legacy-pass"));

        let info = redis_connection_info(&url).expect("redis connection info should parse");
        assert_eq!(info.redis.username, None);
        assert_eq!(info.redis.password.as_deref(), Some("legacy-pass"));
    }

    #[test]
    fn legacy_auth_message_matcher_accepts_auth_arity_errors() {
        assert!(is_legacy_redis_auth_arity_error(
            "redis get_connection: An error was signalled by the server - ERR wrong number of arguments for 'auth' command"
        ));
        assert!(is_legacy_redis_auth_arity_error(
            "redis get_connection: ERR wrong number of arguments for `auth` command"
        ));
        assert!(!is_legacy_redis_auth_arity_error(
            "redis get_connection: WRONGPASS invalid username-password pair"
        ));
    }

    #[test]
    fn redis_timeout_helpers_honor_env_overrides() {
        let _guard = env_lock();
        std::env::set_var(REDIS_CONNECT_TIMEOUT_ENV, "7000");
        std::env::set_var(REDIS_IO_TIMEOUT_ENV, "11000");
        std::env::set_var(REDIS_AUTH_PROBE_TIMEOUT_ENV, "900");
        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "9");
        std::env::set_var(REDIS_RETRY_DELAY_ENV, "80");

        assert_eq!(redis_connect_timeout(), Duration::from_secs(7));
        assert_eq!(redis_io_timeout(), Duration::from_secs(11));
        assert_eq!(redis_auth_probe_timeout(), Duration::from_millis(900));
        assert_eq!(redis_retry_attempts(), 9);
        assert_eq!(redis_retry_delay(), Duration::from_millis(80));

        std::env::remove_var(REDIS_CONNECT_TIMEOUT_ENV);
        std::env::remove_var(REDIS_IO_TIMEOUT_ENV);
        std::env::remove_var(REDIS_AUTH_PROBE_TIMEOUT_ENV);
        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);
        assert_eq!(redis_connect_timeout(), DEFAULT_REDIS_CONNECT_TIMEOUT);
        assert_eq!(redis_io_timeout(), DEFAULT_REDIS_IO_TIMEOUT);
        assert_eq!(redis_auth_probe_timeout(), DEFAULT_REDIS_AUTH_PROBE_TIMEOUT);
        assert_eq!(redis_retry_attempts(), DEFAULT_REDIS_RETRY_ATTEMPTS);
        assert_eq!(redis_retry_delay(), DEFAULT_REDIS_RETRY_DELAY);
    }

    #[test]
    fn route_namespace_redacts_url_credentials() {
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new("redis://user:secret@127.0.0.1:6379/0")
                .keyspace(MetadataKeyspace::new("test/redacted")),
        )
        .expect("redis backend should initialize");

        let namespace = backend.route_namespace();
        assert!(namespace.contains("127.0.0.1:6379/0"));
        assert!(namespace.contains("test/redacted"));
        assert!(!namespace.contains("user:secret"));
        assert_eq!(
            redacted_route_namespace_source("redis://user:secret@127.0.0.1:6379/0"),
            "redis://127.0.0.1:6379/0".to_string()
        );
    }

    // -----------------------------------------------------------------------
    // Adversarial: transient-error classification
    // -----------------------------------------------------------------------

    #[test]
    fn transient_error_classifier_covers_all_io_error_kinds() {
        use super::should_retry_transient_redis_error;
        let transient_kinds = [
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::BrokenPipe,
            std::io::ErrorKind::ConnectionRefused,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::NotConnected,
        ];
        for kind in &transient_kinds {
            let err = redis::RedisError::from(std::io::Error::from(*kind));
            assert!(
                should_retry_transient_redis_error(&err),
                "IO kind {kind:?} must classify as transient"
            );
        }
    }

    #[test]
    fn transient_error_classifier_matches_message_patterns() {
        use super::should_retry_transient_redis_error;
        let transient_messages = [
            "connection refused",
            "Connection reset by peer",
            "CONNECTION ABORTED",
            "connection closed unexpectedly",
            "not connected to server",
            "broken pipe",
            "resource temporarily unavailable",
            "operation timed out",
            "interrupted system call",
        ];
        for message in &transient_messages {
            let err =
                redis::RedisError::from((redis::ErrorKind::IoError, "test", message.to_string()));
            assert!(
                should_retry_transient_redis_error(&err),
                "message '{message}' must classify as transient"
            );
        }
    }

    #[test]
    fn transient_error_classifier_rejects_non_transient_errors() {
        use super::should_retry_transient_redis_error;
        let non_transient = [
            redis::RedisError::from((
                redis::ErrorKind::AuthenticationFailed,
                "WRONGPASS",
                "invalid username-password pair".to_string(),
            )),
            redis::RedisError::from((
                redis::ErrorKind::TypeError,
                "WRONGTYPE",
                "Operation against a key holding the wrong kind of value".to_string(),
            )),
            redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "ERR",
                "unknown command 'FOOBAR'".to_string(),
            )),
            redis::RedisError::from((
                redis::ErrorKind::ReadOnly,
                "READONLY",
                "You can't write against a read only replica".to_string(),
            )),
        ];
        for err in &non_transient {
            assert!(
                !should_retry_transient_redis_error(err),
                "err {err:?} must NOT classify as transient"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Adversarial: retry-config env-var handling
    // -----------------------------------------------------------------------

    #[test]
    fn redis_retry_config_defaults_are_sane_when_env_unset() {
        let _guard = env_lock();
        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);

        assert_eq!(redis_retry_attempts(), DEFAULT_REDIS_RETRY_ATTEMPTS);
        assert_eq!(redis_retry_delay(), DEFAULT_REDIS_RETRY_DELAY);
        assert_eq!(redis_connect_timeout(), DEFAULT_REDIS_CONNECT_TIMEOUT);
        assert_eq!(redis_io_timeout(), DEFAULT_REDIS_IO_TIMEOUT);
    }

    #[test]
    fn redis_retry_config_rejects_zero_and_invalid_values() {
        let _guard = env_lock();
        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "0");
        assert_eq!(
            redis_retry_attempts(),
            DEFAULT_REDIS_RETRY_ATTEMPTS,
            "zero attempts must fall back to default"
        );
        std::env::set_var(REDIS_RETRY_ATTEMPTS_ENV, "not-a-number");
        assert_eq!(
            redis_retry_attempts(),
            DEFAULT_REDIS_RETRY_ATTEMPTS,
            "non-numeric must fall back to default"
        );
        std::env::set_var(REDIS_RETRY_DELAY_ENV, "");
        assert_eq!(
            redis_retry_delay(),
            DEFAULT_REDIS_RETRY_DELAY,
            "empty string must fall back to default"
        );
        std::env::remove_var(REDIS_RETRY_ATTEMPTS_ENV);
        std::env::remove_var(REDIS_RETRY_DELAY_ENV);
    }

    // -----------------------------------------------------------------------
    // Adversarial: Redis integration (skipped if redis-server unavailable)
    // -----------------------------------------------------------------------

    #[test]
    fn redis_backend_concurrent_route_cas_only_one_wins() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = std::sync::Arc::new(
            RedisMetadataBackend::new(
                RedisMetadataConfig::new(server.url())
                    .keyspace(MetadataKeyspace::new("test/redis-concurrent-cas")),
            )
            .expect("redis backend"),
        );

        let key = ObjectKey::new("contested-key");
        let wins = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let backend = backend.clone();
                let key = key.clone();
                let wins = wins.clone();
                std::thread::spawn(move || {
                    let mut route = sample_route(1);
                    route.key = key.clone();
                    route.replicas[0].owner =
                        ClientRuntimeId::new(format!("racer-{i}"), ClientEpoch(1));
                    let res = backend
                        .compare_and_swap_object_route(&key, None, Some(&route))
                        .unwrap();
                    if res.applied {
                        wins.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            wins.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "exactly one insert CAS must win"
        );
    }

    #[test]
    fn redis_backend_route_cas_delete_with_wrong_version_does_not_apply() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-cas-delete")),
        )
        .expect("redis backend");

        let key = ObjectKey::new("delete-me");
        let route = sample_route(1);
        let inserted = backend
            .compare_and_swap_object_route(&key, None, Some(&route))
            .expect("insert");
        assert!(inserted.applied);

        let wrong = backend
            .compare_and_swap_object_route(&key, Some(RouteVersion(99)), None)
            .expect("cas must not crash on wrong version");
        assert!(!wrong.applied);
        assert!(
            backend.get_object_route(&key).unwrap().is_some(),
            "route must survive the failed CAS-delete"
        );
    }

    #[test]
    fn redis_backend_route_cas_version_chain_is_sequential() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-cas-chain")),
        )
        .expect("redis backend");

        let key = ObjectKey::new("versioned-key");
        let r1 = {
            let mut r = sample_route(1);
            r.key = key.clone();
            r
        };
        let r2 = {
            let mut r = sample_route(2);
            r.key = key.clone();
            r
        };
        let r3 = {
            let mut r = sample_route(3);
            r.key = key.clone();
            r
        };
        assert!(
            backend
                .compare_and_swap_object_route(&key, None, Some(&r1))
                .unwrap()
                .applied
        );
        assert!(
            backend
                .compare_and_swap_object_route(&key, Some(RouteVersion(1)), Some(&r2))
                .unwrap()
                .applied
        );
        assert!(
            backend
                .compare_and_swap_object_route(&key, Some(RouteVersion(2)), Some(&r3))
                .unwrap()
                .applied
        );

        let current = backend.get_object_route(&key).unwrap().unwrap();
        assert_eq!(current.version, RouteVersion(3));
    }

    #[test]
    fn redis_backend_multiple_keyspaces_are_isolated() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let url = server.url();

        let backend_a = RedisMetadataBackend::new(
            RedisMetadataConfig::new(url).keyspace(MetadataKeyspace::new("test/ns-a")),
        )
        .expect("backend A");
        let backend_b = RedisMetadataBackend::new(
            RedisMetadataConfig::new(url).keyspace(MetadataKeyspace::new("test/ns-b")),
        )
        .expect("backend B");

        let lease_a = ClientLease {
            runtime: ClientRuntimeId::new("node-a", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        let lease_b = ClientLease {
            runtime: ClientRuntimeId::new("node-b", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: Default::default(),
            expires_at_ms: super::now_ms() + 60_000,
        };
        backend_a.upsert_client_lease(&lease_a).unwrap();
        backend_b.upsert_client_lease(&lease_b).unwrap();

        let listed_a = backend_a.list_live_clients().unwrap();
        let listed_b = backend_b.list_live_clients().unwrap();
        assert_eq!(listed_a.len(), 1);
        assert_eq!(listed_b.len(), 1);
        assert_eq!(listed_a[0].runtime.stable_id.0, "node-a");
        assert_eq!(listed_b[0].runtime.stable_id.0, "node-b");
    }

    #[test]
    fn redis_backend_handoff_put_get_round_trip() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-handoff")),
        )
        .expect("redis backend");

        let stable_id = ClientStableId::new("handoff-node");
        let plan = HandoffPlan {
            stable_id: stable_id.clone(),
            from: ClientRuntimeId::new("old-runtime", ClientEpoch(1)),
            to: ClientRuntimeId::new("new-runtime", ClientEpoch(2)),
            kind: HandoffKind::GracefulDrain,
            barrier_version: 42,
            created_at_ms: 1_000,
            deadline_ms: Some(5_000),
        };
        backend.put_handoff(&plan).unwrap();

        let got = backend
            .get_handoff(&stable_id)
            .unwrap()
            .expect("handoff must be retrievable");
        assert_eq!(got.stable_id.0, "handoff-node");
        assert!(matches!(got.kind, HandoffKind::GracefulDrain));
        assert_eq!(got.barrier_version, 42);
        assert_eq!(got.deadline_ms, Some(5_000));
    }

    #[test]
    fn redis_backend_update_client_state_on_nonexistent_returns_not_found() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-update-ghost")),
        )
        .expect("redis backend");

        let ghost = ClientRuntimeId::new("ghost-node", ClientEpoch(99));
        let err = backend
            .update_client_state(&ghost, ClientLifecycleState::Draining)
            .expect_err("phantom client must not update");
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[test]
    fn redis_backend_unpublish_then_republish_segment() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-unpub-repub")),
        )
        .expect("redis backend");

        let owner = sample_runtime();
        let segment_name = SegmentName::new("volatile-seg");
        let segment = SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            capacity_bytes: 256,
            used_bytes: 0,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 16,
            tags: vec![],
        };

        backend.publish_segment(&segment).unwrap();
        assert_eq!(backend.list_segments(Some(&owner)).unwrap().len(), 1);
        backend.unpublish_segment(&owner, &segment_name).unwrap();
        assert_eq!(backend.list_segments(Some(&owner)).unwrap().len(), 0);
        backend.publish_segment(&segment).unwrap();
        assert_eq!(backend.list_segments(Some(&owner)).unwrap().len(), 1);
    }

    #[test]
    fn redis_backend_segment_draining_rejects_new_reservations() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-seg-drain")),
        )
        .expect("redis backend");

        let owner = sample_runtime();
        let segment_name = SegmentName::new("drain-seg");
        let segment = SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            capacity_bytes: 512,
            used_bytes: 0,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: vec![],
        };
        backend.publish_segment(&segment).unwrap();
        backend
            .update_segment_state(&owner, &segment_name, SegmentLifecycleState::Draining)
            .unwrap();

        let err = backend
            .reserve_segment(&owner, &segment_name, 64)
            .expect_err("draining segment must reject new reservations");
        assert!(matches!(
            err,
            StoreError::InvalidState(_) | StoreError::NotFound(_)
        ));
    }

    #[test]
    fn redis_backend_segment_exhaustion_returns_error() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let backend = RedisMetadataBackend::new(
            RedisMetadataConfig::new(server.url())
                .keyspace(MetadataKeyspace::new("test/redis-seg-exhaust")),
        )
        .expect("redis backend");

        let owner = sample_runtime();
        backend
            .publish_segment(&SegmentAnnouncement {
                owner: owner.clone(),
                segment_name: SegmentName::new("tiny-seg"),
                capacity_bytes: 64,
                used_bytes: 0,
                state: SegmentLifecycleState::Active,
                alignment_bytes: 1,
                tags: vec![],
            })
            .unwrap();

        backend
            .reserve_segment(&owner, &SegmentName::new("tiny-seg"), 32)
            .unwrap();
        backend
            .reserve_segment(&owner, &SegmentName::new("tiny-seg"), 32)
            .unwrap();
        let err = backend
            .reserve_segment(&owner, &SegmentName::new("tiny-seg"), 1)
            .expect_err("must reject reserve past capacity");
        assert!(matches!(err, StoreError::Allocator(_)));
    }
}
