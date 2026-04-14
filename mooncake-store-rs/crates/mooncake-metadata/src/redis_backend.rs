use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RoutePolicy, RoutePolicyDomain, RouteVersion,
    SegmentAnnouncement, SegmentLifecycleState, SegmentName, SegmentReservation, StoreError,
};
use redis::{Commands, ConnectionInfo, IntoConnectionInfo, RedisConnectionInfo, Script};

use crate::keyspace::parse_route_policy_domain;
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
        connection
            .set_read_timeout(Some(redis_io_timeout()))
            .map_err(redis::RedisError::from)?;
        connection
            .set_write_timeout(Some(redis_io_timeout()))
            .map_err(redis::RedisError::from)?;
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
            keys = redis::cmd("KEYS")
                .arg(self.keyspace.segment_pattern(None))
                .query(&mut connection)
                .map_err(|error| metadata_error("redis keys segments", error))?;
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

impl MetadataBackend for RedisMetadataBackend {
    fn route_namespace(&self) -> String {
        self.route_namespace.clone()
    }

    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        let key = self.keyspace.client(&lease.runtime);
        let payload = serde_json::to_string(lease).map_err(json_error)?;
        self.query_idempotent_write("redis set client lease", |connection| {
            let ttl_ms = lease.expires_at_ms.saturating_sub(now_ms()).max(1);
            redis::cmd("SET")
                .arg(&key)
                .arg(payload.as_str())
                .arg("PX")
                .arg(ttl_ms)
                .query::<()>(connection)?;
            connection.sadd::<_, _, ()>(self.keyspace.client_index(), key.as_str())?;
            Ok(())
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
        let mut leases = Vec::with_capacity(entries.len());
        for (key, payload) in entries {
            match payload {
                Some(payload) => {
                    let lease =
                        serde_json::from_str::<ClientLease>(&payload).map_err(json_error)?;
                    if lease.expires_at_ms >= now {
                        leases.push(lease);
                    } else {
                        stale.push(key);
                    }
                }
                None => stale.push(key),
            }
        }
        if !stale.is_empty() {
            let mut connection = self.connection()?;
            connection
                .srem::<_, _, ()>(&index, stale)
                .map_err(|error| metadata_error("redis srem stale client index", error))?;
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
        let prefix = format!("{}/system/route-policy/", self.keyspace.prefix());
        let keys: Vec<String> = connection
            .keys(format!("{}*", prefix))
            .map_err(|error| metadata_error("redis keys route policy", error))?;
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
        SegmentName, StoreError,
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
    fn redis_backend_backfills_and_prunes_object_index() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-object-index");
    fn redis_backend_route_policy_supports_overwrite_list_and_delete() {
        let Some(server) = RedisTestServer::start() else {
            return;
        };
        let keyspace = MetadataKeyspace::new("test/redis-route-policy");
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
        assert!(!should_retry_readonly_redis_error(&refused));
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
}
