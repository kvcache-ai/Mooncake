use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, HandoffPlan,
    MetadataBackend, ObjectKey, ObjectRoute, Result, RouteVersion, SegmentAnnouncement, SegmentName,
    StoreError,
};
use redis::{Commands, Script};

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

#[derive(Clone, Debug)]
pub struct RedisMetadataConfig {
    pub url: String,
    pub keyspace: MetadataKeyspace,
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
    keyspace: MetadataKeyspace,
}

impl RedisMetadataBackend {
    pub fn new(config: RedisMetadataConfig) -> Result<Self> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|error| metadata_error("redis client open", error))?;
        Ok(Self {
            client,
            keyspace: config.keyspace,
        })
    }

    fn connection(&self) -> Result<redis::Connection> {
        self.client
            .get_connection()
            .map_err(|error| metadata_error("redis get_connection", error))
    }
}

impl MetadataBackend for RedisMetadataBackend {
    fn upsert_client_lease(&self, lease: &ClientLease) -> Result<()> {
        let mut connection = self.connection()?;
        let key = self.keyspace.client(&lease.runtime);
        let payload = serde_json::to_string(lease).map_err(json_error)?;
        let ttl_ms = lease.expires_at_ms.saturating_sub(now_ms()).max(1);
        redis::cmd("SET")
            .arg(key)
            .arg(payload)
            .arg("PX")
            .arg(ttl_ms)
            .query::<()>(&mut connection)
            .map_err(|error| metadata_error("redis set client lease", error))
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
        let mut connection = self.connection()?;
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(self.keyspace.client_pattern())
            .query(&mut connection)
            .map_err(|error| metadata_error("redis keys clients", error))?;
        let mut leases = Vec::with_capacity(keys.len());
        for key in keys {
            let payload: Option<String> = connection
                .get(&key)
                .map_err(|error| metadata_error("redis get client lease", error))?;
            if let Some(payload) = payload {
                let lease = serde_json::from_str(&payload).map_err(json_error)?;
                leases.push(lease);
            }
        }
        Ok(leases)
    }

    fn publish_segment(&self, segment: &SegmentAnnouncement) -> Result<()> {
        let mut connection = self.connection()?;
        let key = self.keyspace.segment(&segment.owner, &segment.segment_name);
        let payload = serde_json::to_string(segment).map_err(json_error)?;
        connection
            .set::<_, _, ()>(key, payload)
            .map_err(|error| metadata_error("redis set segment", error))
    }

    fn unpublish_segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> Result<()> {
        let mut connection = self.connection()?;
        let key = self.keyspace.segment(owner, segment);
        connection
            .del::<_, ()>(key)
            .map_err(|error| metadata_error("redis del segment", error))
    }

    fn get_object_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        let mut connection = self.connection()?;
        let key = self.keyspace.object(key);
        let payload: Option<String> = redis::cmd("HGET")
            .arg(key)
            .arg("payload")
            .query(&mut connection)
            .map_err(|error| metadata_error("redis hget route payload", error))?;
        payload
            .map(|payload| serde_json::from_str(&payload).map_err(json_error))
            .transpose()
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
        let current_payload = Script::new(CAS_OBJECT_ROUTE_SCRIPT)
            .key(self.keyspace.object(key))
            .arg(expected.map(|version| version.0.to_string()).unwrap_or_else(|| "__none__".to_string()))
            .arg(next_version.to_string())
            .arg(payload.clone().unwrap_or_else(|| "__delete__".to_string()))
            .invoke::<(i32, String)>(&mut connection)
            .map_err(|error| metadata_error("redis cas object route", error))?;

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

    fn put_handoff(&self, handoff: &HandoffPlan) -> Result<()> {
        let mut connection = self.connection()?;
        let key = self.keyspace.handoff(&handoff.stable_id);
        let payload = serde_json::to_string(handoff).map_err(json_error)?;
        connection
            .set::<_, _, ()>(key, payload)
            .map_err(|error| metadata_error("redis set handoff", error))
    }

    fn get_handoff(&self, stable_id: &ClientStableId) -> Result<Option<HandoffPlan>> {
        let mut connection = self.connection()?;
        let key = self.keyspace.handoff(stable_id);
        let payload: Option<String> = connection
            .get(&key)
            .map_err(|error| metadata_error("redis get handoff", error))?;
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

fn json_error(error: serde_json::Error) -> StoreError {
    StoreError::Metadata(format!("json serialization: {error}"))
}
