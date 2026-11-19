use mooncake_store_core::{ColdBackingState, ObjectRoute, Result, StoreError};
use redis::{Commands, Script};

use super::{json_error, metadata_error, RedisMetadataBackend};

const ACQUIRE_REPAIR: &str = r#"
if redis.call('GET', KEYS[1]) == '1' then return -1 end
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
local token = redis.call('INCR', KEYS[3])
redis.call('SET', KEYS[2], tostring(token), 'PX', 30000)
return token
"#;

const RENEW_REPAIR: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], 30000)
"#;

const RELEASE_REPAIR: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"#;

const COMPLETE_REPAIR: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], '1')
redis.call('SET', KEYS[3], '1')
redis.call('DEL', KEYS[1])
return 1
"#;

const REPAIR_ROUTE: &str = r#"
if redis.call('HGET', KEYS[1], 'payload') ~= ARGV[1] then return 0 end
for i = 2, #KEYS do redis.call('SADD', KEYS[i], KEYS[1]) end
return 1
"#;

impl RedisMetadataBackend {
    pub(super) fn nof_state_indexes(&self) -> [String; 3] {
        [
            ColdBackingState::PendingOffload,
            ColdBackingState::Materialized,
            ColdBackingState::PendingDelete,
        ]
        .map(|state| self.keyspace.object_nof_backing_state_index(state))
    }

    pub(super) fn nof_index_ready_key(&self, kind: &str) -> String {
        format!("{}/nof-indexes/{kind}-v1", self.keyspace.object_index())
    }

    // Namespace migration is shared by every target. A missing target set only
    // means "empty" after this migration has accounted for legacy routes.
    pub(super) fn ensure_nof_backing_indexes(&self) -> Result<()> {
        let ready = self.nof_index_ready_key("targets");
        let states_ready = self.nof_index_ready_key("states");
        let lock = format!("{ready}/repair");
        let sequence = format!("{ready}/sequence");
        let mut connection = self.connection("redis prepare NoF indexes")?;
        let token: i64 = Script::new(ACQUIRE_REPAIR)
            .key(&ready)
            .key(&lock)
            .key(&sequence)
            .invoke(&mut connection)
            .map_err(|error| metadata_error("redis acquire NoF index repair", error))?;
        match token {
            -1 => return Ok(()),
            // Drop the pooled connection on return. Waiting for another worker
            // here would block unrelated metadata operations on a small pool.
            0 => {
                return Err(StoreError::Backpressure(
                    "NoF index repair is in progress; retry later".into(),
                ))
            }
            _ => {}
        }
        let result = (|| {
            let complete: bool = connection
                .exists(&states_ready)
                .map_err(|error| metadata_error("redis inspect NoF state indexes", error))?;
            let sources = if complete {
                self.nof_state_indexes().to_vec()
            } else {
                vec![self.keyspace.object_index()]
            };
            for source in sources {
                let mut cursor = 0u64;
                loop {
                    let renewed: bool = Script::new(RENEW_REPAIR)
                        .key(&lock)
                        .arg(token)
                        .invoke(&mut connection)
                        .map_err(|error| metadata_error("redis renew NoF index repair", error))?;
                    if !renewed {
                        return Err(StoreError::Metadata(
                            "NoF index repair lease expired; retry namespace migration".into(),
                        ));
                    }
                    let (next, keys): (u64, Vec<String>) = redis::cmd("SSCAN")
                        .arg(&source)
                        .arg(cursor)
                        .arg("COUNT")
                        .arg(256)
                        .query(&mut connection)
                        .map_err(|error| {
                            metadata_error("redis scan NoF index migration page", error)
                        })?;
                    if !keys.is_empty() {
                        let mut pipe = redis::pipe();
                        for key in &keys {
                            pipe.hget(key, "payload");
                        }
                        let payloads: Vec<Option<String>> =
                            pipe.query(&mut connection).map_err(|error| {
                                metadata_error("redis load NoF migration page", error)
                            })?;
                        for (key, payload) in keys.iter().zip(payloads) {
                            if let Some(payload) = payload {
                                self.repair_nof_index_entry(&mut connection, key, &payload)?;
                            }
                        }
                    }
                    cursor = next;
                    if cursor == 0 {
                        break;
                    }
                }
            }
            let completed: bool = Script::new(COMPLETE_REPAIR)
                .key(&lock)
                .key(&states_ready)
                .key(&ready)
                .arg(token)
                .invoke(&mut connection)
                .map_err(|error| metadata_error("redis complete NoF index migration", error))?;
            if !completed {
                return Err(StoreError::Metadata(
                    "NoF index repair lease expired before completion".into(),
                ));
            }
            Ok(())
        })();
        if result.is_err() {
            // A failed migration stays incomplete and can be retried. The lease
            // also expires if the repairing process terminates before cleanup.
            let _ = Script::new(RELEASE_REPAIR)
                .key(&lock)
                .arg(token)
                .invoke::<i64>(&mut connection);
        }
        result
    }

    pub(super) fn repair_nof_index_entry(
        &self,
        connection: &mut redis::Connection,
        key: &str,
        payload: &str,
    ) -> Result<()> {
        let route: ObjectRoute = serde_json::from_str(payload).map_err(json_error)?;
        let indexes = self.nof_backing_index_keys(Some(&route));
        if indexes.is_empty() {
            return Ok(());
        }
        // The payload guard and SADDs are atomic with route CAS. A concurrent
        // CAS either runs afterward and removes these entries, or has already
        // maintained the newer route's indexes and invalidates this snapshot.
        Script::new(REPAIR_ROUTE)
            .key(key)
            .key(indexes)
            .arg(payload)
            .invoke::<i64>(connection)
            .map_err(|error| metadata_error("redis repair NoF route indexes", error))?;
        Ok(())
    }
}
