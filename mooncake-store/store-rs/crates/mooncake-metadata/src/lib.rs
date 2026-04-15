mod etcd_backend;
mod in_memory;
mod keyspace;
mod redis_backend;
mod segment_state;

pub use etcd_backend::{EtcdMetadataBackend, EtcdMetadataConfig};
pub use in_memory::InMemoryMetadataBackend;
pub use keyspace::MetadataKeyspace;
pub use redis_backend::{
    is_legacy_redis_auth_arity_error, resolve_redis_auth, RedisMetadataBackend,
    RedisMetadataConfig, ResolvedRedisAuth,
};
