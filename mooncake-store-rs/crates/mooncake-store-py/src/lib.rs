pub mod admin;
#[cfg(feature = "python")]
pub mod buffer_pool;
pub mod build_info;
pub mod config;
pub mod dispatcher;
#[cfg(feature = "python")]
mod dummy_client;
pub mod dummy_service;
mod hot_cache;
pub mod runtime;
mod shm;
#[cfg(test)]
mod test_support;

pub const DEFAULT_COMPAT_WORKER_SCOPE: &str = "worker-1";

#[cfg(feature = "python")]
mod python;
