mod config;
mod dispatcher;
mod dummy_client;
mod dummy_service;
mod hot_cache;
#[cfg(test)]
mod integration_tests;
mod runtime;
mod shm;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use config::{
    CompatSetupArgs, CompatTimeoutCliOverrides, CompatTimeoutConfig, CompatTransportConfig,
};
pub use dispatcher::{CompatNamespaceScope, CompatObjectScope, StoreDispatcher};
pub use dummy_client::DummySession;
pub use dummy_service::{start_dummy_store_server, DummyStoreServerHandle};
pub use runtime::{finalize_real_dispatcher_setup, CompatRuntime, CompatRuntimeArgs};
pub use shm::{allocate_shared_region, allocate_shared_region_with_options, free_shared_region};

pub const DEFAULT_COMPAT_WORKER_SCOPE: &str = "worker-1";
