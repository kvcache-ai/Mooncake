// ---------------------------------------------------------------------------
// perf_invariant_helpers.rs — Self-contained client builder used by the
// perf-invariant tests.  Kept separate from `mod.rs`'s ~12k-line integration
// fixture so the perf gate stays auditable.
// ---------------------------------------------------------------------------

use std::sync::Arc;

use mooncake_store_core::{ClientLifecycleState, MetadataBackend};
use mooncake_store_test_utils::fixtures::test_future_expiry_ms;
use mooncake_store_test_utils::transport::TestTransport;

use crate::{
    LocalMemoryConfig, MooncakeCompatibilityFacade, StoreClient, StoreClientBuilder,
};

/// Tiny `LocalMemoryConfig` suitable for perf-invariant tests:
///   * 64 KiB storage / 4 KiB scratch — large enough to run hundreds of small
///     puts without segment pressure;
///   * `alignment(1)` so 1-byte payloads do not get rounded up;
///   * `numa_aware(false)` so the test runs on any CI host;
///   * `reclaim_grace_ms(0)` so test cleanup is immediate.
pub(super) fn perf_storage_config() -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(64 * 1024)
        .scratch_bytes(4 * 1024)
        .alignment(1)
        .reclaim_grace_ms(0)
}

/// Build an `Active` `StoreClient` wired to the given metadata backend +
/// in-process `TestTransport`.  The client owns its own segment named
/// `<stable_id>-segment`.
pub(super) fn build_perf_client(
    metadata: Arc<dyn MetadataBackend>,
    transport: Arc<TestTransport>,
    stable_id: &str,
    tenant: &str,
) -> StoreClient {
    let client = StoreClientBuilder::new(metadata, stable_id)
        .state(ClientLifecycleState::Active)
        .tenant(tenant)
        .label("pool", "perf-pool")
        .label("storage", "true")
        .segment_name(format!("{stable_id}-segment"))
        .transport(transport)
        .local_memory(perf_storage_config())
        .build(test_future_expiry_ms())
        .expect("perf-test client build should succeed");
    client
        .register_local_memory()
        .expect("perf-test local memory registration should succeed");
    client
}
