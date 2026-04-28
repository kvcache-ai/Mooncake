// ---------------------------------------------------------------------------
// isolation_test_harness.rs — Multi-client test fixtures used by the
// namespace-isolation and namespace-adversarial test modules.
//
// Goals:
//   * Spin up N independent `StoreClient` instances backed by the same
//     `InMemoryMetadataBackend` so cross-tenant / cross-scope isolation can be
//     exercised in a single process.
//   * Each client owns its own segment (`<stable_id>-segment`) and its own
//     transport peer derived from a shared in-process hub, mirroring how the
//     existing integration tests publish multiple storage nodes.
//   * Provide a wait_for_membership_convergence helper since the one in
//     `mod.rs` is private to that file.
//
// The helpers here are intentionally minimal: no advanced placement
// configuration, no eviction watermarks, no routed-write planner.  The
// isolation tests verify *correctness* of namespace boundaries; performance
// invariants live in `perf_invariant_tests.rs` and Criterion benches.
// ---------------------------------------------------------------------------

use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

use mooncake_metadata::InMemoryMetadataBackend;
use mooncake_store_core::{
    ClientLifecycleState, MetadataBackend, TenantPolicy, TenantPolicyScope, TenantPolicySpec,
    TenantQuotaPolicy,
};
use mooncake_store_test_utils::fixtures::test_future_expiry_ms;
use mooncake_store_test_utils::transport::TestTransport;

use crate::{
    LocalMemoryConfig, MooncakeCompatibilityFacade, StoreClient, StoreClientBuilder,
};

/// A fixture composed of several Active `StoreClient` instances sharing the
/// same `InMemoryMetadataBackend` and `TestTransport` hub.  Each client has a
/// distinct segment so the metadata-backend's segment-owner index keeps them
/// separate.
#[allow(dead_code)]
pub(super) struct IsolationCluster {
    pub metadata: Arc<InMemoryMetadataBackend>,
    pub hub: Arc<TestTransport>,
    pub clients: Vec<Arc<StoreClient>>,
}

impl IsolationCluster {
    /// Build `n` clients named `<prefix>-0`, `<prefix>-1`, ... .  Every
    /// client is configured with the **same default tenant** so the test can
    /// exercise true cross-tenant calls via `*_in_tenant` APIs without the
    /// builder's default-tenant value getting in the way.
    pub fn with_clients(prefix: &str, n: usize) -> Self {
        Self::with_clients_setup(prefix, n, |_metadata| {})
    }

    /// Same as `with_clients`, but invokes `setup(&metadata)` BEFORE any
    /// client is built.  Use this to install per-tenant policies (quota,
    /// QoS, etc.) into the metadata backend before the client warms its
    /// hot caches; otherwise the client may negative-cache "no policy"
    /// for the affected tenants and never observe the late install.
    #[allow(dead_code)]
    pub fn with_clients_setup<F>(prefix: &str, n: usize, setup: F) -> Self
    where
        F: FnOnce(&Arc<InMemoryMetadataBackend>),
    {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let hub = Arc::new(TestTransport::new(&format!("{prefix}-hub")));
        setup(&metadata);
        let mut clients = Vec::with_capacity(n);
        for i in 0..n {
            let stable_id = format!("{prefix}-{i}");
            let segment = format!("{stable_id}-segment");
            let client_transport = Arc::new(hub.peer(&segment));
            let client = StoreClientBuilder::new(metadata.clone(), &stable_id)
                .state(ClientLifecycleState::Active)
                .tenant("default") // default; tests use *_in_tenant explicitly
                .label("pool", "isolation-pool")
                .label("storage", "true")
                .segment_name(&segment)
                .transport(client_transport)
                .local_memory(default_isolation_storage_config())
                .build(test_future_expiry_ms())
                .expect("isolation client build should succeed");
            client
                .register_local_memory()
                .expect("isolation local-memory registration should succeed");
            clients.push(Arc::new(client));
        }
        let cluster = Self {
            metadata,
            hub,
            clients,
        };
        cluster.wait_for_convergence();
        cluster
    }

    /// Convenience: borrow the i-th client.
    #[allow(dead_code)]
    pub fn client(&self, i: usize) -> &StoreClient {
        &self.clients[i]
    }

    /// Wait until every client can see every other client's runtime lease via
    /// the metadata backend.  Mirrors the private `wait_for_membership_convergence`
    /// helper in `mod.rs`.
    fn wait_for_convergence(&self) {
        let runtimes: Vec<_> = self
            .clients
            .iter()
            .map(|c| c.runtime_id().clone())
            .collect();
        let deadline = Instant::now() + Duration::from_secs(2);
        for client in &self.clients {
            for runtime in &runtimes {
                if runtime == client.runtime_id() {
                    continue;
                }
                while Instant::now() < deadline {
                    if client.lookup_runtime_lease(runtime).is_ok() {
                        break;
                    }
                    sleep(Duration::from_millis(5));
                }
                if client.lookup_runtime_lease(runtime).is_err() {
                    panic!(
                        "isolation cluster failed to converge: client {} cannot see runtime {}",
                        client.runtime_id(),
                        runtime
                    );
                }
            }
        }
    }
}

/// 64 KiB storage / 4 KiB scratch / alignment 1 / numa-disabled.  Large
/// enough for hundreds of small puts; small enough to keep the test fast.
#[allow(dead_code)]
pub(super) fn default_isolation_storage_config() -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(64 * 1024)
        .scratch_bytes(4 * 1024)
        .alignment(1)
        .reclaim_grace_ms(0)
}

/// Install an admin-managed tenant policy with the given quota cap.  Use
/// `max_bytes`/`max_objects = None` to leave that dimension unbounded.
#[allow(dead_code)]
pub(super) fn install_tenant_quota_policy(
    metadata: &Arc<InMemoryMetadataBackend>,
    tenant: &str,
    max_bytes: Option<u64>,
    max_objects: Option<usize>,
) {
    let policy = TenantPolicy {
        scope: TenantPolicyScope {
            tenant: tenant.to_string(),
            domain: None,
            object_set: None,
        },
        spec: TenantPolicySpec {
            quota: Some(TenantQuotaPolicy {
                max_bytes,
                max_objects,
            }),
            ..Default::default()
        },
        version: 0,
        updated_at_ms: 0,
        updated_by: "isolation-test".to_string(),
    };
    metadata
        .put_tenant_policy(&policy, None)
        .expect("install tenant quota policy should succeed");
}

/// Convenience: install a generous quota (`1 GiB`, `1 000 000` objects) so
/// strict-quota path is exercised but never triggers admit-deny in tests
/// that do not specifically target quota enforcement.
#[allow(dead_code)]
pub(super) fn install_generous_quota_policy(
    metadata: &Arc<InMemoryMetadataBackend>,
    tenant: &str,
) {
    install_tenant_quota_policy(metadata, tenant, Some(1u64 << 30), Some(1_000_000usize));
}
