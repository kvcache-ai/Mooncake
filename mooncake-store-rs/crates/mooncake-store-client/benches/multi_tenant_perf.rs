// ---------------------------------------------------------------------------
// multi_tenant_perf.rs — Criterion micro-benches comparing the client hot path
// with the multi-tenant feature stack disabled vs enabled.
//
// Baseline groups (multi-tenant features minimised):
//   * single tenant, default scope, no admin tenant policy installed
//
// Comparison groups (multi-tenant features exercised):
//   * single tenant, non-default (domain, object_set) scope
//   * many tenants, default scope, admin tenant policies installed (hot cache)
//   * many tenants, default scope, mixed batch_put across tenants
//   * strict-quota path with admin TenantPolicy installed (reserve/finalize)
//
// All benches run inside a single process with `InMemoryMetadataBackend` and
// `TestTransport`, so absolute numbers are meaningful only as relative
// regression signal between groups, not as production latency numbers.
// ---------------------------------------------------------------------------

use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use mooncake_store_client::{
    AllocationLifecycleConfig, LocalMemoryConfig, MooncakeCompatibilityFacade, PlacementPlanner,
    PutRequest, ReplicationPolicy, StoreClient, StoreClientBuilder,
};
use mooncake_store_core::{
    ClientLifecycleState, CompatibilityDescriptor, MetadataBackend, TenantPolicy,
    TenantPolicyScope, TenantPolicySpec, TenantQuotaPolicy, ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY,
};
use mooncake_store_rs_metadata::InMemoryMetadataBackend;
use mooncake_store_test_utils::fixtures::test_future_expiry_ms;
use mooncake_store_test_utils::transport::TestTransport;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn bench_storage_config() -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(64 * 1024)
        .scratch_bytes(4 * 1024)
        .alignment(1)
        .reclaim_grace_ms(0)
}

fn remote_bench_storage_config() -> LocalMemoryConfig {
    LocalMemoryConfig::new()
        .numa_aware(false)
        .storage_bytes(16 * 1024 * 1024)
        .scratch_bytes(4 * 1024)
        .alignment(1)
        .reclaim_grace_ms(0)
}

fn build_active_client(
    metadata: Arc<dyn MetadataBackend>,
    transport: Arc<TestTransport>,
    stable_id: &str,
    tenant: &str,
) -> StoreClient {
    build_active_client_with_lifecycle(
        metadata,
        transport,
        stable_id,
        tenant,
        true,
        AllocationLifecycleConfig::default(),
    )
}

fn build_active_client_with_lifecycle(
    metadata: Arc<dyn MetadataBackend>,
    transport: Arc<TestTransport>,
    stable_id: &str,
    tenant: &str,
    lifecycle_enabled: bool,
    lifecycle: AllocationLifecycleConfig,
) -> StoreClient {
    let mut compatibility = CompatibilityDescriptor::mooncake_v1();
    if !lifecycle_enabled {
        compatibility
            .capabilities
            .remove(ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY);
    }
    let client = StoreClientBuilder::new(metadata, stable_id)
        .state(ClientLifecycleState::Active)
        .tenant(tenant)
        .label("pool", "bench-pool")
        .label("storage", "true")
        .segment_name(format!("{stable_id}-segment"))
        .transport(transport)
        .local_memory(bench_storage_config())
        .compatibility(compatibility)
        .allocation_lifecycle(lifecycle)
        .build(test_future_expiry_ms())
        .expect("bench client build should succeed");
    client
        .register_local_memory()
        .expect("bench local memory registration should succeed");
    client
}

fn build_remote_lifecycle_pair(lifecycle_enabled: bool) -> (StoreClient, StoreClient) {
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    let owner_transport = Arc::new(TestTransport::new("allocation-lifecycle-remote-owner"));
    let writer_transport = Arc::new(owner_transport.peer("allocation-lifecycle-remote-writer"));
    let mut owner_compatibility = CompatibilityDescriptor::mooncake_v1();
    if !lifecycle_enabled {
        owner_compatibility
            .capabilities
            .remove(ALLOCATION_LIFECYCLE_CLOCK_CAPABILITY);
    }
    let owner = StoreClientBuilder::new(
        metadata.clone() as Arc<dyn MetadataBackend>,
        "allocation-lifecycle-remote-owner",
    )
    .state(ClientLifecycleState::Active)
    .tenant("tenant-allocation-lifecycle-remote")
    .label("pool", "bench-pool")
    .label("storage", "true")
    .segment_name("allocation-lifecycle-remote-owner-segment")
    .transport(owner_transport)
    .local_memory(remote_bench_storage_config())
    .compatibility(owner_compatibility)
    .build(test_future_expiry_ms())
    .expect("remote lifecycle owner should build");
    owner
        .register_local_memory()
        .expect("remote lifecycle owner should register memory");

    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let writer = StoreClientBuilder::new(
        metadata as Arc<dyn MetadataBackend>,
        "allocation-lifecycle-remote-writer",
    )
    .state(ClientLifecycleState::Active)
    .tenant("tenant-allocation-lifecycle-remote")
    .label("pool", "bench-pool")
    .label("storage", "false")
    .segment_name("allocation-lifecycle-remote-writer-segment")
    .transport(writer_transport)
    .local_memory(remote_bench_storage_config())
    .routed_writes(planner, 1)
    .build(test_future_expiry_ms())
    .expect("remote lifecycle writer should build");
    writer
        .register_local_memory()
        .expect("remote lifecycle writer should register memory");
    (owner, writer)
}

fn install_generous_quota_policy(metadata: &Arc<InMemoryMetadataBackend>, tenant: &str) {
    let policy = TenantPolicy {
        scope: TenantPolicyScope {
            tenant: tenant.to_string(),
            domain: None,
            object_set: None,
        },
        spec: TenantPolicySpec {
            quota: Some(TenantQuotaPolicy {
                max_bytes: Some(1 << 30),
                max_objects: Some(1_000_000),
            }),
            ..Default::default()
        },
        version: 0,
        updated_at_ms: 0,
        updated_by: "bench".to_string(),
    };
    metadata
        .put_tenant_policy(&policy, None)
        .expect("install generous quota policy should succeed");
}

fn warm_up_client(client: &StoreClient, tenant: &str) {
    client
        .put_in_tenant(tenant, "__bench_warmup__", b"warm")
        .expect("warmup put should succeed");
    client
        .remove_in_tenant(tenant, "__bench_warmup__", true)
        .expect("warmup cleanup should succeed");
}

fn small_payload(size: usize) -> Vec<u8> {
    vec![0xABu8; size]
}

// ---------------------------------------------------------------------------
// Bench groups
// ---------------------------------------------------------------------------

fn bench_put_baseline_single_tenant_default_scope(c: &mut Criterion) {
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("baseline-single-tenant"));
    let client = build_active_client(
        metadata.clone() as Arc<dyn MetadataBackend>,
        transport,
        "baseline-single-tenant",
        "tenant-baseline",
    );
    warm_up_client(&client, "tenant-baseline");

    let value = small_payload(64);
    let mut group = c.benchmark_group("put_baseline_single_tenant_default_scope");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    let mut counter = 0u64;
    group.bench_function("put_64B", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("k-{counter}");
            client
                .put_in_tenant("tenant-baseline", &key, &value)
                .expect("baseline put should succeed");
        });
    });
    group.finish();
}

// Sequence measurement recipes and smoke checks only: TestTransport does not
// measure TENT network latency. Each iteration creates fresh metadata, clients,
// and allocator, then runs the same warmup sequence in both capability modes.
// Warmup readback/removal does not prove asynchronous publication finalization
// has drained: that work may overlap the timed put. These groups therefore do
// not establish isolated put cost or qualify as independent strategy comparisons.
// Preflight and warmup assertions run outside timing. Both arms time put,
// Result::expect, black_box, and route destruction; fixture teardown is excluded.
// Any future comparison requires observable finalize completion plus source,
// lockfile, executable hashes, full argv, toolchain, config, and window records.
fn verify_local_lifecycle_admission() {
    for enabled in [false, true] {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let transport = Arc::new(TestTransport::new("allocation-lifecycle-admission"));
        let client = build_active_client_with_lifecycle(
            metadata,
            transport,
            "allocation-lifecycle-admission",
            "tenant-allocation-lifecycle",
            enabled,
            AllocationLifecycleConfig::default().max_pending_publication_bytes(63),
        );
        let result = client.put_in_tenant(
            "tenant-allocation-lifecycle",
            "admission-probe",
            &[0xAB; 64],
        );
        if enabled {
            let error = result.expect_err("lifecycle put must enforce pending publication bytes");
            assert!(error.to_string().contains("pending publication"), "{error}");
        } else {
            result.expect("legacy put must bypass pending publication byte admission");
        }
    }
}

fn bench_allocation_lifecycle_single_put(c: &mut Criterion) {
    verify_local_lifecycle_admission();
    let value = small_payload(64);
    let mut group = c.benchmark_group("put_allocation_lifecycle");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);
    for (name, enabled) in [("legacy", false), ("lifecycle", true)] {
        group.bench_function(BenchmarkId::new("put_64B", name), |b| {
            b.iter_batched_ref(
                || {
                    let metadata = Arc::new(InMemoryMetadataBackend::new());
                    let transport = Arc::new(TestTransport::new("allocation-lifecycle"));
                    let client = build_active_client_with_lifecycle(
                        metadata,
                        transport,
                        "allocation-lifecycle",
                        "tenant-allocation-lifecycle",
                        enabled,
                        AllocationLifecycleConfig::default(),
                    );
                    warm_up_client(&client, "tenant-allocation-lifecycle");
                    client
                },
                |client| {
                    let route = client
                        .put_in_tenant("tenant-allocation-lifecycle", "measured-key", &value)
                        .expect("allocation lifecycle put should succeed");
                    criterion::black_box(route);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_allocation_lifecycle_remote_put(c: &mut Criterion) {
    let value = small_payload(64);
    let policy = ReplicationPolicy::new().prefer_local(false);
    let mut group = c.benchmark_group("put_allocation_lifecycle_remote");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);
    for (name, enabled) in [("legacy", false), ("lifecycle", true)] {
        group.bench_function(BenchmarkId::new("put_64B", name), |b| {
            b.iter_batched_ref(
                || {
                    let pair = build_remote_lifecycle_pair(enabled);
                    let route = pair
                        .1
                        .put_with_policy("__bench_remote_warmup__", b"warm", &policy)
                        .expect("remote lifecycle warmup should succeed");
                    assert_eq!(route.replicas[0].owner, *pair.0.runtime_id());
                    assert_eq!(pair.1.get("__bench_remote_warmup__").unwrap(), b"warm");
                    pair
                },
                |(_owner, client)| {
                    let route = client
                        .put_with_policy("measured-key", &value, &policy)
                        .expect("remote allocation lifecycle put should succeed");
                    criterion::black_box(route);
                },
                criterion::BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn bench_put_single_tenant_non_default_scope(c: &mut Criterion) {
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("scope-single-tenant"));
    let client = build_active_client(
        metadata.clone() as Arc<dyn MetadataBackend>,
        transport,
        "scope-single-tenant",
        "tenant-scope",
    );
    warm_up_client(&client, "tenant-scope");

    let value = small_payload(64);
    let mut group = c.benchmark_group("put_single_tenant_non_default_scope");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    let mut counter = 0u64;
    group.bench_function("put_64B", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("k-{counter}");
            let req = PutRequest::new(&key, &value)
                .tenant("tenant-scope")
                .domain("d1")
                .object_set("s1");
            client
                .batch_put(std::slice::from_ref(&req))
                .expect("non-default-scope put should succeed");
        });
    });
    group.finish();
}

fn bench_put_multi_tenant_strict_quota_hot_cache(c: &mut Criterion) {
    const TENANTS: usize = 16;
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    for i in 0..TENANTS {
        install_generous_quota_policy(&metadata, &format!("tenant-{i}"));
    }
    let transport = Arc::new(TestTransport::new("multi-tenant-quota"));
    let client = build_active_client(
        metadata.clone() as Arc<dyn MetadataBackend>,
        transport,
        "multi-tenant-quota",
        "tenant-0",
    );
    for i in 0..TENANTS {
        warm_up_client(&client, &format!("tenant-{i}"));
    }

    let value = small_payload(64);
    let mut group = c.benchmark_group("put_multi_tenant_strict_quota_hot_cache");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    let mut counter = 0u64;
    group.bench_function(BenchmarkId::new("put_64B_round_robin", TENANTS), |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let tenant_idx = (counter as usize) % TENANTS;
            let tenant = format!("tenant-{tenant_idx}");
            let key = format!("k-{counter}");
            client
                .put_in_tenant(&tenant, &key, &value)
                .expect("multi-tenant put should succeed");
        });
    });
    group.finish();
}

fn bench_batch_put_multi_tenant(c: &mut Criterion) {
    const TENANTS: usize = 8;
    const BATCH: usize = 32;
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    for i in 0..TENANTS {
        install_generous_quota_policy(&metadata, &format!("tenant-{i}"));
    }
    let transport = Arc::new(TestTransport::new("multi-tenant-batch"));
    let client = build_active_client(
        metadata.clone() as Arc<dyn MetadataBackend>,
        transport,
        "multi-tenant-batch",
        "tenant-0",
    );
    for i in 0..TENANTS {
        warm_up_client(&client, &format!("tenant-{i}"));
    }

    let value = small_payload(64);
    let mut group = c.benchmark_group("batch_put_multi_tenant");
    group.throughput(Throughput::Bytes((BATCH * value.len()) as u64));
    group.measurement_time(Duration::from_secs(4));
    group.sample_size(30);

    let mut counter = 0u64;
    group.bench_function(BenchmarkId::new("batch_32_x_64B", TENANTS), |b| {
        b.iter_batched(
            || {
                counter = counter.wrapping_add(1);
                let base = counter * BATCH as u64;
                let tenants: Vec<String> = (0..BATCH)
                    .map(|i| format!("tenant-{}", i % TENANTS))
                    .collect();
                let keys: Vec<String> = (0..BATCH)
                    .map(|i| format!("k-{}", base + i as u64))
                    .collect();
                (tenants, keys)
            },
            |(tenants, keys)| {
                let reqs: Vec<PutRequest<'_>> = keys
                    .iter()
                    .zip(tenants.iter())
                    .map(|(k, t)| PutRequest::new(k.as_str(), &value).tenant(t.as_str()))
                    .collect();
                client
                    .batch_put(&reqs)
                    .expect("multi-tenant batch_put should succeed");
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_get_baseline_default_scope(c: &mut Criterion) {
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    let transport = Arc::new(TestTransport::new("get-baseline"));
    let client = build_active_client(
        metadata.clone() as Arc<dyn MetadataBackend>,
        transport,
        "get-baseline",
        "tenant-get-baseline",
    );
    warm_up_client(&client, "tenant-get-baseline");

    let value = small_payload(64);
    client
        .put_in_tenant("tenant-get-baseline", "fixed-key", &value)
        .expect("seed put should succeed");

    let mut group = c.benchmark_group("get_baseline_default_scope");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    group.bench_function("get_64B", |b| {
        b.iter(|| {
            let _v = client
                .get_in_tenant("tenant-get-baseline", "fixed-key")
                .expect("baseline get should succeed");
        });
    });
    group.finish();
}

fn bench_get_multi_tenant_with_policy(c: &mut Criterion) {
    let metadata: Arc<InMemoryMetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
    install_generous_quota_policy(&metadata, "tenant-get-mt");
    let transport = Arc::new(TestTransport::new("get-multi-tenant"));
    let client = build_active_client(
        metadata.clone() as Arc<dyn MetadataBackend>,
        transport,
        "get-multi-tenant",
        "tenant-get-mt",
    );
    warm_up_client(&client, "tenant-get-mt");

    let value = small_payload(64);
    client
        .put_in_tenant("tenant-get-mt", "fixed-key", &value)
        .expect("seed put should succeed");

    let mut group = c.benchmark_group("get_multi_tenant_with_policy");
    group.throughput(Throughput::Bytes(value.len() as u64));
    group.measurement_time(Duration::from_secs(3));
    group.sample_size(50);

    group.bench_function("get_64B", |b| {
        b.iter(|| {
            let _v = client
                .get_in_tenant("tenant-get-mt", "fixed-key")
                .expect("multi-tenant get should succeed");
        });
    });
    group.finish();
}

criterion_group!(
    multi_tenant_perf,
    bench_put_baseline_single_tenant_default_scope,
    bench_allocation_lifecycle_single_put,
    bench_allocation_lifecycle_remote_put,
    bench_put_single_tenant_non_default_scope,
    bench_put_multi_tenant_strict_quota_hot_cache,
    bench_batch_put_multi_tenant,
    bench_get_baseline_default_scope,
    bench_get_multi_tenant_with_policy,
);
criterion_main!(multi_tenant_perf);
