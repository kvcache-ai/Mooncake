use std::collections::BTreeSet;
use std::env;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{
    init_tracing_from_env, render_prometheus_metrics, start_metrics_http_server_from_env,
    BandwidthShaping, GetRequest, LocalMemoryConfig, MooncakeCompatibilityFacade,
    MultiBufferGetRequest, MultiBufferPutRequest, ObjectRef, PlacementPlanner, PutFromRequest,
    PutRequest, ReplicationPolicy, StoreClient, StoreClientBuilder, TentTransportFactory,
};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, HandoffKind, MetadataBackend,
    ObjectKey, Result, SegmentLifecycleState, SegmentName, StoreError, TenantObjectAccountingState,
    TenantPolicy, TenantPolicyScope, TenantPolicySpec, TenantQuotaPolicy,
    TenantQuotaReservationState,
};
use mooncake_transport::{TentEngine, TentEngineConfig};

const LEASE_MS: u64 = 600_000;
const MEMORY_BYTES: usize = 128 * 1024 * 1024;
const SCRATCH_BYTES: usize = 16 * 1024 * 1024;
const BENCH_HEARTBEAT_EVERY: usize = 32;
const DEFAULT_BENCH_ITERS: usize = 64;

#[derive(Clone, Debug)]
struct RdmaMode {
    requested: bool,
    supported: bool,
    enabled: bool,
    reason: &'static str,
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    init_tracing_from_env("MC_STORE_RS_TRACE", "MC_STORE_RS_TRACE_FILTER")?;
    if let Some(address) = start_metrics_http_server_from_env("MC_STORE_RS_METRICS_ADDR")? {
        println!("metrics http server: http://{address}/metrics");
    }
    let redis_url = env::var("MC_STORE_RS_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6380/0".to_string());
    let redis_port = env::var("MC_STORE_RS_REDIS_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(6380);
    let value_size = env::var("MC_STORE_RS_VALUE_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4096);
    let batch_bench_iters = env::var("MC_STORE_RS_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BENCH_ITERS);
    let rdma_mode = detect_rdma_mode();
    println!(
        "rdma mode: requested={} supported={} enabled={} reason={}",
        rdma_mode.requested, rdma_mode.supported, rdma_mode.enabled, rdma_mode.reason
    );
    let keyspace = MetadataKeyspace::new(format!("mc/store-rs/e2e/{}", unique_suffix()));
    let metadata = Arc::new(RedisMetadataBackend::new(
        RedisMetadataConfig::new(redis_url).keyspace(keyspace),
    )?);
    let planner = PlacementPlanner::new(metadata.clone()).require_label("storage", "true");
    let memory = LocalMemoryConfig::new()
        .storage_bytes(MEMORY_BYTES)
        .scratch_bytes(SCRATCH_BYTES)
        .location("cpu:0")
        .tags(vec![
            "dram".to_string(),
            "multi-tenant".to_string(),
            "dynamic-membership".to_string(),
        ]);

    let target_a_bundle = build_tent_bundle(redis_port, "target-a-segment", rdma_mode.enabled)?;
    let target_b_bundle = build_tent_bundle(redis_port, "target-b-segment", rdma_mode.enabled)?;
    let target_c_bundle = build_tent_bundle(redis_port, "target-c-segment", rdma_mode.enabled)?;
    let upgrade_bundle =
        build_tent_bundle(redis_port, "target-a-upgrade-segment", rdma_mode.enabled)?;
    let reclaim_bundle =
        build_tent_bundle(redis_port, "target-reclaim-segment", rdma_mode.enabled)?;
    let router_bundle = build_tent_bundle(redis_port, "router-segment", rdma_mode.enabled)?;
    let router_replica_bundle =
        build_tent_bundle(redis_port, "router-replica-segment", rdma_mode.enabled)?;
    let reader_bundle = build_tent_bundle(redis_port, "reader-segment", rdma_mode.enabled)?;
    let elastic_target_bundle =
        build_tent_bundle(redis_port, "elastic-target-segment", rdma_mode.enabled)?;
    let elastic_router_bundle =
        build_tent_bundle(redis_port, "elastic-router-segment", rdma_mode.enabled)?;

    let mut target_a = build_client(
        metadata.clone(),
        "store-a",
        ClientLifecycleState::Active,
        target_a_bundle,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "primary"), ("storage", "true")],
    )?;
    let mut target_b = build_client(
        metadata.clone(),
        "store-b",
        ClientLifecycleState::Active,
        target_b_bundle,
        memory.clone(),
        "tenant-a",
        &[
            ("pool", "pool-a"),
            ("role", "scaleout"),
            ("storage", "true"),
        ],
    )?;
    let mut target_c = build_client(
        metadata.clone(),
        "store-c",
        ClientLifecycleState::Active,
        target_c_bundle,
        memory.clone(),
        "tenant-a",
        &[
            ("pool", "pool-a"),
            ("role", "post-upgrade-scaleout"),
            ("storage", "true"),
        ],
    )?;
    let mut target_upgrade = build_client(
        metadata.clone(),
        "store-a",
        ClientLifecycleState::Standby,
        upgrade_bundle,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "upgrade"), ("storage", "true")],
    )?;
    let mut reclaim_writer = build_client(
        metadata.clone(),
        "store-reclaim",
        ClientLifecycleState::Active,
        reclaim_bundle,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 2)
            .scratch_bytes(value_size * 2)
            .location("cpu:0")
            .reclaim_grace_ms(0)
            .tags(vec!["dram".to_string(), "overwrite-reclaim".to_string()]),
        "tenant-a",
        &[
            ("pool", "pool-reclaim"),
            ("role", "reclaim"),
            ("storage", "true"),
        ],
    )?;
    let mut router = build_routed_client(
        metadata.clone(),
        "router",
        ClientLifecycleState::Active,
        router_bundle,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 8)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .tags(vec!["dram".to_string(), "router".to_string()]),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "router"), ("storage", "false")],
        planner.clone(),
        1,
        None,
    )?;
    let mut router_replica = build_routed_client(
        metadata.clone(),
        "router-replica",
        ClientLifecycleState::Active,
        router_replica_bundle,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 8)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .tags(vec![
                "dram".to_string(),
                "router".to_string(),
                "replicated".to_string(),
            ]),
        "tenant-a",
        &[
            ("pool", "pool-a"),
            ("role", "router-replica"),
            ("storage", "false"),
        ],
        planner.clone(),
        2,
        None,
    )?;
    let mut reader = build_client(
        metadata.clone(),
        "reader",
        ClientLifecycleState::Active,
        reader_bundle,
        memory,
        "tenant-a",
        &[("pool", "pool-a"), ("role", "reader"), ("storage", "false")],
    )?;
    let mut elastic_target = build_client(
        metadata.clone(),
        "elastic-target",
        ClientLifecycleState::Active,
        elastic_target_bundle,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 2)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .reclaim_grace_ms(0)
            .tags(vec![
                "dram".to_string(),
                "elastic".to_string(),
                "storage".to_string(),
            ]),
        "tenant-a",
        &[
            ("pool", "pool-elastic"),
            ("role", "elastic-target"),
            ("storage", "true"),
        ],
    )?;
    let mut elastic_router = build_routed_client(
        metadata.clone(),
        "elastic-router",
        ClientLifecycleState::Active,
        elastic_router_bundle,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 4)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .reclaim_grace_ms(0)
            .tags(vec!["dram".to_string(), "elastic-router".to_string()]),
        "tenant-a",
        &[
            ("pool", "pool-elastic"),
            ("role", "elastic-router"),
            ("storage", "false"),
        ],
        PlacementPlanner::new(metadata.clone()).require_label("storage", "true"),
        1,
        None,
    )?;
    target_a.register_local_memory()?;
    target_b.register_local_memory()?;
    target_upgrade.register_local_memory()?;
    reclaim_writer.register_local_memory()?;
    router.register_local_memory()?;
    router_replica.register_local_memory()?;
    reader.register_local_memory()?;
    elastic_target.register_local_memory()?;
    elastic_router.register_local_memory()?;

    verify_single_put_get(&target_a, &reader, value_size)?;
    verify_multi_tenant_isolation(&target_a, &reader, value_size)?;
    verify_strict_tenant_quota(
        metadata.clone(),
        redis_port,
        &reader,
        value_size,
        rdma_mode.enabled,
    )?;
    verify_batch_put_get(&target_a, &reader, value_size)?;
    verify_batch_put_from_get_into(&target_a, &reader, value_size)?;
    verify_multi_buffer_batch_ops(&target_a, &reader, value_size)?;
    verify_overwrite_reclaims_capacity(&reclaim_writer, &reader, value_size)?;
    verify_request_replication_policy(&target_a, &target_b, &reader, value_size)?;
    verify_delete_reclaim_parity(&reclaim_writer, value_size)?;
    verify_embedded_route_directory_offloads_metadata(
        metadata.as_ref(),
        &router,
        &reader,
        value_size,
    )?;
    verify_routed_scale_out(&router, &reader, value_size)?;
    verify_multi_replica_route_publish(&router_replica, &reader, value_size)?;
    verify_dynamic_expand_and_soft_shrink(&elastic_target, &elastic_router, &reader, value_size)?;
    verify_hot_upgrade(
        &mut target_a,
        &mut target_upgrade,
        &router_replica,
        &reader,
        value_size,
    )?;
    verify_rdma_bandwidth_isolation(
        metadata.clone(),
        planner.clone(),
        redis_port,
        &reader,
        value_size,
        &rdma_mode,
    )?;

    heartbeat_clients(&mut [
        &mut target_a,
        &mut target_b,
        &mut target_c,
        &mut target_upgrade,
        &mut reclaim_writer,
        &mut router,
        &mut router_replica,
        &mut reader,
        &mut elastic_target,
        &mut elastic_router,
    ])?;
    run_batch_put_benchmark(
        &mut router,
        value_size,
        batch_bench_iters,
        &mut [
            &mut target_a,
            &mut target_b,
            &mut target_c,
            &mut target_upgrade,
            &mut reclaim_writer,
            &mut router_replica,
            &mut reader,
            &mut elastic_target,
            &mut elastic_router,
        ],
    )?;
    run_batch_get_benchmark(
        &mut target_upgrade,
        &mut reader,
        value_size,
        batch_bench_iters,
        &mut [
            &mut target_a,
            &mut target_b,
            &mut target_c,
            &mut reclaim_writer,
            &mut router,
            &mut router_replica,
            &mut elastic_target,
            &mut elastic_router,
        ],
    )?;
    sleep(Duration::from_secs(6));
    heartbeat_clients(&mut [
        &mut target_a,
        &mut target_b,
        &mut target_c,
        &mut target_upgrade,
        &mut reclaim_writer,
        &mut router,
        &mut router_replica,
        &mut reader,
        &mut elastic_target,
        &mut elastic_router,
    ])?;
    target_a.activate()?;
    verify_true_client_shrink(&mut target_b, &router, &reader, value_size)?;

    println!(
        "e2e ok: single put/get, strict tenant quota, batch put/get, request-level replication policy, true delete reclaim, routed remote write, multi-replica publish, registered-buffer path, overwrite reclaim, multi-tenant, scale-out, elastic expand-shrink, true client shrink, hot-upgrade, rdma bandwidth isolation"
    );
    if env::var("MC_STORE_RS_PRINT_METRICS")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
    {
        println!("{}", render_prometheus_metrics());
    }
    Ok(())
}

struct TentBundle {
    engine: Arc<TentEngine>,
    factory: Arc<TentTransportFactory>,
}

fn tent_base_config(redis_port: u16, rdma_enable: bool) -> TentEngineConfig {
    let config = TentEngineConfig::new()
        .set("metadata_type", "redis")
        .set("metadata_servers", format!("127.0.0.1:{redis_port}"))
        .set("redis_db_index", "0")
        .set("rpc_server_hostname", "127.0.0.1")
        .set("rpc_server_port", "0")
        .set("log_level", "warning")
        .set(
            "transports/tcp/enable",
            if rdma_enable { "false" } else { "true" },
        )
        .set("transports/shm/enable", "false")
        .set(
            "transports/rdma/enable",
            if rdma_enable { "true" } else { "false" },
        )
        .set("transports/io_uring/enable", "false");
    if rdma_enable {
        let rdma_devices = env::var("MC_STORE_RS_RDMA_DEVICES").unwrap_or_default();
        if !rdma_devices.trim().is_empty() {
            return config.set("devices", rdma_devices);
        }
    }
    config
}

#[allow(clippy::arc_with_non_send_sync)]
fn build_tent_bundle(
    redis_port: u16,
    local_segment_name: &str,
    rdma_enable: bool,
) -> Result<TentBundle> {
    let config = tent_base_config(redis_port, rdma_enable);
    let engine = Arc::new(TentEngine::new(
        &config.clone().set("local_segment_name", local_segment_name),
    )?);
    Ok(TentBundle {
        engine,
        factory: Arc::new(TentTransportFactory::new(config)),
    })
}

#[allow(clippy::too_many_arguments)]
fn build_client(
    metadata: Arc<RedisMetadataBackend>,
    stable_id: &str,
    state: ClientLifecycleState,
    transport: TentBundle,
    memory: LocalMemoryConfig,
    tenant: &str,
    labels: &[(&str, &str)],
) -> Result<StoreClient> {
    let mut builder = StoreClientBuilder::new(metadata, stable_id)
        .state(state)
        .activate_on_local_memory_registration()
        .tenant(tenant)
        .compatibility(CompatibilityDescriptor::default())
        .local_memory(memory)
        .with_tent(transport.engine)
        .transport_factory(transport.factory);
    for (key, value) in labels {
        builder = builder.label(*key, *value);
    }
    builder.build(now_ms() + LEASE_MS)
}

#[allow(clippy::too_many_arguments)]
fn build_routed_client(
    metadata: Arc<RedisMetadataBackend>,
    stable_id: &str,
    state: ClientLifecycleState,
    transport: TentBundle,
    memory: LocalMemoryConfig,
    tenant: &str,
    labels: &[(&str, &str)],
    planner: PlacementPlanner,
    replica_count: usize,
    bandwidth_shaping: Option<BandwidthShaping>,
) -> Result<StoreClient> {
    let mut builder = StoreClientBuilder::new(metadata, stable_id)
        .state(state)
        .activate_on_local_memory_registration()
        .tenant(tenant)
        .compatibility(CompatibilityDescriptor::default())
        .local_memory(memory)
        .with_tent(transport.engine)
        .transport_factory(transport.factory)
        .routed_writes(planner, replica_count);
    if let Some(shaping) = bandwidth_shaping {
        builder = builder.bandwidth_shaping(shaping);
    }
    for (key, value) in labels {
        builder = builder.label(*key, *value);
    }
    builder.build(now_ms() + LEASE_MS)
}

fn verify_single_put_get(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let value = payload("single-put-get", value_size);
    writer.put("single-key", &value)?;

    let round_trip = reader.get("single-key")?;
    ensure_payload("single get", &value, &round_trip)?;

    let mut buffer = vec![0u8; value.len()];
    let size = reader.get_into("single-key", &mut buffer)?;
    if size != value.len() {
        return Err(StoreError::Transport(format!(
            "single get_into size mismatch: got={size} expected={}",
            value.len()
        )));
    }
    ensure_payload("single get_into", &value, &buffer)?;
    Ok(())
}

fn verify_multi_tenant_isolation(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let tenant_a = payload("tenant-a-value", value_size);
    let tenant_b = payload("tenant-b-value", value_size);
    writer.put_in_tenant("tenant-a", "shared-key", &tenant_a)?;
    writer.put_in_tenant("tenant-b", "shared-key", &tenant_b)?;

    let keys = [
        ObjectRef::new("shared-key").tenant("tenant-a"),
        ObjectRef::new("shared-key").tenant("tenant-b"),
    ];
    let results = reader.batch_get(&keys)?;
    ensure_payload("tenant-a isolation", &tenant_a, &results[0])?;
    ensure_payload("tenant-b isolation", &tenant_b, &results[1])?;
    Ok(())
}

fn put_tenant_quota_policy(
    metadata: &RedisMetadataBackend,
    tenant: &str,
    max_bytes: u64,
    max_objects: usize,
) -> Result<()> {
    metadata.put_tenant_policy(
        &TenantPolicy {
            scope: TenantPolicyScope::new(tenant, None::<String>, None::<String>),
            spec: TenantPolicySpec {
                quota: Some(TenantQuotaPolicy {
                    max_bytes: Some(max_bytes),
                    max_objects: Some(max_objects),
                }),
                ..TenantPolicySpec::default()
            },
            version: 1,
            updated_at_ms: now_ms(),
            updated_by: "e2e".to_string(),
        },
        None,
    )?;
    Ok(())
}

fn verify_strict_tenant_quota(
    metadata: Arc<RedisMetadataBackend>,
    redis_port: u16,
    reader: &StoreClient,
    value_size: usize,
    rdma_enabled: bool,
) -> Result<()> {
    let tenant = "tenant-quota-e2e";
    let key_ok = "quota-ok";
    let key_over = "quota-over";
    let key_reuse = "quota-reuse";
    put_tenant_quota_policy(metadata.as_ref(), tenant, value_size as u64, 1)?;

    let bundle = build_tent_bundle(redis_port, "quota-e2e-segment", rdma_enabled)?;
    let writer = build_client(
        metadata.clone(),
        "quota-e2e-writer",
        ClientLifecycleState::Active,
        bundle,
        LocalMemoryConfig::new()
            .storage_bytes(MEMORY_BYTES)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .tags(vec!["dram".to_string(), "quota-e2e".to_string()]),
        tenant,
        &[
            ("pool", "pool-quota"),
            ("role", "quota-e2e"),
            ("storage", "true"),
        ],
    )?;
    writer.register_local_memory()?;

    let first = payload("strict-quota-ok", value_size);
    writer.put(key_ok, &first)?;
    let round_trip = reader.get_in_tenant(tenant, key_ok)?;
    ensure_payload("strict quota first put", &first, &round_trip)?;

    let scope = TenantPolicyScope::new(tenant, None::<String>, None::<String>);
    let quota_after_put = metadata
        .get_tenant_quota_state(&scope)?
        .ok_or_else(|| StoreError::NotFound(format!("quota state missing for {tenant}")))?;
    if quota_after_put.used_bytes != value_size as u64
        || quota_after_put.used_objects != 1
        || quota_after_put.pending_reserved_bytes != 0
        || quota_after_put.pending_reserved_objects != 0
    {
        return Err(StoreError::InvalidState(format!(
            "strict quota state after put mismatch: used_bytes={} used_objects={} pending_bytes={} pending_objects={} expected_used_bytes={} expected_used_objects=1",
            quota_after_put.used_bytes,
            quota_after_put.used_objects,
            quota_after_put.pending_reserved_bytes,
            quota_after_put.pending_reserved_objects,
            value_size,
        )));
    }

    let accounting_after_put = metadata
        .get_tenant_object_accounting(&ObjectKey::new(format!("{tenant}::{key_ok}")))?
        .ok_or_else(|| {
            StoreError::NotFound(format!("quota accounting missing for {tenant}::{key_ok}"))
        })?;
    if accounting_after_put.state != TenantObjectAccountingState::Active
        || accounting_after_put.committed_length != value_size as u64
    {
        return Err(StoreError::InvalidState(format!(
            "strict quota accounting after put mismatch: state={:?} committed_length={} expected_length={}",
            accounting_after_put.state,
            accounting_after_put.committed_length,
            value_size,
        )));
    }

    let reservations_after_put = metadata.list_tenant_quota_reservations(&scope)?;
    let first_reservation = reservations_after_put
        .iter()
        .find(|reservation| reservation.key == ObjectKey::new(format!("{tenant}::{key_ok}")))
        .ok_or_else(|| {
            StoreError::NotFound(format!(
                "strict quota reservation missing for {tenant}::{key_ok}"
            ))
        })?;
    if first_reservation.state != TenantQuotaReservationState::Finalized
        || first_reservation.delta_bytes != value_size as i64
        || first_reservation.delta_objects != 1
    {
        return Err(StoreError::InvalidState(format!(
            "strict quota finalized reservation mismatch: state={:?} delta_bytes={} delta_objects={} expected_bytes={} expected_objects=1",
            first_reservation.state,
            first_reservation.delta_bytes,
            first_reservation.delta_objects,
            value_size,
        )));
    }

    let second = payload("strict-quota-over", value_size);
    let over_error = writer
        .put(key_over, &second)
        .expect_err("strict quota over-limit put should fail");
    if !matches!(
        over_error,
        StoreError::Conflict(_) | StoreError::Metadata(_)
    ) || !over_error.to_string().contains("tenant quota")
    {
        return Err(StoreError::InvalidState(format!(
            "strict quota over-limit error mismatch: {over_error}"
        )));
    }

    let quota_after_reject = metadata.get_tenant_quota_state(&scope)?.ok_or_else(|| {
        StoreError::NotFound(format!("quota state missing for {tenant} after reject"))
    })?;
    if quota_after_reject != quota_after_put {
        return Err(StoreError::InvalidState(format!(
            "strict quota reject changed quota state: before={:?} after={:?}",
            quota_after_put, quota_after_reject
        )));
    }
    let reservations_after_reject = metadata.list_tenant_quota_reservations(&scope)?;
    if reservations_after_reject.len() != reservations_after_put.len() {
        return Err(StoreError::InvalidState(format!(
            "strict quota reject changed reservation count: before={} after={}",
            reservations_after_put.len(),
            reservations_after_reject.len()
        )));
    }

    writer.remove(key_ok, true)?;
    let quota_after_delete = metadata.get_tenant_quota_state(&scope)?.ok_or_else(|| {
        StoreError::NotFound(format!("quota state missing for {tenant} after delete"))
    })?;
    if quota_after_delete.used_bytes != 0
        || quota_after_delete.used_objects != 0
        || quota_after_delete.pending_reserved_bytes != 0
        || quota_after_delete.pending_reserved_objects != 0
    {
        return Err(StoreError::InvalidState(format!(
            "strict quota state after delete mismatch: used_bytes={} used_objects={} pending_bytes={} pending_objects={}",
            quota_after_delete.used_bytes,
            quota_after_delete.used_objects,
            quota_after_delete.pending_reserved_bytes,
            quota_after_delete.pending_reserved_objects,
        )));
    }
    if metadata
        .get_tenant_object_accounting(&ObjectKey::new(format!("{tenant}::{key_ok}")))?
        .is_some()
    {
        return Err(StoreError::InvalidState(
            "strict quota accounting still exists after authoritative delete".to_string(),
        ));
    }
    let reservations_after_delete = metadata.list_tenant_quota_reservations(&scope)?;
    let delete_reservation = reservations_after_delete
        .iter()
        .find(|reservation| {
            reservation.key == ObjectKey::new(format!("{tenant}::{key_ok}"))
                && reservation.delta_bytes == -(value_size as i64)
                && reservation.delta_objects == -1
        })
        .ok_or_else(|| {
            StoreError::NotFound(format!(
                "strict quota delete reservation missing for {tenant}::{key_ok}"
            ))
        })?;
    if delete_reservation.state != TenantQuotaReservationState::Finalized {
        return Err(StoreError::InvalidState(format!(
            "strict quota delete reservation was not finalized: state={:?}",
            delete_reservation.state
        )));
    }

    let third = payload("strict-quota-reuse", value_size);
    writer.put(key_reuse, &third)?;
    let reused = reader.get_in_tenant(tenant, key_reuse)?;
    ensure_payload("strict quota delete refund reuse", &third, &reused)?;
    Ok(())
}

fn verify_batch_put_get(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let items = build_items("tenant-a", "batch-local", 8, value_size);
    let puts = items
        .iter()
        .map(|item| {
            PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    writer.batch_put(&puts)?;

    let refs = items
        .iter()
        .map(|item| ObjectRef::new(item.key.as_str()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    let results = reader.batch_get(&refs)?;
    ensure_batch_payloads("batch_get", &items, &results)?;

    let mut buffers = items
        .iter()
        .map(|item| vec![0u8; item.value.len()])
        .collect::<Vec<_>>();
    let mut gets = items
        .iter()
        .zip(buffers.iter_mut())
        .map(|(item, buffer)| {
            GetRequest::new(item.key.as_str(), buffer.as_mut_slice()).tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    let sizes = reader.batch_get_into(&mut gets)?;
    for (size, item) in sizes.iter().zip(items.iter()) {
        if *size != item.value.len() {
            return Err(StoreError::Transport(format!(
                "batch_get_into size mismatch for {}: got={} expected={}",
                item.key,
                size,
                item.value.len()
            )));
        }
    }
    ensure_batch_payloads("batch_get_into", &items, &buffers)?;
    Ok(())
}

fn verify_batch_put_from_get_into(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let items = build_items("tenant-a", "batch-from", 8, value_size);
    let mut input_buffers = items
        .iter()
        .map(|item| item.value.clone())
        .collect::<Vec<_>>();
    for buffer in &mut input_buffers {
        writer.register_buffer(buffer.as_mut_ptr().cast(), buffer.len())?;
    }
    let put_result = {
        let puts = items
            .iter()
            .zip(input_buffers.iter())
            .map(|(item, buffer)| {
                PutFromRequest::new(item.key.as_str(), buffer.as_ptr().cast(), buffer.len())
                    .tenant(item.tenant.as_str())
            })
            .collect::<Vec<_>>();
        writer.batch_put_from(&puts)
    };
    for buffer in &mut input_buffers {
        writer.unregister_buffer(buffer.as_mut_ptr().cast(), buffer.len())?;
    }
    put_result?;

    let mut output_buffers = items
        .iter()
        .map(|item| vec![0u8; item.value.len()])
        .collect::<Vec<_>>();
    for buffer in &mut output_buffers {
        reader.register_buffer(buffer.as_mut_ptr().cast(), buffer.len())?;
    }
    let get_result = {
        let mut gets = items
            .iter()
            .zip(output_buffers.iter_mut())
            .map(|(item, buffer)| {
                GetRequest::new(item.key.as_str(), buffer.as_mut_slice())
                    .tenant(item.tenant.as_str())
            })
            .collect::<Vec<_>>();
        reader.batch_get_into(&mut gets)
    };
    for buffer in &mut output_buffers {
        reader.unregister_buffer(buffer.as_mut_ptr().cast(), buffer.len())?;
    }
    let sizes = get_result?;
    for (size, item) in sizes.iter().zip(items.iter()) {
        if *size != item.value.len() {
            return Err(StoreError::Transport(format!(
                "batch_put_from/get_into size mismatch for {}: got={} expected={}",
                item.key,
                size,
                item.value.len()
            )));
        }
    }
    ensure_batch_payloads("batch_put_from_get_into", &items, &output_buffers)?;
    Ok(())
}

fn verify_multi_buffer_batch_ops(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let items = build_items("tenant-a", "multi-buffer", 4, value_size);
    let split_payloads = items
        .iter()
        .map(|item| split_payload(&item.value))
        .collect::<Vec<_>>();
    let slice_sets = split_payloads
        .iter()
        .map(|parts| parts.iter().map(|part| part.as_slice()).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let puts = items
        .iter()
        .zip(slice_sets.iter())
        .map(|(item, slices)| {
            MultiBufferPutRequest::new(item.key.as_str(), slices.as_slice())
                .tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    writer.batch_put_from_multi_buffers(&puts)?;

    let refs = items
        .iter()
        .map(|item| ObjectRef::new(item.key.as_str()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    let buffers = reader.batch_get_buffer(&refs)?;
    ensure_batch_payloads("batch_get_buffer", &items, &buffers)?;

    let mut output_parts = items
        .iter()
        .map(|item| {
            split_lengths(item.value.len())
                .into_iter()
                .map(|len| vec![0u8; len])
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let sizes = {
        let mut output_refs = output_parts
            .iter_mut()
            .map(|parts| {
                parts
                    .iter_mut()
                    .map(|part| part.as_mut_slice())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut gets = items
            .iter()
            .zip(output_refs.iter_mut())
            .map(|(item, refs)| {
                MultiBufferGetRequest::new(item.key.as_str(), refs.as_mut_slice())
                    .tenant(item.tenant.as_str())
            })
            .collect::<Vec<_>>();
        reader.batch_get_into_multi_buffers(&mut gets)?
    };
    for (size, item) in sizes.iter().zip(items.iter()) {
        if *size != item.value.len() {
            return Err(StoreError::Transport(format!(
                "batch_get_into_multi_buffers size mismatch for {}: got={} expected={}",
                item.key,
                size,
                item.value.len()
            )));
        }
    }
    let merged = output_parts
        .into_iter()
        .map(|parts| parts.into_iter().flatten().collect::<Vec<_>>())
        .collect::<Vec<_>>();
    ensure_batch_payloads("batch_get_into_multi_buffers", &items, &merged)?;
    Ok(())
}

fn verify_overwrite_reclaims_capacity(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let mut expected = Vec::new();
    for round in 0..64usize {
        let value = payload(&format!("overwrite-{round}"), value_size);
        writer.put("overwrite-key", &value)?;
        expected = value;
    }
    let actual = reader.get("overwrite-key")?;
    ensure_payload("overwrite reclaim", &expected, &actual)?;
    Ok(())
}

fn verify_request_replication_policy(
    writer: &StoreClient,
    preferred_target: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let single_value = payload("policy-single", value_size);
    let single_route = writer.put_with_policy(
        "policy-single-key",
        &single_value,
        &ReplicationPolicy::new().replica_count(2),
    )?;
    if single_route.replicas.len() != 2 {
        return Err(StoreError::InvalidState(format!(
            "request replication policy expected 2 replicas, got {}",
            single_route.replicas.len()
        )));
    }
    let owners = single_route
        .replicas
        .iter()
        .map(|replica| replica.owner.storage_key())
        .collect::<BTreeSet<_>>();
    if owners.len() != 2 {
        return Err(StoreError::InvalidState(format!(
            "request replication policy did not spread across 2 owners: {:?}",
            owners
        )));
    }

    let preferred_segment = preferred_target
        .list_segments()?
        .into_iter()
        .find(|segment| segment.state == SegmentLifecycleState::Active)
        .ok_or_else(|| StoreError::NotFound("preferred target segment".to_string()))?
        .segment_name;
    let batch_a = payload("policy-batch-a", value_size);
    let batch_b = payload("policy-batch-b", value_size);
    let batch = vec![
        PutRequest::new("policy-batch-a", &batch_a)
            .replication(ReplicationPolicy::new().replica_count(2)),
        PutRequest::new("policy-batch-b", &batch_b).replication(
            ReplicationPolicy::new()
                .replica_count(1)
                .preferred_segment(preferred_segment.0.clone()),
        ),
    ];
    writer.batch_put(&batch)?;

    let preferred_route = writer
        .query_route("policy-batch-b")?
        .ok_or_else(|| StoreError::NotFound("policy-batch-b".to_string()))?;
    if preferred_route.replicas.len() != 1
        || preferred_route.replicas[0].segment_name != preferred_segment
    {
        return Err(StoreError::InvalidState(format!(
            "preferred segment policy missed target {}: {:?}",
            preferred_segment.0, preferred_route.replicas
        )));
    }

    let refs = [
        ObjectRef::new("policy-single-key"),
        ObjectRef::new("policy-batch-a"),
        ObjectRef::new("policy-batch-b"),
    ];
    let values = reader.batch_get(&refs)?;
    ensure_payload("policy single", &single_value, &values[0])?;
    ensure_payload("policy batch a", &batch_a, &values[1])?;
    ensure_payload("policy batch b", &batch_b, &values[2])?;
    Ok(())
}

fn verify_delete_reclaim_parity(writer: &StoreClient, value_size: usize) -> Result<()> {
    let deleted = payload("delete-reclaim-a", value_size);
    writer.put("delete-key-a", &deleted)?;
    let first_route = writer
        .query_route("delete-key-a")?
        .ok_or_else(|| StoreError::NotFound("delete-key-a".to_string()))?;
    let first_offset = first_route.replicas[0].segment_offset;

    writer.remove("delete-key-a", true)?;
    if writer.is_exist("delete-key-a")? {
        return Err(StoreError::InvalidState(
            "deleted key still exists after remove".to_string(),
        ));
    }
    if writer.get_size("delete-key-a")? != 0 {
        return Err(StoreError::InvalidState(
            "deleted key still reports non-zero size".to_string(),
        ));
    }
    if writer.query_route("delete-key-a")?.is_some() {
        return Err(StoreError::InvalidState(
            "deleted key still has a published route".to_string(),
        ));
    }

    let replacement = payload("delete-reclaim-b", value_size);
    writer.put("delete-key-b", &replacement)?;
    let second_route = writer
        .query_route("delete-key-b")?
        .ok_or_else(|| StoreError::NotFound("delete-key-b".to_string()))?;
    if second_route.replicas[0].segment_offset != first_offset {
        return Err(StoreError::InvalidState(format!(
            "delete reclaim did not reuse freed space: old={} new={}",
            first_offset, second_route.replicas[0].segment_offset
        )));
    }
    Ok(())
}

fn verify_dynamic_expand_and_soft_shrink(
    target: &StoreClient,
    router: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let primary = SegmentName::new("elastic-target-segment");
    let first = payload("elastic-initial", value_size);
    router.put_with_policy(
        "elastic-key",
        &first,
        &ReplicationPolicy::new().prefer_local(false),
    )?;
    let first_route = router
        .query_route("elastic-key")?
        .ok_or_else(|| StoreError::NotFound("elastic-key".to_string()))?;
    if first_route.replicas[0].segment_name != primary {
        return Err(StoreError::InvalidState(format!(
            "elastic initial write landed on unexpected segment {}",
            first_route.replicas[0].segment_name.0
        )));
    }

    let expanded = target.expand_local_memory(value_size * 4)?;
    target.drain_segment(&primary)?;

    let second = payload("elastic-updated", value_size);
    router.put_with_policy(
        "elastic-key",
        &second,
        &ReplicationPolicy::new().prefer_local(false),
    )?;
    let second_route = router
        .query_route("elastic-key")?
        .ok_or_else(|| StoreError::NotFound("elastic-key".to_string()))?;
    if second_route.replicas[0].segment_name != expanded.segment_name {
        return Err(StoreError::InvalidState(format!(
            "elastic overwrite did not move to expanded segment: {}",
            second_route.replicas[0].segment_name.0
        )));
    }

    let segments = target.list_segments()?;
    let primary_state = segments
        .iter()
        .find(|segment| segment.segment_name == primary)
        .ok_or_else(|| StoreError::NotFound(primary.0.clone()))?;
    if primary_state.state != SegmentLifecycleState::Draining {
        return Err(StoreError::InvalidState(
            "primary elastic segment is not draining".to_string(),
        ));
    }
    if primary_state.used_bytes != 0 {
        return Err(StoreError::InvalidState(format!(
            "primary elastic segment still has live bytes: {}",
            primary_state.used_bytes
        )));
    }
    if !target.retire_segment(&primary)? {
        return Err(StoreError::InvalidState(
            "elastic draining segment did not retire".to_string(),
        ));
    }

    let remaining = target.list_segments()?;
    if remaining
        .iter()
        .any(|segment| segment.segment_name == primary)
    {
        return Err(StoreError::InvalidState(
            "elastic primary segment still published after retire".to_string(),
        ));
    }
    let actual = reader.get("elastic-key")?;
    ensure_payload("elastic expand+shrink", &second, &actual)?;
    Ok(())
}

fn verify_true_client_shrink(
    target: &mut StoreClient,
    router: &StoreClient,
    _reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let target_storage = target.runtime_id().storage_key();
    let mut owned = Vec::new();
    for index in 0..32 {
        let key = format!("shrink-key-{index}");
        let value = payload(&format!("shrink-value-{index}"), value_size);
        router.put_with_policy(
            &key,
            &value,
            &ReplicationPolicy::new()
                .prefer_local(false)
                .preferred_storage_owner(target_storage.clone()),
        )?;
        let route = router
            .query_route(&key)?
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        if route.replicas[0].owner != *target.runtime_id() {
            return Err(StoreError::InvalidState(format!(
                "true client shrink routed key {key} to unexpected owner {}",
                route.replicas[0].owner
            )));
        }
        owned.push((key, value));
    }
    if owned.is_empty() {
        return Err(StoreError::InvalidState(
            "true client shrink did not place any key on the shrink target".to_string(),
        ));
    }
    for (key, value) in &owned {
        let mut actual = None;
        let mut last_error = None;
        for _ in 0..50 {
            match router.get(key) {
                Ok(payload) => {
                    if payload == *value {
                        actual = Some(payload);
                        last_error = None;
                        break;
                    }
                    actual = Some(payload);
                    last_error = None;
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
            sleep(Duration::from_millis(100));
        }
        let actual = actual.unwrap_or_default();
        if actual != *value {
            return Err(StoreError::Transport(format!(
                "true shrink preflight failed for key {key}: target_state={:?} route={:?} last_error={last_error:?}",
                router.runtime_state(target.runtime_id())?,
                router.query_route(key)?
            )));
        }
        ensure_payload(&format!("true shrink preflight {key}"), value, &actual)?;
    }

    let migrated = target.evacuate_owned_replicas_via(router)?;
    if migrated < owned.len() {
        return Err(StoreError::InvalidState(format!(
            "true client shrink migrated {migrated} objects but expected at least {}",
            owned.len()
        )));
    }
    sleep(Duration::from_secs(2));

    if target.lease().state != ClientLifecycleState::Draining {
        return Err(StoreError::InvalidState(
            "true client shrink did not leave the target in draining state".to_string(),
        ));
    }
    if !target.list_segments()?.is_empty() {
        return Err(StoreError::InvalidState(
            "true client shrink left published local segments behind".to_string(),
        ));
    }

    for (key, value) in owned {
        let route = router
            .query_route(&key)?
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        if route
            .replicas
            .iter()
            .any(|replica| replica.owner == *target.runtime_id())
        {
            return Err(StoreError::InvalidState(format!(
                "true client shrink route still references evacuated runtime for key {key}"
            )));
        }
        let mut actual = None;
        let mut last_error = None;
        for _ in 0..50 {
            match router.get(&key) {
                Ok(payload) => {
                    if payload == value {
                        actual = Some(payload);
                        last_error = None;
                        break;
                    }
                    actual = Some(payload);
                    last_error = None;
                }
                Err(error) => {
                    last_error = Some(error);
                }
            }
            sleep(Duration::from_millis(100));
        }
        let actual = actual.unwrap_or_default();
        if actual != value {
            let reader_route = router.query_route(&key)?;
            return Err(StoreError::Transport(format!(
                "true shrink payload mismatch for key {key}: router_route={route:?} reader_route={reader_route:?} last_error={last_error:?}"
            )));
        }
        ensure_payload(&format!("true shrink {key}"), &value, &actual)?;
    }
    Ok(())
}

fn verify_embedded_route_directory_offloads_metadata(
    metadata: &dyn MetadataBackend,
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let key = "wrh-offload-key";
    let value = payload("wrh-offload", value_size);
    writer.put(key, &value)?;

    let actual = reader.get(key)?;
    ensure_payload("embedded route offload", &value, &actual)?;

    let scoped_key = ObjectKey::new(format!("tenant-a::{key}"));
    if metadata.get_object_route(&scoped_key)?.is_some() {
        return Err(StoreError::InvalidState(
            "embedded WRH route unexpectedly persisted to metadata backend".to_string(),
        ));
    }

    let route = reader
        .query_route(key)?
        .ok_or_else(|| StoreError::NotFound(key.to_string()))?;
    if route.key != scoped_key {
        return Err(StoreError::InvalidState(format!(
            "embedded WRH route key mismatch: got={} expected={}",
            route.key.0, scoped_key.0
        )));
    }
    if route.replicas.is_empty() {
        return Err(StoreError::InvalidState(
            "embedded WRH route has no replicas".to_string(),
        ));
    }
    Ok(())
}

fn verify_routed_scale_out(
    router: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let items = build_items("tenant-a", "scale-auto", 32, value_size);
    let puts = items
        .iter()
        .map(|item| {
            PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    router.batch_put(&puts)?;

    let mut placement_counts = std::collections::BTreeMap::<String, usize>::new();
    for item in &items {
        let route = router
            .query_route_in_tenant(item.tenant.as_str(), item.key.as_str())?
            .ok_or_else(|| StoreError::NotFound(item.key.clone()))?;
        let owner = route
            .replicas
            .first()
            .ok_or_else(|| StoreError::InvalidState("scale-out route has no replica".to_string()))?
            .owner
            .storage_key();
        *placement_counts.entry(owner).or_default() += 1;
    }
    if placement_counts.len() < 2 {
        return Err(StoreError::InvalidState(format!(
            "routed scale-out failed to spread across active storage nodes: {:?}",
            placement_counts
        )));
    }
    let refs = items
        .iter()
        .map(|item| ObjectRef::new(item.key.as_str()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    let results = reader.batch_get(&refs)?;
    ensure_batch_payloads("scale-out batch_get", &items, &results)?;
    Ok(())
}

fn verify_multi_replica_route_publish(
    router: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let items = build_items("tenant-a", "replicated", 8, value_size);
    let puts = items
        .iter()
        .map(|item| {
            PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    router.batch_put(&puts)?;

    for item in &items {
        let route = router
            .query_route_in_tenant(item.tenant.as_str(), item.key.as_str())?
            .ok_or_else(|| StoreError::NotFound(item.key.clone()))?;
        if route.replicas.len() != 2 {
            return Err(StoreError::InvalidState(format!(
                "replicated route {} expected 2 replicas, got {}",
                item.key,
                route.replicas.len()
            )));
        }
        if route.replicas[0].owner == route.replicas[1].owner {
            return Err(StoreError::InvalidState(format!(
                "replicated route {} collapsed to one owner",
                item.key
            )));
        }
    }

    let refs = items
        .iter()
        .map(|item| ObjectRef::new(item.key.as_str()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    let results = reader.batch_get(&refs)?;
    ensure_batch_payloads("replicated batch_get", &items, &results)?;
    Ok(())
}

fn verify_hot_upgrade(
    writer_a: &mut StoreClient,
    writer_upgrade: &mut StoreClient,
    routed_writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    writer_a.enter_draining()?;
    writer_a.plan_handoff(
        ClientEpoch(2),
        HandoffKind::HotUpgrade,
        42,
        now_ms(),
        Some(now_ms() + 5_000),
    )?;
    writer_upgrade.activate()?;

    let preserved = payload("upgrade-preserved", value_size);
    let promoted = payload("upgrade-promoted", value_size);
    writer_a.put("upgrade-old-key", &preserved)?;
    writer_upgrade.put("upgrade-new-key", &promoted)?;

    let keys = [
        ObjectRef::new("upgrade-old-key"),
        ObjectRef::new("upgrade-new-key"),
    ];
    let results = reader.batch_get(&keys)?;
    ensure_payload("upgrade old", &preserved, &results[0])?;
    ensure_payload("upgrade new", &promoted, &results[1])?;

    let routed = payload("upgrade-routed", value_size);
    routed_writer.put_with_policy(
        "upgrade-routed-key",
        &routed,
        &ReplicationPolicy::new().prefer_local(false),
    )?;
    let routed_route = routed_writer
        .query_route("upgrade-routed-key")?
        .ok_or_else(|| StoreError::NotFound("upgrade-routed-key".to_string()))?;
    if routed_route
        .replicas
        .iter()
        .any(|replica| replica.owner == *writer_a.runtime_id())
    {
        return Err(StoreError::InvalidState(
            "routed write still selected draining old runtime after upgrade".to_string(),
        ));
    }
    if !routed_route
        .replicas
        .iter()
        .any(|replica| replica.owner == *writer_upgrade.runtime_id())
    {
        return Err(StoreError::InvalidState(
            "routed write did not select activated upgrade runtime".to_string(),
        ));
    }
    let actual = reader.get("upgrade-routed-key")?;
    ensure_payload("upgrade routed", &routed, &actual)?;
    Ok(())
}

fn verify_rdma_bandwidth_isolation(
    metadata: Arc<RedisMetadataBackend>,
    planner: PlacementPlanner,
    redis_port: u16,
    reader: &StoreClient,
    value_size: usize,
    rdma_mode: &RdmaMode,
) -> Result<()> {
    if !rdma_mode.enabled {
        println!(
            "skip rdma bandwidth isolation: {} (requested={} supported={})",
            rdma_mode.reason, rdma_mode.requested, rdma_mode.supported
        );
        return Ok(());
    }

    let high = build_routed_client(
        metadata.clone(),
        "rdma-high-router",
        ClientLifecycleState::Active,
        build_tent_bundle(redis_port, "rdma-high-router-segment", true)?,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 8)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .tags(vec!["dram".to_string(), "rdma-high-router".to_string()]),
        "tenant-high",
        &[
            ("pool", "pool-a"),
            ("role", "rdma-high"),
            ("storage", "false"),
        ],
        planner.clone(),
        1,
        Some(BandwidthShaping::new().max_inflight_bytes_per_batch((value_size * 8) as u64)),
    )?;
    let low = build_routed_client(
        metadata,
        "rdma-low-router",
        ClientLifecycleState::Active,
        build_tent_bundle(redis_port, "rdma-low-router-segment", true)?,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 8)
            .scratch_bytes(SCRATCH_BYTES)
            .location("cpu:0")
            .tags(vec!["dram".to_string(), "rdma-low-router".to_string()]),
        "tenant-low",
        &[
            ("pool", "pool-a"),
            ("role", "rdma-low"),
            ("storage", "false"),
        ],
        planner,
        1,
        Some(BandwidthShaping::new().max_inflight_bytes_per_batch(value_size as u64)),
    )?;
    high.register_local_memory()?;
    low.register_local_memory()?;

    let iterations = env::var("MC_STORE_RS_RDMA_ISOLATION_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64)
        .max(1);
    let warmup_iters = env::var("MC_STORE_RS_RDMA_ISOLATION_WARMUP_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(8);
    let rounds = env::var("MC_STORE_RS_RDMA_ISOLATION_ROUNDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3)
        .max(1);
    let min_ratio = env::var("MC_STORE_RS_RDMA_ISOLATION_MIN_RATIO")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(1.20)
        .max(1.0);

    let warmup = |client: &StoreClient, tenant: &str, prefix: &str| -> Result<()> {
        for index in 0..warmup_iters {
            let key = format!("{prefix}-{index}");
            let value = payload(&key, value_size);
            client.put_in_tenant(tenant, &key, &value)?;
        }
        Ok(())
    };
    warmup(&high, "tenant-high", "rdma-high-warmup")?;
    warmup(&low, "tenant-low", "rdma-low-warmup")?;

    let mut ratios = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let start = Arc::new(Barrier::new(3));
        let high_done = Arc::new(std::sync::Mutex::new(None::<Result<Duration>>));
        let low_done = Arc::new(std::sync::Mutex::new(None::<Result<Duration>>));
        let high_prefix = format!("rdma-high-r{round}");
        let low_prefix = format!("rdma-low-r{round}");

        std::thread::scope(|scope| {
            let start_high = start.clone();
            let done_high = high_done.clone();
            let high_prefix = high_prefix.clone();
            let high_client = &high;
            scope.spawn(move || {
                start_high.wait();
                let begin = Instant::now();
                let result = (|| {
                    for index in 0..iterations {
                        let key = format!("{high_prefix}-{index}");
                        let value = payload(&key, value_size);
                        high_client.put(&key, &value)?;
                    }
                    Ok::<Duration, StoreError>(begin.elapsed())
                })();
                *done_high.lock().expect("high result lock") = Some(result);
            });

            let start_low = start.clone();
            let done_low = low_done.clone();
            let low_prefix = low_prefix.clone();
            let low_client = &low;
            scope.spawn(move || {
                start_low.wait();
                let begin = Instant::now();
                let result = (|| {
                    for index in 0..iterations {
                        let key = format!("{low_prefix}-{index}");
                        let value = payload(&key, value_size);
                        low_client.put(&key, &value)?;
                    }
                    Ok::<Duration, StoreError>(begin.elapsed())
                })();
                *done_low.lock().expect("low result lock") = Some(result);
            });

            start.wait();
        });

        let high_elapsed = high_done
            .lock()
            .expect("high result lock")
            .take()
            .expect("high run result")?;
        let low_elapsed = low_done
            .lock()
            .expect("low result lock")
            .take()
            .expect("low run result")?;

        for index in 0..iterations {
            let high_key = format!("{high_prefix}-{index}");
            ensure_payload(
                "rdma high tenant readback",
                &payload(&high_key, value_size),
                &reader.get_in_tenant("tenant-high", &high_key)?,
            )?;
            let low_key = format!("{low_prefix}-{index}");
            ensure_payload(
                "rdma low tenant readback",
                &payload(&low_key, value_size),
                &reader.get_in_tenant("tenant-low", &low_key)?,
            )?;
        }

        let high_throughput = iterations as f64 * value_size as f64 / high_elapsed.as_secs_f64();
        let low_throughput = iterations as f64 * value_size as f64 / low_elapsed.as_secs_f64();
        let ratio = high_throughput / low_throughput.max(1.0);
        ratios.push(ratio);
        println!(
            "rdma isolation round={round} high_Bps={high_throughput:.2} low_Bps={low_throughput:.2} ratio={ratio:.2}"
        );
    }

    let median_ratio = median_f64(&ratios);
    println!(
        "rdma isolation summary: rounds={rounds} median_ratio={median_ratio:.2} min_ratio={min_ratio:.2} ratios={ratios:?}"
    );
    if median_ratio < min_ratio {
        return Err(StoreError::Transport(format!(
            "rdma bandwidth isolation ratio below threshold: median={median_ratio:.2} required={min_ratio:.2} ratios={ratios:?}"
        )));
    }
    Ok(())
}

fn detect_rdma_mode() -> RdmaMode {
    let requested = env::var("MC_STORE_RS_ENABLE_RDMA")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    let supported = rdma_supported();
    let (enabled, reason) = if supported {
        (
            true,
            if requested {
                "explicitly requested and supported"
            } else {
                "auto-enabled on RDMA-capable host"
            },
        )
    } else if requested {
        (
            false,
            "requested but ibv_devinfo is unavailable or reports no RDMA support",
        )
    } else {
        (
            false,
            "ibv_devinfo is unavailable or reports no RDMA support",
        )
    };
    RdmaMode {
        requested,
        supported,
        enabled,
        reason,
    }
}

fn rdma_supported() -> bool {
    let output = Command::new("ibv_devinfo").output();
    let Ok(output) = output else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout.contains("hca_id") || stdout.contains("transport:")
}

fn run_batch_put_benchmark(
    writer: &mut StoreClient,
    value_size: usize,
    iterations: usize,
    other_clients: &mut [&mut StoreClient],
) -> Result<()> {
    for batch_size in [1usize, 8, 64] {
        let start = Instant::now();
        let mut total_bytes = 0usize;
        for iteration in 0..iterations {
            let items = build_items(
                "bench-put",
                &format!("put-{batch_size}-{iteration}"),
                batch_size,
                value_size,
            );
            let puts = items
                .iter()
                .map(|item| {
                    PutRequest::new(item.key.as_str(), item.value.as_slice())
                        .tenant(item.tenant.as_str())
                })
                .collect::<Vec<_>>();
            batch_put_with_retry(writer, &puts)?;
            total_bytes += batch_size * value_size;
            if (iteration + 1) % BENCH_HEARTBEAT_EVERY == 0 {
                heartbeat_clients(other_clients)?;
                writer.heartbeat(now_ms().saturating_add(LEASE_MS))?;
            }
        }
        print_bench(
            "put",
            batch_size,
            iterations,
            value_size,
            total_bytes,
            start.elapsed(),
        );
    }
    Ok(())
}

fn run_batch_get_benchmark(
    writer: &mut StoreClient,
    reader: &mut StoreClient,
    value_size: usize,
    iterations: usize,
    other_clients: &mut [&mut StoreClient],
) -> Result<()> {
    let items = build_items("bench-get", "get", 64, value_size);
    let puts = items
        .iter()
        .map(|item| {
            PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    batch_put_with_retry(writer, &puts)?;

    for batch_size in [1usize, 8, 64] {
        let subset = &items[..batch_size];
        let mut buffers = subset
            .iter()
            .map(|item| vec![0u8; item.value.len()])
            .collect::<Vec<_>>();
        for buffer in &mut buffers {
            reader.register_buffer(buffer.as_mut_ptr().cast(), buffer.len())?;
        }
        let start = Instant::now();
        let bench_result = (|| {
            for iteration in 0..iterations {
                let mut gets = subset
                    .iter()
                    .zip(buffers.iter_mut())
                    .map(|(item, buffer)| {
                        GetRequest::new(item.key.as_str(), buffer.as_mut_slice())
                            .tenant(item.tenant.as_str())
                    })
                    .collect::<Vec<_>>();
                let sizes = reader.batch_get_into(&mut gets)?;
                for (size, item) in sizes.iter().zip(subset.iter()) {
                    if *size != item.value.len() {
                        return Err(StoreError::Transport(format!(
                            "bench batch_get_into size mismatch for {}: got={} expected={}",
                            item.key,
                            size,
                            item.value.len()
                        )));
                    }
                }
                if (iteration + 1) % BENCH_HEARTBEAT_EVERY == 0 {
                    heartbeat_clients(other_clients)?;
                    writer.heartbeat(now_ms().saturating_add(LEASE_MS))?;
                    reader.heartbeat(now_ms().saturating_add(LEASE_MS))?;
                }
            }
            Ok(())
        })();
        for buffer in &mut buffers {
            reader.unregister_buffer(buffer.as_mut_ptr().cast(), buffer.len())?;
        }
        bench_result?;
        ensure_batch_payloads("bench_get", subset, &buffers)?;
        print_bench(
            "get",
            batch_size,
            iterations,
            value_size,
            iterations * batch_size * value_size,
            start.elapsed(),
        );
    }
    Ok(())
}

fn heartbeat_clients(clients: &mut [&mut StoreClient]) -> Result<()> {
    let expires_at_ms = now_ms().saturating_add(LEASE_MS);
    for client in clients.iter_mut() {
        client.heartbeat(expires_at_ms)?;
    }
    Ok(())
}

fn batch_put_with_retry(writer: &StoreClient, puts: &[PutRequest<'_>]) -> Result<()> {
    let mut last_conflict = None;
    for _ in 0..8 {
        match writer.batch_put(puts) {
            Ok(_) => return Ok(()),
            Err(StoreError::Conflict(error)) => {
                last_conflict = Some(error);
                sleep(Duration::from_millis(20));
            }
            Err(error) => return Err(error),
        }
    }
    Err(StoreError::Conflict(last_conflict.unwrap_or_else(|| {
        "benchmark batch_put exhausted route CAS retries".to_string()
    })))
}

fn median_f64(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).expect("ratios should be finite"));
    let mid = sorted.len() / 2;
    if (sorted.len() & 1) == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

fn print_bench(
    phase: &str,
    batch_size: usize,
    iterations: usize,
    value_size: usize,
    total_bytes: usize,
    elapsed: std::time::Duration,
) {
    let avg_batch_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
    let throughput_mib_s = total_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    println!(
        "bench {phase}: batch_size={batch_size} iterations={iterations} value_size={value_size} avg_batch_us={avg_batch_us:.2} throughput_mib_s={throughput_mib_s:.2}"
    );
}

fn build_items(tenant: &str, prefix: &str, count: usize, value_size: usize) -> Vec<OwnedItem> {
    (0..count)
        .map(|index| {
            let key = format!("{prefix}-{index}");
            OwnedItem {
                tenant: tenant.to_string(),
                key: key.clone(),
                value: payload(&format!("{tenant}-{key}"), value_size),
            }
        })
        .collect()
}

fn ensure_batch_payloads(label: &str, items: &[OwnedItem], actual: &[Vec<u8>]) -> Result<()> {
    if items.len() != actual.len() {
        return Err(StoreError::Transport(format!(
            "{label} length mismatch: items={} actual={}",
            items.len(),
            actual.len()
        )));
    }
    for (item, bytes) in items.iter().zip(actual.iter()) {
        ensure_payload(label, &item.value, bytes)?;
    }
    Ok(())
}

fn ensure_payload(label: &str, expected: &[u8], actual: &[u8]) -> Result<()> {
    if expected.len() != actual.len() {
        return Err(StoreError::Transport(format!(
            "{label} length mismatch: actual={} expected={}",
            actual.len(),
            expected.len()
        )));
    }
    for (index, (left, right)) in actual.iter().zip(expected.iter()).enumerate() {
        if left != right {
            return Err(StoreError::Transport(format!(
                "{label} payload mismatch at offset {index}: actual={left} expected={right}"
            )));
        }
    }
    Ok(())
}

fn payload(seed: &str, size: usize) -> Vec<u8> {
    let mut state = 0u64;
    for byte in seed.as_bytes() {
        state = state.wrapping_mul(131).wrapping_add(*byte as u64 + 17);
    }
    let mut bytes = vec![0u8; size];
    for (index, byte) in bytes.iter_mut().enumerate() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(index as u64 + 1);
        *byte = ((state >> 24) % 251) as u8;
    }
    bytes
}

fn split_payload(payload: &[u8]) -> Vec<Vec<u8>> {
    let lengths = split_lengths(payload.len());
    let mut cursor = 0usize;
    let mut parts = Vec::with_capacity(lengths.len());
    for length in lengths {
        parts.push(payload[cursor..cursor + length].to_vec());
        cursor += length;
    }
    parts
}

fn split_lengths(total: usize) -> Vec<usize> {
    if total <= 3 {
        return vec![total];
    }
    let first = total / 3;
    let second = total / 3;
    let third = total - first - second;
    vec![first, second, third]
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn unique_suffix() -> u64 {
    now_ms() ^ (std::process::id() as u64)
}

struct OwnedItem {
    tenant: String,
    key: String,
    value: Vec<u8>,
}
