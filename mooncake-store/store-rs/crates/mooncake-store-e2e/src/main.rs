use std::collections::BTreeSet;
use std::env;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{
    init_tracing_from_env, render_prometheus_metrics, start_metrics_http_server_from_env,
    GetRequest, LocalMemoryConfig, MooncakeCompatibilityFacade, MultiBufferGetRequest,
    MultiBufferPutRequest, ObjectRef, PlacementPlanner, PutFromRequest, PutRequest,
    ReplicationPolicy, StoreClient, StoreClientBuilder, TentTransportFactory,
};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, HandoffKind, MetadataBackend,
    ObjectKey, Result, SegmentLifecycleState, SegmentName, StoreError,
};
use mooncake_transport::{TentEngine, TentEngineConfig};

const LEASE_MS: u64 = 30_000;
const MEMORY_BYTES: usize = 128 * 1024 * 1024;
const SCRATCH_BYTES: usize = 16 * 1024 * 1024;

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
        .unwrap_or(128);
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

    let target_a_bundle = build_tent_bundle(redis_port, "target-a-segment")?;
    let target_b_bundle = build_tent_bundle(redis_port, "target-b-segment")?;
    let target_c_bundle = build_tent_bundle(redis_port, "target-c-segment")?;
    let upgrade_bundle = build_tent_bundle(redis_port, "target-a-upgrade-segment")?;
    let reclaim_bundle = build_tent_bundle(redis_port, "target-reclaim-segment")?;
    let router_bundle = build_tent_bundle(redis_port, "router-segment")?;
    let router_replica_bundle = build_tent_bundle(redis_port, "router-replica-segment")?;
    let reader_bundle = build_tent_bundle(redis_port, "reader-segment")?;
    let elastic_target_bundle = build_tent_bundle(redis_port, "elastic-target-segment")?;
    let elastic_router_bundle = build_tent_bundle(redis_port, "elastic-router-segment")?;

    let mut target_a = build_client(
        metadata.clone(),
        "store-a",
        ClientEpoch(1),
        ClientLifecycleState::Active,
        target_a_bundle,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "primary"), ("storage", "true")],
    )?;
    let mut target_b = build_client(
        metadata.clone(),
        "store-b",
        ClientEpoch(1),
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
    let target_c = build_client(
        metadata.clone(),
        "store-c",
        ClientEpoch(1),
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
        ClientEpoch(2),
        ClientLifecycleState::Standby,
        upgrade_bundle,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "upgrade"), ("storage", "true")],
    )?;
    let reclaim_writer = build_client(
        metadata.clone(),
        "store-reclaim",
        ClientEpoch(1),
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
    let router = build_routed_client(
        metadata.clone(),
        "router",
        ClientEpoch(1),
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
    )?;
    let router_replica = build_routed_client(
        metadata.clone(),
        "router-replica",
        ClientEpoch(1),
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
    )?;
    let reader = build_client(
        metadata.clone(),
        "reader",
        ClientEpoch(1),
        ClientLifecycleState::Active,
        reader_bundle,
        memory,
        "tenant-a",
        &[("pool", "pool-a"), ("role", "reader"), ("storage", "false")],
    )?;
    let elastic_target = build_client(
        metadata.clone(),
        "elastic-target",
        ClientEpoch(1),
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
    let elastic_router = build_routed_client(
        metadata.clone(),
        "elastic-router",
        ClientEpoch(1),
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
    )?;
    target_a.register_local_memory()?;
    target_b.register_local_memory()?;
    target_c.register_local_memory()?;
    target_upgrade.register_local_memory()?;
    reclaim_writer.register_local_memory()?;
    router.register_local_memory()?;
    router_replica.register_local_memory()?;
    reader.register_local_memory()?;
    elastic_target.register_local_memory()?;
    elastic_router.register_local_memory()?;
    wait_for_membership_convergence(&[
        &target_a,
        &target_b,
        &target_c,
        &target_upgrade,
        &reclaim_writer,
        &router,
        &router_replica,
        &reader,
        &elastic_target,
        &elastic_router,
    ])?;

    verify_single_put_get(&target_a, &reader, value_size)?;
    verify_multi_tenant_isolation(&target_a, &reader, value_size)?;
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

    run_batch_put_benchmark(&router, value_size, batch_bench_iters)?;
    run_batch_get_benchmark(&target_upgrade, &reader, value_size, batch_bench_iters)?;
    verify_true_client_shrink(&mut target_b, &router, &reader, value_size)?;

    println!(
        "e2e ok: single put/get, batch put/get, request-level replication policy, true delete reclaim, routed remote write, multi-replica publish, registered-buffer path, overwrite reclaim, multi-tenant, scale-out, elastic expand-shrink, true client shrink, hot-upgrade"
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

fn tent_base_config(redis_port: u16) -> TentEngineConfig {
    TentEngineConfig::new()
        .set("metadata_type", "redis")
        .set("metadata_servers", format!("127.0.0.1:{redis_port}"))
        .set("redis_db_index", "0")
        .set("rpc_server_hostname", "127.0.0.1")
        .set("rpc_server_port", "0")
        .set("log_level", "warning")
        .set("transports/tcp/enable", "true")
        .set("transports/shm/enable", "false")
        .set("transports/rdma/enable", "false")
        .set("transports/io_uring/enable", "false")
}

#[allow(clippy::arc_with_non_send_sync)]
fn build_tent_bundle(redis_port: u16, local_segment_name: &str) -> Result<TentBundle> {
    let config = tent_base_config(redis_port);
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
    epoch: ClientEpoch,
    state: ClientLifecycleState,
    transport: TentBundle,
    memory: LocalMemoryConfig,
    tenant: &str,
    labels: &[(&str, &str)],
) -> Result<StoreClient> {
    let mut builder = StoreClientBuilder::new(metadata, stable_id)
        .epoch(epoch)
        .state(state)
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
    epoch: ClientEpoch,
    state: ClientLifecycleState,
    transport: TentBundle,
    memory: LocalMemoryConfig,
    tenant: &str,
    labels: &[(&str, &str)],
    planner: PlacementPlanner,
    replica_count: usize,
) -> Result<StoreClient> {
    let mut builder = StoreClientBuilder::new(metadata, stable_id)
        .epoch(epoch)
        .state(state)
        .tenant(tenant)
        .compatibility(CompatibilityDescriptor::default())
        .local_memory(memory)
        .with_tent(transport.engine)
        .transport_factory(transport.factory)
        .routed_writes(planner, replica_count);
    for (key, value) in labels {
        builder = builder.label(*key, *value);
    }
    builder.build(now_ms() + LEASE_MS)
}

fn wait_for_runtime_visibility(
    client: &StoreClient,
    runtime: &mooncake_store_core::ClientRuntimeId,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match client.runtime_state(runtime) {
            Ok(Some(_)) => return Ok(()),
            Ok(None) | Err(StoreError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        sleep(Duration::from_millis(20));
    }
    Err(StoreError::InvalidState(format!(
        "runtime {} did not become visible to {} before deadline",
        runtime,
        client.runtime_id()
    )))
}

fn wait_for_membership_convergence(clients: &[&StoreClient]) -> Result<()> {
    let runtimes = clients
        .iter()
        .map(|client| client.runtime_id().clone())
        .collect::<Vec<_>>();
    for client in clients {
        for runtime in &runtimes {
            if runtime == client.runtime_id() {
                continue;
            }
            wait_for_runtime_visibility(client, runtime)?;
        }
    }
    Ok(())
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
    let mut owned = Vec::new();
    for index in 0..32 {
        let key = format!("shrink-key-{index}");
        let value = payload(&format!("shrink-value-{index}"), value_size);
        router.put_with_policy(&key, &value, &ReplicationPolicy::new().prefer_local(false))?;
        let route = router
            .query_route(&key)?
            .ok_or_else(|| StoreError::NotFound(key.clone()))?;
        if route.replicas[0].owner == *target.runtime_id() {
            owned.push((key, value));
        }
    }
    if owned.is_empty() {
        return Err(StoreError::InvalidState(
            "true client shrink did not place any key on the shrink target".to_string(),
        ));
    }
    for (key, value) in &owned {
        let actual = router.get(key)?;
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
        let mut actual = router.get(&key)?;
        for _ in 0..20 {
            if actual == value {
                break;
            }
            sleep(Duration::from_millis(100));
            actual = router.get(&key)?;
        }
        if actual != value {
            let reader_route = router.query_route(&key)?;
            return Err(StoreError::Transport(format!(
                "true shrink payload mismatch for key {key}: router_route={route:?} reader_route={reader_route:?}"
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
    let preserved = payload("upgrade-preserved", value_size);
    let promoted = payload("upgrade-promoted", value_size);
    writer_a.put("upgrade-old-key", &preserved)?;
    let old_route = routed_writer
        .query_route("upgrade-old-key")?
        .ok_or_else(|| StoreError::NotFound("upgrade-old-key".to_string()))?;
    if !old_route
        .replicas
        .iter()
        .any(|replica| replica.owner == *writer_a.runtime_id())
    {
        return Err(StoreError::InvalidState(
            "seeded old key did not land on the predecessor runtime".to_string(),
        ));
    }

    writer_a.enter_draining()?;
    let created_at_ms = now_ms();
    let deadline_ms = created_at_ms + 5_000;
    writer_a.plan_handoff(
        ClientEpoch(2),
        HandoffKind::HotUpgrade,
        42,
        created_at_ms,
        Some(deadline_ms),
    )?;
    let plan = writer_upgrade
        .activate_if_targeted_handoff()?
        .ok_or_else(|| {
            StoreError::InvalidState(
                "standby successor did not observe the targeted hot-upgrade handoff".to_string(),
            )
        })?;
    if plan.to != *writer_upgrade.runtime_id() {
        return Err(StoreError::InvalidState(format!(
            "handoff targeted unexpected successor {}",
            plan.to
        )));
    }

    let migrated = writer_a.evacuate_owned_replicas_to_runtime(writer_upgrade.runtime_id())?;
    if migrated == 0 {
        return Err(StoreError::InvalidState(
            "hot-upgrade evacuation did not migrate any owned routes".to_string(),
        ));
    }
    if !writer_a.list_segments()?.is_empty() {
        return Err(StoreError::InvalidState(
            "predecessor still owns local segments after hot-upgrade evacuation".to_string(),
        ));
    }

    let migrated_route = routed_writer
        .query_route("upgrade-old-key")?
        .ok_or_else(|| StoreError::NotFound("upgrade-old-key".to_string()))?;
    if migrated_route
        .replicas
        .iter()
        .any(|replica| replica.owner == *writer_a.runtime_id())
    {
        return Err(StoreError::InvalidState(
            "migrated route still references the predecessor runtime".to_string(),
        ));
    }
    if !migrated_route
        .replicas
        .iter()
        .any(|replica| replica.owner == *writer_upgrade.runtime_id())
    {
        return Err(StoreError::InvalidState(
            "migrated route did not pin the successor runtime".to_string(),
        ));
    }

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

fn run_batch_put_benchmark(
    writer: &StoreClient,
    value_size: usize,
    iterations: usize,
) -> Result<()> {
    for batch_size in [1usize, 8, 64] {
        let items = build_items(
            "bench-put",
            &format!("put-{batch_size}"),
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
        let start = Instant::now();
        let mut total_bytes = 0usize;
        for _ in 0..iterations {
            writer.batch_put(&puts)?;
            total_bytes += batch_size * value_size;
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
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
    iterations: usize,
) -> Result<()> {
    let items = build_items("bench-get", "get", 64, value_size);
    let puts = items
        .iter()
        .map(|item| {
            PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str())
        })
        .collect::<Vec<_>>();
    writer.batch_put(&puts)?;

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
            for _ in 0..iterations {
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
