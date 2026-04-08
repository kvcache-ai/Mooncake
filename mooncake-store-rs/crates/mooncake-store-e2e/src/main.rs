use std::env;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{
    GetRequest, LocalMemoryConfig, MooncakeCompatibilityFacade, ObjectRef, PutFromRequest,
    PutRequest, StoreClient, StoreClientBuilder,
};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, HandoffKind, Result, StoreError,
};
use mooncake_transport::{TentEngine, TentEngineConfig};

const LEASE_MS: u64 = 30_000;
const MEMORY_BYTES: usize = 128 * 1024 * 1024;
const SCRATCH_BYTES: usize = 16 * 1024 * 1024;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
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
    let batch_bench_iters = env::var("MC_STORE_RS_BATCH_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(128);
    let keyspace = MetadataKeyspace::new(format!("mc/store-rs/e2e/{}", unique_suffix()));
    let metadata = Arc::new(RedisMetadataBackend::new(
        RedisMetadataConfig::new(redis_url).keyspace(keyspace),
    )?);
    let memory = LocalMemoryConfig::new()
        .storage_bytes(MEMORY_BYTES)
        .scratch_bytes(SCRATCH_BYTES)
        .location("cpu:0")
        .tags(vec![
            "dram".to_string(),
            "multi-tenant".to_string(),
            "dynamic-membership".to_string(),
        ]);

    let engine_target_a = build_tent_engine(redis_port, "target-a-segment")?;
    let engine_target_b = build_tent_engine(redis_port, "target-b-segment")?;
    let engine_upgrade = build_tent_engine(redis_port, "target-a-upgrade-segment")?;
    let engine_reclaim = build_tent_engine(redis_port, "target-reclaim-segment")?;
    let engine_reader = build_tent_engine(redis_port, "reader-segment")?;

    let mut target_a = build_client(
        metadata.clone(),
        "store-a",
        ClientEpoch(1),
        ClientLifecycleState::Active,
        engine_target_a,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "primary")],
    )?;
    let target_b = build_client(
        metadata.clone(),
        "store-b",
        ClientEpoch(1),
        ClientLifecycleState::Active,
        engine_target_b,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "scaleout")],
    )?;
    let mut target_upgrade = build_client(
        metadata.clone(),
        "store-a",
        ClientEpoch(2),
        ClientLifecycleState::Standby,
        engine_upgrade,
        memory.clone(),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "upgrade")],
    )?;
    let reclaim_writer = build_client(
        metadata.clone(),
        "store-reclaim",
        ClientEpoch(1),
        ClientLifecycleState::Active,
        engine_reclaim,
        LocalMemoryConfig::new()
            .storage_bytes(value_size * 2)
            .scratch_bytes(value_size * 2)
            .location("cpu:0")
            .reclaim_grace_ms(0)
            .tags(vec!["dram".to_string(), "overwrite-reclaim".to_string()]),
        "tenant-a",
        &[("pool", "pool-a"), ("role", "reclaim")],
    )?;
    let reader = build_client(
        metadata,
        "reader",
        ClientEpoch(1),
        ClientLifecycleState::Active,
        engine_reader,
        memory,
        "tenant-a",
        &[("pool", "pool-a"), ("role", "reader")],
    )?;

    target_a.register_local_memory()?;
    target_b.register_local_memory()?;
    target_upgrade.register_local_memory()?;
    reclaim_writer.register_local_memory()?;
    reader.register_local_memory()?;

    verify_single_put_get(&target_a, &reader, value_size)?;
    verify_multi_tenant_isolation(&target_a, &reader, value_size)?;
    verify_batch_put_get(&target_a, &reader, value_size)?;
    verify_batch_put_from_get_into(&target_a, &reader, value_size)?;
    verify_overwrite_reclaims_capacity(&reclaim_writer, &reader, value_size)?;
    verify_dynamic_scale_out(&target_a, &target_b, &reader, value_size)?;
    verify_hot_upgrade(&mut target_a, &mut target_upgrade, &reader, value_size)?;

    run_batch_put_benchmark(&target_b, value_size, batch_bench_iters)?;
    run_batch_get_benchmark(&target_upgrade, &reader, value_size, batch_bench_iters)?;

    println!(
        "e2e ok: single put/get, batch put/get, registered-buffer path, overwrite reclaim, multi-tenant, scale-out, hot-upgrade"
    );
    Ok(())
}

fn build_tent_engine(redis_port: u16, local_segment_name: &str) -> Result<Arc<TentEngine>> {
    Ok(Arc::new(TentEngine::new(
        &TentEngineConfig::new()
            .set("metadata_type", "redis")
            .set("metadata_servers", format!("127.0.0.1:{redis_port}"))
            .set("redis_db_index", "0")
            .set("rpc_server_hostname", "127.0.0.1")
            .set("rpc_server_port", "0")
            .set("local_segment_name", local_segment_name)
            .set("log_level", "warning")
            .set("transports/tcp/enable", "true")
            .set("transports/shm/enable", "false")
            .set("transports/rdma/enable", "false")
            .set("transports/io_uring/enable", "false"),
    )?))
}

fn build_client(
    metadata: Arc<RedisMetadataBackend>,
    stable_id: &str,
    epoch: ClientEpoch,
    state: ClientLifecycleState,
    engine: Arc<TentEngine>,
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
        .with_tent(engine);
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

fn verify_batch_put_get(
    writer: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let items = build_items("tenant-a", "batch-local", 8, value_size);
    let puts = items
        .iter()
        .map(|item| PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str()))
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
    let put_result = (|| {
        let puts = items
            .iter()
            .zip(input_buffers.iter())
            .map(|(item, buffer)| {
                PutFromRequest::new(item.key.as_str(), buffer.as_ptr().cast(), buffer.len())
                    .tenant(item.tenant.as_str())
            })
            .collect::<Vec<_>>();
        writer.batch_put_from(&puts)
    })();
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
    let get_result = (|| {
        let mut gets = items
            .iter()
            .zip(output_buffers.iter_mut())
            .map(|(item, buffer)| {
                GetRequest::new(item.key.as_str(), buffer.as_mut_slice()).tenant(item.tenant.as_str())
            })
            .collect::<Vec<_>>();
        reader.batch_get_into(&mut gets)
    })();
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

fn verify_dynamic_scale_out(
    writer_a: &StoreClient,
    writer_b: &StoreClient,
    reader: &StoreClient,
    value_size: usize,
) -> Result<()> {
    let mut left = build_items("tenant-a", "scale-left", 4, value_size);
    let mut right = build_items("tenant-a", "scale-right", 4, value_size);
    let puts_left = left
        .iter()
        .map(|item| PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    let puts_right = right
        .iter()
        .map(|item| PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    writer_a.batch_put(&puts_left)?;
    writer_b.batch_put(&puts_right)?;

    left.append(&mut right);
    let refs = left
        .iter()
        .map(|item| ObjectRef::new(item.key.as_str()).tenant(item.tenant.as_str()))
        .collect::<Vec<_>>();
    let results = reader.batch_get(&refs)?;
    ensure_batch_payloads("scale-out batch_get", &left, &results)?;
    Ok(())
}

fn verify_hot_upgrade(
    writer_a: &mut StoreClient,
    writer_upgrade: &mut StoreClient,
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
    Ok(())
}

fn run_batch_put_benchmark(
    writer: &StoreClient,
    value_size: usize,
    iterations: usize,
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
            writer.batch_put(&puts)?;
            total_bytes += batch_size * value_size;
        }
        print_bench("put", batch_size, iterations, value_size, total_bytes, start.elapsed());
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
        .map(|item| PutRequest::new(item.key.as_str(), item.value.as_slice()).tenant(item.tenant.as_str()))
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

fn ensure_batch_payloads(
    label: &str,
    items: &[OwnedItem],
    actual: &[Vec<u8>],
) -> Result<()> {
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
