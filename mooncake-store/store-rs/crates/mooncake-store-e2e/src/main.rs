use std::env;
use std::ffi::c_void;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use mooncake_metadata::{MetadataKeyspace, RedisMetadataBackend, RedisMetadataConfig};
use mooncake_store_client::{MooncakeCompatibilityFacade, StoreClientBuilder};
use mooncake_store_core::{
    ClientEpoch, ClientLifecycleState, CompatibilityDescriptor, ObjectKey, ObjectRoute,
    ReplicaRoute, ReplicaTier, RouteState, RouteVersion, SegmentName,
};
use mooncake_transport::{Opcode, TentEngine, TentEngineConfig, TransferRequest, TransferStatus};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis_url = env::var("MC_STORE_RS_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6380/0".to_string());
    let keyspace = MetadataKeyspace::new(format!("mc/store-rs/e2e/{}", unique_suffix()));
    let metadata = Arc::new(RedisMetadataBackend::new(
        RedisMetadataConfig::new(redis_url).keyspace(keyspace),
    )?);

    let target_engine = TentEngine::new(
        &TentEngineConfig::new()
            .set("metadata_type", "redis")
            .set("metadata_servers", "127.0.0.1:6380")
            .set("redis_db_index", "0")
            .set("rpc_server_hostname", "127.0.0.1")
            .set("rpc_server_port", "0")
            .set("local_segment_name", "target-segment")
            .set("log_level", "warning")
            .set("transports/tcp/enable", "true")
            .set("transports/shm/enable", "false")
            .set("transports/rdma/enable", "false")
            .set("transports/io_uring/enable", "false"),
    )?;
    let initiator_engine = TentEngine::new(
        &TentEngineConfig::new()
            .set("metadata_type", "redis")
            .set("metadata_servers", "127.0.0.1:6380")
            .set("redis_db_index", "0")
            .set("rpc_server_hostname", "127.0.0.1")
            .set("rpc_server_port", "0")
            .set("local_segment_name", "initiator-segment")
            .set("log_level", "warning")
            .set("transports/tcp/enable", "true")
            .set("transports/shm/enable", "false")
            .set("transports/rdma/enable", "false")
            .set("transports/io_uring/enable", "false"),
    )?;

    let target_segment_name = target_engine.segment_name()?;
    let initiator_segment_name = initiator_engine.segment_name()?;

    let target_client = StoreClientBuilder::new(metadata.clone(), "target")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .rpc_address(target_engine.rpc_server_address()?.0)
        .segment_name(target_segment_name.clone())
        .compatibility(CompatibilityDescriptor::default())
        .build(now_ms() + 30_000)?;
    let initiator_client = StoreClientBuilder::new(metadata.clone(), "initiator")
        .epoch(ClientEpoch(1))
        .state(ClientLifecycleState::Active)
        .rpc_address(initiator_engine.rpc_server_address()?.0)
        .segment_name(initiator_segment_name)
        .compatibility(CompatibilityDescriptor::default())
        .build(now_ms() + 30_000)?;

    let transfer_len = 4096usize;
    let target_buffer = target_engine.allocate_memory(transfer_len, "cpu:0")?;
    let initiator_buffer = initiator_engine.allocate_memory(transfer_len, "cpu:0")?;
    write_pattern(target_buffer, transfer_len);
    initiator_engine.register_memory(initiator_buffer, transfer_len)?;
    target_engine.register_memory(target_buffer, transfer_len)?;

    target_client.mount_segment(transfer_len as u64, transfer_len as u64, vec!["dram".to_string()])?;
    let route = ObjectRoute {
        key: ObjectKey::new("object-1"),
        version: RouteVersion(1),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: target_client.runtime_id().clone(),
            segment_name: SegmentName::new(target_segment_name.clone()),
            offset: target_buffer as u64,
            length: transfer_len as u64,
            checksum: None,
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
    };
    let cas = target_client.cas_route("object-1", None, Some(&route))?;
    if !cas.applied {
        return Err("failed to publish test route".into());
    }

    let queried = initiator_client
        .query_route("object-1")?
        .ok_or("query_route returned no object")?;
    let replica = queried.replicas.first().ok_or("query_route returned no replica")?;

    thread::sleep(Duration::from_millis(300));
    let remote_segment = initiator_engine.open_segment(&replica.segment_name.0)?;
    let batch_id = initiator_engine.allocate_batch(1)?;
    initiator_engine.submit(
        batch_id,
        &[TransferRequest {
            opcode: Opcode::Read,
            source: initiator_buffer,
            target_id: remote_segment,
            target_offset: replica.offset,
            length: replica.length,
        }],
    )?;
    wait_for_completion(&initiator_engine, batch_id, Duration::from_secs(10))?;

    verify_pattern(initiator_buffer, transfer_len)?;
    initiator_engine.free_batch(batch_id)?;
    run_benchmark(&initiator_engine, initiator_buffer, replica.offset, remote_segment, transfer_len)?;

    initiator_engine.close_segment(remote_segment)?;
    initiator_engine.unregister_memory(initiator_buffer, transfer_len)?;
    target_engine.unregister_memory(target_buffer, transfer_len)?;
    initiator_engine.free_memory(initiator_buffer)?;
    target_engine.free_memory(target_buffer)?;

    println!("e2e ok: transferred {transfer_len} bytes through Tent over Redis metadata");
    Ok(())
}

fn run_benchmark(
    engine: &TentEngine,
    local_buffer: *mut c_void,
    remote_offset: u64,
    remote_segment: u64,
    transfer_len: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let iterations = env::var("MC_STORE_RS_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(256);
    let start = Instant::now();
    for _ in 0..iterations {
        let batch_id = engine.allocate_batch(1)?;
        engine.submit(
            batch_id,
            &[TransferRequest {
                opcode: Opcode::Read,
                source: local_buffer,
                target_id: remote_segment,
                target_offset: remote_offset,
                length: transfer_len as u64,
            }],
        )?;
        wait_for_completion(engine, batch_id, Duration::from_secs(10))?;
        engine.free_batch(batch_id)?;
    }
    let elapsed = start.elapsed();
    let total_bytes = transfer_len as u128 * iterations as u128;
    let throughput_mb_s = total_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    let average_us = elapsed.as_secs_f64() * 1_000_000.0 / iterations as f64;
    println!(
        "bench ok: iterations={iterations} bytes_per_transfer={transfer_len} avg_us={average_us:.2} throughput_mib_s={throughput_mb_s:.2}"
    );
    Ok(())
}

fn wait_for_completion(
    engine: &TentEngine,
    batch_id: u64,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = engine.task_status(batch_id, 0)?;
        match status.status {
            TransferStatus::Completed => return Ok(()),
            TransferStatus::Failed | TransferStatus::Canceled | TransferStatus::Timeout => {
                return Err(format!("transfer failed with status {:?}", status.status).into())
            }
            _ => {}
        }
        if Instant::now() >= deadline {
            return Err("transfer timed out".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn write_pattern(addr: *mut c_void, len: usize) {
    let bytes = unsafe { std::slice::from_raw_parts_mut(addr.cast::<u8>(), len) };
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index % 251) as u8;
    }
}

fn verify_pattern(addr: *mut c_void, len: usize) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = unsafe { std::slice::from_raw_parts(addr.cast::<u8>(), len) };
    for (index, byte) in bytes.iter().enumerate() {
        let expected = (index % 251) as u8;
        if *byte != expected {
            return Err(format!(
                "payload mismatch at offset {index}: got {}, expected {expected}",
                *byte
            )
            .into());
        }
    }
    Ok(())
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
