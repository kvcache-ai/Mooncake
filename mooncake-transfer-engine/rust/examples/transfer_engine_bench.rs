// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transfer Engine throughput benchmark (initiator / target), analogous to
//! `transfer_engine_bench.cpp`.
//!
//! ```bash
//! cargo run --example transfer_engine_bench --release -- \
//!   --metadata-server http://127.0.0.1:8080/metadata \
//!   --mode target
//! cargo run --example transfer_engine_bench --release -- \
//!   --metadata-server http://127.0.0.1:8080/metadata \
//!   --mode initiator --segment-id <target-ip>
//! ```

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use dns_lookup::{get_hostname, getaddrinfo, AddrInfoHints, SockType};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{error, info};
use transfer_engine_rust::{
    MemoryPool, Opcode, TransferEngine, TransferRequest, TransferStatusCode,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    #[clap(long, default_value_t = String::from("localhost:2379"), help = "etcd / metadata server address")]
    pub metadata_server: String,

    #[clap(long, default_value_t = String::from("initiator"), value_parser = ["initiator", "target"],
    help = "Running mode: initiator or target. Initiator node read/write data blocks from target node")]
    pub mode: String,

    #[clap(long, default_value_t = String::from("read"), value_parser = ["read", "write"],
    help = "Operation type: read or write")]
    pub operation: String,

    #[clap(long, default_value_t = String::from("127.0.0.1"), help = "Segment ID to access data")]
    pub segment_id: String,

    #[clap(long, default_value_t = 0, help = "Base addr in target")]
    pub target_base: u64,

    #[clap(long, default_value_t = 128, help = "Batch size")]
    pub batch_size: i32,

    #[clap(
        long,
        default_value_t = 4096,
        help = "Block size for each transfer request"
    )]
    pub block_size: i32,

    #[clap(long, default_value_t = 10, help = "Test duration in seconds")]
    pub duration: i32,

    #[clap(long, default_value_t = 4, help = "Task submission threads")]
    pub threads: i32,

    #[clap(long, default_value_t = String::from("tcp"), help = "Transport protocol (tcp, rdma, efa)")]
    pub protocol: String,
}

fn get_host_ip() -> Result<String> {
    let hostname: String = get_hostname().map_err(|e| anyhow!("Failed to get hostname: {}", e))?;

    let hints = AddrInfoHints {
        socktype: SockType::Stream.into(),
        ..AddrInfoHints::default()
    };

    let addr_infos = getaddrinfo(Some(&hostname), None, Some(hints))
        .map_err(|e| anyhow!("Failed to get host entry: {:?}", e))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow!("Failed to process address info: {}", e))?;

    for addr_info in addr_infos {
        if let IpAddr::V4(ip) = addr_info.sockaddr.ip() {
            let ip_str = ip.to_string();
            if ip_str != "127.0.0.1" && ip_str != "127.0.1.1" {
                return Ok(ip_str);
            }
        }
    }

    bail!("No non-loopback IP address found")
}

fn allocate_memory_pool(size: usize) -> *mut u8 {
    let mut buffer = vec![0u8; size].into_boxed_slice();
    let ptr = buffer.as_mut_ptr();
    std::mem::forget(buffer);
    ptr
}

unsafe fn free_memory_pool(ptr: *mut u8, size: usize) {
    if !ptr.is_null() {
        let _buffer = Vec::from_raw_parts(ptr, size, size);
    }
}

static RUNNING: AtomicBool = AtomicBool::new(true);

fn initiator_worker(
    args: Arc<Args>,
    engine: Arc<TransferEngine>,
    segment_id: i32,
    thread_id: i32,
    pool: Arc<MemoryPool>,
    total_batch_count: Arc<AtomicUsize>,
) -> Result<()> {
    let opcode = match args.operation.as_str() {
        "read" => Opcode::Read,
        "write" => Opcode::Write,
        _ => bail!("Unsupported operation: must be 'read' or 'write'"),
    };
    while RUNNING.load(Ordering::SeqCst) {
        let batch_id = engine.allocate_batch_id(args.batch_size as usize)?;
        let mut requests = Vec::with_capacity(args.batch_size as usize);
        for i in 0..args.batch_size {
            let source_offset = args.block_size * (i * args.threads + thread_id);
            requests.push(TransferRequest {
                opcode,
                source: pool.offset(source_offset as usize) as *mut _,
                target_id: segment_id,
                target_offset: args.target_base + source_offset as u64,
                length: args.block_size as u64,
            });
        }

        unsafe { engine.submit_transfer(batch_id, &requests)? };

        for task_id in 0..args.batch_size as usize {
            loop {
                let status = engine.get_transfer_status(batch_id, task_id)?;
                if status.status == TransferStatusCode::Completed
                    || status.status == TransferStatusCode::Failed
                {
                    break;
                }
            }
        }

        engine.free_batch_id(batch_id)?;
        total_batch_count.fetch_add(1, Ordering::SeqCst);
    }

    Ok(())
}

fn initiator(args: Args) -> Result<()> {
    let args = Arc::new(args);
    let local = get_host_ip()?;
    let engine = Arc::new(TransferEngine::initialize(
        &format!("{local}:12345"),
        &args.metadata_server,
        &args.protocol,
        "",
    )?);

    let dram_buffer_size = 1 << 30;
    let pool = Arc::new(MemoryPool::new(dram_buffer_size));
    unsafe {
        engine.register_local_memory(pool.as_void_ptr(), dram_buffer_size, "cpu:0")?;
    }

    let segment_id = engine.open_segment(&args.segment_id)?;
    let total_batch_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();
    let mut workers = vec![];
    for i in 0..args.threads {
        let args_ref = args.clone();
        let engine_ref = engine.clone();
        let pool_ref = pool.clone();
        let total_batch_count_ref = total_batch_count.clone();
        workers.push(thread::spawn(move || {
            initiator_worker(
                args_ref,
                engine_ref,
                segment_id,
                i,
                pool_ref,
                total_batch_count_ref,
            )
        }))
    }

    thread::sleep(Duration::from_secs(args.duration as u64));
    RUNNING.store(false, Ordering::SeqCst);

    for worker in workers {
        worker.join().unwrap()?;
    }

    let duration = start_time.elapsed().as_secs_f64();
    let batch_count = total_batch_count.load(Ordering::SeqCst);

    info!(
        "Test completed: duration {:.2}, batch count {}, throughput {:.3} Gbps",
        duration,
        batch_count,
        (batch_count * args.batch_size as usize * args.block_size as usize) as f64 / duration / 1e9
    );

    unsafe {
        engine.unregister_local_memory(pool.as_void_ptr())?;
    }

    Ok(())
}

fn target(args: Args) -> Result<()> {
    let local = get_host_ip()?;
    let engine = TransferEngine::initialize(
        &format!("{local}:12345"),
        &args.metadata_server,
        &args.protocol,
        "",
    )?;

    let dram_buffer_size = 1 << 30;
    let addr = allocate_memory_pool(dram_buffer_size);

    unsafe {
        engine.register_local_memory(addr as *mut _, dram_buffer_size, "cpu:0")?;
    }

    loop {
        thread::sleep(Duration::from_secs(1));
    }

    #[allow(unreachable_code)]
    {
        unsafe {
            engine.unregister_local_memory(addr as *mut _)?;
            free_memory_pool(addr, dram_buffer_size);
        }
        Ok(())
    }
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!("args: {:?}", args);

    let host_ip = get_host_ip().unwrap();
    info!("Host IP: {}", host_ip);

    match args.mode.as_str() {
        "initiator" => {
            initiator(args).unwrap();
        }
        "target" => {
            target(args).unwrap();
        }
        _ => {
            error!("Unsupported mode: must be 'initiator' or 'target'");
            std::process::exit(1);
        }
    }
}
