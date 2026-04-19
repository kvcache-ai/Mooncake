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

// Rust sample matching the customer's usage pattern on AWS P6-B300:
//   - Target node: per-NUMA registers many MRs (e.g., 2 NUMA × N × size),
//     each registration hits the NICs affiliated with that NUMA via
//     location "cpu:<numa>".  This mirrors "16 NICs × 1000 × 10GB".
//   - Initiator node: opens the remote segment and reads/writes against a
//     specific (numa, buffer_index, offset) tuple on the target.  The
//     initiator's local buffer is registered on a chosen source-numa, so
//     only that NUMA's NICs are exercised for the transfer.
//
// Notes:
//   * In Mooncake's EFA transport, NIC selection happens on the initiator
//     side based on the local source buffer's topology entry.  There is no
//     per-request "pick NIC N" switch at the public API — use source-numa.
//   * This sample scales buffers down by default so it fits a single box.
//     To replicate the customer's 1000×10GB pattern pass
//     --buffers-per-numa 1000 --buffer-size-mb 10240 (needs ≥320GB RAM).

mod memory_pool;
mod transfer_engine;

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use dns_lookup::{get_hostname, getaddrinfo, AddrInfoHints, SockType};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::net::IpAddr;
use tracing::{error, info};
use transfer_engine::{
    BufferEntry, OpcodeEnum, TransferEngine, TransferRequest, TransferStatusEnum,
};

#[derive(Parser, Debug)]
#[command(version, about = "Per-NIC multi-MR registration (customer pattern)", long_about = None)]
pub struct Args {
    #[clap(long, default_value_t = String::from("P2PHANDSHAKE"),
           help = "Metadata backend URI (P2PHANDSHAKE recommended for EFA)")]
    pub metadata_server: String,

    #[clap(long, default_value_t = String::from("target"),
           value_parser = ["initiator", "target"],
           help = "Run mode")]
    pub mode: String,

    #[clap(long, default_value_t = String::from("read"),
           value_parser = ["read", "write"], help = "Operation (initiator only)")]
    pub operation: String,

    #[clap(long, default_value_t = 12345, help = "Local RPC port")]
    pub rpc_port: u64,

    // --- target-only ---
    #[clap(long, default_value_t = 2, help = "[target] number of NUMA nodes to register on")]
    pub num_numa: u32,

    #[clap(long, default_value_t = 8,
           help = "[target] MR count per NUMA (customer uses 1000)")]
    pub buffers_per_numa: u32,

    #[clap(long, default_value_t = 256,
           help = "[target] Per-MR size in MB (customer uses 10240)")]
    pub buffer_size_mb: u64,

    // --- initiator-only ---
    #[clap(long, default_value_t = String::from(""),
           help = "[initiator] Target segment ID (e.g. 172.31.57.234:12345)")]
    pub segment_id: String,

    #[clap(long, default_value_t = 0,
           help = "[initiator] Source NUMA on local — picks which NIC set to use")]
    pub source_numa: u32,

    #[clap(long, default_value_t = 0,
           help = "[initiator] Which remote NUMA region to read from")]
    pub target_numa: u32,

    #[clap(long, default_value_t = 0,
           help = "[initiator] Which buffer index within the target NUMA region")]
    pub target_buffer_index: u32,

    #[clap(long, default_value_t = 0,
           help = "[initiator] Base addr of target NUMA 0 (copy from target log)")]
    pub target_base_numa0: u64,

    #[clap(long, default_value_t = 0,
           help = "[initiator] Base addr of target NUMA 1 (copy from target log)")]
    pub target_base_numa1: u64,

    #[clap(long, default_value_t = 64, help = "[initiator] Batch size")]
    pub batch_size: i32,

    #[clap(long, default_value_t = 1 << 20,
           help = "[initiator] Per-request block size in bytes (1MB default)")]
    pub block_size: i32,

    #[clap(long, default_value_t = 10, help = "[initiator] Test duration seconds")]
    pub duration: i32,

    #[clap(long, default_value_t = 4, help = "[initiator] Submission threads")]
    pub threads: i32,
}

fn get_host_ip() -> Result<String> {
    let hostname = get_hostname().map_err(|e| anyhow!("Failed to get hostname: {}", e))?;
    let hints = AddrInfoHints { socktype: SockType::Stream.into(), ..AddrInfoHints::default() };
    let addrs = getaddrinfo(Some(&hostname), None, Some(hints))
        .map_err(|e| anyhow!("getaddrinfo failed: {:?}", e))?
        .collect::<Result<Vec<_>, _>>()?;
    for a in addrs {
        if let IpAddr::V4(ip) = a.sockaddr.ip() {
            let s = ip.to_string();
            if s != "127.0.0.1" && s != "127.0.1.1" {
                return Ok(s);
            }
        }
    }
    bail!("No non-loopback IPv4 found")
}

static RUNNING: AtomicBool = AtomicBool::new(true);

// ---------------- target ----------------

fn run_target(args: Args) -> Result<()> {
    let engine = TransferEngine::new(
        &args.metadata_server,
        &get_host_ip()?,
        args.rpc_port,
    )?;
    engine.discover_topology()?;
    engine.install_transport("efa")?;

    let per_mr_bytes = (args.buffer_size_mb as usize) * 1024 * 1024;
    let per_numa_bytes = per_mr_bytes * args.buffers_per_numa as usize;
    let total_gb = (per_numa_bytes as f64 * args.num_numa as f64) / (1024.0 * 1024.0 * 1024.0);

    info!(
        "[target] layout: {} NUMA × {} MRs × {} MB = {:.2} GB total",
        args.num_numa, args.buffers_per_numa, args.buffer_size_mb, total_gb
    );

    // Keep allocations alive for the duration of the process.
    let mut pools: Vec<memory_pool::MemoryPool> = Vec::new();

    for n in 0..args.num_numa {
        let pool = memory_pool::MemoryPool::new(per_numa_bytes);
        let base_ptr = pool.offset(0);
        info!(
            "[target] NUMA {} base addr: 0x{:x}  (copy as --target-base-numa{} on initiator)",
            n, base_ptr as usize, n
        );

        // Build a BufferEntry list pointing at equally-sized sub-ranges of
        // the pool.  Each entry becomes one independent MR on the device.
        let mut entries: Vec<BufferEntry> = Vec::with_capacity(args.buffers_per_numa as usize);
        for i in 0..args.buffers_per_numa {
            let off = (i as usize) * per_mr_bytes;
            entries.push(BufferEntry {
                addr: unsafe { base_ptr.add(off) } as *mut _,
                length: per_mr_bytes as u64,
            });
        }

        let location = format!("cpu:{}", n);
        let t0 = Instant::now();
        engine.register_local_memory_batch(&entries, &location)?;
        let elapsed = t0.elapsed().as_secs_f64();
        info!(
            "[target] NUMA {} registered {} × {} MB on '{}' in {:.2}s ({:.1} MR/s)",
            n,
            args.buffers_per_numa,
            args.buffer_size_mb,
            location,
            elapsed,
            args.buffers_per_numa as f64 / elapsed.max(1e-9)
        );

        pools.push(pool);
    }

    info!("[target] ready — sleeping forever; Ctrl-C to exit");
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

// ---------------- initiator ----------------

fn initiator_worker(
    args: Arc<Args>,
    engine: Arc<TransferEngine>,
    segment_id: i32,
    thread_id: i32,
    pool: Arc<memory_pool::MemoryPool>,
    remote_base: u64,
    total_batches: Arc<AtomicUsize>,
) -> Result<()> {
    let opcode = match args.operation.as_str() {
        "read" => OpcodeEnum::Read,
        "write" => OpcodeEnum::Write,
        _ => bail!("unsupported operation"),
    };

    while RUNNING.load(Ordering::SeqCst) {
        let batch_id = engine.allocate_batch_id(args.batch_size as usize)?;
        let mut requests: Vec<TransferRequest> = Vec::with_capacity(args.batch_size as usize);
        for i in 0..args.batch_size {
            let slot = (i * args.threads + thread_id) as i64;
            let src_off = (args.block_size as i64) * slot;
            let tgt_off = remote_base + (args.block_size as u64) * (slot as u64);
            requests.push(TransferRequest {
                opcode,
                source: pool.offset(src_off as isize) as *mut _,
                target_id: segment_id,
                target_offset: tgt_off,
                length: args.block_size as u64,
            });
        }
        engine.submit_transfer(batch_id, &mut requests)?;

        for task_id in 0..args.batch_size {
            loop {
                let (status, _) = engine.get_transfer_status(batch_id, task_id as u64)?;
                if status == TransferStatusEnum::Completed as i32
                    || status == TransferStatusEnum::Failed as i32
                {
                    break;
                }
            }
        }
        engine.free_batch_id(batch_id)?;
        total_batches.fetch_add(1, Ordering::SeqCst);
    }
    Ok(())
}

fn run_initiator(args: Args) -> Result<()> {
    if args.segment_id.is_empty() {
        bail!("--segment-id is required for initiator mode");
    }
    let target_base = match args.target_numa {
        0 => args.target_base_numa0,
        1 => args.target_base_numa1,
        _ => bail!("only --target-numa 0 or 1 is supported"),
    };
    if target_base == 0 {
        bail!("--target-base-numa{} is required (read from target log)", args.target_numa);
    }

    let per_mr_bytes = (args.buffer_size_mb as u64) * 1024 * 1024;
    let remote_base = target_base + (args.target_buffer_index as u64) * per_mr_bytes;

    let args = Arc::new(args);
    let engine = Arc::new(TransferEngine::new(
        &args.metadata_server,
        &get_host_ip()?,
        args.rpc_port,
    )?);
    engine.discover_topology()?;
    engine.install_transport("efa")?;

    // Local source buffer: large enough for batch_size × block_size × threads.
    let local_bytes: usize = (args.batch_size as usize)
        .saturating_mul(args.block_size as usize)
        .saturating_mul(args.threads as usize)
        .max(1 << 20);
    let pool = Arc::new(memory_pool::MemoryPool::new(local_bytes));
    let source_loc = format!("cpu:{}", args.source_numa);
    info!(
        "[initiator] local buffer: {} bytes on '{}', remote target: NUMA {} buf {} base 0x{:x}",
        local_bytes, source_loc, args.target_numa, args.target_buffer_index, remote_base
    );
    engine.register_local_memory(pool.offset(0) as *mut _, local_bytes, &source_loc)?;

    let segment_id = engine.open_segment(args.segment_id.clone())?;
    let total_batches = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let mut workers = vec![];
    for t in 0..args.threads {
        let a = args.clone();
        let e = engine.clone();
        let p = pool.clone();
        let c = total_batches.clone();
        workers.push(thread::spawn(move || {
            initiator_worker(a, e, segment_id, t, p, remote_base, c)
        }));
    }
    thread::sleep(Duration::from_secs(args.duration as u64));
    RUNNING.store(false, Ordering::SeqCst);
    for w in workers {
        w.join().unwrap()?;
    }

    let secs = start.elapsed().as_secs_f64();
    let batches = total_batches.load(Ordering::SeqCst);
    let bytes = batches * args.batch_size as usize * args.block_size as usize;
    info!(
        "[initiator] duration {:.2}s, batches {}, bytes {}, throughput {:.3} Gbps",
        secs,
        batches,
        bytes,
        (bytes as f64 * 8.0) / secs / 1e9
    );

    engine.unregister_local_memory(pool.offset(0) as *mut _)?;
    Ok(())
}

fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    info!("args: {:?}", args);
    match args.mode.as_str() {
        "target" => run_target(args).unwrap(),
        "initiator" => run_initiator(args).unwrap(),
        _ => {
            error!("mode must be 'target' or 'initiator'");
            std::process::exit(1);
        }
    }
}
