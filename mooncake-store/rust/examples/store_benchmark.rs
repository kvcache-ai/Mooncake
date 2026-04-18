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

use std::env;
use std::error::Error;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mooncake_store::MooncakeStore;

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn mib_per_sec(total_bytes: usize, seconds: f64) -> f64 {
    if seconds <= f64::EPSILON {
        return 0.0;
    }
    total_bytes as f64 / (1024.0 * 1024.0) / seconds
}

fn ops_per_sec(total_ops: usize, seconds: f64) -> f64 {
    if seconds <= f64::EPSILON {
        return 0.0;
    }
    total_ops as f64 / seconds
}

fn main() -> Result<(), Box<dyn Error>> {
    let iterations = env_usize("MC_RUST_BENCH_ITERATIONS", 32);
    let value_size = env_usize("MC_RUST_BENCH_VALUE_SIZE", 4096);
    let warmup = env_usize("MC_RUST_BENCH_WARMUP", 4);

    let store = MooncakeStore::new()?;
    store.setup(
        &env_or("MC_RUST_STORE_LOCAL_HOSTNAME", "127.0.0.1"),
        &env_or("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"),
        512 << 20,
        128 << 20,
        &env_or("MC_RUST_STORE_PROTOCOL", "tcp"),
        &env_or("MC_RUST_STORE_DEVICE_NAME", ""),
        &env_or("MC_RUST_STORE_MASTER_ADDR", "127.0.0.1:50051"),
    )?;

    store.health_check()?;

    let unique_prefix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let payload = vec![b'x'; value_size];

    for index in 0..warmup {
        let key = format!("rust-bench-warmup-{unique_prefix}-{index}");
        store.put(&key, &payload, None)?;
        let received = store.get(&key)?;
        assert_eq!(received.len(), payload.len());
        store.remove(&key, false)?;
    }

    let keys: Vec<String> = (0..iterations)
        .map(|index| format!("rust-bench-{unique_prefix}-{index}"))
        .collect();

    let put_start = Instant::now();
    for key in &keys {
        store.put(key, &payload, None)?;
    }
    let put_seconds = put_start.elapsed().as_secs_f64();

    let get_start = Instant::now();
    for key in &keys {
        let received = store.get(key)?;
        assert_eq!(received.len(), payload.len());
    }
    let get_seconds = get_start.elapsed().as_secs_f64();

    let remove_start = Instant::now();
    for key in &keys {
        store.remove(key, false)?;
    }
    let remove_seconds = remove_start.elapsed().as_secs_f64();

    let total_bytes = iterations * payload.len();
    println!("put_seconds={put_seconds:.6}");
    println!("put_mib_per_sec={:.2}", mib_per_sec(total_bytes, put_seconds));
    println!("get_seconds={get_seconds:.6}");
    println!("get_mib_per_sec={:.2}", mib_per_sec(total_bytes, get_seconds));
    println!("remove_seconds={remove_seconds:.6}");
    println!("remove_ops_per_sec={:.2}", ops_per_sec(iterations, remove_seconds));

    Ok(())
}use std::time::{Instant, SystemTime, UNIX_EPOCH};

use mooncake_store::MooncakeStore;

fn env_or_default<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

fn unique_prefix() -> String {
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("rust-bench-{}-{timestamp_ns}", std::process::id())
}

fn mib_per_second(total_bytes: usize, elapsed: std::time::Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    total_bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iterations = env_or_default("MC_RUST_BENCH_ITERATIONS", 64usize);
    let payload_size = env_or_default("MC_RUST_BENCH_VALUE_SIZE", 64 * 1024usize);
    let warmup_iterations = env_or_default("MC_RUST_BENCH_WARMUP", 4usize);
    let metadata_server = std::env::var("MC_METADATA_SERVER")
        .unwrap_or_else(|_| "http://127.0.0.1:8080/metadata".to_string());
    let master_server_addr = std::env::var("MC_RUST_STORE_MASTER_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let local_hostname =
        std::env::var("MC_RUST_STORE_LOCAL_HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
    let protocol = std::env::var("MC_RUST_STORE_PROTOCOL").unwrap_or_else(|_| "tcp".to_string());
    let device_name = std::env::var("MC_RUST_STORE_DEVICE_NAME").unwrap_or_default();
    let global_segment_size = env_or_default("MC_RUST_STORE_GLOBAL_SEGMENT_SIZE", 512u64 << 20);
    let local_buffer_size = env_or_default("MC_RUST_STORE_LOCAL_BUFFER_SIZE", 128u64 << 20);

    let store = MooncakeStore::new()?;
    store.setup(
        &local_hostname,
        &metadata_server,
        global_segment_size,
        local_buffer_size,
        &protocol,
        &device_name,
        &master_server_addr,
    )?;
    store.health_check()?;

    let prefix = unique_prefix();
    let payload: Vec<u8> = (0..payload_size).map(|index| (index % 251) as u8).collect();

    for iteration in 0..warmup_iterations {
        let key = format!("{prefix}-warmup-{iteration}");
        store.put(&key, &payload, None)?;
        let fetched = store.get(&key)?;
        assert_eq!(fetched, payload, "warmup round-trip mismatch");
        store.remove(&key, false)?;
    }

    let keys: Vec<String> = (0..iterations)
        .map(|iteration| format!("{prefix}-key-{iteration}"))
        .collect();

    let put_start = Instant::now();
    for key in &keys {
        store.put(key, &payload, None)?;
    }
    let put_elapsed = put_start.elapsed();

    let get_start = Instant::now();
    for key in &keys {
        let fetched = store.get(key)?;
        assert_eq!(fetched, payload, "benchmark round-trip mismatch for {key}");
    }
    let get_elapsed = get_start.elapsed();

    let remove_start = Instant::now();
    for key in &keys {
        store.remove(key, false)?;
    }
    let remove_elapsed = remove_start.elapsed();

    let total_bytes = iterations * payload.len();
    println!("Mooncake Store Rust benchmark");
    println!("iterations={iterations}");
    println!("payload_size_bytes={payload_size}");
    println!("put_seconds={:.6}", put_elapsed.as_secs_f64());
    println!("put_mib_per_sec={:.2}", mib_per_second(total_bytes, put_elapsed));
    println!("get_seconds={:.6}", get_elapsed.as_secs_f64());
    println!("get_mib_per_sec={:.2}", mib_per_second(total_bytes, get_elapsed));
    println!("remove_seconds={:.6}", remove_elapsed.as_secs_f64());
    println!(
        "remove_ops_per_sec={:.2}",
        if remove_elapsed.is_zero() {
            0.0
        } else {
            iterations as f64 / remove_elapsed.as_secs_f64()
        }
    );

    Ok(())
}
