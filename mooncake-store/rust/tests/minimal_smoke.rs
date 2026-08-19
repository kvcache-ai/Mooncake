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

use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_store::MooncakeStore;

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn should_run_integration() -> bool {
    matches!(
        std::env::var("MC_RUST_STORE_RUN_INTEGRATION").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn unique_key() -> String {
    let timestamp_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("rust-smoke-{}-{timestamp_ns}", std::process::id())
}

#[test]
fn smoke_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    if !should_run_integration() {
        eprintln!("skipping Mooncake Store Rust smoke test; set MC_RUST_STORE_RUN_INTEGRATION=1 to enable");
        return Ok(());
    }

    let metadata_server = env_or_default("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata");
    let master_server_addr = env_or_default("MC_RUST_STORE_MASTER_ADDR", "127.0.0.1:50051");
    let local_hostname = env_or_default("MC_RUST_STORE_LOCAL_HOSTNAME", "localhost");
    let protocol = env_or_default("MC_RUST_STORE_PROTOCOL", "tcp");
    let device_name = std::env::var("MC_RUST_STORE_DEVICE_NAME").unwrap_or_default();

    let store = MooncakeStore::new()?;
    store.setup(
        &local_hostname,
        &metadata_server,
        512 << 20,
        128 << 20,
        &protocol,
        &device_name,
        &master_server_addr,
    )?;
    store.health_check()?;

    let key = unique_key();
    let value = format!("mooncake-rust-smoke-value-{key}").into_bytes();

    store.put(&key, &value, None)?;
    assert!(store.is_exist(&key)?);
    assert_eq!(store.get_size(&key)? as usize, value.len());
    assert_eq!(store.get(&key)?, value);
    assert!(!store.get_hostname()?.is_empty());

    store.remove(&key, true)?;
    assert!(!store.is_exist(&key)?);

    Ok(())
}
