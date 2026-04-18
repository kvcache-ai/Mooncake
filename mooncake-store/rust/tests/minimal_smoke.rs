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
use std::time::{SystemTime, UNIX_EPOCH};

use mooncake_store::MooncakeStore;

fn env_flag(name: &str) -> bool {
    matches!(
        env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_or(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

#[test]
fn smoke_round_trip() {
    if !env_flag("MC_RUST_STORE_RUN_INTEGRATION") {
        eprintln!("skipping Mooncake Store Rust integration smoke test");
        return;
    }

    let store = MooncakeStore::new().expect("create store handle");
    store
        .setup(
            &env_or("MC_RUST_STORE_LOCAL_HOSTNAME", "127.0.0.1"),
            &env_or("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata"),
            512 << 20,
            128 << 20,
            &env_or("MC_RUST_STORE_PROTOCOL", "tcp"),
            &env_or("MC_RUST_STORE_DEVICE_NAME", ""),
            &env_or("MC_RUST_STORE_MASTER_ADDR", "127.0.0.1:50051"),
        )
        .expect("setup store");

    store.health_check().expect("health check");

    let unique_suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let key = format!("rust-smoke-{unique_suffix}");
    let value = b"hello from rust smoke test";

    store.put(&key, value, None).expect("put value");
    assert!(store.is_exist(&key).expect("check existence"));
    assert_eq!(store.get_size(&key).expect("get size") as usize, value.len());
    assert_eq!(store.get(&key).expect("get value"), value);
    assert!(!store.get_hostname().expect("get hostname").is_empty());

    store.remove(&key, false).expect("remove key");
    assert!(!store.is_exist(&key).expect("check absence"));
}use std::time::{SystemTime, UNIX_EPOCH};

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

    store.remove(&key, false)?;
    assert!(!store.is_exist(&key)?);

    Ok(())
}
