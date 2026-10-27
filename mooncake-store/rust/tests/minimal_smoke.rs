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

use std::ffi::c_void;
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

/// A client over the environment's store, or `None` when the integration
/// run is not enabled.
fn client() -> Result<Option<MooncakeStore>, Box<dyn std::error::Error>> {
    if !should_run_integration() {
        eprintln!("skipping Mooncake Store Rust smoke test; set MC_RUST_STORE_RUN_INTEGRATION=1 to enable");
        return Ok(None);
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
    Ok(Some(store))
}

#[test]
fn multi_buffer_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let Some(store) = client()? else {
        return Ok(());
    };

    // Two objects, each written from two slices of one registered buffer and
    // read back into three slices of another: the value is the slices' bytes
    // in order, whatever the split.
    let value: Vec<u8> = (0..4096u32).map(|i| (i * 7 % 251) as u8).collect();
    let mut src = vec![0u8; 2 * value.len()];
    src[..value.len()].copy_from_slice(&value);
    src[value.len()..].copy_from_slice(&value);
    let mut dst = vec![0u8; src.len()];
    unsafe {
        store.register_buffer(src.as_mut_ptr() as *mut c_void, src.len())?;
        store.register_buffer(dst.as_mut_ptr() as *mut c_void, dst.len())?;
    }

    let key = unique_key();
    let keys = [format!("{key}-a"), format!("{key}-b")];
    let keys: Vec<&str> = keys.iter().map(String::as_str).collect();
    let at = |buf: &mut [u8], off: usize| buf[off..].as_mut_ptr() as *mut c_void;
    let n = value.len();
    let src_a = [(at(&mut src, 0), 1000), (at(&mut src, 1000), n - 1000)];
    let src_b = [(at(&mut src, n), 4000), (at(&mut src, n + 4000), n - 4000)];
    let put = unsafe { store.batch_put_from_multi_buffers(&keys, &[&src_a, &src_b], None)? };
    assert_eq!(put, vec![0, 0]);

    let dst_a = [
        (at(&mut dst, 0), 100),
        (at(&mut dst, 100), 2000),
        (at(&mut dst, 2100), n - 2100),
    ];
    let dst_b = [(at(&mut dst, n), n - 1), (at(&mut dst, 2 * n - 1), 1)];
    let got = unsafe { store.batch_get_into_multi_buffers(&keys, &[&dst_a, &dst_b], false)? };
    assert_eq!(got, vec![n as i64, n as i64]);
    assert_eq!(dst, src);

    for k in &keys {
        store.remove(k, true)?;
    }
    unsafe {
        store.unregister_buffer(src.as_mut_ptr() as *mut c_void)?;
        store.unregister_buffer(dst.as_mut_ptr() as *mut c_void)?;
    }
    Ok(())
}
