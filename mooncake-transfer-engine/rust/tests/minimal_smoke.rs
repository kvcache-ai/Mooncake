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

//! Env-gated Transfer Engine smoke test.
//!
//! Defaults to `P2PHANDSHAKE` so it can run without an HTTP metadata server.
//! CI sets `MC_METADATA_SERVER=http://127.0.0.1:8080/metadata`.
//!
//! ```bash
//! MC_RUST_TE_RUN_INTEGRATION=1 cargo test --test minimal_smoke -- --nocapture
//! ```

use std::time::Duration;

use transfer_engine_rust::{MemoryPool, TransferEngine, TransferRequest, WILDCARD_LOCATION};

const CHUNK: usize = 4096;
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn should_run_integration() -> bool {
    matches!(
        std::env::var("MC_RUST_TE_RUN_INTEGRATION").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

fn local_hostname() -> String {
    env_or_default(
        "MC_RUST_TE_LOCAL_HOSTNAME",
        &format!("127.0.0.1:{}", 20_000 + std::process::id() % 10_000),
    )
}

#[test]
fn tcp_loopback_write_and_read() -> Result<(), Box<dyn std::error::Error>> {
    if !should_run_integration() {
        eprintln!(
            "skipping Transfer Engine Rust smoke test; set MC_RUST_TE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }

    let metadata_server = env_or_default("MC_METADATA_SERVER", "P2PHANDSHAKE");
    let local_hostname = local_hostname();
    let protocol = env_or_default("MC_RUST_TE_PROTOCOL", "tcp");

    let engine = TransferEngine::initialize(&local_hostname, &metadata_server, &protocol, "")?;
    let advertised = engine.local_ip_and_port()?;
    assert!(
        !advertised.is_empty(),
        "local_ip_and_port must be non-empty after initialize"
    );

    // P2PHANDSHAKE rewrites the RPC port; HTTP metadata keeps the name we
    // registered. Match tcp_write_visibility_test.cpp.
    let segment_name = if metadata_server == "P2PHANDSHAKE" {
        advertised
    } else {
        local_hostname.clone()
    };

    let mut pool = MemoryPool::new(CHUNK * 2);
    pool.as_mut_slice()[..CHUNK].fill(0xA5);
    pool.as_mut_slice()[CHUNK..].fill(0x00);

    unsafe {
        engine.register_local_memory(pool.as_void_ptr(), pool.len(), WILDCARD_LOCATION)?;

        let segment = engine.open_segment(&segment_name)?;
        let src = pool.as_void_ptr();
        let dst = pool.offset(CHUNK) as *mut std::ffi::c_void;

        engine.submit_and_wait(
            &[TransferRequest::write(
                src,
                segment,
                dst as u64,
                CHUNK as u64,
            )],
            Some(TRANSFER_TIMEOUT),
        )?;
        assert_eq!(
            &pool.as_slice()[..CHUNK],
            &pool.as_slice()[CHUNK..],
            "loopback WRITE must copy the first 4KiB onto the second 4KiB"
        );

        pool.as_mut_slice()[..CHUNK].fill(0x00);
        engine.submit_and_wait(
            &[TransferRequest::read(
                src,
                segment,
                dst as u64,
                CHUNK as u64,
            )],
            Some(TRANSFER_TIMEOUT),
        )?;
        assert!(
            pool.as_slice()[..CHUNK].iter().all(|&b| b == 0xA5),
            "loopback READ must restore the pattern from the destination region"
        );

        engine.unregister_local_memory(pool.as_void_ptr())?;
    }
    Ok(())
}
