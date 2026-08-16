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

//! Env-gated smoke test. Requires a running metadata server.
//!
//! ```bash
//! MC_RUST_TE_RUN_INTEGRATION=1 \
//! MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
//! cargo test --test minimal_smoke -- --nocapture
//! ```

use transfer_engine_rust::{MemoryPool, TransferEngine, WILDCARD_LOCATION};

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn should_run_integration() -> bool {
    matches!(
        std::env::var("MC_RUST_TE_RUN_INTEGRATION").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE")
    )
}

#[test]
fn initialize_and_register_local_buffer() -> Result<(), Box<dyn std::error::Error>> {
    if !should_run_integration() {
        eprintln!(
            "skipping Transfer Engine Rust smoke test; set MC_RUST_TE_RUN_INTEGRATION=1 to enable"
        );
        return Ok(());
    }

    let metadata_server = env_or_default("MC_METADATA_SERVER", "http://127.0.0.1:8080/metadata");
    let local_hostname = env_or_default("MC_RUST_TE_LOCAL_HOSTNAME", "127.0.0.1:12345");
    let protocol = env_or_default("MC_RUST_TE_PROTOCOL", "tcp");

    let engine = TransferEngine::initialize(&local_hostname, &metadata_server, &protocol, "")?;
    let pool = MemoryPool::new(4096);
    unsafe {
        engine.register_local_memory(pool.as_void_ptr(), pool.len(), WILDCARD_LOCATION)?;
        engine.unregister_local_memory(pool.as_void_ptr())?;
    }
    let _ = engine.local_ip_and_port();
    Ok(())
}
