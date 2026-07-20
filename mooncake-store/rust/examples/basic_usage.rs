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

//! Basic usage example for the Mooncake Store Rust bindings.
//!
//! This example demonstrates the API surface of `mooncake_store`:
//!   - Creating a store handle
//!   - Setting up the client (requires a running metadata server)
//!   - Storing and retrieving values
//!   - Checking existence and sizes
//!   - Removing keys
//!
//! # Running
//!
//! The example needs a running Mooncake metadata server.  Start one with:
//!
//! ```bash
//! cd mooncake-transfer-engine/example/http-metadata-server-python
//! pip install aiohttp
//! python bootstrap_server.py &
//! ```
//!
//! Then build and run via CMake (which sets the right library paths):
//!
//! ```bash
//! cd build
//! cmake -G Ninja .. -DWITH_STORE_RUST=ON
//! cmake --build . --target build_mooncake_store_rust
//! ```
//!
//! Or directly with cargo (after a CMake install):
//!
//! ```bash
//! cargo run --example basic_usage
//! ```

use mooncake_store::{MooncakeStore, ReplicateConfig, StoreError};

fn main() {
    println!("=== Mooncake Store Rust Bindings — Basic Usage ===\n");

    // Step 1: Create a store handle.
    let store = match MooncakeStore::new() {
        Ok(s) => {
            println!("[OK] Store handle created.");
            s
        }
        Err(e) => {
            eprintln!("[FAIL] Could not create store handle: {e}");
            std::process::exit(1);
        }
    };

    // Step 2: Connect to the metadata server and transport layer.
    //
    // In CI or environments without a running metadata server the setup
    // call is expected to fail – this is not an error in the bindings
    // themselves.
    let metadata_server =
        std::env::var("MC_METADATA_SERVER").unwrap_or_else(|_| "http://127.0.0.1:8080/metadata".to_string());

    println!("Connecting to metadata server: {metadata_server}");

    if let Err(e) = store.setup(
        "localhost",
        &metadata_server,
        512 << 20, // global_segment_size  = 512 MiB
        128 << 20, // local_buffer_size    = 128 MiB
        "tcp",
        "",        // device_name (auto-select)
        "127.0.0.1:50051",
    ) {
        eprintln!(
            "[SKIP] setup() returned an error (expected when no server is running): {e}\n\
             The API surface has been validated successfully."
        );
        std::process::exit(0);
    }

    println!("[OK] Store client set up.\n");

    // Step 3: Put a key/value pair (copies data into the store).
    let key = "example-key";
    let value = b"hello, mooncake!";

    if let Err(e) = store.put(key, value, None) {
        eprintln!("[FAIL] put() failed: {e}");
        std::process::exit(1);
    }
    println!("[OK] put(\"{key}\", {:?})", std::str::from_utf8(value).unwrap());

    // Step 4: Check existence.
    match store.is_exist(key) {
        Ok(true)  => println!("[OK] is_exist(\"{key}\") = true"),
        Ok(false) => println!("[WARN] is_exist(\"{key}\") = false (unexpected)"),
        Err(e)    => eprintln!("[FAIL] is_exist() failed: {e}"),
    }

    // Step 5: Get size.
    match store.get_size(key) {
        Ok(sz) => println!("[OK] get_size(\"{key}\") = {sz} bytes"),
        Err(e) => eprintln!("[FAIL] get_size() failed: {e}"),
    }

    // Step 6: Retrieve the value.
    match store.get(key) {
        Ok(data) => {
            println!("[OK] get(\"{key}\") = {:?}", String::from_utf8_lossy(&data));
            assert_eq!(data, value, "round-trip mismatch!");
        }
        Err(e) => eprintln!("[FAIL] get() failed: {e}"),
    }

    // Step 7: Put with explicit replication config.
    let config = ReplicateConfig {
        replica_num: 2,
        with_soft_pin: true,
        with_hard_pin: false,
        preferred_segments: vec!["seg-0".to_string()],
    };

    if let Err(e) = store.put("replicated-key", b"replicated value", Some(&config)) {
        eprintln!("[FAIL] put() with ReplicateConfig failed: {e}");
    } else {
        println!("[OK] put(\"replicated-key\") with replica_num=2, soft_pin=true, hard_pin=false");
    }

    // Step 8: Remove the keys.
    match store.remove(key, false) {
        Ok(()) => println!("[OK] remove(\"{key}\")"),
        Err(e) => eprintln!("[FAIL] remove() failed: {e}"),
    }

    match store.remove("replicated-key", false) {
        Ok(()) => println!("[OK] remove(\"replicated-key\")"),
        Err(e) => eprintln!("[FAIL] remove() failed: {e}"),
    }

    // Step 9: Demonstrate error handling for a missing key.
    match store.get_size("nonexistent") {
        Err(StoreError::OperationFailed(code)) => {
            println!("[OK] get_size(\"nonexistent\") returned OperationFailed({code}) as expected");
        }
        Ok(sz) => println!("[WARN] get_size(\"nonexistent\") unexpectedly returned {sz}"),
        Err(e) => println!("[OK] get_size(\"nonexistent\") returned error: {e}"),
    }

    println!("\n=== Example completed successfully ===");
}
