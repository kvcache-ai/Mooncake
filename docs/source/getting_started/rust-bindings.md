# Rust Bindings

Mooncake provides Rust bindings for two components:

| Crate | Path | Purpose |
|-------|------|---------|
| `mooncake_store` | `mooncake-store/rust/` | Distributed KV cache store client |
| Transfer Engine (Rust bench) | `mooncake-transfer-engine/rust/` | Transfer Engine benchmark / integration |

## Mooncake Store Rust Bindings

### Overview

`mooncake_store` is a Rust crate that wraps the Mooncake Store C ABI (`store_c.h`) using `bindgen`-generated bindings. It exposes a safe, idiomatic Rust API for storing and retrieving byte slices (KV cache objects) over RDMA.

### Build Requirements

The crate requires the compiled Mooncake Store shared library and its public headers. These are produced by the standard CMake build:

```bash
mkdir build && cd build
cmake .. \
    -DWITH_STORE_RUST=ON \   # ON by default
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build . --target build_mooncake_store_rust
```

The CMake target sets the necessary Rust environment variables and runs `cargo build` automatically.

If you want to build the crate independently (e.g., in your own workspace after a CMake install), set:

```bash
export MOONCAKE_STORE_LIB_DIR=/path/to/build/mooncake-store
export MOONCAKE_STORE_INCLUDE_DIR=/path/to/Mooncake/mooncake-store/include
cargo build
```

### Adding as a Dependency

After building, you can depend on the crate from a local path:

```toml
# Cargo.toml
[dependencies]
mooncake_store = { path = "/path/to/Mooncake/mooncake-store/rust" }
```

### API Reference

```rust
use mooncake_store::{MooncakeStore, ReplicateConfig, StoreError};

// 1. Create a store handle
let store = MooncakeStore::new()?;

// 2. Connect to a running Mooncake master
store.setup(
    "node1",                           // local_hostname
    "http://10.0.0.1:8080/metadata",   // metadata_server
    512 << 20,                         // global_segment_size (512 MiB)
    128 << 20,                         // local_buffer_size  (128 MiB)
    "rdma",                            // protocol: "tcp" | "rdma" | …
    "mlx5_0",                          // device_name ("" for auto)
    "10.0.0.1:50051",                  // mooncake_master address
)?;

// 3. Store a value
store.put("my-key", b"hello, mooncake!", None)?;

// 4. Store with replication options
let config = ReplicateConfig {
    replica_num: 2,
    with_soft_pin: true,
    with_hard_pin: false,
    preferred_segments: vec!["seg-0".into()],
};
store.put("replicated-key", b"replicated value", Some(&config))?;

// 5. Check existence
let exists: bool = store.is_exist("my-key")?;

// 6. Get size
let size: u64 = store.get_size("my-key")?;

// 7. Retrieve value
let data: Vec<u8> = store.get("my-key")?;
assert_eq!(data, b"hello, mooncake!");

// 8. Remove
store.remove("my-key", /*force=*/false)?;
```

### Error Handling

All fallible operations return `Result<T, StoreError>`. The main variants are:

| Variant | Meaning |
|---------|---------|
| `StoreError::OperationFailed(code)` | The underlying C library returned a non-zero status code |
| `StoreError::SetupError(msg)` | Store setup / connection failed |
| `StoreError::InvalidArgument(msg)` | A null pointer or invalid argument was passed |

A missing key returns `StoreError::OperationFailed(code)` where `code` matches the Mooncake error table in [`mooncake-store/include/types.h`](../../../mooncake-store/include/types.h).

### Running the Example

```bash
# Start metadata server
cd mooncake-transfer-engine/example/http-metadata-server-python
pip install aiohttp && python bootstrap_server.py &

# Start mooncake_master
./build/mooncake_master \
    --enable_http_metadata_server=true \
    --http_metadata_server_port=8080 \
    --rpc_port=50051 &

# Run the bundled example
cd build
cargo run --example basic_usage --manifest-path ../mooncake-store/rust/Cargo.toml
```

Or via CMake:

```bash
cd build
cmake --build . --target build_mooncake_store_rust
# The basic_usage binary is at:
./mooncake-store/rust/target/debug/examples/basic_usage
```

---

## Transfer Engine Rust Bindings

### Overview

The Transfer Engine Rust bindings live under `mooncake-transfer-engine/rust/`. They provide a Rust interface to `TransferEngine` and include a Rust port of the `transfer_engine_bench` benchmark tool.

### Build

```bash
mkdir build && cd build
cmake .. \
    -DWITH_RUST_EXAMPLE=ON \
    -DUSE_CUDA=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build . --target rust_transfer_engine_bench
```

### Running the Benchmark

The Rust benchmark mirrors `transfer_engine_bench.cpp` in behaviour:

```bash
# Target node (receiver)
./build/mooncake-transfer-engine/rust/target/release/transfer_engine_bench \
    --mode=target \
    --protocol=rdma \
    --metadata_server=P2PHANDSHAKE

# Initiator node (sender) — replace <target>:<port> from the target log
./build/mooncake-transfer-engine/rust/target/release/transfer_engine_bench \
    --mode=initiator \
    --protocol=rdma \
    --metadata_server=P2PHANDSHAKE \
    --segment_id=<target>:<port> \
    --operation=write \
    --duration=10 \
    --threads=8
```

---

## CI Integration

The Mooncake CI pipeline validates both Rust crates on every pull request:

```yaml
# Relevant CI steps (simplified from .github/workflows/ci.yml)
- uses: dtolnay/rust-toolchain@stable
- run: |
    export MOONCAKE_STORE_LIB_DIR=$BUILD_DIR/mooncake-store
    export MOONCAKE_STORE_INCLUDE_DIR=$SRC_DIR/mooncake-store/include
    cargo check --manifest-path mooncake-store/rust/Cargo.toml
```

Both crates must pass `cargo check` (and `cargo clippy`) before merging.

## See Also

- [Mooncake Store Python API](../python-api-reference/mooncake-store)
- [Transfer Engine C++ API](../design/transfer-engine/cpp-api)
- [Mooncake Store Deployment Guide](../deployment/mooncake-store-deployment-guide)
