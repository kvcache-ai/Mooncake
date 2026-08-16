# Transfer Engine Rust Bindings

This directory contains high-performance Rust bindings for the Mooncake
Transfer Engine C API (`transfer_engine_c.h`), plus a throughput-benchmark
example analogous to `example/transfer_engine_bench.cpp`.

The crate (`transfer_engine_rust`) is a **library**. Hot-path types
(`TransferRequest`, `BufferEntry`, `TransferStatus`) are `#[repr(C)]` and
layout-checked against the C ABI, so `submit_transfer` / `get_transfer_status`
pass slices and stack structs to C with no heap allocation on the Rust side.

## Prerequisites

Install the Rust toolchain and libclang (bindgen):

```bash
sudo apt-get install libclang-dev clang
```

## Build With CMake

```bash
cmake -S . -B build -G Ninja -DWITH_RUST_EXAMPLE=ON
cmake --build build --target build_transfer_engine_rust
cmake --build build --target build_transfer_engine_rust_example
cmake --build build --target build_transfer_engine_rust_tests
```

## Build With Cargo

```bash
cd mooncake-transfer-engine/rust

export MOONCAKE_BUILD_DIR=/path/to/Mooncake/build
export MOONCAKE_TE_LIB_DIR=$MOONCAKE_BUILD_DIR/mooncake-transfer-engine/src
export MOONCAKE_TE_INCLUDE_DIR=/path/to/Mooncake/mooncake-transfer-engine/include

cargo test --lib --release
cargo build --examples --release
```

If Transfer Engine was built with etcd or CUDA, set `MOONCAKE_WITH_ETCD=1`
and/or `MOONCAKE_WITH_CUDA=1` so `build.rs` pulls in the extra link libraries.

At runtime, add CMake outputs to `LD_LIBRARY_PATH` if they are not installed:

```bash
export LD_LIBRARY_PATH=$MOONCAKE_BUILD_DIR/mooncake-asio:\
$MOONCAKE_BUILD_DIR/mooncake-transfer-engine/src:\
$MOONCAKE_BUILD_DIR/mooncake-transfer-engine/src/common/base:\
${LD_LIBRARY_PATH:-}
```

## Quick start

```rust
use transfer_engine_rust::{MemoryPool, TransferEngine, TransferRequest, WILDCARD_LOCATION};

fn main() -> Result<(), transfer_engine_rust::EngineError> {
    let engine = TransferEngine::initialize(
        "127.0.0.1:12345",
        "http://127.0.0.1:8080/metadata",
        "tcp",
        "",
    )?;

    let pool = MemoryPool::new(1 << 20);
    unsafe {
        engine.register_local_memory(pool.as_void_ptr(), pool.len(), WILDCARD_LOCATION)?;

        let seg = engine.open_segment("peer:12345")?;
        let req = TransferRequest::write(pool.as_void_ptr(), seg, 0, 4096);
        engine.submit_and_wait(&[req], None)?;

        engine.unregister_local_memory(pool.as_void_ptr())?;
    }
    Ok(())
}
```

`initialize` matches the Python `TransferEngine.initialize(local_hostname,
metadata_server, protocol, device_name)` shape. The lower-level
`TransferEngine::new` / `create` constructors map 1:1 onto
`createTransferEngine`.

Device filtering is not part of the C ABI; set `MC_TE_FILTERS` if you need to
restrict NICs.

## Integration smoke test

```bash
cd mooncake-transfer-engine/rust

MC_RUST_TE_RUN_INTEGRATION=1 \
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
MC_RUST_TE_LOCAL_HOSTNAME=127.0.0.1:12345 \
MC_RUST_TE_PROTOCOL=tcp \
cargo test --test minimal_smoke -- --nocapture
```

## Benchmark example

```bash
cargo run --example transfer_engine_bench --release -- --mode target
cargo run --example transfer_engine_bench --release -- \
  --mode initiator --segment-id <target-ip> --protocol tcp
```
