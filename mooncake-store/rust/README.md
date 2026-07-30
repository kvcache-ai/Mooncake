# Mooncake Store Rust Bindings

This directory contains the Rust bindings, examples, benchmarks, and smoke tests
for Mooncake Store.

## Backends (features)

The Rust package has two backends. Enable at least one; if both are enabled, `link`
takes precedence:

- **`link`** (default): links the canonical `libmooncake_store.so` SDK at build
  time. `build.rs` generates the FFI with bindgen, so the shared library and
  `store_c.h` must be present; its C++ dependency graph is not repeated here.
  Everything below assumes this backend.
- **`dlopen`**: loads `libmooncake_store.so` at run time via `libloading`, using
  **committed, pre-generated** bindings (`src/generated/ffi_dlopen_bindings.rs`).
  A consumer needs only `libloading` — no bindgen, no `store_c.h`, no libclang,
  no C++ linking — and only the shared library at run time. Maintainers
  regenerate the bindings after the C ABI changes (see below):

  ```toml
  mooncake_store = { version = "0.1", default-features = false, features = ["dlopen"] }
  ```

  A Store build emits `libmooncake_store.so`; the legacy
  `WITH_STORE_C_SHARED` switch remains accepted but is no longer required. At
  run time the library is located via the `MOONCAKE_STORE_LIBRARY` environment variable (default
  `libmooncake_store.so`, resolved through the OS loader search path), or pin an
  explicit path with `mooncake_store::load_library(path)` before creating a
  store. The public API is identical to the `link` backend.

  Regenerate the committed bindings after the C ABI (`store_c.h`) changes:

  ```bash
  cargo run --example generate_dlopen_bindings   # or: cmake --build build --target generate_store_rust_dlopen_bindings
  ```

  A pre-commit hook (on `store_c.h`/generator changes) and CI both enforce this
  via `git diff --exit-code`, so stale bindings cannot merge. CI additionally
  builds the packaged archive with `--features dlopen` to confirm a published
  consumer stays independent of `store_c.h`.

## Prerequisites

Install the Rust toolchain and libclang before building these bindings. On Ubuntu:

```bash
sudo apt-get install libclang-dev clang
```

## Build With CMake

Configure Mooncake with Store and Rust bindings enabled:

```bash
cmake -S . -B build -G Ninja \
  -DWITH_STORE=ON \
  -DWITH_STORE_RUST=ON
```

Build the Rust package, examples, and tests through the CMake targets:

```bash
cmake --build build --target build_mooncake_store_rust
cmake --build build --target build_mooncake_store_rust_example
cmake --build build --target build_mooncake_store_rust_tests
```

## Build With Cargo

Standalone Cargo builds need the CMake build directory and Store headers:

```bash
cd mooncake-store/rust

MOONCAKE_BUILD_DIR=/path/to/Mooncake/build
export MOONCAKE_STORE_LIB_DIR=$MOONCAKE_BUILD_DIR/mooncake-store/src
export MOONCAKE_STORE_INCLUDE_DIR=/path/to/Mooncake/mooncake-store/include

cargo build --examples --release
cargo test --tests --no-run --release
```

At runtime, the dynamic linker must be able to find the Store SDK. If it is not
installed in a system library directory, add its build output to
`LD_LIBRARY_PATH`:

```bash
export LD_LIBRARY_PATH=$MOONCAKE_STORE_LIB_DIR:${LD_LIBRARY_PATH:-}
```

## Integration Smoke Test

The integration smoke test needs a running metadata server and
`mooncake_master`. The CI workflow is the best reference for the complete
service setup. Once those services are running, execute:

```bash
cd mooncake-store/rust

MC_RUST_STORE_RUN_INTEGRATION=true \
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
MC_RUST_STORE_MASTER_ADDR=127.0.0.1:50051 \
MC_RUST_STORE_LOCAL_HOSTNAME=127.0.0.1 \
MC_RUST_STORE_PROTOCOL=tcp \
MC_RUST_STORE_DEVICE_NAME= \
cargo test --test minimal_smoke -- --nocapture
```

## Benchmark Smoke Test

For a short benchmark run, reduce the iteration counts:

```bash
cd mooncake-store/rust

MC_RUST_BENCH_ITERATIONS=4 \
MC_RUST_BENCH_VALUE_SIZE=4096 \
MC_RUST_BENCH_WARMUP=1 \
cargo run --release --example store_benchmark
```
