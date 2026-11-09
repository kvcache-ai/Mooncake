# mooncake-store-rs

Store-RS is the Rust-native Mooncake Store implementation in the Mooncake
repository. It keeps the familiar Store programming model and uses Mooncake
Transfer Engine for data movement. Routing, leases, placement, and lifecycle
state are coordinated through the configured metadata backend.

Store-RS is an optional root CMake component. The default Python Store facade
selects the C++ backend; build the unified root wheel with Store-RS enabled to
select the Rust backend.

## Source-checkout quickstart

Run the following from the Mooncake repository root. Initialize the checkout's
submodules before configuring CMake.

Requirements:

- CMake and a C++ toolchain.
- The Rust toolchain required by the Store-RS build.
- Python 3.10 or newer for the root wheel.
- Redis or etcd when running a multi-client Store.

Keep source, build, and interpreter locations explicit:

    export MOONCAKE_ROOT_DIR="$PWD"
    export MOONCAKE_STORE_RS_DIR="$MOONCAKE_ROOT_DIR/mooncake-store-rs"
    export MOONCAKE_BUILD_DIR="$MOONCAKE_ROOT_DIR/build/store-rs"
    export MOONCAKE_PYTHON_BIN="$(command -v python3)"

    cmake -S "$MOONCAKE_ROOT_DIR" -B "$MOONCAKE_BUILD_DIR" \
      -DPython3_EXECUTABLE="$MOONCAKE_PYTHON_BIN" \
      -DWITH_STORE=ON \
      -DWITH_STORE_RUST=OFF \
      -DWITH_STORE_RS=ON \
      -DWITH_TE=ON \
      -DUSE_TENT=ON \
      -DBUILD_SHARED_LIBS=ON \
      -DCMAKE_BUILD_TYPE=Release
    cmake --build "$MOONCAKE_BUILD_DIR" --target build_store_rs --parallel

This out-of-tree build provides the native artifacts and standalone binaries.
For a Python wheel only, skip the separate CMake build command; the wheel
builder configures and builds its own CMake tree.

Build the unified root wheel with the optional Store-RS component:

    SKBUILD_CMAKE_ARGS="-DWITH_STORE=ON -DWITH_STORE_RUST=OFF \
      -DWITH_STORE_RS=ON -DWITH_TE=ON -DUSE_TENT=ON \
      -DBUILD_SHARED_LIBS=ON -DPython3_EXECUTABLE=$MOONCAKE_PYTHON_BIN" \
      "$MOONCAKE_PYTHON_BIN" -m pip wheel . --no-deps --wheel-dir dist/store-rs

    "$MOONCAKE_PYTHON_BIN" -m venv .venv-store-rs
    .venv-store-rs/bin/python -m pip install dist/store-rs/mooncake_transfer_engine-*.whl

The root wheel owns the mooncake Python package. Set the backend before the
first import of mooncake.store:

| MOONCAKE_STORE_BACKEND | Selected backend |
|------------------------|------------------|
| Unset or cpp | C++ Store |
| rs | Store-RS |

The setting accepts cpp or rs. The normal build leaves WITH_STORE_RS disabled,
and the default Python facade uses the C++ Store.

Check the Python import and the three standalone command entry points:

    MOONCAKE_STORE_BACKEND=rs .venv-store-rs/bin/python -c \
      "from mooncake.store import MooncakeDistributedStore; print(MooncakeDistributedStore)"

    .venv-store-rs/bin/mooncake-store-rs-client --help
    .venv-store-rs/bin/mooncake-store-rs-admin --help
    .venv-store-rs/bin/mooncake-store-rs-bench --help

For workspace unit tests and installed-package smoke coverage, use
`scripts/ci/run_store_rs_smoke.sh` from the repository root after installing
the CMake `python` component. The [Store-RS validation guide](../docs/source/deployment/store-rs/testing.md#local-validation)
lists the required paths and the separate commands for heavier scenarios.

## Runtime commands

The unified root wheel exposes Store-RS tools under stable command names:

| Command | Purpose |
|---------|---------|
| mooncake-store-rs-client | Run a storage, reader, or routed-writer runtime. |
| mooncake-store-rs-admin | Serve admin HTTP endpoints and run operator commands. |
| mooncake-store-rs-bench | Verify correctness or measure throughput and latency. |

These commands always launch Store-RS. MOONCAKE_STORE_BACKEND selects the
Python facade and does not change the standalone command behavior.

## Documentation

| Topic | Guide |
|-------|-------|
| Source-checkout build and wheel | [Store-RS quickstart](../docs/source/getting_started/store-rs.md) |
| Feature overview | [Store-RS features](../docs/source/getting_started/store-rs-features.md) |
| Rust integration | [Rust API](../docs/source/api-reference/rust/store-rs.md) |
| Python facade and Store-RS semantics | [Python API](../docs/source/api-reference/python/store-rs.md) |
| Configuration and deployment | [Store-RS deployment guides](../docs/source/deployment/store-rs/index.md) |
| Admin endpoints | [Store-RS Admin HTTP API](../docs/source/api-reference/http/store-rs-admin.md) |
| Architecture and design | [Store-RS design](../docs/source/design/store/store-rs/index.md) |
| Benchmark commands | [Store-RS benchmark guide](../docs/source/performance/mooncake/store-rs-benchmark.md) |
| Key access debugging | [Store-RS troubleshooting](../docs/source/troubleshooting/store-rs-key-access.md) |
| Test scenarios | [Store-RS validation guide](../docs/source/deployment/store-rs/testing.md) |
