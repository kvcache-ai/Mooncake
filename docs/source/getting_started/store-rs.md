# Store-RS Source-Checkout Quickstart

Store-RS is an optional Rust-native Mooncake Store component in the root
repository. The normal CMake build leaves Store-RS disabled, and the Python
Store facade uses the C++ backend by default. Enable Store-RS when building a
root wheel that should include the Rust backend and its standalone commands.

## Requirements

- A Mooncake root checkout with its submodules initialized.
- CMake, a C++ toolchain, and the Rust toolchain used by the Store-RS build.
- Python 3.10 or newer for the Python wheel.
- A Redis or etcd metadata service for a multi-client runtime.

## Configure and build

Run these commands from the Mooncake repository root. Keep the source, build,
and Python interpreter paths explicit so CMake, the wheel, and validation
scripts use the same checkout:

```bash
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
```

This out-of-tree build provides the native artifacts and standalone binaries.
For a Python wheel only, skip the separate CMake build command; the wheel
builder configures and builds its own CMake tree.

The root Python wheel packages the C++ Store facade and, when configured with
WITH_STORE_RS=ON, the Store-RS extension and the three standalone commands:

```bash
SKBUILD_CMAKE_ARGS="-DWITH_STORE=ON -DWITH_STORE_RUST=OFF \
  -DWITH_STORE_RS=ON -DWITH_TE=ON -DUSE_TENT=ON \
  -DBUILD_SHARED_LIBS=ON -DPython3_EXECUTABLE=$MOONCAKE_PYTHON_BIN" \
  "$MOONCAKE_PYTHON_BIN" -m pip wheel . --no-deps --wheel-dir dist/store-rs

"$MOONCAKE_PYTHON_BIN" -m venv .venv-store-rs
.venv-store-rs/bin/python -m pip install dist/store-rs/mooncake_transfer_engine-*.whl
```

The root wheel owns the mooncake Python package. Build and install one wheel,
then select its Store backend before importing mooncake.store:

| Setting | Backend |
|---------|---------|
| Unset or cpp | C++ Store |
| rs | Store-RS, included by WITH_STORE_RS=ON |

The facade reads MOONCAKE_STORE_BACKEND when mooncake.store is first imported.
Use exactly cpp or rs. The setting does not change the standalone command
names.

```bash
MOONCAKE_STORE_BACKEND=rs .venv-store-rs/bin/python -c \
  "from mooncake.store import MooncakeDistributedStore; print(MooncakeDistributedStore)"

.venv-store-rs/bin/mooncake-store-rs-client --help
.venv-store-rs/bin/mooncake-store-rs-admin --help
.venv-store-rs/bin/mooncake-store-rs-bench --help
```

## Validate and continue

For Python compatibility validation, use a root wheel built with
`WITH_STORE_RS=ON` and run
`mooncake-store-rs/scripts/e2e/run-python-compat-e2e.sh`. The [Python
compatibility e2e guide](../deployment/store-rs/testing.md#local-validation)
lists the required environment and scenario steps.

The [Store-RS deployment index](../deployment/store-rs/index.md) links to
configuration, operator, and scenario-specific validation guides. For API
details, see the [Rust API](../api-reference/rust/store-rs.md), the
[Python API](../api-reference/python/store-rs.md), and the
[Admin HTTP API](../api-reference/http/store-rs-admin.md).
