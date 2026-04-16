# Mooncake Development Guide

Mooncake is a KVCache-centric disaggregated architecture for LLM serving. Core components: Transfer Engine (data transfer), Store (distributed KVCache), P2P Store (peer-to-peer objects), EP (elastic expert parallelism).

## Build

Dependencies (requires sudo):
```bash
sudo bash dependencies.sh -y
```

Standard build:
```bash
mkdir build && cd build
cmake ..
make -j
sudo make install
```

Key CMake options (all OFF by default unless noted):
- `-DUSE_CUDA=ON` - NVIDIA GPU support (requires CUDA 12.1+)
- `-DUSE_ASCEND=ON` - Ascend NPU support
- `-DUSE_ETCD=ON` - Etcd metadata server (Go wrapper, default is HTTP)
- `-DWITH_EP=ON` - Elastic Expert Parallelism (requires CUDA, PyTorch)
- `-DWITH_STORE=ON` - Mooncake Store (default ON)
- `-DWITH_P2P_STORE=ON` - P2P Store
- `-DUSE_CXL=ON` - CXL transport
- `-DUSE_EFA=ON` - AWS EFA transport (requires libfabric)
- `-DUSE_MNNVL=ON` - Multi-Node NVLink
- `-DBUILD_UNIT_TESTS=ON` - Build C++ tests (default ON)
- `-DBUILD_EXAMPLES=ON` - Build examples (default ON)

Build Python wheel:
```bash
./scripts/build_wheel.sh
```
Environment vars: `PYTHON_VERSION=3.10`, `OUTPUT_DIR=dist`, `BUILD_DIR=build`.

## Test

C++ unit tests (requires `BUILD_UNIT_TESTS=ON`):
```bash
cd build
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata make test -j ARGS="-V"
```

Python tests (requires metadata server running):
```bash
mooncake_http_metadata_server --port 8080 &
./scripts/run_tests.sh
```

## Code Style

C++ formatting requires clang-format-20 (Google style with 4-space indent, 80-column limit):
```bash
./scripts/code_format.sh              # Format changed files vs origin/main
./scripts/code_format.sh --all        # Format all files
./scripts/code_format.sh --check      # Check only, no modifications
./scripts/code_format.sh -b origin/dev  # Compare against different branch
```

Pre-commit hooks: `pip install -r requirements-dev.txt && pre-commit install`

## Repository Structure

- `mooncake-transfer-engine/` - Core transport layer (TCP, RDMA, NVLink, EFA, CXL, Ascend)
- `mooncake-store/` - Distributed KVCache storage
- `mooncake-integration/` - Python bindings via pybind11 (`engine.so`, `store.so`)
- `mooncake-wheel/` - Python wheel packaging
- `mooncake-ep/` - Elastic Expert Parallelism CUDA extensions
- `mooncake-common/` - Shared utilities, etcd wrapper

## CI Notes

- CI runs on push to main, or when PR has `run-ci` label
- CI builds with: `-DUSE_HTTP=ON -DUSE_CXL=ON -DUSE_ETCD=ON -DSTORE_USE_ETCD=ON`
- Spell check uses `crate-ci/typos` (config: `.typos.toml`)
- Coverage via lcov, uploaded to Codecov

## Metadata Server

Start HTTP metadata server (simplest for testing):
```bash
mooncake_http_metadata_server --port 8080 &
```

Environment var: `MC_METADATA_SERVER=http://127.0.0.1:8080/metadata`

## Hardware Prerequisites

- RDMA: Mellanox OFED or similar RDMA driver
- CUDA: CUDA 12.1+ with GPUDirect Storage for `-DUSE_CUDA=ON`
- Ascend: CANN toolkit for `-DUSE_ASCEND=ON`
- Go 1.20+ required when `-DUSE_ETCD=ON` or `-DWITH_P2P_STORE=ON`