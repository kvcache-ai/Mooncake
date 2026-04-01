---
name: mooncake-ci-local
description: Run Mooncake CI test suite locally — maps GitHub Actions CI steps to local commands. Use this skill whenever the user wants to run tests locally, reproduce a CI failure, check if their changes break tests, or run any subset of the CI test suite (C++ unit tests via ctest, Python integration tests, code format checks, or the full test pipeline). Trigger on phrases like "run tests", "run CI locally", "reproduce CI failure", "check my changes", "test before PR", "run ctest", "run python tests", "run all tests".
---

# Mooncake CI Local Test Runner

You help users run the Mooncake CI test suite locally. The CI has three test layers. Map what the user wants to the right layer, check prerequisites, and run the tests.

## CI Test Layers

### Layer 1 — C++ Unit Tests (ctest)
**CI equivalent:** `build` job in `ci.yml` — "Test (in build env) with coverage"

**Prerequisite services:**
```bash
# 1. etcd (port 2379)
etcd --advertise-client-urls http://127.0.0.1:2379 --listen-client-urls http://127.0.0.1:2379 &
sleep 2
etcdctl --endpoints=http://127.0.0.1:2379 endpoint health  # verify

# 2. HTTP metadata server (port 8080)
cd mooncake-transfer-engine/example/http-metadata-server-python
pip install aiohttp
python ./bootstrap_server.py &
cd -
```

**Run:**
```bash
cd build
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 ctest -j --output-on-failure
```

**Run specific test:**
```bash
cd build
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 ctest -R <test_name_pattern> --output-on-failure
# List all available tests: ctest -N
```

### Layer 2 — Python Integration Tests
**CI equivalent:** `test-wheel-ubuntu` job — `run_tests.sh`

**Prerequisite:** Mooncake wheel must be installed (either via `pip install` or via `make install` after build).

**Check install:**
```bash
python -c "import mooncake; print('OK')"
which mooncake_master  # must NOT be /usr/local/bin (must be from Python package)
```

**Run full suite:**
```bash
# Start metadata server first
mooncake_http_metadata_server --port 8080 &
sleep 1

cd mooncake-wheel/tests
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 MC_FORCE_TCP=true \
  bash ../../scripts/run_tests.sh
```

**Individual Python tests** (all require metadata server + mooncake_master on port 50051):
```bash
# Setup shared services
mooncake_http_metadata_server --port 8080 &
mooncake_master --default_kv_lease_ttl=500 &
sleep 2

cd mooncake-wheel/tests
export MC_METADATA_SERVER=http://127.0.0.1:8080/metadata
export DEFAULT_KV_LEASE_TTL=500
export MC_FORCE_TCP=true

# Pick any test:
python test_distributed_object_store.py
python test_replicated_distributed_object_store.py
python test_put_get_tensor.py          # requires torch + numpy
python test_safetensor_functions.py    # requires safetensors
python test_dummy_client.py
python test_cli.py
python test_distributed_object_store_cxl.py  # requires CXL build
```

**Transfer engine tests specifically:**
```bash
cd mooncake-wheel/tests
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true python transfer_engine_target.py &
TARGET_PID=$!
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true python transfer_engine_initiator_test.py
kill $TARGET_PID
```

**Scripts-based tests** (from `test-wheel-ubuntu` job):
```bash
# Tensor API perf test
export MOONCAKE_MASTER="127.0.0.1:50051"
export MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:8080/metadata"
export MOONCAKE_PROTOCOL="tcp"
export LOCAL_HOSTNAME="127.0.0.1"
python scripts/test_tensor_api.py -n 1
python scripts/test_async_store.py
python scripts/test_copy_move_api.py
```

### Layer 3 — Static Checks (no services needed)
**CI equivalent:** `clang-format` and `spell-check` jobs

**Code format (changed files vs main):**
```bash
./scripts/code_format.sh --check --base origin/main
# Auto-fix:
./scripts/code_format.sh --base origin/main
```

**Spell check:**
```bash
# Requires typos tool: cargo install typos-cli
typos
```

**Pre-commit (runs all hooks):**
```bash
pip install pre-commit
pre-commit run --all-files
# Or just on staged files:
pre-commit run
```

## Build Configurations (from CI)

If the user needs to build first, here are the CI-equivalent cmake flags:

**Standard build with coverage (mirrors `build` job):**
```bash
mkdir build && cd build
cmake -G Ninja .. -DUSE_HTTP=ON -DUSE_CXL=ON -DUSE_ETCD=ON -DSTORE_USE_ETCD=ON -DENABLE_ASAN=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build .
sudo cmake --install .
```

**All features ON (mirrors `build-flags` job):**
```bash
mkdir build && cd build
cmake -G Ninja .. -DUSE_ETCD=ON -DUSE_CXL=ON -DUSE_REDIS=ON -DUSE_HTTP=ON -DWITH_STORE=ON -DWITH_P2P_STORE=ON -DWITH_METRICS=ON -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=ON
cmake --build .
sudo cmake --install .
```

**Transfer engine only:**
```bash
cd mooncake-transfer-engine
mkdir build && cd build
cmake -G Ninja .. -DUSE_ETCD=OFF -DUSE_CXL=ON -DUSE_REDIS=ON -DUSE_HTTP=ON -DBUILD_UNIT_TESTS=ON -DBUILD_EXAMPLES=ON
cmake --build .
```

## Workflow: Diagnosing and Running Tests

### Step 1 — Understand what the user wants

Ask (or infer from context):
- All tests, or a specific subset?
- Did a specific CI job fail? Which one?
- Is the build already done, or do they need to build first?

### Step 2 — Check and Fix Prerequisites

**One-command setup** — this script checks all prerequisites and auto-fixes issues:

```bash
bash .claude/skills/mooncake-ci-local/scripts/check-prerequisites.sh
```

**What it checks:**
1. ✓ Build directory exists
2. ✓ mooncake package installed (auto-installs via cmake --install if missing)
3. ✓ ctest available
4. ✓ Restarts all services (etcd, metadata server) in clean state
5. ✓ Verifies all services are healthy

**If you need to build first:**
```bash
mkdir build && cd build
cmake -G Ninja .. -DUSE_HTTP=ON -DUSE_ETCD=ON -DUSE_CXL=ON -DSTORE_USE_ETCD=ON -DCMAKE_BUILD_TYPE=Debug
cmake --build .
sudo cmake --install .
```

**If script fails:**
- Build issues: See "Build Configurations" section below
- mooncake install fails: Try `pip install mooncake-wheel/dist/*.whl` manually
- etcd install fails: Download from https://github.com/etcd-io/etcd/releases

### Step 3 — Run and report

Run the relevant test layer. On failure:
1. Show the exact error message
2. Check if it's a service/env issue (most common) vs a real test failure
3. Suggest the fix (see common issues below)

## Common Local Test Issues

**"mooncake_master found in /usr/local/bin" error in run_tests.sh:**
The test expects mooncake_master to come from the Python package, not a system install.
```bash
# Remove the system-installed binary:
sudo rm /usr/local/bin/mooncake_master
# Or use the wheel-installed one:
pip install mooncake-wheel/dist/*.whl
```

**etcd port conflict:**
```bash
pkill etcd && sleep 1
etcd --advertise-client-urls http://127.0.0.1:2379 --listen-client-urls http://127.0.0.1:2379 &
```

**Metadata server port conflict:**
```bash
pkill -f bootstrap_server.py
pkill -f mooncake_http_metadata_server
```

**Tests hang (master not responding):**
```bash
pkill mooncake_master
sleep 2
mooncake_master --default_kv_lease_ttl=500 &
sleep 1
```

**torch/numpy not installed for tensor tests:**
```bash
pip install torch numpy safetensors packaging
```

**ctest shows no tests found:**
```bash
# Rebuild with unit tests enabled:
cd build
cmake .. -DBUILD_UNIT_TESTS=ON
cmake --build .
```

## Quick One-Liners

```bash
# Run ALL C++ tests (after building with etcd + metadata server running):
# Note: full suite takes 5-15 minutes depending on hardware
cd build && MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 ctest -j --output-on-failure

# Run only fast tests (skip slow integration tests):
cd build && MC_METADATA_SERVER=http://127.0.0.1:8080/metadata DEFAULT_KV_LEASE_TTL=500 ctest -j --output-on-failure --exclude-regex "etcd|ha_test|redis"

# Run ALL Python tests:
mooncake_http_metadata_server --port 8080 & sleep 1 && cd mooncake-wheel/tests && MC_METADATA_SERVER=http://127.0.0.1:8080/metadata MC_FORCE_TCP=true bash ../../scripts/run_tests.sh

# Check code format (changed files only):
./scripts/code_format.sh --check --base origin/main

# Full pre-commit check:
pre-commit run --all-files
```
