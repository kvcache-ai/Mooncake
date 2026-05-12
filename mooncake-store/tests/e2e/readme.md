# Mooncake Store E2E Tests

This directory contains end-to-end (E2E) tests for the Mooncake Store system. These tests verify the functionality, reliability, and fault tolerance of the distributed storage system under various conditions.

## Overview

The E2E test suite includes several executable programs designed to test different aspects of Mooncake Store:

- **clientctl**: An interactive client control tool for manual testing.
- **chaosctl**: A highly configurable tool for conducting chaos tests.
- **e2e_rand_test**: Long-term randomized end-to-end testing.
- **chaos_test**: Short-term chaos testing with predefined scenarios.
- **chaos_rand_test**: Long-term randomized chaos testing with configurable parameters.
- **store_client_e2e.py**: Python `MooncakeDistributedStore` client that continuously issues `put/get` operations.
- **run_nof_heartbeat_tcp_e2e.sh**: Scripted NoF heartbeat end-to-end test using a TCP SPDK target.

## Parameters

All test programs support configurable parameters, such as `--protocol`, `--master_path`, etc. Use the `--help` flag to see detailed information.

## Executable Programs

### clientctl

**Brief**: An interactive client control tool for convenient and straightforward manual testing and debugging. This tool can start multiple clients and perform various operations like `put`, `get`, and `mount` from a specified client.

**Usage**:
1. Start the transfer-engine's meta server and `mooncake_master`. If using HA mode, also start the etcd servers.
2. Launch `clientctl` and manually type in commands. Several predefined scenarios can be found in the `client_ctl_cases` directory.

**Commands**:
- `create [name] [port]`: Create a new client instance.
- `put [client_name] [key] [value]`: Store a key-value pair via the specified client.
- `get [client_name] [key]`: Retrieve a value by key via the specified client.
- `mount [client_name] [segment_name] [size]`: Mount a memory segment from the specified client.
- `remove [client_name]`: Remove a client instance.
- `sleep [seconds]`: Pause execution for the specified duration.
- `terminate`: Exit the program.

### chaosctl

**Brief**: A highly configurable tool for conducting chaos tests. This program starts multiple master instances and clients. The clients continuously send requests to the masters and verify the results. During the test, the program randomly kills or restarts master and client processes. After the test, it prints a test report.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the chaos test with the desired parameters.
3. After the test completes, a test report will be generated. If `TEST_ERROR_STR` is not zero, it indicates undesirable errors occurred — possibly due to misconfigurations or underlying bugs. Users can check the log files for detailed information.

### e2e_rand_test

**Brief**: This randomized end-to-end test starts one or several masters and clients, sends requests from clients to masters, and verifies the results. It simulates a stable cluster (no crashes or network partitions) to test normal behavior. This is a long-term test that may run for hours.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the test.

**[WIP]**:
Currently it only has few test cases. Will add more in the future.

### chaos_test

**Brief**: Chaos testing using predefined fault scenarios.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the test.

### chaos_rand_test

**Brief**: Randomized chaos testing with configurable parameters. This test launches multiple servers and clients, then verifies their behavior under fault conditions. Currently, only master crashes are simulated. This is a long-term test that may run for hours.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the test.

**[WIP]**:
Currently it only has few test cases. Will add more in the future.

### run_nof_heartbeat_tcp_e2e.sh

**Brief**: Launches a real four-component path for NoF heartbeat validation:

- `mooncake_master`
- standalone Python HTTP metadata server
- SPDK `nvmf_tgt` with TCP transport
- Python client built on `MooncakeDistributedStore`

The script first verifies steady-state `put/get` success with `memory + nof` replicas. It then kills the SPDK target, waits for the master heartbeat thread to emit `action=unmount_nof_segment_by_heartbeat`, and finally verifies that the client still observes successful I/O after the NoF segment is removed.

**Prerequisites**:

- `BUILD_DIR` points to a build tree that already contains:
  - `mooncake-store/src/mooncake_master`
  - `mooncake-integration/store*.so`
- SPDK has already been built under `thirdparties/spdk`
- Python environment contains `aiohttp` because the script launches a standalone metadata process with `mooncake-wheel/mooncake/http_metadata_server.py`
- The script uses `sudo -n` to set hugepages and mount `/dev/hugepages`, so the current user must have passwordless sudo

**Usage**:

```bash
cd mooncake-store/tests/e2e
BUILD_DIR=/path/to/build ./run_nof_heartbeat_tcp_e2e.sh
```

To run in **NoF-only** mode (do not mount a local memory segment), set:

```bash
CLIENT_GLOBAL_SEGMENT_SIZE=0 BUILD_DIR=/path/to/build ./run_nof_heartbeat_tcp_e2e.sh
```

To increase the amount of steady-state traffic before killing the target, set:

```bash
PRE_FAULT_SUCCESS_TARGET=10 BUILD_DIR=/path/to/build ./run_nof_heartbeat_tcp_e2e.sh
```

**Notes**:

- The client payload size defaults to `4096` bytes because the current NoF path requires 4K-aligned I/O.
- The script uses a standalone metadata server process (`mooncake-wheel/mooncake/http_metadata_server.py`) instead of the embedded master metadata server so all four components remain explicit during the test.
- In default mode, the script verifies **service continuity** after NoF unmount by checking that post-fault I/O still succeeds.
- In `CLIENT_GLOBAL_SEGMENT_SIZE=0` mode, the script verifies **NoF-only failure behavior** by checking that post-fault I/O starts failing after the NoF segment is removed.
- Logs are written under `LOG_DIR` (default `/tmp/mooncake_nof_heartbeat_e2e`) and the final pass/fail summary is printed from `summary.log`.

### store_client_e2e.py

**Brief**: A standalone Python workload generator built on `MooncakeDistributedStore`. It continuously issues `put/get` against the configured master/metadata pair and prints `put_ok/get_ok/put_fail/get_fail` lines that can be consumed by shell scripts.

**Standalone Usage**:

```bash
PYTHONPATH=/path/to/build/mooncake-integration \
python3 store_client_e2e.py \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --global-segment-size 67108864 \
  --local-buffer-size 33554432 \
  --payload-size 4096 \
  --duration-sec 20 \
  --sleep-ms 200 \
  --key-prefix demo
```

**Key Parameters**:

- `--global-segment-size 0`: run in NoF-only mode
- `--payload-size 4096`: keep NoF writes 4K aligned
- `--duration-sec`: total workload duration
- `--sleep-ms`: interval between operations
