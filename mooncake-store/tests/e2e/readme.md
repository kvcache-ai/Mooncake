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
- SPDK has already been built under `extern/spdk`
- Python environment contains `aiohttp` because the script launches a standalone metadata process with `python/mooncake/http_metadata_server.py`
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
- The script uses a standalone metadata server process (`python/mooncake/http_metadata_server.py`) instead of the embedded master metadata server so all four components remain explicit during the test.
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

### In-process master e2e (no `mooncake_master`)

Linux CI (`test-wheel-ubuntu`) runs `scripts/run_standalone_store_e2e.sh` before
starting `mooncake_master`. This covers `EmbeddedMaster` plumbing used by tests.
The Store client owns the master in the same process; no `mooncake_master` or
metadata-service process is launched. The existing client hosting options remain
unchanged, and this is not a documented user deployment mode.

The CTest targets `embedded_master_test` and `standalone_client_test` also cover
loopback-only listeners, kernel-assigned ports during concurrent startup, and
same-port restart after stopping a master. TransferEngine reuse is checked with
both HTTP metadata and `P2PHANDSHAKE`; the latter deliberately uses different
Store and engine names and checks the published replica endpoint. The TENT
`cuda-off` CI job runs the same metadata-reuse cases with `MC_USE_TENT=1`.
In embedded mode, the legacy external `master_server_addr` argument is ignored
so multiple clients can keep its default without competing for port 50051.

```bash
bash scripts/run_standalone_store_e2e.sh
```

### run_oplog_snapshot_smoke.sh

Runs the batch OpLog snapshot path with two real master processes and a local
etcd instance. It publishes two snapshots with multiple object chunks, stops
and restarts the standby, verifies suffix replay and promotion, and audits
surviving and removed objects.

```bash
./run_oplog_snapshot_smoke.sh \
  --build-dir /path/to/build \
  --run-dir /tmp/mooncake-oplog-snapshot-smoke
```

Set `--failpoint-dir` to verify the same launcher environment path used by
crash tests. The script requires `mooncake_master`, `oplog_ha_client`,
`oplog_batch_inspector`, `hot_standby_snapshot_bootstrap_test`, `etcd`,
`etcdctl`, `curl`, `setsid`, and Python `aiohttp`. It is a manual real-etcd
check and is not registered in CI/nightly.

The run directory must be new. The script stores configurations, master and
client logs, snapshot artifacts, and audit results there, and stops its test
processes on exit. Local snapshot storage is shared by the two test masters;
for multi-host deployment, every master must be able to read the same durable
snapshot artifacts.

The production mode is opt-in with `enable_oplog_snapshot=true` together with
`enable_oplog=true`, HA/etcd and a configured snapshot object store. The default
chunk size is 1,000,000 objects and the default snapshot interval is 600 seconds.
The smoke overrides these to two objects and two seconds. A chunk bounds object
count, not byte size or total standby memory. Legacy catalog restore is not
used by this mode. Snapshot upload failures do not stop OpLog apply, but a node
must not serve if its recovery history cannot be proven complete.

This smoke does not cover the full crash/corruption/lease-contention matrix,
S3 outages, large-scale memory/freeze-time measurements, or safe OpLog pruning.
Keep batch history until retention/pruning has its own verified recovery gate.

## Batch OpLog capacity tests

Build `mooncake_master`, `oplog_batch_inspector`, and `oplog_ha_client` with
`STORE_USE_ETCD=ON`. Put matching etcd/etcdctl 3.5+ binaries on `PATH` and install
Python `aiohttp`. From the repository root:

```bash
bash mooncake-store/tests/e2e/run_oplog_batch_cluster_test.sh
mooncake-store/tests/e2e/run_oplog_batch_cluster.sh capacity-soak \
  --build-dir /path/to/build --run-dir /tmp/n13-soak \
  --capacity-seconds 3600 --capacity-max-batches 2048
mooncake-store/tests/e2e/run_oplog_batch_cluster.sh capacity-nospace \
  --build-dir /path/to/build --run-dir /tmp/n13-nospace
```

Both commands require a fresh directory and reject external etcd endpoints. They
create two masters, one etcd member and shared local snapshots; multi-member quorum
availability and S3 are not tested. Processes stop on exit; artifacts remain.

The soak checks bounded live batch keys, snapshot/floor progress and periodic
compact/defrag reclamation. NOSPACE fills history under a 16 MiB quota after
quiescing masters, then verifies the alarm, reclamation, disarm and recovered writes.
Both audit surviving/deleted keys across restart and promotion, then test new writes.
A 30-second soak is only a smoke test; report the actual measured duration.

`<run-dir>/capacity/` contains consistent-revision key/control samples, raw metrics,
maintenance status before/after compact and defrag, `soak-result.json`, and
`audits.log`. Process logs and acknowledgement manifests are in `logs/` and
`workload/`. Batch-specific helpers are in `tests/ha/snapshot/batch_oplog/`.
