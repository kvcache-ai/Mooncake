# Mooncake Transfer Engine Benchmark Tool (`tebench`) Guide

`tebench` is an end-to-end benchmarking tool for the Mooncake Transfer Engine.
It evaluates **bandwidth and latency** across different `(block_size, batch_size, concurrency)` combinations, and supports both the **classic TE** backend and the **new TENT** backend.

## 1. Build and Deployment

### 1.1 Prerequisites

* A C++ toolchain capable of building the project
* Optional: GPU(s) and corresponding transport stacks (RDMA, shared memory, io_uring, etc.)

> Exact dependencies (CUDA / OFED / io_uring / etc.) depend on the transport backend being used.

### 1.2 Build with TENT Enabled

Typical out-of-tree build:

```bash
mkdir -p build
cd build
cmake .. -DUSE_TENT=ON
cmake --build . -j
```

## 2. Execution Model

`tebench` runs in **two roles**:

* **Target (receiver)**: creates a memory segment and waits for connections
* **Initiator (sender)**: connects to a target and executes benchmark cases

### Role Selection

* If `--target_seg_name` is **empty** → Target mode
* If `--target_seg_name` is **set** → Initiator mode

### Backend Selection

* `--backend=classic` → classic TE implementation
* `--backend=tent` → TENT implementation

## 3. Quick Start

### 3.1 Start Target (First)

On the target machine:

```bash
./tebench \
  --seg_type=DRAM \
  --backend=tent
```

The program will print a command containing a generated segment name:

```text
To start initiators, run
  ./tebench --target_seg_name=<SEG_NAME> --seg_type=DRAM --backend=tent
Press Ctrl-C to terminate
```

Copy the printed `--target_seg_name` to the initiator side.

### 3.2 Start Initiator

On the initiator machine:

```bash
./tebench \
  --target_seg_name=<SEG_NAME_FROM_TARGET> \
  --seg_type=DRAM \
  --backend=tent \
  --op_type=read \
  --duration=5
```

## 4. Output Metrics

Each output row corresponds to one benchmark configuration.

| Column         | Description                                         |
| -------------- | --------------------------------------------------- |
| `BlkSize (B)`  | Block size per request (bytes)                      |
| `Batch`        | Number of requests per submission                   |
| `BW (GB/S)`    | Throughput (total bytes / total time)               |
| `Avg Lat (us)` | Average end-to-end latency (scaled by thread count) |
| `Avg Tx (us)`  | Average per-transfer execution time                 |
| `P99 Tx (us)`  | P99 transfer latency                                |
| `P999 Tx (us)` | P999 transfer latency                               |

A short (~1 second) warmup phase is executed before measurements begin.

## 5. Runtime Configuration

This section summarizes the key runtime options that control workload behavior,
resource usage, and backend selection.

### 5.1 Workload Mode (`--op_type`)

Controls the transfer pattern executed by the initiator:

* `read`  — repeated READ transfers
* `write` — repeated WRITE transfers
* `mix`   — alternating WRITE followed by READ (both recorded)

Example:

```bash
--op_type=write
```

### 5.2 Data Consistency Check (`--check_consistency`)

When enabled, the benchmark validates correctness in `mix` mode:

* Source buffers are filled with a known pattern before WRITE
* Data is READ back and verified on the initiator

Example:

```bash
./tebench --target_seg_name=<SEG> --op_type=mix --check_consistency=true
```

> Consistency checking introduces CPU-side overhead and should be disabled for pure performance measurements.

### 5.3 Notification Feature (`--notifi`)

When enabled, the benchmark sends a notification message along with each transfer batch:

* The notification contains the `target_addr` as the message payload
* The peer can verify the notification was received correctly by checking the message
* Useful for testing notification delivery and end-to-end communication

Example:

```bash
./tebench --target_seg_name=<SEG> --notifi=true
```

> This feature is primarily for testing notification mechanisms. The notification message contains the target address for verification purposes.

### 5.4 Segment, Concurrency, and Memory Layout

**Segment and role**

* `--seg_name` : local segment name (typically left empty)
* `--seg_type` : `DRAM | VRAM` (default: `DRAM`)
* `--target_seg_name` : target segment name (empty → Target mode)

**Scan ranges**

* `--total_buffer_size` : total buffer limit (bytes)
* `--start_block_size`, `--max_block_size` : block size sweep (powers of two)
* `--start_batch_size`, `--max_batch_size` : batch size sweep (powers of two)
* `--start_num_threads`, `--max_num_threads` : thread sweep (powers of two)
* `--duration` : measurement time per case (seconds)

A test case is skipped when:

```
block_size × batch_size × num_threads > total_buffer_size
```

---

### 5.5 GPU Affinity

* `--local_gpu_id`  : initiator base GPU ID
* `--target_gpu_id` : target base GPU ID

Each worker thread uses:

```
gpu_id + thread_id
```

---

### 5.6 Backend, Transport, and Metadata

**Backend**

* `--backend` : `classic | tent` (default: `tent`)

**Transport (TENT only)**

* `--xport_type` : `rdma | shm | mnnvl | gds | iouring`

**Metadata service**

* `--metadata_type` : `p2p | etcd | redis | http` (default: `p2p`)
* `--metadata_url_list` : comma-separated URLs (ignored in `p2p` mode)

---

## 6. All-to-All Multi-Node Testing

`tebench` supports **all-to-all** testing for measuring aggregate bandwidth in multi-node clusters. In this mode, every node connects to all other nodes simultaneously, and results are aggregated across the cluster.

### 6.1 When to Use All-to-All Mode

All-to-all testing is useful for:

* **Cluster-scale validation** — verify full bisection bandwidth
* **Network tuning** — identify bottlenecks before production deployment
* **Scaling tests** — measure how performance changes with node count

### 6.2 All-to-All Configuration

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--enable_alltoall` | Enable all-to-all mode | `true` |
| `--test_id` | Unique identifier for the test run | `test_run_001` |
| `--num_nodes` | Total number of nodes in the test | `4` |
| `--node_rank` | This node's rank (0-based) | `0, 1, 2, 3` |
| `--sync_timeout_sec` | Timeout for node synchronization (default: 120s) | `120` |

> **Note**: All-to-all mode requires the **TENT backend** (`--backend=tent`).

### 6.3 Running an All-to-All Test

On each node, start `tebench` with a unique `node_rank`:

```bash
# Node 0
./tebench \
  --backend=tent \
  --enable_alltoall=true \
  --test_id=my_test \
  --num_nodes=4 \
  --node_rank=0 \
  --seg_type=DRAM \
  --op_type=read

# Node 1 (on different machine)
./tebench \
  --backend=tent \
  --enable_alltoall=true \
  --test_id=my_test \
  --num_nodes=4 \
  --node_rank=1 \
  --seg_type=DRAM \
  --op_type=read

# Node 2
./tebench ... --node_rank=2

# Node 3
./tebench ... --node_rank=3
```

The test automatically:

1. **Generates segment names** for all nodes (`tebench_alltoall_<test_id>_node_<rank>`)
2. **Synchronizes** — waits for all nodes to be ready
3. **Connects** each node to all other nodes
4. **Runs tests** and aggregates results on rank 0

### 6.4 All-to-All Output

Results on rank 0 show **aggregated metrics** across all nodes:

```text
===== All-to-All Aggregated Results (4 nodes) =====
BlkSize(B)  Batch  Thrd  Flows   BW(GB/s)  AvgLat(us)  AvgTx(us)  P99Tx(us)  P999Tx(us)
      4096      8     1     12       48.2        2.1        1.8        3.2         4.1
```

Where:
* **Flows** = `num_threads × target_count × num_nodes` (total concurrent flows)
* **BW** = Aggregate bandwidth across all nodes
* **Latency** = Per-transfer latency (averaged across all nodes)
