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

`tebench` supports **all-to-all** testing for measuring aggregate bandwidth in multi-node clusters. In this mode, every node connects to all other nodes simultaneously, and results are aggregated across the cluster using a central coordinator.

### 6.1 When to Use All-to-All Mode

All-to-all testing is useful for:

* **Cluster-scale validation** — verify full bisection bandwidth
* **Network tuning** — identify bottlenecks before production deployment
* **Scaling tests** — measure how performance changes with node count

### 6.2 All-to-All Configuration with Coordinator

The all-to-all mode uses a **coordinator-based architecture** where a central coordinator server manages node registration and result aggregation.

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--coordinator` | Coordinator server address (enables all-to-all mode) | `192.168.1.100:12345` |
| `--backend` | Backend type (must be `tent`) | `tent` |
| `--wait_timeout` | Timeout for waiting on all nodes (default: 300s) | `300` |

> **Note**: All-to-all mode requires the **TENT backend** (`--backend=tent`). When `--coordinator` is specified, all-to-all mode is automatically enabled.

### 6.3 Running an All-to-All Test

#### Step 1: Start the Coordinator Server

On any machine (typically one of the test nodes or a dedicated coordinator node):

```bash
./coordinator --num_nodes=4 --port=12345
```

The coordinator will:
* Listen for node registrations
* Assign unique ranks to each node
* Broadcast the complete node list to all nodes once everyone is ready
* Aggregate and display results from all nodes

#### Step 2: Start All Nodes

On **each node** in the test, run `tebench` with the coordinator address:

```bash
# On Node 1
./tebench \
  --coordinator=192.168.1.100:12345 \
  --backend=tent \
  --seg_type=DRAM \
  --op_type=read \
  --duration=5

# On Node 2 (same command)
./tebench \
  --coordinator=192.168.1.100:12345 \
  --backend=tent \
  --seg_type=DRAM \
  --op_type=read \
  --duration=5

# On Node 3 (same command)
./tebench ... (same as above)

# On Node 4 (same command)
./tebench ... (same as above)
```

**Key points:**
* **All nodes use the same command** — no need to manually assign ranks
* The coordinator automatically assigns ranks based on registration order
* Each node prints its assigned rank upon registration
* The test waits for all nodes before starting

#### Step 3: Monitor Test Progress

During execution:

1. **Local node results** are printed immediately after each test configuration:
   ```text
   ===== Local Node Results (Node 0/4) =====
   BlkSize(B)  Batch  Threads  BW(GB/s)  ...
   ```

2. **Coordinator aggregates results** and displays cluster-wide statistics at the end:
   ```text
   ===== All-to-All Aggregated Results (4/4 nodes) =====
   ```

#### Step 4: Stop the Test

* Press **Ctrl-C** on any `tebench` node to stop that node
* The coordinator will detect the disconnect and print final results
* Press **Ctrl-C** on the coordinator to shut it down

### 6.4 Troubleshooting

#### Timeout waiting for all nodes

If you see:
```
Timeout waiting for all nodes (waited 300 seconds)
```

This usually means:
1. Not all nodes have started — verify all `num_nodes` instances are running
2. Some nodes crashed — check logs on each node
3. Network connectivity issues — check firewall and routing

#### Connection failures

If nodes cannot connect:
1. Verify the coordinator address is correct
2. Check network reachability (ping, telnet)
3. Ensure RDMA/network interfaces are properly configured

### 6.5 All-to-All Output

Results are displayed in two sections:

#### Local Node Results

Each node prints its own results immediately:

```text
===== Local Node Results (Node 0/4) =====
BlkSize(B)  Batch  Threads  Targets  BW(GB/s)  AvgLat(us)  AvgTx(us)  P99Tx(us)  P999Tx(us)
     4096      8         1        3      12.1        2.1        1.8        3.2         4.1
```

#### Aggregated Cluster Results

The coordinator displays cluster-wide statistics:

```text
===== All-to-All Aggregated Results (4/4 nodes) =====
BlkSize (B)  Batch  Threads  Total BW (GB/S)  Avg Lat (us)  Avg Tx (us)  P99 Tx (us)  P999 Tx (us)
        4096      8         1             48.2           2.1          1.8         3.2          4.1
```

Where:
* **Total BW** = Sum of bandwidth from all nodes (cluster aggregate throughput)
* **Avg Lat** = Average wall-clock time per test run (across all nodes)
* **Avg Tx** = Average per-transfer execution time (single transfer to one target)
* **P99/P999 Tx** = Percentile latencies for individual transfers

### 6.6 Performance Metrics Explained

In all-to-all mode, the metrics have specific meanings:

| Metric | Calculation | Description |
|--------|-------------|-------------|
| **Total BW** | Sum of all node bandwidths | Cluster aggregate throughput |
| **Avg Lat** | Wall-clock time per sample | Average test execution time per run |
| **Avg Tx** | Single transfer duration | Average time for one transfer to one target |

> **Note**: These metrics account for the multi-threaded parallel execution model. Bandwidth is calculated based on actual wall-clock time (not accumulated thread time), ensuring accurate cluster-wide throughput measurement.
