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

### 4.1 QoS Metrics Baseline

Use `--qos_classes` to partition a fixed number of worker threads into QoS
classes:

```text
name:threads:slo_us:weight[:isolated_gbps],...
```

For example, the following closed-loop mixed workload assigns four workers to
an SLO-constrained foreground class and twelve workers to a best-effort
checkpoint class:

```bash
./tebench \
  --target_seg_name=<SEG> \
  --backend=tent \
  --start_num_threads=16 \
  --max_num_threads=16 \
  --qos_classes=foreground:4:1000:4:12.5,checkpoint:12:0:1:10.0 \
  --qos_link_capacity_gbps=25 \
  --qos_output_jsonl=qos-results.jsonl
```

The class thread counts must add up to the fixed `start_num_threads` value.
An `slo_us` of zero marks a best-effort class. The SLO is a reporting threshold:
QoS baseline mode measures whether each completed transfer meets it, without
changing request scheduling policy on either backend.

The human-readable summary and the optional versioned JSONL record report:

| Metric | Definition |
| ------ | ---------- |
| `slo_attainment` | Fraction of completed batches whose measured end-to-end transfer time is at most `slo_us` |
| `p99_us` | P99 end-to-end batch transfer latency for the class |
| `goodput_gbps` | Class throughput multiplied by SLO attainment; best-effort classes use attainment 1 |
| `weighted_goodput_gbps` | Sum of `weight × goodput_gbps` |
| `jain_fairness` | Jain index over per-class `throughput_gbps / weight` |
| `isolation_leakage` | `max(0, 1 - mixed_throughput / isolated_throughput)` |
| `total_utilization` | Aggregate measured throughput divided by `qos_link_capacity_gbps` |

Isolation leakage requires a matching class-only baseline, supplied as the
optional fifth class field. Total utilization requires
`--qos_link_capacity_gbps`. Missing baselines are emitted as `N/A` in text and
`null` in JSON rather than being inferred from the mixed run. Run isolated and
mixed cases with the same block size, batch size, transport, memory type, and
host pair. JSONL records retain `isolated_throughput_gbps` and
`link_capacity_gbps` alongside the derived values so every metric can be
recomputed from one record.

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

### 5.7 QoS Reporting

* `--qos_classes` : class/thread/SLO/weight contract described in Section 4.1
* `--qos_link_capacity_gbps` : measured usable link capacity in decimal GB/s
* `--qos_output_jsonl` : append one schema-versioned JSON object per benchmark
  configuration

QoS mode intentionally requires a fixed thread count. Sweep offered load by
running explicit cases with different class thread allocations so every output
record has an unambiguous workload contract.
