# TEBench Usage Guide

## 1. What is TEBench?

`TEBench` (Transfer Engine Benchmarking Tool) is a benchmark utility provided by Mooncake.
It evaluates **throughput, latency, P99, and P999** performance for **read/write/mixed** workloads based on the TransferEngine.
It supports **multithreading, various block/batch sizes, and optional data consistency checks**.

---

## 2. Running Modes

TEBench runs in two modes:

1. **Target Mode**

   * Starts a server, allocates a local buffer, and registers it with the TransferEngine.
   * Waits for the initiator to connect.
   * Launch without `--target_seg_name`.

   ```bash
   ./tebench [--seg_type=DRAM|VRAM] \
             [--xport_type=rdma|tcp|mnnvl|nvlink]
   ```

   Output will show something like:

   ```
   To start initiators, run
     ./tebench --target_seg_name=HOST:PORT
   Press Ctrl-C to terminate
   ```

2. **Initiator Mode**

   * Runs the benchmark, issues read/write/mix requests, and prints performance results.
   * Requires `--target_seg_name=<segment_name>`.

   ```bash
   ./tebench --target_seg_name=HOST:PORT \
             [--seg_type=DRAM|VRAM] \
             [--xport_type=rdma|tcp|mnnvl|nvlink]
             [--op_type=read|write] \
             [--num_threads=4 ] \
             [--duration=5] \
   ```

## 3. Key Command-Line Parameters

| Option                | Description                                  | Default |
| --------------------- | -------------------------------------------- | ------- |
| `--seg_name`          | Local memory segment name                    | `""`    |
| `--seg_type`          | Memory type: `DRAM` or `VRAM`                | `DRAM`  |
| `--target_seg_name`   | Remote segment name (empty = target mode)    | `""`    |
| `--op_type`           | Operation type: `read`, `write` or `mix`     | `read`  |
| `--check_consistency` | Enable data consistency check (for mix mode) | `false` |
| `--total_buffer_size` | Total buffer size (bytes)                    | `1GB`   |
| `--start_block_size`  | Minimum block size                            | `4KB`   |
| `--max_block_size`    | Maximum block size                            | `64MB`  |
| `--start_batch_size`  | Minimum batch size                            | `1`     |
| `--max_batch_size`    | Maximum batch size                            | `1`     |
| `--duration`          | Duration of each test case (seconds)         | `5`     |
| `--num_threads`       | Number of concurrent threads                 | `2`     |
| `--metadata_type`     | Metadata service options: `p2p, etcd, redis, http` | `p2p` |
| `--metadata_url_list` | Metadata server addresses (comma-separated)  | `""`    |
| `--xport_type`        | Transport options: `rdma, shm, gds, mnnvl, iouring, tcp` | `""` |

## 4. Output Format

Each test prints a table:

```
Block Size (B)    Batch Size     B/W (GB/Sec)   Avg Lat. (us)   Avg Tx (us)   P99 Tx (us)   P999 Tx (us)
--------------------------------------------------------------------------------------------------------
4096              1              5.123456       50.0            48.0          80.0          150.0
8192              1              8.654321       40.0            39.0          70.0          120.0
...
```

* **B/W (GB/Sec):** Aggregate bandwidth
* **Avg Lat. (us):** Average latency (multi-threaded)
* **Avg Tx (us):** Average per-transfer latency
* **P99 / P999 Tx (us):** Per-transfer P99 / P999 latency

## 5. Notes & Tips

1. Make sure **block\_size × batch\_size × num\_threads ≤ total\_buffer\_size**, otherwise the test case will be skipped.
2. `--seg_type=VRAM` requires compilation with `USE_CUDA` and a GPU runtime.
3. Data consistency checking (`--check_consistency`) reduces performance — use for debugging only.
4. Target must be started **before** initiators can connect.

