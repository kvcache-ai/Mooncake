# KVCache Storage Benchmark v1

## Overview

The KVCache Storage Benchmark is a tool for evaluating storage performance of KVCache workloads. It simulates real-world cache access patterns using trace replay and measures storage I/O performance with detailed statistics.

## Usage

### Basic Usage

```bash
cd benchmarks/storage_benchmark_v1
python benchmark.py --scenario conversation \
                    --trace-dir /path/to/Mooncake/FAST25-release/traces \
                    --storage-dir /path/to/test/drive
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--trace-dir` | `../../FAST25-release/traces` | Directory containing trace files |
| `--scenario` | `toolagent` | Test scenario: `conversation`, `synthetic`, `toolagent`, or `all` |
| `--storage-dir` | `/tmp/mooncake_bench` | Directory for storage files |
| `--model` | `glm5` | Model preset: `glm5` or `kimi-k2.6` |
| `--page-size-tokens` | `512` | Page size in tokens |
| `--max-requests` | `None` | Maximum number of requests to process |
| `--max-pages` | `2000` | Maximum number of pages (creates modulo mapping if trace is larger) |
| `--fsync-mode` | `none` | When to fsync: `none`, `batch`, `always`, or `end` |
| `--fsync-batch-size` | `100` | Number of writes between fsync in batch mode |
| `--threads` | `1` | Number of benchmark client worker threads |
| `--replay-scales` | `0` | Comma-separated trace fast-forward speeds; `0` means unpaced |

### Replay Scale

Use `--replay-scales` to run the same trace at different fast-forward speeds:

```bash
python benchmark.py --scenario toolagent \
                    --trace-dir /path/to/Mooncake/FAST25-release/traces \
                    --storage-dir /path/to/test/drive \
                    --replay-scales 1,2,4,8
```

For example, `2` means 2x fast-forward and `8` means 8x fast-forward. `0`
preserves the old unpaced behavior.

### Client Threads

Use `--threads` to add benchmark client worker threads:

```bash
python benchmark.py --scenario toolagent \
                    --trace-dir /path/to/Mooncake/FAST25-release/traces \
                    --storage-dir /path/to/test/drive \
                    --threads 4
```

With `--threads > 1`, each benchmark client thread uses an independent storage
file under `thread_N/data.bin`, similar to running multiple clients at the same
time. Final results aggregate the per-thread counters and latency samples. For
strict single-client trace-order read/write and hit-rate accounting, use
`--threads 1`.

## Output Format

### Progress Output

During execution, each request displays real-time statistics:

```
[    10/12031] ids= 35 tokens= 18060 | QPS=   2.45 | R=    36 ( 22.01ms, 2435.2MB/s) | W=   963 ( 19.35ms, 2770.1MB/s)
```

Fields:
- `[N/Total]`: Current request progress
- `ids=N`: Number of hash_ids in this request
- `tokens=N`: Total tokens (input + output)
- `QPS=X`: Queries per second (overall)
- `R=N (latency, bandwidth)`: Read count, average latency, bandwidth
- `W=N (latency, bandwidth)`: Write count, average latency, bandwidth

### Final Results

```
================================================================================
  [1/1] toolagent_trace.jsonl
================================================================================

[General]
  Model:            glm5
  Threads:          1
  Fast-forward:     unpaced
  Requests:         12031
  Tokens:           123456789
  Total I/O Time:   245.123 s
  QPS:              49.07
  Hit Rate:         3.25%

[Read Operations]
  Count:            390
  Data Volume:      20919.62 MB
  Total Time:       8.590 s
  Bandwidth:         2435.67 MB/s
  Latency:
    Avg:             22.032 ms
    P50:             21.456 ms
    P95:             28.912 ms
    P99:             35.234 ms

[Write Operations]
  Count:            11641
  Data Volume:      654321.45 MB
  Total Time:       236.533 s
  Bandwidth:         2765.89 MB/s
  Latency:
    Avg:             20.312 ms
    P50:             19.876 ms
    P95:             25.123 ms
    P99:             31.456 ms

[Storage Info]
  Max Pages:        2000
  Written Pages:    2000
  Sync Count:       0
```

## Modulo Mapping

When the trace requires more pages than `--max-pages`, modulo mapping is enabled:

```
physical_page_id = logical_page_id % max_pages
```

This allows simulating large traces (millions of pages) with limited storage (thousands of pages). The first-hit/read-then-write logic is preserved by tracking written logical pages in memory.

**Example**: With `--max-pages 2000`, logical page IDs 0-1999 map directly to physical pages 0-1999. Logical page 2000 maps to physical page 0, logical page 2001 maps to physical page 1, etc.

## Graceful Interruption

Press `Ctrl-C` at any time to stop the benchmark and view partial results. The output will display all statistics collected up to the interruption point, using the same format as final results.

## Troubleshooting

### Insufficient Storage Warning
```
⚠️  Modulo mapping ENABLED (limited storage)
⚠️  Storage insufficient: 14,257,620 pages shortfall (762.34 GB)
```
Increase `--max-pages` to reduce modulo mapping effects.

### Low Bandwidth or High Latency
- Check fsync mode (`--fsync-mode none` for best performance)
- Verify disk performance with `fio` or `dd`
- Verify storage device health
