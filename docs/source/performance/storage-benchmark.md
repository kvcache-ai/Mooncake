# Mooncake KVCache Storage Benchmark

High-performance KVCache storage benchmark tool based on Mooncake Store architecture.

## Overview

Evaluates I/O performance of KVCache storage systems using:
- Single large file (100GB) with offset-based block management
- Prefix caching simulation with hash-based block lookup
- Timestamp-based request replay for realistic testing
- Comprehensive metrics: latency, bandwidth, hit rates

## Test Flow

1. **Load Traces**: Read request sequences from JSONL files (`FAST25-release/traces`)
2. **Process Requests**: For each request, check hash_id prefix cache hits/misses
3. **Perform I/O**: Read cached blocks from disk, write new blocks to storage
4. **Collect Metrics**: Track latency, bandwidth, and cache hit rates

## Quick Start

```bash
# Quick test (100 requests, no timestamp replay)
python storage_benchmark.py --scenario=toolagent --max-requests=100

# Test all scenarios with replay
python storage_benchmark.py --scenario=all --time-scale=1.0
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--trace-dir` | Trace files directory | `../FAST25-release/traces` |
| `--scenario` | Test scenario: `conversation`, `synthetic`, `toolagent`, `all` | `toolagent` |
| `--storage-dir` | Storage directory | `/tmp/mooncake_bench` |
| `--bytes-per-token` | Bytes per token | `2048` (7B FP16) |
| `--max-requests` | Maximum requests per scenario (unlimited if not specified) | `None` |
| `--max-blocks` | Maximum number of blocks | `100000` |
| `--replay-timestamps` | Enable timestamp replay | `False` |
| `--time-scale` | Time scaling factor (1.0 = real-time, 0.1 = 10x faster) | `1.0` |

## Test Scenarios

- **`conversation`**: Write-intensive workload (dialogue patterns)
- **`synthetic`**: Read-intensive workload (cached patterns)
- **`toolagent`**: Balanced read/write mix (tool use patterns)

## Output Example

```
====================================================================================================
              Mooncake KVCache Storage Benchmark (Offset Allocator - High Performance)
====================================================================================================

Performance Summary:
Scenario                  Reqs     QPS      P50 Latency  P95 Latency  P99 Latency  Hit Rate
----------------------------------------------------------------------------------------------------
toolagent_trace.jsonl     100      14.4     15.49        239.99       10.58        24.27%

Detailed Statistics:

toolagent_trace.jsonl:
  Block Stats: Total=1,949, Read=473, Write=1,476
  Prefix Hits: 376
  Cache Hit Rate: 24.27%
  Write Ratio: 75.73%
  Storage Blocks: 1,476, Free Blocks: 0
  Time: Wall=8.4s, I/O=6.9s, Sleep=1.5s
  I/O: Read=473.0MB, Write=1476.0MB
  Latency: Read P99=0.28ms, Write P99=5.50ms
  Bandwidth: 280.8 MB/s (based on I/O time)
```

## Metrics

| Metric | Description |
|--------|-------------|
| **QPS** | Queries per second (based on I/O time, excluding sleep) |
| **P50/P95/P99 Latency** | Request latency percentiles (milliseconds) |
| **Hit Rate** | Cache hit ratio for blocks |
| **Bandwidth** | Throughput calculated based on I/O time only |
| **Read/Write P99** | P99 percentile for individual read/write operations |

## Trace Data Format

```json
{
  "timestamp": 1234.567,
  "hash_ids": [1, 2, 4, 7],
  "input_length": 2048,
  "output_length": 512
}
```

## Requirements

- Python 3.8+
- No third-party dependencies (standard library only)
