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

# Test with large model preset (Llama-3.1-405B)
python storage_benchmark.py --scenario=toolagent --model=llama-3.1-405b --max-requests=100

# Test with Deepseek V3 (extra large model)
python storage_benchmark.py --scenario=toolagent --model=deepseek-v3 --max-requests=100

# Realistic replay (with timestamps, 10x speed)
python storage_benchmark.py --scenario=toolagent --max-requests=1000 \
    --replay-timestamps --time-scale=0.1

# Test all scenarios with replay
python storage_benchmark.py --scenario=all --time-scale=1.0
```

## Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--trace-dir` | Trace files directory | `../FAST25-release/traces` |
| `--scenario` | Test scenario: `conversation`, `synthetic`, `toolagent`, `all` | `toolagent` |
| `--storage-dir` | Storage directory | `/tmp/mooncake_bench` |
| `--model` | Model preset (overrides `--bytes-per-token`) | `default` |
| `--bytes-per-token` | Bytes per token (2048 for 7B FP16) | `2048` |
| `--max-requests` | Maximum requests per scenario (unlimited if not specified) | `None` |
| `--max-blocks` | Maximum number of blocks | `100000` |
| `--replay-timestamps` | Enable timestamp replay | `False` |
| `--time-scale` | Time scaling factor (1.0 = real-time, 0.1 = 10x faster) | `1.0` |

## Model Presets

The tool includes presets for popular LLM models with accurate KVCache sizes based on the [LMCache KVCache Calculator](https://lmcache.ai/kv_cache_calculator.html).

| Model | Bytes/Token | Size | Notes |
|-------|-------------|------|-------|
| **Small Models (7B-13B)** |
| `llama-3-8b` | 128 | 128 B/token | GQA optimized |
| `mistral-7b` | 128 | 128 B/token | GQA optimized |
| `qwen-14b` | 40 | 40 B/token | GQA optimized |
| `gemma-7b` | 224 | 224 B/token | |
| `llama-2-7b` | 512 | 512 B/token | |
| `llama-2-13b` | 800 | 800 B/token | |
| **Large Models (70B-405B)** |
| `llama-2-70b` | 320 | 320 B/token | GQA optimized |
| `llama-3-70b` | 320 | 320 B/token | GQA optimized |
| `mixtral-8x7b` | 128 | 128 B/token | GQA optimized |
| `mixtral-8x22b` | 224 | 224 B/token | GQA optimized |
| `qwen-72b` | 320 | 320 B/token | GQA optimized |
| `qwen-110b` | 320 | 320 B/token | GQA optimized |
| `llama-3.1-405b` | 516018 | ~504 KB/token | Very large KVCache |
| **Extra Large Models** |
| `glm-4.6` | 156991 | ~153 KB/token | |
| `deepseek-v3` | 1749384 | ~1.67 MB/token | Largest KVCache |
| **Default** |
| `default` | 2048 | 2 KB/token | Legacy 7B FP16 |

**Usage**: `--model=llama-3.1-405b` (overrides `--bytes-per-token`)

## Test Scenarios

- **`conversation`**: Write-intensive workload (dialogue patterns)
- **`synthetic`**: Read-intensive workload (cached patterns)
- **`toolagent`**: Balanced read/write mix (tool use patterns)

## Output Example

```
================================================================================
Mooncake KVCache Storage Benchmark
================================================================================
Using model preset: llama-3.1-405b (516018 bytes/token, ~504.0 KB/token)

[1/1] toolagent_trace.jsonl
================================================================================

[Performance Overview]
  Total Requests:           100
  Queries Per Second (QPS): 14.45
  Cache Hit Rate:           24.27%
  Write Ratio:              75.73%
  Total Blocks:             1,949
    Read Blocks:            473
    Write Blocks:           1,476
    Prefix Hits:            376

[Latency Analysis]
  Request Latency (End-to-End): Avg=69.18ms, P50=15.49ms, P95=239.99ms, P99=310.58ms
  Single I/O Operation (Per Block):
    Read:  Avg=14.572ms, P50=0.280ms, P95=0.280ms, P99=0.280ms
    Write: Avg=5.120ms, P50=5.120ms, P95=5.120ms, P99=5.120ms

[I/O & Bandwidth]
  Total Read I/O:          473.0 MB  (473 ops)
  Total Write I/O:       1476.0 MB  (1,476 ops)
  Effective Bandwidth:    280.8 MB/s

[Storage Details]
  Blocks in Use:            1,476
  Free Blocks:                  0
  Tokens per Block:            512
  Block Size:                   1.00 MB

[Execution Time]
  Total Execution Time:      8.42 s

================================================================================
```

## Metrics

| Metric | Description |
|--------|-------------|
| **QPS** | Queries per second (based on I/O time, excluding sleep) |
| **Request Latency** | End-to-end latency for entire request (all I/O operations) |
| **Single I/O Latency** | Latency for individual block read/write operations (512 tokens) |
| **P50/P95/P99** | Latency percentiles (milliseconds) using linear interpolation |
| **Hit Rate** | Cache hit ratio for blocks |
| **Write Ratio** | Percentage of blocks that needed to be written |
| **Bandwidth** | Effective throughput based on I/O time only |
| **Prefix Hits** | Number of blocks served from prefix cache |

**Note**: Request Latency measures the total time to process all blocks in a request, while Single I/O Latency measures the time for one block operation (512 tokens).

## Trace Data Format

```json
{
  "timestamp": 1234.567,
  "hash_ids": [1, 2, 4, 7],
  "input_length": 2048,
  "output_length": 512
}
```

Each `hash_id` corresponds to a 512-token block. The tool simulates prefix caching by checking if blocks are already in storage before writing.

## Requirements

- Python 3.8+
- No third-party dependencies (standard library only)
