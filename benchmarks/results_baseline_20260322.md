# Mooncake Store Baseline Benchmark Results

**日期**: 2026-03-22
**节点**: skv-node1 (Xeon Gold 5218R 40c x2, 192GB DRAM, 100GbE ConnectX-6 DX)
**分支**: feat/optimize-mooncake-store (baseline, 未修改)
**编译**: gcc-11, Release, C++20

## 1. Allocator Benchmark (OffsetAllocator)

### Uniform Size Allocation
| Alloc Size | Time (ns/op) | Util Ratio |
|-----------|-------------|-----------|
| 32 B | 418 | 1.0 |
| 128 B | 343 | 1.0 |
| 512 B | 226 | 1.0 |
| 2 KB | 342 | 1.0 |
| 8 KB | 227 | 1.0 |
| 32 KB | 190 | 1.0 |
| 128 KB | 171 | 1.0 |
| 512 KB | 168 | 1.0 |
| 2 MB | 167 | 1.0 |
| 8 MB | 164 | 1.0 |
| 32 MB | 165 | 1.0 |

### Random Size Allocation
- Avg alloc time: **225.8 ns/op**
- Util ratio: min 0.55, avg 0.84, max 0.95

## 2. Allocation Strategy Benchmark

### Random Strategy (selected rows)
| Segments | AllocSize | Replicas | Throughput (ops/s) | Avg (ns) | P99 (ns) |
|---------|-----------|---------|-------------------|---------|---------|
| 1 | 512KB | 1 | 518,263 | 1,779 | 7,446 |
| 10 | 512KB | 1 | 2,210,516 | 427 | 1,991 |
| 100 | 512KB | 1 | 1,787,292 | 535 | 2,139 |
| 512 | 512KB | 1 | 1,514,124 | 636 | 2,005 |
| 1024 | 512KB | 1 | 1,230,833 | 788 | 2,304 |
| 10 | 8MB | 1 | 2,783,087 | 335 | 834 |
| 100 | 8MB | 1 | 2,098,817 | 452 | 997 |
| 1024 | 8MB | 1 | 1,235,704 | 783 | 2,383 |

### FreeRatioFirst Strategy (selected rows)
| Segments | AllocSize | Replicas | Throughput (ops/s) | Avg (ns) | P99 (ns) |
|---------|-----------|---------|-------------------|---------|---------|
| 1 | 512KB | 1 | 1,572,474 | 605 | 1,099 |
| 10 | 512KB | 1 | 1,224,018 | 789 | 1,242 |
| 100 | 512KB | 1 | 945,506 | 1,031 | 2,900 |
| 512 | 512KB | 1 | 749,636 | 1,304 | 4,151 |
| 1024 | 512KB | 1 | 662,338 | 1,483 | 4,965 |
| 10 | 8MB | 1 | 1,196,705 | 807 | 1,642 |
| 100 | 8MB | 1 | 964,240 | 1,010 | 2,389 |
| 1024 | 8MB | 1 | 633,246 | 1,550 | 5,047 |

### Key Observations
- **Random 比 FreeRatioFirst 快 1.5-2x** — 因为 FreeRatioFirst 需要扫描所有 segment
- **随 segment 数增加，FreeRatioFirst 退化明显** — 1024 segments 时 P99 达 5μs
- **Random 策略在高 segment 数下也有退化** — 但程度较轻

## 3. Master RPC Benchmark (from Master Metrics)

10s BatchPut test (2 clients, 2 threads each, batch_size=64):
- **Total BatchPutStart requests**: 70,266
- **Total items Put**: 4,497,024
- **Throughput**: ~7,000 batch req/s, ~450K items/s
- **Peak storage**: 13.81 GB / 256 GB

## Performance Bottleneck Analysis

1. **FreeRatioFirst O(n) scan**: 最明显的瓶颈，segment 数 1024 时吞吐量仅为 Random 的 54%
2. **Allocator 本身很快**: 164-430 ns，不是瓶颈
3. **Master RPC** 需要更详细的延迟分析（benchmark 客户端因日志问题崩溃）

## TODO
- [ ] 修复 benchmark 客户端的 TRACE 日志导致的崩溃问题
- [ ] 添加多线程并发 benchmark
- [ ] 添加 Remove 操作 benchmark
- [ ] 在多节点 RDMA 集群上跑端到端 benchmark
