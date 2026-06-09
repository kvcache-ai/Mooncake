# Mooncake Store Performance Benchmarks

Benchmarks evaluating Mooncake Store's core modules and storage subsystems.

| Document | Module | Key Findings |
|----------|--------|---------------|
| [KVCache Storage Benchmark](../storage-benchmark) | Storage I/O | 100GB single-file I/O throughput, hash-based prefix cache lookup, timestamp-based request replay for realistic KVCache workloads |
| [Allocator Performance](../allocator-benchmark-result) | OffsetAllocator / MmapArena | Optimized OffsetAllocator achieves significant utilization gains for uniform-size workloads; MmapArena lock-free allocation at ~50ns |
| [Allocation Strategy](../allocation-strategy-benchmark-result) | Random vs FreeRatioFirst | Benchmarks segment routing overhead and memory utilization across 1–16 segments with varying replica counts |
| [SSD Offload](../ssd-offload-benchmark-results) | NVMe Tier | Enabling SSD offload cuts **average TTFT by 57%** vs GPU-only and **34%** vs Mooncake without SSD on a DGX node (8× A100), with **2.4×** input throughput improvement |

:::{toctree}
:maxdepth: 1
:hidden:

../storage-benchmark
../allocator-benchmark-result
../allocation-strategy-benchmark-result
../ssd-offload-benchmark-results
:::
