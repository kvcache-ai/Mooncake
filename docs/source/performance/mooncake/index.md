# Mooncake Performance Benchmarks

Benchmarks evaluating Mooncake Store's core storage, allocation, and cache hierarchy behavior.

| Document | Area | Key Findings |
|----------|------|---------------|
| [Storage Benchmark](../storage-benchmark) | Mooncake Store storage | Measures end-to-end storage performance across Mooncake Store operations and deployment configurations. |
| [Allocator Benchmark](../allocator-benchmark-result) | Segment allocation | The optimized OffsetAllocator significantly improves utilization for uniform-size LLM KV cache allocation patterns. |
| [Allocation Strategy Benchmark](../allocation-strategy-benchmark-result) | Allocation routing | Compares random and free-ratio-first allocation across segments, replicas, skewed capacity, and DSA-style KV+indexer workloads. |
| [SSD Offload Benchmark](../ssd-offload-benchmark-results) | Cache hierarchy | SSD offload extends the KV cache hierarchy with NVMe, reducing the performance cliff after DRAM cache capacity is exhausted in long multi-turn conversations. |

:::{toctree}
:maxdepth: 1
:hidden:

../storage-benchmark
../allocator-benchmark-result
../allocation-strategy-benchmark-result
../ssd-offload-benchmark-results
:::
