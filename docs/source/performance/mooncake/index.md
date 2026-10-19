# Mooncake Performance

This section collects Mooncake performance evaluations and benchmark results for Mooncake core components.

| Document | Area | Key Findings |
|----------|------|---------------|
| [Storage Benchmark](storage-benchmark) | Mooncake Store storage | Measures end-to-end storage performance across Mooncake Store operations and deployment configurations. |
| [Allocator Benchmark](allocator-benchmark-result) | Segment allocation | The optimized OffsetAllocator significantly improves utilization for uniform-size LLM KV cache allocation patterns. |
| [Allocation Strategy Benchmark](allocation-strategy-benchmark-result) | Allocation routing | Compares random and free-ratio-first allocation across segments, replicas, skewed capacity, and DSA-style KV+indexer workloads. |
| [SSD Offload Benchmark](ssd-offload-benchmark-results) | Cache hierarchy | SSD offload extends the KV cache hierarchy with NVMe, reducing the performance cliff after DRAM cache capacity is exhausted in long multi-turn conversations. |
| [Store-RS Benchmark Guide](store-rs-benchmark) | Rust-native Mooncake Store | CLI reference for throughput, latency, correctness verification, and soak runs. |
| [Store-RS Cold-Tier Performance Design](store-rs-cold-tier-performance) | Cold-tier storage | Optimization priorities and profiling guidance for ExtentStore; this page records design direction rather than benchmark results. |
| [Guide for tebench](tebench) | Transfer Engine | End-to-end bandwidth and latency benchmarking for classic TE and TENT backends across block size, batch size, and concurrency. |

:::{toctree}
:maxdepth: 1
:hidden:

storage-benchmark
allocator-benchmark-result
allocation-strategy-benchmark-result
ssd-offload-benchmark-results
store-rs-benchmark
store-rs-cold-tier-performance
tebench
:::
