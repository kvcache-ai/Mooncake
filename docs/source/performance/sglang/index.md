# SGLang Integration Performance Benchmarks

Benchmarks evaluating Mooncake's integration with SGLang across PD disaggregation and HiCache hierarchical KV cache storage.

| Document | Scenario | Key Findings |
|----------|----------|---------------|
| [PD Disaggregation Performance](../sglang-benchmark-results-v1) | SGLang PD disaggregation with Mooncake Transfer Engine | 1P1D PD disaggregation achieves approximately **30% lower ITL** while maintaining comparable throughput against two regular instances. |
| [HiCache with Mooncake Backend Benchmark](../sglang-hicache-benchmark-results-v1) | SGLang HiCache using Mooncake Store as L3 storage | Mooncake-backed HiCache improves prefill performance in multi-turn workloads by maintaining higher KV cache hit rates as conversation rounds grow. |

:::{toctree}
:maxdepth: 1
:hidden:

../sglang-benchmark-results-v1
../sglang-hicache-benchmark-results-v1
:::
