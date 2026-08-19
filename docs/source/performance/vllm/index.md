# vLLM Integration Performance

Benchmarks evaluating Mooncake's integration with vLLM across different backends and scenarios.

| Document | Scenario | Highlights |
|----------|----------|---------------|
| [PD Disaggregation Performance](vllm-v1-pd-performance) | PD disaggregation with Mooncake Connector | 1P1D PD disaggregation on H800 with 8x RoCE: **142.25 GB/s** peak transfer bandwidth (71.1% of theoretical), KV transfer overhead just **4.2%** of total TTFT at 32K tokens |
| [vLLM x Mooncake Store Performance](vllm-v1-mooncake-store) | distributed KV cache pool with Mooncake Store | Distributed KV cache pool improves throughput by **3.8x**, reduces P50 TTFT and E2E latency by **46x** and **8.6x**, and scales to **60 GPUs** with >95% cache hit rate |

:::{toctree}
:maxdepth: 1
:hidden:

vllm-v1-pd-performance
vllm-v1-mooncake-store

:::
