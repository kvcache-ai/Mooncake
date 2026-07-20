# vLLM Integration Performance Benchmarks

Benchmarks evaluating Mooncake's integration with vLLM across different backends and scenarios.

| Document | Backend | Key Findings |
|----------|---------|---------------|
| [vLLM V1 + MooncakeConnector](../vllm-v1-support-benchmark) | vLLM V1 | 1P1D PD disaggregation on H800 with 8x RoCE: **142.25 GB/s** peak transfer bandwidth (71.1% of theoretical), KV transfer overhead just **4.2%** of total TTFT at 32K tokens |
| [vLLM V1 + MooncakeStore vs Redis](../vllm-benchmark-results-v1) | vLLM V1 | MooncakeStore RDMA consistently outperforms Redis across all XpYd topologies — e.g., **~32% lower** mean TTFT in 2P2D tp=2 |
| [vLLM V0 + MooncakeConnector (Legacy)](../vllm-benchmark-results-v0.2) | vLLM V0 | TP=4 reduces TTFT by ~80% vs TP=1; RDMA provides significant latency advantage over TCP across varying QPS and input lengths |

:::{toctree}
:maxdepth: 1
:hidden:

../vllm-v1-support-benchmark
../vllm-benchmark-results-v1
../vllm-benchmark-results-v0.2
:::
