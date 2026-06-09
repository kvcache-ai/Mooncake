# Mooncake x vLLM Integration

## Overview

Mooncake integrates with vLLM to accelerate large language model serving through high-performance KV cache transfer and shared storage. The integration supports two primary scenarios:

- **Disaggregated Prefill-Decode Serving**: Seamlessly split prefill and decode across nodes using `MooncakeConnector`, with RDMA-powered cross-node KV cache transfer achieving up to **142.25 GB/s** peak bandwidth (71.1% utilization of 8x RoCE). Transfer overhead is negligible &mdash; for 32K-token prompts (4.50 GB of KV data), transfer takes only **31.65 ms**, accounting for just **4.2%** of total TTFT.
- **KV Cache Storage & Sharing**: Extend effective KV cache capacity via `MooncakeStore` / `MooncakeStoreConnector`, with hash-based prefix caching that enables multiple vLLM instances to share cached KV blocks. Supports CPU/Disk offloading and dynamic XpYd topologies at runtime.

| Scenario | Guide | vLLM Backend |
|----------|-------|-------------|
| PD Disaggregation (KV transfer) | [Disaggregated Prefill-Decode](disagg-prefill-decode) | V1 ✅ / V0 ⚠️ |
| KV Cache Storage & Sharing | [KV Cache Storage with MooncakeStore](kv-cache-storage) | V1 ✅ / V0 ⚠️ |

```{admonition} New to Mooncake + vLLM?
:class: tip
Start with the V1 guides above. Legacy V0 documentation is available for existing deployments only.
```

---

## Getting Started

### Disaggregated Prefill-Decode

Direct KV cache transfer between prefill and decode nodes via `MooncakeConnector` using RDMA.

::::{toctree}
:maxdepth: 1

disagg-prefill-decode
::::

### KV Cache Storage & Sharing

Distributed KV cache storage via `MooncakeStore` / `MooncakeStoreConnector` for offloading, prefix caching, and cross-instance sharing.

::::{toctree}
:maxdepth: 1

kv-cache-storage
::::

---

## Archived Documentation

The following pages are from earlier versions of the integration and are no longer maintained. All content has been consolidated into the scenario-based guides above.

- [MooncakeStoreConnector (Original)](vllm-mooncakestoreconnector)
- [vLLM V0 PD Disaggregation Demo (Original)](vllm-integration-v0.2)
- [vLLM V0 MooncakeStore (Original)](vllm-integration-v0.3)
- [vLLM V1 PD Disaggregation (Original)](vllm-integration-v1.0)
