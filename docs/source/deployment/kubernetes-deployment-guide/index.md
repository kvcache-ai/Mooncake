# Kubernetes Deployment Guide

Run Mooncake on Kubernetes as a shared **Store** cluster. The primary scenario below pairs it with SGLang **prefill/decode** inference — the Store serves as a HiCache L3 backend, while Mooncake's **Transfer Engine** moves KV cache directly between prefill and decode. The same Store also backs other orchestrators and engine stacks: see [RBG Integration](rbg-integration) for the RBG operator, and [llm-d Integration](llm-d-integration) for vLLM-based KV offloading and P/D transfer under llm-d.

---

## Store + Transfer Engine (P/D disaggregation)

A long-lived `mooncake-master` plus a replicated set of `mooncake-store` nodes form a shareable DRAM KV pool. The SGLang **prefill** pods use that pool as their hierarchical-cache L3 backend; **prefill and decode** use Mooncake's Transfer Engine for zero-copy P/D KV transfer over RDMA/TCP. A router fronts the prefill and decode endpoints.

```
                 Store cluster  (no GPU)
           +--------------------------------------------+
           |  mooncake-master      metadata + RPC       |
           |  mooncake-store ×N     (DRAM KV pool)      |
           +--------------------------------------------+
                     ▲
          HiCache L3 │ (Get/Put) + metadata / RPC
                     │
               +-----┴-----+   Transfer    +-----------+
               |  SGLang   |    Engine     |  SGLang   |
               |  Prefill  |◄═════════════►|  Decode   |
               |   (GPU)   |   KV blocks   |   (GPU)   |
               +-----┬-----+  (RDMA/TCP)   +-----┬-----+
                     ▲                           ▲
                     │                           │
  +--------+   +-----┴---------------------------┴-----+
  | client |──►|             sglang-router             |
  +--------+   +---------------------------------------+
```

**This section covers:**

- [Mooncake on Kubernetes](mooncake-on-kubernetes) — stand up the Mooncake Store cluster with plain `Deployment` / `Service` objects.
- [RBG Integration](rbg-integration) — the full Store + P/D scenario with the [sgl-project/rbg](https://github.com/sgl-project/rbg) operator, including a production Mooncake cluster case.
- [llm-d Integration](llm-d-integration) — Mooncake's two integration points in [llm-d](https://github.com/llm-d/llm-d): the Store as a vLLM KV offload tier, and `MooncakeConnector` for P/D transfer. Links to the upstream llm-d examples.

See also the [Mooncake Store Deployment & Tuning Guide](../mooncake-store-deployment-guide.md) for the component overview, client configuration, and tuning knobs.

:::{toctree}
:maxdepth: 1
:hidden:

mooncake-on-kubernetes
rbg-integration
llm-d-integration
:::
