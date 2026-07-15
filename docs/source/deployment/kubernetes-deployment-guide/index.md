# Kubernetes Deployment Guide

Run Mooncake on Kubernetes as a shared **Store** cluster paired with SGLang **prefill/decode** inference — the Store serves as a HiCache L3 backend, while Mooncake's **Transfer Engine** moves KV cache directly between prefill and decode.

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

See also the [Mooncake Store Deployment & Tuning Guide](../mooncake-store-deployment-guide.md) for the component overview, client configuration, and tuning knobs.

:::{toctree}
:maxdepth: 1
:hidden:

mooncake-on-kubernetes
rbg-integration
:::
