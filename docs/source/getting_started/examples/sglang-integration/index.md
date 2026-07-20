# Mooncake x SGLang Integration

Mooncake integrates with SGLang through two paths — **PD Disaggregation** for cross-instance KV cache transfer via the Transfer Engine, and **HiCache L3 Backend** for hierarchical KV cache storage with Mooncake Store.

---

## PD Disaggregation

SGLang uses Mooncake's Transfer Engine for direct zero-copy KV cache transfer between prefill and decode instances over RDMA, with support for EP and EPD backends. In benchmarks, PD disaggregation with Mooncake achieves **~30% lower ITL** while maintaining comparable throughput.

```
  +-----------+   Transfer Engine (RDMA)   +------------+
  | SGLang    | ◄━━━━━━━━━━━━━━━━━━━━━━►   | SGLang     |
  | Prefill   |     KV cache blocks        | Decode     |
  +-----------+                            +------------+
```

**Related:** [Full PD Disaggregation Guide](../sglang-integration-v1) — installation, cross-node/same-node setup, XpYd topology, EP backend for MoE models, and EPD backend for multimodal models.

**Benchmark:** [PD Disaggregation Performance](../../../performance/sglang-benchmark-results-v1) — compares 1P1D disaggregation with regular SGLang instances.

---

## HiCache with Mooncake Store

HiCache extends SGLang's RadixAttention with three memory tiers, using Mooncake Store as the distributed L3 backend. When local cache misses, HiCache automatically prefetches KV blocks from remote storage via RDMA.

```
  +----------------------------------------------+
  | SGLang + HiCache                             |
  |  ┌─────────┐  ┌─────────┐  ┌──────────────┐  |
  |  │ L1(GPU) │  │ L2(CPU) │  │ L3(Mooncake) │  |
  |  └─────────┘  └─────────┘  └──────┬───────┘  |
  +-----------------------------------+----------+
                                      |
                             +--------+---------+
                             | Mooncake Store   |
                             | Distributed Pool |
                             +------------------+
```

**Related:**
- [HiCache Quick Start](hicache-quick-start) — minimal setup steps with hugepage sizing
- [HiCache Complete Guide](hicache-integration-v1) — full deployment, prefetch strategies, memory tuning, and architecture deep dive
- [SGLang Performance Benchmarks](../../../performance/sglang/index) — benchmark overview for PD disaggregation and HiCache with Mooncake

::::{toctree}
:maxdepth: 1
:hidden:

../sglang-integration-v1
hicache-quick-start
hicache-integration-v1
::::
