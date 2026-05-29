# SGLang x Mooncake Integration

Mooncake integrates with SGLang through two paths — **PD Disaggregation** for cross-instance KV cache transfer via the Transfer Engine, and **HiCache L3 Backend** for hierarchical KV cache storage with Mooncake Store.

---

## PD Disaggregation

SGLang uses Mooncake's Transfer Engine for direct zero-copy KV cache transfer between prefill and decode instances over RDMA.

```
  +-----------+   Transfer Engine (RDMA)    +-----------+
  | SGLang    | ◄━━━━━━━━━━━━━━━━━━━━━━► | SGLang     |
  | Prefill   |     KV cache blocks        | Decode     |
  +-----------+                            +-----------+
```

Since [PR 5460](https://github.com/sgl-project/sglang/pull/5460), no `mooncake.json` is needed — devices are auto-detected.

```bash
# Prefill node
python -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
  --disaggregation-mode prefill \
  --port 30000 --host 192.168.0.137 --tp-size 2

# Decode node
python -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4 \
  --disaggregation-mode decode \
  --port 30001 --host 192.168.0.140 --tp-size 2

# Router (front the pair with sglang_router)
python3 -m sglang_router.launch_router \
  --pd-disaggregation \
  --prefill "http://192.168.0.137:30000" \
  --decode  "http://192.168.0.140:30001" \
  --policy round_robin \
  --host 0.0.0.0 --port 8000
```

Both prefill and decode can run on the same node with `--base-gpu-id` to avoid GPU conflicts. XpYd topology (multiple prefills, multiple decodes) is supported.

**Related:** [Full PD Disaggregation Guide](../sglang-integration-v1) — installation, EP/EPD backends, and advanced configuration.

---

## HiCache with Mooncake Store

HiCache extends SGLang's RadixAttention with three memory tiers, using Mooncake Store as the distributed L3 backend.

```
  +----------------------------------------------+
  | SGLang + HiCache                             |
  |  ┌─────────┐  ┌─────────┐  ┌──────────────┐ |
  |  │ L1(GPU) │  │ L2(CPU) │  │ L3(Mooncake) │ |
  |  └─────────┘  └─────────┘  └──────┬───────┘ |
  +------------------------------------+----------+
                                      |
                             +--------+--------+
                             | Mooncake Store  |
                             | Distributed Pool |
                             +-----------------+
```

When local cache misses, HiCache automatically prefetches KV blocks from remote storage via RDMA.

```bash
# 1. Start Mooncake Store master
mooncake_master --enable_http_metadata_server=true

# 2. Launch SGLang with HiCache + Mooncake L3
export MOONCAKE_MASTER=127.0.0.1:50051
export MC_STORE_USE_HUGEPAGE="1"

python -m sglang.launch_server \
  --model-path [model_path] \
  --page-size 64 \
  --enable-hierarchical-cache \
  --hicache-storage-prefetch-policy timeout \
  --hicache-storage-backend mooncake
```

**Related:**
- [HiCache Quick Start](hicache-quick-start) — comprehensive setup including hugepage sizing
- [HiCache Complete Guide](hicache-integration-v1) — prefetch strategies, tuning, and architecture deep dive

::::{toctree}
:maxdepth: 1
:hidden:

../sglang-integration-v1
hicache-quick-start
hicache-integration-v1
::::
