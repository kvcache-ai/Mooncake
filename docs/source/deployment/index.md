---
orphan: true
---

# Deployment

Deploy and operate Mooncake across standalone hosts, SSD-backed storage, and
Kubernetes environments.

| Guide | Description |
|-------|-------------|
| [Mooncake Store Deployment and Tuning](mooncake-store-deployment-guide) | Configure Store clients, metadata services, storage tiers, and production tuning. |
| [KV Cache Sharing and Isolation](kv-cache-sharing-and-isolation) | Define cache-sharing boundaries across models, releases, request groups, and Mooncake tenants. |
| [Kubernetes Deployment](kubernetes-deployment-guide/index) | Deploy Mooncake Store and Transfer Engine integrations on Kubernetes. |
| [SSD Storage](ssd/index) | Configure local SSD offload or a shared NVMe-over-Fabrics storage pool. |

## Framework Integrations

| Integration | Description |
|-------------|-------------|
| [SGLang](integrations/sglang/index) | Deploy PD disaggregation and HiCache L3 storage with Mooncake. |
| [vLLM](integrations/vllm/index) | Deploy disaggregated prefill/decode and shared KV cache storage. |
| [LMCache](integrations/lmcache/index) | Use Mooncake as a distributed storage backend for LMCache. |
| [LMDeploy](integrations/lmdeploy) | Configure Mooncake as the PD disaggregation backend for LMDeploy. |
