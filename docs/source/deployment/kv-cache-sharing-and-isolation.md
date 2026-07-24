# KV Cache Sharing and Isolation

## Overview

Mooncake Store treats object keys as opaque strings. The inference framework constructs those keys and therefore defines which requests and instances may reuse the same KV cache entries.

A shared Mooncake Store can safely serve multiple models and releases when each cache-compatible group uses a stable, unique namespace. This is useful for:

- Hosting multiple model families on the same Store cluster.
- Rolling from one model revision to another without mixing their KV cache.
- Running canary, A/B, quantized, or fine-tuned variants side by side.
- Separating cache reuse by request group with `cache_salt`.
- Applying a Mooncake quota to each logical tenant.

## Choose the Isolation Boundary

The available isolation mechanisms are complementary:

| Boundary | Use it for | Configuration |
|----------|------------|---------------|
| Model | Different model families or incompatible model variants | SGLang `--served-model-name`; vLLM derives the name from the model path |
| Deployment or release | Rolling upgrades, canaries, and environments that use the same model name | SGLang `extra_backend_tag`; vLLM `cache_prefix` |
| Request group | Reuse within one application, user group, or other isolation domain | vLLM API `cache_salt` |
| Mooncake tenant | A logical object namespace with its own quota | Store client `tenant_id` and master `--enable_multi_tenants=true` |

Instances should use identical values only when their KV cache entries are compatible and are intended to be shared. Prefill and decode instances in the same deployment must therefore use the same values.

## SGLang

Use an SGLang version that includes [model-aware Mooncake key isolation](https://github.com/sgl-project/sglang/pull/31920), and set a stable, unique `--served-model-name`:

```bash
python -m sglang.launch_server \
  --model-path Qwen/Qwen3-8B \
  --served-model-name qwen3-8b \
  --enable-hierarchical-cache \
  --hicache-storage-backend mooncake
```

All SGLang instances that should share KV cache entries must use the same `--served-model-name`.

For two deployments of the same served model, add a release-specific `extra_backend_tag`:

```bash
python -m sglang.launch_server \
  --model-path /models/Qwen3-8B \
  --served-model-name qwen3-8b \
  --enable-hierarchical-cache \
  --hicache-storage-backend mooncake \
  --hicache-storage-backend-extra-config \
    '{"extra_backend_tag":"production-2026-07"}'
```

Keep the tag identical across compatible instances in one release, and change it for a release whose KV cache must not be reused.

## vLLM

The vLLM `MooncakeStoreConnector` automatically uses the final component of the model passed to `vllm serve` as its model identifier. For example, both `Qwen/Qwen3-8B` and `/models/Qwen3-8B` produce `Qwen3-8B`.

Set `cache_prefix` when paths with the same final component must remain separate, or when a deployment needs a release-specific namespace:

```bash
MOONCAKE_CONFIG_PATH=/path/to/mooncake_config.json \
vllm serve /models/Qwen3-8B \
  --kv-transfer-config '{
    "kv_connector": "MooncakeStoreConnector",
    "kv_role": "kv_both",
    "kv_connector_extra_config": {
      "cache_prefix": "production-2026-07"
    }
  }'
```

All vLLM instances that should share KV cache entries must use the same model and `cache_prefix`.

### Request-Level Isolation with `cache_salt`

vLLM also accepts an optional `cache_salt` in each OpenAI-compatible request:

```json
{
  "model": "qwen3-8b",
  "messages": [
    {
      "role": "user",
      "content": "Summarize this document."
    }
  ],
  "cache_salt": "application-a"
}
```

vLLM inserts the salt into the first KV block hash. Because subsequent block hashes depend on the preceding block, the salt affects the Mooncake keys for the request:

- Requests with the same salt can reuse compatible cached prefixes.
- Requests with different salts use separate cache entries.
- An omitted or empty salt retains the default shared-cache behavior.

LMCache uses the same isolation-domain convention when it propagates vLLM's `cache_salt` into its cache object identity: one stable salt defines one sharing group. Avoid generating a random salt for every request unless disabling cross-request reuse is intentional.

## Rolling Model Upgrades

During a rolling upgrade, the old and new releases may temporarily use the same Mooncake Store. Give each release a distinct deployment namespace:

| Release | SGLang `extra_backend_tag` | vLLM `cache_prefix` |
|---------|----------------------------|---------------------|
| Current | `production-2026-07` | `production-2026-07` |
| New | `production-2026-08` | `production-2026-08` |

A typical rollout is:

1. Keep all current-release instances on the existing namespace.
2. Start the new prefill and decode instances with the new namespace.
3. Warm the new release and gradually shift traffic to it.
4. Retire the old instances after in-flight requests finish.
5. Allow old cache entries to leave the Store through the normal eviction or cleanup lifecycle.

This approach prevents the new release from reading KV cache generated by the old release, while preserving cache reuse among instances of the same release. Plan for temporarily higher Store capacity because both releases may be warm at the same time.

Use a new namespace whenever cache compatibility may have changed, including changes to model weights, KV layout, quantization, adapters, or other inference settings that affect generated KV values.

## Mooncake Tenants

Mooncake tenant configuration is independent of the framework-level model, release, and request namespaces. When the master is started with `--enable_multi_tenants=true`, the client `tenant_id` selects a tenant-scoped object namespace and the master applies that tenant's quota during admission.

Use the same `tenant_id` for framework instances that should share one quota and tenant namespace. See [Tenant Quota Management](mooncake-store-deployment-guide.md#tenant-quota-management) for configuration details.

## Operational Checklist

- Use deterministic names that remain stable across restarts.
- Keep model and release fields identical across compatible prefill, decode, and replica instances.
- Change the release namespace before introducing a cache-incompatible deployment.
- Keep `cache_salt` stable within a group that should benefit from prefix reuse.
- Account for duplicate cache occupancy while old and new releases overlap.
