# TieredBackend Configuration Reference

This document describes every JSON field consumed by `TieredBackend::Init()` and
`ClientScheduler` when initialising a P2P real client.

The configuration is passed via `tiered_backend_config_json` in
`ClientConfigBuilder::build_p2p_real_client()`. It accepts either:
- An **inline JSON string** (the string must start with `{`)
- A **file path** to a JSON file (default: `conf/tiered_backend.json`)

---

## Top-level Schema

```json
{
    "tiers": [ ... ],     // required – at least one tier
    "scheduler": { ... }  // optional – defaults apply when omitted
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tiers` | array | yes | Storage tier definitions. At least one entry required. |
| `scheduler` | object | no | Scheduler policy and tuning. All sub-fields optional. |

---

## Tier Fields

### Common Fields (all tier types)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | yes | Tier type: `"DRAM"`, `"ASCEND_NPU"`, `"STORAGE"`, or `"DISK"` (`"DISK"` is an alias for `"STORAGE"`) |
| `capacity` | integer or string | yes | Tier capacity. Accepts a plain byte count (`1073741824`) or a human-readable string (`"1GB"`, `"512MB"`, `"2TB"`). Strings are case-insensitive and may include a space before the unit. |
| `priority` | integer | yes | Scheduling priority. Higher value = preferred for acquired write or read route. |
| `tags` | string array | no | Arbitrary labels forwarded to the Master segment metadata. Used for topology-aware routing. |

**Supported size units for string capacity values:** `B`, `KB`/`K`, `MB`/`M`, `GB`/`G`, `TB`/`T`, `"infinite"` (→ `UINT64_MAX`)

---

### DRAM Tier (`"type": "DRAM"`)

Allocates memory directly from DRAM. Fastest tier; typically assigned the highest priority.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `allocator_type` | string | `"OFFSET"` | Buffer allocator implementation. `"OFFSET"` (offset allocator) or `"CACHELIB"` (Facebook CacheLib allocator). |
| `numa_node` | integer | *(none)* | Pin allocation to a specific NUMA node via `numa_alloc_onnode`. Negative values are ignored and allocation falls back to the system default. |

---

### ASCEND NPU Tier (`"type": "ASCEND_NPU"`)

Allocates on an Ascend NPU device. Only available when compiled with `USE_ASCEND_CACHE_TIER`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `device_id` | integer | `0` | Ascend device index. |

---

### STORAGE / DISK Tier (`"type": "STORAGE"` or `"type": "DISK"`)

Persists data to local NVMe/SSD via a staging DRAM pool. Typically assigned the lowest priority.

**Flush aggregation** — controls when the background flush thread writes staged data to disk:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `batch_size_threshold` | integer or string | `"64MB"` | Trigger a flush when the pending batch reaches this total byte size. Accepts human-readable strings. |
| `batch_count_threshold` | integer | `1000` | Trigger a flush when the pending batch contains this many keys. |
| `staging_buffer_capacity` | integer or string | `"128MB"` | Size of the staging DRAM pool used to buffer writes before they are flushed to disk.|

**Storage backend** — JSON values take precedence over the corresponding environment variable:

| Field | Type | Default | Env var override | Description |
|-------|------|---------|-----------------|-------------|
| `storage_backend_type` | string | `"bucket_storage_backend"` | `MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR` | StorageTier **only supports `"bucket_storage_backend"`**. Setting this to `"file_per_key_storage_backend"` will cause initialization to fail. |
| `storage_filepath` | string  | `"/data/file_storage"` | `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | Root directory for on-disk data files. Must be an absolute path. |
| `total_size_limit` | integer or string  | `"2TB"` | `MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES` | Maximum on-disk data size. Used as the `GetCapacity()` fallback when no explicit `capacity` is set. |
| `bucket_size_limit` | integer or string | `"256MB"` | `MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES` | Maximum total byte size of a single bucket. |
| `bucket_keys_limit` | integer  | `500` | `MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT` | Maximum number of keys per bucket. |

> **Note:** `heartbeat_interval_seconds`, `scanmeta_iterator_keys_limit`, and `total_keys_limit`
> are `FileStorageConfig` fields used exclusively by the centralized-mode `FileStorage` class.
> They have **no effect** in P2P StorageTier and should not be configured here.

---

## Scheduler Fields (`"scheduler"` object)

The scheduler drives background promotion, eviction, and statistics collection.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `policy` | string | `"SIMPLE"` | Scheduling policy: `"SIMPLE"` (heat-score-based promotion) or `"LRU"` (least-recently-used eviction). |
| `eviction_mode` | string | `"async"` | `"async"`: eviction runs in the background worker loop. `"sync"`: eviction is triggered synchronously on allocation failure. |
| `loop_interval_ms` | integer | `1000` | Background scheduler worker loop interval in milliseconds. |
| `stats_shards` | integer | *(internal default)* | Shard count for the access-statistics collector. Increase to reduce lock contention under high concurrency. |
| `stats_snapshot_limit` | integer | *(internal default)* | Maximum number of entries in a single stats snapshot passed to the policy. |

**SIMPLE policy only:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `promotion_threshold` | double | `5.0` | Minimum access count for a key to be eligible for promotion to a higher-priority tier. |

**LRU policy only:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `high_watermark` | double | *(LRU default)* | Tier usage ratio (0–1) at which eviction begins. |
| `low_watermark` | double | *(LRU default)* | Tier usage ratio (0–1) at which eviction stops. |

---

## Complete Examples

### Minimal – single DRAM tier

```json
{
    "tiers": [
        {
            "type": "DRAM",
            "capacity": "4GB",
            "priority": 10
        }
    ]
}
```

### Multi-tier – DRAM (hot) + STORAGE (warm/cold), SIMPLE scheduler

```json
{
    "tiers": [
        {
            "type": "DRAM",
            "capacity": "8GB",
            "priority": 100,
            "allocator_type": "OFFSET",
            "numa_node": 0,
            "tags": ["fast", "local"]
        },
        {
            "type": "STORAGE",
            "capacity": "500GB",
            "priority": 10,
            "tags": ["nvme"],
            "batch_size_threshold": "128MB",
            "batch_count_threshold": 2000,
            "storage_backend_type": "bucket_storage_backend",
            "storage_filepath": "/nvme/mooncake_store",
            "staging_buffer_capacity": "2GB",
            "total_size_limit": "500GB",
            "bucket_size_limit": "512MB",
            "bucket_keys_limit": 1000
        }
    ],
    "scheduler": {
        "policy": "SIMPLE",
        "promotion_threshold": 3.0,
        "eviction_mode": "async",
        "loop_interval_ms": 500
    }
}
```

### Multi-tier – DRAM + STORAGE, LRU scheduler

```json
{
    "tiers": [
        {
            "type": "DRAM",
            "capacity": "16GB",
            "priority": 100
        },
        {
            "type": "STORAGE",
            "capacity": "1TB",
            "priority": 10,
            "storage_filepath": "/data/mooncake",
            "local_buffer_size": "4GB"
        }
    ],
    "scheduler": {
        "policy": "LRU",
        "eviction_mode": "async",
        "high_watermark": 0.9,
        "low_watermark": 0.7,
        "loop_interval_ms": 1000
    }
}
```
