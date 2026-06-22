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

**Event-driven scheduler (MultiLRU policy):**

Selected with `"type": "event_driven"` (top-level scheduler selector; defaults
to `"legacy"`, which uses the `policy` field above). The MultiLRU policy drives
TinyLFU-ranked, watermark-based fast-tier eviction. Its eviction **trigger** is a
watermark that floats within `[evict_watermark_low, evict_watermark_high]`: it
rests at the high bound when idle and floats **down** toward the low bound as
write load rises (evicting earlier to keep headroom for incoming writes).

*Eviction trigger & reclaim rate:*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `evict_watermark_low` | double | `0.70` | Lower bound (0–1) of the floating eviction trigger; also the reclaim floor and the startup value (startup assumes high write load). |
| `evict_watermark_high` | double | `0.90` | Upper bound (0–1) of the floating eviction trigger (the no-write-load resting point). Must be ≥ `evict_watermark_low`. |
| `limit_watermark` | double | `0.95` | Usage ratio (0–1) at which the proportional reclaim rate saturates to its max. Must be ≥ `evict_watermark_high`. |
| `evict_rate_k` | double | `1.0` | Proportional reclaim-rate coefficient. Each pass reclaims `rate · (used − low·capacity)`, where `rate = evict_rate_k · (usage − low) / (limit − low)` clamped to [0,1]. Higher = reclaim more per pass. |
| `evict_load_window_s` | double | `2.0` | Write-load response window in seconds. A sustained throughput that would fill the fast tier within this window drives the trigger fully down to `evict_watermark_low`; also the reaction time constant. Larger = more sensitive to slow writes but slower to react. Must be > 0. |

*Promotion / demotion (TinyLFU-gated):*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `offload_freq_threshold` | integer | `2` | A fast-tier key is offloaded (a slow-tier replica is pre-created while the fast copy is kept, so a later eviction is a cheap drop) only when its TinyLFU frequency **exceeds** this and the slow tier has room. Lower = pre-demote more eagerly. |
| `onboard_freq_threshold` | integer | `4` | A slow-tier key is promoted (onboarded) to the fast tier only when its TinyLFU frequency **exceeds** this. Lower = promote more eagerly. |
| `onboard_fast_threshold` | double | `0.50` | Fast-tier usage-ratio (0–1) ceiling for onboarding: promotion is skipped when fast usage is at/above this (hysteresis, so onboarding does not fight eviction). Should be < `evict_watermark_low`. |

*Frequency sketch & scan:*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sketch_capacity` | integer | `65536` | Counter count of the TinyLFU Count-Min frequency sketch (table = `capacity/4` 64-bit words; the halving-decay sample size derives from it). Larger = more accurate frequency estimates at higher memory cost. |
| `candidate_scan_limit` | integer | `4096` | Max eviction-candidate victims scanned per pass. `0` = unlimited (scan all). |

*Heat bands (MultiLRU):*

Eviction candidates are ordered coldest-first across four heat bands derived from a key's TinyLFU frequency: cold `[0, warm)`, warm `[warm, hot)`, hot `[hot, very_hot)`, very-hot `[very_hot, ∞)`. These cutoffs set how aggressively rising frequency protects a key from eviction.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `band_warm_threshold` | integer | `3` | Frequency at/above which a key leaves the cold band. Must be ≥ 1. |
| `band_hot_threshold` | integer | `8` | Frequency at/above which a key enters the hot band. Must be > `band_warm_threshold`. |
| `band_veryhot_threshold` | integer | `15` | Frequency at/above which a key enters the very-hot (most-protected) band. Must be > `band_hot_threshold`. |

*Reporting:*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `hot_key_num` | integer | `2000` | Default number of hottest keys reported (e.g. to HA recovery) when no explicit count is requested. `0` = all tracked keys. Also honored by the legacy scheduler. |

> Out-of-range values are clamped at startup with a warning: `evict_watermark_low ≤ evict_watermark_high ≤ limit_watermark`, `evict_load_window_s > 0`, and `1 ≤ band_warm_threshold < band_hot_threshold < band_veryhot_threshold`.

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

### Multi-tier – DRAM + STORAGE, event-driven (MultiLRU) scheduler

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
            "storage_filepath": "/data/mooncake"
        }
    ],
    "scheduler": {
        "type": "event_driven",
        "loop_interval_ms": 1000,
        "evict_watermark_low": 0.70,
        "evict_watermark_high": 0.90,
        "limit_watermark": 0.95,
        "evict_rate_k": 1.0,
        "evict_load_window_s": 2.0,
        "offload_freq_threshold": 2,
        "onboard_freq_threshold": 4,
        "onboard_fast_threshold": 0.50,
        "sketch_capacity": 65536,
        "candidate_scan_limit": 4096,
        "hot_key_num": 2000
    }
}
```

> All scheduler fields are optional; the values above are the defaults. Omit any
> field to accept its default.
