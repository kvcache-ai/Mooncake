# TieredBackend Configuration Reference

This document describes every JSON field consumed by `TieredBackend::Init()` and
the client scheduler when initialising a P2P real client.

> ⚠️ **Subsystem refactoring notice.** The entire `TieredBackend` subsystem is
> under active development and **will be refactored**. Tier semantics, scheduler
> selection, and the configuration keys documented here may change
> significantly — and without backward compatibility — in a future release. Pin
> to a specific commit if you depend on the current behaviour, and re-read this
> reference after upgrading.
>
> Within the scheduler, the **legacy scheduler is deprecated** and will be
> removed; new deployments should use `"type": "event_driven"`. See
> [Scheduler Fields](#scheduler-fields-scheduler-object).

The configuration is passed via `tiered_backend_config_json` in
`ClientConfigBuilder::build_p2p_real_client()`. It accepts either:
- An **inline JSON string** (the string must start with `{`)
- A **file path** to a JSON file (default: `conf/tiered_backend.json`)

Two ready-to-use example files ship in this directory:
- [`tiered_backend.json`](tiered_backend.json) — a single DRAM tier (the
  default, loaded when no path is supplied).
- [`tiered_backend_dram_ssd.json`](tiered_backend_dram_ssd.json) — a DRAM (fast)
  + STORAGE/SSD (slow) two-tier setup with the event-driven scheduler.

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

Persists data to local NVMe/SSD via a staging DRAM pool. Typically assigned the lowest priority. When paired with a DRAM tier under the event-driven scheduler, this tier acts as the slow/offload destination: `offload_freq_threshold` controls when DRAM keys are pre-copied here, and `onboard_freq_threshold` controls promotion back to DRAM (see [Scheduler Fields](#scheduler-fields-scheduler-object) below).

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

The scheduler drives background promotion, eviction, and statistics collection. The implementation is selected by the `"type"` key inside the `"scheduler"` object:

| `"type"` | Status | Description |
|----------|--------|-------------|
| `"event_driven"` | **recommended** | TinyLFU + MultiLRU, watermark-based eviction with onboard/offload between tiers. |
| `"legacy"` *(default)* | **deprecated** | Original `SIMPLE`/`LRU` scheduler. Used when `"type"` is omitted. Will be removed. |

> **Note:** When `"type"` is omitted the **legacy** scheduler is selected for backward compatibility, *not* the recommended one. Set `"type": "event_driven"` explicitly. An unknown `"type"` logs a warning and falls back to legacy.

> **Numeric values only.** Unlike tier `capacity` and the STORAGE byte-size fields, scheduler numeric fields are read as plain numbers (`asUInt64`/`asInt`/`asDouble`). Human-readable strings such as `"64MB"` are **not** accepted here — use integers/doubles.

---

### Event-driven scheduler (`"type": "event_driven"`) — **recommended**

The event-driven scheduler uses a pluggable policy (default `multi_lru`: a MultiLRU backed by a TinyLFU frequency sketch). Its eviction **trigger** is a watermark that floats within `[evict_watermark_low, evict_watermark_high]`: it rests at the high bound when idle and floats **down** toward the low bound as write load rises (evicting earlier to keep headroom for incoming writes).

**Tier roles (fast / slow).** The event-driven scheduler does **not** key off tier *type*. It resolves two roles from the live tier topology:
- **auto** mode (default): `fast` = highest-`priority` tier, `slow` = next-highest. With a single tier there is **no slow role**, so onboard/offload are disabled and only fast-tier watermark eviction applies.
- **manual** mode: roles follow `fast_tier_tag` / `slow_tier_tag` (falling back to auto when a tag is empty or unmatched). Priority-inverting manual tags are rejected with a warning and auto-corrected.

Because roles come from priority/tags, any tier type (DRAM, NVMe, CXL, NPU, …) can play either role — give it a `priority` (and optionally a tag) and the role falls out.

> **Which knobs matter where.** In a **single-tier (pure DRAM)** deployment only the *eviction-trigger / reclaim* and *victim-ranking* knobs are active (watermarks, sketch, heat bands, `candidate_scan_limit`); the *promotion / demotion* group and the role-tag fields are inert because there is no slow role. In a **multi-tier (DRAM + SSD)** deployment all groups apply. Fields tagged *(advanced)* below have sound defaults and rarely need tuning.

*Policy & roles:* — *(advanced; only relevant in `manual` mode / for custom policies)*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `policy` | string | `"multi_lru"` | *(advanced)* Event-driven policy to instantiate (registry-backed). Currently `"multi_lru"` is the only built-in. An unknown name warns and falls back to `"multi_lru"`. |
| `tier_role_mode` | string | `"auto"` | `"auto"`: roles follow tier `priority`. `"manual"`: roles follow `fast_tier_tag` / `slow_tier_tag`. |
| `fast_tier_tag` | string | `""` | Manual mode only: tag identifying the fast tier. Empty ⇒ fall back to highest priority. |
| `slow_tier_tag` | string | `""` | Manual mode only: tag identifying the slow tier. Empty ⇒ fall back to next-highest priority. |

*Concurrency:* — *(advanced — defaults are fine for most)*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `thread_count` | integer | `2` | *(advanced)* Worker threads executing movement (onboard / offload / evict) tasks. Clamped to ≥ 1. |
| `queue_capacity` | integer | `1024` | *(advanced)* Capacity of the bounded, deduplicating movement-task queue. |

*Eviction trigger & reclaim rate:* — *active in single- and multi-tier; the watermarks are the primary tuning lever*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `evict_watermark_low` | double | `0.70` | Lower bound (0–1) of the floating eviction trigger; also the reclaim floor and the startup value (startup assumes high write load). |
| `evict_watermark_high` | double | `0.90` | Upper bound (0–1) of the floating eviction trigger (the no-write-load resting point). Must be ≥ `evict_watermark_low`. |
| `limit_watermark` | double | `0.95` | Usage ratio (0–1) at which the proportional reclaim rate saturates to its max. Must be ≥ `evict_watermark_high`. |
| `evict_rate_k` | double | `1.0` | *(advanced)* Proportional reclaim-rate coefficient. Each pass reclaims `rate · (used − low·capacity)`, where `rate = evict_rate_k · (usage − low) / (limit − low)` clamped to [0,1]. Higher = reclaim more per pass. |
| `evict_load_window_s` | double | `2.0` | *(advanced)* Write-load response window in seconds. A sustained throughput that would fill the fast tier within this window drives the trigger fully down to `evict_watermark_low`; also the reaction time constant. Larger = more sensitive to slow writes but slower to react. Must be > 0. |

*Promotion / demotion (TinyLFU-gated):* — *multi-tier only; inert when there is no slow tier (single DRAM). Defaults are fine for most.*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `offload_freq_threshold` | integer | `2` | A fast-tier key is offloaded (a slow-tier replica is pre-created while the fast copy is kept, so a later eviction is a cheap drop) only when its TinyLFU frequency **exceeds** this and the slow tier has room. Lower = pre-demote more eagerly. |
| `onboard_freq_threshold` | integer | `4` | A slow-tier key is promoted (onboarded) to the fast tier only when its TinyLFU frequency **exceeds** this. Lower = promote more eagerly. |
| `onboard_fast_threshold` | double | `0.50` | Fast-tier usage-ratio (0–1) ceiling for onboarding: promotion is skipped when fast usage is at/above this (hysteresis, so onboarding does not fight eviction). Should be < `evict_watermark_low`. |

*Frequency sketch & scan:* — *(advanced — defaults are fine for most; active in single- and multi-tier victim ranking)*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sketch_capacity` | integer | `65536` | *(advanced)* Counter count of the TinyLFU Count-Min frequency sketch (table = `capacity/4` 64-bit words; the halving-decay sample size derives from it). Larger = more accurate frequency estimates at higher memory cost. |
| `candidate_scan_limit` | integer | `4096` | *(advanced)* Max eviction-candidate victims scanned per pass. `0` = unlimited (scan all). |

*Heat bands (MultiLRU):* — *(advanced — defaults are fine for most; active in single- and multi-tier victim ranking)*

Eviction candidates are ordered coldest-first across four heat bands derived from a key's TinyLFU frequency: cold `[0, warm)`, warm `[warm, hot)`, hot `[hot, very_hot)`, very-hot `[very_hot, ∞)`. These cutoffs set how aggressively rising frequency protects a key from eviction.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `band_warm_threshold` | integer | `3` | *(advanced)* Frequency at/above which a key leaves the cold band. Must be ≥ 1. |
| `band_hot_threshold` | integer | `8` | *(advanced)* Frequency at/above which a key enters the hot band. Must be > `band_warm_threshold`. |
| `band_veryhot_threshold` | integer | `15` | *(advanced)* Frequency at/above which a key enters the very-hot (most-protected) band. Must be > `band_hot_threshold`. |

*Common fields (shared with legacy):*

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `loop_interval_ms` | integer | `1000` | Background scheduler worker loop interval in milliseconds. |
| `hot_key_num` | integer | `2000` | Default number of hottest keys reported (e.g. to HA recovery) when no explicit count is requested. `0` = all tracked keys. |

> Out-of-range values are clamped at startup with a warning: `evict_watermark_low ≤ evict_watermark_high ≤ limit_watermark`, `evict_load_window_s > 0`, and `1 ≤ band_warm_threshold < band_hot_threshold < band_veryhot_threshold`.

---

### Legacy scheduler (`"type": "legacy"`) — **deprecated**

> **Deprecated.** The legacy scheduler (used when `"type"` is omitted or set to `"legacy"`) and its `"SIMPLE"` / `"LRU"` policies will be removed in a future release. Migrate to `"type": "event_driven"`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `policy` | string | `"SIMPLE"` | Scheduling policy: `"SIMPLE"` (heat-score-based promotion) or `"LRU"` (least-recently-used eviction). |
| `eviction_mode` | string | `"async"` | `"async"`: eviction runs in the background worker loop. `"sync"`: eviction is triggered synchronously on allocation failure. |
| `loop_interval_ms` | integer | `1000` | Background scheduler worker loop interval in milliseconds. |
| `stats_shards` | integer | *(internal default)* | Shard count for the access-statistics collector. Increase to reduce lock contention under high concurrency. |
| `stats_snapshot_limit` | integer | *(internal default)* | Maximum number of entries in a single stats snapshot passed to the policy. |

**SIMPLE policy only (legacy):**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `promotion_threshold` | double | `5.0` | Minimum access count for a key to be eligible for promotion to a higher-priority tier. |

**LRU policy only (legacy):**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `high_watermark` | double | *(LRU default)* | Tier usage ratio (0–1) at which eviction begins. |
| `low_watermark` | double | *(LRU default)* | Tier usage ratio (0–1) at which eviction stops. |

---

## Complete Examples

The first two examples use the **recommended** event-driven scheduler and match
the files shipped in this directory. The legacy examples that follow are kept
for reference only and use the **deprecated** scheduler.

### 1. Single DRAM tier, event-driven scheduler — shipped default

This matches [`tiered_backend.json`](tiered_backend.json), the file loaded when
no config path is supplied. With a single tier there is no slow role, so the
scheduler only performs **fast-tier watermark eviction** — onboard/offload and
the role-tag fields are inert here. The eviction-trigger watermarks (and the
victim-ranking knobs) **do** apply, so the example sets the three watermark
fields explicitly; every other scheduler field falls back to its default.

```json
{
    "tiers": [
        {
            "type": "DRAM",
            "capacity": "100GB",
            "priority": 10,
            "allocator_type": "OFFSET"
        }
    ],
    "scheduler": {
        "type": "event_driven",
        "evict_watermark_low": 0.70,
        "evict_watermark_high": 0.90,
        "limit_watermark": 0.95
    }
}
```

### 2. DRAM (fast) + STORAGE/SSD (slow), event-driven scheduler — recommended multi-tier

This matches [`tiered_backend_dram_ssd.json`](tiered_backend_dram_ssd.json). The
higher-priority DRAM tier is the fast role and STORAGE is the slow role, so the
scheduler onboards hot keys into DRAM and offloads cold keys to SSD around the
floating eviction watermark. Every scheduler field shown is optional; the values
are the defaults — omit any field to accept its default.

```json
{
    "tiers": [
        {
            "type": "DRAM",
            "capacity": "16GB",
            "priority": 100,
            "allocator_type": "OFFSET"
        },
        {
            "type": "STORAGE",
            "capacity": "1TB",
            "priority": 10,
            "storage_backend_type": "bucket_storage_backend",
            "storage_filepath": "/data/mooncake",
            "staging_buffer_capacity": "2GB",
            "total_size_limit": "1TB",
            "bucket_size_limit": "256MB",
            "bucket_keys_limit": 500
        }
    ],
    "scheduler": {
        "type": "event_driven",
        "policy": "multi_lru",
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
        "band_warm_threshold": 3,
        "band_hot_threshold": 8,
        "band_veryhot_threshold": 15,
        "thread_count": 2,
        "queue_capacity": 1024,
        "hot_key_num": 2000
    }
}
```

---

### Legacy examples *(deprecated — kept for reference)*

> The examples below use the deprecated legacy scheduler (`"policy": "SIMPLE"` /
> `"policy": "LRU"`, no `"type": "event_driven"`). They still work today but will
> be removed. Prefer the event-driven examples above for new deployments.

#### Multi-tier – DRAM (hot) + STORAGE (warm/cold), SIMPLE scheduler *(legacy)*

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

#### Multi-tier – DRAM + STORAGE, LRU scheduler *(legacy)*

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
