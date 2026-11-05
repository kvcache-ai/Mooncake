# DataManager V2 configuration

Select `"data_manager_version": "v2"` in the tier configuration file. The
[full example](datamanager_v2_full_example.json) contains the supported runtime
component sections and a DRAM/SSD layout using physical target arrays.
Omitting the new fields preserves the previous defaults.

## Runtime component sections

Counts must be JSON integers, booleans must be JSON booleans, and time values
are non-negative integer milliseconds with an `_ms` suffix. Invalid types,
out-of-range conversions and invalid component settings return
`INVALID_PARAMS`. Byte counts accept integers or size strings such as `"64MB"`
(binary units). The paths below are relative to the root `v2` object.

| Path | Default | Meaning |
| --- | ---: | --- |
| `migration.max_batch_items` | 16 | Maximum requests in one batch. `max_batch_size` is an alias; conflicting simultaneous values are rejected. |
| `migration.max_batch_bytes` | 64 MiB | Byte threshold for submitting/forming a batch. A block is not split, so the last item can take a batch above this threshold. |
| `migration.max_batch_delay_ms` | 20 | Maximum batching delay measured from the oldest queued request. Must be positive. |
| `migration.max_inflight_per_route` | 2 | Concurrent batches on one route. Must be positive. |
| `migration.max_inflight_per_device` | 4 | Concurrent batches targeting one logical tier, not one file within the pool. Must be positive. |
| `migration.retry_limit` | 2 | Maximum retries for transient failures. Zero disables retries. |
| `migration.retry_backoff_ms` | 50 | Retry backoff. Zero is allowed. |
| `migration.max_queued_requests` | 8192 | Queue limit across all routes. Zero means unbounded. |
| `movement_tracker.shard_count` | 16 | Movement-state table shards. Must be positive. |
| `movement_tracker.cooldown_ms` | 2000 | Cooldown after successful movement, tracked per direction. Zero disables this gate. |
| `movement_tracker.minimum_residency_ms` | 1000 | Minimum residence on the source tier before movement. Zero disables this gate. |
| `movement_tracker.max_tracked_keys` | 65536 | Budget for remembered movement-state records. Must be positive. |
| `frequency_tracker.shard_count` | 16 | Heat-tracker shards. Must be positive. |
| `frequency_tracker.max_snapshot_keys` | 4096 | Maximum hot-key snapshot size. Must be positive. |
| `frequency_tracker.default_hot_key_num` | 64 | Snapshot size used when the caller does not request a count. |
| `frequency_tracker.half_life_ms` | 30000 | Heat decay half-life. Must be positive. |
| `frequency_tracker.entry_ttl_ms` | 300000 | Untouched time after which a sufficiently cold record can expire. Must be positive. |
| `frequency_tracker.expiry_threshold` | 0.05 | Heat threshold used with entry TTL. Must be positive. |
| `frequency_tracker.max_tracked_keys` | 1048576 | Heat-record budget. Must be positive; this does not limit stored keys. |
| `movement.offload_high_watermark` | 0.9 | Offload default inherited by tiers without a per-tier override. Must be in `(0, 1)`. |
| `movement.onboard_min_read_heat` | 8.0 | Minimum read heat for onboarding. Zero disables onboarding. |
| `movement.movement_deadline_ms` | 30000 | Deadline budget assigned to a movement command. Must be positive. |
| `events.lifecycle_reserve` | 64 | Lifecycle-only slots per event queue. The queue clamps this to at most one quarter of its capacity. Zero disables the reservation. |
| `register_tiers_with_transfer_engine` | true | Register DRAM memory with TE. Set false only for deliberate local-only use; remote transfers require registration. An explicit false from the C++ factory config also disables registration. |

The actual concurrency is also limited by `v2.events.movement_worker_count`
(default 2). Raising inflight quotas alone does not create more workers.
Movement and frequency record budgets are separate from allocator node budgets.

Retries apply only to failed items with `NO_AVAILABLE_HANDLE`, `TRANSFER_FAIL`
or `RPC_FAIL`. Successful neighbors are not replayed. Every attempt revalidates
the registration and source block; stale/invalid/file errors are not retried.
The request's deadline and shutdown stop further retries, and shutdown wakes
the backoff wait immediately. These two retry fields previously existed only
as unused C++ settings; they now control execution as well as parsing.

Legacy names remain supported. The new component field wins **fieldwise** if
both spellings are supplied, independent of the order of JSON properties:

| Legacy path under `v2` | Component path under `v2` |
| --- | --- |
| `events.movement_queue_capacity` | `migration.max_queued_requests` |
| `hot_key_snapshot_limit` | `frequency_tracker.max_snapshot_keys` |
| `placement_policy.offload_high_watermark` | `movement.offload_high_watermark` |
| `placement_policy.onboard_min_frequency` | `movement.onboard_min_read_heat` |

## Physical targets inside a logical tier

The original flat tier syntax is still supported. Alternatively, DRAM tiers
accept `arenas`, and SSD/STORAGE/NVME tiers accept `devices`, each a non-empty
array. An array does not add logical tiers: one `tiers[]` entry still has one
UUID, Master segment and block/eviction index.

Every target must provide its own positive `capacity`. The tier-level
`capacity` is optional with arrays; if present, it must equal the sum of target
capacities. Overflowing sums are rejected.

Physical defaults are inherited fieldwise from the tier and can be overridden
on each target:

- DRAM: `alignment` (default 64), `numa_node`, `allocator_init_capacity`,
  `allocator_max_capacity`. `numa_node: null` clears an inherited NUMA node.
- SSD: `alignment` (default 4096), `fsync_on_commit` (default false),
  `allocator_init_capacity`, `allocator_max_capacity`.
- Every SSD `devices[]` entry requires an explicit, independent `file_path`.
  A tier-level `file_path` cannot be combined with `devices`. Repeated paths,
  including existing symlink/hardlink aliases, are rejected. Flat SSD tiers
  retain the existing environment-based path derivation when `file_path` is
  omitted.

Pool allocation rotates across targets and tries the other targets on
`NO_AVAILABLE_HANDLE`; it does not evict on its own. All targets share the
logical tier's reclaim policy. Minimum alignment reported by the pool is the
strictest configured target alignment.

Allocator budgets apply per target. See
[allocator node capacity](datamanager_v2_allocator_capacity.md) for sizing and
memory costs. The full example's two SSD allocators each reserve approximately
512 MiB for their 16M-node budgets, approximately 1 GiB in total before other
metadata costs.

## Other configuration boundaries

Local transfer mode/worker counts and key-lease timing still come from the
outer P2P client configuration. Existing block-index, tier-placement,
allocation-failure, copier and shutdown settings keep their current JSON
entry points.

The standalone `EvictionPolicyConfig` dynamic-watermark component is not wired
into DataManagerV2's runtime. `v2.eviction_policy` is rejected rather than
silently ignored; exposing that component requires a separate integration.
