# Eviction Age & Duration Metrics — Design

- Date: 2026-10-12
- Status: Approved (design reviewed interactively; see "Decisions" below)
- Scope: mooncake-store master (mem eviction path only)

## Goal

Add two Prometheus histograms to the mooncake master to answer two questions
that eviction counters (`master_evicted_key_count`, `master_evicted_size_bytes`)
cannot:

1. **How old are keys when they get evicted?** — distribution of key age at
   eviction time (`now - put_start_time`), which distinguishes
   freshly-written-but-evicted keys from long-tail stock being reclaimed.
2. **How long does one eviction cycle take?** — wall-clock duration of a single
   `BatchEvict` round (scan + lock contention + erase + offload RPC).

## Decisions (confirmed with user)

- **Coverage: mem path only** (`BatchEvict`). NoF, tenant-quota, and
  client-initiated `EvictDiskReplica` paths are out of scope.
- **Duration definition: total wall time of one `BatchEvict` call**, including
  zero-eviction cycles (matches the `[EVICT-DONE]` log cadence).
- **Age observation point: Approach A** — the two `freed > 0` success branches
  inside `try_evict_group_or_object` (non-grouped and grouped-member), next to
  `PublishKvRemovedAfterEvict`. This keeps the metric's accounting identical to
  `inc_eviction_success(evicted_count, ...)`: in non-oplog mode, a key
  contributes to the age histogram exactly when it is counted as evicted
  (oplog mode intentionally records no age, mirroring the
  `PublishKvRemovedAfterEvict` guard).
- **Buckets: logarithmic.** Age in seconds
  `{1, 10, 60, 300, 900, 3600, 21600, 86400, +Inf}`; duration in milliseconds
  `{1, 5, 10, 50, 100, 250, 500, 1000, 5000, 15000, 30000, 60000, 120000,
  300000, 600000, +Inf}` — revised after production measurement showed one
  `BatchEvict` round ranges from fractions of a second to 10 minutes (the
  original design reused the offboarding bucket set capped at 30s, which
  piled everything above 30s into +Inf).

## Semantics

- **Age origin**: latest put of the key. `put_start_time` is reset on same-key
  rewrite (Upsert path, `master_service.cpp` `metadata.put_start_time = now`),
  so age = time since the most recent put start. This follows the existing
  field semantics; no new definition is introduced.
- **Persistence**: `put_start_time` is serialized via msgpack in master
  metadata, so the age distribution remains meaningful across master restarts
  and snapshot recovery.
- **Offload-queued keys are not counted**: when offload-on-evict queues the
  memory replica for disk offload and `freed == 0`, the object has not been
  evicted yet and contributes no age observation. Its eventual disk-side
  release is out of scope (disk path).
- **Clock sources**: age uses the `system_clock` snapshot (`now`) already taken
  at the top of `BatchEvict` — same clock as `put_start_time`. Duration uses
  `steady_clock` (immune to wall-clock adjustments).
- **Defensive clamp**: negative age (clock skew) is clamped to 0.
- **Precision**: age is observed as `duration<double>` seconds to keep
  sub-second resolution (keys evicted shortly after write must land in the
  first bucket accurately). ylt's histogram `_sum` is int64-truncated (bucket
  placement keeps sub-second resolution); an eviction batch whose keys are
  all younger than ~1s produces sum 0 and the histogram is omitted from
  `/metrics` output.

## Metric definitions

| Metric | Type | Labels | Description |
|---|---|---|---|
| `master_evicted_key_age_seconds` | histogram | none | Age of evicted keys at eviction time (seconds since latest put start) |
| `master_eviction_duration_ms` | histogram | none | Wall-clock duration of one `BatchEvict` cycle (milliseconds) |

## Changes

### 1. `mooncake-store/include/master_metric_manager.h`

Add two histogram members next to `client_offboarding_duration_ms_` (age:
`ylt::metric::basic_static_histogram<double>`; duration:
`ylt::metric::histogram_t`):

- `evicted_key_age_seconds_`
- `eviction_duration_ms_`

### 2. `mooncake-store/src/master_metric_manager.cpp`

Framework "four-piece" wiring, mirroring the existing histograms:

1. Constructor initialization of both histograms (name + help + buckets as
   above).
2. Public observe methods, named after the existing
   `observe_value_size` / `observe_client_offboarding_duration_ms` style:
   - `observe_evicted_key_age_seconds(double seconds)`
   - `observe_eviction_duration_ms(int64_t duration_ms)`
3. `update_metrics_for_zero_output()`: add
   `evicted_key_age_seconds_.observe(0)` and
   `eviction_duration_ms_.observe(0)` so empty histograms still appear in
   `/metrics` output (existing convention). Note: due to ylt's sum==0
   omission gate, the series only actually appears in `/metrics` output
   after the first observation with a nonzero (int64-truncated) sum.
4. Serialization block (next to `serialize_metric(value_size_distribution_)`):
   add both `serialize_metric(...)` calls.

### 3. `mooncake-store/src/master_service.cpp` — `BatchEvict`

- **Entry**: record `const auto cycle_start = std::chrono::steady_clock::now();`
  near the top (after the parameter clamp). `BatchEvict` has two function-level
  exits — the `total_eviction_base == 0` early return and the natural tail —
  and each records the duration (two mutually-exclusive observation points, one
  per call, preserving the "every cycle measured" semantic); no RAII guard is
  introduced.
- **Age** — in `try_evict_group_or_object`, at both `freed > 0` branches (the
  non-grouped branch and `evict_one_member`), next to the
  `PublishKvRemovedAfterEvict` call:

  ```cpp
  MasterMetricManager::instance().observe_evicted_key_age_seconds(
      std::chrono::duration<double>(now - metadata.put_start_time).count());
  ```

  (member variant uses `member_metadata`; negative ages are clamped to 0
  inside `observe_evicted_key_age_seconds`, so call sites pass the raw
  difference).
- **Exit**: at the tail, next to the `inc_eviction_success` /
  `inc_eviction_fail` block:

  ```cpp
  MasterMetricManager::instance().observe_eviction_duration_ms(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - cycle_start)
          .count());
  ```

## Performance

Per evicted key: one field read on an already-touched `ObjectMetadata` (same
struct as the `EvictionDeadline()` / lease checks on the hot path), one
subtraction, one O(1) histogram observe (bucket lookup + a few atomic
increments, uncontended — only the eviction thread writes these histograms).
Per cycle: one `steady_clock::now()` and one observe. This is nanosecond-scale
against an existing per-key path that already performs `PublishKvRemovedAfterEvict`
(event serialization) and `EraseMetadata` under the shard lock, and a per-cycle
`LOG(INFO) << "[EVICT-RESULT]"` — impact is far below 1% of eviction cost.

## Testing

- `mooncake-store/tests/master_metrics_test.cpp`
  - Observe methods record counts/bucket placements.
  - After `update_metrics_for_zero_output()`, serialized output contains both
    new metric names.
- `mooncake-store/tests/master_service_evict_scenario_test.cpp`
  - Trigger a `BatchEvict` that evicts keys: age histogram count increases and
    observed values are ≥ 0.
  - Duration histogram count increments per `BatchEvict` call.
  - Note: ylt omits a histogram from serialization entirely while its (int64)
    `sum` is 0 — including the zero-output touch `observe(0)`, which bumps
    bucket counters but not the sum — so the test parses `_count` with absence
    treated as 0 and compares deltas (the scenario test additionally uses
    real age > 1s and two eviction cycles so the sums become non-zero).

## Out of scope

- NoF / tenant-quota / disk-replica eviction coverage.
- Per-phase duration breakdown (scan vs. offload push).
- Eviction trigger-interval metric.
