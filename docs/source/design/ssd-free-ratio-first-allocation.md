# SSD Free-Ratio-First Allocation Design

## Overview

Mooncake Store distributes KV cache objects across multiple memory segments hosted on different nodes. The master's allocation strategy decides which segment receives each new object replica. When using DDR-only strategies such as `random` or `free_ratio_first`, the allocator ignores SSD state entirely. In deployments where some segments have SSD offload enabled and others do not, this blind allocation can concentrate traffic on a small subset of segments whose SSD capacity is quickly exhausted, while segments with ample SSD headroom remain underutilized.

This document describes `SsdFreeRatioFirstAllocationStrategy`, an allocation strategy that ranks candidate segments by their SSD free ratio and preferentially allocates to segments with the most available SSD space. It integrates with the existing allocation framework and adds SSD usage tracking to `LocalDiskSegment` so that the master can make informed placement decisions.

---

## Architecture

```
                         Allocation Request
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Sample candidate      │
                    │  segments (up to       │
                    │  6 * replica_num)      │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Compute SSD free     │
                    │  ratio per candidate  │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Sort by SSD free     │
                    │  ratio descending     │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Allocate from top    │
                    │  candidates           │
                    └───────────┬───────────┘
                                │
                     ┌──────────┴──────────┐
                     │  Remaining replicas? │
                     └──────┬──────┬───────┘
                            │      │
                       Yes ◄┘      └► No → Done
                            │
                            ▼
                    ┌───────────────────────┐
                    │  Fallback: random     │
                    │  allocation for       │
                    │  remaining replicas   │
                    └───────────────────────┘
```

The flow follows the same high-level structure as the existing `FreeRatioFirstAllocationStrategy`: sample a subset of candidates, compute a ranking metric, sort, and allocate from the top. The key difference is that the ranking metric is SSD free ratio rather than DRAM free ratio.

`MasterService` only passes an `SsdMetricsProvider` when the effective allocation strategy is `SSD_FREE_RATIO_FIRST`. Non-SSD strategies receive `nullptr`, so they avoid unnecessary local-disk segment access.

---

## Core Algorithm

### Candidate sampling

For each allocation request requesting `replica_num` replicas, the strategy samples `min(6 * replica_num, total_segments)` candidate segments. This bounded sampling keeps the sorting cost predictable regardless of cluster size while still providing a statistically diverse candidate set.

### SSD free ratio

For each sampled segment, the SSD free ratio is computed as:

```
ssd_free_ratio = (ssd_total_capacity - ssd_used_bytes) / ssd_total_capacity
```

A segment with 1 TB total SSD capacity and 200 GB used has an SSD free ratio of 0.80. A segment whose SSD is full has a ratio of 0.0.

Before calculating the ratio, `ssd_used_bytes` is clamped to `[0, ssd_total_capacity]`. This keeps transient concurrent accounting drift from producing a negative free ratio or a value greater than 1.0. If no SSD metrics provider is available, or if the reported total capacity is not positive, the strategy treats the segment as fully free.

### Sorting

Candidates are sorted by SSD free ratio in descending order. Segments with more available SSD space appear first and are preferred for allocation.

### Preferred segments

As with other allocation strategies, segments marked as preferred by the caller are handled first. Preferred segments bypass the SSD free ratio ranking and are allocated immediately if they have sufficient capacity.

### Fallback to random allocation

After allocating from the SSD-ranked candidates, any remaining replicas that could not be satisfied are allocated using the standard random strategy as a fallback. This ensures that allocation succeeds even when SSD metrics are unavailable (for example, on segments without SSD offload configured).

---

## SSD Usage Tracking

### `ssd_used_bytes` counter

`LocalDiskSegment` maintains `ssd_total_capacity_bytes`, updated by `ReportSsdCapacity`, and an atomic counter `ssd_used_bytes` that tracks the total number of bytes currently occupied by offloaded replicas on the segment's SSD. `ssd_used_bytes` is updated alongside metadata changes:

- **Increment**: `NotifyOffloadSuccess` increments `ssd_used_bytes` by the object size only after the master successfully adds a `LOCAL_DISK` replica to the object entry. If the object has already disappeared from metadata, the notification is ignored and the counter is not changed.
- **Decrement**: The master decrements `ssd_used_bytes` when a `LOCAL_DISK` replica is actually removed from metadata. Full object deletion releases all associated local-disk usage through `EraseMetadata`, while partial replica deletion uses `EraseReplicasAndTrackSsdUsage` to pop erased replicas and release usage for removed `LOCAL_DISK` entries.

The counter is atomic to allow concurrent updates from multiple RPC handler threads without requiring a separate lock. The allocation strategy treats it as an eventually consistent placement signal and clamps it before computing the free ratio.

### `SsdMetricsProvider` interface

`ScopedLocalDiskSegmentAccess` implements the `SsdMetricsProvider` interface, which exposes two methods:

| Method | Return type | Description |
|--------|-------------|-------------|
| `getSsdTotalCapacity` | `int64_t` | Total SSD capacity configured for the segment, in bytes |
| `getSsdUsedBytes` | `int64_t` | Current SSD usage, read from `ssd_used_bytes` |

The allocation strategy queries these methods through the `SsdMetricsProvider` interface, keeping the strategy decoupled from the concrete segment implementation.

---

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `allocation_strategy` | string | `"random"` | Set to `"ssd_free_ratio_first"` to enable SSD-ratio-based load balancing |

The parameter is passed as a gflag to the master process at startup.

---

## Code Structure

| File | Change |
|------|--------|
| `mooncake-store/include/types.h` | Add `SSD_FREE_RATIO_FIRST` enum value to the allocation strategy enum |
| `mooncake-store/include/allocation_strategy.h` | Add `SsdMetricsProvider` interface and `SsdFreeRatioFirstAllocationStrategy` class |
| `mooncake-store/include/segment.h` | Add `ssd_total_capacity_bytes` and `ssd_used_bytes` fields to `LocalDiskSegment`; inherit `SsdMetricsProvider` |
| `mooncake-store/src/segment.cpp` | Implement `getSsdTotalCapacity` and `getSsdUsedBytes` |
| `mooncake-store/src/master_service.cpp` | Pass SSD metrics only to `SSD_FREE_RATIO_FIRST`; update `ssd_used_bytes` after successful `NotifyOffloadSuccess` metadata insertion; release usage when `LOCAL_DISK` replicas are erased |

---

## Usage Example

Start the master with the SSD free-ratio-first strategy:

```bash
./mooncake_master --allocation_strategy=ssd_free_ratio_first
```

With this configuration:

1. The master samples up to `6 * replica_num` candidate segments for each allocation request.
2. Candidates are ranked by SSD free ratio (descending).
3. Allocation proceeds from the top-ranked candidates.
4. Any remaining replicas fall back to random allocation.
