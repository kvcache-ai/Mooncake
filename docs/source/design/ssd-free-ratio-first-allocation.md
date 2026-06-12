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

### Sorting

Candidates are sorted by SSD free ratio in descending order. Segments with more available SSD space appear first and are preferred for allocation.

### Preferred segments

As with other allocation strategies, segments marked as preferred by the caller are handled first. Preferred segments bypass the SSD free ratio ranking and are allocated immediately if they have sufficient capacity.

### Fallback to random allocation

After allocating from the SSD-ranked candidates, any remaining replicas that could not be satisfied are allocated using the standard random strategy as a fallback. This ensures that allocation succeeds even when SSD metrics are unavailable (for example, on segments without SSD offload configured).

---

## SSD Usage Tracking

### `ssd_used_bytes` counter

`LocalDiskSegment` maintains an atomic counter `ssd_used_bytes` that tracks the total number of bytes currently occupied by offloaded replicas on the segment's SSD. This counter is updated at two points:

- **Increment**: `NotifyOffloadSuccess` increments `ssd_used_bytes` by the object size when the master adds a `LOCAL_DISK` replica to the object entry. This happens after the client has confirmed a successful write to SSD.
- **Decrement**: `EvictDiskReplica` decrements `ssd_used_bytes` by the object size when the master removes a `LOCAL_DISK` replica (either through explicit eviction or object deletion).

The counter is atomic to allow concurrent updates from multiple RPC handler threads without requiring a separate lock.

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
| `mooncake-store/include/segment.h` | Add `ssd_used_bytes` atomic field to `LocalDiskSegment`; inherit `SsdMetricsProvider` |
| `mooncake-store/src/segment.cpp` | Implement `getSsdTotalCapacity` and `getSsdUsedBytes` |
| `mooncake-store/src/master_service.cpp` | Integrate `SsdFreeRatioFirstAllocationStrategy` into allocation dispatch; update `ssd_used_bytes` in `NotifyOffloadSuccess` and `EvictDiskReplica` |

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
