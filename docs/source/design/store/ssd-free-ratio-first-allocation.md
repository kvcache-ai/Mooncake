# SSD Free-Ratio-First Allocation Design

## Overview

Mooncake Store distributes KV cache objects across multiple memory segments hosted on different nodes. The master's allocation strategy decides which segment receives each new object replica. When using DDR-only strategies such as `random` or `free_ratio_first`, the allocator ignores SSD state entirely. In deployments where some segments have SSD offload enabled and others do not, this blind allocation can concentrate traffic on a small subset of segments whose SSD capacity is quickly exhausted, while segments with ample SSD headroom remain underutilized.

This document describes `SsdFreeRatioFirstAllocationStrategy`, an allocation strategy that ranks candidate segments by their SSD free ratio and preferentially allocates to segments with the most available SSD space. It integrates with the existing allocation framework and reads authoritative SSD usage from `LocalSsdManager` so that the master can make informed placement decisions.

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

`MasterService` constructs `SsdFreeRatioFirstAllocationStrategy` with its `LocalSsdManager`. During allocation, `ScopedAllocatorAccess` resolves each candidate segment to its owning client, and the strategy queries that client's SSD usage. Other allocation strategies use the same placement view but do not query local-SSD state.

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

Before calculating the ratio, `ssd_used_bytes` is clamped to `[0, ssd_total_capacity]`. This keeps transient concurrent accounting drift from producing a negative free ratio or a value greater than 1.0. If no SSD usage view is available, or if the reported total capacity is not positive, the strategy treats the segment as fully free.

### Sorting

Candidates are sorted by SSD free ratio in descending order. Segments with more available SSD space appear first and are preferred for allocation.

### Preferred segments

As with other allocation strategies, segments marked as preferred by the caller are handled first. Preferred segments bypass the SSD free ratio ranking and are allocated immediately if they have sufficient capacity.

### Fallback to random allocation

After allocating from the SSD-ranked candidates, any remaining replicas that could not be satisfied are allocated using the standard random strategy as a fallback. This ensures that allocation succeeds even when SSD usage state is unavailable (for example, on segments without SSD offload configured).

---

## SSD Usage Tracking

### Per-client usage state

`LocalSsdManager` maintains a record for each client. Each record contains `total_capacity_bytes`, updated by `ReportCapacity`, and an atomic `used_bytes` counter that tracks the bytes occupied by offloaded replicas on that client's SSD. Usage is updated alongside metadata changes:

- **Increment**: `NotifyOffloadSuccess` calls `AdjustUsedBytes` only after the master successfully adds a `LOCAL_DISK` replica to the object entry. If the object has already disappeared from metadata, the notification is ignored and usage is not changed.
- **Decrement**: The master calls `AdjustUsedBytes` through `ReleaseLocalDiskUsage` whenever a `LOCAL_DISK` replica is removed from metadata — on full object deletion (`EraseMetadata`), partial replica removal through `EraseReplicasWithCacheTotalAccounting`, and the local-disk eviction path.

The counter is atomic to allow concurrent updates from multiple RPC handler threads. `LocalSsdManager::GetUsage` returns the capacity and usage under the client's lifecycle protection; the allocation strategy still clamps usage before computing the free ratio.

### Domain-state boundary

SSD placement reads domain state directly from `LocalSsdManager`, rather than from Prometheus gauges or a metrics-oriented provider. The relevant operations are:

| Method | Return type | Description |
|--------|-------------|-------------|
| `ScopedAllocatorAccess::GetOwnerClientId` | `std::optional<UUID>` | Resolve a segment name to its owning client |
| `LocalSsdManager::GetUsage` | `std::optional<Usage>` | Read that client's total capacity and used bytes |

`SsdFreeRatioFirstAllocationStrategy` keeps a reference to `LocalSsdManager` and receives `ScopedAllocatorAccess` for the segment-to-client mapping. These values are authoritative domain state and remain independent of Prometheus or other telemetry backends.

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
| `mooncake-store/include/allocation_strategy.h` | Define `SsdFreeRatioFirstAllocationStrategy` with a `LocalSsdManager` dependency |
| `mooncake-store/src/allocation_strategy.cpp` | Resolve candidate owners and rank them with `LocalSsdManager::GetUsage` |
| `mooncake-store/include/local_ssd/manager.h` | Own per-client SSD capacity and usage state |
| `mooncake-store/src/master_service.cpp` | Update SSD domain usage after successful offload metadata insertion and release it when `LOCAL_DISK` replicas are erased |

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
