# SSD Free-Ratio-First Allocation Design

## Overview

Mooncake Store distributes KV cache objects across memory segments on different nodes. DDR-only strategies such as `random` or `free_ratio_first` ignore SSD state and can overload SSD-enabled segments while leaving others underutilized.

`SsdFreeRatioFirstAllocationStrategy` ranks candidate segments by their SSD free ratio and preferentially allocates to segments with the most available SSD headroom. It plugs into the ranked allocation framework and reads per-client SSD capacity and usage from `LocalSsdManager`.

---

## Architecture

```
                         Allocation Request
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Allocate preferred   │
                    │  segments first       │
                    └───────────┬───────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │  Sample candidate     │
                    │  segments (up to      │
                    │  6 * remaining)       │
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
                     │ Remaining replicas? │
                     └──────┬──────┬───────┘
                            │      │
                       Yes ◄┘      └► No → Done
                            │
                            ▼
                    ┌───────────────────────┐
                    │  Fallback: inline     │
                    │  random-start scan    │
                    │  across segments      │
                    └───────────────────────┘
```

The flow matches `FreeRatioFirstAllocationStrategy` (`RankedAllocationStrategy`), replacing DRAM free ratio with SSD free ratio. `MasterService` wires `local_ssd_manager_` into `SsdFreeRatioFirstAllocationStrategy` only when the effective strategy is `SSD_FREE_RATIO_FIRST`; non-SSD strategies do not query `LocalSsdManager`.

---

## Core Algorithm

### Preferred segments

Preferred segments requested by the caller are allocated first and bypass SSD free-ratio ranking when they have sufficient capacity.

### Candidate sampling

For the remaining `remaining` replicas, `RankedAllocationStrategy::AllocateRanked` samples up to `min(6 * remaining, total_segments)` candidate segments (non-serving segments do not consume the sampling budget), keeping sorting cost bounded regardless of cluster size.

### SSD free ratio

For each sampled segment, the strategy resolves the owner's client ID, queries `LocalSsdManager::GetUsage(client_id)`, and computes:

```
ssd_free_ratio = (ssd_total_capacity - ssd_used_bytes) / ssd_total_capacity
```

`ssd_used_bytes` is clamped to `[0, ssd_total_capacity]` before division to guard against transient accounting drift. Segments with no `LocalSsdManager` entry or non-positive capacity are scored as `1.0` (fully free).

### Sorting

Candidates are sorted by SSD free ratio in descending order so segments with more available SSD space are tried first.

### Fallback scan

If the ranked pass still leaves replicas unallocated (e.g., top candidates lack DRAM space), `AllocateRanked` runs an inline random-start scan across the full segment list rather than calling `RandomAllocationStrategy`. Missing-metric segments already score `1.0` during ranking and do not trigger fallback on their own.

---

## SSD Usage Tracking

### `LocalSsdManager` accounting

`LocalSsdManager` tracks per-client `total_capacity_bytes` (from `ReportSsdCapacity`) and `used_bytes` (occupied by `LOCAL_DISK` replicas) alongside master metadata changes:

- **Increment**: `NotifyOffloadSuccess` adds the object size via `LocalSsdManager::AdjustUsedBytes` when a `LOCAL_DISK` replica is newly added (not on in-place replacement). Missing keys or restart `ScanMeta` re-registrations without an `offloading_task` go through `AddReplicaForRetainedClient`, which checks `AdmitWrite` and mounted local-disk state before publishing the replica and updating `used_bytes`.
- **Decrement & cleanup**: `ReleaseLocalDiskUsage` decrements `used_bytes` whenever a `LOCAL_DISK` replica is removed (object deletion, partial erase, or local-disk eviction). `UnmountLocalDiskSegment` and client offboarding call `LocalSsdManager::UnregisterClient` to remove the client record.

`used_bytes` is treated as an eventually consistent placement signal and clamped before computing the free ratio.

### `LocalSsdManager` integration

`SsdFreeRatioFirstAllocationStrategy` queries `LocalSsdManager::GetUsage(client_id)`, returning `std::optional<LocalSsdManager::Usage>`:

| Field | Type | Description |
|-------|------|-------------|
| `total_capacity_bytes` | `int64_t` | Reported SSD capacity for the client, in bytes |
| `used_bytes` | `int64_t` | Tracked SSD usage for the client, in bytes |

In the placement layer, `LocalSSDMetricsView` exposes the same data to `SsdFreeRatioFirstPlacementPolicy`.

---

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `allocation_strategy` | string | `"random"` | Set to `"ssd_free_ratio_first"` to enable SSD-ratio-based allocation |

Passed as a master startup gflag. Setting `--enable_cxl=true` overrides `allocation_strategy` to `CXL`.

---

## Code Structure

| File | Role |
|------|------|
| `mooncake-store/include/types.h` | Defines `AllocationStrategyType::SSD_FREE_RATIO_FIRST` |
| `mooncake-store/include/allocation_strategy.h`<br>`mooncake-store/src/allocation_strategy.cpp` | `RankedAllocationStrategy::AllocateRanked` and `SsdFreeRatioFirstAllocationStrategy` |
| `mooncake-store/include/local_ssd/manager.h`<br>`mooncake-store/src/local_ssd/manager.cpp` | `LocalSsdManager` per-client capacity, `used_bytes`, and registration |
| `mooncake-store/include/placement/replica_allocator.h`<br>`mooncake-store/src/placement/replica_allocator.cpp` | `LocalSSDMetricsView` and `SsdFreeRatioFirstPlacementPolicy` |
| `mooncake-store/src/master_service.cpp` | Strategy wiring, `NotifyOffloadSuccess` / `AddReplicaForRetainedClient`, and erase/unmount/offboard cleanup |

---

## Usage Example

```bash
./mooncake_master --allocation_strategy=ssd_free_ratio_first
```

1. Preferred segments are allocated first when specified.
2. Up to `6 * remaining` candidate segments are sampled for the remaining replicas.
3. Candidates are ranked by SSD free ratio (descending) via `LocalSsdManager::GetUsage`.
4. Top candidates are allocated first, falling back to an inline random-start scan if replicas remain.
