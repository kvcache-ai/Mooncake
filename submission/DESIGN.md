# DESIGN

## Motivation

Mooncake Store already exposes batch APIs, but several hot paths still process
batch requests as repeated single-key operations. Under high-QPS KVCache
workloads, this leads to avoidable overhead from:

- repeated shard lock acquisition
- repeated hash table lookups
- repeated request-side temporary container construction
- extra batch RPC wrapper loops

This optimization targets those overheads without changing the external API.

## Problem Statement

The main bottleneck is not only data transfer bandwidth. For small and
medium-sized KV objects, the metadata path becomes a large part of end-to-end
latency.

Affected paths:

- `BatchExistKey`
- `BatchGetReplicaList`
- `BatchPutEnd`
- `BatchPutRevoke`
- client-side `put_batch` request preparation

## Design Goals

- keep public APIs backward compatible
- preserve request order in batch responses
- preserve error-code behavior
- reduce metadata lock contention
- reduce per-request temporary object churn

## Design Overview

### 1. Shard-grouped batch execution

Instead of calling single-key logic in a loop, the optimized implementation:

- groups keys by metadata shard
- acquires one shard lock per shard group
- processes all keys in that shard in one pass
- writes results back to the original request order

This reduces repeated lock/unlock operations and repeated key-to-shard work.

### 2. Direct wrapped batch RPC dispatch

`WrappedMasterService` previously looped over some batch requests and called
single-key APIs one by one.

The optimized implementation routes batch RPCs directly to the corresponding
batch fast path in `MasterService`.

### 3. Reduced client-side batch reshuffling

`RealClient::put_batch_internal` previously built an intermediate hash map and
then rebuilt an ordered vector for `BatchPut`.

The optimized implementation stores batch slices directly in request order and
passes them to `BatchPut` without an extra reorder stage.

## Interface Changes

### Public compatibility

No public user-facing API was removed or changed.

### Internal interface change

Added:

- `MasterService::BatchGetReplicaList(const std::vector<std::string>& keys)`

This is an internal service-level extension used by the wrapped RPC layer to
avoid repeated single-key dispatch.

## Behavioral Compatibility

The implementation preserves:

- response order
- success/failure positions
- error code semantics
- lease grant behavior for valid reads
- promotion-on-hit enqueue behavior after batch replica queries

## Trade-offs

### Benefits

- lower control-plane overhead per batch
- better throughput under concurrent batch requests
- lower P50 and P99 for metadata-heavy workloads
- no breaking API change

### Costs

- more logic inside batch methods
- larger responsibility for `MasterService` batch implementations
- more branch-sensitive behavior to validate in tests

## Validation Strategy

### Functional tests

- order preservation in mixed-result batch queries
- object-not-found handling
- processing-state handling
- batch put completion behavior

### Stress and benchmark validation

- `master_bench` for metadata throughput
- `batch_remove_benchmark.py` for batch remove speedup
- metrics collection from `/metrics` and `/metrics/summary`

## Deployment Notes

No deployment topology changes are required.

Recommended runtime setup:

- enable master metrics endpoint
- keep baseline and optimized runs on identical hardware
- record both throughput and latency percentiles

## Known Limitations

- the current work focuses on metadata-path batch optimization
- it does not yet redesign the deeper transfer-engine data path
- it does not yet add a new C++ benchmark binary dedicated to mixed
  put/get/remove workloads

## Next Steps

- extend the benchmark harness to collect more detailed per-operation latency
- add an end-to-end mixed workload pressure test
- evaluate temp-buffer reuse for disk-backed batch get paths
- investigate additional fast paths for `BatchPutStart`
