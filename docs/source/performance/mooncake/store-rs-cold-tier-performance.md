# Store-RS Cold-Tier Performance Design

This page captures optimization and profiling direction for ExtentStore. It
contains design analysis rather than benchmark results. Measured Store-RS
throughput and latency commands are in the
[Store-RS benchmark guide](store-rs-benchmark.md).

## ExtentStore Performance Review and Optimization Direction

The community Mooncake `io_uring` implementations and this ExtentStore direction operate at different abstraction layers:

- community `io_uring` file/transport code is a simple low-risk offset I/O accelerator
- ExtentStore is a KVCache-aware cold storage engine that owns object layout, extent locators, route-authoritative visibility, restore/offload semantics, liveness, and recovery boundaries

The optimization direction is therefore not to replace ExtentStore with a generic `io_uring` transport. ExtentStore should keep the KV-like engine semantics, while absorbing the community design lessons that avoid global contention and keep the I/O fast path simple, observable, and fallback-friendly.

Key architectural conclusions:

- ExtentStore's main advantage is KVCache lifecycle awareness, not fixed-file or fixed-buffer registration by itself.
- Append-only segment layout can create restore locality only if runtime restore uses backend batch reads instead of issuing mostly single-object restores.
- Route metadata remains the authoritative index; the engine should not introduce an independent LSM-style object index for visibility.
- Restore correctness must not depend on DRAM promotion succeeding.
- Cleaner/GC is a storage-engine responsibility once cold objects are packed into shared segments.
- Delete journal durability should support a strict mode for tests/strong durability and a normal group-commit mode for high-churn cache workloads.
- Worker/ring design should evolve toward per-device/per-queue sharding when profiling shows the current dedicated worker is a bottleneck; avoid a global ring or global lock hot path.
- Direct I/O, fixed file, fixed buffer, and GDS paths are optimizations, not the core design. They must expose hit/fallback counters and should not dominate the control flow before batch restore and copy reduction are addressed.

Profiling must come before structural optimization. The minimum profiling surface should split restore/offload latency into route lookup, locator parse, singleflight wait/leader work, backend queue wait, physical I/O submit/complete, checksum/decode, copy-to-caller, promotion allocation/copy/CAS, delete journal append/sync, and total p50/p95/p99. It should also expose object size distribution, batch size distribution, coalesced read ratio, average physical read size, SQE count per logical object, direct/scratch/buffered ratios, scratch copy bytes, worker busy ratio, queue depth, and route CAS retry counts.

The prioritized optimization sequence is:

1. Add fine-grained profiling and benchmark coverage for restore/offload and worker contention.
2. Connect runtime cold batch restore to backend batch read/read-into-buffer by grouping locators by cold device/backend and preserving object-level singleflight initially.
3. Reduce restore copies with read-into-destination singleflight: single waiter reads into caller or promotion buffer; multiple waiters share a leader buffer and copy only when necessary.
4. Implement a minimal conservative cleaner for sealed segments: choose high-dead-ratio candidates, verify each live record against the current route, copy-forward live records, CAS routes to new locators, and treat CAS-lost copies as dead.
5. Add delete journal batching/group commit with strict and normal modes; normal mode may conservatively leak space across a crash window but must not make live data unreachable.
6. Add minimal KVCache-aware segment placement, starting with size class and TTL/session bucket before adding more dimensions.
7. Shard I/O workers/rings by device and queue class only if profiling shows the single worker/queue is limiting throughput or p99.
8. Clarify GPU/GDS restore paths with large aligned direct reads, pooled pinned-host fallback, and success/fallback counters.

For architecture-level changes in these phases, use profiling data and compare against mature storage/runtime patterns before implementation. Relevant references include log-structured segment cleaning, RocksDB-style compaction tradeoffs where applicable, `io_uring` per-thread/per-device queue patterns, and cache admission/promotion strategies. Do not introduce speculative abstractions or complex fast paths without a measured bottleneck and a clear fallback.
