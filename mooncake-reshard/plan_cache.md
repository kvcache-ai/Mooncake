# Optional weight transfer plan cache

`WeightTransferPlanCache` caches address-free local-target `LogicalTransferPlan`
objects in process. It is disabled by default and requires an explicit caller.
Existing planner, Reader, Sink and Store entry points do not use it automatically.

```python
from mooncake.reshard.weight.plan_cache import WeightTransferPlanCache

cache = WeightTransferPlanCache.from_environment()
plan = cache.plan(source_placement, target_placement, target_participant_id)
print(cache.cache_info())
cache.clear()
```

Only `from_environment()` reads configuration. Importing the module and calling
the constructor do not read environment variables. Set
`MC_RESHARD_PLAN_CACHE_ENABLED=1` to enable caching (`0` is the default). Set
`MC_RESHARD_PLAN_CACHE_MAX_ENTRIES` to a positive decimal integer (default `2`).
Malformed values raise `ValueError` when the factory is called, including an
invalid capacity while disabled. Direct construction accepts `enabled` and
`max_entries` with the same defaults.

Disabled calls invoke the existing public local-target planner directly, without
serializing placements or retaining plans. Enabled calls serialize the complete
source and target placement using the existing canonical JSON serde. The key
also includes the local target participant and every `PlanningLimits` value.
Limits are validated and copied into the canonical `PlanningLimits` type, so
caller subclasses, custom hashing and attached runtime objects are not retained.
Omitted limits and explicit default limits share an entry. Placement identities,
topology, tensors, fragments, and weight generation therefore affect the key;
cached placement digests are not used as keys.

A cold miss reconstructs both placements through the validated JSON parser
before planning. This gives the cache its own canonical snapshots and excludes
caller-attached runtime tensors or other objects. Invalid inputs fail through
the existing serde/planner validation and failed calculations are not retained.
Cache hits return the same logical plan object; callers must respect its frozen
contracts and must not mutate nested objects through unsupported mechanisms.

The cache stores no bound plans, runtime bindings, native engines or readers.
Every execution must bind and validate current addresses, endpoints, allocation
guards, leases and runtime instance generations independently. Reallocating
physical memory can reuse a logical entry, while changing weight generation
requires a different entry. A cache hit is not evidence of runtime readiness.

The implementation uses `functools.lru_cache`. Capacity limits entries, not
bytes: each retained entry includes both serialized placement keys, canonical
placement snapshots and its plan. Every enabled lookup pays full serialization
cost; cold misses additionally pay JSON parsing, validation and snapshot
construction. There is no claim of universal speedup or fixed memory usage.
`cache_info()` reports hits, misses, maxsize and currsize; `clear()` drops retained
entries and resets counters. The builtin LRU keeps bookkeeping coherent across
threads, but concurrent cold calls may compute the same key more than once.
Clearing does not cancel in-flight calculations or provide an execution fence.

Validation is CPU-only. Cache tests cover disabled forwarding, equivalence,
canonical snapshots, complete identity and limits, LRU eviction, clearing,
configuration validation and concurrent requests. Native transfer and GPU
correctness require separate validation of the execution path.
