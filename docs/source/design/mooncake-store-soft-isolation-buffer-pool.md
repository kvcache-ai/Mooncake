# Mooncake Store Soft-Isolation Registered Buffer Pool

## Background

Mooncake already allocates and registers a per-client local buffer during setup. That buffer is managed by `ClientBufferAllocator` and is reused internally by several data paths:

- temporary staging for Python batch tensor upsert
- temporary CPU staging for disk and ranged reads
- general scratch allocations used by `RealClient`

At the same time, Python zero-copy APIs benefit from a reusable pool of registered buffers so callers do not need to repeatedly call `register_buffer()` and `unregister_buffer()` around every operation.

A previous design introduced a separate registered buffer pool that allocates extra regions, registers them one by one, and reuses them through lease objects. That approach provides strong isolation, but it also creates a second registered scratch-buffer system next to the existing local buffer allocator.

This document proposes a simpler direction: build the external registered buffer pool on top of the existing local buffer allocator, keep the pool logically separate at the API layer, and use **soft isolation** rather than hard partitioning.

## Problem Statement

We want to support reusable registered scratch buffers for external callers while preserving low latency for Mooncake's internal staging paths.

The main tension is not functional correctness: the existing local buffer is already registered and sub-allocatable. The real tension is **resource contention**:

- internal paths need short-lived scratch allocations
- external callers may hold buffers for longer
- bursty external demand can amplify allocator pressure and introduce latency spikes

We want to reuse the existing registered memory path without turning it into a source of long-tail latency.

## Goals

1. Reuse the existing `client_buffer_allocator_` and setup-time local memory registration.
2. Avoid per-buffer `register_buffer()` / `unregister_buffer()` in the common path.
3. Expose a Python-friendly leased buffer API for repeated zero-copy operations.
4. Preserve burst behavior: external demand may temporarily surge and should still be served when possible.
5. Prevent sustained external pressure from causing unacceptable latency inflation for internal Mooncake operations.
6. Keep the implementation materially smaller than a fully separate registered buffer pool.

## Non-Goals

1. Hard partitioning the local buffer into permanently reserved internal/external slices.
2. Adding a strict static quota for external allocations.
3. Replacing all internal scratch paths with a new allocator architecture.
4. Solving every future allocator QoS problem across CPU, GPU, disk, and network memory in one change.

## Why Soft Isolation

Hard isolation would reserve a dedicated external sub-pool inside local buffer memory. That avoids competition, but in practice it has the same utilization downside as a separate registered buffer pool: memory can sit idle in one partition while the other side is under pressure.

The preferred direction is therefore:

- one shared registered local buffer allocator
- separate internal and external usage semantics
- burst-aware scheduling and fallback behavior
- lightweight protections against long-lived external pressure

This keeps memory fungible while still giving Mooncake room to protect internal critical paths.

## High-Level Design

### Summary

We introduce a **soft-isolated external buffer pool** that leases sub-allocations from the existing `client_buffer_allocator_`.

The external pool does **not** own memory registration. Instead:

- the whole local buffer remains registered once at setup time
- external pool leases carve subregions from that already-registered buffer
- leases are represented as `BufferHandle`-backed Python objects
- the pool adds contention policy, usage accounting, and burst control on top of the allocator

### Core Idea

Instead of creating a second registered memory arena, we layer three things over the current allocator:

1. **External lease management**
   - acquire/release semantics
   - Python memoryview support
   - active lease tracking

2. **Soft-isolation accounting**
   - track internal transient demand and external outstanding usage
   - distinguish normal pressure from burst pressure

3. **Burst-aware admission policy**
   - allow external borrowing during bursts
   - bias allocator access toward short-lived internal work
   - shed or delay external growth only when contention becomes harmful

## API Shape

## Python API

The public API should remain close to the current buffer-pool usage model:

```python
from mooncake.buffer_pool import BufferPool

pool = BufferPool(store)

with pool.buffer(1 << 20) as lease:
    n = store.get_into("key", lease.ptr, lease.size)
    view = lease.buffer[:n]
```

Recommended constructor direction:

```python
BufferPool(
    store,
    enable_burst=True,
    block_on_exhaustion=True,
    default_timeout=None,
)
```

Notably absent from the soft-isolation version:

- no separate `max_bytes` for a dedicated arena
- no per-region registration knobs
- no mandatory size-class configuration in the first version

Optional tuning knobs may still be added later if operationally needed, but they should not define the initial design.

## Internal C++ API

Add a small external-pool layer in store/integration code that acquires from `RealClient::client_buffer_allocator_`.

Possible shape:

```c++
class SharedRegisteredBufferPool {
 public:
  std::shared_ptr<Lease> Acquire(size_t size,
                                 bool block = true,
                                 std::optional<double> timeout_s = std::nullopt);
  void Release(...);
};
```

This layer should not replace `ClientBufferAllocator`. It should wrap it.

## Allocation Model

### Base allocator

The underlying allocator remains `ClientBufferAllocator`.

Properties we inherit:

- one contiguous local buffer
- setup-time registration of that full region
- sub-allocation via `OffsetAllocator`
- RAII release through `BufferHandle`

### External lease objects

Each external acquisition returns a lease backed by a `BufferHandle` sub-allocation.

The lease is responsible for:

- pointer exposure (`ptr`)
- logical size exposure (`size`)
- Python buffer export (`memoryview`)
- release safety while exported views exist

This part can reuse most of the lease ergonomics from the current PR, but without region-level register/unregister.

## Soft-Isolation Policy

### Principle

Internal and external users share the same memory arena, but not the same priority.

Internal scratch use is latency-sensitive and typically short-lived.
External pool use is user-facing and may be bursty, but should back off before it materially harms internal work.

### Policy states

We define three operational states.

#### 1. Normal

- allocator has ample free space
- external acquisitions proceed immediately
- no extra contention logic is triggered

#### 2. Burst

- external requests arrive faster than normal steady-state reuse
- pool is allowed to grow external outstanding usage aggressively
- external callers may still acquire as long as internal pressure signals remain low

This preserves the main benefit the PR was trying to capture: bursts should not be rejected early just because they exceed a conservative steady-state guess.

#### 3. Contended

- free space drops below a safety watermark, or
- internal allocations begin waiting/failing, or
- external outstanding lifetime/volume indicates sustained occupation rather than a short burst

In this state, the system should protect internal paths by making external growth more conservative.

## Contention Signals

Soft isolation depends on measuring pressure, not on static partitioning.

The external pool should observe at least:

1. **Allocator free bytes estimate**
   - total local buffer size
   - outstanding external leased bytes
   - recent internal temporary allocation demand

2. **Internal allocation pain**
   - allocation failures in internal staging paths
   - wait time or retries in internal critical sections
   - fallback frequency when internal staging cannot allocate immediately

3. **External holding behavior**
   - current external outstanding bytes
   - oldest active lease age
   - lease reuse ratio vs fresh acquisition ratio

4. **Burst shape**
   - recent acquisition rate
   - recent release rate
   - net growth of external outstanding bytes over a short window

These signals let us distinguish a real burst from a slow memory capture by external callers.

## Burst Handling

### Requirement

We should support temporary surges in external zero-copy demand without forcing a hard split or a static external cap.

### Proposed behavior

During a burst:

- external allocations continue to use available space from the shared local buffer
- the pool does not enforce a hard ceiling derived from a fixed external quota
- short-lived surges are tolerated as long as internal pressure remains healthy

### When to intervene

We only intervene when burst behavior stops looking transient.

Examples:

- external outstanding bytes stay elevated for longer than a burst window
- internal staging allocations begin to slow down or fail
- free space approaches a critical watermark and does not recover

### Intervention order

When contention is detected, prefer the following sequence:

1. **Delay new external growth**
   - block or timeout new external acquisitions
   - do not revoke existing leases

2. **Prefer external reuse over external expansion**
   - encourage waiting for returned leases before taking more shared memory

3. **Fallback internal path if needed**
   - for selected internal operations, allow emergency fallback allocation outside the shared local buffer when that is already supported and safe

This keeps burst support intact while still protecting critical internal work.

## Safety Watermarks

Even without a strict quota, the pool should maintain internal safety margins.

We recommend two watermarks.

### Soft watermark

Below this level, the pool enters **contended** mode for new external growth:

- existing leases remain valid
- new external acquisitions may block
- metrics/logging become more verbose

### Hard watermark

Below this level, the pool treats further external growth as unsafe:

- non-blocking acquisitions fail immediately
- blocking acquisitions wait for release
- internal paths retain priority

This is not a static external quota. It is a shared-pool health mechanism.

## Internal Path Protection

The most important rule is:

> Mooncake internal short-lived staging allocations must be able to cut in front of sustained external pressure.

We do **not** need a fully preemptive allocator to achieve this. A simpler design is enough.

### Recommended protection mechanisms

#### 1. Separate accounting for external leases

Track all active external leases explicitly even though actual memory comes from the shared allocator.

This gives visibility into:

- bytes held by external users
- lease age
- whether pressure is bursty or sticky

#### 2. Internal fast-path priority

When allocator pressure rises, internal code paths should continue attempting allocation directly.
External acquisitions, by contrast, should consult the contention policy first.

That means:

- internal users stay close to raw allocator behavior
- external users see throttling first

#### 3. Optional emergency fallback for internal staging

For a narrow set of internal paths, we may allow a fallback scratch allocation outside the shared local buffer when allocator pressure is caused by external occupation and the operation is latency-critical.

This fallback should be narrowly scoped and used only where correctness and transport assumptions already allow it.

## Registration and Address Validation

The shared local buffer is registered as one large region during setup, but some APIs currently reason about registration using exact buffer pointers or explicit registration bookkeeping.

The soft-isolation design must therefore handle sub-buffer validation carefully.

### Requirement

Sub-allocations leased from the shared local buffer must be recognized as valid registered destinations for APIs such as ranged reads.

### Proposed change

Instead of treating registration as exact pointer membership only, extend the validation path to accept:

- exact explicit registrations from `register_buffer()`, and
- addresses that fall inside the setup-time registered local buffer region

This can be implemented by tracking the base pointer and size of the setup-time local buffer and resolving whether a queried address is a subrange within that region.

For externally leased sub-buffers, we should also retain logical capacity metadata so APIs that need destination capacity checks can validate safely.

## Reuse Strategy

The first version should prefer simplicity over a sophisticated slab allocator.

### Recommended first step

- keep `ClientBufferAllocator` as the only memory allocator
- do not add an extra size-class free-list layer initially
- let released `BufferHandle`s naturally return memory to the shared allocator

This already gives us:

- reuse at the allocator level
- no extra registration churn
- much less code than a separate region-based pool

### Future enhancement

If profiling later shows repeated churn in certain sizes, we can add an optional lease cache on top, but that should not be in the first version.

## Failure and Backpressure Semantics

### External acquire

If a request cannot be satisfied immediately:

- `block=False`: fail fast
- `block=True`: wait until memory is released or timeout expires

### Internal allocation

Internal paths should preserve current semantics as much as possible.
If new fallback behavior is introduced, it must be opt-in per path and observable in metrics.

### Lease release

- release returns memory to the shared allocator immediately
- release fails if exported Python views are still alive
- leaked leases are reclaimed by object destruction as today

## Observability

The design needs metrics from day one, otherwise burst tuning will be guesswork.

Recommended counters/gauges:

### Gauges

- local buffer total bytes
- local buffer estimated free bytes
- external active lease bytes
- external active lease count
- oldest external lease age
- pool contention state (`normal`, `burst`, `contended`)

### Counters

- external acquire requests
- external acquire successes
- external acquire waits
- external acquire timeouts
- external acquire failures due to pressure
- internal allocation failures
- internal fallback allocations
- transitions into contended mode

### Histograms

- external acquire latency
- external lease hold time
- internal staging allocation latency

## Expected Advantages

1. **Smaller implementation surface**
   - no second registered-memory lifecycle
   - no per-region register/unregister churn

2. **Better architectural unity**
   - one registered local memory system
   - one allocator foundation

3. **Better memory utilization than hard partitioning**
   - external bursts can borrow idle shared capacity
   - no permanently stranded partition by default

4. **Good fit for bursty usage**
   - external demand can expand opportunistically
   - throttling happens only when shared-pool health degrades

## Risks

1. **Allocator contention remains real**
   - soft isolation reduces harm; it does not eliminate competition

2. **Pressure inference may be imperfect**
   - poor watermarks can either under-protect internal paths or over-throttle external bursts

3. **Some APIs need registration-range awareness**
   - exact-pointer assumptions must be fixed where they are too strict

4. **Debuggability depends on metrics**
   - without observability, latency regressions will be hard to explain

## Alternatives Considered

### 1. Separate registered buffer pool with its own regions

Pros:

- strong isolation
- simple mental model
- independent lifecycle and sizing

Cons:

- second registered scratch-buffer system
- duplicate pool logic
- extra registration churn
- lower effective memory fungibility

### 2. Hard-partition existing local buffer

Pros:

- strong internal protection
- no repeated registration

Cons:

- effectively same stranded-capacity issue as a separate pool
- more invasive core allocator split
- weaker payoff relative to added complexity

### 3. Strict external quota

Pros:

- simple control rule
- easy to reason about

Cons:

- poorly matches burst workloads
- leaves throughput on the table during idle periods
- user preference is to avoid a rigid cap

## Rollout Plan

### Phase 1: Minimal soft-isolation pool

- expose external lease API backed by `client_buffer_allocator_`
- add active lease tracking
- add registration-range awareness for shared local buffer subregions
- add blocking/timeout acquire semantics
- add basic contention metrics

### Phase 2: Burst-aware contention policy

- add watermarks and state transitions
- throttle only new external growth under pressure
- add external hold-time and pressure metrics

### Phase 3: Targeted internal protection

- identify the few internal paths that need emergency fallback under contention
- add narrow fallback behavior only where justified by measurements

## Open Questions

1. Which internal paths are truly latency-critical and deserve emergency fallback?
2. Should the first version expose any tuning knobs at all, or keep policy fully internal?
3. Do we need explicit fairness across multiple Python callers, or is process-level fairness sufficient for now?
4. Is allocator-level fragmentation acceptable without an extra lease cache layer?
5. Which metrics should be surfaced to Python users versus internal telemetry only?

## Recommendation

Proceed with the soft-isolation design.

Concretely:

- build the external registered buffer pool on top of the existing local buffer allocator
- keep burst behavior permissive by default
- do not impose hard partitioning or a fixed external quota in the first version
- protect Mooncake internal latency by throttling only new external growth when shared-pool health degrades
- invest early in contention metrics and registration-range correctness

This gives us the smallest design that still matches the desired behavior: reusable registered buffers, burst tolerance, and better protection against latency spikes caused by internal/external contention.