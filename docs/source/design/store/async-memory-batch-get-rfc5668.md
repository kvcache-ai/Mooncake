---
orphan: true
---

# RFC #5668 — bounded asynchronous memory batch read

This branch implements a **CPU-verifiable first implementation** for
[Mooncake issue #5668](https://github.com/kvcache-ai/Mooncake/issues/5668).
It is an experimental implementation pending integration and RDMA verification.
It does not implement the downstream CUDA / Ascend graph barriers.

## Python API

```python
op0 = store.batch_get_into_multi_buffers_async(
    ["layer0-key"], [[registered_ptr0]], [[byte_count0]])
op1 = store.batch_get_into_multi_buffers_async(
    ["layer1-key"], [[registered_ptr1]], [[byte_count1]])

assert op0.poll() is None  # if not yet finished
values0 = op0.wait(timeout_ms=1000)  # raises TimeoutError if not yet complete
values1 = op1.wait()  # per-key byte counts or negative error codes
```

A timeout only stops the caller's wait. It **does not** cancel I/O; destinations
must remain alive and registered through terminal completion. The next
`poll()`/`wait()` may still report success or failure.

## Pipeline and admission

1. `RealClient` checks that every provided slice lies in a writable
   registered region. Only the `rdma` protocol is supported.
2. `AsyncMemoryGetManager::Submit()` validates the key-major descriptors,
   duplicate keys, integer bounds, overlapping destination ranges, and
   both in-flight budgets. It reserves credits before accepting a request.
3. The **single submission thread** resolves replica metadata, validates
   memory routing/size/lease and submits native TransferFuture operations.
   Preflight happens before any transfer for that operation.
4. The **single completion thread** polls in-flight native futures and
   independently validates transfer results, checksums and leases before
   publishing the operation's immutable per-key result.
5. Credits and destination reservations are released only after the
   completion path drains backend accesses.

Default per-RealClient budgets in this implementation are 16 admitted reads
and 256 MiB of cumulative admitted destination bytes. Both queued and
submitted requests count. Full admission fails immediately without writing
destination memory.

The first implementation deliberately rejects overlapping destinations,
duplicate keys, unsupported source replica types (including SSD/DFS/NoF)
and non-RDMA protocols rather than silently using a staged path.

## Lifecycle contract

The manager retains every accepted handle, even if Python drops its reference.
`RealClient.unregister_buffer()` blocks new admissions, drains outstanding
operations, then deregisters a buffer. `RealClient.tearDownAll_internal()`
closes admission and joins both manager threads before destroying the
underlying Client/TransferEngine.

Caller-owned memory is borrowed, not reference-counted by the C++ API:
do not free/remap a destination without first completing the operation
or deregistering through the Store API. Arbitrary external memory free
cannot be intercepted.

## CPU regression verification

The GTest `async_memory_batch_get_test` uses an injected deterministic
completion backend and checks:

- L1 reaches the submission worker while L0 is still pending.
- L1 completes first; per-operation results remain independent.
- Timeouts do not cancel or prematurely publish readiness.
- Rejected operation/byte credits never invoke the backend.
- Duplicate keys, overlapping buffers and invalid/zero slices fail before
  dispatch. Destination reservations release on completion.
- Shutdown rejects admissions and drains accepted operations before return.

These tests validate the **control-plane state machine** only. They do
not prove RDMA completion, direct placement, checksum data correctness,
or failover behavior without a data-plane integration setup.

## Outstanding validation before proposing upstream

- [ ] Re-run native C++ target build and the focused GTest on the latest SHA.
- [ ] Re-run C++ format, Sphinx and DCO checks on the squashed commit.
- [ ] Test actual registered RDMA memory with discontiguous destinations.
- [ ] Verify transfer failures, checksum failures, partial writes and lease
      expiry against real `TransferFuture` completion.
- [ ] Compare against bounded concurrent blocking reads on the same hardware,
      measure backend acceptance overlap, CPU overhead and idle transfer gaps.
- [ ] Validate consumer stream/event barriers for GPU or Ascend integrations.

The earlier two-phase prototype
[#1861](https://github.com/kvcache-ai/Mooncake/pull/1861)
informed the interface design, but this implementation keeps both metadata
and transfer progress inside the Store-owned bounded dispatcher.
