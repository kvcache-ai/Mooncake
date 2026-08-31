# Deadline-safe scatter lifecycle

`TransferEngine::ScatterTransferOperation` separates a caller's logical
deadline from the physical lifetime of submitted I/O. This distinction matters
for RDMA, DMA, and staged transfers: returning a timeout does not prove that a
device has stopped accessing the local buffer or its memory registration.

## Operation states

An operation reports one of these states through `snapshot()`:

```text
SUBMITTED -> IN_PROGRESS -> SUCCEEDED
                         -> FAILED

SUBMITTED/IN_PROGRESS -> TIMED_OUT
TIMED_OUT             -> CANCEL_REQUESTED -> DRAINING -> DRAINED
                                                  `-> QUARANTINED
QUARANTINED           -> DRAINING -> DRAINED
```

`SUCCEEDED`, `FAILED`, and `DRAINED` are physically complete states. A buffer
may be reused only when `snapshot().buffer_reusable` is true. `DRAINED` records
resource safety after a logical failure; it does not convert that failure into
success.

`QUARANTINED` is a bounded caller-visible state for work whose physical
termination could not be established before the drain deadline. Keep the
operation alive and do not reuse the buffer. Destroying the operation preserves
the original safe behavior and waits for physical completion.

## Deadlines and drain

Use `waitUntil()` when several transfers share one total deadline. Unlike
repeated relative waits, an absolute `std::chrono::steady_clock` deadline cannot
accidentally grant every fragment a new timeout budget.

When the deadline expires:

- the first timeout is preserved as the logical result;
- pending fragment callbacks receive that result exactly once;
- physical work remains owned and tracked;
- a late physical completion updates the snapshot without invoking callbacks a
  second time.

`cancelAndDrainUntil()` dispatches best-effort cancellation to backends that
support it and then polls for physical completion. Backends without generic
cancellation drain naturally. If the drain deadline expires, the operation
becomes quarantined rather than claiming the buffer is safe.

## Buffer ownership

Scatter ranges still accept caller-managed raw buffer addresses. Callers that
need a bounded return path may also pass `ScatterTransferOptions` containing
opaque `std::shared_ptr<void>` lifetime anchors that own the local allocations
and/or registrations. Transfer Engine retains the anchors until physical
completion, including while the operation is quarantined.

The anchor does not register or validate memory and does not replace the
caller's normal memory-registration API. It only makes the required lifetime
explicit and mechanically retainable.

## Integration boundary

Transfer Engine reports per-operation status, physical completion, first
failure, bytes, fragments, timeout/cancel/drain/quarantine events, late
completions, and completion latency. It does not coordinate tensor-parallel
ranks or commit framework state.

A multi-rank KV restore should therefore:

1. transfer each rank's temporary shard independently;
2. validate every rank;
3. commit only after all ranks succeed;
4. abort all temporary shards after any failure;
5. drain or quarantine unresolved operations before local recomputation reuses
   their staging memory.

Large-object windowing belongs above the transfer lifecycle. A Store-level
bounded retrieve should limit in-flight bytes, fragments, and operations and
must not recycle a staging window until its transfer snapshot reports that the
buffer is reusable.
