# Ordered ReadPlan

ReadPlan executes a caller-defined sequence of grouped range reads through the
existing Store get-session API. It prepares range arguments in native code,
validates results, and publishes group readiness in order. The
{ref}`Python API reference <store-ordered-readplan>`
describes the layout format, methods, and caller obligations.

## Motivation and scope

Layer-wise KV loading requires a background reader to restore data before each
model layer consumes it. A Python implementation repeatedly constructs address,
size, and offset lists, crosses the binding boundary, checks results, and signals
the consumer. ReadPlan moves this loop into C++ and releases the GIL during
execution and waits, while retaining the existing session and transport paths.

Groups are application-defined rather than model-specific. Multiple layouts
can describe different pools needed by a group. The application supplies the
background thread or executor and decides when to consume ready data.

The initial scope is memory-replica reads. ReadPlan introduces no new wire
protocol, storage format, replica-selection policy, or device-stream completion
mechanism. It is one-shot and provides no cancellation or consumer backpressure.

## Execution lifecycle

The implementation separates native execution from its Python binding:

- `mooncake-store/src/read_plan.cpp` expands layouts, manages the session,
  validates results, and coordinates publication and cleanup.
- `mooncake-integration/store/store_py.cpp` exposes creation, execution, waiting,
  and statistics, and protects client lifetime while plans are unfinished.

Execution reserves the plan's keys on its client, deduplicates them for session
startup, and reuses the resulting replica metadata across groups. For each group,
native code expands destination row indices into addresses and constructs the
per-key sizes and source offsets. Layout dimensions and arithmetic overflow are
validated while preparing ranges.

A group is publishable only after the result count and each key's returned byte
count match the requested reads. Publication is monotonic: all earlier groups
must be ready before a later group is exposed to consumers. Empty groups still
participate in ordering but do not contribute validated-read statistics.

The final group is held back until session cleanup succeeds. This makes final
readiness a stronger condition than an intermediate group's readiness. Callers
still join `run()` before releasing resources, including on failure.

## Optional two-group pipeline

Sequential execution is the default. With `MOONCAKE_READ_PLAN_PIPELINE=1`, a
multi-group plan uses two worker threads and a two-group window. This overlaps
adjacent reads without changing the underlying synchronous ranged-read API.
A single-group plan remains sequential.

For example, if group 1 completes while group 0 is still reading, group 1 is not
published early. Once group 0 validates, publication can advance through both
completed groups. The window advances with publication, not with the consumer's
calls to `wait()`.

Consequently, the window bounds concurrent group reads but does not bound the
distance between completed reads and model computation. Applications must not
reuse one group's destination storage for a later group. Such reuse would need a
separate consumer-acknowledgment protocol, which this design does not provide.

The pipeline is opt-in because concurrent submission consumes additional CPU
resources and may not improve every workload. Its benefit depends on layout,
request size, and the underlying transport. It does not change RDMA queue
settings or introduce transport-level packing.

## Failure handling

Session startup, range reads, result validation, and session cleanup may fail.
In pipeline mode, in-flight reads are drained before cleanup and failure
publication. This prevents an error notification from racing ongoing reads that
still use the client or destination memory.

An already published group remains successful if a subsequent read or cleanup
fails. Unpublished groups report failure after draining and the cleanup attempt;
`run()` propagates the error as well. Cleanup failure therefore prevents final
successful readiness without invalidating earlier published data.

Destination writes are not transactional. An unpublished or failed group may
have modified some or all of its destination bytes. Consumers must use readiness
results rather than infer success from buffer contents.

## Ownership and concurrency decisions

ReadPlan owns a strong client reference, but receives only raw destination
addresses. The Python binding also retains the Store wrapper and rejects
`close()` while a plan is unfinished, including before execution starts.
Discarding an unstarted plan releases that protection; completed and failed
plans no longer block explicit close.

Destination allocation owners are intentionally not retained by the plan.
Retaining a Python object would not prevent explicit deallocation, resizing, or
unregistration of its storage. The caller is responsible for validity and
registration throughout execution, and for joining `run()` before release.

Different groups must have disjoint destination ranges, including across
layouts and nonadjacent groups. The requirement applies even in sequential mode:
reading the next group could otherwise overwrite data still being consumed.
Address-alias detection is not implemented.

A native reservation prevents concurrent executing plans from sharing keys on
the same client. The legacy session API uses a key-indexed table and does not
participate in this reservation mechanism, so callers must not mix legacy
sessions on those keys with an executing plan. This is not a general session
ownership redesign.
