# Classic TE task congestion queries

Classic Transfer Engine provides two independent, read-only queries for a
submitted logical task: `getTaskCongestionState` returns a classification and
`getTaskCongestionDetail` returns the available supporting evidence. Existing
completion APIs and result layouts are unchanged. Queries do not poll a CQ,
post work, advance retries, or query the NIC.

## Scope and states

| State | Meaning |
| --- | --- |
| `NORMAL` | A supported RDMA task has no unresolved congestion evidence. |
| `CONGESTED` | A slice has unresolved byte-window or transient pressure evidence. |
| `LONG_UNAVAILABLE` | Existing transport health evidence identifies an unavailable path. This does not prove permanent peer or NIC failure. |
| `UNKNOWN` | Control is disabled, the transport is unsupported, or evidence is insufficient. Do not interpret this as healthy. |

This change supports Classic TE's RDMA path only. Non-RDMA transfers and
compile-time/runtime-disabled control return `UNKNOWN`. TENT adapters, TENT
recovery changes and Python bindings are not included. A Classic facade using
the TENT backend returns `NotImplemented` and resets the output to `UNKNOWN`;
it does not advance the TENT task to obtain a snapshot.

For multiple slices, unresolved `LONG_UNAVAILABLE` takes precedence over
`CONGESTED`. Details retain best-effort evidence from an abnormal event, not
live NIC counters. Among slices in the worst unresolved state, detail selects
one with the greatest observed timestamp. If no slice is unresolved, it selects
from retained slice evidence using the same rule. Observed timestamps take
precedence over missing timestamps; ties (including two missing timestamps)
select the larger slice index. Untouched slices have no retained evidence.
Each slice retains only its latest recorded evidence, so this is not an event
history. `resolved` means that the selected slice has returned to normal.
The supported RDMA task has one logical attempt, reported as attempt ID 1.
Fields with `observed=false` in C++, or without their `TASK_CONGESTION_HAS_*`
bit in C, have no observed value. A zero is not a substitute for missing data.

Repeated byte-window denials for the same slice, worker, path, selected domain
and controller generation retain the first event's timestamp, window and
inflight values for that episode. They are not refreshed on every retry.
Successful admission resolves the episode. A later denial records fresh
evidence after resolution, a worker/path/domain/generation change, or an
intervening unavailable-path or recorded completion-error evidence.

The queries reuse the existing admission controller and rail/context health;
they do not add another recovery state machine or change window parameters.
They do not change PUT APIs or caller retry policies.

If all eligible peer rails are paused during initial submission or redispatch,
the failed task retains `LONG_UNAVAILABLE` evidence with reason `kNoUsablePath`.
The affected path identifies the selected paused rail; no aggregate retry
deadline is inferred for the other rails.

## C++

```cpp
mooncake::TaskCongestionState state;
mooncake::TaskCongestionDetail detail;
auto simple = engine.getTaskCongestionState(batch_id, task_id, state);
auto detailed = engine.getTaskCongestionDetail(batch_id, task_id, detail);
if (simple.ok() && detailed.ok()) {
    // Use the classification and only the detail fields marked observed.
}
```

Use the existing completion API to wait for the task. Query before freeing
the batch. Queries do not extend its lifetime and must not run concurrently
with its release. Do not use stale or arbitrary batch handles. An unsubmitted
task index or a sentinel batch ID is invalid. If submission fails before a task
exists, use the submission error instead of querying an invented task ID.

## C

The Classic C functions use the same names and return the existing
`Status::Code` values (`0` for success). State and detail types are declared in
`task_congestion_status_c.h`, included by `transfer_engine_c.h`.

```c
int state = TASK_CONGESTION_UNKNOWN;
task_congestion_detail_t detail = {0};
size_t required = 0;
int rc = getTaskCongestionState(engine, batch_id, task_id, &state);
if (rc == 0) {
    rc = getTaskCongestionDetail(engine, batch_id, task_id, &detail,
                                 NULL, 0, &required);
}
```

For an observed affected path, `required` includes the terminating NUL.
Allocate that many bytes and query again. An absent path has length zero.
An undersized buffer returns an invalid-argument error without truncating or
writing the path. Two queries can observe different snapshots, so recheck the
returned length and status. Null engine/output pointers are invalid.
