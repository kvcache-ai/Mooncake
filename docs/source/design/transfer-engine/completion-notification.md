# Completion notification

Classic Transfer Engine can publish one terminal record per opted-in batch
on an engine-owned eventfd (RFC #5164 v1). The facility is a wake for
asynchronous batch completion. It does not replace `getBatchTransferStatus`,
and it does not change batches that never opt in.

The callable contract is on `TransferEngine` in `transfer_engine.h`. This
page records the design choices behind that contract.

## Scope

TENT does not implement the facility: `completionFd()` is `-1`, and
`drainCompletions` returns `CompletionNotificationUnsupported`. On classic
TE the fd is created once per engine. A non-negative fd means the engine
can notify. It does not mean every batch can finish without polling.

A batch opts in with `enableBatchCompletionNotification` after
`allocateBatchID` and before its only `submitTransfer`. The opt-in is
rejected when the fd is unavailable, when `slice_timeout` is enabled, or
when the batch already has tasks. A second submit on an opted-in batch is
rejected. Transports that complete only by foreground polling are not
eligible; those batches stay on today's submit and status-query path.

Poll-driven transports, a disabled facility, and an application deadline
all fall back to `getBatchTransferStatus`. An application deadline ends
the wait. It does not by itself settle a batch that still has a running
task.

## One fd for every opted-in batch

Each engine has one counter-mode eventfd. Every opted-in batch on that
engine shares it. A readable fd means at least one terminal record is
queued. It does not name the batch.

`drainCompletions` is the only way to learn which batches reached a
terminal snapshot. It removes the records it returns and does not free
batch handles. One call at a time owns that consumer. The queue tail and
the eventfd counter are not shared by concurrent drains. A dispatcher
therefore matches `batch_id` itself. Reading the fd and ignoring the
records drops completions that belonged to other batches.

The counter is level-triggered. The engine writes it when the queue goes
from empty to non-empty. Further records that arrive before the next drain
stay in the queue and do not each produce another write. A waiter that is
already awake observes them on the drain it is about to run, or on the
next one.

`close` on the original fd does not reliably wake a thread blocked on a
dup. Engine destruction writes the counter before closing it. That write
is the shutdown wake.

## One publication per batch

Publication is a latch, separate from `finished_task_count` and from the
status-query walk.

After the submit path has accepted the tasks, the latch is armed to one
more than the number of submitted tasks. Each task that reaches
`finishIfComplete` releases one token. The submit call releases the extra
token only when it returns success. The call that drops the counter to
zero publishes a single snapshot. A failed submit never releases the extra
token, so the latch does not reach zero and no record is queued for a
batch the caller did not accept.

The snapshot is `COMPLETED` or `FAILED`, plus a byte count. A failed task
contributes no bytes, matching a settled `getBatchTransferStatus` walk.
The failure flag is stored before that snapshot is taken, so a failed
event batch is not published as `COMPLETED`. Batches that did not opt in
still record the flag after the completion count, and their status
sequence is unchanged.

Tasks in the same batch are not signaled one by one. The record appears
when the last submitted task finishes. Notification latency for an earlier
task therefore waits on the slowest task in that batch. The RDMA or TCP
completion of that earlier task is not delayed.

`TIMEOUT` and `CANCELED` are not published in v1. A path that finishes a
task without going through `finishIfComplete` would also skip the latch.
Foreground timeout handling stays on the status query.

## Notify stays on the status query

The eventfd reports local completion only. Peer notify is still sent from
`getBatchTransferStatus` through `sendNotifyByID`. The completion callback
does not send it and must not call `getBatchTransferStatus`.

`submitTransferWithNotify` on an event-enabled batch returns
`InvalidArgument`. Storing the notify after submit would race a drain and
`freeBatchID` that reuse the batch id. The caller sends the notify after
it has observed the terminal record.

## Drain and free are different lifetimes

A terminal record names a batch. It does not own the batch. `drainCompletions`
copies the record out. `freeBatchID` invalidates the handle and releases
the descriptor once no completion callback is still inside it.

If `freeBatchID` runs before the record is published, the publication is
suppressed and nothing is queued. If it runs after publication but before
drain, the queued record is marked skipped and a later drain does not
return that id. `freeBatchID` may return before the transport has finished
touching buffers. A deferred free means in-flight work may still own those
buffers. The caller keeps them until `freeBatchID` returns success.

Buffers also stay allocated until the batch itself is fully settled.
Draining some other batch, or observing one failed task while a sibling
task is still running, is not that point.
