# Completion eventfd wait (RFC #5164 v1)

Sketch of the supported polling-free path. Classic TE RDMA only; TENT
returns `completionFd() == -1`. One engine-owned fd is shared by every
opted-in batch, so a readable fd may belong to some other batch.

```text
fd = engine.completionFd()
if fd < 0:
    # Stay on getBatchTransferStatus.
    ...

batch = engine.allocateBatchID(n)
status = engine.enableBatchCompletionNotification(batch)
if not status.ok():
    # Unsupported path (poll-driven transport, slice_timeout, …).
    # Submit and poll getBatchTransferStatus as today.
    ...

status = engine.submitTransfer(batch, entries)  # one submit
if not status.ok():
    # Nothing was accepted, or the completion record was dropped.
    # Keep the buffers until freeBatchID returns OK. A deferred free
    # means in-flight work may still own them.
    freeBatchID(batch)
    ...

# Wait until this batch, not merely the fd, is fully settled.
settled = false
while not settled:
    wait until fd is readable, or until the application deadline
    if readable:
        engine.drainCompletions(out)    # does not free the handle
        for record in out:
            if record.batch_id == batch:
                settled = true           # every submitted task finished
    else:
        # A deadline only ends the wait. One FAILED query is not enough
        # while another task in this batch is still running.
        poll getBatchTransferStatus until this batch is fully settled
        settled = true

# Both branches retain buffers until `batch` itself is fully settled.
engine.freeBatchID(batch)
```

Python:

```python
fd = engine.completion_fd()
try:
    batch_id = engine.batch_transfer_async(
        ..., enable_completion_notification=True)
except CompletionNotificationFallback:
    # Poll-driven transport. Use the ordinary submit/poll path.
    ...
except RuntimeError:
    # openSegment, a length mismatch, or a failed notification submit.
    # Buffers may be reused unless the message says they are still in use.
    ...
else:
    target = None
    while target is None:
        # wait on fd (epoll / asyncio.add_reader)
        for row in engine.drain_completions():
            if row["batch_id"] == batch_id:
                target = row
    # `target` means every submitted task has settled. Then:
    engine.free_batch_id(batch_id)
```

Notes:

- Local completion only; peer notify still goes through
  `getBatchTransferStatus` → `sendNotifyByID`.
- v1 does not publish `TIMEOUT` / `CANCELED` onto the fd.
- Closing the fd does not by itself wake a poller blocked on a dup.
  Engine destruction writes the counter first; that write is the wake.
- See unit coverage in `tests/batch_counter_wait_test.cpp`.
