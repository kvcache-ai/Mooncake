# Completion eventfd wait (RFC #5164 v1)

Classic Transfer Engine only. TENT returns `completionFd() == -1`. One
engine-owned fd is shared by every opted-in batch, so a readable fd may
belong to some other batch. `drainCompletions` does not free the batch
handle.

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

Python, including an asyncio wait:

```python
import asyncio

async def wait_for_batch(engine, batch_id):
    fd = engine.completion_fd()
    loop = asyncio.get_running_loop()
    target = None
    while target is None:
        readable = loop.create_future()
        loop.add_reader(fd, readable.set_result, None)
        try:
            await readable
        finally:
            loop.remove_reader(fd)
        for row in engine.drain_completions():
            if row["batch_id"] == batch_id:
                target = row
    return target

async def run(engine):
    try:
        batch_id = engine.batch_transfer_async(
            target_hostname, buffers, peer_addrs, lengths, opcode,
            enable_completion_notification=True)
    except CompletionNotificationFallback:
        # Poll-driven transport. Use the ordinary submit/poll path.
        return
    except RuntimeError:
        # openSegment, a length mismatch, or a failed notification submit.
        # Buffers may be reused unless the message says they are still in use.
        return
    # `target` means every submitted task has settled. Then:
    target = await wait_for_batch(engine, batch_id)
    engine.free_batch_id(batch_id)
    return target
```

Notes:

- Local completion only. Peer notify still goes through
  `getBatchTransferStatus` → `sendNotifyByID`.
- v1 does not publish `TIMEOUT` / `CANCELED` onto the fd.
- Closing the fd does not by itself wake a poller blocked on a dup.
  Engine destruction writes the counter first; that write is the wake.
- Unit coverage is in `tests/batch_counter_wait_test.cpp`. Real RDMA
  coverage is `tests/rdma_completion_fd_test.cpp` (`ctest -L rdma`).
