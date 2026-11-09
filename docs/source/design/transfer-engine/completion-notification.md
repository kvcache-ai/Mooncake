# Completion eventfd wait (RFC #5164 v1)

Classic Transfer Engine only. TENT returns `completionFd() == -1`. One
engine-owned fd is shared by every opted-in batch, so a readable fd may
belong to some other batch. `drainCompletions` does not free the batch
handle. Full contract text lives on `TransferEngine` in
`transfer_engine.h`.

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

Python asyncio should use one dispatcher per engine that routes drained
rows to per-batch futures. Register the future before submit so a fast
completion cannot be drained and discarded before `wait()` runs:

```python
import asyncio

class CompletionDispatcher:
    def __init__(self, engine, loop):
        self._engine = engine
        self._pending = {}  # batch_id -> Future
        self._early = {}    # batch_id -> row, if drained before wait()
        loop.add_reader(engine.completion_fd(), self._on_readable)

    def _on_readable(self):
        for row in self._engine.drain_completions():
            batch_id = row["batch_id"]
            fut = self._pending.pop(batch_id, None)
            if fut is not None and not fut.done():
                fut.set_result(row)
            else:
                self._early[batch_id] = row

    def wait(self, batch_id):
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        if batch_id in self._early:
            fut.set_result(self._early.pop(batch_id))
            return fut
        self._pending[batch_id] = fut
        return fut

async def run(engine):
    loop = asyncio.get_running_loop()
    dispatcher = CompletionDispatcher(engine, loop)
    try:
        # Prefer registering before submit when the batch id is known in
        # advance (C++ allocate + enable + submit). The Python helper
        # allocates inside batch_transfer_async, so register immediately
        # after it returns and keep early rows in `_early`.
        batch_id = engine.batch_transfer_async(
            target_hostname, buffers, peer_addrs, lengths, opcode,
            enable_completion_notification=True)
    except CompletionNotificationFallback:
        # Poll-driven transport. Use the ordinary submit/poll path.
        return
    if batch_id == 0:
        # openSegment, a length mismatch, or a failed submit.
        return
    target = await dispatcher.wait(batch_id)
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
