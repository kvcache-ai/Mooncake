# Completion eventfd wait (RFC #5164 v1)

Sketch of the supported polling-free path. Classic TE RDMA only; TENT
returns `completionFd() == -1`.

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

engine.submitTransfer(batch, entries)   # one submit; a second is rejected
wait until fd is readable, or until the application deadline
if readable:
    engine.drainCompletions(out)        # does not free the handle
    engine.freeBatchID(batch)
else:
    # Deadline ended the wait only. Keep buffers until status is terminal.
    poll getBatchTransferStatus until COMPLETED/FAILED
    engine.freeBatchID(batch)
```

Python:

```python
fd = engine.completion_fd()
batch_id = engine.batch_transfer_async(
    ..., enable_completion_notification=True)
# wait on fd (epoll / asyncio.add_reader), then:
for row in engine.drain_completions():
    ...
engine.free_batch_id(batch_id)
```

Notes:

- Local completion only; peer notify still goes through
  `getBatchTransferStatus` → `sendNotifyByID`.
- v1 does not publish `TIMEOUT` / `CANCELED` onto the fd.
- See unit coverage in `tests/batch_counter_wait_test.cpp`.
