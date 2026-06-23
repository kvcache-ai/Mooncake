# TENT Failover

TENT hides transfer failures from the application by recovering inside the data path.
This document describes how the recovery works, which knobs control it, and how it is tested.

The design has three layers:

1. **Cross-transport failover** in `TransferEngineImpl`. When a transport reports a recoverable completion-stage failure, the engine moves that task to the next available transport (for example RDMA → TCP). Submit-stage failures are not retried today; see Known Gaps.
2. **Event-driven progress** through `ProgressWorker`. When supported transports write a terminal task status (`COMPLETED`, `FAILED`, or `TIMEOUT`), they notify the engine-side `BatchEventSink`, which wakes the background progress worker. This removes the old implicit requirement that an application must continuously poll `getTransferStatus()` before failover can advance.
3. **Intra-RDMA rail recovery** in `RailMonitor`. When a specific (local NIC, remote NIC) rail keeps failing, the monitor pauses it with exponential cooldown; a successful transfer or the cooldown expiry brings it back.

Application code submits a batch and either waits, polls `getTransferStatus`, or relies on the progress worker when it is enabled. It never sees a `FAILED` task as long as any healthy path remains and the failover budget is not exhausted.

## Fault Model

TENT focuses on three kinds of transient faults:

| Fault | Surface | Recovery action |
|-------|---------|-----------------|
| Work request completion error (WC error) | RDMA worker sees a bad completion | Rail-level `markFailed` + task-level resubmit |
| QP / endpoint failure | `submitTransferTasks` returns non-OK | *Not retried today*: task surfaces as `FAILED`. See Known Gaps. |
| Peer disconnect mid-transfer | Transport writes or reports `FAILED`/`TIMEOUT` | Cross-transport failover, driven either by polling or by the progress worker |

Permanent or application-visible errors (invalid arguments, out-of-memory, segment not found) are *not* retried; they are returned to the caller as-is.

## Architecture

```
Application
    |
    | submitTransfer(batch, requests)
    v
+--------------------------------------------------------------+
| TransferEngineImpl                                           |
|  - classify requests by TransportType                        |
|  - allocate one SubBatch per transport                       |
|  - attach BatchEventSink(batch_id, generation) to each batch  |
+------------------------------+-------------------------------+
                               |
                               | submitTransferTasks()
                               v
                +--------------------------------+
                | Supported terminal transports  |
                | RDMA / TCP / SHM / bufio       |
                +---------------+----------------+
                                |
                                | terminal status write
                                | COMPLETED / FAILED / TIMEOUT
                                v
                +--------------------------------+
                | BatchEventSink::notifyMaybeReady|
                +---------------+----------------+
                                |
                                | enqueue (BatchID, generation)
                                v
                +--------------------------------+
                | ProgressWorker                 |
                | coalesce duplicate work items  |
                +---------------+----------------+
                                |
                                | progressBatchIfAlive()
                                v
                +--------------------------------+
                | Aggregate task status          |
                | validate generation/lifetime   |
                +---------------+----------------+
                                |
                                | recoverable terminal failure
                                v
                +--------------------------------+
                | resubmitTransferTask           |
                | bump priority, pick next path  |
                +--------------------------------+

RDMA also has an independent per-rail recovery loop:

                +--------------------------------+
                | RdmaTransport workers          |
                | post / poll work completions   |
                +---------------+----------------+
                                |
                                | WC success / WC error
                                v
                +--------------------------------+
                | RailMonitor                    |
                | per-rail state + cooldown      |
                +---------------+----------------+
                                |
                                | available(local, remote)
                                v
                +--------------------------------+
                | RDMA scheduler picks a rail    |
                +--------------------------------+
```

* Each request is owned by one `TaskInfo`. `type` names the transport currently executing the task; `xport_priority` is the index into the ranked fallback list; `failover_count` caps how many times we may re-resolve the transport.
* The ranked fallback list comes from `getTransportType(req, priority)`. Priority 0 yields the best available transport; increasing priority walks down the list; `UNSPEC` means no transport left.
* `BatchEventSink` is attached per sub-batch. A terminal transport event calls `notifyMaybeReady()`, and the engine forwards `(BatchID, generation)` to `ProgressWorker`.
* RDMA rail state lives in `RailMonitor`. Its lifecycle is independent of the task-level state machine: a rail can be paused while tasks keep flowing on other rails.

## State Machine

### Cross-transport failover

`resubmitTransferTask` is the single entry point that promotes a failing task to the next transport:

```
++task.failover_count
if failover_count > max_failover_attempts  -> return error (exhausted)

task.xport_priority++
type = resolveTransport(task.request, task.xport_priority)
if type == UNSPEC                          -> return error (no transport)

transport_list_[type]->submitTransferTasks(...)
```

It has two recovery drivers plus one terminal exhaustion outcome:

1. **Completion-stage failure observed by polling.** `getTransferStatus(batch_id, task_id, status)` and the batch-form overload call `resubmitTransferTask` once per `FAILED` completion when `enable_auto_failover_on_poll` is true. On success the task is re-marked `PENDING` so the aggregated batch status does not latch to `FAILED` because of a task that is actually retrying.

2. **Completion-stage failure observed by `ProgressWorker`.** When `enable_progress_worker` is true, a supported transport terminal-status write wakes the background worker through `BatchEventSink`. The worker calls `progressBatchIfAlive`, which validates the batch generation and then runs the same aggregation/resubmit path as polling. This path is what allows `waitTransferCompletion` or an idle application thread to make progress without a tight polling loop.

3. **Exhaustion.** When the budget is hit, `resubmitTransferTask` sets the returned status to `InvalidEntry("Failover limit exceeded, all transports exhausted")`. Callers leave `task.type` unchanged; the task then reports `FAILED` through the normal status flow.

Submit-stage failures (`submitTransferTasks` returning non-OK) are **not** retried today. They mark the task as `UNSPEC`, and `getTransferStatus` short-circuits to `FAILED`. See Known Gaps for why.

### Event-driven progress worker

The progress worker is a coalescing notification path, not a second state machine:

1. `submitTransfer` allocates a transport `SubBatch` and attaches an engine-owned `BatchEventSink` to it.
2. A supported transport writes a terminal task status and calls the sink.
3. `TransferEngineImpl::notifyBatchMaybeReady(batch_id, generation)` pushes one work item into `ProgressWorker`.
4. `ProgressWorker` deduplicates work by `(BatchID, generation)` and wakes its worker thread.
5. The worker calls `progressBatchIfAlive(batch_id, generation, status)`. If the batch is still alive and the generation matches, the engine aggregates sub-task status and performs any needed failover/resubmit.

The generation check prevents ABA bugs when a batch slab address is freed and later reused. A late terminal event from the old batch carries the old generation and is ignored instead of progressing the new batch that happens to have the same `BatchID` pointer value.

The sink lifetime is also protected against late transport callbacks:

* Transports store a `std::shared_ptr<BatchEventSink>` rather than a raw owner pointer.
* `freeBatch` closes attached sinks before sub-batches are freed, so any late notification becomes a no-op.
* `TransferEngineImpl::deconstruct` closes sinks, drains transport async workers, and only then frees the remaining sub-batches.

### Supported terminal-event transports

Only transports with a clear active terminal status write are wired into the event path in this PR:

| Transport | Terminal event source | Notes |
|-----------|-----------------------|-------|
| RDMA | `updateSliceStatus` changes the task `status_word` from `PENDING` to a final status | The notify is emitted only when the whole task reaches a terminal status, not per slice. |
| TCP | `startTransfer` stores `COMPLETED` or `FAILED` | The task keeps a shared sink copied from the TCP sub-batch. |
| SHM | `startTransfer` stores `COMPLETED` or `FAILED` | Uses the sub-batch helper after the final status write. |
| bufio | `submitTransferTasks` finishes the synchronous file operation and writes the final status | Emits one terminal notification after the operation result is known. |

B-class transports such as io_uring, nvlink, mnnvl, sunrise_link, gds, and ascend_direct are intentionally out of scope here because they do not all expose the same active terminal-write moment. They need a separate ticker/event-hook design.

### RDMA rail recovery

Inside `RdmaTransport`, each completion drives the rail monitor:

* Bad completion → `rail.markFailed(local_nic, remote_nic)`
* Good completion → `rail.markRecovered(local_nic, remote_nic)`

`markFailed` bumps `error_count` inside `error_window_`. Once the count hits `error_threshold_` the rail is paused until `now + cooldown_`; the cooldown doubles on every repeat failure up to `kMaxCooldown` (300 s).

`markRecovered` clears the error count, un-pauses the rail, and resets the exponential-backoff memory so the next failure cycle starts from the initial cooldown. A fast path returns without work when the rail is already healthy, which is the common case on the completion hot path.

`available(local, remote)` is the gate every work request passes through before posting. If the cooldown has expired, `available` itself resets all backoff state (error count, resume time, cooldown) and logs `Rail recovered: ... (cooldown expired)`. Otherwise it returns false and the scheduler picks another rail via `findBestRemoteDevice`.

This produces two independent recovery signals — cooldown expiry and live success — so a flaky rail does not stall forever if no other rail is posted to, and a recovered rail returns to service at the first good completion instead of waiting for the full cooldown.

## Configuration

All knobs live in the top-level `transfer-engine.json`. Defaults are safe for production; tune only if you have evidence.

| Key | Default | Meaning |
|-----|---------|---------|
| `enable_auto_failover_on_poll` | `true` | Controls whether `getTransferStatus` automatically resubmits tasks that report a recoverable `FAILED` completion. Set to `false` to make status polling observational only; internal completion paths can still trigger failover/resubmit. |
| `enable_progress_worker` | `false` | Enables the background progress worker. When enabled, supported transport terminal events can advance batch aggregation and failover without requiring an application polling loop. |
| `max_failover_attempts` | `3` | Upper bound on `resubmitTransferTask` calls per task. `0` disables cross-transport failover entirely. `1` allows exactly one switch. |
| `transports/rdma/rail_error_threshold` | `3` | Number of failures inside `rail_error_window_secs` that trips a rail into the paused state. |
| `transports/rdma/rail_error_window_secs` | `10` | Sliding window for counting rail errors. A failure older than the window resets `error_count` to 1. |
| `transports/rdma/rail_cooldown_secs` | `30` | Initial cooldown after tripping. Doubles on each repeat failure, capped at 300 s. |

The RDMA keys are read by `RailMonitor::load`. Example:

```json
{
  "enable_auto_failover_on_poll": true,
  "enable_progress_worker": true,
  "max_failover_attempts": 3,
  "transports": {
    "rdma": {
      "rail_error_threshold": 3,
      "rail_error_window_secs": 10,
      "rail_cooldown_secs": 30
    }
  }
}
```

## Observability

### Metric

`tent_transport_failover_total` is a counter incremented once per successful transport switch inside `resubmitTransferTask`. A non-zero rate means the engine is actively recovering; a sudden jump usually points at a single bad link or flaky peer.

The counter is only built when TENT is compiled with `-DTENT_METRICS_ENABLED=ON` (see `metrics.md`). Without that flag the macro is a no-op.

### Log keywords

| Keyword | Interpretation |
|---------|----------------|
| `Transport failover: X -> Y (attempt N/M)` | A task has successfully switched transports. |
| `Task failover limit reached (M), last transport=X` | Task exhausted its budget and will surface `FAILED`. |
| `No more transports available after X failed` | `resolveTransport` returned `UNSPEC`; no further fallback exists for this request. |
| `ProgressWorker started` | Background progress worker is enabled and ready to consume terminal-event work items. |
| `Rail recovered: local_nic=... remote_nic=... (cooldown expired)` | Cooldown elapsed and the rail is back in service. |
| `Rail recovered: ... (un-paused by successful transfer)` | Live success on a previously paused rail brought it back early. |

## Testing

Real hardware faults are hard to stage, so TENT tests the failover machinery with decorator-style fault injection and progress-worker unit tests.

### FaultProxyTransport

`FaultProxyTransport` wraps any `Transport` and injects four policy-driven faults:

* `submit_fail_rate` — probability that `submitTransferTasks` returns an error.
* `status_corrupt_rate` — probability that `getTransferStatus` flips `COMPLETED → FAILED`.
* `fail_after_n_submits` — deterministic variant: succeed the first N submits, then always fail.
* `fail_install` — make `install()` fail, simulating a transport that cannot come up.

Because it implements the `Transport` interface, the engine sees an ordinary transport. All failover paths (`submitTransfer`, `getTransferStatus`, `ProgressWorker`, `resubmitTransferTask`) run unmodified.

### Test-only injection hook

`TransferEngineImpl::swapTransportForTest` replaces the transport in one slot after `construct()`. This is the only way the end-to-end test can wrap the real transport with `FaultProxyTransport` without bypassing `resolveTransport` or `resubmitTransferTask`. Production code never calls it.

### End-to-end suite

The end-to-end failover test suite drives the real `TransferEngineImpl` with fake transports (`FakeTransport`) wrapped in `FaultProxyTransport`. It uses a `p2p` metadata backend on `127.0.0.1` so no external services are required — the whole suite is self-contained.

Current cases:

| Test | What it exercises |
|------|-------------------|
| `StatusCorruptionTriggersFailoverToSecondary` | Primary reports `FAILED` in `getTransferStatus`; engine must resubmit on the secondary. |
| `BothTransportsFailExhaustsFailoverBudget` | Both transports fail at the completion stage; task must surface `FAILED` once the budget is drained. |
| `MixedFaultsAcrossManySubmissions` | 10 one-request batches with 30% completion corruption on RDMA; every task must end `COMPLETED`, and submit-counter math must hold. |
| `MaxFailoverAttemptsZeroDisablesFailover` | `max_failover_attempts = 0` → the first completion fault is permanent, TCP is never touched. |
| `MaxFailoverAttemptsOneAllowsSingleFailover` | `max_failover_attempts = 1` → one switch allowed; RDMA fault → TCP success. |
| `PerTaskFailoverCountsAreIndependent` | A failing task must not consume another task's budget; `failover_count` is strictly per-task. |

A test-local `PerRequestFaultProxy` (in the same file) subclasses `FaultProxyTransport` to take a `std::function` predicate, remembers which sub-task ids it marked as "poisoned" at submit time, and flips only those completions from `COMPLETED` to `FAILED` at status-query time.

### Progress-worker and terminal-event tests

`tent_progress_worker_test` covers the worker and transport-terminal integration directly:

| Test area | What it exercises |
|-----------|-------------------|
| Event-driven completion | A terminal transport event wakes `ProgressWorker` and advances a batch without a user polling loop. |
| Event-driven failover | A failed terminal status wakes the worker, which calls the same resubmit path used by poll-driven failover. |
| Disabled worker | Terminal notifies are harmless when `enable_progress_worker=false`. |
| Free/late-notify races | Closing sinks and generation checks prevent late transport callbacks from progressing freed or reused batches. |
| Deduplication | Multiple terminal events for the same `(BatchID, generation)` coalesce into one queued work item. |

### Running manually

The TENT tests are **not** in CI today (the upstream workflow builds with `USE_TENT=OFF`). Run them locally:

```bash
cmake -S . -B build-tent -DUSE_TENT=ON -DUSE_CUDA=OFF -DBUILD_UNIT_TESTS=ON
cmake --build build-tent --target tent_failover_test tent_engine_failover_e2e_test tent_progress_worker_test -j
./build-tent/mooncake-transfer-engine/tent/tests/tent_failover_test
./build-tent/mooncake-transfer-engine/tent/tests/tent_engine_failover_e2e_test
./build-tent/mooncake-transfer-engine/tent/tests/tent_progress_worker_test
```

Setting `USE_CUDA=OFF` forces `CpuPlatform`, which always reports `MTYPE_CPU`. With `USE_CUDA=ON` on a host without a GPU, `cudaPointerGetAttributes` fails, `getMemoryType` returns `MTYPE_UNKNOWN`, every transport reports unavailable, and `resolveTransport` returns `UNSPEC` before the fault injection ever runs.

Companion unit tests cover the rail monitor and related building blocks: `tent_rail_monitor_test`, `tent_failover_test`, `tent_fault_proxy_test`, and `tent_progress_worker_test`.

## Known Gaps

* **Submit-stage failures do not trigger failover.** When `submitTransferTasks` returns non-OK, every task in that call is marked `UNSPEC` and surfaces as `FAILED`. A naive retry loop here is unsafe for two reasons:
  1. **Merged requests.** When `merge_requests` is enabled (default), `task_id_list[type]` contains both the real merged task and its derived aliases. Resubmitting per task-id re-posts one logical transfer multiple times on the fallback transport, breaking the deduplication the merge pass established.
  2. **Partial enqueue.** Some transports (for example `ShmTransport::submitTransferTasks`, `NVLinkTransport::submitTransferTasks`) enqueue or start work for earlier requests in `request_list` before returning an error on a later one. The return status alone does not tell us which tasks partially succeeded, so a blanket resubmit would duplicate already-started transfers.
  A safe submit-stage recovery needs either (a) a transport-level "atomic submit" capability flag plus per-task skip of derived ids, or (b) per-request status returned from `submitTransferTasks`. Neither exists today.
* **B-class transport terminal events are not wired yet.** io_uring, nvlink, mnnvl, sunrise_link, gds, and ascend_direct need a separate progress source before they can participate in event-driven progress.
* `markRecovered` (and cooldown expiry in `available`) clears the exponential-backoff memory entirely. A rail that flaps repeatedly therefore does not accumulate a growing cooldown across recovery cycles. If this becomes a problem the fix is to decay rather than reset.
* Cross-transport failover is driven purely by return status; there is no latency-based "this transport is healthy but too slow, try another" signal. That belongs to the scheduler, not this document.
* TENT tests are not exercised by CI. A follow-up can add a CI job that builds with `-DUSE_TENT=ON -DUSE_CUDA=OFF -DBUILD_UNIT_TESTS=ON` and runs the `tent_*` test targets; none of the code in this document changes in that case.
