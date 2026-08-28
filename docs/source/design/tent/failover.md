(tent-failover)=
# TENT Failover

TENT hides transfer failures from the application by recovering inside the data path.
This document describes how the recovery works, which knobs control it, and how it is tested.

The design has two layers:

1. **Cross-transport failover** in `TransferEngineImpl`. When a transport fails a task at the completion stage, the engine moves that task to the next available transport (for example RDMA → TCP). Submit-stage failures are not retried today; see Known Gaps.
2. **Intra-RDMA rail recovery** in `RailMonitor`. When a specific (local NIC, remote NIC) rail keeps failing, the monitor pauses it with exponential cooldown; a successful transfer or the cooldown expiry brings it back.

Application code submits a batch and polls `getTransferStatus`. It never sees a `FAILED` task as long as any healthy path remains and the failover budget is not exhausted.

## Fault Model

TENT focuses on three kinds of transient faults:

| Fault | Surface | Recovery action |
|-------|---------|-----------------|
| Work request completion error (WC error) | RDMA worker sees a bad completion | Rail-level `markFailed` + task-level resubmit |
| QP / endpoint failure | `submitTransferTasks` returns non-OK | *Not retried today*: task surfaces as `FAILED`. See Known Gaps. |
| Peer disconnect mid-transfer | `getTransferStatus` returns `FAILED` | Cross-transport failover |

Permanent or application-visible errors (invalid arguments, out-of-memory, segment not found) are *not* retried; they are returned to the caller as-is.

## Architecture

```
                +-------------------------+
 submitTransfer |  TransferEngineImpl     |
 ---------------> classify by TransportType
                |  submitTransferTasks    |----failure----+
                +-------------------------+               |
                      |                                   v
                      |                       resubmitTransferTask
                      |                       (bump priority, pick next
                      |                        transport, resubmit)
                      v
         +---------------------------+
         | RdmaTransport / workers   |
         |   +---------------------+ |
         |   | RailMonitor         | |
         |   |  per-rail state     | |
         |   |  cooldown / recover | |
         |   +---------------------+ |
         +---------------------------+
```

* Each request is owned by one `TaskInfo`. `type` names the transport currently executing the task; `xport_priority` is the index into the ranked fallback list; `failover_count` caps how many times we may re-resolve the transport.
* The ranked fallback list comes from `getTransportType(req, priority)`. Priority 0 yields the best available transport; increasing priority walks down the list; `UNSPEC` means no transport left.
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

It has two callers, one per recoverable failure surface:

1. **Completion-stage failure.** `getTransferStatus(batch_id, task_id, status)` and the batch-form overload call `resubmitTransferTask` once per `FAILED` completion. On success the task is re-marked `PENDING` so the aggregated batch status does not latch to `FAILED` because of a task that is actually retrying.

2. **Exhaustion.** When the budget is hit, `resubmitTransferTask` sets the returned status to `InvalidEntry("Failover limit exceeded, all transports exhausted")`. Callers leave `task.type` unchanged; the task then reports `FAILED` through the normal status flow.

Submit-stage failures (`submitTransferTasks` returning non-OK) are **not** retried today. They mark the task as `UNSPEC`, and `getTransferStatus` short-circuits to `FAILED`. See Known Gaps for why.

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
| `max_failover_attempts` | `3` | Upper bound on `resubmitTransferTask` calls per task. `0` disables cross-transport failover entirely. `1` allows exactly one switch. |
| `transports/rdma/rail_error_threshold` | `3` | Number of failures inside `rail_error_window_secs` that trips a rail into the paused state. |
| `transports/rdma/rail_error_window_secs` | `10` | Sliding window for counting rail errors. A failure older than the window resets `error_count` to 1. |
| `transports/rdma/rail_cooldown_secs` | `30` | Initial cooldown after tripping. Doubles on each repeat failure, capped at 300 s. |

The RDMA keys are read by `RailMonitor::load`. Example:

```json
{
  "enable_auto_failover_on_poll": true,
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
| `Rail recovered: local_nic=... remote_nic=... (cooldown expired)` | Cooldown elapsed and the rail is back in service. |
| `Rail recovered: ... (un-paused by successful transfer)` | Live success on a previously paused rail brought it back early. |

(tent-failover-testing)=
## Testing

Real hardware faults are hard to stage, so failover is tested by driving the real `TransferEngineImpl` with FakeTransport backends and a fault-injecting decorator. The engine is unmodified: a completion-stage `FAILED` looks like a WC error or a dropped peer, and `resubmitTransferTask` runs as it would in production.

The harness — why a fake `Transport` is enough, how fakes are swapped in, and what this can and cannot prove — is in {ref}`TENT Testing <tent-testing>`. That page is the mechanism; this section only notes what failover uses it for:

* Completion-stage `FAILED` on the primary must resubmit on the next available transport.
* Exhausting `max_failover_attempts` (including `0` and `1`) must surface `FAILED` and must not touch a transport beyond the budget.
* `failover_count` is per-task: one failing request must not spend another request's budget.
* With `enable_auto_failover_on_poll=false`, status polling is observational; `progressBatch` / `waitTransferCompletion` / `transferSync` still recover.

Submit-stage failures are intentionally not covered here; see Known Gaps.

## Known Gaps

* **Submit-stage failures do not trigger failover.** When `submitTransferTasks` returns non-OK, every task in that call is marked `UNSPEC` and surfaces as `FAILED`. A naive retry loop here is unsafe for two reasons:
  1. **Merged requests.** When `merge_requests` is enabled (default), `task_id_list[type]` contains both the real merged task and its derived aliases. Resubmitting per task-id re-posts one logical transfer multiple times on the fallback transport, breaking the deduplication the merge pass established.
  2. **Partial enqueue.** Some transports (for example `ShmTransport::submitTransferTasks`, `NVLinkTransport::submitTransferTasks`) enqueue or start work for earlier requests in `request_list` before returning an error on a later one. The return status alone does not tell us which tasks partially succeeded, so a blanket resubmit would duplicate already-started transfers.
  A safe submit-stage recovery needs either (a) a transport-level "atomic submit" capability flag plus per-task skip of derived ids, or (b) per-request status returned from `submitTransferTasks`. Neither exists today.
* `markRecovered` (and cooldown expiry in `available`) clears the exponential-backoff memory entirely. A rail that flaps repeatedly therefore does not accumulate a growing cooldown across recovery cycles. If this becomes a problem the fix is to decay rather than reset.
* Cross-transport failover is driven purely by return status; there is no latency-based "this transport is healthy but too slow, try another" signal. That belongs to the scheduler, not this document.
* Runtime-layer failover is covered by FakeTransport tests in the `tent-ci` `cuda-off` legs. DMA integrity, real WC errors, and staging under NVLink still need hardware runners; see {ref}`TENT Testing <tent-testing>`.
