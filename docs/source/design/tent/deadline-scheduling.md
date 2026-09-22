# TENT Deadline Scheduling

## Overview

This document describes how TENT schedules transfers that carry a deadline: the
**admission queue** that orders, promotes and drops requests before they reach
a transport, and the **RDMA bandwidth arbitration** that orders slices on a NIC
before they are posted. Both are opt-in and both predict deadline feasibility
with one shared formula; they differ in what they can see ahead of a request,
and in what they do about it.

It also sets the two next to [device selection](slice-spraying.md), which
runs on the same NICs and keeps its own bandwidth estimate, because the three
are easy to confuse and are deliberately not the same thing.

The document is ordered from the shared parts to the specific ones: the path a
deadline takes, the prediction both layers make and the bandwidth series that
feeds it, then the admission queue and the arbitration each with a worked
example, then configuration, metrics and known limits.

## Background

A KV-cache transfer in disaggregated serving is only useful if it lands before
the consumer needs it. TENT's QoS priorities (see [Quality of
Service](qos.md)) separate business classes, but within one class every flow is
served alike: a NIC splits its bandwidth equally among contenders, and the
engine dispatches in arrival order. Neither knows that one flow has ten
milliseconds left and another has a second.

RFC #2519 introduced an absolute deadline on `Request` and built on it in three
steps, each opt-in and each leaving the previous behavior byte-identical when
off:

1. **Observe**: on completion, record how much of its window a transfer
   actually took, as measured latency over window. This ratio is the MLU
   (`tent_deadline_mlu_permille`).
2. **Order**: dispatch the request with the earliest deadline first.
3. **Degrade**: predict which requests cannot make their window and cancel
   them up front, so the caller can fall back (recompute locally, for
   example) instead of waiting for a late transfer.

RFC #2792 added the same prediction at the NIC: among slices that are about to
be posted on one queue pair, post the most urgent first.

## The Path of a Deadline

```
Request{deadline_ns, intent_type, priority}
        |
        v
  submitTransfer
        |
        v
  Admission queue (enable_runtime_queue)          <- runtime_queue/*
     tryAdmit      : capacity limits, EDF insert
     pickForDispatch: promotion, drop, dispatch window
        |
        v
  Transport (RDMA)
     device selection : which NIC                 <- transports/rdma/* (slice-spraying.md)
     arbitration      : post order on that NIC    <- deadline_bw_arbitration
        |
        v
  Completion -> tent_deadline_mlu_permille
```

A request with `deadline_ns == 0` takes the same path and is never reordered,
promoted or dropped: every predicate below treats "no deadline" as "nothing to
predict".

## Predicting Feasibility: MLU

The scheduling layers use the same ratio as the metric, with a **predicted**
transfer time in place of the measured latency: the metric reports what a
transfer took, the predictor estimates what it will take before it runs. Both
layers use one definition, `DeadlineMlu()` in `tent/runtime/deadline_mlu.h`:

```
predicted_mlu = ((bytes_ahead + length) / bandwidth) / (deadline_ns - now_ns)
```

- `length` is the request's own bytes.
- `bytes_ahead` is what must move before them. The deadline is absolute, so
  this wait counts against the window as an **additive** delay over the wire
  rate. It is not folded into a slower bandwidth, which would multiply the
  wait by the request's size.
- `bandwidth` is the **transmit estimate**, one of the two per-NIC series
  described in [Bandwidth Series](#bandwidth-series) below, never the
  selection EWMA that device selection reads.

MLU below 1 means the request is predicted to make its window; 1 is the
boundary; above 1 it is predicted to miss. Two edge rules: no deadline or no
usable bandwidth (`<= 0`) yields 0, so such a request is never urgent and never
dropped; a deadline already in the past is infinitely urgent, which in the
queue means it is dropped whenever the drop is on, and in the arbitration
means it posts first.

The two layers deliberately read **different** `bytes_ahead`, each counting the
bytes it can actually see at its decision point:

| Layer | Decides | `bytes_ahead` |
|---|---|---|
| Admission queue | before the request reaches a worker | every drop-eligible owner already dispatched and not yet completed, including slices still waiting in worker queues (`dispatching_bytes_`) |
| RDMA arbitration | at posting time, on one NIC | bytes already posted to that NIC's hardware (`getPostedBytes`), plus the slots the arbitration has already placed ahead in this pass |

The admission side reads the wider set on purpose: its decision (a drop) is
irreversible, so it is the conservative one. The arbitration only reorders, and
the worker-queue slices it would otherwise count are its own contenders.

### How Each Layer Evaluates It

The formula is shared; every input to it is chosen per layer.

| | Admission drop | RDMA arbitration |
|---|---|---|
| Evaluated | once per queued owner, in `pickForDispatch`, before the request reaches any worker | once per slice per slot, in `orderByDeadline`, just before `submitSlices` |
| `length` | the whole request | one slice |
| `bandwidth` | sum of the transmit estimates of all available RNICs (`getEstimatedBandwidth`) | the transmit estimate of the NIC these slices will post on |
| `bytes_ahead` on entry | `dispatching_bytes_`: every eligible owner dispatched and not yet completed, on any NIC, including slices still in worker queues | `getPostedBytes(dev)`: bytes on this NIC's hardware, not yet completed; nothing from worker queues |
| `bytes_ahead` during the pass | grows by each owner **dispatched** in this call; a dropped owner adds nothing | grows by each slice **placed** in an earlier slot; every slice is placed eventually |
| `now` | read once per `pickForDispatch` | read once per `orderByDeadline` |
| Who is scored | eligible owners only (RDMA route, not staged); others are dispatched without a prediction | every slice in the group; a slice without a deadline scores 0 |
| Threshold | `mlu >= mlu_local_threshold` → cancel | none; scores are only compared with each other |
| Effect of a high value | the request never runs | the slice posts earlier |
| Past deadline | infinite → cancelled | infinite → first slot |
| No usable bandwidth | drop disabled for this pass, everything dispatched | order left as it arrived |

Two consequences follow. The admission prediction treats all in-flight bytes
as if they were served serially ahead of the request, although they are spread
over several NICs and move in parallel, so it is systematically pessimistic;
that is why θ_local sits above 1.0. The arbitration prediction is per NIC and
per slice, so it is close to what that queue pair will actually do, but it
only ever sees the slices that reached this worker in this tick: a more urgent
slice arriving one tick later takes its turn in the next batch.

## Bandwidth Series

Each RDMA device keeps two bandwidth estimates, learned from the same
completion events but along two different loops. Both use the update
`new = α × old + (1 − α) × sample` and both are clamped to
`ewma_min_bandwidth_multiplier` .. `ewma_max_bandwidth_multiplier` of the
link speed; everything else differs.

### One Slice, Two Meters

```
 worker posts slice            NIC completes it          poller handles it
 ---------+------------------------+-------------------------+----------------
          | submit_ts recorded     |                         | poll_ts
          | posted_bytes += len    |                         | posted_bytes -= len
          | (busy stretch opens    |                         | completed_bytes += len
          |  if it was 0)          |                         |
          |                        |                         |
          |<------- post -> completion latency ------------->|  selection sample:
          |                                                  |  len / latency
          |                                                  |
          |  busy time: only while posted_bytes > 0          |  transmit sample, at most
          |  (this and every other slice on the NIC)         |  every 10 ms, last completion
          |                                                  |  of a poll pass:
          |                                                  |  Δcompleted_bytes / Δbusy_time
```

### Selection EWMA (device selection)

1. **Sample source**: one slice, on its own successful completion. The
   worker takes `poll_ts − submit_ts`, the time from this post to this
   completion, and the sample is `slice length / that time`.
2. **What it contains**: the slice's wire time **plus** its wait behind every
   work request the NIC already held when it was posted. A backed-up NIC
   therefore produces low samples.
3. **When**: every successful completion, unconditionally. Failed, flushed,
   timed-out, cancelled or re-routed slices contribute nothing.
4. **Update**: α = `bandwidth_learning_rate` = 0.01, so the estimate is
   almost entirely the latest sample; it reacts within one or two
   completions.
5. **Reader**: device selection, as `predicted_time = (inflight + slice) /
   ewma` when scoring NICs. Queueing being inside the sample is what the
   selector wants: it is choosing *among* NICs, and a NIC that is slow
   because it is busy should lose the comparison.

### Transmit Estimate (admission drop and arbitration)

1. **Sample source**: the NIC as a whole, over a time interval. Two counters
   run per device: `completed_bytes`, added to by every completion, and
   `busy_ns`, which accumulates only while the NIC has at least one posted,
   uncompleted work request (a stretch opens when `posted_bytes` goes from 0
   to non-zero and closes when it returns to 0).
2. **What it contains**: bytes moved per unit of time the NIC was actually
   working. Idle gaps between bursts are not charged, and a slice's own wait
   behind earlier work is not visible, because the ratio does not look at
   individual slices at all.
3. **When**: at most once per `transmit_meter_interval_ns` (10 ms), taken at
   the last completion of a poll pass (every completion in a pass carries the
   same timestamp, so a sample mid-pass would split one burst across two
   intervals). One sampler per interval is chosen by CAS when several lanes
   poll at once. The first sample only sets the baseline. An interval that
   spans more than `transmit_meter_max_interval_ns` (50 ms) of wall clock is
   discarded: it describes a link too far in the past. A slice that ends
   without moving its bytes (failed, flushed, timed out) invalidates the
   current stretch and the meter starts fresh.
4. **Update**: α = `transmit_bandwidth_learning_rate` = 0.9, so a single
   sample moves the estimate by a tenth of the difference; about ten
   intervals, roughly 100 ms under load, to follow a real change. With no
   usable interval the estimate keeps its last value, or the link-speed seed
   from `openDevice()`: the optimistic direction, which cannot cause a false
   drop.
5. **Readers**: the admission drop, as the sum over available devices
   (`getEstimatedBandwidth()`), and the arbitration, as this NIC's value.
   Both add the queueing term themselves through `bytes_ahead`, so the rate
   must not contain it, or the wait would be counted twice.

### Side by Side

The two series, input by input:

| | Selection EWMA | Transmit estimate |
|---|---|---|
| Unit of measurement | one slice | one NIC over one interval |
| Numerator | slice length | bytes completed in the interval |
| Denominator | post → completion of that slice | NIC busy time in the interval |
| Includes queueing behind earlier work | yes, on purpose | no, on purpose |
| Sampled | every successful completion | at most every 10 ms, last completion of a pass |
| α | 0.01 (follows the latest sample) | 0.9 (~100 ms to converge) |
| Fed by | `release()` | `maybeSampleTransmit()` |
| Read by | `DeviceSelector::allocate()` | `getEstimatedBandwidth()` (sum), `orderByDeadline()` (per NIC) |
| Question answered | which NIC is the better choice right now | how fast does this NIC move bytes once posted |

Why per-completion timing cannot serve the predictors: up to `max_qp_wr` work
requests are posted in one call with effectively one timestamp, and a poll
pass timestamps every completion it collects alike, so a slice's own
post-to-completion time grows with the depth of the batch it travelled in.
Deep enough, the selection sample would sit on its lower clamp on a perfectly
healthy link. Bytes over busy time does not depend on how the work was
batched.

## Admission Queue

The queue lives in `LocalTransferAdmissionQueue`
(`tent/runtime/admission_queue.*`) and is owned by `TransferEngineImpl`. It is
off unless `enable_runtime_queue` is true; with it off, `submitTransfer` hands
requests straight to the transport, as before.

### What Enters the Queue

A submit is queued as a whole or not at all. A submit that contains any
**staged** owner (one that must be copied through a staging buffer, see the
proxy path) bypasses the queue entirely; staging-internal submits always
queue. Each owner records whether it is **degradation eligible**: routed to
RDMA and not staged. Only eligible owners can be dropped and only their bytes
count toward `bytes_ahead`, because the bandwidth provider is the RDMA
transport's and says nothing about a TCP, NVLink or staging transfer.

`tryAdmit` enforces the capacity limits: `max_outstanding_owners` and
`max_outstanding_bytes` bound everything admitted and not yet terminal, with
`staging_owner_reserve` and `staging_byte_reserve` held back so staging-internal
work can always make progress. A request longer than `max_dispatch_bytes` is
rejected at submit, since it could never fit a dispatch window.

### Ordering

With `deadline_aware` off the queue is FIFO. With it on, owners carrying a
deadline are inserted in **earliest-deadline-first** order at admission time,
and owners without a deadline keep FIFO order behind them. Dispatch then
consumes from the front, so the hot path stays O(picked) rather than re-sorting
on every call.

### Promotion

`promotion_slack_ns`, when positive, moves any queued owner whose remaining
slack (`deadline_ns - now`) has fallen below that value to the front of the
queue, ahead of owners with more slack or none. A stable partition keeps EDF
order inside each group. This is how a request that was comfortable when it
was admitted but has since become urgent overtakes the ones admitted before
it. It requires `deadline_aware`.

### Deadline-Infeasible Drop

`mlu_local_threshold` (θ_local), when positive and combined with
`deadline_aware` and an installed bandwidth provider, turns on the drop.
`pickForDispatch` walks the queue from the front; for each eligible owner it
computes the predicted MLU against the current `dispatching_bytes_` and, if the
value reaches θ_local, marks the owner **CANCELED** instead of dispatching it,
releases its capacity, and raises the `on_local_decode_suggested` hook. The
scan continues, since later owners have looser deadlines. A dropped owner does
not consume dispatch budget.

The caller sees the drop as a `CANCELED` task status. The hook is the intended
signal for "recompute locally"; today the engine installs an empty hook set,
so nothing beyond the status is delivered (see [Known Limits](#known-limits)).

### Bandwidth Provider

The provider installed by `TransferEngineImpl` is
`RdmaTransport::getEstimatedBandwidth()`: the sum over the local RNICs of each
device's transmit estimate. It is installed only when the RDMA transport is
present; without it the drop is inactive even if θ_local is set, and a warning
is logged at startup.

### Choosing θ_local

MLU 1.0 is the boundary between met and missed, so a
threshold **below 1.0 drops requests the predictor itself expects to
succeed**. Measured on hardware with θ = 0.5, dispatch fell to a fraction of
the offered load while every request that was dropped would have made its
window. Dropping does not depress the bandwidth estimate in return: the
transmit meter charges bytes to the NIC's busy time, so a NIC carrying half the
load reads the same rate over half the time. The loss is throughput, in
proportion to how much feasible work the threshold refuses.

A value slightly **above** 1.0 is the useful setting, because the predictor is
conservative by construction. `bytes_ahead` is every eligible byte dispatched
and not yet completed, and the formula charges all of it ahead of the new
request as if it were served serially; in practice those bytes are spread over
several NICs and queue pairs and move in parallel, and the new request's own
slices are spread the same way. So a predicted MLU of 1.1 usually still lands
on time. θ in the range 1.2 to 1.5 absorbs that bias and drops only requests
the wait alone rules out. The code does not currently validate the lower
bound; treat 1.0 as the floor.

### Dispatch Window and Progress

`max_dispatch_owners` and `max_dispatch_bytes` bound how much the queue hands
to transports at once. The window refills on every submit and on every poll,
and the `ProgressWorker` refills it again whenever a transport reports task
completion. RDMA completions do not yet wake the worker, so with the queue
active the worker also refills on a timer, `progress_fallback_interval_us`
(50 ms by default). That timer is the upper bound on how long a dispatch slot
can sit free after an RDMA transfer finishes.

### Worked Example: Three Requests

Two RNICs whose transmit estimates sum to 800 Gb/s (100 GB/s), `deadline_aware`
on, θ_local = 1.2, `max_dispatch_bytes` raised to 1 GiB so the sizes below fit
one window, and an empty queue. Three submits arrive in this order:

| Request | Length | Deadline window at submit | Eligible |
|---|---|---|---|
| R1 | 64 MiB | 20 ms | yes (RDMA) |
| R2 | 512 MiB | 5 ms | yes (RDMA) |
| R3 | 16 MiB | 3 ms | no (routed to TCP) |

**Admission.** Each `tryAdmit` inserts by deadline, so the queue reads R3
(3 ms), R2 (5 ms), R1 (20 ms) regardless of arrival order. Suppose the first
`pickForDispatch` runs after all three are queued.

**Dispatch pass 1.** `bytes_ahead` starts at 0 (`dispatching_bytes_`).

- R3 is first. It is not eligible, so the drop check is skipped; it is
  dispatched to TCP. Its bytes do not enter `dispatching_bytes_`.
- R2: predicted time 512 MiB / 100 GB/s ≈ 5.4 ms against a 5 ms window, MLU
  ≈ 1.07. Below 1.2, so it is dispatched; `dispatching_bytes_` becomes
  512 MiB.
- R1: predicted time (512 + 64) MiB / 100 GB/s ≈ 6.0 ms against 20 ms, MLU
  ≈ 0.30. Dispatched; `dispatching_bytes_` is now 576 MiB.

**A fourth request.** R4, 256 MiB with a 6 ms window, arrives while R2 and R1
are still in flight. Its predicted time is (576 + 256) MiB / 100 GB/s ≈ 8.7
ms, MLU ≈ 1.45. That reaches 1.2: R4 is marked `CANCELED` at pick time and
never reaches a transport. The caller sees `CANCELED` on its next poll and can
recompute locally instead of waiting almost 9 ms for a transfer that would
land close to 3 ms late.

Had θ_local been 0.9, R2 would have been dropped in pass 1 with MLU 1.07: a
request the predictor expected to miss by only 0.4 ms, and one that on a
slightly better link would have made it. That is the sense in which values
below 1.0 are too aggressive and values just above 1.0 are the useful range.

**Completion.** When R2 completes, `complete()` subtracts its 512 MiB from
`dispatching_bytes_`, and the transmit meter has meanwhile learned from the
bytes R2 moved: if the NICs sustained 380 Gb/s rather than 400, the provider's
sum drifts down over the next ~100 ms and later predictions become slightly
more conservative.

## RDMA Bandwidth Arbitration

Arbitration is in `Workers::orderByDeadline` with the pure ordering policy in
`tent/transport/rdma/bw_arbitration.h`. It is off unless
`transports/rdma/deadline_bw_arbitration` is true; off, the post order is
exactly the arrival order.

It runs **within one priority tier**: QoS priorities still decide which tier
posts first, arbitration decides the order inside the tier. When a worker has
gathered the slices it is about to post on one NIC, it:

1. Reads that NIC's transmit estimate. Zero or below means nothing to predict
   from and the order is left alone.
2. Checks whether any slice carries a deadline; if none does, every MLU would
   be 0 and the pass is skipped.
3. Takes `bytes_ahead` as the NIC's **posted bytes**: work that has reached
   the hardware and has not completed. None of the candidates is in it, and
   neither is work still sitting in a worker queue.
4. Builds the order one slot at a time: the slice with the highest predicted
   MLU takes the next slot and its bytes join `bytes_ahead` for the ones still
   waiting, since the queue pair will post them in that order. Ties keep
   arrival order. The first 64 slots are resolved exactly; the remainder is
   ranked once against the bytes those slots accumulated. `submitSlices`
   posts a prefix of the order, as long as the queue pair's remaining budget
   (`max_qp_wr`, 256 by default, minus what is already outstanding), and the
   rest wait for the next tick.

Arbitration never drops, admits or moves a slice to another NIC; it only
decides the order in which already-selected slices are posted.

### Worked Example: Three Slices

A NIC has 32 MiB posted and not yet completed, and its transmit estimate is
400 Gb/s (50 GB/s). One worker is about to post three 16 MiB slices on it:

| Slice | Deadline window remaining |
|---|---|
| A | 10 ms |
| B | 2 ms |
| C | none |

First slot, `bytes_ahead` = 32 MiB. A and B each predict (32 + 16) MiB / 50
GB/s ≈ 1.0 ms of transfer: A's MLU is 1.0 / 10 = 0.10, B's is 1.0 / 2 = 0.50,
C's is 0. B takes the slot and its 16 MiB join `bytes_ahead`, now 48 MiB.

Second slot: A now predicts (48 + 16) MiB / 50 GB/s ≈ 1.34 ms, MLU 0.134; C is
still 0. A takes it. C posts last.

Post order: B, A, C. In arrival order (A, B, C) B would have waited behind A's
16 MiB, about 0.34 ms of its 2 ms window. The example also shows the two
rules a reader should expect: a slice with no deadline never overtakes one
with a deadline, and a slice's urgency rises as the slots ahead of it fill.

## Comparison with Device Selection

The earlier tables compared the two predictors' inputs and the two bandwidth
series. This one compares the three mechanisms themselves: what each is for,
where it runs, and what it can and cannot do.

| | Device selection | Admission queue | RDMA arbitration |
|---|---|---|---|
| Question | which NIC | whether and when to dispatch | in what order to post on a NIC |
| Runs | per slice, in the worker | per submit and per poll, in the engine | per post batch, in the worker |
| Reads | selection EWMA, NUMA tier, inflight charge | transmit estimate (sum), `dispatching_bytes_` | transmit estimate (this NIC), posted bytes |
| Can | choose, split across NICs | reorder, promote, cancel | reorder within a tier |
| Cannot | see deadlines | choose a NIC | drop or move a slice |
| Switch | `enable_smart_scheduling` | `enable_runtime_queue` + `deadline_aware` (+ `mlu_local_threshold`) | `deadline_bw_arbitration` |

Device selection is documented in [Slice Spraying](slice-spraying.md).

## Configuration

### Enabling the Queue

```json
{
  "enable_runtime_queue": true
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_runtime_queue` | bool | `false` | Route non-staged submits through the admission queue. Everything under `runtime_queue/` is read only when this is on |

### Admission Queue

```json
{
  "runtime_queue": {
    "max_outstanding_owners": 1024,
    "max_outstanding_bytes": 1073741824,
    "staging_owner_reserve": 0,
    "staging_byte_reserve": 0,
    "max_dispatch_owners": 64,
    "max_dispatch_bytes": 67108864,
    "deadline_aware": true,
    "promotion_slack_ns": 0,
    "mlu_local_threshold": 1.2,
    "progress_fallback_interval_us": 50000
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_outstanding_owners` | int | `1024` | Owners admitted and not yet terminal, queued or dispatched |
| `max_outstanding_bytes` | int | `1073741824` (1 GiB) | Same bound in bytes |
| `staging_owner_reserve` | int | `0` | Owners held back from user submits so staging-internal work can always be admitted |
| `staging_byte_reserve` | int | `0` | Same reserve in bytes |
| `max_dispatch_owners` | int | `64` | Owners in flight at the transports at once |
| `max_dispatch_bytes` | int | `67108864` (64 MiB) | Bytes in flight at the transports at once; a single request longer than this is rejected at submit |
| `deadline_aware` | bool | `false` | Earliest-deadline-first dispatch; owners without a deadline keep FIFO order behind those with one. Required by promotion and drop |
| `promotion_slack_ns` | int | `0` | Owners with less remaining slack than this move to the front of the queue. `0` disables promotion |
| `mlu_local_threshold` | float | `0.0` | θ_local. Eligible owners whose predicted MLU reaches this value are canceled instead of dispatched. `0` disables the drop. Values below `1.0` cancel requests predicted to succeed |
| `progress_fallback_interval_us` | int | `50000` | Timer on which the progress worker refills the dispatch window when no completion wake arrives. `0` disables the timer |

### RDMA Arbitration and the Transmit Estimate

```json
{
  "transports": {
    "rdma": {
      "deadline_bw_arbitration": true,
      "transmit_bandwidth_learning_rate": 0.9,
      "transmit_meter_interval_ns": 10000000,
      "transmit_meter_max_interval_ns": 50000000
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `deadline_bw_arbitration` | bool | `false` | Order slices most-urgent-first before posting on a NIC |
| `transmit_bandwidth_learning_rate` | float | `0.9` | α for the transmit estimate (`1.0` = never learn, `0.0` = always take the latest sample) |
| `transmit_meter_interval_ns` | int | `10000000` (10 ms) | Minimum wall-clock span of one meter interval |
| `transmit_meter_max_interval_ns` | int | `50000000` (50 ms) | An interval longer than this is discarded instead of learned from |

The transmit estimate also inherits the clamp and seed shared with device
selection: `ewma_min_bandwidth_multiplier`, `ewma_max_bandwidth_multiplier`,
`default_bandwidth_gbps`, `min_bandwidth_gbps`, `max_bandwidth_gbps`. See the
[Slice Spraying configuration](slice-spraying.md#configuration).

### Request Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `deadline_ns` | uint64 | `0` | Absolute steady-clock time in nanoseconds by which the transfer should complete. `0` means no deadline |
| `intent_type` | enum | `INTENT_UNSPEC` | What the transfer is for (foreground get, background prefetch, migration). Used by the transport selector's intent policies; not read by the scheduling described here |
| `priority` | int | `PRIO_HIGH` | QoS tier. Arbitration reorders only within a tier |

C++:

```cpp
Request req;
req.opcode = Request::WRITE;
req.source = local_buffer;
req.target_id = remote_segment;
req.target_offset = 0;
req.length = 1 << 20;
req.deadline_ns = now_ns() + 20'000'000;  // 20 ms from now, steady clock
engine.submitTransfer(batch, {req});
```

Python:

```python
req = tent.Request(tent.OpCode.WRITE, src, target_id, 0, length,
                   deadline_ns=now_ns + 20_000_000)
```

`now_ns` must come from the same steady clock the engine uses
(`std::chrono::steady_clock` on the C++ side). A wall-clock timestamp is not
comparable and will be read as a deadline far in the past or future.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `tent_deadline_mlu_permille` | Histogram | Measured MLU at completion, times 1000: submit-to-completion latency over the window the transfer had at submit. `1000` is the met/missed boundary. Recorded only for transfers that reached `COMPLETED` with a deadline still ahead at submit; this is the measured ratio, not the predictor's estimate |
| `tent_deadline_infeasible_total` | Counter | Transfers whose deadline was already in the past at submit time. Kept separate from the histogram so they are not read as high-MLU samples |

A dropped owner surfaces as a `CANCELED` task; it produces neither metric,
because it never ran. See [Metrics](metrics.md) for labels and export.

## Where Each Effect Is Observable

Ordering and promotion are engine-level and apply on any transport. The drop
and the arbitration only ever act on RDMA: the drop requires an RDMA-routed,
non-staged owner and an installed RDMA bandwidth provider, and the arbitration
runs in the RDMA workers. On a host with TCP only, `deadline_aware` and
`promotion_slack_ns` change dispatch order, while `mlu_local_threshold` and
`deadline_bw_arbitration` do nothing. That is by design, not a
misconfiguration.

## Known Limits

- **Drop notification.** The engine installs an empty `DegradationHooks`, and
  there is no public API to register one, so a caller learns of a drop only by
  observing `CANCELED`. The hook exists for an upper layer to trigger local
  recompute.
- **Threshold validation.** `mlu_local_threshold` below `1.0` is accepted and
  cancels requests the predictor expects to succeed. Treat `1.0` as the floor.
- **Mixed submits.** A submit that mixes staged and non-staged owners bypasses
  the queue as a whole, so its RDMA owners do not enter `dispatching_bytes_`
  and the drop predictor underestimates the queue ahead of later requests.
- **Quarantined owners.** An owner that is dispatched and then never reaches
  a terminal status (a batch abandoned by the lazy free path) keeps its bytes
  in `dispatching_bytes_`, so the drop predictor grows more pessimistic over
  time in that failure mode.
- **RDMA completion wake.** RDMA does not yet signal the progress worker on
  completion, so dispatch-window refill after an RDMA completion waits for the
  fallback timer.
- **Failover keeps the RDMA charge.** Eligibility is fixed at admission from
  the planned route. A request that enters as RDMA, fails there and is
  recovered over TCP keeps its bytes in `dispatching_bytes_` until it
  completes, so the drop predictor is pessimistic by one transfer's length for
  the duration of that recovery.
- **One provider, one transport.** The only bandwidth provider is the RDMA
  transport's, and `bytes_ahead` is a single sum, so only RDMA-routed,
  non-staged requests can be predicted and dropped. Extending the drop to
  another transport would need a provider per transport and a queue-ahead
  term per transport, with each owner recording which one predicts it. The
  structure allows this; nothing implements it.

## References

- RFC #2519: deadline-aware transfers (observe, order, degrade)
- RFC #2792: deadline-aware NIC bandwidth arbitration
- [TENT Slice Spraying](slice-spraying.md): device selection and the two bandwidth series
- [TENT Quality of Service](qos.md): priority tiers the arbitration works within
- [TENT Metrics](metrics.md)
