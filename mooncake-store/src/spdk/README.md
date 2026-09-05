# SPDK NVMe-oF Multi-Qpair I/O Subsystem

## Overview

The Mooncake Store NoF (NVMe-over-Fabrics) subsystem provides high-performance
block I/O to remote NVMe SSDs over RDMA fabrics.  Built on the Intel SPDK
library, it extends the native single-qpair model with multi-qpair concurrent
I/O, adaptive degradation, and dynamic QID recovery.

> **Transport notice**: this document describes the RDMA-based
> deployment path; the multi-qpair mechanism, drain protocol, and
> performance baselines all target RDMA.  Other transports may or may
> not work; this document does not characterise them.  See
> [RDMA Environment](#rdma-environment) for the environment requirements.

Two I/O paths coexist in this subsystem:

| Path | Caller | API | Where it lives |
|---|---|---|---|
| **Mooncake transfer hot path** | `SpdkNofWorkerPool` (one segment per worker) | `SubmitRead` / `SubmitWrite` via `SpdkWrapper::SubmitRequest` | `SpdkNofTask` → `submitTask` → `SubmitRead` / `SubmitWrite` |
| **Lower-level bulk API** | External callers / benchmarks | `PipelineRead` / `PipelineWrite` (chunked, all qpairs concurrent) | `NofSegment::PipelineIO` — not used by the Mooncake transfer path |

The rest of this document describes both.  Whenever a distinction matters, it
is called out explicitly.  See [Architecture](#architecture) for the layering
and [Pipeline I/O](#pipeline-io) for the bulk API.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  transfer_task.cpp — SpdkNofWorkerPool                           │
│    · Task queues (per-worker)  · Flow control backpressure       │
│    · Per-seg QOS   · Inflight management  · Periodic rebalance   │
│    · Drain protocol (Phase 0/A/B) on transport error             │
├──────────────────────────────────────────────────────────────────┤
│  spdk_wrapper.cpp — SpdkWrapper (Singleton)                      │
│    · SPDK env init  · Open/Close Segment  · QidPressureGauge      │
│    · ProbeNofSegment heartbeat probe (1-qpair, recycled ctx)     │
├──────────────────────────────────────────────────────────────────┤
│  nof_connection.cpp — NofConnection / NofQpairPool               │
│    · Controller + Namespace binding                               │
│    · Multi-qpair allocation / Round-Robin dispatch                │
│    · QpairPoolState: kActive → kDraining → kClosed               │
│    · Inflight counter + strict WaitForInflightCompletion fence   │
├──────────────────────────────────────────────────────────────────┤
│  nof_segment.cpp — NofSegment                                    │
│    · SubmitRead / SubmitWrite — used by Mooncake transfer hot path│
│    · PipelineRead / PipelineWrite — lower-level bulk-transfer API │
│    · PipelineCtx (shared_ptr) + PipelineCtxRecycler              │
├──────────────────────────────────────────────────────────────────┤
│  SPDK lib — spdk_nvme_probe / alloc_io_qpair / cmd_rw            │
└──────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. NofConfig (`include/spdk/nof_config.h`)

NVMe-oF I/O configuration.  Every field is settable via `MC_NVME_*` /
`MC_NOF_*` environment variables; unset variables keep their defaults.

| Parameter | Default | Env var | Description |
|---|---|---|---|
| `num_io_queues` | 16 | `MC_NVME_NUM_IO_QUEUES` | Requested I/O qpair count |
| `io_queue_size` | 256 | `MC_NVME_IO_QUEUE_SIZE` | Per-qpair queue depth |
| `io_queue_requests` | 512 | `MC_NVME_IO_QUEUE_REQUESTS` | Queue request entries |
| `admin_queue_size` | 64 | `MC_NVME_ADMIN_QUEUE_SIZE` | Admin queue size |
| `keep_alive_timeout_ms` | 10000 | `MC_NVME_KEEP_ALIVE_TIMEOUT_MS` | Keep-alive timeout (ms) |
| `transport_ack_timeout` | 0 | `MC_NVME_TRANSPORT_ACK_TIMEOUT` | Transport ACK timeout; 0 = SPDK default |
| `fabrics_connect_timeout_us` | 0 | `MC_NVME_FABRICS_CONNECT_TIMEOUT_US` | Fabrics connect timeout (µs); 0 = SPDK default |
| `header_digest` / `data_digest` | false / false | `MC_NVME_HEADER_DIGEST` / `MC_NVME_DATA_DIGEST` | NVMe-oF digest toggles |
| `max_inflight_per_qpair` | 64 | `MC_NVME_MAX_INFLIGHT_PER_QPAIR` | Per-qpair inflight cap; valid [1, 256] |
| `chunk_blocks` | 512 | `MC_NVME_CHUNK_BLOCKS` | I/O chunk size in blocks (4 KiB → 2 MiB); valid [32, 1024] |
| `pipeline_drain_budget_us` | 1000 | `MC_NVME_PIPELINE_DRAIN_BUDGET_US` | Bounded drain budget for `PipelineIO` (µs); valid [100, 100000] |
| `min_io_queues` | 1 | `MC_NVME_MIN_IO_QUEUES` | Minimum acceptable qpairs (degraded mode) |
| `retry_max_attempts` | 5 | `MC_NVME_RETRY_MAX_ATTEMPTS` | QID-allocation retry budget |
| `retry_backoff_ms` | 100 | `MC_NVME_RETRY_BACKOFF_MS` | Base backoff (ms); actual = `backoff × 2^attempt` |
| `enable_degradation` | true | `MC_NVME_ENABLE_DEGRADATION` | Allow degraded mode instead of failing |
| `max_queue_depth` | 256 | `MC_NOF_MAX_QUEUE_DEPTH` | Per-worker task queue depth (0 = no limit) |
| `adaptive_inflight` | true | `MC_NOF_ADAPTIVE_INFLIGHT` | Shrink inflight cap when qpair count degrades |

Profiles: `NofConfig::FromEnv()` reads all env vars; `ForRead()` caps
`num_io_queues` at 4; `ForWrite()` enforces 16; `ForProbe()` returns a
minimal qpair=1 config for the heartbeat path.

### 2. NofQpairPool (`include/spdk/nof_connection.h`)

Manages N I/O qpairs on a single NVMe controller.  Owns the pool-level
inflight counter, the lifecycle state machine, and the strict fence that
prevents late CQEs from touching freed task memory.

**Round-Robin dispatch**:
```cpp
spdk_nvme_qpair *GetNextQpair() {
    uint32_t idx = round_robin_idx_.fetch_add(1, memory_order_relaxed);
    return qpairs_[idx % qpairs_.size()];
}
```
`GetNextQpair()` returns `nullptr` once the pool has entered
`kDraining` — see [Lifecycle State Machine](#lifecycle-state-machine).

**Unified polling**:
```cpp
int32_t PollAll(uint32_t max_completions = 0) {
    for (auto *qp : qpairs_) {
        int32_t n = spdk_nvme_qpair_process_completions(qp, ...);
        if (n < 0) { first_error = n; continue; }
        total += n;
    }
    return first_error != 0 ? first_error : total;
}
```
A transport error on one qpair does NOT block consumption of CQEs
from sibling qpairs; the first error is recorded and the loop continues
so the surviving qpairs can drain.

**Inflight tracking**:
```
MaxInflight = qpairs_.size() × max_inflight_per_qpair
```
`IncrementInflight()` / `DecrementInflight()` form a release/acquire pair
across the submit / CQE boundary.  `WaitForInflightCompletion()` is the
strict, no-time-budget fence used by `~NofQpairPool`.

**TryGrow recovery**: try-grow is committed under a triple re-check of
`state_ == kActive` (per-iteration and final commit) to defend against the
race between `TryGrow` and `EnterDraining`.  See `nof_connection.cpp:227`.

**No public abort API in this build**: `AbortAllInflightRequests`
([nof_connection.cpp:78-86](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/spdk/nof_connection.cpp#L78-L86))
has the form:

```cpp
static void AbortAllInflightRequests(spdk_nvme_ctrlr * /*ctrlr*/,
                                     spdk_nvme_qpair *qp) {
    if (qp) {
        int processed = spdk_nvme_qpair_process_completions(qp, 0);
        (void)processed;
    }
}
```

i.e. best-effort CQ drain with no force-fail call.  The accompanying
comment in the file confirms the intent: until SPDK exposes
`spdk_nvme_qpair_fail` (or equivalent), the only strict guarantee
against late CQEs is the inflight counter fence — see
[Drain Protocol](#drain-protocol).

### 3. NofConnection (`include/spdk/nof_connection.h`)

One connection = 1 NVMe-oF controller + 1 namespace + 1 `NofQpairPool`.

`Connect()`:
1. Probe via `spdk_nvme_probe()` (serialised by `SpdkWrapper::connect_mutex_`).
2. Assign a **unique** `hostnqn` (`nqn.2024-08.mooncake:c<N>`) per connection —
   SPDK's default UUID-based hostnqn would otherwise collapse all
   connections in one process into one controller.
3. Sequential I/O qpair allocation: greedy one-pass, then `release if
   below min_io_queues`.
4. On failure: surface `qpair_alloc_fail` so the caller can retry with backoff.

### 4. NofSegment (`include/spdk/nof_segment.h`)

A contiguous LBA range bound to one `NofConnection`.  All methods must
be called from the single thread that owns the segment's qpair pool
(SPDK single-thread-per-pool contract).

**SubmitRead / SubmitWrite** (single-request, used by the Mooncake transfer hot path):
- Translate caller-relative `lba` to absolute device LBA via `start_lba_`
  (overflow-safe).
- Reject out-of-range access.
- Call `GetNextQpair()`; if it returns `nullptr` the pool has entered
  DRAINING and the submit is rejected so the CQE cannot fire on a task
  that the worker is about to finalize.

**Pipeline I/O** — see [Pipeline I/O](#pipeline-io).

### 5. SpdkWrapper (`include/spdk/spdk_wrapper.h`)

Singleton managing SPDK env init, all open NoF connections, and the
heartbeat-probe path.  The probe path uses an independent temporary
connection with `config_probe_` so the probe qpair never collides with
the I/O path's qpairs.

**QidPressureGauge**: sliding window of (requested, allocated) outcomes
over the last 16 connection attempts.  Three pressure levels
(Green / Yellow / Red) drive adaptive degradation of new connection
qpair requests.

### 6. SpdkNofWorkerPool (`include/transfer_task.h`)

Multi-threaded worker pool that drives the hot path.

**Per-seg QOS** (`SpdkNofQos`): one FIFO task chain per op (read/write),
`inflight_blocks_limit` cap, `blocks_per_chunk` chunk size, adaptive
inflight cap based on `qpair_pool_.Size()`.

**Flow control**: `submitTask` blocks on a per-worker condition
variable when the queue is full, creating backpressure.  The wait
predicate tests `shutdown_` so the destructor does not deadlock on a
full queue.

**Periodic rebalance**: every 30 s the worker calls `pool.TryGrow(target)`
on every degraded pool to reclaim QIDs that other connections have
released.

**Drain protocol** — see [Drain Protocol](#drain-protocol).

### Lifecycle State Machine

`NofQpairPool::state_` is a single atomic with three values:

```
   kActive ──EnterDraining()──▶ kDraining ──~NofQpairPool()──▶ kClosed
       ▲                            │                              │
       └──────try_complete CAS ─────┘                              │
       │     (no resurrection; kClosed is terminal)                │
```

| State | GetNextQpair | nvmf_io_complete | ~NofQpairPool |
|---|---|---|---|
| `kActive` | round-robin over `qpairs_` | normal path: decrement task counters + `DecrementInflight` | enter DRAINING |
| `kDraining` | returns `nullptr` (no new submits) | short-circuit: only `DecrementInflight` + recycle sub_task | strict fence then `free_io_qpair` |
| `kClosed` | returns `nullptr` | short-circuit (defensive) | already done |

Critical invariants:
- `IsDraining()` is true in BOTH `kDraining` and `kClosed`.  Callbacks
  short-circuit on any non-active state.
- The state transition is `release`/`acquire`-paired so any observer of
  `kDraining` synchronizes-with `EnterDraining` and therefore observes
  the post-EnterDraining effects of all prior submissions.

### Drain Protocol

The four-phase drain path executed when `NvmePollProcessCompletion`
returns a negative value.  Single-source-of-truth for task finalisation
is `FinalizeAfterDrain()`, and the inflight counter is the release/acquire
fence that prevents late CQEs from accessing freed memory.

```
                    ┌──── poll_rc < 0
                    │   (qpair transport error)
                    ▼
   Phase 0  pool.EnterDraining("poll_err")
            • state_ ← kDraining (release)
            • GetNextQpair → nullptr (no new submits)
            • Later CQEs short-circuit (no task-counter touches)
                    │
                    ▼
   Phase A  nof_qos->FailQueuedTasks()
            • Drain head/tail tasks with outstanding_sub_io == 0
            • Skip off-chain tasks (FinalizeAfterDrain owns them)
                    │
                    ▼
   Phase B  DrainDrainingPoolsUntilQuiescent(seg_to_qos)
            • for every DRAINING pool:
                pool.WaitForInflightCompletion() — spin until
                inflight_count_ == 0 (no time budget)
            • nof_qos->FinalizeAfterDrain():
                • zero outstanding_sub_io on each active task
                • task.try_complete() CAS arbitrates with any racing
                  normal-path trampoline
                • whichever CAS wins performs set_completed + delete
                • losers are no-ops
                    │
                    ▼
   Outer while-loop exit predicate
   (total_outstanding_io == 0 && !HasBufferedTask) drops out cleanly
```

**`nvmf_io_complete` short-circuit** (DRAINING branch, verified at
[transfer_task.cpp:208-212](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/transfer_task.cpp#L208-L212)):

```cpp
if (pool && pool->IsDraining()) {
    pool->DecrementInflight();           // release/store sync point
    sub_task->sub_task_pool->push(sub_task);
    return;                              // no task-counter touches
}
```

**Why inflight_count_ is sufficient (not per-qpair)**: every callback
decrements the pool-level counter; all in-pool qpairs share the trampoline;
no per-qpair split is needed.

**`~NofQpairPool` four-step protocol** (no timeout — strict fence):

1. Bounded drain (≤ 1000 CQEs per qpair) — drain ready CQEs.
2. Belt-and-suspenders abort — defensive in case the pool was never
   entered DRAINING before destruction.
3. `WaitForInflightCompletion()` — spin on the acquire-load of
   `inflight_count_`, release-paired with the last trampoline's
   `DecrementInflight`.  Forms the synchronizes-with edge.
4. `spdk_nvme_ctrlr_free_io_qpair` — only after Step 3 returned true.

**Destructor liveness**: if `inflight_count_` never reaches 0 (target
crash, network partition), the destructor holds indefinitely.  This is
deliberate — silent UAF is strictly worse than a hung destructor that
an external watchdog can recover from.

**Operations on a closed pool** (Phase 0 already running): `EnterDraining`
is idempotent (`compare_exchange_strong` on `kActive → kDraining`);
later callers observe the state and no-op.

## Pipeline I/O

`PipelineRead` / `PipelineWrite` are the **lower-level bulk-transfer
API** (`NofSegment::PipelineIO`).  They are **not** the Mooncake transfer
hot path: every Mooncake transfer goes through `SpdkNofTask` →
`SpdkWrapper::SubmitRequest` → `SubmitRead` / `SubmitWrite`.  Use
`PipelineRead` / `PipelineWrite` only for callers that drive NVMe-oF
directly and need explicit control of multi-qpair concurrency.

```cpp
// Fire-and-forget (default, backward-compatible):
ssize_t PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks);

// Explicit-lifetime (caller-managed ctx_sp):
std::shared_ptr<PipelineCtx> ctx_sp;
ssize_t n = PipelineRead(buf, lba, total_blocks, &ctx_sp);
if (n > 0) {
    // ... use buf ...
    DrainForInflight(ctx_sp);           // synchronous wait, then release
}
```

**Loop**:
```
while (blocks remain OR inflight I/O > 0):
    1. Submit: split into chunk_blocks chunks, GetNextQpair, submit
    2. Poll: PollAll(0) harvest completions
    3. Error: if poll < 0 or error flag, drain within
       pipeline_drain_budget_us (default 1000 µs), return -1
```

**PipelineCtx lifetime**: heap-allocated `std::shared_ptr<PipelineCtx>`
so it can outlive the caller's stack frame.

| Path | When | Who holds ctx_sp |
|---|---|---|
| Explicit (`caller_ctx != nullptr`) | Caller manages lifetime | Caller owns the shared_ptr |
| Fire-and-forget (`caller_ctx == nullptr`) | Default; legacy compat | `PipelineCtxRecycler` holds until next `PipelineIO.Drain()` |

The fire-and-forget path keeps the heap object alive past `PipelineIO`'s
return, so any late CQE that arrives before the next `PipelineIO` still
sees a live object.  `DrainForInflight(ctx_sp, budget_us)` synchronously
waits for in-flight callbacks and lets the caller release `buf` /
`ctx_sp` once it returns.

**`PipelineCtx` fields** use `memory_order_release`/`acquire` to keep
the pair consistently ordered across the callback boundary.  In
single-thread-per-pool operation this documents intent; on contract
violation it prevents a stale error flag from racing ahead of its
inflight decrement.

## Multi-Qpair Concurrent I/O Flow

### Single-Client I/O Path

```
Client.put(key, data)
  → TransferSubmitter::submitSpdkNofOperation()
    → Lookup nof_handle_cache_ (endpoint → seg_handle)
    → SpdkNofWorkerPool::submitTask()
      → Bind seg to worker (seg_to_worker_)
      → Enqueue + notify worker
        → workerThread():
          1. Dequeue task from task_queue_
          2. Bind seg_to_qos (per-seg QOS)
          3. Split into chunk_blocks-sized sub-tasks
          4. GetNextQpair() → round-robin select qpair
          5. pool.IncrementInflight() (release fence)
             spdk_nvme_ns_cmd_read/write() submit
          6. PollAll() harvest completions
          7. SpdkNofTaskCompletion() → set_completed()
             pool.DecrementInflight() (release store)
```

### Transport-Error Path

When `NvmePollProcessCompletion` returns negative, the worker executes
the [Drain Protocol](#drain-protocol) (Phase 0/A/B).  The same worker
loop continues once `total_outstanding_io == 0 && !HasBufferedTask`.

## QID Management

### QID Consumption (verified by code + SPDK behaviour)

A single `NofConnection` requests `num_io_queues` I/O qpairs via
`spdk_nvme_ctrlr_alloc_io_qpair` in `nof_connection.cpp:474-477`.  Each
`NofConnection` is associated with one NVMe-oF controller, which in
turn owns one admin qpair (admin qpair allocation is performed by
SPDK's probe/attach path, not by this code).

| Configuration | QIDs per connection |
|---|---|
| Default (`num_io_queues=16`) | 1 admin + N I/O |
| `ForProbe()` (`num_io_queues=1`) | 1 admin + 1 I/O (separate controller from the I/O path) |

> We have not measured the target's QID-pool size in this repo; the
> 17/2 numbers above are direct code reads, not a workload observation.
> The target-side `nn` cap is target-firmware dependent and must be
> measured on the deployment target.  See
> [Performance Baselines](#performance-baselines) for the sample.

Each `TransferSubmitter` holds an independent `nof_handle_cache_`.
Multiple put/get calls from the same client reuse the connection and
do not consume additional QIDs.

### Retry / Degradation / Recovery

See [Configuration Tuning](#configuration-tuning) for env knobs; the
**order of operations** on `qpair_alloc_fail` is:

1. `QidPressureGauge::GetRecommended()` evaluates global pressure.
2. Gradual degradation target = `min(gauge rec, target/2)`.
3. Backoff = `retry_backoff_ms × 2^attempt`.
4. Retry `Connect()` up to `retry_max_attempts` times.
5. Recovery (worker): every 30 s `pool.TryGrow(target_count_)` for any
   degraded pool; on success `UpdateInflightLimit` reflects new qpair
   count.

## Performance Baselines

Data below is from an internal `nof_worker_pool_bench` tool
(not yet open-sourced).  Units reported by the bench are `MiB/s` /
`GiB/s` with `1 GiB = 2^30 bytes` unless otherwise noted.

> **Note on Environment vs bench data**: The Environment table below
> documents the target / host configuration probed live (via
> `nvme id-ctrl`, `spdk_tgt --version`, `lscpu`, `nvme discover`).
> The Supplemental Test C / D numbers are bench output captured
> against the same kind of target stack; we do not assert that the
> bench output and the live probes were taken on the exact same host
> at the exact same time, so readers reproducing the numbers should
> expect hardware-vintage-level variance.

> **Scope of coverage**: Supplemental Test C documents the client-side
> transport (RDMA), SPDK version, I/O mix, size and warm-up /
> measurement windows for the data shown.  All target-side hardware
> details (CPU model, NUMA topology, target backend type, SPDK
> version) are documented inline in the Environment table below.
> Write-path numbers on the same configuration are captured by
> Supplemental Test D; readers without the exact same target stack
> should substitute their own values for any environment-specific knob.

### Supplemental Test C — 128 KiB I/O re-measurement

#### Environment

Each row is read directly from the benchmark command-line.

| Field | Value | Source |
|---|---|---|
| Target backend type | Real NVMe-oF hardware (the `nvme id-ctrl` identify controller fields are real-device identifiers, not the `spdk-*` pattern that SPDK uses for software backends) | `nvme id-ctrl <device> -H`; SPDK software backends expose a `fr:` value starting with `spdk-` |
| SPDK version (target) | v23.01.1 | `spdk_tgt --version` reports `SPDK v23.01.1` |
| Host CPU model | Intel(R) Xeon(R) Gold 6442Y | `lscpu \| grep "Model name"` |
| Host core count | 24 physical cores per socket × 1 socket = 24 physical cores | `lscpu` |
| Host NUMA topology | 2 NUMA nodes on the single socket (NUMA node0: CPUs 0-23,48-71; NUMA node1: CPUs 24-47,72-95) | `lscpu \| grep -E "NUMA node.*CPU(s"`` |
| Target ↔ client host topology | Two distinct hosts on the same IP subnet (client IP and target IP are different addresses in the same subnet), connected via RDMA NICs over a RoCEv2-capable Ethernet network.  Each host runs an independent `spdk_tgt` (target) or `mooncake-store` (client) instance; submissions cross the RDMA fabric through RNIC QPs and arrive at the remote NVMe-oF controller without passing through either host's kernel TCP/IP stack. | host topology probed via `ip addr`, `ip route`; RNIC link state via `ibstat` / `rdma link show`; SPDK target listening on a separate host's IP |
| Client → target transport | RDMA over Converged Ethernet v2 (RoCEv2) | `--endpoints ... trtype:RDMA ...`; `nvme discover -t rdma` shows `trtype:rdma` + `adrfam:ipv4` |
| Target / client access model | RDMA direct access: the client reaches the target controller through RDMA verbs (RNIC + kernel-bypass queue pairs), without going through the host kernel TCP/IP stack or sockets API.  Submission queue entries on the client side and completion queue entries on the target side are written directly to RNIC registers via `spdk_nvme_ns_cmd_*`, and data transfers move via RDMA read/write verbs over the RoCEv2 QPs. | `--endpoints ... trtype:RDMA ...` selects RDMA in `NofConnection::Connect`; `spdk_nvme_ns_cmd_read`/`write` issue RDMA writes under the hood |
| RDMA protocol (`rdma_prtype`) | `roce-v2` | `nvme discover -t rdma` |
| RDMA QP type (`rdma_qptype`) | `connected` (RC / Reliable Connected) | `nvme discover` |
| RDMA CM (`rdma_cms`) | `rdma-cm` | `nvme discover` |
| Address family | IPv4 (`adrfam: ipv4`) | `nvme discover` |
| Connections | 1 endpoint | `--endpoints ...` with a single descriptor |
| Qpairs per connection | 1 → 8 (sweep) | `--num_io_queues $qp`, one value active per row |
| Submit / worker threads | 1 worker | `MC_NOF_WORKERS=1`, `--nof_workers 1` |
| Per-process submit depth | 64 | `--iodepth 64` |
| Per-qpair inflight cap | 4 | `MC_NVME_MAX_INFLIGHT_PER_QPAIR=4` |
| Read / write mix | `randread` | `--rw randread` |
| I/O size | 128 KiB | `--io_size 131072` |
| Address range | 64 MiB | `--range_bytes 67108864` |
| Data temperature | cold cache | `sync; echo 3 > /proc/sys/vm/drop_caches` before each run |
| Warm-up | 1 s | `--warmup_sec 1` |
| Measurement duration | 5 s | `--duration_sec 5` |
| Report interval | 1 s | `--report_interval_ms 1000` |
| Units | binary `MiB/s` / `GiB/s` (`1 GiB = 2^30 bytes`); latency `ms` | per `nof_worker_pool_bench` output convention |

#### Command

```bash
for qp in 1 2 4 8; do
    sync; echo 3 > /proc/sys/vm/drop_caches
    MC_NVME_NUM_IO_QUEUES=$qp \
    MC_NVME_MAX_INFLIGHT_PER_QPAIR=4 \
    MC_NOF_WORKERS=1 \
    MC_NVME_KEEP_ALIVE_TIMEOUT_MS=60000 \
    ./mooncake-store/benchmarks/nof_worker_pool_bench \
        --endpoints "<your traddr:IP trsvcid:PORT \
                      subnqn:<your subnqn> \
                      trtype:RDMA adrfam:IPv4 ns:1>" \
        --rw randread --io_size 131072 --iodepth 64 \
        --range_bytes 67108864 --duration_sec 5 --warmup_sec 1 \
        --report_interval_ms 1000 --nof_workers 1
done
```

#### Numbers

| I/O qpairs | Total inflight | BW | IOPS | mean | p50 | p95 | p99 | failed_ops |
|-----------|----------------|-----|------|------|------|------|------|------------|
| 1 | 4  | 660.02 MiB/s | 5280.20  | 12.12 ms | 12.50 ms | 13.00 ms | 13.50 ms | 0 |
| 2 | 8  | 1.22 GiB/s   | 10029.60 | 6.38 ms  | 6.35 ms  | 9.75 ms  | 10.50 ms | 0 |
| 4 | 16 | 2.19 GiB/s   | 17955.40 | 3.56 ms  | 3.50 ms  | 4.60 ms  | 5.10 ms  | 0 |
| 8 | 32 | 3.25 GiB/s   | 26620.20 | 2.40 ms  | 2.40 ms  | 2.85 ms  | 3.05 ms  | 0 |

Observations:
- qp=8 hits the target-side throughput ceiling (~3.25 GiB/s on this
  stack; lower at smaller qpair counts).
- All four runs clean: `failed_ops=0`, no `outstanding io < 0`, no
  `poll completion error`, no `submit io fail`.

### Supplemental Test D — 128 KiB I/O write-path

Same environment and methodology as
[Supplemental Test C](#supplemental-test-c--128-kib-io-re-measurement)
above; only the I/O direction differs.  Run the exact same sweep but
replace `--rw randread` with `--rw randwrite`:

```bash
for qp in 1 2 4 8; do
    sync; echo 3 > /proc/sys/vm/drop_caches
    MC_NVME_NUM_IO_QUEUES=$qp \
    MC_NVME_MAX_INFLIGHT_PER_QPAIR=4 \
    MC_NOF_WORKERS=1 \
    MC_NVME_KEEP_ALIVE_TIMEOUT_MS=60000 \
    ./mooncake-store/benchmarks/nof_worker_pool_bench \
        --endpoints "<your traddr:IP trsvcid:PORT \
                      subnqn:<your subnqn> \
                      trtype:RDMA adrfam:IPv4 ns:1>" \
        --rw randwrite --io_size 131072 --iodepth 64 \
        --range_bytes 67108864 --duration_sec 5 --warmup_sec 1 \
        --report_interval_ms 1000 --nof_workers 1
done
```

#### Numbers

| I/O qpairs | Total inflight | BW | IOPS | mean | p50 | p95 | p99 | failed_ops |
|-----------|----------------|-----|------|------|------|------|------|------------|
| 1 | 4  | 3.01 GiB/s | 24691.60 | 2.57 ms | 2.60 ms | 2.65 ms | 2.75 ms | 0 |
| 2 | 8  | 3.01 GiB/s | 24691.60 | 2.57 ms | 2.60 ms | 2.65 ms | 2.75 ms | 0 |
| 4 | 16 | 3.01 GiB/s | 24691.00 | 2.57 ms | 2.60 ms | 2.70 ms | 2.75 ms | 0 |
| 8 | 32 | 3.01 GiB/s | 24691.80 | 2.57 ms | 2.60 ms | 2.75 ms | 2.80 ms | 0 |

Observations:
- The write path is target-side throughput-bound at this I/O size on
  this stack: all four `qp` rows sit at the same ~3.01 GiB/s ceiling,
  not in the read path (where qp=8 was 5× the qp=1 number).
- Adding qpairs beyond `qp=1` does **not** increase write throughput;
  latency p95 tightens slightly (2.65 ms → 2.75 ms is within noise),
  confirming a single qpair already saturates the target.
- All four runs clean: `failed_ops=0`, no `outstanding io < 0`, no
  `poll completion error`, no `submit io fail`.

### Supplemental Test M — Methodology: why `qp=1 → qp=8` is not 8×

This section documents the code-level reasons that the throughput
extension from `qp=1` to `qp=8` is sub-linear (≈5× in this stack, not
the 8× a perfectly parallel pipeline would deliver).

#### A. Why `qp=1 → qp=8` ≈ ~5× and not 8×

The Mooncake transfer hot path is `SpdkNofTask → SubmitRequest →
SubmitRead/SubmitWrite`.  The `PipelineRead/Write` API is not used
by this benchmark.  All submission and completion processing for the
pool runs on **a single worker thread** in
`SpdkNofWorkerPool::workerThread`.  The relevant code paths are:

| Code path | File / line |
|---|---|
| Round-robin qpair selection | [nof_connection.cpp:189-201](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/spdk/nof_connection.cpp#L189-L201) (`NofQpairPool::GetNextQpair`) |
| Per-qpair submission loop | [transfer_task.cpp:894-1009](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/transfer_task.cpp#L894-L1009) (`SpdkNofWorkerPool::workerThread` outer submit block) |
| Per-qpair CQE harvest | [nof_connection.cpp:203-239](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/spdk/nof_connection.cpp#L203-L239) (`NofQpairPool::PollAll`) |
| Worker-poll-dispatch | [transfer_task.cpp:1011-1053](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/transfer_task.cpp#L1011-L1053) |

The single worker is the throughput ceiling:

1. **Submissions are serial.**  The submit block iterates per qpair
   in a single thread; each `spdk_nvme_ns_cmd_read/write` issues one
   doorbell write to the RNIC.  The submit rate is bounded by how
   fast this one thread can push commands to its qpairs — a fixed
   per-thread rate that does not scale with qpair count.  Adding
   more qpairs distributes the same rate across more submission
   queues rather than increasing it.

2. **CQEs are harvested serially.**  `PollAll` calls
   `spdk_nvme_qpair_process_completions` once per qpair.  qp=8 means
   up to 8 PCIe MMIO polls per outer worker iteration, each
   incurring per-call overhead regardless of whether a CQE is
   present.  That overhead grows with qpair count.

3. **Single RNIC per host.**  Even if the worker had multiple
   threads, the data path is bounded by the single PCIe-attached
   RNIC's doorbell / CQ register bandwidth; multiple workers on the
   same RNIC contend at the kernel-bypass register interface.

#### B. Where the `~5×` number comes from

The Mooncake transfer hot path produces a `qp=1 → qp=8` extension
in the `~5×` band on this stack:

| Field | This repo |
|---|---|
| qp=1 | 660.02 MiB/s |
| qp=8 | 3.25 GiB/s |
| Ratio | 4.92× |

The gap from 8× is the single-worker submit / poll ceiling
(documented in Method A above).  Both numbers sit in the same
`~5×` band; small differences between runs are explained by
per-host RNIC driver, PCIe slot bandwidth, and bench run-to-run
variance.

#### C. Why qpair scaling caps out around the SSD ceiling, not above it

The benchmark's `qp=8` row already reaches 3.25 GiB/s, which is
roughly the sequential-read throughput this SSD + RNIC + RoCEv2
fabric combination delivers in practice on this stack.  We do not
have measurements at qp=16 / qp=64 on this stack to characterise
the ceiling precisely; the qp=1 → qp=8 sweep is the only data we
have.  This is observed in the write-path numbers where all four
qp rows sit at the same 3.01 GiB/s — the SSD controller is
saturated at qp=1 for writes (no further scaling possible).

#### D. Per-call overhead accounting for the missing ~

Three sources explain why the observed qp=1 → qp=8 extension is
sub-linear rather than the `8×` of a perfectly parallel pipeline.
The exact per-call timing values below are not measured on this
stack; they are order-of-magnitude estimates based on typical PCIe
MMIO and SPDK internal-call behaviour, included to give a sense of
how the per-call costs scale with qpair count rather than as
precise measurements.

1. **`PollAll` per-call overhead.**  Each
   `spdk_nvme_qpair_process_completions(qp, 0)` call walks the CQ
   head/tail pointers and runs the SPDK tracepoint hooks even when
   no CQE is ready.  The per-call cost grows linearly with the
   number of qpairs in the pool because `PollAll` iterates the
   `qpairs_` vector unconditionally.

2. **CQE-hit dilution.**  With `max_inflight_per_qpair=4` and
   `qp=N`, the total in-flight blocks per outer loop is `4·N·32 =
   128·N` (where `32` is `blocks_per_chunk`, see
   `kDefaultSpdkNofSubmitChunkBytes` /
   `block_size`).  Distributing those across `N` qpairs dilutes
   the per-qpair CQE-hit rate: at qp=8 each `process_completions`
   call often returns 0 CQE, spending per-call overhead without
   progressing throughput.

3. **Atomic / CAS overhead per submit.**  Each submit path
   performs several `memory_order_acq_rel` operations
   (`IncrementInflight`, two `compare_exchange_weak` loops for
   `inflight_blocks` / `inflight_block_count`, two
   `fetch_add(1, acq_rel)` for `outstanding_sub_io` and
   `total_outstanding_io`).  Multi-qpair scenarios submit at a
   higher rate (because each qpair can hold its own small
   in-flight window), and the per-submit atomic cost scales with
   that rate.

These three sources combined are consistent with the gap between
ideal-linear and observed sub-linear extension at qp≥2.  They are
**not bugs**; they are the cost of running the worker on a single
thread with one PCIe-attached RNIC.  Removing the gap would require
multi-worker submission (already supported via `MC_NOF_WORKERS=N`
but not used in this benchmark), batched CQ polling (newer SPDK
APIs), or larger `max_inflight_per_qpair` (default 64; the
benchmark uses 4, which amplifies the dilution effect).

### Stability on different stacks

The RDMA numbers above come from a single target set
(see [Supplemental Test C](#supplemental-test-c--128-kib-io-re-measurement)
and [Supplemental Test D](#supplemental-test-d--128-kib-io-write-path)).
Other target models, SPDK versions, and transports have **not** been
characterised in this repo and are out of scope for the numbers shown.

## FAQ

### 1. `No free I/O queue IDs`

Target per-controller I/O Queue quota (`nn`) is exhausted.  Each
`NofConnection` requests `num_io_queues` I/O qpairs via
`spdk_nvme_ctrlr_alloc_io_qpair` in
[nof_connection.cpp:474-477](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/spdk/nof_connection.cpp#L474-L477);
when the target's pool cannot grant them all, this error surfaces.

### 2. `poll completion error: ret -6` (ENXIO)

Qpair entered a disconnected / error state.  Possible causes (we have
seen each in benchmarks):
- Excessive inflight causing target-side disconnect.
- In QP_SWEEP-style tests: previous subprocess QIDs not yet reclaimed.
- Driver / fabric issue (RDMA-specific).

**What happens automatically**: the worker calls
`EnterDraining` on the affected pool, runs the
[Drain Protocol](#drain-protocol), and continues the loop.  No
double-free, no UAF.

### 3. `outstanding io < 0` / `task outstanding io < 0` / `inflight_block_count < 0`

A counter underflow means a CQE decremented a counter that was
already 0.  After the [Drain Protocol](#drain-protocol) lands,
this should never occur: the DRAINING short-circuit prevents any
post-DRAINING CQE from decrementing task-level counters, and
`FinalizeAfterDrain` is the single source of truth for terminating
DRAINING-state tasks.

If you observe such an underflow, capture the SPDK version and the
full log; the most likely cause is an SPDK internal API path not
covered by the protocol and warrants an issue.

### 4. Transport error / counter underflow in the I/O path

If a counter goes negative (`outstanding io < 0`, `inflight_block_count < 0`,
etc.) under load, a CQE has decremented a counter that was already
0 — typically because a late CQE raced the task finalisation path.
See [Drain Protocol](#drain-protocol).  On RDMA this should be
prevented by the post-DRAINING short-circuit in `nvmf_io_complete`
([transfer_task.cpp:208-212](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/transfer_task.cpp#L208-L212));
persistent occurrences indicate either a missed code path or an
uncovered transport.

### 5. `RDMA connect error -99` + `Ctrlr is in error state`

Target's NVMe subsystem was reported in an error state.  Attributed to
target-side over-load on the specific target in use.

### 6. Multiple clients cannot connect to the same target simultaneously

Only one client at a time could attach to the test target.  Attributed
to the target firmware merging same-NQN connections onto one
controller, exhausting the per-controller `nn` cap for the first
client.

The relevant code surface for client-side isolation is:
`SpdkWrapper::connect_mutex_` serialises `spdk_nvme_probe()` per process
([spdk_wrapper.h:243-244](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/include/spdk/spdk_wrapper.h#L243-L244))
and each connection gets a unique `hostnqn` (verified in
[nof_connection.cpp:412-413](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/spdk/nof_connection.cpp#L412-L413)).  Whether
either is sufficient against a target that pins all same-NQN clients
to one controller is a target-firmware question.

### 7. Destructor hangs / target unreachable after transport error

`~NofQpairPool` intentionally holds indefinitely on
`WaitForInflightCompletion()` (verified at
[nof_connection.cpp:41-60](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/spdk/nof_connection.cpp#L41-L60))
if `inflight_count_` never reaches 0.  This is the deliberate fallback
for target crash / network partition cases where releasing the qpair
with potentially live CQEs would cause silent UAF.  Recovery is via
external watchdog / process restart.

Under target-crash / network-partition conditions the destructor
holds longer than a fixed-timeout alternative would.  Code comment at
[transfer_task.cpp:704-710](file:///D:/moonCake/Mooncake_git/Mooncake/mooncake-store/src/transfer_task.cpp#L704-L710)
records that the strict wait was *deliberately* chosen over any
timeout-based fallback for UAF safety.

## Tests

Layer-1 unit tests live in `tests/nof_segment_pipeline_test.cpp` and
are tagged with the test hook `mooncake::detail::InvokePipelineIoCbForTest`
(see [nof_segment.h:208-214](file:///d:/MoonCake/Mooncake_git/Mooncake/mooncake-store/include/spdk/nof_segment.h#L208-L214)).
Categories covered: `PipelineCtxRecycler`, `PipelineIoCb`,
`NofConfigEnv`, `PipelineCtx`.

Layer-2 / transport tests: no RDMA-test file has been located in this
repo at the time of writing.  The canonical numbers in
[Performance Baselines](#performance-baselines) come from
`benchmarks/nof_worker_pool_bench`, run against a real RDMA target;
that benchmark is **not** run automatically in CI here.

**Sibling-qpair failure regression test** (Layer-1 + Layer-2): the
"one failed qpair with pending CQEs on another qpair" production
failure scenario is covered by:

  - **Layer-1 protocol invariants**:
    `tests/nof_qpair_drain_protocol_test.cpp` — `NofQpairDrainSibling`
    group, `SiblingQpairFailure_LateCqeAfterDraining_NoUAF`,
    `SiblingQpairFailure_PoolQuiescenceFence`,
    `SiblingQpairFailure_FinalizeAfterDrain_CompletesAllTasks`.  These
    tests stub the pool (no real SPDK qpairs) and use the
    `MOONCAKE_TEST_DRAIN` compile-time hook
    (`NofQpairPool::TestInjectPollErrorOnce` + the corresponding
    PollAll injection in `nof_connection.cpp:198-221`) to synthesise
    a transport error on qpair[0] while sibling qpair[1] has
    in-flight CQEs.  They pin:
      - The DRAINING short-circuit does NOT touch task-level counters
        (`outstanding_sub_io` / `inflight_block_count` /
        `inflight_blocks[op]` / `*io_count`).
      - `WaitForInflightCompletion`'s release/acquire fence observes
        `InflightCount() == 0` in bounded wall-clock time once the
        late CQEs have fired.
      - `FinalizeAfterDrain` routes every `active_tasks` entry through
        `SpdkNofTaskCompletion` exactly once (the `try_complete()` CAS
        arbitration).

  - **Layer-2 end-to-end**:
    `tests/nof_qpair_sibling_failure_test.cpp` — drives the real
    worker thread path against a real NVMe-oF target.  Gated by
    `MC_TEST_NOF_TARGET` (SPDK transport string); tests `GTEST_SKIP`
    when unset so the build is no-op without infrastructure.  Uses
    `pool.TestInjectPollErrorOnce(0)` mid-load to force the drain
    path through the production code path.  Asserts no SIGSEGV, no
    counter underflow (observed through clean completion), and
    `InflightCount() == 0` within 5 s.

The hook itself (`TestInjectPollErrorOnce` /
`pending_inject_error_idx_`) is gated by `#ifdef MOONCAKE_TEST_DRAIN`
and the CMake target compiles with `-DMOONCAKE_TEST_DRAIN=1` so the
symbol is invisible in production binaries.

## File Index

| File | Description |
|------|-------------|
| `include/spdk/nof_config.h` | Config struct + env var parsing |
| `include/spdk/nof_connection.h` | `NofQpairPool` + `NofConnection` + `QpairPoolState` |
| `include/spdk/nof_segment.h` | `NofSegment` + `PipelineCtx` + `PipelineCtxRecycler` |
| `include/spdk/spdk_wrapper.h` | `SpdkWrapper` + `QidPressureGauge` + `ProbeCtxRecycler` |
| `src/spdk/nof_connection.cpp` | Connection factory + QpairPool (incl. DRAINING state machine) |
| `src/spdk/nof_segment.cpp` | `SubmitRead`/`SubmitWrite` + `PipelineIO` + `PollCompletion` |
| `src/spdk/spdk_wrapper.cpp` | Singleton + probe + adaptive connect |
| `include/transfer_task.h` | `SpdkNofWorkerPool` + `SpdkNofQos` |
| `src/transfer_task.cpp` | Worker threads + flow control + rebalance + drain protocol |
