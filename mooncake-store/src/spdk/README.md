
# SPDK NVMe-oF Multi-Qpair I/O Subsystem

## Overview

The Mooncake Store NoF (NVMe-over-Fabrics) subsystem provides high-performance
block I/O to remote NVMe SSDs over RDMA/TCP fabrics.  Built on the Intel SPDK
library, it extends the native single-qpair model with multi-qpair concurrent
I/O, adaptive degradation, and dynamic QID recovery.  The subsystem also
exposes a lower-level `PipelineRead`/`PipelineWrite` chunked-submission API for
callers that want to drive NVMe-oF directly without going through
`SpdkNofWorkerPool`; this API is **not** part of the Mooncake transfer hot
path — see [Architecture](#architecture) and the `Pipeline I/O` section below.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  transfer_task.cpp — SpdkNofWorkerPool                           │
│    · Task queues (per-worker)  · Flow control backpressure       │
│    · Per-seg QOS   · Inflight management  · Periodic rebalance   │
├──────────────────────────────────────────────────────────────────┤
│  spdk_wrapper.cpp — SpdkWrapper (Singleton)                      │
│    · SPDK env init  · Open/Close Segment                         │
│    · QidPressureGauge global pressure sensing                    │
│    · ProbeNofSegment heartbeat probe                             │
├──────────────────────────────────────────────────────────────────┤
│  nof_connection.cpp — NofConnection / NofQpairPool               │
│    · Controller + Namespace binding                              │
│    · Multi-qpair allocation / Round-Robin dispatch               │
│    · Sequential allocation + backoff retry + TryGrow             │
├──────────────────────────────────────────────────────────────────┤
│  nof_segment.cpp — NofSegment                                    │
│    · SubmitRead / SubmitWrite — used by Mooncake transfer hot path│
│    · PipelineRead / PipelineWrite — lower-level bulk-transfer API │
├──────────────────────────────────────────────────────────────────┤
│  SPDK lib — spdk_nvme_probe / alloc_io_qpair / cmd_rw            │
└──────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. NofConfig (`include/spdk/nof_config.h`)

NVMe-oF I/O configuration.  All fields are set via `MC_NVME_*` / `MC_NOF_*`
environment variables; unset variables keep their default values.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_io_queues` | 16 | Requested I/O qpair count |
| `io_queue_size` | 256 | Per-qpair queue depth |
| `io_queue_requests` | 512 | Queue request entries |
| `max_inflight_per_qpair` | 64 | Max in-flight I/Os per qpair |
| `chunk_blocks` | 512 | I/O chunk size (512 blk = 2 MiB @ 4 KiB) |
| `keep_alive_timeout_ms` | 10000 | Keep-alive timeout (ms) |
| `min_io_queues` | 1 | Minimum acceptable qpairs in degraded mode |
| `retry_max_attempts` | 5 | Max retries on allocation failure |
| `retry_backoff_ms` | 100 | Retry backoff base interval (exponential) |
| `enable_degradation` | true | Enable adaptive degradation |
| `max_queue_depth` | 256 | Worker task queue depth limit (0 = no limit) |
| `adaptive_inflight` | true | Adaptive inflight capping based on qpair count |
| `pipeline_drain_budget_us` | 1000 | PipelineIO drain budget (µs); valid range [100, 100000] via `MC_NVME_PIPELINE_DRAIN_BUDGET_US` |

### 2. NofQpairPool (`include/spdk/nof_connection.h`)

Manages N I/O qpairs on a single NVMe controller.

**Round-Robin dispatch**:
```cpp
spdk_nvme_qpair *GetNextQpair() {
    uint32_t idx = round_robin_idx_.fetch_add(1, memory_order_relaxed);
    return qpairs_[idx % qpairs_.size()];
}
```
Each I/O submission rotates through qpairs via an atomic incrementing index,
ensuring uniform load distribution.

**Unified polling**:
```cpp
int32_t PollAll(uint32_t max_completions = 0) {
    for (auto *qp : qpairs_)
        total += spdk_nvme_qpair_process_completions(qp, ...);
    return total;
}
```
Harvests completions from all qpairs in a single call.

**Inflight tracking**: `inflight_count_` atomic counter tracks current in-flight
I/O count for flow control.  Pool capacity is a separate calculation:
```
MaxInflight = qpairs_.size() × max_inflight_per_qpair
```

**TryGrow recovery**: When other connections disconnect and free QIDs, a
degraded pool can gradually recover back to its initial `target_count_`:
```cpp
uint32_t TryGrow(uint32_t target_total) {
    for (uint32_t i = qpairs_.size(); i < target_total; i++) {
        auto *qp = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr_, nullptr, 0);
        if (!qp) break;  // QID pool exhausted; wait for next cycle
        qpairs_.push_back(qp);
    }
    return added;
}
```

### 3. NofConnection (`include/spdk/nof_connection.h`)

One connection = 1 NVMe-oF controller + 1 namespace + 1 QpairPool.

**Connection establishment** (`Connect()`):
1. Probe the target via `spdk_nvme_probe()`
2. Assign a unique `hostnqn` (`nqn.2024-08.mooncake:c<N>`) so each connection
   gets an independent controller
3. Sequential I/O qpair allocation: request `num_io_queues`, take as many as
   available
4. If allocated < `min_io_queues`, release partial allocations and return failure

**Key design — hostnqn uniqueness**:
```cpp
// Global counter, incremented per connection
static atomic<uint32_t> g_hostnqn_counter{0};
// Set unique hostnqn in the probe callback
snprintf(opts->hostnqn, sizeof(opts->hostnqn),
         "nqn.2024-08.mooncake:c%u", pctx->hostnqn_id);
```
Without this, SPDK's default UUID-based hostnqn would cause all connections in
the same process to share one UUID; the target would merge them onto a single
controller and I/O qpair allocation would fail.

### 4. NofSegment (`include/spdk/nof_segment.h`)

A contiguous LBA range bound to one NofConnection.

**Single-request API** (backwards-compatible):
```cpp
int SubmitRead(void *buf, uint64_t lba, uint32_t num_blocks, ...);
int SubmitWrite(void *buf, uint64_t lba, uint32_t num_blocks, ...);
```
The caller-supplied `lba` is relative to the segment base.  Both methods
translate to the absolute device LBA via `start_lba_` and reject requests
that exceed the segment extent.

**Pipeline I/O** (lower-level API for high-throughput bulk transfer — not used by the Mooncake transfer hot path):
```cpp
// Fire-and-forget (default, backward-compatible):
ssize_t PipelineRead(void *buf, uint64_t lba, uint32_t total_blocks);
ssize_t PipelineWrite(const void *buf, uint64_t lba, uint32_t total_blocks);

// Explicit-lifetime (caller-managed ctx_sp):
std::shared_ptr<PipelineCtx> ctx_sp;
ssize_t n = PipelineRead(buf, lba, total_blocks, &ctx_sp);
if (n > 0) {
    // ... use buf ...
    DrainForInflight(ctx_sp);  // synchronous wait, then release
}
```

Pipeline I/O loop:
```
while (blocks remain OR inflight I/O > 0):
    1. Submit phase: while inflight < max_inflight AND blocks remain:
       - Split data into chunk_blocks-sized chunks
       - GetNextQpair() select a qpair
       - spdk_nvme_ns_cmd_read/write() submit
    2. Poll phase: PollAll(0) harvest completions from all qpairs
    3. Error handling: drain remaining I/O within pipeline_drain_budget_us
       (default 1000 µs), then return -1
```

> **Lower-level API**: `PipelineRead`/`PipelineWrite` are chunked-submission
> APIs for callers that need explicit control over how many I/Os are in
> flight across all qpairs simultaneously.  They are **not** used by
> Mooncake's NoF transfer hot path — `TransferSubmitter` drives every
> transfer via `SpdkNofTask` → `submitTask()` → `SpdkWrapper::SubmitRequest()`
> → `SubmitRead`/`SubmitWrite`.  Callers integrating NVMe-oF outside the
> Mooncake transfer flow (e.g. custom bulk-loaders) can use these APIs
> directly; see `nof_segment.h` for the explicit-lifetime contract.

#### Pipeline Ctx Lifecycle

`PipelineCtx` is **heap-allocated** (`std::shared_ptr<PipelineCtx>`) so it
can outlive the caller's stack frame.  Two ownership paths exist:

| Path | When | Where the ctx lives |
|---|---|---|
| **Explicit lifetime** (`caller_ctx != nullptr`) | Caller manages lifetime | Caller owns the shared_ptr until the caller itself releases it |
| **Fire-and-forget** (`caller_ctx == nullptr`, default) | Caller does not manage lifetime | `PipelineCtxRecycler` holds the shared_ptr until the next `PipelineIO` entry's `Drain()` |

The fire-and-forget path keeps the heap object alive past `PipelineIO`'s
return: any late CQE that arrives *before* the next `PipelineIO` call still
sees a live object.  Together with the explicit-lifetime path and
`DrainForInflight`, this gives callers:

1. **Default safety** (recycler-as-fallback) — `PipelineCtxRecycler::Push()`
   runs at the end of every `PipelineIO`; `Drain()` runs at the start of
   every `PipelineIO`.
2. **Explicit lifetime for short-lived `buf`** — `caller_ctx` and
   `DrainForInflight` synchronously wait for in-flight callbacks before
   the caller releases `buf` or `ctx_sp`.

The drain helper (`DrainPipelineInflight`, file-scope in `nof_segment.cpp`)
respects a bounded wall-clock budget (`pipeline_drain_budget_us`,
default 1000 µs).  On budget expiry, the recycler still holds the ctx,
so the object stays alive until the next `PipelineIO` entry's `Drain()`.

The `inflight`/`error` fields use `memory_order_release`/`acquire` to
keep the pair consistently ordered across the callback boundary.  In
single-thread-per-pool operation this documents intent; on contract
violation it prevents a stale error flag from racing ahead of its
`inflight` decrement.

#### DrainForInflight

`DrainForInflight(ctx_sp, budget_us=0)` polls all qpairs until
`ctx_sp->inflight` reaches 0 or `budget_us` elapses.  After it returns,
the caller may safely release both `ctx_sp` and the buffer that was passed
to `PipelineRead/Write`.  `budget_us == 0` selects
`NofConfig::pipeline_drain_budget_us`; an explicit value overrides the
config.  `SpdkWrapper::PipelineDrain` exposes the same logic through the
singleton for callers that don't have a direct `NofSegment*` handle.

#### Layer-1 Tests (`tests/nof_segment_pipeline_test.cpp`)

18 unit tests cover:
- `PipelineCtxRecycler` (5): singleton identity, Push/Drain release,
  multi-Push/single-Drain, empty-Drain idempotency, external
  reference outlives recycler transfer.
- `PipelineIoCb` (4): success decrement, error flag + decrement,
  multi-callback accumulation, error monotonicity latch.
  Driven via `mooncake::detail::InvokePipelineIoCbForTest` test hook
  (compiled in only under `-DMOONCAKE_TEST_PIPELINE_IO=1`).
- `NofConfigEnv` (6): default value, valid parse, low / high clamp,
  invalid (non-numeric), boundary values [100, 100000] not clamped.
- `PipelineCtx` (3): default state, `static_assert` atomic-field type
  enforcement, only-atomic compile-time guard.

### 5. SpdkWrapper (`include/spdk/spdk_wrapper.h`)

Global singleton managing SPDK environment init and all open NoF connections.

**Connection management**:
- `open_segments_`: handle → NofConnection map
- `connect_mutex_`: serializes `spdk_nvme_probe()` calls (SPDK probe is not thread-safe)

**QID pressure sensing** (`QidPressureGauge`):
- Sliding window (16 samples) recording (requested, allocated) outcomes
- Three pressure levels: Green (>75%) / Yellow (50–75%) / Red (<50%)
- New connections adaptively reduce qpair requests based on pressure level
- Thread-safe: `Record()` and `GetRecommended()` are protected by an internal mutex

**Heartbeat probe** (`ProbeNofSegment`):
- Uses an independent temporary connection with `config_probe_` (num_io_queues=1)
- Does not share qpairs with the I/O path, avoiding completion stealing
- Connection auto-destructs on return → all QIDs reclaimed
- Probe context is recycled only after the caller has copied results (no use-after-free)

### 6. SpdkNofWorkerPool (`include/transfer_task.h`)

Manages multiple worker threads, each independently executing SPDK I/O.

**Task dispatch — seg-to-worker affinity binding**:
```cpp
// All tasks for the same seg_handle are pinned to the same worker thread
if (seg_to_worker_.find(seg) != seg_to_worker_.end())
    worker_idx = seg_to_worker_[seg];          // Existing binding: reuse
else
    worker_idx = (seg_num++ % worker_count_);  // New binding: round-robin
```
SPDK qpairs are not thread-safe; a connection's qpair pool must be accessed by a
single worker exclusively.

**Per-seg QOS** (`SpdkNofQos`):
- One FIFO task list per operation (read / write)
- `inflight_blocks_limit` caps the number of in-flight blocks for that segment
- `blocks_per_chunk` controls the maximum chunk size per submission
- Adaptive inflight: automatically shrinks the cap when the qpair count degrades

**Flow-control backpressure**:
```cpp
if (task_queue_[worker_idx].size() >= max_queue_depth_)
    queue_not_full_cv_[worker_idx].wait(lock, [&] {
        return shutdown_.load() ||
               task_queue_[worker_idx].size() < max_queue_depth_;
    });
```
Prevents unbounded request accumulation under degraded conditions.  The
predicate checks `shutdown_` so that teardown does not deadlock on a full queue.

**Periodic rebalance** (every 30 seconds):
```cpp
if (pool.Size() < pool.GetTargetCount()) {
    uint32_t added = pool.TryGrow(target);
    if (added > 0) nof_qos->UpdateInflightLimit(pool.Size(), ...);
}
```

## Multi-Qpair Concurrent I/O Flow

### Single-Client I/O Path

```
Client.put(key, data)
  → TransferSubmitter::submitSpdkNofOperation()
    → Lookup nof_handle_cache_ (endpoint → seg_handle)   [double-checked locking]
    → SpdkNofWorkerPool::submitTask()
      → Bind seg to worker
      → Enqueue + notify worker
        → workerThread():
          1. Dequeue task from task_queue_
          2. Bind seg_to_qos (per-seg QOS)
          3. Split into chunk_blocks-sized sub-tasks
          4. GetNextQpair() → round-robin select qpair
          5. spdk_nvme_ns_cmd_read/write() submit
          6. PollAll() harvest completions
          7. SpdkNofTaskCompletion() → set_completed()
```

### Multi-Qpair Parallelism

```
Thread 1 ────┐
Thread 2 ────┤
Thread 3 ────┼──→ SpdkNofWorkerPool (4 workers)
Thread 4 ────┘         │
                       ├── Worker 0 → seg_A → NofQpairPool[qp0, qp1, qp2, qp3]
                       │                     Round-Robin: qp0 → qp1 → qp2 → qp3
                       │
                       ├── Worker 1 → (idle)
                       ├── Worker 2 → (idle)
                       └── Worker 3 → (idle)

Multi-qpair submit/harvest loop (SubmitRead/SubmitWrite via SpdkNofTask — the actual Mooncake hot path):
  [qp0 ████░░░░] [qp1 ████░░░░] [qp2 ████░░░░] [qp3 ████░░░░]
      ↑ submit       ↑ submit       ↑ submit       ↑ submit
      ↓ complete     ↓ complete     ↓ complete     ↓ complete
  PollAll() harvests completions from all qpairs at once

> Note: the diagram above describes the submit/harvest pattern used by
> `SpdkNofWorkerPool` via `SpdkWrapper::SubmitRequest()` →
> `NofSegment::SubmitRead/SubmitWrite`.  This is **not** the same loop as
> `NofSegment::PipelineIO`; the latter is the lower-level API described in
> the `Pipeline I/O` section above.
```

## QID Management

### QID Consumption Model

```
One connection → QIDs on the same target = 1 (admin qpair) + num_io_queues (I/O qpairs)

Default (full capacity):
  Admin Queue Pair: 1
  I/O Queue Pairs:  16  (MC_NVME_NUM_IO_QUEUES)
  Total:            17  QIDs
```

Each Client (TransferSubmitter) holds an independent `nof_handle_cache_`.
Multiple put/get calls from the same client reuse the same connection and do not
consume additional QIDs.

### Connection Retry with Backoff

```
Attempt 1: request full num_io_queues
  ├─ Success → connection established, Record(requested, allocated)
  └─ Failure (qpair_alloc_fail):
      ├─ QidPressureGauge::GetRecommended() evaluate global pressure
      ├─ Gradual degradation: target = min(gauge rec, target/2)
      ├─ Backoff wait: retry_backoff_ms × 2^attempt
      └─ Retry Connect() (up to retry_max_attempts times)
```

### Degradation & Recovery

**Degradation**: when the QID pool is insufficient, take as many qpairs as
available (≥ `min_io_queues`):
```
Requested 16 qpairs → allocated 4 → degraded state (4/16)
qpair_pool_ internally records target_count_=16
Log: "QID degraded: allocated 4/16 qpairs"
```

**Recovery**: worker thread calls TryGrow every 30 seconds:
```
Check pool.Size() < pool.GetTargetCount()
  → TryGrow(target_count)
    → Success → update inflight limit
```

## Configuration Tuning

```bash
# Recommended production config — 8 qpairs, low per-qpair inflight
MC_NVME_NUM_IO_QUEUES=8
MC_NVME_MAX_INFLIGHT_PER_QPAIR=4       # total inflight ≤ 8×4 = 32
MC_NVME_CHUNK_BLOCKS=512               # 2 MiB chunks @ 4 KiB blocks
```

| Scenario | Recommended Config |
|----------|-------------------|
| Stable production | `NUM_IO_QUEUES=8`, `MAX_INFLIGHT_PER_QPAIR=4` |
| Target with small nn cap (≤ 4) | `NUM_IO_QUEUES=4`, `MAX_INFLIGHT_PER_QPAIR=4` |
| Small-block I/O | `MAX_INFLIGHT_PER_QPAIR=8`, `CHUNK_BLOCKS=256` |
| Large-block I/O (32 MiB+) | `MAX_INFLIGHT_PER_QPAIR=2`, `CHUNK_BLOCKS=1024` |

**Key formula**: `total_inflight = num_io_queues × max_inflight_per_qpair`
Keep `max_inflight_per_qpair` low (≤ 8) — high per-qpair depth causes target-side
completion congestion.  Multiple qpairs with shallow depth outperform few qpairs
with deep depth.

## Performance Baselines

The following data was collected with an internal `nof_worker_pool_bench` tool
(not yet open-sourced).  Test environment: **FORINN HWE62P447T6L00LN**
NVMe-oF target, single SSD, random read (`--rw randread`), `--io_size 8388608
--iodepth 64 --duration_sec 20 --warmup_sec 5`, 1 job, 1 endpoint.

### Test A: Multi-qpair scaling (corrected — fixed per-qpair inflight)

`MC_NVME_MAX_INFLIGHT_PER_QPAIR=4` is held constant.  Only
`MC_NVME_NUM_IO_QUEUES` varies, so qpair count is the sole independent variable.

```bash
for qp in 1 2 4 8 16 32 64; do
    MC_NVME_NUM_IO_QUEUES=$qp \
    MC_NVME_MAX_INFLIGHT_PER_QPAIR=4 \
    ./nof_worker_pool_bench \
        --endpoints "traddr:10.10.10.100 trsvcid:4420 \
                     subnqn:nqn.ForinnBase5000.lsjs:nvme.1 \
                     trtype:RDMA adrfam:IPv4 ns:1" \
        --rw randread --io_size 8388608 --iodepth 64 \
        --duration_sec 20 --warmup_sec 5
    sleep 5
done
```

| Req qpairs | Actual qpairs¹ | Per-qpair inflight | Total inflight | BW | IOPS | Scaling vs qp=1 | Efficiency |
|-----------|---------------|--------------------|----------------|-----|------|-----------------|------------|
| 1 | 1 | 4 | 4 | 616 MiB/s | 77 | 1.00× | — |
| 2 | 2 | 4 | 8 | 1.15 GiB/s | 147 | 1.91× | 96% |
| 4 | 4 | 4 | 16 | 2.11 GiB/s | 270 | 3.50× | 88% |
| 8 | 8 | 4 | 32 | 3.25 GiB/s | 416 | 5.40× | 68% |
| 16 | 8¹ | 4 | 32¹ | 3.28 GiB/s | 420 | 5.45× | — |
| 32 | 8¹ | 4 | 32¹ | 3.26 GiB/s | 418 | 5.43× | — |
| 64 | 8¹ | 4 | 32¹ | 3.26 GiB/s | 418 | 5.43× | — |

> ¹ At qp ≥ 16 the target's per-controller I/O Queue cap (`nn`) limits actual
> allocation to ~8 qpairs.  Throughput flatlines at ~3.26 GiB/s from qp=8
> through qp=64, confirming the target-side ceiling is the sole bottleneck — not
> client-side qpair management.

**Conclusions from Test A**:

1. **Multi-qpair scaling is nearly linear up to the target's nn cap.**
   qp=1→2 achieves 96% linear efficiency; qp=2→4 achieves 92%.
2. **The target's per-controller I/O Queue cap is the scaling ceiling.**
   Throughput flatlines at ~3.26 GiB/s from qp=8 through qp=64.  The 8→64
   range shows zero additional throughput, confirming the target-side ceiling
   is the sole bottleneck — not client-side qpair management.
3. **Low per-qpair inflight (=4) works correctly.**  Total inflight = 32 at the
   optimal point — well below the 64 that caused overload in earlier tests.

### Test B: Per-qpair inflight sweep (fixed qpair count) — historical reference

The original benchmark (pre-review) varied `MAX_INFLIGHT_PER_QPAIR` inversely
with qpair count to hold total inflight constant.  This conflated two variables
and overstated the qpair scaling efficiency.  Its primary valid finding was that
**reducing per-qpair inflight from 64 → 4 dramatically improves throughput** by
avoiding target-side completion congestion:

| Total inflight | BW | Notes |
|---------------|-----|-------|
| 64 | 1.87 GiB/s | Overloaded target |
| 32 | 3.21 GiB/s | Reduced congestion |
| 16 | 3.28 GiB/s | Optimal — matches Test A ceiling |

### Recommended Config

```bash
MC_NVME_NUM_IO_QUEUES=8              # Match target nn cap
MC_NVME_MAX_INFLIGHT_PER_QPAIR=4     # Low depth per qpair
# Total inflight = 8 × 4 = 32        # Well below overload threshold
```

## FAQ

### 1. `No free I/O queue IDs`

**Cause**: Target per-controller I/O Queue quota (`nn`) exhausted.  The target
caps the number of I/O qpairs per controller at a fixed limit (varies by target
firmware; observed values: 4 to 8 on FORINN).

**Resolution**:
- Check for lingering connections (wait for keep-alive timeout or restart target)
- Reduce `MC_NVME_NUM_IO_QUEUES` to match the target's `nn` cap
- Use multiple subsystems with different NQNs

### 2. `poll completion error: ret -6` (ENXIO)

**Cause**: Qpair entered a disconnected / error state.
- In QP_SWEEP: previous subprocess QIDs not yet reclaimed by the target
- In benchmark: excessive inflight causing target-side RDMA disconnect

**Resolution**:
- Increase sweep gap: `QP_SWEEP_GAP_SEC=60`
- Reduce `MC_NVME_MAX_INFLIGHT_PER_QPAIR`

### 3. `RDMA connect error -99` + `Ctrlr is in error state`

**Cause**: The FORINN target NVMe subsystem has crashed; the target must be
restarted.

**Prevention**: Keep total inflight ≤ 64.

### 4. Multiple clients cannot connect to the same target simultaneously

**Cause**: The FORINN target merges connections with different hostnqns under
the same NQN onto a single controller.  The per-controller I/O Queue quota
(`nn`) is consumed by the first client.

**Resolution**:
- Use single-client mode + multi-threaded concurrency
- Configure multiple subsystems (different NQNs)
- Use different physical targets

## File Index

| File | Description |
|------|-------------|
| `include/spdk/nof_config.h` | Config struct + env var parsing |
| `include/spdk/nof_connection.h` | NofQpairPool + NofConnection declarations |
| `include/spdk/nof_segment.h` | NofSegment + PipelineCtx declarations |
| `include/spdk/spdk_wrapper.h` | SpdkWrapper + QidPressureGauge declarations |
| `src/spdk/nof_connection.cpp` | Connection factory + QpairPool implementation |
| `src/spdk/nof_segment.cpp` | SubmitRead/Write + PipelineIO + PollCompletion |
| `src/spdk/spdk_wrapper.cpp` | Singleton management + adaptive connect + heartbeat probe |
| `include/transfer_task.h` | SpdkNofWorkerPool + SpdkNofQos |
| `src/transfer_task.cpp` | Worker threads + flow control + rebalance |

## Changelog

- **2026-07-31** — Initial multi-qpair implementation (NofQpairPool, NofConnection, NofSegment, PipelineIO).  Note: `PipelineIO` is a lower-level API; the Mooncake transfer hot path goes through `SpdkNofTask` → `SpdkWrapper::SubmitRequest` → `SubmitRead`/`SubmitWrite`.
- **2026-08-03** — Sequential allocation + backoff retry + TryGrow rebalance + QidPressureGauge + flow-control backpressure
- **2026-08-20** — Pipeline I/O lifetime and error-path safety
  - `PipelineCtx` is now heap-allocated (`shared_ptr`); callers may opt into
    explicit lifetime control via the new `caller_ctx` out-param on
    `PipelineRead/Write`
  - `DrainForInflight` + `SpdkWrapper::PipelineDrain` synchronously wait
    for in-flight callbacks before the caller releases the buffer
  - `pipeline_drain_budget_us` env-tunable drain budget (default 1000 µs)
    bounds the wait after a qpair transport error
  - `PollAll(0)` negative return is now handled (drain + return -1)
    instead of spinning without checking
  - 18 Layer-1 unit tests in `tests/nof_segment_pipeline_test.cpp`

---
