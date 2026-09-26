# Offline master RPC trace replay

This tool loads a timestamped RPC-intent trace, calls a dedicated real Mooncake
master, and records latency, throughput and resource usage. Trace generation is
external: neither a serving framework nor generated workload traces are needed
in this repository. Stop the producer before running a measurement.

## Build and verify

```bash
cmake -S . -B build -DBUILD_BENCHMARK=ON -DUSE_CUDA=OFF -DWITH_STORE_RUST=OFF
cmake --build build --target master_rpc_trace_bench mooncake_master -j "$(nproc)"
cmake -S mooncake-store/benchmarks/master_rpc_trace/tests -B build-trace-tests
cmake --build build-trace-tests -j "$(nproc)"
ctest --test-dir build-trace-tests --output-on-failure
python3 mooncake-store/benchmarks/master_rpc_trace/tests/test_rpc_smoke.py \
  --master build/mooncake-store/src/mooncake_master \
  --replayer build/mooncake-store/benchmarks/master_rpc_trace_bench
```

The standalone parser/scheduler tests require C++20, JsonCpp and GoogleTest.
The Python monitor uses only the standard library; it requires Linux and
`taskset`. Lower build parallelism if compiler memory exceeds available RAM.

## Run and monitor

Use a fresh, dedicated master: segments have fake addresses, and no KV payload
is allocated or transferred. They cannot serve requests from real applications.
The launcher starts a loopback-only master, waits for readiness, runs the
replayer, and stops both child processes on completion or error:

```bash
python3 mooncake-store/benchmarks/master_rpc_trace/run_benchmark.py \
  --trace /outside/repo/master-rpc.jsonl \
  --master build/mooncake-store/src/mooncake_master \
  --replayer build/mooncake-store/benchmarks/master_rpc_trace_bench \
  --output-dir /outside/repo/results/run-001 \
  --master-cpus 0-3 --replay-cpus 4-7 \
  --rpc-threads 4 --workers 16 --speed 1
```

Choose CPU sets using `lscpu -e=CPU,CORE,SOCKET,NODE`; keep physical cores and
SMT siblings together. The launcher rejects overlapping logical CPU sets but
cannot eliminate shared memory, NUMA or host contention. Output directories
must be new. The launcher clears `MOONCAKE_CONFIG_PATH`, sets client I/O threads
to 2, enables master metrics and uses zero default KV lease TTL for controlled
remove tests. These choices are recorded in the manifest.

For manual master management, invoke the C++ binary with `--trace`,
`--master_server`, `--workers`, `--speed`, `--output` and `--samples`.
`--validate_only` validates the entire file without contacting the master.

## Trace contract: version 2

The first nonblank JSONL row is a header:

```json
{"type":"master_rpc_trace","version":2,"time_unit":"us","metadata":{"initial_state":"empty","seed":42}}
```

Every event has `id`, `client_id`, `op`, `phase` and `timestamp_us`. Phases are
ordered `setup`, `workload`, `teardown`, with a completion barrier between them.
Timestamps are nonnegative integer microseconds relative to **that phase's**
origin, sorted within each phase. These differ from AutoBench request times in
milliseconds. Parsing and connection setup precede replay; mount time has its
own measured phase and does not consume the workload's arrival-time budget.

| Operation | Additional fields |
| --- | --- |
| `ReMountSegment` | `segments: []`; one initial handshake per client in setup. |
| `MountSegment` | `segment_id`, positive `size_bytes`; setup only. |
| `UnmountSegment` | `segment_id`; teardown only, same owner as mount. |
| `BatchExistKey`, `BatchGetReplicaList`, `BatchRemove` | Nonempty ordered `keys`. |
| `BatchPutStart` | `keys`, either `value_sizes` or `value_slices`, optional `replica_num` (default 1). |
| `BatchPutEnd`, `BatchPutRevoke` | Same client and ordered keys as Start, plus `put_start` referencing its ID. |

Register request-only clients without mounting memory. Mount capacity belongs
to independently configured storage clients: adding serving clients must not
silently enlarge storage. A segment ID is trace-local; the replayer maps it to a
fresh real UUID. Every mounted segment requires exactly one explicit unmount.
The teardown barrier drains all workload calls before storage is removed.
Mid-workload topology changes and nonempty remount recovery are unsupported.

All key operations belong to workload. Each Start requires exactly one End or
Revoke. `put_start` implies a completion dependency; unsuccessful Start keys are
skipped at End/Revoke. If all keys are skipped, no finalization RPC is issued.
`value_sizes` gives one positive byte length per key, with one slice per value.
For multi-slice objects, `value_slices` instead gives one nonempty array of
positive slice lengths per key: `[[4194288, 4194288, 32], [8192]]` describes two
objects, with three slices and one slice respectively. The replayer preserves
these boundaries; the producer must follow its Mooncake client's slice limits.
The two fields are mutually exclusive. Remove respects leases (`force=false`).

`depends_on` lists earlier event IDs. Dependencies wait for completion; they do
not imply success. Producers must preserve write/read/remove ordering for shared
keys, including reads that must precede a later mutation. Unrelated ready calls
remain concurrent. Unknown fields, invalid lifecycles and forward references
fail validation before any connection is opened.

Record producer revision, model, physical key layout, batch sizes, object sizes,
cache capacities, routing, topology, random seed, initial state and timing model
in `metadata`. A logical GPU count is not a calibration of real serving load.
The replayer does not infer requests, prefix relationships or cache policy.
Pinning, group IDs, placement overrides, disk replicas, transfers and HA are not
modeled. Do not silently discard these semantics in a producer.

## Arrival control and results

`--speed=1` preserves intervals; larger values compress them and must be reported
as stress-test transformations. `--workers` caps concurrent API calls, independent
of logical client count. Overdue events stay queued; planned arrival times do
not move to conceal overload. Dispatch lag includes worker and dependency waits.
A fixed trace does not model serving feedback caused by a slow or failed master.

The launcher saves:

- `manifest.json`: commands, parameters, environment and input/binary SHA-256.
- `replay.json`: per-operation and per-phase summaries, key outcomes, heartbeat
  counts, P50/P95/P99/max call latency and dispatch lag.
- `samples.jsonl`: planned/start/finish times, phase origins, issued calls,
  original key counts, outcomes and errors. Written after timed replay.
- `process.jsonl`: monotonic time, CPU seconds, RSS and thread counts for both
  processes; `metrics.jsonl` and before/after `.prom` snapshots contain master
  Prometheus metrics. Sampling continues during setup and teardown.
- `result.json`: workload-only CPU/RSS and traffic summaries, one-second offered,
  sent and completed call counts, issued keys and peak calls in flight.
  `offered_calls_per_second` uses the planned arrival span, while completed
  throughput includes draining the backlog. For an all-at-once trace, the
  offered average is null; per-second buckets still show the burst.
- Child process logs.

For capacity-pressure experiments, grow the recorded KV working set beyond the
trace's fixed mounted capacity and let the real master select eviction victims.
`--eviction-high-watermark-ratio` and `--eviction-ratio` override the master's
corresponding settings; omitted values retain master defaults. Add
`--require-eviction` to fail a run unless successful eviction and freed bytes
are observed during workload. `result.json` includes `workload_evictions` with
sampled counter deltas and the intervals in which eviction occurred, including
allocation failures. `pids.json` exposes the owned processes for an external
profiler; use a separate profiling run when measuring profiler overhead matters.

Changing capacity while keeping a trace's requests fixed measures master
behavior under those offered intents. Eviction can make recorded reads miss,
so their hit counts need not reproduce the generating simulator's state.
Report misses, not-ready results and allocation errors explicitly. This is not
a closed-loop serving simulation, and injecting client `BatchRemove` calls is
not a substitute for exercising the master's background eviction path.

CPU utilization is expressed in cores (1 means one fully occupied core), derived
from Linux process CPU time. Very short phases can have too few samples to estimate
CPU use. Sampled RSS is a sampled peak, not an exact high-water mark. Metrics
scraping adds overhead; hold its interval fixed between comparisons.

`master_total_capacity_bytes` and `master_allocated_bytes` describe logical Store
capacity and allocation, which differ from the master's actual process RSS.
Use the metrics to check capacity during workload and cleanup afterward.
Throughput includes phase-leading idle time and final drain. Client-call latency
includes API/RPC work; it is not server-only processing time. Calls count
MasterClient API invocations; retries can create extra wire traffic. Compare
server metrics as well. Miss, not-ready and already-exists outcomes have separate
counts and are not successful-hit counts.

Heartbeat calls are separate maintenance traffic. Each client is pinged after
registration; a failed heartbeat invalidates the run instead of silently
remounting. `--heartbeat_workers` (default 16) partitions clients across a bounded
set of workers independently of workload calls. `--heartbeat_interval_ms`
(default 1000) is the pause between sweeps within each partition. Slow sweeps
still lengthen the effective client heartbeat period; inspect heartbeats and
master liveness metrics when increasing client count or pressure.

High replay lag with low master utilization indicates a load-generator limit.
Sweep workers or arrival compression and check offered versus achieved rates
before attributing a throughput ceiling to master locks. This benchmark reports
whole-master load; lock attribution additionally requires profiling.

## Legacy version 1

The handwritten `example.jsonl` and v1 contract remain accepted. V1 has a single
workload phase and implicitly registers one fake segment per client before timing
(default 64 MiB, `--segment_size`). Cleanup occurs after timing. Use v2 for explicit
and independently sized storage lifecycle. V1's optional `--prefill_trace` runs
before workload and requires all keys to succeed; it cannot be combined with v2.
