# Mooncake Store Benchmarks

This directory contains benchmark tools for Mooncake Store internals.

## Allocation Strategy Benchmark

`allocation_strategy_bench` evaluates Store allocation behavior across segment
counts, replica counts, allocation strategies, and workload patterns.

Build the benchmark from an existing CMake build directory:

```bash
cmake --build build --target allocation_strategy_bench -j$(nproc)
```

### Size-Class Churn Fragmentation Benchmark

The `size_class_churn` workload measures fragmentation under mixed-size
KVCache-like allocation pressure. It pre-fills the simulated cluster when
`--prefill_pct` is set, then repeatedly allocates objects from weighted size
classes. On allocation failure it randomly evicts a fraction of live objects and
retries.

When prefill is enabled, the prefill attempt cap is auto-derived from target
utilization, total cluster capacity, weighted average object size, and replica
count, with a 5000-attempt minimum for small cases.

This is an allocation-strategy-layer benchmark. It complements the existing
`dsa` workload by adding explicit fragmentation sampling and configurable
weighted size-class patterns. It is not a replacement for `allocator_bench`,
which remains the low-level `OffsetAllocator` microbenchmark.

Run a small local validation:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn \
  --segment_capacity=1024 \
  --num_allocations=10000 \
  --prefill_pct=70
```

Run a larger baseline:

```bash
./build/mooncake-store/benchmarks/allocation_strategy_bench \
  --workload=size_class_churn \
  --segment_capacity=1024 \
  --num_allocations=100000 \
  --prefill_pct=80
```

Supported size-class patterns:

- `kv_mixed`: 4KB at 70%, 256KB at 20%, and 3.12MB at 10%.
- `dsa_pair`: 3.12MB KV pages at 50% and 643KB indexer entries at 50%.
- `all`: run both patterns.

Key output columns:

- `Throughput`, `Avg(ns)`, `P50(ns)`, `P90(ns)`, and `P99(ns)` measure
  allocation performance.
- `Frag_avg`, `Frag_p50`, `Frag_p90`, and `Frag_p99` summarize sampled
  fragmentation ratios.
- `LargestFreeMB` shows the final largest contiguous free region.
- `Evictions` counts fail-triggered eviction rounds during measurement.
- `Full/Partial/Fail/Total` reports allocation outcomes. Only results with
  `result->size() == replica_num` count as full success; shorter replica
  results are counted as partial allocations.

Fragmentation is computed per `OffsetBufferAllocator` and then averaged by free
space:

```text
1 - largest_free_region / total_free_space
```

The weighted average avoids treating free space in different Store segments as
one mergeable region. `LargestFreeMB` still reports the final largest contiguous
free region across all segments.

The benchmark also prints a one-line `Prefill summary`, `Fragmentation summary`,
and `Size-class breakdown` after each result row, so reviewers can read the
actual prefill utilization, fragmentation, and per-size-class latency numbers
without manually deriving them from the table.

## Master RPC Trace Replay Benchmark

This tool loads a timestamped RPC-intent trace, calls a dedicated real Mooncake
master, and records latency, throughput and resource usage. Trace generation is
external: neither a serving framework nor generated workload traces are needed
in this repository. Stop the producer before running a measurement.

The C++ replayer is contained in `master_rpc_trace_bench.cpp`;
`run_master_rpc_trace.py` launches the processes and collects metrics.

### Build and verify

```bash
cmake -S . -B build -DBUILD_BENCHMARK=ON -DBUILD_UNIT_TESTS=OFF \
  -DUSE_CUDA=OFF -DWITH_STORE_RUST=OFF -DSTORE_USE_JEMALLOC=ON
cmake --build build --target master_rpc_trace_bench mooncake_master -j "$(nproc)"
```

The Python monitor uses only the standard library; it requires Linux and
`taskset`. Install the jemalloc development package (for example,
`libjemalloc-dev` on Ubuntu) before configuring the build. The launcher checks
that the master has loaded the shared jemalloc library before replay and records
its path in `result.json` under `master_allocator`. A master without this library
is rejected. Lower build parallelism if compiler memory exceeds available RAM.

To check the RPC path manually, run the launcher below with
`--trace mooncake-store/benchmarks/master_rpc_trace_example.jsonl`. After cleanup,
`result.json` should report `success: true`.
The example registers two request clients and one storage client, mounts a
64 MiB segment, and unmounts it after the reads complete.
`replay.json` should show 9 recorded calls, including one Exist miss and two
successful Get keys. Background Ping calls are reported separately. This example
does not validate eviction.

### Run and monitor

Use a fresh, dedicated master: segments have fake addresses, and no KV payload
is allocated or transferred. They cannot serve requests from real applications.
The launcher starts a loopback-only master, waits for readiness, runs the
replayer, and stops both child processes on completion or error:

```bash
python3 mooncake-store/benchmarks/run_master_rpc_trace.py \
  --trace /outside/repo/master-rpc.jsonl \
  --master build/mooncake-store/src/mooncake_master \
  --replayer build/mooncake-store/benchmarks/master_rpc_trace_bench \
  --output-dir /outside/repo/results/run-001 \
  --master-cpus 0-3 --replay-cpus 4-7 \
  --rpc-threads 4
```

Choose CPU sets using `lscpu -e=CPU,CORE,SOCKET,NODE`; keep physical cores and
SMT siblings together. The launcher rejects overlapping logical CPU sets but
cannot eliminate shared memory, NUMA or host contention. Output directories
must be new. The launcher clears `MOONCAKE_CONFIG_PATH`, sets client I/O threads
to 2 and enables master metrics. The master retains its default KV read lease
TTL.

For manual master management, invoke the C++ binary with `--trace`,
`--master_server`, `--output`, `--samples` and `--heartbeats`.
Each registered client has an independent heartbeat thread. By default, the
replayer preconnects eight TCP connections per client, without using the SDK's
process-wide shared pool. `--connections N` changes this default;
`--shared-connections` instead creates exactly N connections for the whole
replayer, useful for connection-count controls. Requests select connections
round-robin and can be pipelined: waiting for a response does not reserve a
connection. This benchmark transport uses the normal master wire protocol and
timeout configuration; transport failures are reported without retrying mutations.
The replayer loads the trace once before issuing RPCs and checks that its header
uses a supported format. There is no separate validation pass; the producer is
responsible for satisfying the trace contract below.

### Trace contract

The first nonblank JSONL row is a header:

```json
{"type":"master_rpc_trace","time_unit":"us","metadata":{"initial_state":"empty","seed":42}}
```

Every event has `id`, `client_id`, `op` and `timestamp_us`. Timestamps are
nonnegative integer microseconds relative to a single replay start, sorted
throughout the file. These differ from AutoBench request times in milliseconds.
Parsing and connection setup precede replay. Registration, mount, unmount and
key operations share the same timeline and may be interleaved.

| Operation | Additional fields |
| --- | --- |
| `ReMountSegment` | `segments: []`; one initial handshake per client. Optional positive `workers` and `connections` override that client's defaults. Shared-connection mode ignores per-client connection counts. |
| `MountSegment` | `segment_id`, positive `size_bytes`. |
| `UnmountSegment` | `segment_id`; same owner as mount. |
| `BatchExistKey`, `BatchGetReplicaList`, `BatchRemove` | Nonempty ordered `keys`. |
| `BatchPutStart` | `keys`, either `value_sizes` or `value_slices`, optional `replica_num` (default 1). |
| `BatchPutEnd`, `BatchPutRevoke` | Same client and ordered keys as Start, plus `put_start` referencing its ID. |

Register request-only clients without mounting memory. Mount capacity belongs
to independently configured storage clients: adding serving clients must not
silently enlarge storage. A segment ID is trace-local; the replayer maps it to a
fresh real UUID. Use a new segment ID for each mount lifetime. Clients may
register and segments may mount or unmount at any recorded time. Each client
operation implicitly depends on its registration, and each unmount depends on
its corresponding mount. Other ordering must be recorded in `depends_on`:
for example, a put that requires newly mounted capacity should depend on that
mount, and an unmount that must follow particular reads should depend on them.
Different clients can execute concurrently. Unmount does not implicitly drain
other clients; record the required completion dependencies.
The replayer sends only recorded lifecycle calls, including unmounts. A trace
may end with segments still mounted; the launcher then stops its dedicated
master. Nonempty remount recovery is unsupported.

Do not include `Ping` events or dependencies on them. The replayer rejects
recorded Ping events and owns heartbeat timing, as described below.

Each Start requires exactly one End or Revoke. `put_start` implies a completion
dependency; unsuccessful Start keys are skipped at End/Revoke. If all keys are
skipped, no finalization RPC is issued.
`value_sizes` gives one positive byte length per key, with one slice per value.
For multi-slice objects, `value_slices` instead gives one nonempty array of
positive slice lengths per key: `[[4194288, 4194288, 32], [8192]]` describes two
objects, with three slices and one slice respectively. As in `MasterClient`,
slice lengths are summed per object before sending the master RPC. The producer
must follow its Mooncake client's slice limits.
The two fields are mutually exclusive. Remove respects leases (`force=false`).

`depends_on` lists earlier event IDs. Dependencies wait for completion; they do
not imply success. Producers must preserve write/read/remove ordering for shared
keys, including reads that must precede a later mutation. Producers must also
preserve serial call-flow dependencies, including completion of a blocking put
before the next batch in that flow.

For parallel RealClient replay, give **every event** an `operation_id`. Events
sharing an operation ID must belong to one client and execute in file order on
one worker. Put Start and End/Revoke must share an operation ID. Other calls can
each be their own operation. Dependencies on an event in another operation wait
for that entire operation, including its transfer hold, to finish. Cycles between
operations are rejected before workers start. Do not create dependencies that
require another operation to interleave inside an unfinished operation.

`delay_us` waits after the preceding RPC in an operation, in addition to observing
the event's planned timestamp. For example, record a put's simulated transfer
duration on its End event. `hold_us` keeps the worker occupied after a call, such
as the simulated transfer following GetReplicaList. Both default to zero and are
synthetic delays; no KV payload moves. Record how these durations were obtained,
since a fixed trace cannot adapt transfer sizes to different replay outcomes.

`--workers-per-client N` sets the default number of whole-operation workers;
registration's `workers` can override it. One TP8 inference instance per host
can be represented by one client with eight workers and eight connections, while
its eight dummy ranks retain separate key namespaces, batches and flow edges.
Separate storage clients can use one worker and one connection each. Heartbeats
run independently of these workers.

Legacy traces without operation IDs retain one worker per client and file-order
serialization. They require a worker count of one and cannot specify delays or
holds. Mixing the two event formats is rejected.
Unknown fields are ignored. Event fields and lifecycle
consistency are not prevalidated; JSON decoding or dependency lookup can still
fail while loading a malformed trace.

Record producer revision, model, physical key layout, batch sizes, object sizes,
cache capacities, routing, topology, random seed, initial state and timing model
in `metadata`. A logical GPU count is not a calibration of real serving load.
The replayer does not infer requests, prefix relationships or cache policy.
Pinning, replica group IDs, placement overrides, disk replicas, actual transfers and HA are not
modeled. Do not silently discard these semantics in a producer.

### Arrival control and results

Timestamps are replayed unchanged, relative to the replay start. Each client has
its own queue of dependency-ready operations, ordered by planned start time.
Each worker executes one complete operation at a time. A slow call occupies its
worker and delays dependent operations; independent operations can use the
client's other workers. Background Ping may overlap business calls. Overdue events stay queued;
planned arrival times do not move to conceal overload. Dispatch lag includes
waiting for earlier calls from the same client, dependencies and scheduling.
A fixed trace does not model serving feedback caused by a slow or failed master.

The launcher saves all outputs under `--output-dir`. `result.json` keeps the
run summary and references the detail files using paths relative to that
directory:

```text
run-001/
  result.json
  samples.jsonl
  heartbeats.jsonl
  connections.json
  traffic.json
  evictions.json
  replay.json
  process.jsonl
  metrics.jsonl
  metrics-before.prom
  master.log
  replayer.log
```

`connections.json` records actual connection counts and each connection's total
calls (including readiness probes), peak concurrent calls and remaining calls.
For operation traces, each sample also records `operation_id`, the client-local
worker index and `operation_finish_us`. RPC latency excludes synthetic transfer
waits; operation completion includes them.

For example, `result.json` contains these references alongside its summaries:

```json
{
  "success": true,
  "samples": "samples.jsonl",
  "heartbeats": "heartbeats.jsonl",
  "traffic": "traffic.json",
  "evictions": "evictions.json"
}
```

The files contain:

- `replay.json`: per-operation summaries, key outcomes,
  P50/P95/P99/max call latency and dispatch lag for recorded operations.
  `heartbeats` separately reports Ping calls, failures, client count, response
  latency percentiles and the interval between a response and the next Ping.
- `samples.jsonl`: planned/start/finish times, issued calls,
  original key counts, outcomes and errors. Written after timed replay.
- `heartbeats.jsonl`: generated Ping client IDs, start/finish times relative to
  the same replay origin, and errors. Rows are grouped by client, and are written
  after the heartbeat threads stop. An in-flight Ping can finish after the last
  trace event; it is included here and in the heartbeat summary.
- `process.jsonl`: monotonic time, CPU seconds, RSS and thread counts for the
  master; `metrics.jsonl` and `metrics-before.prom` contain master Prometheus
  metrics. Sampling covers the full replay.
- `result.json`: success status, `master_process`, `store_sampled_peak`, the
  replay time window, `heartbeat_summary`, and references to `samples.jsonl`,
  `heartbeats.jsonl`, `traffic.json` and `evictions.json`.
  `operations` contains the per-operation counts and latency
  summaries, including MountSegment and UnmountSegment.
- `traffic.json`: traffic totals, one-second offered, sent and completed call
  counts, issued keys and peak calls in flight. All buckets use the replay's
  common time origin.
  `offered_calls_per_second` uses the scheduled arrival span, while completed
  throughput includes draining the backlog.
  For an all-at-once trace, the offered average is null; per-second buckets still
  show the burst. Traffic covers the full replay, including registration,
  mount and unmount. Generated heartbeats are excluded from these trace traffic
  statistics; their load is included in measured master CPU and server metrics.
- `evictions.json`: sampled counter deltas and intervals with eviction activity,
  including allocation failures and incomplete-write discard/release counters.
- Child process logs.

For capacity-pressure experiments, grow the recorded KV working set beyond the
trace's fixed mounted capacity and let the real master select eviction victims.
The launcher uses the master's default eviction and incomplete-write timeout
settings. `result.json` links to `evictions.json` for the sampled observations.
Eviction observations are reported separately from replay success; use them to
evaluate the experiment's capacity-pressure requirements.
Use a separate profiling run when measuring profiler overhead matters.

Queueing can stretch the actual interval between PutStart and PutEnd beyond the
master's incomplete-write timeouts. Report discard/release counters and PutEnd
errors separately from capacity eviction.

Changing capacity while keeping a trace's requests fixed measures master
behavior under those offered intents. Eviction can make recorded reads miss,
so their hit counts need not reproduce the generating simulator's state.
Report misses, not-ready results and allocation errors explicitly. This is not
a closed-loop serving simulation, and injecting client `BatchRemove` calls is
not a substitute for exercising the master's background eviction path.

CPU utilization is expressed in cores (1 means one fully occupied core), derived
from Linux process CPU time. Very short replays can have too few samples to estimate
CPU use. Sampled RSS is a sampled peak, not an exact high-water mark. Metrics
scraping adds overhead; hold its interval fixed between comparisons.

`master_total_capacity_bytes` and `master_allocated_bytes` describe logical Store
capacity and allocation, which differ from the master's actual process RSS.
Use the metrics to check capacity throughout the replay.
Throughput includes initial idle time and final drain. Client-call latency
includes connection scheduling, transport and master work; it is not server-only
processing time. Calls count RPCs issued by the replayer; skipped finalizations
do not count. There are no automatic mutation retries. Connection readiness
probes precede the replay clock, and heartbeat traffic is reported separately.
Compare server metrics as well. Miss, not-ready and already-exists outcomes have
separate counts and are not successful-hit counts.

After successful registration, each client starts a background Ping loop that
waits one second after each response before sending again. It shares that
client's identity and RPC client, but does not wait on the trace worker or its
dependencies. Every registered client, including storage-only clients, keeps
pinging until all trace workers finish, covering overdue calls and teardown.
Shutdown stops the loops and joins any in-flight Ping before writing results.
The master retains its default liveness timeouts. Slow or failed Ping RPCs can
still cause expiry; the replayer does not remount or hide these failures.
A Ping error or non-OK client status makes the run unsuccessful.
When migrating older traces, remove Ping events and their dependency edges;
retain ordering between all remaining events. Compare recorded business calls
separately from heartbeat traffic when comparing measurements.

High replay lag alone does not locate the bottleneck. Compare RPC latency,
worker occupancy, offered versus achieved rates, and master and replayer CPU
use. Low master CPU use can also occur while its threads wait for locks. Vary
the input workload or client count to distinguish a workload limit from a master
limit. This benchmark reports whole-master load; lock attribution additionally
requires profiling.
