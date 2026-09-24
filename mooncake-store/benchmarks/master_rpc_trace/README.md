# Offline master RPC trace benchmark

This benchmark separates **offline workload generation** from **real master
measurement**. Stop the simulator before replay: the measurement process only
loads a saved trace and calls `MasterClient` against a real Mooncake master.
It does not run a model or transfer KV payloads.

The initial implementation provides:

- A versioned JSONL RPC-intent format and a Python writer for producer adapters.
- Validation before connecting to a master, plus optional initialization traces.
- Timestamp-driven concurrent replay with explicit operation dependencies.
- Real batch existence, replica lookup, put-start, put-end, revoke and remove calls.
- Per-operation latency/throughput summaries and per-event timing records.

**Integration boundary:** `recorder.py` is an export interface, not an installed
SGLang simulator hook. The SGLang-side adapter still needs to translate HiCache
events into physical Mooncake keys, object sizes and actual client API calls.
Do not treat AutoBench prompts, logical pages, or the included handwritten
smoke trace as production master traffic. One KV page is not necessarily one
Mooncake object. Simulator logical time, not its execution wall time, must be
supplied to the writer.

## Build and test

Build the real RPC executable with the normal Mooncake dependencies:

```bash
cmake -S . -B build -DBUILD_BENCHMARK=ON -DUSE_CUDA=OFF \
  -DWITH_STORE_RUST=OFF
cmake --build build --target master_rpc_trace_bench mooncake_master -j 4
```

The scheduler/parser tests can also run without the Store, transfer engine,
SGLang or GPU dependencies. They only need a C++20 compiler, CMake, JsonCpp,
GoogleTest and Python 3:

```bash
cmake -S mooncake-store/benchmarks/master_rpc_trace/tests \
  -B build-trace-tests
cmake --build build-trace-tests -j 4
ctest --test-dir build-trace-tests --output-on-failure
```

After building the real executables, run the optional RPC integration checks:

```bash
python3 mooncake-store/benchmarks/master_rpc_trace/tests/test_rpc_smoke.py \
  --master=build/mooncake-store/src/mooncake_master \
  --replayer=build/mooncake-store/benchmarks/master_rpc_trace_bench
```

These checks start their own loopback-only masters and exercise cross-client
reuse, partial write failure, initialization, revoke and removal. They verify
correctness, not performance.

## Smoke replay

Use a **dedicated, empty benchmark master**. Registered segments have fake
addresses and cannot serve payload reads to real serving clients. Stop this
master after the experiment. The replayer never removes arbitrary pre-existing
objects to reset a run.

Start the master in a separate terminal:

```bash
build/mooncake-store/src/mooncake_master
```

Then validate and replay the example:

```bash
build/mooncake-store/benchmarks/master_rpc_trace_bench \
  --trace=mooncake-store/benchmarks/master_rpc_trace/example.jsonl \
  --validate_only

build/mooncake-store/benchmarks/master_rpc_trace_bench \
  --trace=mooncake-store/benchmarks/master_rpc_trace/example.jsonl \
  --master_server=127.0.0.1:50051 --workers=4 --speed=1 \
  --output=result.json --samples=samples.jsonl
```

The example writes two objects, commits them, and reuses them from another
logical client. Its existence query also contains one intentionally missing
key. A miss is counted separately from a transport or server error.

Each logical client receives a fresh UUID and a fake segment, with capacity
controlled by `--segment_size` (default 64 MiB per client). The capacity must
match the intended modeled storage; it is not the actual payload RAM usage.
BatchPutStart object sizes affect the master's real allocation and metadata.
End/Revoke use the same owning client as Start. Failed Start keys are skipped
at End/Revoke rather than incorrectly marked complete. Segment registration,
connection setup, trace parsing and worker startup occur outside measurement.
Initial registration uses `ReMountSegment` to complete the master's first-client
handshake before measurement. Later requests to remount invalidate the run.
Segments are unmounted on exit; do not assume this restores every master state
immediately. Use a fresh master for repeated comparisons.

`--prefill_trace=initial.jsonl` replays a separate trace before measurement,
using the same clients and segments. All prefill key outcomes must succeed.
The measured trace must then describe accesses against that initialized state.
Dependencies cannot cross file boundaries; completion of prefill is the barrier.

## JSONL format, version 1

The first nonblank row is a header. Event times are nonnegative integer
**microseconds relative to the replay origin**; this differs from AutoBench's
millisecond request timestamps.

```json
{"type":"master_rpc_trace","version":1,"time_unit":"us","metadata":{"source":"simulator-adapter","initial_state":"empty","seed":42}}
{"id":"s0","timestamp_us":0,"client_id":"instance-0","op":"BatchPutStart","keys":["prefix/page-0"],"value_sizes":[4096],"replica_num":1}
{"id":"e0","timestamp_us":1000,"client_id":"instance-0","op":"BatchPutEnd","keys":["prefix/page-0"],"put_start":"s0"}
{"id":"r1","timestamp_us":2000,"client_id":"instance-1","op":"BatchGetReplicaList","keys":["prefix/page-0"],"depends_on":["e0"]}
```

| Field | Contract |
| --- | --- |
| `id` | Unique event ID. |
| `timestamp_us` | Planned invocation time on the simulator's logical timeline. Rows must be nondecreasing. |
| `client_id` | Logical client identity; multiple events from one client may overlap. |
| `op` | One of the six supported `Batch*` operations listed above. |
| `keys` | Ordered, nonempty list of physical object keys; batch boundaries are preserved. |
| `value_sizes` | Required for Start: one positive byte count per key. Each is passed as one slice. |
| `replica_num` | Optional positive memory replica count for Start; default 1. |
| `depends_on` | Optional earlier event IDs. Completion dependencies do not imply success. |
| `put_start` | Required for End/Revoke; implies a completion dependency on Start and selective handling of its successful keys. |

Each Start must have exactly one End or Revoke with the same client and ordered
keys. General dependencies can refer only to earlier rows, so cycles cannot be
introduced. Unknown fields and operations fail validation instead of being
silently ignored. All events and parameters are retained in memory during a run;
size the replay host for the trace as well as the master metadata.

The v1 adapter models MEMORY replicas and default `ReplicateConfig` options
(except `replica_num`). Remove respects leases (`force=false`). Pinning, group
IDs, per-request placement overrides, disk replicas, dynamic topology and HA
failover are not modeled. A producer must not silently discard those semantics
when exporting a workload that relies on them.

Use metadata to record the SGLang/Mooncake commits, model/layout, page size,
topology, cache capacities, routing, random seeds and initial object state.
Object sizes and client counts are explicit assumptions, not a calibrated
conversion from a GPU count.

## Recording from an adapter

The producer resolves physical keys and supplies a logical clock explicitly:

```python
from recorder import RpcTraceWriter

with RpcTraceWriter("rpc.jsonl", metadata={"clock": "logical"}) as trace:
    start = trace.record(
        timestamp_us=0, client_id="instance-0", op="BatchPutStart",
        keys=["physical-key-0"], value_sizes=[4096],
    )
    trace.record(
        timestamp_us=1000, client_id="instance-0", op="BatchPutEnd",
        keys=["physical-key-0"], put_start=start,
    )
```

Add this directory to the producer's Python import path. The writer creates a
new file and refuses to overwrite an existing trace. Merge multiple simulator
streams on their shared logical timeline before writing; the writer does not
invent routing, keys, timestamps, arrival distributions or dependency edges.

## Arrival control and CPU isolation

`--speed=1` preserves trace intervals. Higher values compress them; explicitly
report this as a stress-test transformation. Changing time scale also changes
its relationship to real master lease/TTL timers. `--workers` caps in-flight
calls and is independent of the number of logical clients.

Due calls are dispatched without waiting for unrelated requests to complete.
When all workers are busy, overdue calls remain queued and the lag is recorded;
their planned timestamps are never moved forward to conceal overload.
Dependency waits are also included in dispatch lag. A frozen trace does not
model the serving feedback caused by a slower master.

On a single host, use `lscpu -e=CPU,CORE,SOCKET,NODE` to choose separate physical
cores for master and replayer; avoid assigning SMT siblings across them. Start
each with `taskset -c <chosen-cpus> ...`. The benchmark does not clear inherited
CPU affinity. Keep master core count fixed between runs, and report residual
shared memory/NUMA contention. Set `MC_STORE_RPC_CLIENT_IO_THREADS` explicitly
to keep the replay client's I/O pool bounded. Observe master CPU/RSS separately
(for example with `pidstat`); the replayer's own process usage is not master usage.

## Result interpretation

Outputs are written **after** replay, not from the hot path:

- `samples.jsonl`: event/client/op, scheduled/start/finish times, whether an API
  call was issued, original key count, key outcomes and error details.
- `result.json`: per-operation attempted call/key throughput, key status counts,
  P50/P95/P99/max dispatch lag, client-call latency, and scheduled-to-completion
  latency; plus replay configuration and producer metadata.

Client-call latency includes the client API and its RPC work; it is not a
server-only service-time measurement. `rpc_calls` counts issued MasterClient
API invocations. Retries may cause additional wire requests; use server metrics
to validate actual arrivals. Heartbeats are separate maintenance traffic and
are counted separately. A failed heartbeat invalidates the run; the benchmark
does not silently remount and continue with a changed cache state.
`--heartbeat_interval_ms` controls the pause between sequential client sweeps
(default 1000 ms); slow sweeps lengthen each client's actual heartbeat period.

Throughput uses the full measured elapsed time, including leading idle time and
draining the final calls. Miss, not-ready and already-exists outcomes have their
own counters; they are not silently counted as successful hits or new writes.
Already-existing keys are skipped by End/Revoke, as for other non-OK Start keys.
Unexpected RPC/executor errors
produce a nonzero exit code. An entirely skipped End/Revoke is not counted as an
issued RPC. Increasing worker count should reduce replay lag before master
saturation; a large lag at low master utilization indicates insufficient replay
capacity, not a measured master limit.
