# Master Memory Segment Benchmark

`master_memory_bench` measures the Master metadata path on a large Memory
Segment data set. It mounts memory segments, creates objects with
`BatchPutStart` / `BatchPutEnd`, then reads them with `BatchExistKey` and
`BatchGetReplicaList`. Object payloads are never written.

The default workload creates 100 million 512-byte objects on two 64 GiB
segments.

## Workload

- Fill sends one `BatchPutStart` and one `BatchPutEnd` per key.
- Read sends 50% `BatchExistKey` and 50% `BatchGetReplicaList` requests.
- The read phase uses 64 keys per batch by default.
- Random batch starts are derived from `--seed` and the global request ID.
  The same seed and request ID select the same batch. The number of requests
  completed before the deadline can vary with scheduling and RPC latency.
- `--model` only labels the result. It does not change the workload.

## Build

```bash
cmake --build build --target master_memory_bench -j$(nproc)
```

## Run

Start one Master with enough memory for the object metadata, and disable
eviction so the read phase sees every key:

```bash
./build/mooncake-store/src/mooncake_master \
  --port=50051 \
  --rpc_thread_num=16 \
  --memory_allocator=offset \
  --eviction_high_watermark_ratio=1.0 \
  --allow_evict_soft_pinned_objects=false
```

```bash
./build/mooncake-store/benchmarks/master_memory_bench \
  --master=127.0.0.1:50051 \
  --keys=100000000 \
  --object_bytes=512 \
  --segments=2 \
  --segment_bytes=68719476736 \
  --fill_threads=16 \
  --read_threads=16 \
  --read_rate=1000 \
  --read_duration=10 \
  --amplify=1 \
  --model=qwen3-8b \
  --result_file=result.json
```

The fill result reports keys per second and separate `BatchPutStart` /
`BatchPutEnd` latency. The read result reports batch and key throughput,
separate `BatchExistKey` / `BatchGetReplicaList` latency,
`missed_deadlines`, and p50/p90/p99/p999 latency. `read.seconds` is the measured
wall time; throughput is computed from it.

Every invocation mounts its own segments and fills the data set. Use a smaller
`--keys` value while tuning load points.

## Multiple clients

To fill more keys than one client process can hold, run one client per host
against the same Master. Give each client a disjoint key range and a disjoint
fake segment address range:

```bash
# Host A
master_memory_bench --master=<master-ip>:50051 \
  --keys=50000000 --key-offset=0 \
  --key-prefix=multi --segment-prefix=host-a --segment-base-offset=0 \
  --segments=2 --segment_bytes=68719476736

# Host B
master_memory_bench --master=<master-ip>:50051 \
  --keys=50000000 --key-offset=50000000 \
  --key-prefix=multi --segment-prefix=host-b \
  --segment-base-offset=137438953472 \
  --segments=2 --segment_bytes=68719476736
```

`--key-prefix` must be the same on both hosts so key numbering is shared.
`--segment-base-offset` must differ by at least
`--segments * --segment_bytes`. Each client mounts and unmounts its own
segments; a client's keys are removed when it exits.

Use the Master's private or public address that the other hosts can reach. A
public address may work for SSH while the RPC port is blocked by a firewall;
check the RPC port from each client host before starting the run.

## Read rate and amplification

`--read_rate` is the target number of read batch RPCs per second. `0` means
saturate. `--amplify` multiplies that target:

```text
target = --read_rate * --amplify
```

Only the read RPC rate changes. Fill, key count, object size, batch size, read
thread count, duration, and the 50/50 mix stay fixed. Gate each version at the
same amplification factor; results from different factors are different
workloads.

The scheduler keeps a next-send time and sleeps only until that time, so RPC
latency does not reduce the offered rate. Use `--read_rate=200 --amplify=2` for
400 batches/s, and `--amplify=3` for 600. Run each load point separately; do not
use a single run as a latency curve.

## Release comparison

`master_memory_gate.py` compares two result files from the same workload. It
checks keys, object size, batch size, fill threads, seed, segments, segment
size, model label, read threads, read rate, read duration, and amplification.

It requires every fill to complete, then fails when:

- read batch throughput drops by more than 5%;
- read p50 rises by more than 10%; p90 and p99 are reported as advisory because tail latency is noisy on shared hosts;
- fill throughput drops by more than 10% (only when both fills take at least
  5 seconds);
- per-operation p50 rises by more than 10%, when both reports include the
  per-operation fields;
- either side reports fill/read errors or missing keys.

```bash
python3 mooncake-store/benchmarks/master_memory_gate.py \
  --baseline=base.json \
  --current=pr.json
```

Exit codes: `0` pass, `1` regression, `2` invalid input.

Start each side from a fresh Master. After a 100M-key run, the Master may spend
minutes deleting the unmounted keys; starting the next run during cleanup can
cause RPC timeouts.

## Base/current runner

`master_memory_runner.py` starts a fresh Master for each run, alternates base
and current runs, records Master CPU and RSS, aggregates medians, and calls the
gate:

```bash
python3 mooncake-store/benchmarks/master_memory_runner.py \
  --base-master=/tmp/base/mooncake_master \
  --current-master=/tmp/current/mooncake_master \
  --bench=./build/mooncake-store/benchmarks/master_memory_bench \
  --keys=100000000 \
  --runs=5 \
  --workdir=/tmp/master-memory-run
```

Use `--base-bench` and `--current-bench` when the two trees have different
client code; otherwise `--bench` is used for both sides.

The runner refuses to start if a configured port is already in use, and checks
that the spawned Master owns the listening socket. Use `--runs=5` with a 30 s
read phase for a release check. A single run is enough for a smoke test. The gate
uses p50, not p99: on a shared host the p99 tail varies by more than 2x across
identical A/A runs, while p50 stayed within 5%. p90 and p99 remain in the report
as advisory metrics.

## Scope

The 100M-key run is a metadata scale test, not a model KV-page simulation. A
512-byte object keeps the data set near 100 GiB after allocator overhead.

The `--model=qwen3-8b` label refers to the Qwen3-8B shape: 36 layers, 8 KV
heads, head dimension 128, BF16, 64-token pages. One page is 9,437,184 bytes.
One hundred million pages would be about 944 TB, so they cannot fit on one
host. Use a smaller key count when testing model-sized pages.
