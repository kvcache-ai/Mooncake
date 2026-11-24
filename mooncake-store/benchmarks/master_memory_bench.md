# Master Memory Segment Benchmark

`master_memory_bench` measures the Master metadata path on a large Memory
Segment data set. It is an independent benchmark: it does not require the
traffic recorder or replay tools.

The default workload mounts two 64 GiB memory segments and creates one hundred
million 512-byte objects with `BatchPutStart` / `BatchPutEnd`. It then reads
random batches with `BatchExistKey` and `BatchGetReplicaList`. The payload is
never written; the benchmark exercises object allocation, key lookup, replica
listing, and the Master's metadata path.

## Workload mix

- Fill: 100% put. Every key gets one `BatchPutStart` and one `BatchPutEnd`; there
  are no reads during fill.
- Read: 50/50 by batch. Even batch numbers call `BatchExistKey`; odd batch
  numbers call `BatchGetReplicaList`. Each batch contains 64 keys.
- No data-plane `Get` and no object payload are involved. `BatchGetReplicaList`
  returns replica descriptors only.
- Random starts come from `mooncake::RandomEngine(seed + thread_id)` and
  `mooncake::randomIndex`; the same seed and thread count reproduce the same
  key sequence.
- The 100M run uses `--read_rate=1000`, `--read_duration=10`,
  `--fill_threads=16`, `--read_threads=16`, `--batch_size=64`, and
  `--seed=1`.

This is a fixed synthetic Master workload, not a recorded model trace. The
`--model` label is result metadata; it does not change key size, mix, or
schedule.

## Build

```bash
cmake --build build --target master_memory_bench -j$(nproc)
```

## Run the 100M-key baseline

Start a Master with enough memory for the metadata and disable eviction:

```bash
./build/mooncake-store/src/mooncake_master \
  --port=50051 --rpc_thread_num=16 \
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
  --result_file=result-1x.json
```

The fill phase reports keys per second. The read phase reports batch and key
throughput and p50/p90/p99/p999 latency. `read.seconds` is the measured wall
time, and throughput is computed from it. `--read_rate` is the target number of
batch RPCs per second; 0 saturates. `--amplify` multiplies that rate, so
`--amplify=2` and `--amplify=3` produce the 2x and 3x load points. Each
invocation mounts its own segments and fills the data set; use a smaller
`--keys` value while tuning the load points.

## Release comparison

`master_memory_gate.py` compares two result files from the same workload. It
checks the object count, object size, segment layout, model label, read
threads, read rate, and amplification. It requires the fill to complete for
every key, then fails when read batch throughput drops by more than 5%, or
read p99 rises by more than 10%, by default. Fill throughput is also gated when
both fills take at least 5 seconds; shorter fills only check completion.

Start each side from a fresh Master. After a 100M-key run the Master may spend
minutes cleaning up the unmounted keys; starting the next run during cleanup
causes RPC timeouts.

```bash
python3 mooncake-store/benchmarks/master_memory_gate.py \
  --baseline=base.json --current=pr.json
```

Exit codes are 0 for pass, 1 for a regression, and 2 for invalid input.

## Automated base/current comparison

`master_memory_runner.py` runs the same workload against two prebuilt Master
binaries. Build the base and current Masters separately, then point the runner
at both binaries and one benchmark binary:

```bash
python3 mooncake-store/benchmarks/master_memory_runner.py \
  --base-master=/tmp/base/mooncake_master \
  --current-master=/tmp/current/mooncake_master \
  --bench=./build/mooncake-store/benchmarks/master_memory_bench \
  --keys=100000000 \
  --runs=3 \
  --workdir=/tmp/master-memory-run
```

The runner starts a fresh Master for every run, stops it before the next run,
writes one result file per run, and aggregates the median fill/read throughput
and latency values. It then calls `master_memory_gate.py` on the two median
reports. The final exit code is the gate's exit code: 0 pass, 1 regression, 2
invalid input or workload mismatch.

Use `--runs=3` for a release check. A single run is enough for smoke tests.
`--read-rate` and `--amplify` apply to both sides, so the workload is identical
by construction; the gate still verifies the plan fields in each report.

## Scope and model size

The 100M-key scenario is a metadata standard, not a model KV-page simulation.
A 512-byte object keeps the complete data set near 100 GiB after allocator
overhead.

The `--model=qwen3-8b` label refers to the classic Qwen3-8B shape: 36 layers,
8 KV heads, head dimension 128, BF16, 64-token pages. One such page is
9,437,184 bytes (9 MiB). One hundred million pages would be about 944 TB, so
they cannot fit on one host. Use a smaller key count when testing model-sized
pages; the 100M baseline deliberately uses 512-byte metadata objects.
