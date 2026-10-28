# Large-working-set RDMA Write: 8 threads × 1 GiB

**The experiment has NOT saturated 400 GB/s.** The best repeated mean was
**274.464 GB/s**, versus **258.671 GB/s** at the start of this round (+6.11%).
Do not apply the earlier small-working-set ~385 GB/s result to this workload.

## Fixed workload and ordered changes

Two Intel KVM guests, eight ConnectX-7 VFs per host, A→B CPU DRAM Write.
One TE process has eight submission threads plus TE background threads, all
within 16 authorized logical CPUs per host. Each submission thread traverses a
separate 1 GiB range: **8 GiB active on each host**, with prefetch disabled.
On two NUMA nodes `--total_buffer_size=8589934592` registers 16 GiB/host; the
registered size is not the active footprint. Reserve sufficient memory first.

TE configurations were interleaved in three rounds, 20 seconds/configuration.
The perftest control was run separately: eight processes, one per NIC, with
`-D24 -f2` (20-second measurement interval). It is a reference, not the same
single-process implementation. Units are decimal **GB/s** summed across NICs.

| Step | Code change | Settings / command | Mean | Range |
| --- | --- | --- | ---: | ---: |
| 0: this round's starting point | Explicit thread→NIC hints, prior rolling implementation | `QPS=8 WINDOW_REQUESTS=legacy REUSE_REQUESTS=0 run_te "$RUNTIME_BASE"` | 258.671 | 257.443–259.875 |
| 1: bounded rolling window | `te_backend.cpp`: traverse the full range, prevent reuse of live chunks, reuse descriptors; `main.cpp`: count actual slot bytes | `QPS=2 WINDOW_REQUESTS=32 REUSE_REQUESTS=1 run_te "$RUNTIME_WINDOW"` | 272.635 | 271.362–273.662 |
| 2: remove unnecessary notifications | `worker_pool.cpp`: keep diagnostic timestamps but remove per-poll `notify_all()`; endpoint test uses bounded polling | Same settings, `run_te "$RUNTIME_NOTIFY"` | 274.464 | 272.211–277.114 |
| Reference | `experiments/perftest-cycle.patch`: configurable address ring | 256 KiB, 8 QP/NIC, 128 MiB/QP, depth 64, post-list/CQ moderation 1 | 267.941 | 266.715–269.541 |

Step 0 already includes earlier optimizations. The earlier 8 GiB serial result
was 131.268 GB/s; rolling4/rolling16 were 263.703/265.607 GB/s. These are different
rounds, not additional gains to add to this table.

Step 1's 32 requests are split into 16 slots of two 256 KiB requests. This is
8 MiB **in flight per thread**, while each thread still traverses all 1 GiB.
A busy-chunk bitmap prevents wraparound over an outstanding transfer, including
out-of-order completions. Descriptor reuse never writes/prefetches payloads.
The JSON batch size is the actual per-slot request count, not 4096.

The profile's `performPollCq` self share fell from 28.02% to 3.89% after step 2.
Its extra throughput gain is only ~0.67%, with overlapping ranges; this is **not
established as an independently stable bandwidth gain**. Profiles are separate
runs, excluded from the clean throughput means, and use software CPU samples.

## Versions and evidence boundaries

The measurements were made on archived experimental sources based on
`12a7e9f10a41d578364a58c1d94fd3c5a3a33d6f` plus the historical optimization patches.
The earlier PR port starts from main `179ffb21c81f86f4cc61a0162841fa6118abac07`.
This update ports the additional changes while preserving that main's newer
NUMA allocator, DRAM guard, partition validation and rejected-submit cleanup.
**The updated PR head has not been rebuilt/rebenchmarked on Linux/RDMA.**
The commands below are a recipe to repeat the comparison; they are not new
measurements of the port. Exact measured binary hashes and individual repeated
results are in [large_working_set_results.json](experiments/large_working_set_results.json).
Use separately archived stage binaries to reproduce the historical numbers;
using the final binary for every row does not isolate the code changes.

The archived implementation passed seed-write/read-verify across 8 GiB, measured
full-range coverage on all eight threads, NIC traffic/error checks, byte accounting
and authorized CPU affinity checks. Its modified endpoint state test passed 10/10.
The port's CPU-only test compiles the actual rolling method with ASan/UBSan;
it covers descriptor reuse on/off, delayed completion, source/target bounds,
no live overlap, full traversal, exact bytes, rejected submissions and stop/drain.
This is not production-header compilation or an RDMA transport test.

## Run on two reserved idle hosts

Build classic TE using the existing [build instructions](rdma_optimization.md),
with `USE_MLX5DV=ON`, and deploy the **same stage runtime** to both hosts.
A runtime directory must contain `bin/tebench` and its required `lib/` dependencies.
If using your own build tree, provide these paths accordingly; verify with `ldd`.
For the historical comparison keep `RUNTIME_BASE`, `RUNTIME_WINDOW`, and
`RUNTIME_NOTIFY` separately. Restart the task's target when changing stage.

First inspect `numactl -H`, NIC NUMA nodes and allowed CPUs. Set `A_CPUS` and
`B_CPUS` to the 16 authorized CPUs per host. Set `NIC_ORDER` to eight NIC names
in submission-thread order; the two-node test topology used
`mlx5_0,mlx5_4,mlx5_1,mlx5_5,mlx5_2,mlx5_6,mlx5_3,mlx5_7`. Do not copy this mapping
without checking the local topology. The helper explicitly disables prefetch
and split-two-CPU mode, and starts exactly one TE process per host.

```bash
# On each host, from mooncake-transfer-engine/benchmark:
export NIC_ORDER=mlx5_0,mlx5_4,mlx5_1,mlx5_5,mlx5_2,mlx5_6,mlx5_3,mlx5_7
# Fill A_IP/B_IP, authorized CPU lists and absolute runtime paths yourself.
# B: leave the target running; copy its full printed target segment name.
QPS=2 bash experiments/run_large_working_set.sh target "$RUNTIME_WINDOW" "$B_CPUS" "$B_IP"
```

```bash
# A: TARGET_SEG is the full value printed by B, not just B_IP.
# Verify first (the same target stays running).
QPS=2 WINDOW_REQUESTS=32 REUSE_REQUESTS=1 \
  bash experiments/run_large_working_set.sh initiator "$RUNTIME_WINDOW" "$A_CPUS" "$A_IP" "$TARGET_SEG" 16 4096 1 write_seed
QPS=2 WINDOW_REQUESTS=32 REUSE_REQUESTS=1 \
  bash experiments/run_large_working_set.sh initiator "$RUNTIME_WINDOW" "$A_CPUS" "$A_IP" "$TARGET_SEG" 16 4096 1 read_verify
```

```bash
# A: shorthand for the table. Use a fresh result filename every time.
run_te() {
  bash experiments/run_large_working_set.sh initiator "$1" "$A_CPUS" "$A_IP" "$TARGET_SEG" 16 4096 20 write
}
QPS=8 WINDOW_REQUESTS=legacy REUSE_REQUESTS=0 RESULT_JSONL=baseline-r1.jsonl run_te "$RUNTIME_BASE"
# After restarting B with the corresponding stage runtime/QPS:
QPS=2 WINDOW_REQUESTS=32 REUSE_REQUESTS=1 RESULT_JSONL=window-r1.jsonl run_te "$RUNTIME_WINDOW"
QPS=2 WINDOW_REQUESTS=32 REUSE_REQUESTS=1 RESULT_JSONL=notify-r1.jsonl run_te "$RUNTIME_NOTIFY"
# Repeat baseline -> window -> notify for rounds 2 and 3; do not run cases concurrently.
```

Check `aggregate_transferred_bytes == aggregate_operations * block_size * batch_size`.
The final eight coverage messages must report the entire 1 GiB/thread; warmup
coverage may be smaller. Confirm all NICs carry traffic and errors do not increase.
Deferred batch-cleanup warnings can occur; record them and independently verify
completion accounting and shutdown. Stop only this task's processes and release
the resource reservation afterward.

## Matched-footprint perftest

The perftest source archive used was 24.01.0 (`ib_write_bw -V` reports 6.20).
Apply [perftest-cycle.patch](experiments/perftest-cycle.patch) to that source and
build `ib_write_bw` using its normal configure/make instructions and dependencies.
The patch changes only the configurable cyclic buffer size; unset preserves the
original behavior. The reference used 128 MiB per QP × 8 QP × 8 NIC = 8 GiB.
A 256 KiB request gives 512 positions/QP, with only 64 requests in flight/QP.
`-l1` is required for address advancement; `-Q1` simplifies completed-byte checks.

```bash
# CPU_PAIRS: eight semicolon-separated pairs, in mlx5_0..mlx5_7 order.
# Set LD_LIBRARY_PATH for your perftest dependencies as needed.
# B first; wait until all eight listening ports 24000..24007 are ready.
bash experiments/run_cyclic_perftest.sh target "$PERF_BINARY" "$NUMACTL" "$B_CPU_PAIRS" '' target-r1
# A, in a separate terminal/host:
bash experiments/run_cyclic_perftest.sh initiator "$PERF_BINARY" "$NUMACTL" "$A_CPU_PAIRS" "$B_IP" client-r1
```

Sum the eight `BW_average` values and divide by 8 (Gb/s → GB/s). Repeat three
times with fresh output directories. The 10-second smoke result was 299.539 GB/s,
but the repeated 20-second mean was 267.941 GB/s; do not select the smoke peak as
a stable result. This control does not establish the hardware's absolute ceiling.

## What remains unresolved

QP count, request size, window size, slot count, CQ moderation, WR depth,
worker count, polling delay, relaxed ordering, process-local THP and NUMA variants
were tried; none reached 400 GB/s. Source-side remote NUMA reduced TE to ~179 GB/s;
fixed-NIC target-side remote NUMA reduced perftest to ~130 GB/s. NUMA matters.

No BIOS/DDIO/CAT/global-THP/firmware setting was changed. These guests expose VFs,
restricted PCI capabilities, no host MSR device and no IMC/IIO/CHA PMU. The user
has guest access only. There is **no verified guest-side DDIO disable control**,
and this data does not establish DDIO's actual state or prove it is the sole
bottleneck. CPU memory-loop results are not a DRAM/RDMA hardware ceiling either.
