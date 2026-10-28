# Reproducing the classic TE RDMA optimization steps

This is an opt-in benchmark experiment. Except for `globalConfig()` initialization,
all changes are disabled by default. It does not disable DDIO or change BIOS,
CAT, firmware, or the system THP policy.

The measurements below were made on 2026-10-26 from base
`12a7e9f10a41d578364a58c1d94fd3c5a3a33d6f`, on two Intel KVM guests with eight
ConnectX-7 RDMA devices each. Each device had two LAG physical ports. Each host
used 16 logical CPUs: one process and two NUMA-local CPUs per device.
Write used 8 MiB/device and Read 4 MiB/device throughout the optimization path.
Rates are decimal **GB/s, summed across eight devices, one direction at a time**.

The PR ports the successful options onto main at
`179ffb21c81f86f4cc61a0162841fa6118abac07`. Main now has a different NUMA allocator;
that allocator is preserved. Historical throughput is **not a measurement of the
rebased PR**. Unsuccessful experimental page-policy, polling-pause, demand-read,
and worker-clock switches are not included.

## Ordered change / command / result table

The shell functions below expand the command column into eight concurrent
processes. Use the same stage binary on both hosts. Build the referenced commits
in order for a source-level comparison; comparing all rows with the final binary
cannot isolate the `globalConfig()` change. Start a fresh target group for every
row, use a fresh shell or reset the environment, and repeat each case three times.

`RR` means `MC_EXPERIMENTAL_RR_QP`. Read keeps RR=0 after step 2.
The final prefetch helper is active on **B for Write** and **A for Read**.

| Step / commit | Code change | Commands / settings (on both hosts unless stated) | Historical observation |
| --- | --- | --- | --- |
| 0 / `179ffb21` (PR baseline only) | No new code. NUMA-local memory and two CPUs/device. | DV OFF; `write_run 1048576 8 0`; `read_run 262144 16 0`. Omit the new rolling flag on the baseline. | Original base: Write 284.38, Read 278.06. Current main baseline must be remeasured. |
| 1 / `63aab8a3` | `main.cpp`, `te_backend.{h,cpp}`: disjoint rolling slots; refill a completed slot; include final drain and completed bytes. | DV OFF; `write_run 1048576 8 2`, then `write_run 1048576 8 4`; `read_run 262144 16 2`, then `read_run 262144 16 4`. | Write 296.33 / 300.60; Read 379.70 / 376.05. Three 8-second runs/configuration. |
| 2 / `af1819ff` | `rdma_endpoint.{h,cpp}`: preserve the next QP across calls; extend rolling slots to powers of two through 64. | DV ON, `MC_MLX5_QP_LAG_PORT_BALANCE=1`; run `write_run 1048576 8 8` with RR=0, then RR=1. | Write 193.48 → 310.71 in this specific 8-slot comparison; one short run each. The 193.48 result is not step 1's baseline. LAG balancing already existed; this step enables it. |
| 3a / `b906c873` | `src/config.cpp`: replace repeated `call_once` with thread-safe local-static initialization. | Same settings as step 2, RR=1; `write_run 1048576 8 8`. Compare step 2/3 binaries; there is no runtime toggle. | `globalConfig` on-CPU self samples: Write 9.75% → 4.53%, Read 8.53% → 3.77%. No isolated bandwidth claim. |
| 3b / `b906c873` | Parameter change only; keep the same working set. | RR=1; `write_run 262144 32 16`. Read: RR=0, `read_run 262144 16 4`. | Write 334.44 (three-run mean, range 326.37–341.50). Not solely attributable to 3a. |
| 4 / `a505b47e` | `te_backend.cpp::pinThread`, `worker_pool.cpp::transferWorker`: lower CPU ID submits; higher CPU ID runs worker. | `MC_EXPERIMENTAL_SPLIT_CPU=1`; Write RR=1, `MC_IB_PCI_RELAXED_ORDERING=1`; `write_run 262144 32 16`. | Write short comparison 335.50 → 337.89. Read still fluctuated without prefetch. CPU budget stays unchanged. |
| 5 / `18db13b3` | `runTarget` / `runRollingTransfers`: periodic prefetch of the unchanged active range, using existing threads. | Keep step 4 settings. Write: on B set `MC_EXPERIMENTAL_TARGET_WARM_BYTES=8388608`, `MC_EXPERIMENTAL_TARGET_WARM_US=10000`; A runs `write_run 262144 32 16`. Read: clear target warm flags; on A set `MC_EXPERIMENTAL_INITIATOR_WARM_US=100000`, RR=0; `read_run 262144 16 4`. | Write prefetch short comparison 338.02 → 385.42. Final 3×20 s means: Write **385.310**, Read **385.327**; 60 s: 385.392 / 385.446. |

The final PR also formats changed lines, rejects initiator prefetch on non-DRAM
memory, and releases a rejected rolling submission without polling it. Use the
final PR head for deployment; the commit IDs in the table identify each change.
Stage results mix source and parameter changes and are not additive gains.

## Build each stage

Use a Linux build host with CMake, a C++ compiler, libnuma, libibverbs/mlx5,
gflags, glog, jsoncpp, yaml-cpp and OpenSSL development dependencies. Initialize
submodules. Limit build parallelism to the allocated CPU budget.

```bash
# Run from a checkout of the stage commit. Use a new build directory each time.
# Set DV=OFF for stages 0/1, DV=ON for stages 2 and later.
cmake -S . -B build-stage -DCMAKE_BUILD_TYPE=Release \
  -DENABLE_DEBUG_SYMBOLS=ON -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF \
  -DUSE_CUDA=OFF -DUSE_TENT=OFF -DUSE_MLX5DV="$DV" \
  -DUSE_HTTP=OFF -DUSE_ETCD=OFF -DUSE_REDIS=OFF \
  -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=ON -DBUILD_UNIT_TESTS=OFF
cmake --build build-stage --target tebench -j 4
```

Copy the same executable and required libraries to both hosts. Compare hashes
and check `ldd` for missing dependencies. The historical build used GCC 13.3,
pybind11 2.13.6 and perftest 24.01.0. Rebuilding can change binary hashes.

## Eight-device commands (Bash, on each host)

Reserve idle resources first. Determine the actual NIC/NUMA/CPU mapping, GID and
reachable host IPs. Do not infer available resources from GPU utilization alone.
`NICS`, `CPUS`, `NODES` and `PEER_NODES` must contain eight entries. Each `CPUS[i]`
is two distinct NUMA-local logical CPUs, with no overlap between devices.
Do not copy CPU numbers from another machine. Keep the RPC port range clear of
other services and the ephemeral port range.

Fill these variables in a fresh Bash shell on **both** hosts:

```bash
BENCH=/absolute/path/to/tebench
LOCAL_IP=REPLACE_WITH_THIS_HOST_REACHABLE_IP
GID=REPLACE_WITH_VALID_GID_INDEX
NICS=(mlx5_0 mlx5_1 mlx5_2 mlx5_3 mlx5_4 mlx5_5 mlx5_6 mlx5_7)
# Fill arrays using the actual local and peer topology:
CPUS=(CPU_PAIR_0 CPU_PAIR_1 CPU_PAIR_2 CPU_PAIR_3 CPU_PAIR_4 CPU_PAIR_5 CPU_PAIR_6 CPU_PAIR_7)
NODES=(0 0 0 0 1 1 1 1)
PEER_NODES=(0 0 0 0 1 1 1 1)
OUT=/absolute/path/to/a/new/case-directory
mkdir "$OUT"                     # fails if reused
DURATION=20
export MC_WORKERS_PER_CTX=1 MC_NUM_QP_PER_EP=8 MC_MAX_WR=1024
export MC_MAX_CQE_PER_CTX=32768 MC_RDMA_SIGNAL_INTERVAL=32
export MC_RDMA_WORKER_IDLE_SPIN_US=1000
# Apply only this row's optional MC_* settings; clear previous row's settings.

te() {
  local i=$1; shift
  exec env MC_TE_FILTERS="${NICS[i]}" MC_GID_INDEX="$GID" \
    MC_MIN_RPC_PORT="$((20000+i*2))" MC_MAX_RPC_PORT="$((20001+i*2))" \
    GLOG_logtostderr=1 numactl --physcpubind="${CPUS[i]}" --membind="${NODES[i]}" \
    "$BENCH" --backend=classic --seg_type=DRAM --xport_type=rdma \
    --metadata_type=p2p --seg_name="$LOCAL_IP" --total_buffer_size=33554432 \
    --local_gpu_id="${NODES[i]}" --target_gpu_id="${PEER_NODES[i]}" "$@"
}
```

On **B**, start targets first and record their actual advertised segment names:

```bash
TARGET_PIDS=()
for i in {0..7}; do
  te "$i" > "$OUT/target-$i.log" 2>&1 & TARGET_PIDS+=("$!")
done
# Wait until all eight logs contain --target_seg_name=... .
# Check startup errors if any entry is missing. Copy those eight names to A,
# in NIC order, as TARGETS=(name0 name1 ... name7). Do not substitute plain IPs.
```

On **A**, define the initiator helpers, then execute the table's commands:

```bash
# TARGETS=(...)  # exact segment names from B's corresponding target logs
run_eight() {
  local op=$1 block=$2 batch=$3 slots=$4
  local pids=() rolling=() failed=0
  if (( slots > 0 )); then rolling=(--experimental_inflight_batches="$slots"); fi
  for i in {0..7}; do
    te "$i" --target_seg_name="${TARGETS[i]}" --op_type="$op" \
      --start_block_size="$block" --max_block_size="$block" \
      --start_batch_size="$batch" --max_batch_size="$batch" \
      --start_num_threads=1 --max_num_threads=1 --duration="$DURATION" \
      --result_output_jsonl="$OUT/result-$i.jsonl" "${rolling[@]}" \
      > "$OUT/initiator-$i.log" 2>&1 & pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
  return "$failed"
}
write_run() { MC_SLICE_SIZE="$1" run_eight write "$@"; }
# For step 0/1 Read use MC_SLICE_SIZE=262144; later Read uses 1048576.
read_run() { MC_SLICE_SIZE="${READ_SLICE:-1048576}" run_eight read "$@"; }
```

For step 0/1, set `READ_SLICE=262144`; later use `READ_SLICE=1048576`.
Use new `OUT` directories on both hosts and restart B between cases, so flags
and prefetch roles cannot leak between comparisons. Stop only this case's target
processes on B (`kill -INT "${TARGET_PIDS[@]}"; wait`), then verify exit and release
resources. The helper uses `exec`, so recorded PIDs belong to the benchmark
processes; never use global `pkill`.

Before measuring each final footprint, use `run_eight write_seed BLOCK BATCH SLOTS`
and then `run_eight read_verify BLOCK BATCH SLOTS` against the same targets, with
separate initiator output directories. All eight checks must succeed. Prefetch
never writes payloads and is included in the initiator's measured wall time.

## Interpretation and controls

- Sum the eight `aggregate_throughput_gbps` values in the final JSONL records:
  this field is **GB/s**, despite its name. Rolling output `batch_size` is the
  number of requests **per slot**; completed bytes equal operations × block size
  × that reported batch size. Include drain time; do not count submitted bytes.
- Compare with eight concurrent `ib_write_bw` or `ib_read_bw` processes, bound to
  the same NIC/NUMA mapping: `-d NIC -i 1 -c RC -x GID -p PORT -s 262144 -q 8
  -t 1024 -D 24 -f 2 --report_gbits -l 32 -Q 32`. Only clients append the peer IP.
  Divide reported Gbit/s by eight before summing. Effective timing is about 20 s.
- Historical perftest means were Write 386.476 / Read 386.723 GB/s. TE reached
  99.70% / 99.64% of those peaks, but perftest's active footprint was about
  **2 MiB/device**, not TE's 8/4 MiB. This is not a matched-footprint comparison.
- At **64 MiB/device**, historical TE and perftest were around 275–280 GB/s.
  Do not generalize the 385 GB/s small-working-set result to cold DRAM traffic.
- The guest exposed no usable CPU/uncore PMU. Cache residency is an explanation
  supported by the prefetch comparison, not direct LLC/DDIO/IMC measurement.
  Relaxed Ordering is not a DDIO-disable switch.
- Check all devices carry traffic, no error/discard counters increase, all thread
  affinities stay within allocated CPUs, and no background traffic contaminates
  the case. NIC data counters use four-byte units. Repeat three times, report
  mean/min/max and CPU use, then set `DURATION=60` for a sustained check.
- Capture CPU profiles separately, never average profiled runs into throughput:
  `perf record -e cpu-clock:u -F 99 --call-graph dwarf,4096 -p PID0,PID1,... -- sleep 8`.
  Original final TE initiators used about 15.3–15.5 CPUs vs about 8 for perftest.

## CPU-only validation

```bash
python3 mooncake-transfer-engine/benchmark/tests/rolling_transfer_mock_test.py
```

This compiles the production rolling method body against a fake engine with
ASan/UBSan. It checks disjoint in-flight regions, completion byte accounting,
drain on stop, rejected submissions, deferred cleanup, invalid partitions and
the DRAM-only prefetch guard. It does not compile the production headers or
validate RDMA, QP distribution, thread placement, or the rebased throughput.
