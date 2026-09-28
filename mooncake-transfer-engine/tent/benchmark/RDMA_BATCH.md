# Opt-in RDMA batch allocation

`transports/rdma/batch_allocation_policy` is a startup option. Its default is
`inverse_score`; set it to `virtual_load` to include already planned bytes when
choosing the next slice. Restart with `inverse_score` (or remove the option) to
revert. Smart scheduling must be enabled and the request must reach the aggregate
allocation path. Single-slice selection, candidate filtering, EWMA feedback,
every-100th-call probes, workers and QPs retain their existing behavior.

```json
{"transports":{"rdma":{"enable_smart_scheduling":true,
 "batch_allocation_policy":"virtual_load","batch_trace_interval":0}}}
```

The candidate's existing bandwidth, NUMA penalty, jitter and accounted inflight
bytes are snapshotted once. Each slice minimizes
`numa_penalty * (inflight_bytes + planned_bytes + slice_length) / bandwidth_Bps + jitter`.
Actual slice lengths, including folded tails, come from the upstream slice plan;
accounting and completion release use the same lengths. Planning costs O(slices *
candidates). This is a local batch plan, not a reservation across concurrent calls.

`static_capacity` is a comparison baseline using positive, finite
`transports/rdma/batch_capacity_gbps` entries indexed by local NIC ID. Its weights
are fixed at startup, independent of backlog and online bandwidth. It preserves
the inverse allocator's integer apportionment and remainder rule.

`batch_trace_interval=0` disables tracing. A positive interval samples ordinary
allocations and probe allocations independently, labelled `stream=normal` and
`stream=probe`. Thus intervals 100 and 1000 do not alias exclusively with probes.
Records include candidate load/rate, assigned bytes and a diagnostic source-slot
address for request correlation. Enable these logs only for diagnostic runs.

Fixed-input selector tests and TCP entry checks below are separate from real RDMA
performance validation. No real dual-rail performance improvement is established
by these tests.

## Native burst / biased-backlog comparison

The compiled target is `tent_rdma_workload`. The existing Python runner retains
`--workload steady` as its default; use the native binary for the two new modes.
All runs use host DRAM and WRITE requests, followed by READ verification outside
the measured interval. No GPU or model deployment is involved.

## Build and local control checks

Configure a host-memory Release build on each Linux endpoint:

```bash
cmake -S . -B build/rdma-batch -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DWITH_TE=ON -DUSE_TENT=ON -DUSE_CUDA=OFF -DUSE_HIP=OFF \
  -DWITH_STORE=OFF -DWITH_STORE_RUST=OFF -DWITH_P2P_STORE=OFF \
  -DWITH_EP=OFF -DBUILD_UNIT_TESTS=ON -DBUILD_BENCHMARK=ON -DBUILD_EXAMPLES=OFF
cmake --build build/rdma-batch --target tebench tent_rdma_workload tent_rdma_workload_test tent_device_selector_test -j4
build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload --help
build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload --config mooncake-transfer-engine/tent/benchmark/workload/client.example.json --dry-run
build/rdma-batch/mooncake-transfer-engine/tent/tests/tent_rdma_workload_test
build/rdma-batch/mooncake-transfer-engine/tent/tests/tent_device_selector_test --gtest_filter=DeviceSelectorBatchTest.TraceSamplesNormalAndProbeIndependently
python3 mooncake-transfer-engine/tent/tests/check_rdma_workload_control.py --binary build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload --output control-check-01
```

The control helper runs two processes with the real TENT/TCP transport on local
loopback. It checks arrival/terminal recording, simultaneous outstanding requests,
non-overlapping memory, WRITE/READ data verification and separate cohort counts.
It also passes an intentionally conflicting inherited `MC_TENT_CONF` and checks
that the explicit workload policies/transport remain effective. Its output says `tcp_control_only`: no NIC loads, RDMA timings or policy rankings
are inferred. Unit tests additionally cover submit failures, timeout/cancel,
unresolved work and rejection of invalid backlog evidence. This is not a NIC simulator.

## First hardware group

Replace A_IP/B_IP and NICs with verified mappings. Base JSON files preserve each
host's GID and transport setup. Static capacities are frozen **by local topology
NIC ID**, not by command-line rail order; `100,100` below is illustrative and must
be replaced with the calibration for the actual topology. The same capacities,
arrival plan, priority, slice size and background amount apply to all policies.
Do not set fixed-bandwidth/jitter/filter overrides for the main result.

First run the existing steady check with tracing disabled (server on B, client
on A in another terminal); stop that server after the client finishes:

```bash
python3 mooncake-transfer-engine/tent/benchmark/rdma_batch_compare.py server --binary build/rdma-batch/mooncake-transfer-engine/benchmark/tebench --host B_IP --port 19001 --rails mlx5_0,mlx5_1 --base-config B-rdma.json --output steady-server-01
python3 mooncake-transfer-engine/tent/benchmark/rdma_batch_compare.py client --binary build/rdma-batch/mooncake-transfer-engine/benchmark/tebench --host A_IP --port 19002 --peer B_IP:19001 --rails mlx5_0,mlx5_1 --base-config A-rdma.json --static-baseline --capacity-gbps 100,100 --repeats 1 --seconds 10 --output steady-first-01
```

Then start a native server on host B:

```bash
python3 mooncake-transfer-engine/tent/benchmark/rdma_batch_compare.py server \
  --binary build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload \
  --host B_IP --port 19001 --rails mlx5_0,mlx5_1 --base-config B-rdma.json \
  --workload biased_backlog --requests 8 --background-mib 32 \
  --output native-server-01
```

The server reserves 8 disjoint `(32 MiB background + 64 MiB target)` slots plus
64 MiB scratch. This is also large enough for the burst command below. It stays
running until stopped after clients finish. Start a larger server plan if the
client's count/background amount needs more registered memory.

On host A, this command executes **inverse_score, virtual_load, static_capacity**
once each, using the upstream #3981 slice planner and actual-byte accounting:

```bash
python3 mooncake-transfer-engine/tent/benchmark/rdma_batch_compare.py client \
  --binary build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload \
  --host A_IP --port 19002 --peer B_IP:19001 --rails mlx5_0,mlx5_1 \
  --base-config A-rdma.json --capacity-gbps 100,100 --static-baseline \
  --workload burst --requests 8 --burst-size 4 --interval-us 5000 \
  --repeats 1 --output burst-first-01
```

Two groups of four arrivals are scheduled at 0 and 5 ms. A delayed submit does
not shift later arrivals. `submit_ns` begins before batch allocation/admission;
`submit_return_ns` and `terminal_visible_ns` are also recorded relative to the
arrival epoch. Full wait starts at **planned arrival**, including admission,
software queueing and polling visibility. `detail.engine_submit_begin_ns` and
`detail.engine_submit_return_ns` additionally delimit the exact engine API call;
terminal visibility is captured immediately after the status API returns, before
batch reclamation. Completion is never a trigger for the
next request's arrival.

For real asymmetric work, first diagnose one configuration in both directions:

```bash
for busy in 0 1; do
  python3 mooncake-transfer-engine/tent/benchmark/rdma_batch_compare.py client \
    --binary build/rdma-batch/mooncake-transfer-engine/tent/tent_rdma_workload \
    --host A_IP --port 19002 --peer B_IP:19001 --rails mlx5_0,mlx5_1 \
    --base-config A-rdma.json --capacity-gbps 100,100 --static-baseline \
    --workload biased_backlog --requests 8 --background-mib 32 \
    --background-gap-us 0 --interval-us 5000 --busy-rail "$busy" \
    --diagnostic --repeats 1 --output "backlog-diag-rail${busy}-01"
done
```

The background and target use the SAME engine. Named transport policies restrict
background to the selected NIC and target to both NICs. They have equal request
priority. The code submits real background work and immediately submits target
work; it never writes an inflight counter. Every request has separate local and
remote offsets, all inside registered host memory. Normal EWMA/probes/filtering
continue unchanged. Warmup uses a separate scratch region before the epoch.

A diagnostic target is `confirmed_live_asymmetric_backlog` only when exactly one
normal aggregate allocation has two candidates, assigned bytes sum to 64 MiB,
the selected busy rail has more actual accounted bytes than the other rail, and
the paired background was observed PENDING at or after that allocation record.
This last check is conservative: if completion races ahead of polling, the case
is rejected rather than claimed valid. Probe/filtered/missing/retry traces or
completed background do not count as valid cases. Every raw case is retained.
The source-slot trace ID also works if dispatch happens after submit returns.

Inspect `native/requests.jsonl` for candidate inputs and per-rail assignments.
If diagnostics form no valid cases, first inspect why (completion, filtering,
probe, dispatch delay), then try one nearby background amount, e.g. 16 or 64 MiB.
Do not replace these with forged selector state.

For the formal timing group repeat the identical two commands **without**
`--diagnostic`, using fresh output names such as `backlog-perf-rail0-01`.
There are no allocation logs in these runs. Per-request exact selector evidence
is therefore explicitly `unverified_trace_disabled`; it is not borrowed from a
different run. Paired-background pending observations remain in the request
records. Diagnostic timings are never combined with the formal timing table.
After the first table, use `--repeats 3` (alternating policy order) and only then
`--baselines` if limited inverse-score alpha tuning is warranted.

## Output and interpretation

Each policy directory retains configuration, GNU time CPU output and process
log. `native/summary.json` has separate background/target sample, success,
failure, timeout and unfinished counts; verified-success useful GB/s; successful
request full-wait P50/P95/P99; process CPU during the measured interval. Counts
are mandatory alongside success percentiles so failures cannot disappear.
Throughput for each cohort uses its verified bytes divided by the common epoch
through last terminal visibility/drain. Raw records retain all terminal waits,
including failures; unfinished work has null terminal time, never a fabricated
completion. Readback runs after timing and checks every byte. A mismatch removes
that request from successful bytes/percentiles and returns a failing exit status.

Timeout is measured from planned arrival. Cancellation does not free a still-live
batch or reuse its region. The entry continues polling through `drain_ms`; if a
request is still unresolved, it writes unfinished records and exits the process
without explicitly freeing live memory. This is a failed run, not throughput.
Failed terminal RDMA tasks can retain outstanding hardware work. After any such
failure the entry also skips memory reuse/unregistration and exits after saving
evidence; a successful `freeBatch` alone is not treated as proof of DMA quiescence.
The server must remain alive throughout client drain and readback.

## Configuration and SSH

The native entry accepts `--config FILE [--dry-run]`; example specs are in
`workload/server.example.json` and `workload/client.example.json`. Dry-run parses
the same plan/config and emits exact memory offsets/arrival times without an
engine, memory registration or network connection. Unknown CLI options fail.

The existing wrapper's `--run-config` SSH fields work unchanged for new modes.
Set `binary` to the remote native executable and `workload` to `burst` or
`biased_backlog`; the same `--ssh-host`, `--ssh-port`, `--ssh-user`,
`--identity-file`, `--ssh-config`, `--jump-host`, `--remote-root` are available.
Controller `--dry-run` never opens SSH. A wrapper dry-run writes the native spec;
run the native `--dry-run` against it on Linux to validate the plan itself.

The native executable ignores inherited `MC_TENT_CONF` because its explicit spec
already contains the complete engine configuration; otherwise the general
runner's environment could erase directed policies or diagnostic settings.
Legacy hardware environment options (e.g. GID) still follow existing TENT rules.
`engine-effective.json` and `topology.json` preserve the actual startup settings
and NIC IDs; `layout.json` is the requested plan. Process CPU covers both cohorts
jointly, and is not attributed artificially to individual background/target flows.
