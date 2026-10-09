# Peer attribution experiment

This experiment changes only the inputs to the existing inverse-score allocator.
Lane dispatch, probing, slice planning, admission and transmit metering are
unchanged. Peer accounting is **off by default**: the original NIC path does
not create peer entries, acquire peer locks or read the peer clock. Set
`transports/rdma/peer_accounting: true` explicitly for every V0–V3 experiment.
`peer_scoring` alone does not enable accounting. V1 means NIC scoring **with**
the common ledger; `baseline` in the runner means original NIC scoring with
the ledger disabled. Native smart-disabled round robin is not V0.

| `transports/rdma/peer_scoring` | Scoring inflight | Scoring bandwidth |
|---|---|---|
| `v0` | zero | nominal link rate |
| `v1` | NIC total | NIC latency EWMA |
| `v2` | NIC total | peer latency EWMA |
| `v3` | peer | peer latency EWMA |

V2/V3 use NIC bandwidth for **all** candidates if any candidate has no sample
or its sample is older than `peer_feedback_ttl_ns`. V3's exact inflight count
does not fall back. Link-speed reseeding updates both NIC and peer EWMAs.
The separate TTL defaults to one second for setup only. In each scenario run
a V1 preflight with normal feedback/probing and unique request slots. For every
noncold aggregate decision, compute the maximum candidate sample age using
the shared monotonic decision timestamp and each candidate's `last_sample_ns`.
Take nearest-rank P99 across these request maxima, round **up** to whole
milliseconds (minimum 1 ms), and freeze it before formal comparisons. Cold
decisions are counted separately; no age zero is imputed and no multiplier is
applied. `analyze_peer_diagnostic.py RUN_DIR CLIENT_LOG` implements this rule
and refuses to propose a TTL from incomplete or non-RDMA diagnostic evidence.

All four enabled variants share the same per-device mutex and peer ledger.
`peer_history_target` (default 4096) retains idle history, not transmission
admission. Insertion may reclaim unexpired idle entries, restarting their
feedback cold. Active entries can exceed the target; releases trim excess
entries as they become idle. All map operations hold the mutex and no entry
reference escapes it. There is no table-capacity transmission failure.

Worker charge ownership remains `charged_dev.exchange(-1)`. A peer settlement
error is counted but cannot block release of the owned NIC charge; a NIC
underflow is rejected. Corrupt peer bookkeeping is not silently repaired, so
negative tests deliberately retain that mismatch while testing NIC settlement.
Legacy overloads preserve absent attribution as `nullopt`, distinct from valid
peer ID zero. With accounting enabled, legacy unattributed charges still run
and increment accounting errors. All RDMA production callers pass target ID.
Configuration is startup-only; do not toggle accounting with live charges.

## Native two-peer workload

Build `tent_peer_workload` and `tent_peer_workload_test` with `USE_TENT=ON`,
`BUILD_UNIT_TESTS=ON`, `BUILD_BENCHMARK=ON`. The controller and safe drain/readback
logic are adapted from the previous arrival-driven RDMA workload; the new plan
has two independent periodic streams in **one** TransferEngine.

An example client specification follows. Replace host placeholders with verified
addresses. Create the output parent directory; each output directory must be new.

```json
{
  "role": "client", "output": "run-v1", "variant": "v1",
  "engine": {
    "local_segment_name": "SND:19400", "metadata_type": "p2p",
    "rpc_server_hostname": "SND", "rpc_server_port": 19400,
    "transports": {"rdma": {"num_lanes": 6, "device": {"gid_index": 3}}}
  },
  "rails": ["mlx5_1", "mlx5_2"], "slots": 32,
  "peer_feedback_ttl_ns": 1000000000,
  "target": {"peer": "P:19401", "count": 1000, "interval_us": 5000},
  "background": {"peer": "Q:19402", "count": 1000, "interval_us": 5000},
  "local_contention": false, "busy_rail": 1,
  "diagnostic": false, "warmup": 8, "max_memory_mib": 8192
}
```

Both server specifications have the same stream/slot layout, `role: server`,
their own output directory and engine address/port. Start them first and wait
for their `ready.json`. Requests default to 64 MiB; P and Q source slots do not
overlap. `--dry-run` validates the plan without opening an engine.

```sh
tent_peer_workload --config p-server.json
tent_peer_workload --config q-server.json
tent_peer_workload --config client-v0.json
tent_peer_workload --config client-v1.json
tent_peer_workload --config client-v2.json
tent_peer_workload --config client-v3.json
```

Use the same binary and configurations except variant/output for five complete
paired blocks. Before measurements, shuffle a four-row balanced Latin-square
order with seed 20261001 and append one seeded permutation for the fifth block;
save the exact order. Never change the order or stopping point based on effect. Preserve failed runs and stop to
investigate them. Normal comparison uses no mask or interferer. H2 sets
`local_contention: true` and shortens Q's period sufficiently to validate real
near-line-rate traffic on L1. H1 has no mask and adds a real INT-to-Q0 WRITE
stream; verify the remote-only slowdown and unchanged SND-to-P0 path under V0
before interpreting it. Do not infer switch isolation from reachability alone.

Short diagnostics set `diagnostic: true` and both stream counts no greater
than `slots`, so `(peer, request_key)` identifies each unique request. Allocation
logs report feedback source (0 nominal, 1 NIC, 2 peer), probe flag, candidate
count, scoring/NIC inflight, bandwidth and assigned bytes. Completion sample
logs include the first sample. Allocation records also include candidate sample
counts and last-sample times. Detailed logging is disabled in performance runs.
Enabled-ledger teardown always emits low-cost decision/source counts, disjoint
cold/expired fallback counts (cold takes precedence), aggregate/probe counts,
queue scope, and per-device cold/expired/relearned/reclaimed/accounting errors.
The native runner also snapshots these counters immediately before and after
the measured WRITE interval, outside latency/CPU timing. Its `summary.json`
contains the difference, excluding warmup and readback, even on failure exits.
Use these interval counts for formal fallback fractions. Teardown log counts
cover the complete process lifetime and are separately labelled. V3 retains
`queue_scope=peer` even when its feedback source is NIC.

The output records planned arrival, actual submission and visible terminal time.
The summary separates P (`target`), Q (`background`) and all requests. It reports
successful full-wait latency, goodput, CPU, failures, timeouts and unfinished
requests. Performance runs verify final slot contents; short diagnostics with
unique slots verify every request. `control_tcp: true` is solely a control-path
check and never RDMA performance evidence.

Starved peer/NIC pairs still depend on the existing every-100th aggregate probe
to relearn; this experiment adds no exploration policy. Deterministic T1/T2
selection tests establish information and blind spots, not hardware benefit.

## Cost reference and scenario gates

Use `variant: baseline` versus `variant: v1` with identical arrivals, slots and
normal feedback. For the small-slice cost check set `request_bytes: 2097152`
and `block_bytes: 65536`: the production WRITE planner must yield 32 slices of
65536 bytes, including the last. `layout.json` reports the result of the same
planner function; verify it with `engine-effective.json` and the short RDMA
allocation diagnostic before claiming a hardware cost measurement. The
64 MiB / 2 MiB-block workload remains the V0–V3 mechanism comparison.

Normal control and H2 do not require LLDP. Validate actual shared local RNIC
competition for H2. H1 needs V0 concurrent interference off/on controls before
the scoring comparison. Predeclare Q's designated-path slowdown threshold and
P's one-sided allowable worsening bound in the frozen run plan. P improving is
allowed. A wide interval is inconclusive, not proof P is unaffected. PFC/ECN
counters are supporting evidence only. Without verified switch-port topology,
report selective peer end-to-end slowdown, not a proven switch-egress bottleneck.
If P also generates interference, include its own receive path in these checks.
