# Opt-in virtual-load RDMA batch allocation

The normal aggregate allocator assigns the whole batch using one inverse-score
snapshot. With an uneven initial backlog it can over-concentrate a finite request
on the initially less-loaded NIC. `virtual_load` instead chooses each slice using
that snapshot plus the bytes already planned for the same batch:

`numa_penalty * (inflight_bytes + planned_bytes + slice_length) / bandwidth_Bps + jitter`

Enable it at startup with smart scheduling enabled:

```json
{"transports":{"rdma":{"enable_smart_scheduling":true,
 "batch_allocation_policy":"virtual_load"}}}
```

The default remains `inverse_score`. Restart with `inverse_score`, or remove the
option, to revert. Unknown policy names warn and fall back to `inverse_score`.

Candidate filtering, bandwidth feedback, NUMA penalties, single-slice selection,
every-100th-call probes and worker/QP execution are unchanged. Slice lengths and
accounting use the existing upstream planner, including folded tails. Planning
costs O(slices * candidates) and does not reserve load across concurrent calls.

Selector regressions cover asymmetric backlog in both directions, folded tails,
device masks, probe accounting and release conservation. These are fixed-input
correctness checks, not hardware performance results. Dual-rail RDMA throughput,
request latency and CPU validation remain pending.
