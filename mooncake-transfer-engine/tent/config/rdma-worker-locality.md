# NIC-local RDMA worker dispatch

`transports/rdma/workers/prefer_nic_local` selects workers on the NUMA node of
each preselected NIC:

```json
{
  "transports": {
    "rdma": {
      "workers": {"prefer_nic_local": true}
    }
  }
}
```

The default is `false`. Enabled dispatch uses the workers' published CPU NUMA
placement and a separate round-robin cursor per NIC. Unknown NIC placement,
unpublished worker placement, and a NUMA node without a matching worker retain
the ordinary lane cursor. Worker startup placement and retry ownership remain
unchanged.

This option selects a subset of the worker/QP lanes for each NIC. Enable it
when measuring CPU/DRAM locality; compare against the default with the same
lane count, workload, and competing traffic.

Only requests large enough to preselect their NICs are dispatched this way;
small requests keep the ordinary lane cursor. Measurements so far are from a
single host, where initiator and target share memory and NUMA nodes, so the
gain across hosts may be smaller.

tebench's `--poll_backoff` (TENT backend, default off) bounds status polling
to cut initiator CPU: 128 hot queries, then one-microsecond sleep requests
between queries. OS scheduling can make a sleep longer than one microsecond.
Keep it off when comparing results across thread counts or batch sizes.
