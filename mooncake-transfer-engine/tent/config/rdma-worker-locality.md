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

For concurrent host-memory RDMA in tebench, batches of at least 8 MiB with at
least 32 request threads use bounded status polling: an initial 128 hot
queries followed by one-microsecond sleep requests between queries. Smaller
batches, lower concurrency, and device-memory/other transport waits keep the
hot polling path. OS scheduling can make a sleep longer than one microsecond.
