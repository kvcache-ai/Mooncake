# EPD CI Tiers

`/.github/workflows/epd-cpu.yml` is the pull-request CPU gate. It runs with
model hubs offline, so a real-model fixture must skip rather than download.

`/.github/workflows/epd-hardware-gates.yml` is the scheduled/manual hardware
surface. Its GPU, two-node TCP, and two-node RDMA jobs are disabled by default
and are explicitly skipped until repository variables enable a matching
self-hosted runner. In particular, enabling TCP never satisfies the RDMA job:
the RDMA tier requires a command-produced artifact and
`check_epd_transport_evidence.py --requested rdma --enforce`.

The following gates require self-hosted hardware and must be scheduled rather
than simulated by a GitHub-hosted runner:

1. **Single-node GPU nightly**: strict Qwen-VL E->P->D, explicit vLLM adapter
   report, exact L1/L2 hidden cache, same-node Agent capture/fork/CoW, and the
   Omni CUDA IPC/NVLink case when available.
2. **Two-node TCP/RDMA scheduled**: remote host/IP assertions, actual transport
   backend evidence, cross-node Store manifest clone/materialize/branch decode,
   restart/cancel/lease fault cases, and RDMA NIC/GID/registration metadata.
3. **Benchmark scheduled**: equal-resource three-run workload matrix, raw
   request/response/log/metrics/environment archive, then
   `check_epd_performance_claims.py --enforce`.

TCP compatibility success is never emitted as RDMA evidence. Missing hardware
is a skipped scheduled capability, not a passing RDMA result.
