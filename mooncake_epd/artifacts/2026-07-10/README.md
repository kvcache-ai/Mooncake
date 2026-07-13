# Benchmark Evidence Snapshot

This directory contains the compact, reviewable claim record for the
2026-07-10 Mooncake EPD evaluation. `benchmark_summary.json` records the test
environment, workload boundaries, measured values, source artifact paths, and
SHA-256 digests.

The full raw serving artifacts remain outside the repository because they
contain large service logs. Stable source IDs and digests make the summarized
claims auditable without publishing machine-specific runtime details.

## Interpretation Boundary

The reported baseline comparison uses four GPUs for the 2P2D EPD deployment
and one GPU for the colocated baseline. It demonstrates scale-out capacity and
tail-latency behavior under the stated load. It does not establish
equal-resource efficiency.

All public performance claims should cite `benchmark_summary.json` and retain
this boundary.
