# CI performance regression gate

The `Performance regression` workflow builds the PR base and test merge commit
in Release mode on the same Linux runner. It runs the existing
`offset_allocator_concurrency_bench` with the **base revision's benchmark source**
in both builds. The baseline's policy and Python harness are also used, so a PR
cannot quietly change its own workload or thresholds. The first PR introducing
this directory necessarily bootstraps the harness from the candidate and emits
a warning. Benchmark / policy changes take effect for subsequent PRs after merge.

The initial suite covers successful allocate/free, capacity rejection, and
fragmentation rejection with one and two threads. It measures allocator CPU
performance; it does not validate master RPC, Store end-to-end, RDMA, or GPUs.
Those need their own workload adapters and, for RDMA, a fixed pair of machines.

The opt-in [T-One RDMA gate](tone/mooncake-rdma-perf/README.md) adds that fixed-pair
adapter for TENT DRAM read/write. It has a separate workflow and policy; the CPU
suite does not require RDMA credentials or hardware. Deploy and calibrate the
T-One template before enabling `RDMA_PERF_ENABLED`.

## Results and failure behavior

Open **Performance Gate → Details → Summary** to see each case's baseline and
candidate medians, regression percentage, threshold, and measurement spread.
The `performance-results` artifact contains raw CSV/stderr for every invocation,
the exact policy, build logs, binary hashes, commit SHAs, CPU affinity, compiler,
environment, normalized samples, `comparison.json`, and `summary.md`.

`policy.json` specifies five measured runs per revision. Each invocation performs
100,000 warmup operations per thread, then separate throughput and latency phases
of 2,000,000 operations per thread. Measurement order alternates A/B, B/A.
All subprocesses inherit the same two-CPU affinity. No benchmark invocations run
concurrently. The default limits are:

| Metric | Better | Fail when regression exceeds | Maximum relative MAD |
|---|---|---:|---:|
| `qps` | Higher | 5% | 5% |
| `p50_ns` | Lower | 5% | 5% |
| `p99_ns` | Lower | 10% | 10% |

A case can override individual limits, for example
`"max_regression_percent": {"qps": 8}` inside that case's configuration.
Other metrics retain their default limits. Threshold changes require review.

For baseline median B and candidate median C, regression is `100 * (B-C)/B`
for throughput and `100 * (C-B)/B` for latency. Equal-to-threshold changes pass.
Every metric must pass; improvements do not cancel other regressions.
Relative MAD is `100 * median(abs(sample - median)) / median` for each revision.
A noisy metric is an ERROR, not a confirmed code regression or a passing check.

The comparator writes its reports **before** exiting: 0 = passed, 1 = regression,
2 = invalid/incomplete/unstable measurement. Missing cases, units or SHAs that do
not match, non-finite or non-positive values, benchmark errors, and incomplete
sample counts fail closed. Expected allocation rejection is part of the capacity
and fragmentation workloads; any *unexpected* result fails correctness checks.
Setup/build failures also produce an ERROR summary. Reports are uploaded even on
failure. Re-running creates a new artifact; there is no retry-until-green logic.

## Runner calibration and branch protection

The default runner is `ubuntu-22.04`. Both revisions share the same VM, but hosted
machines can still have host contention and frequency variation. Start with A/A
calibration: manually dispatch the workflow with **the same full commit SHA** for
both `baseline_ref` and `candidate_ref`. Run across multiple times of day before
choosing thresholds or making the check required. MAD detects within-run spread;
it cannot establish that the host is free of systematic bias.

For repository-owned PRs and manual runs, `PERF_RUNNER` can name an exclusive
Linux runner label. Provision the normal Mooncake dependencies, CMake, Ninja,
Python 3, and a C++20 compiler in advance; dependency installation is automatic
only on hosted runners. Use one runner process per machine and reserve its CPUs
for this suite. Fork PRs always use disposable hosted VMs. A dedicated runner
must isolate and discard PR build state; do not give it production credentials.
Calibrate hosted and dedicated environments separately.

After validation, add **Performance Gate** to required status checks in the
repository ruleset / branch protection. The workflow has no path filters, and
the named gate runs even if the harness-test job fails. This PR does not modify
repository protection settings. Only make it required after calibration.

For release-to-release comparisons or an A/A run, use `workflow_dispatch` with
explicit baseline/candidate SHAs. The baseline must contain the selected
benchmark and support the same flags. Old or incompatible baselines produce an
ERROR; they are never silently replaced. Each PR's base is resolved anew, so a
separate fixed release anchor is needed to detect cumulative small regressions.

## Local reproduction

Build the benchmark target for both revisions with the workflow's CMake flags.
On Linux, collect results into a fresh directory:

```bash
python3 scripts/ci/performance/perf_gate.py collect \
  --baseline /path/to/base/build-perf/mooncake-store/benchmarks/offset_allocator_concurrency_bench \
  --candidate /path/to/candidate/build-perf/mooncake-store/benchmarks/offset_allocator_concurrency_bench \
  --baseline-sha FULL_BASE_SHA --candidate-sha FULL_CANDIDATE_SHA --output results
python3 scripts/ci/performance/perf_gate.py compare \
  --baseline results/baseline.json --candidate results/candidate.json \
  --baseline-sha FULL_BASE_SHA --candidate-sha FULL_CANDIDATE_SHA --output results
python3 -m unittest discover -s scripts/ci/tests -p test_perf_gate.py -v
```

Use the same policy and benchmark source for both revisions. The comparison
command also runs on non-Linux machines. Unit tests use controlled fixture
executables to verify reporting and exit status; they do not measure Mooncake
performance or replace a native A/A calibration.
