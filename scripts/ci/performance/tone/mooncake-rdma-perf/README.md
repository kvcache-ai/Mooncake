# T-One RDMA performance gate

This is a tone-cli **functional** suite whose Pass/Fail result enforces a
performance policy. Version 1 measures TENT host DRAM RDMA read/write on a fixed
pair. It does not measure GPU Direct, Store RPC, or model-serving performance.
The suite is shipped with Mooncake so its adapter and controller can be reviewed
together. Installing these files and registering a T-One template are separate
deployment steps; adding the GitHub workflow does not provision hardware.

## Provision and install

1. Reserve two Linux RDMA hosts exclusively for this template. Install matching
   RDMA kernel/userspace drivers, Mooncake build dependencies, C++20, CMake,
   Ninja, Python 3.10+, Git, numactl, rdma-core utilities, GNU timeout, and OpenSSH.
   Use a fixed image/toolchain. eRDMA hosts need a mode supporting Mooncake's OOB
   QP setup; validate connectivity first. Keep eRDMA/RoCE/IB hardware profiles
   and thresholds separate. RXE/SIW are not accepted as hardware measurements.
2. Give the T-One controller key-based SSH access to **both** hosts using the
   same dedicated benchmark account for every job. Populate known_hosts in
   advance; host-key verification is mandatory. Allow the pair's RDMA traffic
   and p2p metadata RPC ports. Configure unlimited memlock (at least twice the
   256 MiB buffer is checked). Do not share these hosts with inference jobs.
3. Copy `scripts/ci/performance/tone/hosts.example.json` to
   `/etc/mooncake-rdma/hosts.json` on the controller. Replace the documentation
   addresses, device names, ports, GID indices, and NUMA nodes with the actual
   pair. The first host is the initiator; the second is the target. Validate
   with `ibv_devinfo -d DEVICE -i PORT` and `numactl --hardware`. This file is
   operator-controlled, not supplied by the PR.
4. From a reviewed Mooncake checkout, install the adapter into tone-cli:

   ```bash
   cp -R scripts/ci/performance/tone/mooncake-rdma-perf "$TONE_ROOT/tests/"
   cp scripts/ci/performance/tone/mooncake-rdma-perf.conf "$TONE_ROOT/conf/functional/"
   ```

5. Register `mooncake-rdma-perf` as a **functional** suite with the single
   `paired` case. Create a template named `mooncake-rdma-perf` in workspace
   `mooncake_test`, project `mooncake-ci`. Bind the reserved controller/pair and
   set `RDMA_HOSTS_FILE=/etc/mooncake-rdma/hosts.json`. Do not reuse the SGLang
   template's case list or mutable container images. Grant the existing T-One
   CI account create/query/stop/log-read access to this template.
6. Verify `/api/job/log/query/` exports the suite's `rdma-results.log` and that
   its download URL is reachable from GitHub over HTTPS. Set the GitHub variable
   `TONE_RESULT_ORIGIN` to that exact origin (scheme + hostname + optional port,
   no path). Redirects and other download origins are rejected. If the deployed
   T-One storage endpoint only supports HTTP or private addresses, provision an
   HTTPS download endpoint before enabling the gate. No credentials are sent
   to the result-download URL.

No changes to the existing `mooncake-ci-test` suite or its SGLang/vLLM jobs are
needed. The public tone-web API implementation is the integration contract;
confirm the deployed service's response shape during commissioning.

## Inputs and execution

The GitHub controller injects `BASELINE_SHA`, `CANDIDATE_SHA`, `HARNESS_SHA`
(all full 40-character commit IDs in kvcache-ai/Mooncake) and `RDMA_RUN_ID`.
No branch-tip clone or PR patch application is used. `install.sh` fetches the
exact harness revision. `RDMA_HOSTS_FILE` belongs to the template configuration.

Each host acquires `~/.mooncake-rdma-perf.lock` for the entire run. A busy host
fails immediately; all suites sharing the pair must honor this lock/account.
The GitHub workflow serializes the fixed pair with `queue: max`, retaining up to
100 pending runs instead of replacing the previous pending run. Additional runs
are canceled when the queue is full. This queue does not cover jobs submitted
directly to T-One; the host leases still reject those if the pair is busy.
The two hosts build both revisions using the **same harness benchmark source**,
baseline tracked CMake recipes and Release flags, with CUDA/Store disabled and
TENT enabled. Candidate-only CMake recipes are removed from disposable checkouts;
target definitions and parent flags therefore cannot change the paired workload.
This compares implementation changes under a fixed build configuration, not
candidate build-system changes, and is not a sandbox for candidate C++ code.
Building on the
pair keeps the eRDMA provider and runtime libraries consistent; it does not
depend on `tebench` being present in a wheel. Builds finish before measurements.
Build-configuration, binary and linked-library hashes, compiler, device/port/GID, kernel, boot IDs,
NUMA CPU set, policy digest, harness identity and both revisions are recorded.

`tebench` is forced to TENT + RDMA with an explicit RDMA transport hint and NIC
allowlist. `--dram_numa_node` selects one DRAM node, with CPU/memory NUMA binding
on each host. The node's available CPUs are used; this is not a promise of
single-core affinity. The default cases are 4 KiB read/write P99 (one thread,
batch 1) and 1 MiB read/write bandwidth (four threads, batch 16).

An integrity smoke with `--check_consistency` runs before each case/version's
first measurement and is excluded from the samples. Every measured invocation
has tebench's built-in one-second warmup followed by ten seconds of measurement.
Five pairs alternate A/B, B/A, with both hosts switching to the same revision.
The policy gates bandwidth loss above 5% and P99 growth above 10%; excessive
relative MAD is an ERROR. These initial limits require hardware A/A calibration.

## Results, cancellation and rollout

`rdma-results.log` contains a JSON envelope with the run ID and both normalized
result sets. `.log` is intentional: tone-web's log API indexes that suffix.
`parse()` copies all results and native/build/SSH logs into
`TONE_CURRENT_RESULT_DIR`, then emits `rdma_perf: Pass` or `rdma_perf: Fail`.
Missing reports are Fail. An ERROR is also Fail in T-One; its reason is in the
comparison report.

GitHub downloads this envelope and recomputes the verdict using its trusted
policy. It does not trust a returned Pass/Fail string or merely a completed job.
The Actions artifact contains the envelope, normalized samples, identity/build
manifests, job status, policy and comparison. The full raw native/build logs
remain in the T-One job artifacts. In both systems, 0 means pass, 1 regression,
and 2 invalid/incomplete/unstable execution. Summary tables and errors survive
failures. A 45-minute controller timeout or a cancellation requests T-One stop;
workers also have a 45-minute watchdog. SSH EOF normally releases each host
lease and stops only that job's processes. Network partitions/build timeouts can
delay cleanup until the worker timeout. Do not override a busy lease.

Targets run directly in their own process group, without an inner `timeout`
wrapper that could forward the group's SIGTERM a second time. Normal stop sends
one SIGTERM and waits up to ten seconds for a clean exit before escalating to
SIGKILL; an already-dead target or a nonzero exit remains an ERROR. Initiator
commands retain their per-sample timeout, and the worker watchdog bounds target
lifetime if the controller disconnects without delivering EOF.

The GitHub workflow uses existing `TONE_USER_NAME`/`TONE_USER_TOKEN` secrets.
Optional variables `TONE_WORKSPACE`, `TONE_PROJECT`, and `TONE_RDMA_TEMPLATE`
override the defaults. PR runs remain disabled until `RDMA_PERF_ENABLED=true`.
Manually dispatch the workflow first with the same full SHA for both inputs,
using a reviewed workflow revision that contains this harness. Native A/A
calibration, an injected regression, lost-target, and cancellation tests must
pass before making **RDMA Performance Gate** required.

PR runs use the base revision's harness/policy/benchmark and compare the base
against the PR test merge commit. Candidate code runs only on the isolated pair.
Fork PRs do not receive GitHub secrets; if automatic gating is enabled they fail
closed without submitting a T-One job. Maintainers can run reviewed comparisons
manually; such a manual run is not automatically a required check on the fork PR.
Choose a fork-approval/check-publication policy before enabling a required check
for all contributors. This change does not alter branch protection or grant
fork code credentials.

For direct controller reproduction (no T-One or GitHub credentials needed):

```bash
python3 scripts/ci/performance/rdma_collect.py \
  --hosts /etc/mooncake-rdma/hosts.json \
  --benchmark mooncake-transfer-engine/benchmark \
  --baseline-sha FULL_BASE_SHA --candidate-sha FULL_HEAD_SHA \
  --harness-sha FULL_REVIEWED_HARNESS_SHA --run-id local-1 \
  --output fresh-rdma-results
```

Old revisions that cannot compile the frozen benchmark fail explicitly. They
are never replaced with a newer baseline. CPU/fixture tests validate the control
path only; they are not evidence of RDMA bandwidth, latency or A/A stability.
