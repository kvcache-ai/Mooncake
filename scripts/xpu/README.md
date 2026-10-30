# XPU hardware core (Stage 2)

Stage 1 builds a CPython 3.12 wheel and matching native tests on hosted CPU
runners. Stage 2 consumes that exact artifact on a fixed two-B60 allocation.
It does not build while holding GPUs, use models, publish wheels, or enable
nightly hardware/application coverage (Stage 3).

## Runner contract

Register dedicated, isolated, **non-root, ephemeral** runners with labels
`self-hosted, Linux, X64, xpu`. PR code executes inside the runtime container;
do not place credentials, other tenants, or persistent trusted workloads on
these runners. Destroy/reimage the runner after each job. Docker access is
host administration capability: container flags are not a substitute for this
runner isolation.

Install Docker, `gh`, Python 3.12, `flock`, and `timeout`. Supply these
**host-owned runner environment variables**, not PR-controlled workflow inputs:

```sh
ZE_AFFINITY_MASK=0,1
XPU_RENDER_DEVICES="/dev/dri/renderD128 /dev/dri/renderD129"
XPU_LOCK_DIRECTORY=/var/lock/mooncake-xpu
XPU_SECCOMP_PROFILE=/etc/mooncake-ci/xpu-seccomp.json
```

`ZE_AFFINITY_MASK` must select two distinct whole GPUs. `XPU_RENDER_DEVICES`
can be omitted only on a host exposing exactly two render nodes. Verify that
the mask still identifies the assigned B60 pair inside the device-restricted
container: GPU ordinals can change when devices are hidden. The container must
see exactly two B60s. No privileged mode, host network/PID/IPC namespace, Docker
socket, publishing token, or `CAP_SYS_PTRACE` is passed to the test container.

Create a shared writable lock directory for allocations on the same host.
Every user of a render node must honor its per-node lock; do not provision
overlapping runners. Locks cover container startup through verified teardown,
and are per device rather than global across both runner pairs.

Provide a root-owned, runner-readable but **not runner-writable** seccomp
profile. Start from the Docker default for the deployed Docker version and
explicitly allow `pidfd_open`, `pidfd_getfd`, `io_uring_setup`,
`io_uring_enter`, and `io_uring_register` without adding `CAP_SYS_PTRACE`.
Do not use `seccomp=unconfined`. Host Yama `ptrace_scope` must be 0 or 1,
Level Zero IPC/P2P must work, and the run's output filesystem must support
`O_DIRECT` and io_uring. Missing permissions/capabilities fail core, not skip it.

The runtime uses the same digest-pinned Intel PyTorch image as Stage 1.
Provision compatible Intel GPU drivers and matching SYCL/Level Zero runtimes;
dependency loading, wheel RECORD/native capabilities, and real GPU arithmetic
are checked before the suites. The hosted builder also downloads the wheel's
Python dependencies into the hashed native bundle. The offline container
installs only this wheel and those dependencies (`--no-index --find-links`)
into a fresh temporary import directory, retaining the pinned image's PyTorch.
Intel's native runtime is resolved from that image's Python prefix; no SYCL or
Level Zero runtime is added to the Mooncake wheel.

## Trigger and revision binding

After these workflows are available on upstream `main`, a maintainer with
write/maintain/admin permission can add `run-xpu-ci` to an open PR targeting
`main`. Remove/re-add the label to rerun. A new PR head needs new authorization;
leaving the label attached does not authorize later pushes.

Alternatively dispatch **XPU Hardware Core** on trusted `main`, supplying
`pr_number` (current head) and optionally `checkout_sha` (must match), or a full
40-character `checkout_sha` without a PR. No branch names or implicit latest
artifacts are accepted. Fork PR heads are supported through their commit SHA.

The hosted builder receives that exact SHA. Hardware downloads its artifact
by ID in the same run and verifies the API identity, run/attempt, ZIP digest,
payload/native hashes, source/image provenance, and build/runtime inventories.
The host runs the trusted workflow-revision harness, not PR host scripts.

`XPU Core Gate` requires authorization, build, hardware tests, and cleanup to
succeed. A hosted-only job publishes the `XPU Core` check on the authorized
commit, rechecking the PR head before success. Failed, cancelled, skipped,
missing, or stale required results cannot pass. An interrupted controller can
leave an in-progress check, never a successful check. This label workflow does
not start/cancel Linux, ROCm, or other accelerator CI. The normal Stage 1
hosted build remains independent and required by the existing CI gate.

## Required coverage and diagnostics

Core executes sequentially:

- 19 platform cases, including foreign/interior USM, dma-buf export, and
  same-/cross-device IPC and copies.
- 7 XPU transport cases.
- 114 engine cases: local copies, TCP/HP-TCP staging (tests explicitly disable
  IPC for those paths), retry/routing, IPC lifetime/policies, and io_uring.
- 8 real cross-process IPC/fallback cases, including permission-denied peers.
- 1 C++ Store foreign-USM roundtrip with an in-process master.
- 1 existing Python Store tensor/registered-buffer roundtrip with a packaged
  local master.
- Native EP on two ranks: eight fixed-seed rounds covering BF16/FP8,
  event/hook completion and dispatch weights, plus empty-rank, overlap,
  duplicate-expert, handle, cross-stream, and fallback-agreement checks.
  The existing script asserts native selection and must print its exact final
  completion record; missing rounds or zero-test execution fail.

The **149 C++ cases** require exact GoogleTest identities and completed XML
results. Ten parameter combinations that unconditionally skip because they
are inapplicable are excluded *before* execution, with reasons in
`selection.json`. No runtime skips are allowed. The eight direct-RDMA
parameterizations are explicitly excluded; dma-buf export itself needs no NIC.
GDS, multi-node, more than two EP ranks, and vLLM are not claimed as coverage.

Artifacts retain runner/container cleanup state, device/runtime identity,
selection, XML, logs, inventories, dependency diagnostics, artifact metadata,
source SHA, and elapsed suite time, including on failure. Only the current
container is stopped/removed; its PID namespace owns all test descendants.
Cleanup failure fails the hardware job. Runner reimaging remains mandatory
after failure or cancellation.

CPU-only regression validation:

```sh
python3 -m unittest discover -s scripts/ci/tests -p 'test_xpu*.py' -v
bash -n scripts/ci/run_xpu_core.sh
```

These regressions validate orchestration contracts, not actual B60 behavior.
Enable hardware only after provisioning and successfully running the exact
core workflow; record real queue/runtime measurements during rollout.
