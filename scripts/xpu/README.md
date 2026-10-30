# XPU hardware core (Stage 2)

Stage 1 builds a CPython 3.12 wheel and matching native tests on hosted CPU
runners. Stage 2 consumes that exact artifact on a fixed two-B60 allocation.
It does not build while holding GPUs, use models, publish wheels, or enable
nightly hardware/application coverage (Stage 3).

## Runner contract

Register dedicated, isolated, **non-root, ephemeral** runners with the `xpu`
label (`runs-on: xpu`). PR code executes inside the runtime container;
do not place credentials, other tenants, or persistent trusted workloads on
these runners. Destroy/reimage the runner after each job. Docker access is
host administration capability: container flags are not a substitute for this
runner isolation.

Each eight-GPU host runs four runners, each owning a fixed, non-overlapping
pair. GitHub runs one job at a time per runner; no additional allocation lock
or lock-directory configuration is needed. No other workload may use those
assigned devices.

Install Docker, `gh`, Python 3.12, and `timeout`. The only required GPU
configuration is the runner's existing **host-owned** environment variable:

```sh
ZE_AFFINITY_MASK=0,1
```

For example, the four masks can be `0,1`, `2,3`, `4,5`, and `6,7`, using the
host's Level Zero whole-device ordinals. All render nodes are passed into the
container to preserve device enumeration; the mask selects the assigned pair.
Preflight requires exactly two visible B60s and real GPU arithmetic.
**The mask is not a security boundary:** this setup trusts the fixed runner
allocation and test code not to use other devices. It is not suitable for
untrusted multi-tenant code. No privileged mode, host network/PID/IPC namespace,
Docker socket, publishing token, or `CAP_SYS_PTRACE` is passed to the container.

CI automatically downloads a commit-pinned Moby default seccomp profile,
verifies its SHA256, and writes a per-run profile allowing `pidfd_open`,
`pidfd_getfd`, `io_uring_setup`, `io_uring_enter`, and `io_uring_register`.
All other default rules remain unchanged. No `XPU_SECCOMP_PROFILE`,
`XPU_LOCK_DIRECTORY`, or `XPU_RENDER_DEVICES` setting is required or consumed.
The host needs HTTPS access to `raw.githubusercontent.com`; download or
checksum failure aborts the job, never falls back to `seccomp=unconfined`.
The generated profile is retained in the diagnostic artifact.
Host Yama `ptrace_scope` must be 0 or 1,
Level Zero IPC/P2P must work, and the run's output filesystem must support
`O_DIRECT` and io_uring. Missing permissions/capabilities fail core, not skip it.

The container keeps `--network none`: Store and EP peers communicate only
inside its private loopback network. CI sets both the Python Store's
`LOCAL_HOSTNAME` and TENT's distinct `MOONCAKE_LOCAL_HOSTNAME` to `127.0.0.1`.
TENT's automatic address discovery skips loopback, so the latter must be
explicit even when Store setup already supplies a local segment name.
EP's oneCCL KVS also excludes loopback during automatic discovery, so CI sets
`CCL_KVS_IFACE=lo`; the EP test's CPU process group uses
`GLOO_SOCKET_IFNAME=lo`. These are container-local interface selections, not
additional runner-service settings. XCCL and the native Level Zero IPC fast
path remain enabled; no network access or fallback-only coverage is introduced.

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
write/maintain/admin permission can add `run-e2e-ci` to an open PR targeting
`main`. The shared **E2E CI** entry point runs XPU alongside the existing ROCm,
Ascend and other integration jobs; XPU is required, not an optional selection.
The existing E2E label cleanup removes the label after the gate; add it again
to rerun. A new PR head needs new authorization; leaving the label attached
does not authorize later pushes.

Alternatively dispatch **E2E CI** on trusted `main`, supplying `pr_number`
(current head) and optionally `pr_sha` (must match), or a full 40-character
`pr_sha` without a PR. With neither input, XPU tests the dispatch's `main`
commit. No branch names or implicit latest artifacts are accepted.
Fork PR heads are supported through their commit SHA.

The hosted builder receives that exact SHA. Hardware downloads its artifact
by ID in the same run and verifies the API identity, run/attempt, ZIP digest,
payload/native hashes, source/image provenance, and build/runtime inventories.
The host runs the trusted workflow-revision harness, not PR host scripts.

The shared `E2E Gate` requires XPU authorization, build, hardware tests and
cleanup to succeed. Its hosted-only XPU step publishes the `XPU Core` check
on the authorized commit, rechecking the PR head and build SHA before success.
Failed, cancelled, skipped, missing or stale required XPU results cannot pass.
The existing E2E concurrency policy cancels older runs for the same PR;
an interrupted controller can leave an in-progress check, never a successful
check. Other platforms retain their existing jobs and gate policy.
The normal Stage 1 hosted build remains independent and required by the
existing PR CI gate. Nightly still builds only the XPU artifact, without
running XPU hardware tests.

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
source SHA, and elapsed suite time, including on failure. A failed/timed-out
command's final 40 log lines are also printed so nested test errors appear in
the Actions step; full logs remain in the artifact. Only the current container
is stopped/removed; its PID namespace owns all test descendants.
Cleanup failure fails the hardware job. Runner reimaging remains mandatory
after failure or cancellation.

CPU-only regression validation:

```sh
python3 -m unittest scripts.ci.tests.test_xpu_ci -v
```

All XPU CI regressions share this one test module and common workflow/module
loaders; they validate packaging and orchestration contracts, not actual B60
behavior. Docker launch, device mapping, traps and cleanup are inline in
`.github/workflows/integration-test-xpu.yml`. A temporary cleanup helper is
created from that trusted workflow in the host run directory and shared by
the exit trap and the `always()` cleanup recheck; it is not mounted into the
test container.

`scripts/e2e/python/verify_xpu_wheel.py` is the single verification entry point:
no arguments retain the hosted wheel smoke checks; `unpack` verifies the
downloaded artifact, `seccomp` prepares the pinned profile, and `run` installs
the wheel offline and launches the `suite`/`store` hardware checks. There are
no separate XPU runtime scripts under `scripts/ci/`.

Enable hardware only after provisioning and successfully running the exact
core workflow; record real queue/runtime measurements during rollout.
