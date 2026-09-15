# PR6 task congestion status API: review-ready repair plan

Date: 2026-09-16. Base: PR5 `b0eeddd197ed55c9336c4b5a0fe1b0dbb1dd9969`. Candidate before repair: `88f7354c5d8c5c2785bd0dc7e7d5fa3f32cb6b7d`. The existing 18 semantic PR6 commits and their author/sign-off boundaries remain the stack contract.

## Acceptance contract

- The Classic TE and TENT task-level minimal/detailed C++ and C APIs, plus the TENT Python API, retain the existing design contract and batch lifetime rules. Runtime or compile-time congestion control OFF remains `Unknown` without changing transfer behavior.
- All-target builds and full CTest suites pass in all four `MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL` × `USE_TENT` quadrants on the HCA toolchain. Built-only suites do not count as complete.
- An isolated, repaired Mooncake wheel built with TENT exposes both `import mooncake.tent` and compatibility `import tent`; an installed consumer can call the new task-state API. The wheel without TENT retains its previous import behavior.
- At least one two-host RDMA public-path transfer test calls the new task API before `freeBatch` and checks result/payload. State classification under induced congestion or faults is claimed only when actually observed.
- A same-precondition PR5/PR6 compile-OFF/runtime-OFF/observe/enforce comparison reports measured throughput and latency, with binary/toolchain/host/GID/workload/runner provenance. No fixed percentage is assumed.
- Two independent owner/mentor reviews inspect the exact repaired head. Formal findings require executed public-path RED on that head; after repair the same probe must be GREEN. Community approval itself is not inferred from local evidence.

## Implementation sequence

1. Preserve exact candidate with a local backup ref; lock branch/base/SHA and verify commit identity. Add failing installed-wheel contract tests and retain exact build-failure logs as RED evidence.
2. In `mooncake-transfer-engine/tent/tests/CMakeLists.txt`, conditionally link the RDMA cancel test to the congestion-control core only when congestion control is enabled. Run the exact former commit-5 target and its tests GREEN, then fold only this change into semantic commit 5.
3. In `mooncake-transfer-engine/tests/rdma_endpoint_state_test.cpp`, value-initialize the three local slices and remove their helper's redundant move assignment. Run the exact former commit-12 target and its tests GREEN, then fold only this change into semantic commit 12.
4. Stage the existing TENT pybind extension and its local shared-library dependencies into the Mooncake wheel when `USE_TENT` is built; add the smallest top-level `tent` compatibility shim and CMake Python-component install target if required by the installed contract. Build and install an isolated wheel; turn the installed-import RED test GREEN. Fold package changes into the existing Python-binding semantic commit.
5. Replay/fold the repair commits without a cleanup commit, preserving the 18 logical boundaries and original author/date/sign-offs; compare old/new stacks with `git range-diff`. Verify PR5 ancestry and a clean exact final worktree.
6. Freeze the repaired head on the HCA host. Run four fresh all-target Ninja builds and complete CTests, each with an isolated build directory and known HTTP-metadata/source-layout preconditions. Record exact commands, outcomes, and any baseline/environment failures.
7. Run installed C++/C/Python consumer checks, two-host RDMA task-query and failure/pressure probes, then an isolated, repeated performance comparison. Keep observed, inferred, and unverified claims separate.
8. Run changed-file formatting/pre-commit, English-comment diff scan, documentation check, independent exact-head owner/mentor review, and public-main merge preview. Update the Chinese RFC and validation note with final evidence, limitations, and a reproducible rollback/PR handoff.

No remote CI/CD job, public PR, RFC issue, or force push is part of this plan unless separately authorized.
