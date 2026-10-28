# Large working set follow-up: still below 400 GB/s

This directory preserves the **measured experimental patches**, not a port of
these extra changes into the current production source. Do not apply them to
this PR head. The earlier rolling-window and poll-notification changes are
already implemented in the PR. These additional patches depend on the archived
notify-stage source based on `12a7e9f10a41d578364a58c1d94fd3c5a3a33d6f`.

See [the comparison and commands](../../rdma_large_working_set.md#follow-up-explicit-affinity-and-completion-polling).
Full archived source, stage binaries, raw commands/results, contract tests and
software CPU profiles are delivered with the experiment handoff archive.
The minimal public patch series is reviewable here. All new flags default off.

| Apply order | Code | Purpose |
| --- | --- | --- |
| 1. `pin.patch` | `te_backend.cpp`, `worker_pool.cpp` | Pin one submission thread and worker per NIC, narrowing the inherited authorized NUMA affinity. |
| 2. `fast.patch` | `te_backend.cpp`, `transfer_engine_impl.h` | Precheck the owned batch completion counters while waiting; final status/error/cleanup uses the official API. Skip the notification map lock only before notifications have ever been used; a monotonic flag restores the original locking path. |
| 3. `source-prefetch.patch` | `te_backend.cpp` | Experimental full-range source prefetch on Write only. Negative result; disabled in the final candidate. Read verification remains supported. |

Prepare the archived baseline in a separate directory: extract the original
source snapshot, then apply `thread-nics.patch`, `rolling-window.patch`, and
`poll-notify.patch` from the previous large-set handoff archive, in that order.
`build_reproduce_notify.sh` in that archive records the extraction/build recipe.
The final candidate was built with all three patches above, with source prefetch
disabled at runtime. Verify the input source and apply this series:

```bash
bash apply.sh /absolute/disposable/notify-stage-src
# Build classic CPU TE in this source directory with USE_MLX5DV=ON.
# Use the previous archive's Linux dependency/build recipe; do not reuse a
# stale CMake cache. Deploy the same runtime to both hosts and check ldd.
```

`apply.sh` checks the exact SHA256 of all touched baseline files before applying.
On macOS use `shasum -a 256 -c base-files.sha256` if verifying manually.
The patch chain was replayed and final files compared byte-for-byte locally;
the archived candidate was actually built on Linux and benchmarked on RDMA.
That does **not** validate the current PR head on Linux/RDMA.

`run_te.sh` is the portable, environment-isolated command wrapper. Set authorized
CPU lists and verify local NIC/NUMA topology; examples in the linked guide.
The wrapper has Bash syntax validation; original measured per-process argv/env
are preserved in the handoff archive. Do not interpret the 8 MiB in-flight
window per thread as the active address range: all 1 GiB/thread is traversed.
