---
orphan: true
---

# Adaptive Congestion Control: Shared Core and Startup Configuration

Classic TE and TENT RDMA connect the shared pressure/window controller to
posting admission and completion feedback. The build option and runtime mode
both default to `off`.

The core tracks in-flight bytes, adapts byte windows using completion feedback,
and fences permits by generation. It does not own failure detection,
quarantine, cooldowns, or recovery probes. Those decisions remain with the
existing transport health mechanisms. Error classification describes evidence;
it does not create a second health/recovery state machine.

## Modes and configuration

`MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL` defaults to `OFF`. When enabled,
the shared core target is available to Classic TE and TENT builds. Classic RDMA
worker pools and the TENT RDMA transport snapshot the same environment
configuration during construction.

Installed headers read this build setting from `mooncake_te_build_config.h` so
their public layouts match the library. Consumers do not need to define the
macro themselves; the generated header overrides conflicting definitions.

| Variable | Default | Description |
| --- | ---: | --- |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MODE` | `off` | `off` bypasses core accounting and admission; `observe` records pressure without deferring transfers; `enforce` applies byte-window admission. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES` | 4194304 | Minimum byte window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES` | 67108864 | Maximum byte window; must be at least the minimum. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US` | 2000 | Retained and validated for configuration compatibility; not used by window decisions. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS` | 2 | Retained and validated for configuration compatibility; explicit receiver pressure reduces the window in the current epoch. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS` | 3 | Retained and validated for configuration compatibility; clean completions allow growth in the current epoch. |

Numeric variables accept positive decimal integers only. Epoch counts must fit
in an unsigned 32-bit integer. Invalid input returns an invalid result with
adaptive control disabled. Classic and TENT RDMA log the initialization error
once and preserve their original transfer behavior.

## Classic RDMA integration

Each worker pool owns a device window and a window for each peer NIC path.
`observe` accounts admitted bytes and updates pressure windows without limiting
posts. `enforce` admits a queue prefix that fits both windows; the remaining
slices stay queued on their existing posting owner. At most the admitted prefix
is submitted to the endpoint.

In enforce mode, a partially admitted queue on an already-active single route
replenishes in groups bounded by the configured minimum window, without
splitting slices. Unsubmitted permits are returned. Idle routes, fully admitted
tails, mixed-route queues, and off/observe modes do not wait for a group. This
only changes posting granularity; it adds no configuration or recovery gate.

Successful and failed completions release permits. Unposted work also releases
its permit before retry or local-worker handoff. Endpoint and device generations
fence feedback from retired attempts. Window updates run in the existing monitor
worker. Existing rail/context health continues to control failure recovery.

Ordinary backlog and estimated drain time do not reduce windows or veto growth.
Each monitor epoch with successful completions and no explicit error grows the
window by one quarter of the configured minimum, up to the configured maximum.
Explicit receiver pressure halves it, bounded by the minimum. Root errors veto
growth for the addressed domain in that epoch; peer-local errors do not veto
healthy sibling routes on the device. Errors do not create another recovery
state machine. Idle epochs do not grow the window.

With the build option disabled, adapter state and branches are compiled out.
With runtime mode `off`, workers use the original posting path. No task-status
API is added by this integration.

## TENT RDMA integration

TENT uses the same shared controller and configuration as Classic RDMA. The
transport owns one domain for every local NIC and a route domain for each
resolved local-NIC/peer-NIC path. All workers posting to the same physical
resource share its domain. Admission runs after TENT has selected the path and
obtained the endpoint, immediately before `ibv_post_send`. A denied slice
remains in its existing worker-owned path queue; the adapter creates no second
queue and does not split or merge requests.

When QP capacity accepts only part of an admitted prefix, permits for the
unsubmitted tail are returned in the same worker pass and reacquired later.
This keeps the byte window tied to work submitted to the QP rather than queued
work. A physical CQ completion returns the corresponding posted permit. Local
post rejection, cancellation before posting, endpoint replacement, and slice
teardown return an unposted permit without reporting network pressure.

Endpoint replacement advances a separate binding generation, while
local-device recovery advances the shared core's device generation. Late
completions still settle their old byte reservation, but cannot update the
replacement endpoint's feedback. TENT's existing `RailMonitor`, endpoint
lifecycle, retry logic, and device selector remain responsible for path
availability and recovery.

## Health ownership and draft migration

The earlier draft's `MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD`,
`MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS`, and
`MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` are no longer read. Use the
existing transport's health/recovery configuration instead. Legacy health
signal fields and path-state enum values remain source-compatible, but the
shared controller does not use them to quarantine, probe, or recover a path.

Task-scoped status APIs and physical congestion-benefit validation remain
separate follow-ups. The core microbenchmark measures only controller overhead
and is not evidence of end-to-end RDMA performance.
