---
orphan: true
---

# Adaptive Congestion Control: Shared Core and Startup Configuration

Classic TE RDMA connects the shared pressure/window controller to worker
admission and completion feedback. TENT transport integration remains a separate
follow-up. The build option and runtime mode both default to `off`.

The core tracks in-flight bytes, adapts byte windows using pressure epochs,
and fences permits by generation. It does not own failure detection,
quarantine, cooldowns, or recovery probes. Those decisions remain with the
existing transport health mechanisms. Error classification describes evidence;
it does not create a second health/recovery state machine.

## Modes and configuration

`MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL` defaults to `OFF`. When enabled,
the shared core target is available to Classic TE and TENT builds. Each Classic
RDMA worker pool snapshots the environment during construction.

| Variable | Default | Description |
| --- | ---: | --- |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MODE` | `off` | `off` bypasses core accounting and admission; `observe` records pressure without deferring transfers; `enforce` applies byte-window admission. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES` | 4194304 | Minimum byte window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES` | 67108864 | Maximum byte window; must be at least the minimum. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US` | 2000 | Target drain interval, converted to nanoseconds with overflow checking. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS` | 2 | Consecutive high-pressure epochs before shrinking the window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS` | 3 | Consecutive low-pressure epochs before growing the window. |

Numeric variables accept positive decimal integers only. Epoch counts must fit
in an unsigned 32-bit integer. Invalid input returns an invalid result with
adaptive control disabled. Classic RDMA logs the initialization error once and
preserves its original transfer behavior.

## Classic RDMA integration

Each worker pool owns a device window and a window for each peer NIC path.
`observe` accounts admitted bytes and updates pressure windows without limiting
posts. `enforce` admits a queue prefix that fits both windows; the remaining
slices stay queued on their existing posting owner. At most the admitted prefix
is submitted to the endpoint.

Successful and failed completions release permits. Unposted work also releases
its permit before retry or local-worker handoff. Endpoint and device generations
fence feedback from retired attempts. Window updates run in the existing monitor
worker. Existing rail/context health continues to control failure recovery.

With the build option disabled, adapter state and branches are compiled out.
With runtime mode `off`, workers use the original posting path. No task-status
API or TENT admission behavior is added by this integration.

## Health ownership and draft migration

The earlier draft's `MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD`,
`MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS`, and
`MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` are no longer read. Use the
existing transport's health/recovery configuration instead. Legacy health
signal fields and path-state enum values remain source-compatible, but the
shared controller does not use them to quarantine, probe, or recover a path.

TENT integration, task-scoped status APIs, and physical congestion-benefit
validation are separate follow-ups. The core microbenchmark measures only
controller overhead and is not evidence of end-to-end RDMA performance.
