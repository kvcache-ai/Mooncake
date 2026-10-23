---
orphan: true
---

# Adaptive Congestion Control: Shared Core and Startup Configuration

This change provides a shared pressure/window controller, an RDMA error
classifier, and environment-variable parsing for future Classic TE and TENT
adapters. It does not yet connect admission or completion feedback to either
transport, so enabling this build option alone does not throttle transfers.

The core tracks in-flight bytes, adapts byte windows using pressure epochs,
and fences permits by generation. It does not own failure detection,
quarantine, cooldowns, or recovery probes. Those decisions remain with the
existing transport health mechanisms. Error classification describes evidence;
it does not create a second health/recovery state machine.

## Modes and configuration

`MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL` defaults to `OFF`. When enabled,
the shared core target is available to Classic TE and TENT builds. The runtime
loader snapshots the environment when `loadConfigFromEnvironment()` is called.

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
adaptive control disabled; future adapters must report the initialization
error and preserve their original transfer behavior.

## Health ownership and draft migration

The earlier draft's `MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD`,
`MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS`, and
`MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` are no longer read. Use the
existing transport's health/recovery configuration instead. Legacy health
signal fields and path-state enum values remain source-compatible, but the
shared controller does not use them to quarantine, probe, or recover a path.

Transport adapters, task-scoped status APIs, and physical congestion-benefit
validation are separate follow-ups. The core microbenchmark measures only
controller overhead and is not evidence of end-to-end RDMA performance.
