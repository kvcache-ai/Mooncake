---
orphan: true
---

# Adaptive Congestion Control: Startup Configuration

This module provides environment-variable parsing and validation designed for
the Classic TE and TENT RDMA adapters. The loader snapshots the environment
when `loadConfigFromEnvironment()` is called. Set
`MC_ADAPTIVE_CONGESTION_CONTROL_MODE=off` or build with
`MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=OFF` to preserve the original
path.

| Variable | Default | Description |
| --- | ---: | --- |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS` | 2 | Consecutive high-pressure epochs required before shrinking the window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS` | 3 | Consecutive low-pressure epochs required before growing the window. |

These variables accept only positive decimal integers. Epoch counts must fit
in an unsigned 32-bit integer. Invalid input makes the
shared loader return an invalid result with adaptive control disabled. An
adapter consuming this loader must log the initialization error and preserve
the original transfer behavior.

Hard-error thresholds, cooldowns, and recovery probes belong to the existing
transport health mechanisms, not the adaptive window controller. The draft's
`MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD`,
`MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS`, and
`MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` settings have been removed.
See [pressure and recovery](adaptive-rdma-control.md#pressure-and-recovery) for
the ownership boundary and migration guidance.
