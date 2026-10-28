---
orphan: true
---

# Adaptive Congestion Control: Startup Configuration

This module provides environment-variable parsing and validation designed for
the Classic TE and TENT RDMA adapters. The loader snapshots the environment
when `loadConfigFromEnvironment()` is called. Set
`MC_ADAPTIVE_CONGESTION_CONTROL_MODE=off` or build with
`MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=OFF` to bypass adaptive admission
and feedback. TENT's independent metadata-fenced rail recovery rule has its own
`transports/rdma/rail_recovery_probe_enabled` switch; set it to `false` to retain
cooldown-only recovery.

| Variable | Default | Description |
| --- | ---: | --- |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS` | 2 | Parsed for compatibility; not used by the current window policy. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS` | 3 | Parsed for compatibility; not used by the current window policy. |

These variables accept only positive decimal integers. Epoch counts must fit
in an unsigned 32-bit integer. Invalid input makes the
shared loader return an invalid result with adaptive control disabled. An
adapter consuming this loader must log the initialization error and preserve
the original transfer behavior.

Successful epochs without explicit errors now grow by the existing additive
step, regardless of ordinary backlog. Explicit congestion or receiver pressure
halves the affected window once per epoch. Minimum and maximum window settings
still apply; drain targets and high/low streak counts no longer gate these
decisions. All numeric defaults and configuration validation remain unchanged.

Hard-error thresholds, cooldowns, and recovery probes belong to the existing
transport health mechanisms, not the adaptive window controller. The draft's
`MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD`,
`MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS`, and
`MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` settings have been removed.
See [pressure and recovery](adaptive-rdma-control.md#pressure-and-recovery) for
the ownership boundary and migration guidance.
