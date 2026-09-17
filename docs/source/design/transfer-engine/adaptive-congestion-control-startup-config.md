---
orphan: true
---

# Adaptive Congestion Control: Startup Configuration

This module provides environment-variable parsing and validation shared by the
Classic TE and TENT RDMA adapters. After the follow-up adapter integration,
workers will read this configuration during construction; changing the
environment at runtime will not reload it. Set
`MC_ADAPTIVE_CONGESTION_CONTROL_MODE=off` or build with
`MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=OFF` to preserve the original
path.

| Variable | Default | Description |
| --- | ---: | --- |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS` | 2 | Consecutive high-pressure epochs required before shrinking the window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS` | 3 | Consecutive low-pressure epochs required before growing the window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD` | 3 | Hard-error count required before quarantining the path. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS` | 30000 | Quarantine cooldown in milliseconds. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` | 65536 | Byte window used for recovery probes. |

These five variables accept only positive decimal integers. Epoch counts and
the hard-error threshold must fit in an unsigned 32-bit integer, and the
cooldown must fit in an unsigned 64-bit integer after conversion to
nanoseconds. An explicitly configured probe window must not exceed
`MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES`. Invalid input makes the
shared loader return an invalid result with adaptive control disabled.
Follow-up adapters should log the initialization error and preserve the
original transfer behavior.
