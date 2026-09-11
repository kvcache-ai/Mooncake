# Adaptive RDMA Congestion Control

Classic Transfer Engine and TENT share an optional software admission controller
for RDMA. It limits bytes in flight on a resolved local device and remote route
immediately before posting work. It is disabled at both build time and runtime
by default.

The controller is intended to reduce sustained queue pressure and repeated use
of unhealthy paths. It does not configure NIC congestion control, ECN, or PFC,
and it does not replace transport queues, QoS ordering, endpoint recovery, or
application timeouts. Other transport backends are unaffected by this feature.

## Build and enable

Use the normal [build instructions](../../getting_started/build.md), adding
`-DMOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=ON` to the CMake configuration. Include
`-DUSE_TENT=ON` when building the TENT backend. The setting applies to the shared
core and both RDMA adapters in a combined build.

Set `MC_ADAPTIVE_CONGESTION_CONTROL_MODE` before starting the application:

| Mode | Behavior |
| --- | --- |
| `off` (default) | Bypass adaptive admission and feedback. Existing transport behavior remains in charge. |
| `observe` | Collect feedback and compute control state without deferring or avoiding work. Existing failover remains active. |
| `enforce` | Apply device/route byte windows and quarantine decisions before RDMA post. |

For an initial observation run:

```bash
export MC_ADAPTIVE_CONGESTION_CONTROL_MODE=observe
# Start the application with its normal command and arguments.
```

Setting an environment variable has no effect when the feature was compiled
out. When compiled in, workers read configuration during construction; changing
the launching shell's environment does not reconfigure running workers.

## Optional settings

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES` | `4194304` (4 MiB) | Lower bound for the normal adaptive window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES` | `67108864` (64 MiB) | Upper bound for the normal adaptive window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US` | `2000` | Target estimated queue-drain time, in microseconds. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS` | `2` | Consecutive high-pressure epochs before shrinking the window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS` | `3` | Consecutive low-pressure epochs before growing the window. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD` | `3` | Hard errors before quarantining the path. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS` | `30000` | Quarantine cooldown, in milliseconds. |
| `MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES` | `65536` | Recovery probe window, in bytes. |

Byte and time settings must be positive decimal integers; the minimum window
must not exceed the maximum. Explicit probe windows must not exceed the minimum
window; epoch counts and the hard-error threshold must fit in 32 bits. Invalid
or overflowing settings disable the
controller and produce an initialization error message. The application still
uses its existing transport behavior. Leaving all settings unset keeps it off.

See the [Chinese startup-parameter reference](adaptive-congestion-control-startup-config.md)
for the shared TE/TENT defaults and units.

The windows control admission, not memory registration or the transport's
static queue capacity. A valid slice is indivisible at this boundary: if a
domain is empty, it may admit one slice larger than its current window. The full
slice length is reserved and prevents additional over-window admission until
it drains. This rule also applies to the smaller recovery probe window, so that
window is not an absolute transfer-size limit.

## Pressure and recovery

The shared policy uses five states: healthy, congested, suspect, quarantined,
and probing. Sustained pressure reduces the window; successful low-pressure
epochs increase it. Quarantine stops new admission on the affected path until
a cooldown permits a limited probe. Success or failure of current-generation
attempts determines the next state.

Deferral leaves work with its existing queue owner. Avoiding an unhealthy route
uses existing redispatch and failover machinery; the controller does not create
a second retry queue or rebuild QPs itself. Old-generation feedback cannot
update replacement-path health. Application completion/status APIs remain
unchanged; this feature does not add a public per-request failure-detail API.

Failure evidence must retain its scope:

- Receiver-not-ready (RNR) is pressure on the corresponding route, not evidence
  that all receivers sharing a source NIC are congested.
- Retry exhaustion is ambiguous path evidence. It does not prove permanent
  peer failure.
- Local configuration errors and remote metadata errors are not treated as
  ordinary congestion.
- Flush completions are consequences of teardown, not independent root causes.
- QP, CQ, port, and device events affect their corresponding scopes. Events for
  another port do not quarantine the current device domain.

No finite sequence of timeouts establishes that a remote host is permanently
lost. Recovery may still require transport reconnection, metadata refresh, or
operator intervention. For TENT's existing mechanisms, see
[failover](../tent/failover.md) and [QoS](../tent/qos.md).

TENT also applies a bounded rail recovery rule independently of the adaptive
controller mode. A new remote segment snapshot with the same NIC and memory
layout does not erase a paused rail's failure history. Instead, each affected
worker rail may admit one slice for that metadata generation. Endpoint
connection retries for that slice retain the same probe token; other slices
remain blocked. Only a successful RDMA work completion recovers the rail. A
failed connection, post, timeout, or non-flush completion keeps the existing
cooldown and backoff. A real layout change retains the pre-existing behavior of
rebuilding the rail map from the new topology.

The rule is enabled by default. Set the bootstrap configuration key
`transports/rdma/rail_recovery_probe_enabled` to `false` to retain the previous
cooldown-only behavior. This switch does not change wire metadata or public
completion APIs.

## Rollback and compatibility

To stop enforcement, stop admitting application work, drain outstanding
transfers, and restart with `MC_ADAPTIVE_CONGESTION_CONTROL_MODE=off`. There is no live mode-switch
API. Rebuilding with `MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=OFF` also removes the controller
code and conditional fields.

Use matching build settings for libraries and C++ consumers. The core CMake
target exports the feature definition through its usage requirements, so linked
consumer targets receive the matching conditional type layout. Manually mixing
ON/OFF objects that exchange affected transport types is unsupported; rebuild
dependent consumers when changing the compile setting. Startup `off` does not
change the compiled C++ layout.

## Performance validation

Measure compile-out, runtime off, observe, and enforce separately on the target
HCA and peer topology. Use identical message sizes, load, CPU/NUMA placement,
warm-up policy, and measurement windows. Each comparison arm must start from
equivalent process, connection, and queue state. Verify payloads, achieved
throughput, latency tails, and actual controller activity; also test incast and
recovery. A healthy-path microbenchmark alone does not prove congestion benefit.

With unit tests enabled, build `adaptive_congestion_control_benchmark` to measure
the shared CPU gate. It accepts one mode argument: `baseline`, `off`, `observe`,
or `enforce`. Its baseline is a synthetic CPU loop, not an unmodified TE/TENT
data path. Reported `p99_ns_per_op` is the tail of round-average operation times,
not request p99 latency.

Hardware-free tests and Soft-RoCE do not establish physical-HCA throughput,
fabric congestion behavior, or a no-regression guarantee. Keep enforcement off
until the relevant hardware and workload pass the deployment's acceptance
criteria.
