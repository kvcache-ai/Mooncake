# TENT Slice Spraying

## Overview

This document describes TENT's Slice Spraying mechanism, which enables efficient data movement in multi-rail RDMA environments through intelligent device selection and adaptive load balancing.

## Background

In multi-rail RDMA environments, naive round-robin striping leads to suboptimal performance because:

1. **NUMA Effects**: Cross-NUMA access incurs additional latency and reduces effective bandwidth
2. **Load Imbalance**: Static striping cannot adapt to dynamic load conditions
3. **Heterogeneous Link Quality**: Different rails may have different effective bandwidth due to congestion or hardware characteristics

TENT addresses these issues through:
- **NUMA-aware device selection** with configurable penalties
- **EWMA-based bandwidth estimation** for adaptive load balancing
- **Dynamic multi-path allocation** for large transfers

## Architecture

### Device Selector

The `DeviceSelector` component is responsible for choosing which RDMA device(s) to use for each transfer request. It operates in two modes:

#### Baseline Mode (Round-Robin)

When `enable_smart_scheduling = false`, the selector uses simple round-robin within the highest-priority device tier (typically local NUMA devices):

```
For each request:
  1. Find first non-empty device tier (local NUMA preferred)
  2. Select devices round-robin within that tier
  3. Ignore lower-priority tiers
```

**Characteristics**:
- Deterministic behavior
- No runtime overhead for tracking
- Consistent with original TE behavior
- Does not adapt to load conditions

#### Smart Mode (EWMA-Based Selection)

When `enable_smart_scheduling = true`, the selector uses an EWMA-based algorithm:

```
For each request:
  1. Calculate predicted completion time for each device:
     predicted_time = (inflight_bytes + slice_bytes) / ewma_bandwidth

  2. Apply NUMA penalty based on tier:
     score = predicted_time × numa_tier_weights[tier]

  3. Select device(s) with minimum score:
     - Single slice: best device only
     - Multiple slices: weighted distribution across devices

  4. Update EWMA bandwidth on completion:
     ewma_bandwidth = α × ewma_bandwidth + (1 - α) × observed_bandwidth
     where α = bandwidth_learning_rate
```

**Characteristics**:
- Adapts to changing load conditions
- Prefers local NUMA devices
- Spreads load across multiple rails
- Higher runtime overhead

### NUMA-Aware Selection

Devices are organized into tiers based on NUMA distance:

| Tier | Description | Default Penalty |
|------|-------------|-----------------|
| Rank 0 | Local NUMA | 1.0 (baseline) |
| Rank 1 | Remote NUMA (tier 1) | 5.0 |
| Rank 2 | Remote NUMA (tier 2) | 10.0 |

The penalty is applied as a multiplier to predicted completion time, making remote devices less attractive unless local devices are heavily loaded.

### EWMA Bandwidth Estimation

Each device maintains an EWMA (Exponentially Weighted Moving Average) of its effective bandwidth:

```
initial_value = theoretical_bandwidth

on_transfer_complete:
  observed_bandwidth = transfer_size / transfer_time
  ewma_bandwidth = α × ewma_bandwidth + (1 - α) × observed_bandwidth
  ewma_bandwidth = clamp(ewma_bandwidth,
                        0.1 × theoretical,
                        10.0 × theoretical)
```

where `α = bandwidth_learning_rate`.

**Note on terminology**: The EWMA formula uses α as the coefficient for the old value. Therefore:
- **Lower α** (closer to 0) → more weight on new observations → **faster adaptation**
- **Higher α** (closer to 1) → more weight on old value → **slower adaptation**

Examples:
- α = 0: `ewma_bandwidth = observed_bandwidth` (full adaptation, always use new value)
- α = 1: `ewma_bandwidth = ewma_bandwidth` (no learning, never update)
- α = 0.01: `ewma_bandwidth = 0.01 × old + 0.99 × new` (default, gradual adaptation)

The EWMA provides:
- **Memory**: Recent observations have more influence than old ones
- **Stability**: Smooths out transient fluctuations
- **Adaptability**: Tracks gradual changes in link quality

### Multi-Path Allocation

For large transfers, TENT distributes slices across multiple devices:

**Single Path** (small requests):
- All slices go to the single best device
- Minimizes coordination overhead

**Multi Path** (large requests):
- **Normal mode** (99% of calls): Slices distributed proportionally to device capacity
  - Each device gets: `(device_weight / total_weight) × num_slices`
  - Remaining slices assigned to best device
- **Probe mode** (1% of calls, every 100th call): Slices distributed round-robin
  - Purpose: Ensure all devices are continuously sampled for EWMA updates
  - Prevents EWMA starvation for less-used devices

### Request Flow

```
┌──────────────┐
│ Application  │
└──────┬───────┘
       │ submitTransfer()
       ▼
┌──────────────────────────────────────┐
│  RdmaTransport::submitTransferTasks  │
│  - Split large requests into slices   │
│  - Call DeviceSelector for allocation │
│  - Only if num_slices >= max_slice_count/2 │
└──────┬───────────────────────────────┘
       │
       ▼
┌──────────────────────────────────────┐
│     DeviceSelector::allocate         │
│  ┌────────────────────────────────┐  │
│  │ smart_selection_enabled?       │  │
│  └────┬──────────────────────┬────┘  │
│       │ Yes                  │ No     │
│       ▼                     ▼        │
│  ┌─────────┐          ┌─────────┐   │
│  │  Smart  │          │ Baseline│   │
│  │  Mode   │          │   Mode  │   │
│  └────┬────┘          └────┬────┘   │
│       │                    │         │
│       └────────┬───────────┘         │
│                ▼                     │
│  ┌────────────────────────────────┐  │
│  │  Return slice_dev_ids          │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
```

## Configuration

All slice spraying parameters are configurable via the configuration file:

### Core Scheduling

```json
{
  "transports": {
    "rdma": {
      "enable_smart_scheduling": true
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_smart_scheduling` | bool | `true` | Enable EWMA-based selection (false = round-robin) |

### NUMA Penalties

```json
{
  "transports": {
    "rdma": {
      "numa_penalties": [1.0, 5.0, 10.0]
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `numa_penalties` | array[float] | `[1.0, 5.0, 10.0]` | Penalty multipliers for each NUMA tier |

**Guidelines**:
- Higher values = stronger preference for local devices
- Set all to `1.0` to disable NUMA awareness
- Increase remote penalties if cross-NUMA latency is high

### Bandwidth Estimation

```json
{
  "transports": {
    "rdma": {
      "bandwidth_learning_rate": 0.01,
      "ewma_min_bandwidth_multiplier": 0.1,
      "ewma_max_bandwidth_multiplier": 10.0
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `bandwidth_learning_rate` | float | `0.01` | EWMA learning rate (0.0 = full adaptation, 1.0 = no learning) |
| `ewma_min_bandwidth_multiplier` | float | `0.1` | Minimum bandwidth as fraction of theoretical |
| `ewma_max_bandwidth_multiplier` | float | `10.0` | Maximum bandwidth as fraction of theoretical |

**Guidelines**:
- Lower α (e.g., 0.001) → faster adaptation, more volatile → responds quickly to changes
- Higher α (e.g., 0.1) → slower adaptation, more stable → smooths out transient fluctuations
- Default α = 0.01 provides balanced adaptation
- Multipliers constrain EWMA to reasonable range [0.1×, 10.0×] of theoretical bandwidth

### Device Selection Scoring

```json
{
  "transports": {
    "rdma": {
      "score_jitter_range": 1e-9,
      "score_epsilon": 1e-12
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `score_jitter_range` | float | `1e-9` | Random jitter range to avoid deterministic selection |
| `score_epsilon` | float | `1e-12` | Small value to prevent division by zero |

### Bandwidth Constants

```json
{
  "transports": {
    "rdma": {
      "default_bandwidth_gbps": 400.0,
      "min_bandwidth_gbps": 10.0,
      "max_bandwidth_gbps": 800.0
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `default_bandwidth_gbps` | float | `400.0` | Default NIC bandwidth when topology info unavailable |
| `min_bandwidth_gbps` | float | `10.0` | Minimum valid NIC bandwidth (Gbps) |
| `max_bandwidth_gbps` | float | `800.0` | Maximum valid NIC bandwidth (Gbps) |

**Notes**:
- These constants define the valid range and default for device bandwidth
- Used in EWMA calculations and theoretical bandwidth estimation
- If a device's reported bandwidth is outside [min, max], default_bandwidth is used

## Usage Examples

### Example 1: Latency-Sensitive Workload

For latency-sensitive queries where local NUMA access is critical:

```json
{
  "transports": {
    "rdma": {
      "enable_smart_scheduling": true,
      "numa_penalties": [1.0, 100.0, 1000.0],
      "bandwidth_learning_rate": 0.001
    }
  }
}
```

**Effect**: Strongly prefers local devices, slow adaptation for stability.

### Example 2: Bulk Data Transfer

For bulk transfers where throughput is more important than latency:

```json
{
  "transports": {
    "rdma": {
      "enable_smart_scheduling": true,
      "numa_penalties": [1.0, 2.0, 3.0],
      "bandwidth_learning_rate": 0.1
    }
  }
}
```

**Effect**: Allows cross-NUMA transfers, fast adaptation to load.

### Example 3: Baseline Mode

For deterministic performance matching original TE:

```json
{
  "transports": {
    "rdma": {
      "enable_smart_scheduling": false
    }
  }
}
```

**Effect**: Round-robin within local NUMA tier, no adaptation, minimal overhead.

## Performance Considerations

### Overhead Comparison

| Mode | CPU Overhead | Adaptability | NUMA Awareness |
|------|--------------|--------------|----------------|
| Baseline | Minimal | None | Tier-based (static) |
| Smart | Moderate | EWMA-based | Dynamic + penalty |

### When to Use Each Mode

**Use Baseline Mode when**:
- Workload is uniform and predictable
- Deterministic performance is required
- CPU overhead must be minimized
- All devices are in same NUMA node

**Use Smart Mode when**:
- Workload is heterogeneous
- Link quality varies over time
- NUMA effects are significant
- Maximum throughput is desired

### Tuning Guidelines

1. **Start with baseline mode** to establish performance baseline
2. **Enable smart mode** with conservative parameters:
   - `numa_penalties = [1.0, 2.0, 5.0]`
   - `bandwidth_learning_rate = 0.01`
3. **Monitor performance** and adjust based on observations:
   - If cross-NUMA transfers are too frequent: increase remote penalties
   - If adaptation is too slow (EWMA not keeping up with load changes): decrease α
   - If performance is unstable (too much fluctuation): increase α

## Troubleshooting

### Problem: All requests go to cross-NUMA devices

**Symptoms**: Poor performance, high latency

**Diagnosis**:
```cpp
device_selector_->printTrafficStats();
```

**Solution**: Check `numa_penalties` configuration. Ensure local devices have lowest penalty (1.0).

### Problem: Performance worse than baseline

**Symptoms**: Smart mode slower than baseline mode

**Possible causes**:
1. Learning rate too high (volatile decisions)
2. NUMA penalties too low (not preferring local)
3. Score jitter too large (too much randomness)

**Solution**: Use more conservative:
```json
{
  "bandwidth_learning_rate": 0.001,
  "numa_penalties": [1.0, 10.0, 100.0],
  "score_jitter_range": 1e-12
}
```

## References

- [TENT Overview](../overview.md)
- [TENT QoS](../features/qos.md)
- [TENT C++ API](../api/cpp-api.md)
