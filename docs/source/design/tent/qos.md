# TENT Quality of Service (QoS)

## Overview

TENT provides Quality of Service (QoS) support to ensure that high-priority requests receive preferential treatment in multi-tenant and multi-workload environments. This document describes TENT's QoS architecture and configuration.

## Background

In shared RDMA clusters, different types of transfers have different priority requirements:

1. **Metadata and Control Messages**: Require low latency, small size
2. **Interactive Queries**: Require low to medium latency, medium size
3. **Bulk Data Transfer**: Can tolerate higher latency, large size

Without QoS, low-priority bulk transfers can monopolize bandwidth and cause high tail latency for critical requests.

TENT addresses this through:
- **Per-worker priority queues** for intra-process isolation
- **Global time-sliced coordination** for inter-process isolation
- **Priority-aware device filtering** for NUMA-aware scheduling

## Architecture

### Priority Levels

TENT supports three priority levels:

| Priority | Value | Description | Use Cases |
|----------|-------|-------------|-----------|
| `PRIO_HIGH` | 0 | High-priority requests | Metadata, control messages, latency-sensitive operations |
| `PRIO_MEDIUM` | 1 | Medium-priority requests | Interactive queries, serving workloads |
| `PRIO_LOW` | 2 | Low-priority requests | Bulk data transfer, background jobs |

### Per-Worker Priority Queues

Each worker thread maintains separate queues for each priority level:

```
┌─────────────────────────────────────┐
│         Worker Thread               │
├─────────────────────────────────────┤
│  PRIO_HIGH Queue     │              │
│  PRIO_MEDIUM Queue   │              │
│  PRIO_LOW Queue      │              │
├─────────────────────────────────────┤
│  Dequeue Priority: HIGH→MEDIUM→LOW   │
└─────────────────────────────────────┘
```

**Scheduling Logic**:
1. Always drain HIGH priority queue first
2. Only process MEDIUM when HIGH is empty
3. Only process LOW when both HIGH and MEDIUM are empty

**Priority Promotion (Anti-Starvation)**:
To prevent low-priority requests from starving indefinitely, TENT implements timeout-based priority promotion:
- MEDIUM priority requests are promoted to HIGH after waiting too long
- LOW priority requests are promoted to MEDIUM after waiting too long
- Promotion checks run periodically (every 1ms by default)

This ensures that:
- High-priority requests normally never wait behind lower-priority work
- Low-priority requests eventually get serviced even under continuous high-priority load

### Global Slot Coordination

For multi-process environments, TENT implements global time-sliced coordination using shared memory:

```
Time slices rotate every N milliseconds:

Slot 0 (0-Nms):     Only HIGH priority requests allowed
Slot 1 (N-2Nms):    MEDIUM + HIGH priority requests allowed  
Slot 2 (2N-3Nms):   All priorities allowed
...repeats...
```

**Default Configuration**: 2ms per slot (6ms full cycle)

This mechanism ensures that:
- High-priority requests get dedicated service windows
- No process can monopolize bandwidth indefinitely
- Fair access across process boundaries

### Shared Memory Structure

The global slot state is maintained in shared memory:

```cpp
struct SharedHeader {
    uint64_t magic;              // Magic number for validation
    int32_t version;             // Format version
    std::atomic<int> current_slot;  // Current global slot (0, 1, or 2)
    pthread_mutex_t global_mutex;    // For synchronization (robust)
};
```

**Operations**:
- Background thread rotates slot every N milliseconds
- Workers check `canSend()` before processing requests
- Only requests with priority ≤ slot level are processed

## Configuration

### Priority Filtering

```json
{
  "transports": {
    "rdma": {
      "enable_priority_filtering": true,
      "local_rotation_interval_us": 200,
      "priority_promotion_timeout_us": 10000
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_priority_filtering` | bool | `true` | Enable priority-based device filtering |
| `local_rotation_interval_us` | int | `200` | Local device priority rotation interval (microseconds) |
| `priority_promotion_timeout_us` | int | `10000` | Timeout for priority promotion (microseconds) |

### Global Coordination

```json
{
  "transports": {
    "rdma": {
      "slot_rotation_interval_ms": 2,
      "shared_quota_shm_path": "/mooncake_rdma_slots"
    }
  }
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `slot_rotation_interval_ms` | int | `2` | Global slot rotation interval (milliseconds) |
| `shared_quota_shm_path` | string | `""` | Shared memory path for multi-process coordination |

**Note**: Leave `shared_quota_shm_path` empty to disable global coordination (single-process mode).

## Usage Examples

### Example 1: Latency-Critical Workload

For workloads where high-priority requests must have minimal latency:

```json
{
  "transports": {
    "rdma": {
      "enable_priority_filtering": true,
      "slot_rotation_interval_ms": 1,
      "shared_quota_shm_path": "/mooncake_rdma_slots"
    }
  }
}
```

**Effect**: High-priority requests get dedicated windows every 1ms.

### Example 2: Single-Process Mode

For single-process deployments where global coordination is not needed:

```json
{
  "transports": {
    "rdma": {
      "enable_priority_filtering": true,
      "shared_quota_shm_path": ""
    }
  }
}
```

**Effect**: Per-worker priority queues only, no cross-process coordination.

### Example 3: Bulk-Friendly Configuration

For workloads where low-priority bulk transfers should not be starved:

```json
{
  "transports": {
    "rdma": {
      "slot_rotation_interval_ms": 10,
      "shared_quota_shm_path": "/mooncake_rdma_slots"
    }
  }
}
```

**Effect**: Longer slots allow more low-priority work to complete.

## Priority Assignment

### Setting Request Priority

Priority is assigned when creating `Request` objects. The default priority is `PRIO_HIGH`.

### C++ API

```cpp
// In C++
#include "tent/transfer_engine.h"

using namespace mooncake::tent;

// Create request with default priority (HIGH)
Request req;
req.opcode = Request::OpCode::READ;
req.source = buffer;
req.target_id = segment_id;
req.target_offset = 0;
req.length = size;
// req.priority is PRIO_HIGH by default

// Or specify priority explicitly
req.priority = PRIO_MEDIUM;  // or PRIO_HIGH, PRIO_LOW

// Submit the request
engine.submitTransfer(batch_id, {req});
```

### Python API

```python
# In Python
import tent

# Create request with default priority (HIGH)
req = tent.Request(
    opcode=tent.OpCode.READ,
    source=buffer_addr,
    target_id=segment_id,
    target_offset=0,
    length=size
)
# req.priority is tent.PRIO_HIGH by default

# Or specify priority in constructor
req = tent.Request(
    opcode=tent.OpCode.READ,
    source=buffer_addr,
    target_id=segment_id,
    target_offset=0,
    length=size,
    priority=tent.PRIO_LOW  # or PRIO_HIGH, PRIO_MEDIUM
)

# Or set after creation
req.priority = tent.PRIO_MEDIUM

# Submit the request
engine.submit_transfer(batch_id, [req])
```

### C API

```c
// In C
#include "tent/transfer_engine.h"

// Create request with priority
tent_request_t req = {
    .opcode = OPCODE_READ,
    .source = buffer,
    .target_id = segment_id,
    .target_offset = 0,
    .length = size,
    .priority = 0  // 0=HIGH, 1=MEDIUM, 2=LOW
};

// Submit the request
tent_submit(engine, batch_id, &req, 1);
```

## Performance Considerations

### Trade-offs

| Configuration | High-Priority Latency | Low-Priority Throughput | Fairness |
|---------------|----------------------|------------------------|----------|
| Short slot interval (1ms) | Excellent | Poor | High |
| Default slot interval (2ms) | Good | Fair | High |
| Long slot interval (10ms) | Fair | Good | Medium |
| No global coordination | Variable | Excellent | Low (per-process only) |

### Starvation Prevention

TENT prevents starvation through two mechanisms:

1. **Global slot mechanism**:
   - **HIGH priority**: Never starved (always allowed in slot 0)
   - **MEDIUM priority**: Never starved (allowed in slots 1 and 2)
   - **LOW priority**: Never starved (always allowed in slot 2)

2. **Priority promotion timeout**:
   - Low-priority requests waiting longer than `priority_promotion_timeout_us` are promoted
   - MEDIUM → HIGH promotion ensures medium priority gets service
   - LOW → MEDIUM promotion ensures low priority eventually gets service
   - Configurable via `priority_promotion_timeout_us` (default 10ms)

### Tuning Guidelines

1. **Start with default settings** (2ms slot interval)
2. **Measure tail latency** for each priority level
3. **Adjust slot interval** based on observations:
   - If HIGH priority latency is too high: decrease interval
   - If LOW priority throughput is too low: increase interval

## Troubleshooting

### Problem: High-priority requests have high latency

**Symptoms**: `PRIO_HIGH` requests experiencing unexpected delays

**Possible causes**:
1. Slot interval too long
2. Global coordination not enabled
3. Worker threads blocked on LOW priority work

**Solution**:
```json
{
  "slot_rotation_interval_ms": 1,
  "enable_priority_filtering": true
}
```

### Problem: Low-priority transfers starved

**Symptoms**: `PRIO_LOW` requests making no progress

**Possible causes**:
1. HIGH priority load is continuous
2. Slot interval too short

**Solution**: Increase slot interval to give LOW priority more time:
```json
{
  "slot_rotation_interval_ms": 10
}
```

### Problem: Shared memory creation fails

**Symptoms**: Error messages about `/mooncake_rdma_slots`

**Possible causes**:
1. Permission issues (need write access to `/dev/shm`)
2. Stale shared memory from previous run

**Solution**:
```bash
# Remove stale shared memory
rm -f /dev/shm/mooncake_rdma_slots

# Or use a different path
{
  "shared_quota_shm_path": "/mooncake_rdma_slots_v2"
}
```

## Monitoring

### Traffic Statistics

Monitor per-device traffic distribution:

```cpp
device_selector_->printTrafficStats();
```

Output example:
```
=== Device Traffic Statistics ===
Dev 0: Total=10.5 GB, EWMA BW=45.23 Gbps, Inflight=0 bytes
Dev 1: Total=8.2 GB, EWMA BW=42.18 Gbps, Inflight=0 bytes
Dev 2: Total=0.5 GB, EWMA BW=38.91 Gbps, Inflight=0 bytes
Dev 3: Total=0.3 GB, EWMA BW=39.12 Gbps, Inflight=0 bytes
```

### Priority Statistics

Monitor queue depths for each priority level (requires instrumentation).

## References

- [TENT Overview](overview.md)
- [TENT Slice Spraying](slice-spraying.md)
- [TENT C++ API](cpp-api.md)
