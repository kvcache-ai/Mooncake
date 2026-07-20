# TENT Transport Selector

## Overview

The Transport Selector is responsible for choosing the optimal transport and devices for each transfer request based on configuration policies.

## Request Priority

TENT uses a unified priority system across all components. See [QoS.md](qos.md) for detailed description of priority levels and their usage throughout the system.

Quick reference:
- `"high"` / `0` - High-priority requests (metadata, control, latency-sensitive)
- `"medium"` / `1` - Medium-priority requests (interactive queries, serving)
- `"low"` / `2` - Low-priority requests (bulk transfer, background jobs)

## Configuration-Based Transport Selection

Transport selection is driven by configuration with pattern-based rules.

### Configuration Example

```json
{
  "policy": [
    {
      "name": "high_prio_fast",
      "segment_type": "memory",
      "priority": "high",
      "devices": ["mlx5_0", "mlx5_1", "mlx5_2"],
      "transports": ["nvlink", "rdma", "shm"]
    },
    {
      "name": "low_prio_slow",
      "segment_type": "memory",
      "priority": "low",
      "devices": ["mlx5_0"],
      "transports": ["rdma", "tcp"]
    },
    {
      "name": "file_storage",
      "segment_type": "file",
      "transports": ["gds", "io_uring", "rdma"]
    }
  ]
}
```

### Policy Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Policy identifier (for logging) |
| `segment_type` | string | Yes | `"memory"` or `"file"` |
| `priority` | string or int | No | Match only requests with this priority: `"high"` (0), `"medium"` (1), `"low"` (2) |
| `devices` | array[string] | No | List of allowed device names (empty = all devices) |
| `transports` | array[string] | No | Transport preference list (evaluated in order) |

### Memory Type Filters

For `memory` segments, you can filter by source/destination memory type:

| Pattern | Matches |
|---------|---------|
| `"cuda"` | CUDA GPU memory |
| `"cpu"` | CPU/host memory |
| `"hip"` | ROCm/HIP GPU memory |
| `"npu"` | Ascend NPU memory |
| `"*"` | Any memory type |

### Size Filters

Restrict policies to specific transfer sizes:

```json
{
  "name": "small_transfers",
  "segment_type": "memory",
  "min_size": 0,
  "max_size": 1048576,
  "transports": ["shm", "nvlink"]
}
```

## Device Mask

The `devices` field in a policy creates a bitmask that restricts which NICs can be used:

```json
{
  "name": "use_nic_0_only",
  "segment_type": "memory",
  "devices": ["mlx5_0"],
  "transports": ["rdma"]
}
```

This is translated internally to a 64-bit bitmask where each bit represents one device:
- `devices: ["mlx5_0"]` → `device_mask = 0x0001` (bit 0 set)
- `devices: ["mlx5_1", "mlx5_2"]` → `device_mask = 0x0006` (bits 1 and 2 set)
- `devices: []` (empty) → `device_mask = ~0ULL` (all devices)

## Transport Fallback

When multiple transports are listed in the `transports` array, they act as fallback options:

```json
{
  "transports": ["rdma", "tcp"]
}
```

This means:
1. Try RDMA first
2. If RDMA is unavailable or fails, fall back to TCP

### Fallback with `transport_index`

For programmatic control, the `transport_index` parameter selects which transport to use:

```cpp
// transport_index = 0 → First transport (rdma)
// transport_index = 1 → Second transport (tcp)
// transport_index = 2 → Third transport (if exists)
auto result = selector.select(context, transports, transport_index);
```

## Per-request override (`transport_hint`)

`Request::transport_hint` lets a caller bypass the policy lookup for one request at a time. It's the per-request analogue of `transport_index` and sits on top of the configured policies, so callers can keep the global policy config as the default while still pinning specific requests.

```cpp
Request r{};
r.opcode = Request::WRITE;
r.source = local_ptr;
r.target_id = seg;
r.target_offset = 0;
r.length = 4096;
r.transport_hint = TransportType::TCP;   // UNSPEC (default) defers to policy
engine.submitTransfer(batch_id, {r});
```

### Semantics

* **`UNSPEC` (default)**: unchanged — `TransportSelector::select(...)` runs as documented above.
* **Pinned**: the request's first attempt uses the hinted transport. On failover (when enabled), the engine asks the selector for the next candidate **with the hint excluded**, so failover never loops back onto the transport that already failed.

## Default Behavior

If no `policy` is configured, TENT falls back to original behavior:

| Segment Type | Default Transport Order |
|--------------|-------------------------|
| File | GDS → IOURING → RDMA |
| Memory | Uses `buffer_transports` order from buffer registration |

## Complete Example

```json
{
  "policy": [
    {
      "name": "high_priority_local",
      "segment_type": "memory",
      "same_machine": true,
      "priority": "high",
      "transports": ["nvlink", "shm"]
    },
    {
      "name": "high_priority_remote",
      "segment_type": "memory",
      "same_machine": false,
      "priority": "high",
      "local_memory": "cuda",
      "remote_memory": "cuda",
      "devices": ["mlx5_0", "mlx5_1", "mlx5_2"],
      "transports": ["rdma"]
    },
    {
      "name": "medium_priority",
      "segment_type": "memory",
      "priority": "medium",
      "transports": ["rdma"]
    },
    {
      "name": "bulk_transfer",
      "segment_type": "memory",
      "priority": "low",
      "min_size": 104857600,
      "transports": ["rdma", "tcp"]
    },
    {
      "name": "file_ops",
      "segment_type": "file",
      "transports": ["gds", "io_uring"]
    }
  ]
}
```

## Unified Priority System

The priority value propagates through multiple layers:

```
Request.priority (application layer)
    ↓
TransportSelector policy matching
    ↓
DeviceSelector allocation (QoS filtering)
    ↓
Worker thread scheduling queues
```

**Layer interactions**:

1. **TransportSelector**: Policy's `priority` field filters which requests match
2. **DeviceSelector**: `priority` parameter controls device eligibility (QoS mode)
3. **Workers**: Separate queues per priority level with strict draining order

See [QoS.md](qos.md) for details on worker scheduling and global slot coordination.

## Data Flow

```
Request with priority
    ↓
TransportSelector.select(context, transports, transport_index)
    ↓
Match policy by:
  - segment_type (file/memory)
  - priority (exact match if specified in policy)
  - location constraints
  - size constraints
    ↓
Build device_mask from policy.devices
    ↓
Select transport from policy.transports[transport_index]
    ↓
Return SelectionResult { transport, device_mask }
    ↓
RdmaTransport.submitTransferTasks(batch, requests)
    ↓
DeviceSelector.allocate(..., request.priority, batch.device_mask)
    ↓
Worker scheduling (separate queues per priority)
```
