# TENT Configuration Reference

This document provides a complete reference for all configuration options in `transfer-engine.json`.

## Configuration File Location

TENT looks for the configuration file in the following locations (in order of priority):

1. Path specified via `Config` constructor: `TransferEngine("/path/to/config.json")`
2. Path specified via environment variable: `MC_CONFIG_FILE=/path/to/config.json`
3. Default location: `./transfer-engine.json`

## Configuration Structure

```json
{
  "local_segment_name": "",
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379",
  "rpc_server_hostname": "127.0.0.1",
  "rpc_server_port": 0,
  "topology": { ... },
  "log_level": "warning",
  "max_failover_attempts": 3,
  "enable_auto_failover_on_poll": true,
  "enable_progress_worker": false,
  "metrics": { ... },
  "transports": { ... },
  "policy": [ ... ]
}
```

## Top-Level Configuration

### `local_segment_name`

- **Type**: `string`
- **Default**: `""` (auto-generated from hostname)
- **Description**: The name of the local segment. If empty, TENT will automatically generate a name based on the local hostname. This name must be unique across all TENT instances in the cluster.

### `metadata_type`

- **Type**: `string`
- **Default**: `"p2p"`
- **Valid Values**: `"p2p"`, `"etcd"`, `"redis"`, `"http"`
- **Description**: The type of metadata service to use for segment discovery and coordination.

| Value | Description |
|-------|-------------|
| `p2p` | Peer-to-peer mode using RPC-based control plane |
| `etcd` | Use etcd as metadata store |
| `redis` | Use Redis as metadata store |
| `http` | Use HTTP-based metadata server |

### `metadata_servers`

- **Type**: `string`
- **Default**: `"127.0.0.1:2379"`
- **Description**: Comma-separated list of metadata server addresses. Format depends on `metadata_type`:
  - `etcd`: `"host1:2379,host2:2379"`
  - `redis`: `"host1:6379,host2:6379"`
  - `http`: `"http://host1:8080,http://host2:8080"`

### `rpc_server_hostname`

- **Type**: `string`
- **Default**: `"127.0.0.1"`
- **Description**: The hostname or IP address that the RPC server will bind to. In P2P mode, this is the address other peers will use to connect to this instance.

### `rpc_server_port`

- **Type**: `integer`
- **Default**: `0` (auto-assign)
- **Description**: The port number for the RPC server. If set to 0, TENT will automatically assign an available port. Use `getRpcServerPort()` to retrieve the assigned port.

### `topology`

- **Type**: `object`
- **Description**: Network topology configuration for RDMA device selection.

#### `topology.rdma_whitelist`

- **Type**: `array[string]`
- **Default**: `[]` (use all devices)
- **Description**: List of RDMA device names to use. Only devices in this list will be used for data transfer.

```json
"rdma_whitelist": ["mlx5_0", "mlx5_2"]
```

#### `topology.rdma_blacklist`

- **Type**: `array[string]`
- **Default**: `[]`
- **Description**: List of RDMA device names to exclude. Devices in this list will not be used even if they are in the whitelist.

### `log_level`

- **Type**: `string`
- **Default**: `"warning"`
- **Valid Values**: `"trace"`, `"debug"`, `"info"`, `"warning"`, `"error"`, `"critical"`, `"off"`
- **Description**: The logging level for TENT internal logging.

### `max_failover_attempts`

- **Type**: `integer`
- **Default**: `3`
- **Range**: `0` to `10`
- **Description**: Maximum number of transport failover attempts per transfer request. If all failover attempts are exhausted, the request will return a FAILED status.

### `enable_auto_failover_on_poll`

- **Type**: `boolean`
- **Default**: `true`
- **Description**: When enabled, `getTransferStatus()` will automatically trigger failover for failed tasks. When disabled, you must explicitly call `progressBatch()` to handle failover.

### `enable_progress_worker`

- **Type**: `boolean`
- **Default**: `false`
- **Description**: When enabled, a background worker thread will continuously poll batch status and handle failover automatically. This is useful for fire-and-forget workloads.

## Metrics Configuration

### `metrics.enabled`

- **Type**: `boolean`
- **Default**: `true`
- **Description**: Enable or disable metrics collection. Note: Metrics must also be enabled at compile time with `-DTENT_METRICS_ENABLED=ON`.

### `metrics.http_port`

- **Type**: `integer`
- **Default**: `9100`
- **Description**: HTTP port for the metrics endpoint. Prometheus can scrape metrics from `http://<host>:<port>/metrics`.

### `metrics.http_host`

- **Type**: `string`
- **Default**: `"0.0.0.0"`
- **Description**: Host address to bind the metrics HTTP server to.

### `metrics.http_server_threads`

- **Type**: `integer`
- **Default**: `2`
- **Description**: Number of worker threads for the metrics HTTP server.

### `metrics.report_interval_seconds`

- **Type**: `integer`
- **Default**: `30`
- **Description**: Interval in seconds between metrics report updates.

### `metrics.enable_prometheus`

- **Type**: `boolean`
- **Default**: `true`
- **Description**: Enable Prometheus-formatted metrics output.

### `metrics.enable_json`

- **Type**: `boolean`
- **Default**: `true`
- **Description**: Enable JSON-formatted metrics output.

### `metrics.latency_buckets`

- **Type**: `array[number]`
- **Default**: `[0.000125, 0.00015, 0.0002, 0.00025, 0.0003, 0.0004, 0.0005, 0.00075, 0.001, 0.0015, 0.002, 0.003, 0.005, 0.007, 0.015, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0]`
- **Description**: Histogram buckets for transfer latency metrics (in seconds). Adjust based on your expected latency distribution.

### `metrics.size_buckets`

- **Type**: `array[number]`
- **Default**: `[1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824]`
- **Description**: Histogram buckets for transfer size metrics (in bytes).

## Transport Configuration

Each transport has its own configuration section under `transports`.

### RDMA Transport

```json
"transports": {
  "rdma": {
    "enable": true,
    "shared_quota_shm_path": "mooncake_quota_shm",
    "max_timeout_ns": 10000000000,
    "num_lanes": 1,
    "device": { ... },
    "endpoint": { ... },
    "workers": { ... }
  }
}
```

#### `rdma.enable`

- **Type**: `boolean`
- **Default**: `true`
- **Description**: Enable or disable the RDMA transport.

#### `rdma.shared_quota_shm_path`

- **Type**: `string`
- **Default**: `"mooncake_quota_shm"`
- **Description**: Shared memory path name for cross-process quota coordination. Multiple TENT instances on the same host use this to coordinate RDMA device usage.

#### `rdma.max_timeout_ns`

- **Type**: `integer`
- **Default**: `10000000000` (10 seconds)
- **Description**: Maximum timeout in nanoseconds for RDMA operations before they are considered failed.

#### `rdma.num_lanes`

- **Type**: `integer`
- **Default**: `1`
- **Description**: Number of parallel lanes for RDMA operations. Experimental feature for multi-path transfers.

##### RDMA Device Configuration

```json
"device": {
  "num_comp_channels": 1,
  "port": 1,
  "gid_index": 0,
  "max_cqe": 4096
}
```

###### `device.num_comp_channels`

- **Type**: `integer`
- **Default**: `1`
- **Description**: Number of completion channels per RDMA context.

###### `device.port`

- **Type**: `integer`
- **Default**: `1`
- **Description**: RDMA device port number to use.

###### `device.gid_index`

- **Type**: `integer`
- **Default**: `0`
- **Description**: GID index for RoCE connections.

###### `device.max_cqe`

- **Type**: `integer`
- **Default**: `4096`
- **Description**: Maximum number of completion queue entries. Larger values allow more outstanding operations but consume more memory.

##### RDMA Endpoint Configuration

```json
"endpoint": {
  "endpoint_store_cap": 256,
  "max_sge": 4,
  "max_qp_wr": 256,
  "max_inline_bytes": 64,
  "path_mtu": 4096,
  "pkey_index": 0,
  "hop_limit": 16,
  "flow_label": 0,
  "traffic_class": 0,
  "service_level": 0,
  "src_path_bits": 0,
  "static_rate": 0,
  "rq_psn": 0,
  "max_dest_rd_atomic": 16,
  "min_rnr_timer": 12,
  "sq_psn": 0,
  "send_timeout": 14,
  "send_retry_count": 7,
  "send_rnr_count": 7,
  "max_rd_atomic": 16
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `endpoint_store_cap` | int | 256 | Maximum number of cached endpoints |
| `max_sge` | int | 4 | Maximum scatter-gather entries per request |
| `max_qp_wr` | int | 256 | Maximum work requests per queue pair |
| `max_inline_bytes` | int | 64 | Maximum inline data size |
| `path_mtu` | int | 4096 | Path MTU for RoCE |
| `pkey_index` | int | 0 | Partition key index |
| `hop_limit` | int | 16 | Hop limit for RoCEv2 |
| `flow_label` | int | 0 | Flow label for IPv6 traffic |
| `traffic_class` | int | 0 | Traffic class (DS field) |
| `service_level` | int | 0 | Service level (SL) |
| `src_path_bits` | int | 0 | Source path bits |
| `static_rate` | int | 0 | Static rate limit |
| `rq_psn` | int | 0 | Receive queue packet sequence number |
| `max_dest_rd_atomic` | int | 16 | Maximum destination RDMA atomic operations |
| `min_rnr_timer` | int | 12 | Minimum receiver not ready timer |
| `sq_psn` | int | 0 | Send queue packet sequence number |
| `send_timeout` | int | 14 | Send timeout (0.022us × 2^value) |
| `send_retry_count` | int | 7 | Maximum send retry count |
| `send_rnr_count` | int | 7 | Maximum RNR retry count |
| `max_rd_atomic` | int | 16 | Maximum RDMA atomic operations |

##### RDMA Workers Configuration

```json
"workers": {
  "max_retry_count": 8,
  "block_size": 65536,
  "grace_period_ns": 50000,
  "rail_topo_path": "/path/to/rail_topo.json"
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_retry_count` | int | 8 | Maximum retry count for failed RDMA operations |
| `block_size` | int | 65536 | Block size for slice-based transfers (bytes) |
| `grace_period_ns` | int | 50000 | Grace period in nanoseconds for rail recovery |
| `rail_topo_path` | string | `""` | Path to custom rail topology JSON file |

### TCP Transport

```json
"tcp": {
  "enable": true,
  "max_retry_count": 3,
  "retry_base_delay_ms": 100,
  "retry_max_delay_ms": 2000,
  "max_concurrent_tasks": 16
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | boolean | true | Enable or disable TCP transport |
| `max_retry_count` | int | 3 | Maximum retry count for failed TCP operations |
| `retry_base_delay_ms` | int | 100 | Base delay for exponential backoff (ms) |
| `retry_max_delay_ms` | int | 2000 | Maximum delay between retries (ms) |
| `max_concurrent_tasks` | int | 16 | Maximum concurrent TCP transfer tasks |

### GDS Transport (GPUDirect Storage)

```json
"gds": {
  "enable": true
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | boolean | true | Enable or disable GDS transport |

### SHM Transport (Shared Memory)

```json
"shm": {
  "enable": true,
  "cxl_mount_path": "",
  "async_memcpy_threshold": 4
}
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable` | boolean | true | Enable or disable shared memory transport |
| `cxl_mount_path` | string | `""` | Mount path for CXL shared memory |
| `async_memcpy_threshold` | int | 4 | Threshold for async memcpy (KB) |

### MNNVL Transport (Moore Threads NVLink)

```json
"mnnvl": {
  "enable": false
}
```

## Policy Configuration

The `policy` array defines transport selection rules for different scenarios.

```json
"policy": [
  {
    "name": "default_memory",
    "segment_type": "memory",
    "devices": ["mlx5_0", "mlx5_2"],
    "transports": ["nvlink", "rdma", "shm"]
  },
  {
    "name": "file_storage",
    "segment_type": "file",
    "transports": ["gds", "io_uring", "rdma"]
  }
]
```

### Policy Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Policy identifier for logging |
| `segment_type` | string | Yes | `"memory"` or `"file"` |
| `priority` | string/int | No | Match only this priority: `"high"`/`0`, `"medium"`/`1`, `"low"`/`2` |
| `devices` | array[string] | No | Allowed device names (empty = all) |
| `transports` | array[string] | No | Transport preference list (evaluated in order) |
| `local_memory` | string | No | Filter by local memory type (`"cuda"`, `"cpu"`, `"hip"`, `"npu"`, `"*"`) |
| `remote_memory` | string | No | Filter by remote memory type |
| `min_size` | integer | No | Minimum transfer size (bytes) |
| `max_size` | integer | No | Maximum transfer size (bytes) |
| `same_machine` | boolean | No | Match only same-machine transfers |

See [Transport Selector](../features/transport-selector.md) for detailed policy usage.

## Environment Variables

Environment variables have **higher priority** than configuration file settings. They allow runtime overrides without modifying config files.

### Priority Order

Configuration is applied in the following order (later sources override earlier ones):

1. **Default values** (hardcoded defaults)
2. **Configuration file** (`transfer-engine.json` or path specified)
3. **Environment variables** (override both defaults and config file)

### Available Environment Variables

#### Configuration File

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_CONFIG_FILE` | Path to configuration file | `export MC_CONFIG_FILE=/etc/tent/config.json` |

#### Metadata & RPC

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_METADATA_SERVER` | Default metadata server address | `export MC_METADATA_SERVER=10.0.0.1:2379` |
| `MC_LOCAL_SEGMENT_NAME` | Local segment name override | `export MC_LOCAL_SEGMENT_NAME=node-1` |

#### Network Binding

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_TCP_BIND_ADDRESS` | TCP transport bind address | `export MC_TCP_BIND_ADDRESS=192.168.1.10` |
| `MC_RDMA_BIND_ADDRESS` | RDMA transport bind address (dual-NIC) | `export MC_RDMA_BIND_ADDRESS=192.168.2.10` |
| `MC_LEGACY_RPC_PORT_BINDING` | Enable legacy RPC port binding | `export MC_LEGACY_RPC_PORT_BINDING=1` |

#### Topology & Device

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_CUSTOM_TOPO_JSON` | Path to custom topology JSON file | `export MC_CUSTOM_TOPO_JSON=/etc/tent/topology.json` |

#### Performance Tuning

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_TRANSFER_TIMEOUT` | Transfer timeout in seconds | `export MC_TRANSFER_TIMEOUT=30` |
| `MC_MAX_FAILOVER_ATTEMPTS` | Maximum failover attempts | `export MC_MAX_FAILOVER_ATTEMPTS=5` |

#### Compatibility

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_USE_TENT` | Use TENT with TE-compatible API (shim) | `export MC_USE_TENT=1` |

#### Logging

| Variable | Description | Example |
|----------|-------------|---------|
| `MC_LOG_LEVEL` | Logging level override | `export MC_LOG_LEVEL=debug` |

### Usage Example

```bash
# Override config file settings at runtime
export MC_METADATA_SERVER=10.0.0.1:2379
export MC_TCP_BIND_ADDRESS=192.168.1.10
export MC_LOG_LEVEL=debug
export MC_TRANSFER_TIMEOUT=60

# Run TENT application
./my_tent_app
```

### When to Use Environment Variables

Environment variables are particularly useful for:

- **Deployment flexibility**: Same config file across environments, with environment-specific overrides
- **Container orchestration**: Kubernetes ConfigMaps / Docker env variables
- **Debugging**: Quick parameter testing without editing config files
- **Multi-homed hosts**: Different bind addresses per instance

## Configuration Examples

### Minimal Configuration

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379"
}
```

### Multi-Rail RDMA Configuration

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "etcd://10.0.0.1:2379",
  "topology": {
    "rdma_whitelist": ["mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3"]
  },
  "transports": {
    "rdma": {
      "enable": true,
      "device": {
        "max_cqe": 8192
      },
      "workers": {
        "block_size": 131072
      }
    }
  },
  "policy": [
    {
      "name": "default",
      "segment_type": "memory",
      "devices": ["mlx5_0", "mlx5_1", "mlx5_2", "mlx5_3"],
      "transports": ["rdma", "tcp"]
    }
  ]
}
```

### High-Throughput Configuration

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379",
  "enable_progress_worker": true,
  "max_failover_attempts": 5,
  "transports": {
    "rdma": {
      "enable": true,
      "device": {
        "max_cqe": 16384
      },
      "workers": {
        "block_size": 262144
      }
    }
  },
  "policy": [
    {
      "name": "bulk_transfer",
      "segment_type": "memory",
      "priority": "low",
      "transports": ["rdma", "tcp"]
    }
  ]
}
```

### Low-Latency Configuration

```json
{
  "metadata_type": "p2p",
  "metadata_servers": "127.0.0.1:2379",
  "max_failover_attempts": 3,
  "transports": {
    "rdma": {
      "enable": true,
      "max_timeout_ns": 5000000000,
      "workers": {
        "block_size": 32768
      }
    }
  },
  "policy": [
    {
      "name": "low_latency",
      "segment_type": "memory",
      "priority": "high",
      "devices": ["mlx5_0"],
      "transports": ["rdma"]
    }
  ]
}
```
