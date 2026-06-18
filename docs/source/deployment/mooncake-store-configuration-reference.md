# Mooncake Store Configuration Reference

This page collects the `mooncake_master` command builder, client deployment
modes, master startup flags, and client/engine environment variables used when
deploying Mooncake Store.

## Master Command Builder

Use this builder to compose common `mooncake_master` startup commands. The
flag tables below describe each option in detail.

```{dynamic-command}
base: mooncake_master
command_label: Generated master startup command
format:
  line_break: options
  indent: "  "
options:
  - label: RPC address
    key: rpc_address
    default: default
    choices:
      - label: default bind
        value: default
      - label: explicit host
        value: host_ip
        args: --rpc_address=10.0.0.1
      - label: interface
        value: interface
        args: --rpc_interface=eth0
  - label: Metadata backend
    key: metadata
    default: p2p
    choices:
      - label: P2P handshake
        value: p2p
      - label: embedded HTTP
        value: embedded_http
        args: --enable_http_metadata_server=true --http_metadata_server_host=0.0.0.0 --http_metadata_server_port=8080
      - label: external etcd/Redis
        value: external
  - label: High availability
    key: ha
    default: disabled
    choices:
      - label: disabled
        value: disabled
      - label: etcd
        value: etcd
        args: '--enable_ha=true --ha_backend_type=etcd --etcd_endpoints="10.0.0.1:2379;10.0.0.2:2379;10.0.0.3:2379"'
      - label: Redis
        value: redis
        args: '--enable_ha=true --ha_backend_type=redis --ha_backend_connstring="redis://127.0.0.1:6379"'
  - label: Tiered storage
    key: offload
    default: disabled
    choices:
      - label: disabled
        value: disabled
      - label: eager offload
        value: eager
        args: --enable_offload=true --root_fs_dir=/mnt/ssd_cache
      - label: offload on eviction
        value: on_evict
        args: --enable_offload=true --offload_on_evict=true --promotion_on_hit=true --promotion_admission_threshold=2 --root_fs_dir=/mnt/ssd_cache
  - label: CXL memory
    key: cxl
    default: disabled
    choices:
      - label: disabled
        value: disabled
      - label: /dev/dax0.0
        value: dax0
        args: --enable_cxl=true --cxl_path=/dev/dax0.0 --cxl_size=17179869184 --allocation_strategy=cxl
  - label: Snapshot
    key: snapshot
    default: disabled
    choices:
      - label: disabled
        value: disabled
      - label: local restore
        value: local_restore
        env: MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots
        args: --enable_snapshot=true --snapshot_object_store_type=local --snapshot_interval_seconds=300 --snapshot_retention_count=5 --enable_snapshot_restore=true
```

## Client Deployment Modes

### Standalone Real Client via RPC

To run a resource-owning real client as a standalone RPC process:

```bash
mooncake_client \
  --global_segment_size="4GB" \
  --master_server_address="localhost:50051" \
  --metadata_server="P2PHANDSHAKE"
```

The real client connects to the master and listens on port `50052` by default.
Application processes such as vLLM or SGLang can then use dummy clients to
forward requests to this real client.

Common `mooncake_client` flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Client service bind host |
| `--port` | `50052` | Client service listen port |
| `--global_segment_size` | `4GB` | Global segment size contributed by the client |
| `--master_server_address` | `localhost:50051` | Master service address |
| `--metadata_server` | `http://127.0.0.1:8080/metadata` | Transfer Engine metadata backend; use `P2PHANDSHAKE` for the smallest setup |
| `--protocol` | `tcp` | Transfer protocol |
| `--device_name` | empty | Transfer device name |
| `--threads` | `1` | Client worker thread count |

### Standalone Real Client via HTTP

Use `python -m mooncake.mooncake_store_service` to start a real client with a
lightweight HTTP API for manual `Get` and `Put` debugging.

For a P2P handshake setup, no external metadata service is required:

```bash
python -m mooncake.mooncake_store_service \
  --local_hostname=localhost \
  --metadata_server=P2PHANDSHAKE \
  --master_server=127.0.0.1:50051
```

Create a JSON config:

```json
{
  "local_hostname": "localhost",
  "metadata_server": "http://localhost:8080/metadata",
  "global_segment_size": 268435456,
  "local_buffer_size": 268435456,
  "protocol": "tcp",
  "device_name": "",
  "master_server_address": "localhost:50051"
}
```

Start the service:

```bash
python -m mooncake.mooncake_store_service --config=<config_path> --port=8081
```

The main startup parameters are `--config`, the path to the JSON configuration
file, and `--port`, the HTTP server port.

### Verify Installed Examples

For a Python integration check, run
`mooncake-store/tests/distributed_object_store_provider.py` after starting the
metadata service and `mooncake_master`.

For a C++ integration check, run `build/mooncake-store/tests/client_integration_test`
after building tests and starting the required services.

## Master Startup Flags

### RPC

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc_port` | `50051` | RPC listen port |
| `--rpc_thread_num` | `min(4, CPU cores)` | RPC worker threads |
| `--rpc_address` | `0.0.0.0` | RPC bind address |
| `--rpc_interface` | empty | Network interface to resolve RPC address at startup (overrides `--rpc_address`) |
| `--rpc_conn_timeout_seconds` | `0` | Idle connection timeout; `0` disables |
| `--rpc_enable_tcp_no_delay` | `true` | Enable TCP_NODELAY |

### Logging

The master uses glog. When `--log_dir` is set, all severities are merged into a single journal file in that directory (`mooncake_master.INFO.<date>-<time>.<pid>`), reachable through the stable `mooncake_master.INFO` symlink.

glog's standard flags (`--log_dir`, `--max_log_size`, `--logtostderr`, ...) control the rest.

### Metrics

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_metric_reporting` | `true` | Periodically log master metrics |
| `--metrics_port` | `9003` | HTTP port for `/metrics` endpoints |

### HTTP Metadata Server (Embedded)

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_http_metadata_server` | `false` | Enable embedded HTTP metadata server |
| `--http_metadata_server_host` | `0.0.0.0` | Metadata bind host |
| `--http_metadata_server_port` | `8080` | Metadata TCP port |

### Memory Allocator

| Flag | Default | Description |
|------|---------|-------------|
| `--memory_allocator` | `offset` | Memory allocator: `offset` (default) or `cachelib` |

### Allocation Strategy

| Flag | Default | Description |
|------|---------|-------------|
| `--allocation_strategy` | `random` | `random` (pure random, fastest), `free_ratio_first` (best load balance), or `cxl` (prefer CXL memory) |

### PutStart Timeouts

| Flag | Default | Description |
|------|---------|-------------|
| `--put_start_discard_timeout_sec` | `30` | Seconds before an uncompleted `PutStart` is discarded |
| `--put_start_release_timeout_sec` | `300` (5 min) | Seconds before `PutStart`-allocated space is released |

### Eviction & TTLs

| Flag | Default | Description |
|------|---------|-------------|
| `--default_kv_lease_ttl` | `5000` ms | Lease TTL for KV objects. Supports `5000ms`, `5s`, `30m`, `1h` |
| `--default_kv_soft_pin_ttl` | `1800000` ms | Soft pin TTL (30 min) |
| `--allow_evict_soft_pinned_objects` | `true` | Allow evicting soft-pinned objects |
| `--eviction_ratio` | `0.05` | Fraction evicted at high watermark |
| `--eviction_high_watermark_ratio` | `0.95` | Usage ratio triggering eviction |
| `--client_ttl` | `10` s | Seconds before a silent client is considered disconnected |

### High Availability

**Master Node High Availability**

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_ha` | `false` | Enable HA mode |
| `--ha_backend_type` | `etcd` | HA backend: `etcd`, `redis`, or `k8s` |
| `--ha_backend_connstring` | empty | HA backend connection string |
| `--etcd_endpoints` | empty | etcd endpoints, semicolon separated (when `--ha_backend_type=etcd`) |
| `--cluster_id` | `mooncake_cluster` | Cluster ID for HA persistence |

```{caution}
Metadata Snapshot And Restore is experimental feature.
```

**Metadata Snapshot And Restore**

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_snapshot` | `false` | Enable periodic metadata snapshot |
| `--snapshot_interval_seconds` | `300` (5 min) | Interval between snapshots |
| `--snapshot_child_timeout_seconds` | `3600` (1 hour) | Timeout per snapshot child process |
| `--snapshot_retention_count` | `10` | Number of recent snapshots retained |
| `--snapshot_object_store_type` | required | Object store: `local` or `s3` |
| `--snapshot_catalog_store_type` | empty | Catalog store: `embedded` or `redis` |
| `--snapshot_catalog_store_connstring` | empty | Catalog store connection string (required for `redis`) |
| `--snapshot_backup_dir` | empty | Optional local backup directory |
| `--enable_snapshot_restore` | `false` | Restore from latest snapshot at startup |

**Environment variable:** `MOONCAKE_SNAPSHOT_LOCAL_PATH` (required when `--snapshot_object_store_type=local`) — persistent directory for local snapshots.

```{warning}
The snapshot storage path is a **managed directory** exclusively controlled by Mooncake. Old snapshots exceeding `--snapshot_retention_count` are automatically deleted. Use a dedicated directory to avoid data loss.
```

### Task Manager

| Flag | Default | Description |
|------|---------|-------------|
| `--max_total_finished_tasks` | `10000` | Max finished tasks kept in memory |
| `--max_total_pending_tasks` | `10000` | Max queued pending tasks |
| `--max_total_processing_tasks` | `10000` | Max simultaneously processing tasks |
| `--pending_task_timeout_sec` | `300` (5 min) | Timeout for pending tasks (`0` = no timeout) |
| `--processing_task_timeout_sec` | `300` (5 min) | Timeout for processing tasks (`0` = no timeout) |
| `--max_retry_attempts` | `10` | Max retries for failed tasks (`NO_AVAILABLE_HANDLE`) |

### Offload / Tiered Storage

Flags for controlling data movement between DRAM and SSD.

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_offload` | `false` | Enable offload from DRAM to SSD |
| `--offload_on_evict` | `false` | Defer offload to eviction time rather than at `Put` |
| `--offload_force_evict` | `false` | Force-evict objects exceeding capacity without offload |
| `--promotion_on_hit` | `false` | Promote SSD-resident keys to DRAM on read hit |
| `--promotion_admission_threshold` | `2` | Min CountMinSketch count to allow promotion (`1` = disable gating) |
| `--promotion_queue_limit` | `50000` | Max in-flight promotion tasks |
| `--quota_bytes` | `0` (90% of capacity) | Storage quota in bytes |
| `--enable_disk_eviction` | `true` | Enable disk eviction |

Start with `--enable_offload=true` for eager asynchronous SSD persistence after `Put` completion. Add `--offload_on_evict=true` when you want SSD writes to happen only when memory pressure selects an object for eviction. Add `--promotion_on_hit=true` to allow hot SSD-only data to be promoted back to DRAM, and tune `--promotion_admission_threshold` to control how many observed reads are required before promotion is queued.

### CXL Memory

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_cxl` | `false` | Enable CXL memory support |
| `--cxl_path` | `/dev/dax0.0` | DAX device path for CXL memory |
| `--cxl_size` | `8GB` (`8589934592`) | CXL memory size in bytes |

When `--allocation_strategy=cxl` is set alongside `--enable_cxl=true`, the master preferentially allocates new objects on CXL memory.

### DFS Storage

| Flag | Default | Description |
|------|---------|-------------|
| `--root_fs_dir` | empty | DFS mount directory for multi-layer storage backend |
| `--global_file_segment_size` | `4GB` (`4294967296`) | Max available space for DFS segments |

### Master Configuration File

In addition to CLI flags, the master accepts JSON/YAML config files:

```bash
mooncake_master --config_path=mooncake-store/conf/master.yaml
```

```yaml
rpc_interface: "eth0"
rpc_port: 50051
```

## Client & Engine Tuning Environment Variables

### Runtime Protocol

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_RPC_PROTOCOL` | `tcp` | RPC transport protocol between master and clients: `tcp` or `rdma` |
| `MC_USE_TENT` / `MC_USE_TEV1` | unset | Set to any value to enable the TENT (next-gen) transfer engine |
| `MC_STORE_CLUSTER_ID` | unset | Cluster ID label attached to client metrics |

### Topology Discovery

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_MS_AUTO_DISC` | `1` | Auto-discover NIC/GPU topology. Set `0` to provide `rdma_devices` manually |
| `MC_MS_FILTERS` | empty | Comma-separated NIC whitelist (e.g., `mlx5_0,mlx5_2`) |

When `MC_MS_AUTO_DISC=0`, pass `rdma_devices` (comma-separated) to the Python `setup()` call.

### Transfer Engine Metrics (disabled by default)

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_TE_METRIC` | `0` | Set to `1` to enable engine metrics. Not supported with TENT |
| `MC_TE_METRIC_INTERVAL_SECONDS` | `5` | Seconds between reports |

### Client Metrics (enabled by default)

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_CLIENT_METRIC` | `1` | Set `0` to disable |
| `MC_STORE_CLIENT_METRIC_INTERVAL` | `0` | Reporting interval; `0` collects but does not periodically report |
| `MC_STORE_CLIENT_MIN_PORT` | `12300` | Min local port for client connections |
| `MC_STORE_CLIENT_MAX_PORT` | `14300` | Max local port for client connections |

### Local Hot Cache

Local hot cache provides a DRAM read cache on top of SSD-resident objects for faster access.

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_LOCAL_HOT_CACHE_SIZE` | unset | Size of the local hot cache (e.g., `"8gb"`). Set to enable the hot cache |
| `MC_STORE_LOCAL_HOT_BLOCK_SIZE` | unset | Block size for hot cache (e.g., `"2mb"`) |
| `MC_STORE_LOCAL_HOT_CACHE_USE_SHM` | unset | Set `1` to use memfd-backed shared memory |
| `MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD` | unset | Minimum CountMinSketch count before a key is admitted to hot cache |

### Local Memory Optimization

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_MEMCPY` | `0` | Set `1` to prefer local memcpy when source/destination are on the same client |
| `MC_STORE_CLIENT_SETUP_RETRIES` | `20` | Number of times to retry client registration on failure |
| `MC_CXL_DEV_SIZE` | unset | CXL device size (overrides `--cxl_size` for client-side allocation) |

### MMap Buffer & HugePages

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_USE_HUGEPAGE` | unset | Set `1` to request HugeTLB-backed `mmap()` |
| `MC_STORE_HUGEPAGE_SIZE` | `2MB` | Supported: `2MB`, `1GB` |
| `MC_MMAP_ARENA_POOL_SIZE` | unset | Pre-allocated arena pool size (e.g., `8gb`). Explicitly set to enable the arena |
| `MC_DISABLE_MMAP_ARENA` | unset | Set `1` to disable arena, fall back to per-call `mmap()` |

### yalantinglibs Log Level

```bash
export MC_YLT_LOG_LEVEL=info
```

Available: `trace`, `debug`, `info`, `warn` (or `warning`), `error`, `critical`.
