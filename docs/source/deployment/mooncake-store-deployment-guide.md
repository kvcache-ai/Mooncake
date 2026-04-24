# Mooncake Store Deployment & Operations Guide

This page summarizes useful flags, environment variables, and HTTP endpoints to help advanced users tune Mooncake Master and observe metrics.

## Master Startup Flags (with defaults)

- RPC Related
  - `--rpc_port` (int, default 50051): RPC listen port.
  - `--rpc_thread_num` (int, default min(4, CPU cores)): RPC worker threads. If not set, uses `--max_threads` (default 4) capped by CPU cores.
  - `--rpc_address` (str, default `0.0.0.0`): RPC bind address.
  - `--rpc_interface` (str, default empty): Network interface used to resolve the final RPC address. When set, Mooncake Master resolves the interface's current IPv4 address at startup and uses it as the final `rpc_address`. This overrides `--rpc_address`.
  - `--rpc_conn_timeout_seconds` (int, default `0`): RPC idle connection timeout; `0` disables.
  - `--rpc_enable_tcp_no_delay` (bool, default `true`): Enable TCP_NODELAY.

- Metrics
  - `--enable_metric_reporting` (bool, default `true`): Periodically log master metrics to INFO.
  - `--metrics_port` (int, default `9003`): HTTP port for `/metrics` endpoints.

- HTTP Metadata Server For Mooncake Transfer Engine
  - `--enable_http_metadata_server` (bool, default `false`): Enable embedded HTTP metadata server.
  - `--http_metadata_server_host` (str, default `0.0.0.0`): Metadata bind host.
  - `--http_metadata_server_port` (int, default `8080`): Metadata TCP port.

- Allocation Strategy
  - `--allocation_strategy` (str, default `random`): Memory allocation strategy for replica placement. Available options:
    - `random`: Pure random selection across segments (baseline, fastest).
    - `free_ratio_first`: Free-ratio-first strategy. Samples multiple candidates and selects those with highest free space ratio for better load balancing.

- Eviction and TTLs
  - `--default_kv_lease_ttl` (duration, default `5000` ms): Default lease TTL for KV objects. The default unit is milliseconds, so `5000` means `5000ms`. Duration strings such as `5000ms`, `5s`, `30m`, or `1h` are also supported.
  - `--default_kv_soft_pin_ttl` (duration, default `1800000` ms): Soft pin TTL (30 minutes). The default unit is milliseconds, so `1800000` means `1800000ms`. Duration strings such as `1800000ms`, `30m`, or `1h` are also supported.
  - `--allow_evict_soft_pinned_objects` (bool, default `true`): Allow evicting soft-pinned objects.
  - `--eviction_ratio` (double, default `0.05`): Fraction evicted when hitting high watermark.
  - `--eviction_high_watermark_ratio` (double, default `0.95`): Usage ratio to trigger eviction.

- High Availability (optional)
  - `--enable_ha` (bool, default `false`): Enable HA (requires etcd).
  - `--etcd_endpoints` (str, default empty unless HA config): etcd endpoints, semicolon separated.
  - `--client_ttl` (int64, default `10` s): Client alive TTL after last ping (HA mode).
  - `--cluster_id` (str, default `mooncake_cluster`): Cluster ID for persistence in HA mode.

- Task Manager (optional)
  - `--max_total_finished_tasks` (uint32, default `10000`): Maximum number of finished tasks to keep in memory. When this limit is reached, the oldest finished tasks will be pruned from memory.
  - `--max_total_pending_tasks` (uint32, default `10000`): Maximum number of pending tasks that can be queued in memory. When this limit is reached, new task submissions will fail with `TASK_PENDING_LIMIT_EXCEEDED` error.
  - `--max_total_processing_tasks` (uint32, default `10000`): Maximum number of tasks that can be processing simultaneously. When this limit is reached, no new tasks will be popped from the pending queue until some processing tasks complete.
  - `--max_retry_attempts` (uint32, default `10`): Maximum number of retry attempts for failed tasks. Tasks that fail with `NO_AVAILABLE_HANDLE` error will be retried up to this many times before being marked as failed.

- DFS Storage (optional)
  - `--root_fs_dir` (str, default empty): DFS mount directory for storage backend, used in Multi-layer Storage Support.
  - `--global_file_segment_size` (int64, default `int64_max`): Maximum available space for DFS segments.

- Snapshot / Restore (optional)
  - `--enable_snapshot` (bool, default `false`): Enable periodic snapshot of master metadata data (effective when using the `offset` memory allocator).
  - `--snapshot_interval_seconds` (uint64, default `600`): Interval in seconds between periodic snapshots of master data.
  - `--snapshot_child_timeout_seconds` (uint64, default `300`): Timeout in seconds for each snapshot child process.
  - `--snapshot_retention_count` (uint32, default `2`): Number of recent snapshots to keep. Older snapshots beyond this limit will be automatically deleted.
  - `--snapshot_backend_type` (str, required when snapshot enabled): Snapshot storage backend type: `local` for local filesystem, `s3` for S3 storage.
  - `--snapshot_backup_dir` (str, default empty): Optional local directory for snapshot backup. If empty (default), local backup is disabled. When set, it serves two purposes: (1) during snapshot persistence, data will be saved locally as a fallback if uploading to the backend fails; (2) during restore, downloaded metadata will also be saved to this directory as a local backup.
  - `--enable_snapshot_restore` (bool, default `false`): Enable restore from the latest snapshot at master startup.
  - **Environment variable** `MOONCAKE_SNAPSHOT_LOCAL_PATH` (**required** when `--snapshot_backend_type=local`): Persistent directory path for local snapshot storage. This variable **must** be set before starting the master; there is no default value. Example: `export MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots`.

  > **Warning: Managed Directory**
  >
  > The snapshot storage path (`MOONCAKE_SNAPSHOT_LOCAL_PATH` for local backend, or S3 bucket for S3 backend) is a **managed directory** exclusively controlled by the Mooncake snapshot system. **DO NOT store other files or data in this directory.** Old snapshots exceeding `--snapshot_retention_count` will be automatically and permanently deleted during cleanup. Use a dedicated, isolated directory for snapshot storage to avoid accidental data loss.

Example (enable embedded HTTP metadata and metrics):

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --rpc_thread_num=64 \
  --metrics_port=9003 \
  --enable_metric_reporting=true
```

Example (resolve the master RPC address from a stable interface name in a container):

```bash
mooncake_master \
  --rpc_interface=eth0 \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```

This resolves the current IPv4 address of `eth0` at startup and uses it as the final `rpc_address`.

Example (use free-ratio-first allocation strategy for better load balancing):

```bash
mooncake_master \
  --allocation_strategy=free_ratio_first \
  --enable_http_metadata_server=true \
  --http_metadata_server_port=8080
```

**Tips:**

In addition to command-line flags, the Master also supports configuration via JSON and YAML files. For example:

```bash
mooncake_master \
  --config_path=mooncake-store/conf/master.yaml
```

For config files, the equivalent setting is:

```yaml
rpc_interface: "eth0"
rpc_port: 50051
```

## Metrics Endpoints

The master exposes Prometheus-style metrics over HTTP on `--metrics_port`:

- `GET /metrics` — Prometheus format (`text/plain; version=0.0.4`).
- `GET /metrics/summary` — Human-readable summary.

Examples:

```bash
curl -s http://<master_host>:9003/metrics
curl -s http://<master_host>:9003/metrics/summary
```

## Client/Engine Tuning (Env Vars, with defaults)

- Topology discovery (Store Client → Transfer Engine)
  - `MC_MS_AUTO_DISC` (default `1`): Auto-discover NIC/GPU topology. Set `0` to disable and provide `rdma_devices` manually.
  - `MC_MS_FILTERS` (default empty): Optional comma-separated NIC whitelist when auto-discovery is enabled (e.g., `mlx5_0,mlx5_2`).
  - If `MC_MS_AUTO_DISC=0`, pass `rdma_devices` (comma-separated) to the Python `setup(...)` call.

- Transfer Engine metrics (disabled by default)
  - `MC_TE_METRIC` (default `0`/unset): Set to `1` to enable periodic engine metrics logging. **Note:** Not supported when using Transfer Engine TENT.
  - `MC_TE_METRIC_INTERVAL_SECONDS` (default `5`): Positive integer seconds between reports (effective only if metrics enabled).

- Client metrics (enabled by default)
  - `MC_STORE_CLIENT_METRIC` (default `1`): Client-side metrics on by default; set `0` to disable entirely.
  - `MC_STORE_CLIENT_METRIC_INTERVAL` (default `0`): Reporting interval in seconds; `0` collects but does not periodically report.

- Local memcpy optimization (Store transfer path)
  - `MC_STORE_MEMCPY` (default `0`/false): Set to `1` to prefer local memcpy when source/destination are on the same client.

## Set the Log Level for yalantinglibs coro_rpc and coro_http
By default, the log level is set to warning. You can customize it using the following environment variable:

`export MC_YLT_LOG_LEVEL=info`

This sets the log level for yalantinglibs (including coro_rpc and coro_http) to info.

Available log levels: trace, debug, info, warn (or warning), error, and critical.

## S3 Snapshot Backend

When `--snapshot_backend_type=s3` is set, Mooncake Master stores snapshots in an S3-compatible object store.

### Required Environment Variables

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | S3 access key (or IAM role credential) |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `AWS_DEFAULT_REGION` | AWS region (e.g., `us-east-1`) |
| `MOONCAKE_SNAPSHOT_S3_BUCKET` | Target S3 bucket name |
| `MOONCAKE_SNAPSHOT_S3_PREFIX` | (Optional) Key prefix inside the bucket |

For IAM role-based authentication (recommended in production), omit `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` — the SDK will use the instance profile automatically.

### Bucket Naming Conventions

- Create a dedicated bucket for Mooncake snapshots (e.g., `my-org-mooncake-snapshots`).
- Enable versioning on the bucket for additional safety.
- Set a lifecycle rule to expire old snapshot objects beyond the `--snapshot_retention_count` limit.

> **Warning:** The S3 bucket is a managed directory. Do not store other data under the same key prefix as Mooncake snapshots — old snapshots are deleted automatically during cleanup.

### Example Startup Command (S3 backend)

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1
export MOONCAKE_SNAPSHOT_S3_BUCKET=my-org-mooncake-snapshots
export MOONCAKE_SNAPSHOT_S3_PREFIX=prod-cluster/

mooncake_master \
    --rpc_port=50051 \
    --enable_snapshot=true \
    --snapshot_backend_type=s3 \
    --snapshot_interval_seconds=300 \
    --snapshot_retention_count=3 \
    --enable_snapshot_restore=true
```

### Restore Procedure

On master restart with `--enable_snapshot_restore=true`, the master:
1. Lists snapshots in the configured S3 bucket/prefix.
2. Downloads the latest snapshot.
3. Applies the snapshot to restore in-memory metadata.
4. Resumes serving client RPCs.

If `--snapshot_backup_dir` is also set, the downloaded snapshot is additionally saved locally as a fallback.

---

## Redis HA Backend

Mooncake Store HA mode can use **Redis** instead of etcd for distributed leader election and cluster coordination.

### Build Requirement

Redis HA support must be enabled at compile time:

```bash
cmake .. \
    -DSTORE_USE_REDIS=ON \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j$(nproc)
```

This adds the `STORE_USE_REDIS` compile definition and links `hiredis`.

### Connection Configuration

| Environment Variable | Description |
|----------------------|-------------|
| `MC_REDIS_PASSWORD` | Redis AUTH password (omit if no auth is configured) |
| `MC_REDIS_DB_INDEX` | Redis database index (default: `0`) |

The Redis server URL is passed via `--etcd_endpoints` using the `redis://` scheme:

```bash
mooncake_master \
    --rpc_port=50051 \
    --enable_ha=true \
    --etcd_endpoints="redis://10.0.0.10:6379" \
    --cluster_id=prod-cluster \
    --client_ttl=15
```

For a Redis Cluster or Sentinel setup:

```bash
# Redis Cluster (semicolon-separated nodes)
--etcd_endpoints="redis://10.0.0.10:6379;redis://10.0.0.11:6379;redis://10.0.0.12:6379"
```

### Key Hash-Tag Conventions

Mooncake uses Redis hash tags to ensure that all keys for a given cluster land on the same Redis cluster slot. The tag is derived from `--cluster_id` and sanitised via `SanitizeHashTagComponent()` (which strips characters that are illegal inside `{}`). For example:

- `--cluster_id=prod-cluster` → Redis keys use `{prod-cluster}` as the hash tag.
- All leader election keys, oplog entries, and session heartbeats share this tag.

Do not use the same Redis instance / keyspace for other applications without ensuring their keys use different hash tags.

### ConnectRedis() Helper

The internal `ha::backends::redis::ConnectRedis()` helper reads `MC_REDIS_PASSWORD` and `MC_REDIS_DB_INDEX` from the environment automatically. No additional code changes are needed; configure these variables before starting the master.

### Redis vs etcd

| Feature | Redis | etcd |
|---------|-------|------|
| Build flag | `STORE_USE_REDIS=ON` | `STORE_USE_ETCD=ON` |
| URL scheme | `redis://` | `etcd://` or bare `host:port` |
| Cluster support | Redis Cluster / Sentinel | etcd cluster |
| Recommended for | Environments already running Redis | New deployments |

---

## Quick Tips

- Scale `--rpc_thread_num` with available CPU cores and workload.
- Start with default eviction settings; adjust `--eviction_high_watermark_ratio` and `--eviction_ratio` based on memory pressure and object churn.
- Use `/metrics/summary` during bring-up; integrate `/metrics` with Prometheus/Grafana for production.


---

:::{toctree}
:caption: Advanced Topics
:maxdepth: 1

ssd-offload
ha-hot-standby
monitoring
multi-tier-storage
:::
