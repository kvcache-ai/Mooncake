# Mooncake Store Deployment & Operations Guide

This page summarizes useful flags, environment variables, and HTTP endpoints to help advanced users tune Mooncake Master and observe metrics.

## Master Startup Flags (with defaults)

- RPC Related
  - `--rpc_port` (int, default 50051): RPC listen port.
  - `--rpc_thread_num` (int, default min(4, CPU cores)): RPC worker threads. If not set, uses `--max_threads` (default 4) capped by CPU cores.
  - `--rpc_address` (str, default `0.0.0.0`): RPC bind address.
  - `--rpc_conn_timeout_seconds` (int, default `0`): RPC idle connection timeout; `0` disables.
  - `--rpc_enable_tcp_no_delay` (bool, default `true`): Enable TCP_NODELAY.

- Metrics
  - `--enable_metric_reporting` (bool, default `true`): Periodically log master metrics to INFO.
  - `--metrics_port` (int, default `9003`): HTTP port for `/metrics` endpoints.

- HTTP Metadata Server For Mooncake Transfer Engine
  - `--enable_http_metadata_server` (bool, default `false`): Enable embedded HTTP metadata server.
  - `--http_metadata_server_host` (str, default `0.0.0.0`): Metadata bind host.
  - `--http_metadata_server_port` (int, default `8080`): Metadata TCP port.

- Eviction and TTLs
  - `--default_kv_lease_ttl` (uint64, default `5000` ms): Default lease TTL for KV objects.
  - `--default_kv_soft_pin_ttl` (uint64, default `1800000` ms): Soft pin TTL (30 minutes).
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
  - `--max_retry_attempts` (uint32, default `3`): Maximum number of retry attempts for failed tasks. Tasks that fail with `NO_AVAILABLE_HANDLE` error will be retried up to this many times before being marked as failed.

- DFS Storage (optional)
  - `--root_fs_dir` (str, default empty): DFS mount directory for storage backend, used in Multi-layer Storage Support.
  - `--global_file_segment_size` (int64, default `int64_max`): Maximum available space for DFS segments.

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

**Tips:**

In addition to command-line flags, the Master also supports configuration via JSON and YAML files. For example:

```bash
mooncake_master \
  --config_path=mooncake-store/conf/master.yaml 
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

## Quick Tips

- Scale `--rpc_thread_num` with available CPU cores and workload.
- Start with default eviction settings; adjust `--eviction_high_watermark_ratio` and `--eviction_ratio` based on memory pressure and object churn.
- Use `/metrics/summary` during bring-up; integrate `/metrics` with Prometheus/Grafana for production.
