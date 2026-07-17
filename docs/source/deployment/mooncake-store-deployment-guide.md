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
  - `--enable_ha` (bool, default `false`): Enable HA.
  - `--election_backend` (str, default `etcd`): Election backend, either `etcd` or `redis`.
  - `--etcd_endpoints` (str, default empty unless HA config): etcd endpoints, semicolon separated.
  - `--redis_endpoint` (str, default empty): Redis endpoint for Redis-based HA, such as `10.0.0.10:6379`.
  - `--redis_username` (str, default empty): Redis ACL username for Redis-based HA.
  - `--redis_password` (str, default empty): Redis AUTH password for Redis-based HA.
  - `--redis_db_index` (int, default `0`): Redis DB index for Redis-based HA.
  - `--redis_master_view_ttl_sec` (int, default `5`): TTL for the Redis master view key.
  - `--redis_heartbeat_interval_sec` (int, default `2`): Redis leader renewal interval. It must be smaller than `--redis_master_view_ttl_sec`.
  - `--client_ttl` (int64, default `10` s): Client alive TTL after last ping (HA mode).
  - `--cluster_id` (str, default `mooncake_cluster`): Cluster ID for persistence and HA metadata isolation.
  - `--enable_oplog` (bool, default `false`): Enable master metadata OpLog recording.
  - `--oplog_store_type` (str, default `localfs`): OpLog backend, for example `localfs` or `redis`.
  - `--oplog_data_dir` (str, default `/tmp/mooncake_oplog`): OpLog data path for `localfs`; Redis endpoint for `redis`.

### P2P HA OpLog Coverage

The current P2P primary master records oplog entries for explicit client and segment lifecycle changes (`REGISTER_CLIENT`, `UNREGISTER_CLIENT`, `MOUNT_SEGMENT`, `UNMOUNT_SEGMENT`) and replica mapping changes (`ADD_REPLICA`, `REMOVE_REPLICA`). Remaining failover-visible metadata mutations still need follow-up coverage, including client crash cleanup, heartbeat state transitions, replica eviction/rebalance, and task metadata.

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

## Redis-based Master HA

Redis-based HA uses Redis as the coordination backend for master election. It is independent from the Transfer Engine metadata backend: the Store master election backend can use Redis even if the Transfer Engine metadata service uses etcd, Redis, or HTTP.

Build with Redis HA support before deployment:

```bash
cmake -S . -B build -DSTORE_USE_REDIS=ON
cmake --build build --target mooncake_master
```

Start multiple master processes with the same Redis endpoint and cluster ID. Each master must advertise an RPC address that clients can reach; do not use `0.0.0.0` as the advertised address in a multi-node deployment.

```bash
mooncake_master \
  --enable_ha=true \
  --election_backend=redis \
  --redis_endpoint=10.0.0.10:6379 \
  --redis_username=<redis_user> \
  --redis_password=<redis_password> \
  --cluster_id=mooncake_cluster \
  --rpc_address=10.0.0.1
```

Repeat the command on the other master nodes with their own reachable `--rpc_address` values, for example `10.0.0.2` and `10.0.0.3`, while keeping `--redis_endpoint` and `--cluster_id` identical.

Client configs should use the matching Redis endpoint for HA master discovery:

```text
master_server_entry = "redis://10.0.0.10:6379"
redis_username = "<redis_user>"
redis_password = "<redis_password>"
```

When starting `mooncake_client`, pass the same settings through the client flags:

```bash
mooncake_client \
  --master_server_address=redis://10.0.0.10:6379 \
  --redis_cluster_id=mooncake_cluster \
  --redis_username=<redis_user> \
  --redis_password=<redis_password>
```

The client Redis cluster ID must match the masters' `--cluster_id`. If the client does not set a cluster ID explicitly, it uses the default `mooncake_cluster`. If Redis does not require ACL authentication, leave `redis_username` and `redis_password` empty.

Operational notes:

- Keep `--redis_heartbeat_interval_sec` smaller than `--redis_master_view_ttl_sec`. The default pair is `2` seconds and `5` seconds.
- A smaller TTL reduces failover latency but is more sensitive to transient Redis or scheduling delays. Increase the TTL if CI or production logs show unexpected leader churn.
- The active leader renews the Redis master view key periodically. If the leader process exits or loses renewal, the key expires and another master can be elected.
- Clients using `redis://` resolve the current leader from Redis and reconnect after heartbeat failures. Existing requests may fail during the failover window and should be retried by the caller.
- Redis HA metadata is isolated by `--cluster_id`, so different Mooncake Store clusters can share one Redis instance when they use different cluster IDs.

For P2P master HA, run the masters in P2P deployment mode and enable Redis-backed OpLog storage. The election Redis endpoint and the OpLog Redis endpoint may be the same Redis instance, but every master in the cluster must use the same values. Redis-backed OpLog uses the same `--redis_username` and `--redis_password` authentication settings.

```bash
mooncake_master \
  --deployment_mode=P2P \
  --enable_ha=true \
  --election_backend=redis \
  --redis_endpoint=10.0.0.10:6379 \
  --redis_username=<redis_user> \
  --redis_password=<redis_password> \
  --cluster_id=p2p_mooncake_cluster \
  --enable_oplog=true \
  --oplog_store_type=redis \
  --oplog_data_dir=10.0.0.10:6379 \
  --rpc_address=10.0.0.1
```

P2P clients should also use Redis master discovery and the same cluster ID:

```text
master_server_entry = "redis://10.0.0.10:6379"
redis_cluster_id = "p2p_mooncake_cluster"
redis_username = "<redis_user>"
redis_password = "<redis_password>"
deployment_mode = "P2P"
```

For `mooncake_client` in P2P mode:

```bash
mooncake_client \
  --deployment_mode=P2P \
  --metadata_server=P2PHANDSHAKE \
  --master_server_address=redis://10.0.0.10:6379 \
  --redis_cluster_id=p2p_mooncake_cluster \
  --redis_username=<redis_user> \
  --redis_password=<redis_password>
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
