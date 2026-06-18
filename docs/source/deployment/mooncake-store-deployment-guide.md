# Mooncake Store Deployment & Tuning Guide

This guide covers minimal deployment, and operational tuning of Mooncake Store.

## Architecture Overview


![architecture](../image/mooncake-store-preview.png)

**Master Service** (`mooncake_master`): The central coordinator. It manages cluster membership, allocates object storage across client nodes, and enforces eviction/placement policies. Runs as a standalone process.

**Client Node**: Each node contributes DRAM (and optionally VRAM/SSD) to form the distributed cache pool. Clients communicate with the master over RPC for control operations (`Put`/`Get`/`Remove`), but transfer actual data directly between each other via the Transfer Engine — the master is never in the data path.

**Metadata Service**: A separate service (etcd, Redis, or HTTP) used by the Transfer Engine for peer discovery and configuration. The master's embedded HTTP metadata server can replace an external etcd/Redis for simple deployments. We also provide a P2P handshake mechanism (`P2PHANDSHAKE`) that enables decentralized metadata management by storing metadata locally on each node, eliminating the need for a centralized service — this is the simplest metadata handshake method and the recommended starting point (see [Quick Start](#quick-start)).

For a detailed design discussion, see the [Mooncake Store Design](../design/mooncake-store.md).

---

## Quick Start

Deploy a minimal single-node Mooncake Store in three steps.

### 1. Start the Metadata Service

This quick start uses **P2P handshake** — the simplest option, with **nothing to start**: each node exchanges and stores Transfer Engine metadata locally during connection setup. You just pass the literal string `P2PHANDSHAKE` as the client's `metadata_server` (step 3).

For large or long-lived clusters, use the master's embedded HTTP metadata server or an external etcd/Redis instead — see [Deployment Scenarios](#deployment-scenarios).

### 2. Start the Master Service

With P2P handshake the master needs no metadata-server flags:

```bash
mooncake_master
```

On success the master logs a single line like:

```
Master service started on port 50051, max_threads=4, ...
```

The master's default RPC port is `50051`. (To embed an HTTP metadata server instead of using P2P, add `--enable_http_metadata_server=true --http_metadata_server_port=8080`.)

(start-a-store-client)=
### 3. Start a Store Client

A client contributes DRAM (and optionally SSD) to the cluster. The simplest way is to embed Mooncake in a Python process and call `store.setup(...)` with `metadata_server="P2PHANDSHAKE"`:

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    local_hostname="localhost",
    metadata_server="P2PHANDSHAKE",           # decentralized; no metadata service
    global_segment_size=3200 * 1024 * 1024,   # DRAM contributed to the cluster
    local_buffer_size=512 * 1024 * 1024,      # Transfer Engine buffer
    protocol="tcp",
    rdma_devices="",                          # keyword is rdma_devices (not device_name)
    master_server_addr="127.0.0.1:50051",     # keyword is master_server_addr
)
```

There are **three ways** to run a client — programmatic (above), a standalone `mooncake_store_service` process (configured via `MOONCAKE_*`), and the `mooncake_client` real-client RPC process. See [Reference: Client Configuration & Tuning](#reference-client-configuration-tuning) for all three, with full parameter/env tables.

**What just happened:**

1. The client registered itself with the master via RPC.
2. The master allocated a 3.2 GB segment on this node and added it to the cluster's memory pool.
3. The client is now ready to serve `Put`/`Get`/`Remove` requests.

### Run the Stress Benchmark

Mooncake Store includes sample programs for validating C++ and Python integrations. The [stress benchmark script](gh-file:mooncake-store/tests/stress_cluster_benchmark.py) can be used to verify a two-role prefill/decode setup.

Configure the script with command-line flags (run with `--help` for the full list):

- `--local-hostname`: the local machine's reachable IP address or hostname.
- `--metadata-server`: the Transfer Engine metadata service, e.g. `P2PHANDSHAKE`, `http://127.0.0.1:8080/metadata`, or an etcd address.
- `--master-server`: the Mooncake Store master address. Use `IP:Port` in default mode, or `etcd://IP:Port;IP:Port;...;IP:Port` in etcd-backed HA mode.
- `--protocol`: transport, `tcp` / `rdma` / `cxl` / `ascend` (defaults to `rdma`).

Then start the roles:

```bash
python3 mooncake-store/tests/stress_cluster_benchmark.py --role prefill
python3 mooncake-store/tests/stress_cluster_benchmark.py --role decode
```

For RDMA, topology auto-discovery and NIC filters can be passed through environment variables:

```bash
MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 mooncake-store/tests/stress_cluster_benchmark.py --role prefill
MC_MS_AUTO_DISC=1 MC_MS_FILTERS="mlx5_1,mlx5_2" python3 mooncake-store/tests/stress_cluster_benchmark.py --role decode
```

The absence of errors indicates successful data transfer.

### Verify Installed Examples

For a Python integration check, run `mooncake-store/tests/distributed_object_store_provider.py` after starting the metadata service and `mooncake_master`.

For a C++ integration check, run `build/mooncake-store/tests/client_integration_test` after building tests and starting the required services.

### Verify

```bash
# Health check — master metrics endpoint
curl -s http://localhost:9003/metrics/summary

# List registered clients
# (exposed through the store's Python API or RPC)
```

---

## Deployment Scenarios

### Single-Node (TCP) — Development / Quick Evaluation

The simplest deployment, as shown in [Quick Start](#quick-start). A single `mooncake_master` orchestrates clients over TCP. Suitable for development, testing, and single-host evaluation.

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```

Limitation: the master is a single point of failure. If it crashes, cluster operations pause until it is restored.

---

### High-Availability (etcd) — Production HA

Runs a cluster of master instances coordinated through etcd. If the leader fails, the remaining instances elect a new leader automatically.

```bash
# Start each master instance with:
mooncake_master \
  --enable_ha=true \
  --etcd_endpoints="10.0.0.1:2379;10.0.0.2:2379;10.0.0.3:2379" \
  --rpc_address=10.0.0.1
```

Each instance must specify its own reachable `--rpc_address`. The etcd cluster used for HA can be shared with or separate from the Transfer Engine's metadata etcd.

**Client addressing:** to reach an HA cluster, clients must use the `etcd://` master-address form (so they can discover the current leader) instead of a single `IP:Port` — set `master_server_addr` (Method A) / `MOONCAKE_MASTER` (Method B) / `--master_server_address` (Method C) to `etcd://10.0.0.1:2379;10.0.0.2:2379;...`.

---

### High-Availability (Redis) — Alternative HA Backend

Same HA semantics but using Redis instead of etcd for leader election:

```bash
mooncake_master \
  --enable_ha=true \
  --ha_backend_type=redis \
  --ha_backend_connstring="redis://127.0.0.1:6379" \
  --rpc_address=10.0.0.1
```

**Client addressing:** clients reach a Redis-backed HA cluster with the `redis://connstring` master-address form (e.g. `redis://127.0.0.1:6379`) for `master_server_addr` / `MOONCAKE_MASTER` / `--master_server_address`, instead of a single `IP:Port`.


---

### Snapshot & Restore — Backup / Disaster Recovery

```{caution}
Metadata Snapshot And Restore is experimental feature.
```

Periodically persist master metadata to local disk or S3, enabling recovery from a recent snapshot after a crash.

```bash
export MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots

mooncake_master \
  --enable_snapshot=true \
  --snapshot_interval_seconds=300 \
  --snapshot_retention_count=5 \
  --snapshot_object_store_type=local \
  --enable_snapshot_restore=true
```

---

### Tiered Storage with SSD Offload - Cost-Effective Capacity

Extends the cache pool from DRAM to SSD while keeping normal reads and writes on the distributed memory path. With `--enable_offload=true`, completed memory writes are queued for asynchronous SSD persistence through the master control plane. Set `--offload_on_evict=true` to defer that SSD write until the memory eviction path selects an object for reclamation. When `--promotion_on_hit=true`, SSD-only objects can be promoted back to DRAM after repeated reads; admission is gated by `--promotion_admission_threshold`.

Promotion execution runs on the FileStorage holder client. By default one background worker drains promotion tasks outside the heartbeat thread. Increase `MOONCAKE_OFFLOAD_PROMOTION_WORKER_THREADS` only when SSD bandwidth, network bandwidth, and DRAM allocation headroom can absorb more concurrent L2-to-L1 copies.

```bash
mooncake_master \
  --enable_offload=true \
  --offload_on_evict=true \
  --promotion_on_hit=true \
  --promotion_admission_threshold=2 \
  --root_fs_dir=/mnt/ssd_cache \
  --enable_http_metadata_server=true \
  --http_metadata_server_port=8080
```

---

### CXL-Aware Allocation — Memory Tiering

When the host has CXL-attached memory, the master can preferentially allocate new objects on the CXL tier, reserving local DRAM for latency-sensitive operations.

```bash
mooncake_master \
  --enable_cxl=true \
  --cxl_path=/dev/dax0.0 \
  --cxl_size=17179869184 \
  --allocation_strategy=cxl
```

---

### Container / Dynamic Network Interface

When the master runs in a container with a dynamic IP, use `--rpc_interface` to resolve the RPC address from a stable interface name:

```bash
mooncake_master \
  --rpc_interface=eth0 \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080
```

The master resolves the current IPv4 address of `eth0` at startup and uses it as the advertised RPC address.


---

## Metrics Endpoints

The master exposes Prometheus-style metrics on `--metrics_port`:

```bash
# Prometheus format
curl -s http://<master_host>:9003/metrics

# Human-readable summary
curl -s http://<master_host>:9003/metrics/summary
```

When tenant quota is enabled, `/metrics` also includes per-tenant quota gauges and quota counters:

- `mooncake_tenant_quota_requested_bytes{tenant_id}`
- `mooncake_tenant_quota_effective_bytes{tenant_id}`
- `mooncake_tenant_quota_used_bytes{tenant_id}`
- `mooncake_tenant_quota_reserved_bytes{tenant_id}`
- `mooncake_tenant_quota_committed_count{tenant_id}`
- `mooncake_tenant_quota_over_quota{tenant_id}`
- `mooncake_tenant_quota_explicit_policy{tenant_id}`
- `mooncake_tenant_quota_reject_total{tenant_id,reason}`
- `mooncake_tenant_evict_bytes_total{tenant_id}`
- `mooncake_tenant_quota_allocatable_capacity_bytes`
- `mooncake_tenant_quota_requested_bytes_sum`
- `mooncake_tenant_quota_effective_bytes_sum`

---

## Tenant Quota Management

Tenant quota admission is disabled by default. Enable it on the master when you want memory writes admitted against per-tenant quota:

```bash
mooncake_master \
  --enable_tenant_quota=true \
  --default_tenant_quota_bytes=1073741824 \
  --tenant_quota_pool_capacity_bytes=0
```

`tenant_quota_pool_capacity_bytes=0` uses the full registered memory capacity as the quota allocation pool. A nonzero value caps the capacity used to compute effective tenant quotas.

The same HTTP port used for metrics exposes the tenant quota admin API:

```bash
# List tenant quota snapshots
curl -s http://<master_host>:9003/api/v1/tenant_quotas

# Query one tenant
curl -s "http://<master_host>:9003/api/v1/tenant_quotas?tenant_id=tenant-a"

# Upsert an explicit policy. Explicit tenant policies must be positive.
curl -s -X PUT "http://<master_host>:9003/api/v1/tenant_quotas?tenant_id=tenant-a" \
  -H 'Content-Type: application/json' \
  -d '{"requested_quota_bytes":2147483648}'

# Delete an explicit policy so the tenant inherits the default policy again.
curl -s -X DELETE "http://<master_host>:9003/api/v1/tenant_quotas?tenant_id=tenant-a"

# Query or update the default requested quota. The default may be 0.
curl -s http://<master_host>:9003/api/v1/tenant_quotas/default
curl -s -X PUT http://<master_host>:9003/api/v1/tenant_quotas/default \
  -H 'Content-Type: application/json' \
  -d '{"requested_quota_bytes":1073741824}'
```

Each tenant quota snapshot returns:

```json
{
  "success": true,
  "data": {
    "tenant_id": "tenant-a",
    "requested_quota_bytes": 2147483648,
    "effective_quota_bytes": 2147483648,
    "used_bytes": 0,
    "reserved_bytes": 0,
    "committed_count": 0,
    "over_quota": false,
    "has_explicit_policy": true
  }
}
```

In HA mode, quota admin requests are served only by the active master service. Standby, candidate, or inactive services return HTTP 503. If tenant quota is disabled, the quota admin API returns HTTP 409 with `UNAVAILABLE_IN_CURRENT_MODE`.

---

## Quick Tips

- Scale `--rpc_thread_num` with available CPU cores and workload.
- Start with default eviction settings; adjust `--eviction_high_watermark_ratio` and `--eviction_ratio` based on memory pressure and object churn.
- Use `/metrics/summary` during bring-up; integrate `/metrics` with Prometheus/Grafana for production.
- For detailed SSD offload configuration (storage backends, eviction policies, io_uring), see the [SSD Offload guide](ssd-offload).
- For NVMe-oF SSD pool configuration see the [NVMe-oF SSD Pool Deployment Guide](nvmf-ssd-deployment-guide)
- For experimental 3FS (USRBIO) integration as a persistent storage backend, see the [3FS USRBIO Plugin guide](../getting_started/plugin-usage/3FS-USRBIO-Plugin).
- For detailed monitoring and observation see [Observability](../getting_started/observability)

:::{toctree}
:maxdepth: 1
:hidden:

ssd-offload
NvMe-Of SSD Pool<nvmf-ssd-deployment-guide>
HF3FS Plugin (Experimental)<../getting_started/plugin-usage/3FS-USRBIO-Plugin>
../getting_started/observability
:::

---

## Reference: Master Startup Flags

### RPC

| Flag | Default | Description |
|------|---------|-------------|
| `--rpc_port` | `0` → effective `50051` | RPC listen port. The literal default is `0`, which falls back to the deprecated `--port` (default `50051`) |
| `--rpc_thread_num` | `0` → effective `min(4, CPU cores)` | RPC worker threads. The literal default is `0`, which falls back to the deprecated `--max_threads` → `min(4, CPU cores)` |
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
| `--put_start_release_timeout_sec` | `600` (10 min) | Seconds before `PutStart`-allocated space is released

### Eviction & TTLs

| Flag | Default | Description |
|------|---------|-------------|
| `--default_kv_lease_ttl` | `5000` ms | Lease TTL for KV objects. Supports `5000ms`, `5s`, `30m`, `1h` |
| `--default_kv_soft_pin_ttl` | `1800000` ms | Soft pin TTL (30 min) |
| `--allow_evict_soft_pinned_objects` | `true` | Allow evicting soft-pinned objects |
| `--eviction_ratio` | `0.05` | Fraction evicted at high watermark |
| `--eviction_high_watermark_ratio` | `0.95` | Usage ratio triggering eviction |
| `--client_ttl` | `10` s | Seconds before a silent client is considered disconnected |

### Tenant Quota

| Flag | Default | Description |
|------|---------|-------------|
| `--enable_tenant_quota` | `false` | Enable per-tenant memory quota admission |
| `--default_tenant_quota_bytes` | `0` | Default requested quota for tenants without explicit policy; `0` is allowed and inherited-default tenants still share capacity left by explicit tenants |
| `--tenant_quota_pool_capacity_bytes` | `0` | Capacity used to compute effective tenant quotas; `0` means total registered memory capacity |

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
| `--snapshot_interval_seconds` | `600` (10 min) | Interval between snapshots |
| `--snapshot_child_timeout_seconds` | `300` (5 min) | Timeout per snapshot child process |
| `--snapshot_retention_count` | `2` | Number of recent snapshots retained |
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
| `--promotion_max_per_heartbeat` | `1` | Max promotion tasks handed to a single client per heartbeat. Each task is a synchronous SSD-read + RDMA-write on the client; serializing them avoids blocking past the client-liveness window |
| `--promotion_queue_limit` | `50000` | Max in-flight promotion tasks |
| `--quota_bytes` | `0` (90% of capacity) | Storage quota in bytes |
| `--enable_disk_eviction` | `true` | Enable disk eviction |

Start with `--enable_offload=true` for eager asynchronous SSD persistence after `Put` completion. Add `--offload_on_evict=true` when you want SSD writes to happen only when memory pressure selects an object for eviction. Add `--promotion_on_hit=true` to allow hot SSD-only data to be promoted back to DRAM, and tune `--promotion_admission_threshold` to control how many observed reads are required before promotion is queued.

FileStorage holder clients also accept the following environment variables:

| Env | Default | Description |
|-----|---------|-------------|
| `MOONCAKE_OFFLOAD_PROMOTION_WORKER_THREADS` | `1` | Background workers used to execute L2-to-L1 promotion tasks; `0` falls back to synchronous heartbeat execution |
| `MOONCAKE_OFFLOAD_PROMOTION_QUEUE_CAPACITY` | `1024` | Soft local backlog cap used to limit additional promotion pulls from the master |
| `MOONCAKE_OFFLOAD_PROMOTION_DRAIN_BATCH_SIZE` | `64` | Max promotion heartbeat pulls per worker per FileStorage heartbeat tick |

Keep the default worker count for latency-sensitive deployments. Raising it can drain bursty HiCache prefix-hit promotion backlogs faster, but it also increases SSD reads, transfer writes, and memory allocation pressure.

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
| `--global_file_segment_size` | `INT64_MAX` (unlimited) | Max available space for DFS segments; default does not cap DFS usage |

### NoF (NVMe-oF SSD Pool)

```{caution}
NVMe-oF SSD Pool (NoF) is an experimental feature.
```

Master-side flags for the NVMe-oF SSD pool. They control eviction within the NoF SSD tier and the heartbeat used to detect and unmount unresponsive NoF segments. For the client-side NoF I/O tuning (`MC_NOF_*`), see the [NVMe-oF SSD Pool Deployment Guide](nvmf-ssd-deployment-guide.md).

| Flag | Default | Description |
|------|---------|-------------|
| `--nof_eviction_ratio` | `0.05` | Fraction of objects evicted when NoF SSD space is full |
| `--nof_eviction_high_watermark_ratio` | `0.95` | Usage ratio that triggers eviction in the NoF SSD tier |
| `--nof_heartbeat_interval_sec` | `10` | How often the master probes each mounted NoF segment |
| `--nof_heartbeat_probe_timeout_ms` | `1000` | Timeout for a single NoF heartbeat probe |
| `--nof_heartbeat_failures_threshold` | `3` | Consecutive NoF heartbeat failures before a segment is unmounted |

### Master Configuration File

In addition to CLI flags, the master accepts JSON/YAML config files:

```bash
mooncake_master --config_path=mooncake-store/conf/master.yaml
```

```yaml
rpc_interface: "eth0"
rpc_port: 50051
```

---

(reference-client-configuration-tuning)=
## Reference: Client Configuration & Tuning

A client is configured through one of the **methods** introduced in [Start a Store Client](#start-a-store-client), plus a shared family of engine-tuning variables:

- **Method A — Programmatic (`setup()` arguments)**: you pass configuration as explicit Python arguments. `MOONCAKE_*` variables are **not** read in this method.
- **Method B — Service / Integration (`MOONCAKE_*` + CLI)**: `mooncake.mooncake_store_service` and the vLLM/SGLang connectors read `MOONCAKE_*` environment variables (via `MooncakeConfig`).
- **Method C — Resource-owning real client (`mooncake_client`)**: configured through `mooncake_client` CLI flags (see the **Method C** subsection below).
- **Engine runtime tuning (`MC_*`)**: low-level variables read by the C++ Transfer Engine / store client at runtime. They are orthogonal to the above and **apply to all methods**.

The Method A arguments and the `MOONCAKE_*` variables are the **same logical fields in two forms** (Method B maps onto Method A); note that the `mooncake_client` CLI (Method C) uses yet another spelling for some of them (e.g. `--device_names`, `--master_server_address`).

### Method A — Programmatic (`setup()` arguments)

Arguments of `MooncakeDistributedStore.setup(...)`:

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `local_hostname` | str | required | This node's hostname / IP |
| `metadata_server` | str | required | `P2PHANDSHAKE` / `http://…:8080/metadata` / etcd address |
| `global_segment_size` | int (bytes) | required | DRAM contributed to the cluster (the sample uses 3.2 GB) |
| `local_buffer_size` | int (bytes) | required | Transfer Engine buffer |
| `protocol` | str | required | `tcp` / `rdma` / `cxl` / `ascend` |
| `rdma_devices` | str | required | RDMA NIC(s), comma-separated (pass `""` for non-RDMA). **Keyword is `rdma_devices`, not `device_name`** |
| `master_server_addr` | str | required | Master `host:port`. **Keyword is `master_server_addr`, not `master_server_address`** |
| `engine` | TransferEngine | `None` | *(advanced)* Reuse an existing Transfer Engine instance instead of creating one |
| `enable_ssd_offload` | bool | `false` | *(advanced)* Enable client-side SSD offload |
| `ssd_offload_path` | str | empty | *(advanced)* SSD offload directory |
| `tenant_id` | str | `default` | *(advanced)* Tenant identifier |

```{note}
The first seven arguments have **no Python default** — the C++ defaults are not exposed by the pybind binding, so they must all be supplied (a bare `setup(local_hostname, metadata_server)` raises `TypeError`). Only `engine` / `enable_ssd_offload` / `ssd_offload_path` / `tenant_id` are optional. Also, in Method A only the `MC_*` engine variables below have any effect — `MOONCAKE_*` are ignored.
```

### Method B — Service / Integration (`MOONCAKE_*` + CLI)

`python -m mooncake.mooncake_store_service` (and the vLLM/SGLang connectors) build their configuration through `MooncakeConfig`, resolved in this order:

1. `--config <path>` CLI argument → load from that JSON file.
2. Otherwise `MOONCAKE_CONFIG_PATH` (if set) → load from that file; else read the `MOONCAKE_*` variables below.
3. `-D key=value` CLI overrides individual fields (keys must match the `MooncakeConfig` field names, e.g. `-Dmaster_server_address=...`).

```{note}
The store service CLI only accepts `--config`, `-D/--define`, `--port`, and `--max-wait-time`. There are **no** `--local_hostname` / `--metadata_server` / `--master_server` flags — use the `MOONCAKE_*` variables (or `-D`) instead.
```

| Variable | Maps to (`setup()` arg) | Default | Description |
|----------|-------------------------|---------|-------------|
| `MOONCAKE_MASTER` | `master_server_addr` | — (required unless `MOONCAKE_CONFIG_PATH`) | Master `host:port` |
| `MOONCAKE_TE_META_DATA_SERVER` | `metadata_server` | `P2PHANDSHAKE` | `P2PHANDSHAKE` / `http://…:8080/metadata` / etcd address |
| `MOONCAKE_PROTOCOL` | `protocol` | `tcp` | `tcp` / `rdma` / `cxl` / `ascend` |
| `MOONCAKE_DEVICE` | `rdma_devices` | empty | RDMA device(s), comma-separated; `auto-discovery` supported |
| `MOONCAKE_GLOBAL_SEGMENT_SIZE` | `global_segment_size` | `3355443200` (3.125 GiB) | DRAM contributed; accepts byte integer **or** suffixed form like `500gb` |
| `MOONCAKE_LOCAL_BUFFER_SIZE` | `local_buffer_size` | `1073741824` (1 GiB) | Transfer Engine buffer; same parsing as above |
| `MOONCAKE_LOCAL_HOSTNAME` | `local_hostname` | `localhost` | |
| `MOONCAKE_OFFLOAD_ENABLED` | `enable_ssd_offload` | `false` | Client-side SSD offload |
| `MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` | `ssd_offload_path` | empty | Offload directory |
| `MOONCAKE_CONFIG_PATH` | — | unset | Path to a JSON config file (takes precedence over the variables above) |

```{note}
`MooncakeConfig` (Method B) defaults `global_segment_size`/`local_buffer_size` to 3.125 GiB / 1 GiB. A direct `setup()` (Method A) has **no** default for these — they are required arguments. Unlike `MC_STORE_LOCAL_HOT_CACHE_SIZE` (raw bytes only), `MOONCAKE_GLOBAL_SEGMENT_SIZE` / `MOONCAKE_LOCAL_BUFFER_SIZE` accept human-readable suffixes (`kb`/`mb`/`gb`/…) because they are parsed by `MooncakeConfig`.
```

**Launch examples:**

```bash
# P2P handshake
MOONCAKE_MASTER=127.0.0.1:50051 \
MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE \
python -m mooncake.mooncake_store_service

# HTTP metadata server
MOONCAKE_MASTER=127.0.0.1:50051 \
MOONCAKE_TE_META_DATA_SERVER=http://127.0.0.1:8080/metadata \
python -m mooncake.mooncake_store_service
```

Or via a JSON config file. The service also exposes a lightweight HTTP API (on `--port`, default `8080`) for manual `Get`/`Put` debugging:

```json
{
  "local_hostname": "localhost",
  "metadata_server": "http://127.0.0.1:8080/metadata",
  "global_segment_size": 268435456,
  "local_buffer_size": 268435456,
  "protocol": "tcp",
  "device_name": "",
  "master_server_address": "127.0.0.1:50051"
}
```

```bash
python -m mooncake.mooncake_store_service --config=<config_path> --port=8081
```

### Method C — Resource-owning Real Client (`mooncake_client`)

Run the `mooncake_client` binary as a standalone RPC process that owns storage resources; application processes (vLLM / SGLang) use lightweight **dummy clients** to forward requests to it. It connects to the master and listens on port `50052` by default.

```bash
mooncake_client \
  --global_segment_size="4GB" \
  --master_server_address="127.0.0.1:50051" \
  --metadata_server="http://127.0.0.1:8080/metadata"
```

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `0.0.0.0` | Client service bind host |
| `--port` | `50052` | Client service listen port |
| `--global_segment_size` | `4 GB` | Global segment size contributed by the client |
| `--master_server_address` | `127.0.0.1:50051` | Master service address |
| `--metadata_server` | `http://127.0.0.1:8080/metadata` | Transfer Engine metadata service |
| `--protocol` | `tcp` | Transfer protocol |
| `--device_names` | empty | Transfer device name(s), comma-separated |
| `--threads` | `1` | Client worker thread count |
| `--enable_offload` | `false` | Enable client-side SSD offload |
| `--start_offload_rpc_server` | `true` | Start the offload RPC server for dummy clients |

### Engine Runtime Tuning (`MC_*`)

The following `MC_*` variables are read directly by the engine/client at runtime and **apply to all methods (A, B, and C)**.

#### Runtime Protocol

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_RPC_PROTOCOL` | `tcp` | RPC transport protocol between master and clients: `tcp` or `rdma` |
| `MC_RPC_TIMEOUT_MS` | `30000` | Per-request deadline (ms) for all client→master RPCs. Applies uniformly to every RPC method. A negative value disables the timeout. On expiry the call returns `RPC_TIMEOUT` |
| `MC_RPC_CONNECT_TIMEOUT_MS` | `30000` | Connection-establishment timeout (ms) for the master RPC client |
| `MC_USE_TENT` / `MC_USE_TEV1` | unset | Set to any value to enable the TENT (next-gen) transfer engine |
| `MC_STORE_CLUSTER_ID` | unset | Cluster ID label attached to client metrics |

#### Topology Discovery

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_MS_AUTO_DISC` | unset | Auto-discover NIC/GPU topology. Set `1` to force on, `0` to provide `rdma_devices` manually. When unset, auto-discovery is **off** except for `rdma`/`efa` protocols when no `rdma_devices` are given, where it defaults **on**. Ignored when TENT is enabled |
| `MC_MS_FILTERS` | empty | Comma-separated NIC whitelist (e.g., `mlx5_0,mlx5_2`) |

When `MC_MS_AUTO_DISC=0`, pass `rdma_devices` (comma-separated) to the Python `setup()` call.

#### Transfer Engine Metrics (disabled by default)

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_TE_METRIC` | `0` | Set to `1` to enable engine metrics. Not supported with TENT |
| `MC_TE_METRIC_INTERVAL_SECONDS` | `5` | Seconds between reports |

#### Client Metrics (enabled by default)

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_CLIENT_METRIC` | `1` | Set `0` to disable |
| `MC_STORE_CLIENT_METRIC_INTERVAL` | `0` | Reporting interval; `0` collects but does not periodically report |
| `MC_STORE_CLIENT_MIN_PORT` | `12300` | Min local port for client connections |
| `MC_STORE_CLIENT_MAX_PORT` | `14300` | Max local port for client connections |

#### Local Hot Cache

Local hot cache provides a DRAM read cache on top of SSD-resident objects for faster access.

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_LOCAL_HOT_CACHE_SIZE` | unset | Size of the local hot cache **in raw bytes** (decimal integer, e.g., `8589934592` for 8 GB). Suffixed forms like `"8gb"` are **not** parsed. Set to a positive value to enable the hot cache |
| `MC_STORE_LOCAL_HOT_BLOCK_SIZE` | `16777216` (16 MB) | Block size for hot cache **in raw bytes** (decimal integer, e.g., `2097152` for 2 MB). Suffixed forms like `"2mb"` are **not** parsed. Only read when the hot cache is enabled |
| `MC_STORE_LOCAL_HOT_CACHE_USE_SHM` | unset | Set `1` to use memfd-backed shared memory |
| `MC_STORE_LOCAL_HOT_ADMISSION_THRESHOLD` | unset | Minimum CountMinSketch count before a key is admitted to hot cache |

#### Local Memory Optimization

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_MEMCPY` | auto | Prefer local memcpy when source/destination are on the same client. When unset, auto-detected by transport: **enabled** in a TCP-only environment, **disabled** when an RDMA/other transport is available. Accepts `1`/`true`/`yes`/`on` or `0`/`false`/`no`/`off` to override |
| `MC_STORE_CLIENT_SETUP_RETRIES` | `20` | Number of times to retry client registration on failure |
| `MC_CXL_DEV_SIZE` | unset | CXL device size in raw bytes for client-side allocation. **Required when `protocol="cxl"`** — the client aborts at startup if it is missing |

#### MMap Buffer & HugePages

| Variable | Default | Description |
|----------|---------|-------------|
| `MC_STORE_USE_HUGEPAGE` | unset | Set `1` to request HugeTLB-backed `mmap()` |
| `MC_STORE_HUGEPAGE_SIZE` | `2MB` | Supported: `2MB`, `1GB` |
| `MC_MMAP_ARENA_POOL_SIZE` | unset | Pre-allocated arena pool size (e.g., `8gb`). Explicitly set to enable the arena |
| `MC_DISABLE_MMAP_ARENA` | unset | Disable arena, fall back to per-call `mmap()`. Accepts `1`/`true`/`yes`/`on` (or `0`/`false`/`no`/`off`) |

#### yalantinglibs Log Level

```bash
export MC_YLT_LOG_LEVEL=info
```

Available: `trace`, `debug`, `info`, `warn` (or `warning`), `error`, `critical`. When unset (or set to an unrecognized value), the level defaults to `warn`.
