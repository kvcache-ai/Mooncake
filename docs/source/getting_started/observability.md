# Observability

This document describes how to monitor and observe a running Mooncake Store deployment.

## Master Metrics Log

When started, the Mooncake master periodically prints a metrics summary log every 10 seconds (configurable via `kMetricReportIntervalSeconds`). This log provides a comprehensive snapshot of the master's runtime state.

### Log Format

```
I0512 15:03:30.321475 239489 rpc_service.cpp:269] Master Admin Metrics: role=leader, state=serving, service_ready=true, master={...}, ha={...}, leader=127.0.0.1:50051, view_version=1
```

Each log line contains:
- **GLog header**: timestamp, thread ID, source file and line number
- **role**: HA role — `leader` or `standby`
- **state**: HA runtime state — `serving`, `starting`, `stopping`, etc.
- **service_ready**: whether the gRPC service is accepting requests
- **master**: master metrics block (see below)
- **ha**: HA metrics block
- **leader**: (only when available) the current leader address and view version

### Master Metrics Block

A typical `master={...}` block looks like this:

```
Mem Storage: 94.09 MB / 100.00 MB (94.1%) | SSD Storage: 0 B / 0 B | Keys: 16058 (soft-pinned: 0) | Clients: 1 | Requests (Success/Total per sec): PutStart=0.00/0.00, PutEnd=0.00/0.00, PutRevoke=0.00/0.00, Get=0.00/0.00, Exist=0.00/0.00, Del=0.00/0.00, DelAll=0.00/0.00, Ping=1.00/1.00, CopyStart=0.00/0.00, CopyEnd=0.00/0.00, CopyRevoke=0.00/0.00, MoveStart=0.00/0.00, MoveEnd=0.00/0.00, MoveRevoke=0.00/0.00, EvictDiskReplica=0.00/0.00 | Batch Requests (per sec, Req=Success/PartialSuccess/Total, Item=Success/Total): PutStart:(Req=0.00/0.00/0.00, Item=0.00/0.00), PutEnd:(Req=0.00/0.00/0.00, Item=0.00/0.00), PutRevoke:(Req=0.00/0.00/0.00, Item=0.00/0.00), Get:(Req=0.00/0.00/0.00, Item=0.00/0.00), ExistKey:(Req=0.00/0.00/0.00, Item=0.00/0.00), QueryIp:(Req=0.00/0.00/0.00, Item=0.00/0.00), Clear:(Req=0.00/0.00/0.00, Item=0.00/0.00), CreateMoveTask:(Req=0.00/0.00), CreateCopyTask:(Req=0.00/0.00), QueryTask:(Req=0.00/0.00), FetchTasks:(Req=0.00/0.00), MarkTaskToComplete:(Req=0.00/0.00) | Eviction: Success/Attempts=0/0, AllocFail=0, keys=0, size=0 B | Discard: Released/Total=0/0, StagingSize=0 B | Snapshots: Success=0, Fail=0
```

Request counters are reported as **rates per second** over the time window between two consecutive log outputs (10 seconds by default). Real-time state values (storage, key count, client count, discard staging size) are not rate-limited and reflect the current value at log time.

The metrics block consists of the following sections:

#### Storage

| Field | Description |
|-------|-------------|
| `Mem Storage` | Current memory usage / total memory capacity, with percentage |
| `SSD Storage` | Current SSD-backed storage usage / total SSD capacity |

#### Keys and Clients

| Field | Description |
|-------|-------------|
| `Keys` | Total number of keys managed by the master |
| `soft-pinned` | Number of keys with active soft-pin leases (protected from eviction) |
| `Clients` | Number of currently connected clients |

#### Requests (Success/Total per sec)

Rate counters for individual (non-batch) RPC requests over the last time window. Each shows `<success_rate>/<total_rate>` in requests per second:

| Counter | Description |
|---------|-------------|
| `PutStart` | Put object allocation requests |
| `PutEnd` | Put object commit requests |
| `PutRevoke` | Put object cancellation requests |
| `Get` | Get replica list requests |
| `Exist` | Key existence check requests |
| `Del` | Single key deletion requests |
| `DelAll` | Delete-all objects requests |
| `Ping` | Client heartbeat/ping requests |
| `CopyStart` | Copy object allocation requests |
| `CopyEnd` | Copy object commit requests |
| `CopyRevoke` | Copy object cancellation requests |
| `MoveStart` | Move object allocation requests |
| `MoveEnd` | Move object commit requests |
| `MoveRevoke` | Move object cancellation requests |
| `EvictDiskReplica` | Evict disk replica requests |

#### Batch Requests (per sec)

Batch operations aggregate multiple items into a single RPC. Rates are per second over the last time window. Format: `Req=<success>/<partial_success>/<total>`, `Item=<success_items>/<total_items>`:

| Counter | Description |
|---------|-------------|
| `PutStart` | Batch put object allocation requests |
| `PutEnd` | Batch put object commit requests |
| `PutRevoke` | Batch put object cancellation requests |
| `Get` | Batch get replica list requests |
| `ExistKey` | Batch key existence check requests |
| `QueryIp` | Batch query IP requests |
| `Clear` | Batch replica clear requests |

A request is considered "partial success" when it succeeds for some items but not all.

#### Task Operations

| Counter | Description |
|---------|-------------|
| `CreateMoveTask` | Move task creation requests |
| `CreateCopyTask` | Copy task creation requests |
| `QueryTask` | Task status query requests |
| `FetchTasks` | Pending task fetch requests (polled by store clients) |
| `MarkTaskToComplete` | Task completion acknowledgement requests |

#### Eviction & Discard

Eviction counters are **deltas** between two consecutive log outputs — they show what happened in the time window, not cumulative totals.

| Field | Description |
|-------|-------------|
| `Eviction: Success/Attempts` | Eviction rounds that succeeded at least partially vs. total attempts in this window |
| `AllocFail` | Number of PutStart failures caused by replica allocation failure (triggers eviction) in this window |
| `keys` | Number of keys evicted in this window |
| `size` | Total size of evicted data in this window |
| `Discard: Released/Total` | Released (cleaned up) vs. total discarded PutStart staging replicas (live values) |
| `StagingSize` | Current size of discarded but not-yet-released staging buffers (live value) |

## Prometheus Metrics Endpoint

Mooncake master exposes Prometheus-format metrics at the HTTP admin endpoint. This allows integration with Prometheus, Grafana, or any Prometheus-compatible monitoring stack.

### Endpoints

The admin HTTP server runs on `metrics_port` (default: **9003**) and exposes the following endpoints:

| Endpoint | Content-Type | Description |
|----------|-------------|-------------|
| `GET /metrics` | `text/plain; version=0.0.4` | All metrics in Prometheus exposition format |
| `GET /metrics/summary` | `text/plain; version=0.0.4` | Human-readable summary (same content as the periodic log) |
| `GET /health` | `application/json` | Health check with role, HA state, and service readiness |
| `GET /role` | `text/plain` | Current HA role (`leader` / `standby`) |
| `GET /ha_status` | `text/plain` | Current HA runtime state (`serving` / `starting` / etc.) |

### Usage

**Scrape the /metrics endpoint with Prometheus:**

Add a scrape config to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'mooncake-master'
    static_configs:
      - targets: ['<master-host>:9003']
    metrics_path: '/metrics'
```

**Quick check with curl:**

```bash
# Get Prometheus metrics
curl http://<master-host>:9003/metrics

# Get human-readable summary
curl http://<master-host>:9003/metrics/summary

# Check health
curl http://<master-host>:9003/health
```

### Configuration

The admin HTTP server is configured in the master config file (`master.json` or `master.yaml`):

```json
{
  "enable_metric_reporting": true,
  "metrics_port": 9003,
  ...
}
```

Set `enable_metric_reporting` to `false` to disable the periodic metrics log. HTTP endpoints (`/metrics`, `/health`, etc.) remain available regardless of this setting.
