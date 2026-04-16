# Monitoring Mooncake with Prometheus and Grafana

Mooncake Master exposes a Prometheus-compatible `/metrics` endpoint. The `monitoring/` directory in the repository contains a ready-to-use Docker Compose stack that wires Prometheus and Grafana together with a pre-built dashboard for `mooncake_master`.

## Quick Start

### Prerequisites

- Docker and Docker Compose installed on the monitoring host.
- `mooncake_master` accessible from the monitoring host.

### Step 1: Start the Monitoring Stack

```bash
cd monitoring
docker-compose up -d
```

This starts two containers:
- **Prometheus** — scrapes metrics from `mooncake_master` every 15 s.
- **Grafana** — pre-configured with a Prometheus data source and a sample dashboard.

### Step 2: Open the UIs

| UI | URL | Credentials |
|----|-----|-------------|
| Prometheus | <http://localhost:9090> | — |
| Grafana | <http://localhost:3000> | `admin` / `admin` |

Navigate to **Grafana → Dashboards** to find the pre-built `mooncake_master` dashboard.

### Step 3: Start `mooncake_master` with Metrics Enabled

```bash
./build/mooncake_master \
    --metrics_port=9003 \
    --enable_metric_reporting=true \
    --rpc_port=50051
```

Verify the metrics endpoint is live:

```bash
curl -s http://localhost:9003/metrics | head -20
```

You should see Prometheus-format lines such as:

```
# HELP mooncake_master_kv_object_count Total number of KV objects in the store
# TYPE mooncake_master_kv_object_count gauge
mooncake_master_kv_object_count 4096
```

Check **Prometheus → Status → Targets** — the `mooncake-master` job should show `UP`.

## Configuration Files

| File | Description |
|------|-------------|
| `monitoring/docker-compose.yml` | Service definitions for Prometheus and Grafana |
| `monitoring/prometheus/prometheus.yml` | Prometheus scrape configuration |
| `monitoring/grafana/` | Grafana provisioning (data source + dashboard JSON) |

### Prometheus Scrape Target

By default, `prometheus.yml` scrapes `host.docker.internal:9003`. On Linux, `host.docker.internal` may not be available — add the following to the `prometheus` service in `docker-compose.yml`:

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
```

To scrape a remote `mooncake_master`, change the target in `prometheus/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: mooncake-master
    static_configs:
      - targets:
          - "10.0.0.1:9003"   # replace with actual master host
```

## Available Metrics

The `/metrics/summary` endpoint (human-readable) and `/metrics` endpoint (Prometheus format) expose the following categories:

| Category | Example Metric | Description |
|----------|---------------|-------------|
| KV objects | `mooncake_master_kv_object_count` | Total objects in the store |
| Memory | `mooncake_master_segment_free_bytes` | Free bytes per segment |
| Eviction | `mooncake_master_eviction_total` | Eviction events |
| Tasks | `mooncake_master_pending_tasks` | Pending transfer tasks |
| RPC | `mooncake_master_rpc_requests_total` | Total RPC requests served |

Browse all available metrics via:

```bash
curl -s http://localhost:9003/metrics/summary
```

## Grafana Alerts (Optional)

To set up alerts in Grafana:

1. Open the dashboard and click the panel you want to alert on.
2. Choose **Edit → Alert → Create alert rule**.
3. Example: alert when `mooncake_master_segment_free_bytes` drops below 10 % of total.

## Multi-Master Monitoring

To monitor multiple masters in one Prometheus instance, add multiple targets:

```yaml
scrape_configs:
  - job_name: mooncake-masters
    static_configs:
      - targets:
          - "master-0:9003"
          - "master-1:9003"
          - "master-2:9003"
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
```

## See Also

- [Mooncake Store Deployment Guide](mooncake-store-deployment-guide) — master startup flags including `--metrics_port`
- [Mooncake Store HA Hot Standby](ha-hot-standby) — monitoring standby lag
