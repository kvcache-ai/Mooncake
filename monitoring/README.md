# Monitoring Mooncake with Prometheus and Grafana

This document provides instructions on how to set up and run Prometheus and Grafana to monitor the `mooncake_master`, Store clients, and the Classic Transfer Engine.

## 1. Running Monitoring Services

The Prometheus and Grafana services are configured to run inside Docker containers.

### Prerequisites

- Docker
- Docker Compose

### Steps

1.  **Navigate to the monitoring directory:**

    ```bash
    cd monitoring
    ```

2.  **Start the containers:**

    ```bash
    docker-compose up -d
    ```

    This command will pull the Prometheus and Grafana images and start the containers in detached mode.

3.  **Access the Prometheus UI:**

    You can access the Prometheus web interface by navigating to `http://localhost:9090` in your web browser.

4.  **Access the Grafana UI:**

    You can access the Grafana web interface by navigating to `http://localhost:3000` in your web browser.

    -   **Default credentials:** `admin` / `admin`

    Grafana is pre-configured with the Prometheus datasource and two dashboards:

- **Mooncake Master Dashboard** (`mooncake.json`) retains the existing Master-only view.
- **Mooncake Core | TE, Store & Master** (`mooncake-core.json`) adds the unified Overview, Store, Classic Transfer Engine, and Master view. Use its datasource, job, instance, and Store operation selectors to narrow the displayed targets.

### Configuration

-   **`docker-compose.yml`**: Defines the Prometheus and Grafana services.
-   **`prometheus/prometheus.yml`**: The Prometheus configuration file. By default, it's configured to scrape metrics from the `mooncake_master`.
-   **`grafana/`**: Contains Grafana provisioning files for the datasource and a sample dashboard.

The `prometheus.yml` is configured to scrape metrics from `host.docker.internal:9003`. The `host.docker.internal` hostname allows the Prometheus container to communicate with services running on the host machine. If you are running on Linux and `host.docker.internal` is not available, you may need to add `extra_hosts: ["host.docker.internal:host-gateway"]` to the prometheus service definition in `docker-compose.yml`.

## 2. Running Mooncake Master with Metrics

To be monitored by Prometheus, the `mooncake_master` must be started with metric reporting enabled.

### Steps

1.  **Start the `mooncake_master`:**

    Run the `mooncake_master` executable with the following flags to enable the metrics endpoint. For a complete list of flags, see the [Mooncake Store Deployment Guide](../docs/source/deployment/mooncake-store-deployment-guide.md).

    ```bash
    # Make sure you are in the root directory of the project
    # The path to mooncake_master might vary depending on your build output directory
    ./build/mooncake_master \
      --metrics_port=9003 \
      --enable_metric_reporting=true
    ```

    Ensure that `--metrics_port` is set to `9003`, as this is the port Prometheus is configured to scrape.

2.  **Verify Metrics Endpoint:**

    You can verify that the metrics endpoint is active by running:

    ```bash
    curl -s http://localhost:9003/metrics
    ```

    This should return a list of Prometheus-style metrics.

Once both Prometheus and `mooncake_master` are running, you can go to the Prometheus UI (`http://localhost:9090`), navigate to **Status -> Targets**, and you should see the `mooncake-master` job with a state of "UP". You can then explore the pre-built dashboard in Grafana.

## 3. Scraping Store Clients and the Classic Transfer Engine

The Master scrape target alone is enough for the Master panels. Store and TE panels require separate opt-in client endpoints. Enable the client-local HTTP server as described in the [Observability guide](../docs/source/getting_started/observability.md), then enable TE metrics in each client process that should export them:

```bash
export MC_TE_METRIC=1
export MC_TE_METRIC_HTTP_HOST=127.0.0.1
export MC_TE_METRIC_HTTP_PORT=9400
```

`MC_TE_METRIC` collects the metrics. `MC_TE_METRIC_HTTP_PORT` separately opts in to the TE HTTP exporter; leaving it at its default `0` does not start an HTTP server. `MC_TE_METRIC_INTERVAL_SECONDS` controls periodic summary reporting, not the Prometheus scrape interval.

The loopback address above is appropriate only when the scraper runs in the same network namespace. For a remote Prometheus or a container that reaches the client through another interface, bind a private address reachable only from a trusted monitoring network. The TE exporter host defaults to `0.0.0.0` when a port is configured, so do not expose its unauthenticated metrics endpoints on a public interface. Apply the same restriction to Store client and Master HTTP endpoints with firewall or network policy.

Add the enabled endpoints to `prometheus/prometheus.yml`, replacing the example hosts and ports with the actual deployment:

```yaml
scrape_configs:
  - job_name: mooncake-master
    static_configs:
      - targets: ['master-host:9003']
  - job_name: mooncake-client
    static_configs:
      - targets: ['client-a:9300', 'client-b:9300']
  - job_name: mooncake-classic-te
    static_configs:
      - targets: ['client-a:9400', 'client-b:9400']
```

The Store and TE endpoints can belong to the same process but remain independent scrape targets. The dashboard does not join their series or treat them as the same measurement boundary. `MC_TE_METRIC` is not supported with TENT; this dashboard intentionally uses TE `mooncake_te_*` series, not TENT metric names.

## 4. Reading the Core Dashboard

- **Overview** summarizes per-target scrape state, per-client heartbeat status and age, TE non-success ratios, p99 latency, completed throughput, and terminal observation rates. Target and heartbeat status are never averaged across instances, and missing heartbeat series remain Unknown or inapplicable.
- **Store** shows per-client heartbeat details and operation-attributed counts, logical payload bytes, and latency by `op_name`. Store operation counts are not a unified success rate.
- **TE task results and latency** separates completed, failed or canceled, and timed-out terminal observations. `requests_total` counts terminal observations, not submissions. A timeout is not rewritten as a success if a late completion arrives.
- **TE request rate and throughput** shows terminal-observation rates and completed-transfer throughput. Bytes and latency include completed transfers only, and latency measures submission to the first terminal observation, including polling delay.
- **Master** shows batch request and item outcomes, eviction, capacity, clients, and keys. Batch item rates use an `items/s` suffix, TE task rates use `tasks/s`, and evicted-key rates use `keys/s`; requests, items, tasks, operations, keys, bytes, ratios, and latency never share one axis.

Derived non-success ratios have no denominator when there is no terminal TE traffic, so Grafana displays **No data** instead of a misleading zero. All rate queries use `$__rate_interval`, and Prometheus handles counter resets. The dashboard does not add request IDs, object keys, segment IDs, peer addresses, or raw pointers as labels.

Run the static dashboard contract checks after editing the JSON:

```bash
python3 monitoring/tests/test_core_dashboard.py
```
