# Monitoring Mooncake with Prometheus and Grafana

This document provides instructions on how to set up and run Prometheus and Grafana to monitor the `mooncake_master` service.

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

    Grafana is pre-configured with the Prometheus datasource and a simple dashboard for the `mooncake_master`.

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

