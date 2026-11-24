# Monitoring Mooncake with Prometheus and Grafana

This document provides instructions on how to set up and run Prometheus and Grafana to monitor the `mooncake_master`, Store clients, and TE.

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
- **Mooncake Core | TE, Store & Master** (`mooncake-core.json`) adds the unified Overview, Store, TE, and Master view. Use its datasource, job, instance, and Store operation selectors to narrow the displayed targets.

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

## 3. Scraping Store Clients and TE

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
- **TE request rate and throughput** shows terminal-observation rates and completed-transfer throughput. Bytes and latency include completed transfers only. Latency measures submission to the completion callback timestamp when available, falling back to the terminal observation time otherwise.
- **Master** shows batch request and item outcomes, eviction, capacity, clients, and keys. Batch item rates use an `items/s` suffix, TE task rates use `tasks/s`, and evicted-key rates use `keys/s`; requests, items, tasks, operations, keys, bytes, ratios, and latency never share one axis.

Derived non-success ratios have no denominator when there is no terminal TE traffic, so Grafana displays **No data** instead of a misleading zero. All rate queries use `$__rate_interval`, and Prometheus handles counter resets. Aggregate metrics avoid request IDs, object keys, segment IDs, peer labels, and raw pointers.

Run the static dashboard contract checks after editing the JSON:

```bash
python3 monitoring/tests/test_core_dashboard.py
```

## 5. Classic TE RDMA Rails

A rail connects one local RDMA NIC to one remote RDMA NIC. Set `MC_TE_METRIC_RAIL=1` before starting the process and enable the TE HTTP endpoint with `MC_TE_METRIC=1` and a nonzero `MC_TE_METRIC_HTTP_PORT` as above. Rail collection is off by default. The four counters appear in `/metrics` with labels `local_nic`, `peer` (the advertised server name, including its port if present), and `peer_nic`.

| Metric | Counts |
| --- | --- |
| `mooncake_te_rail_completions_total` | Retired RDMA work requests (WRs), including errors and flushes |
| `mooncake_te_rail_bytes_total` | Payload bytes of those WRs, including failed attempts and retries |
| `mooncake_te_rail_latency_us_sum` | Sum of post-to-observed-completion time, in microseconds |
| `mooncake_te_rail_errors_total` | Error CQEs excluding `IBV_WC_WR_FLUSH_ERR` |

For a CQE that retires several WRs, the latency sum uses the completing WR's post timestamp for each retired WR. Missing or non-earlier timestamps add zero to the sum. Rate of the sum divided by rate of completions gives the mean latency per WR. This includes time until software polls the CQ.

This is an opt-in exception to the no-peer-label rule. `MC_TE_METRIC_RAIL_MAX` defaults to 4096 distinct rails per process. Entries persist through endpoint eviction and reconnection to the same NIC path. New advertised ports can create new entries. After the cap, unseen rails share one series per counter with all three labels set to `other`. The maximum is four times the configured cap plus four overflow series per process. Set the cap to `0` for overflow only.

The **TE rails** row shows per-rail mean latency, retired bytes/s, non-flush errors/s, and latency divided by the median of active rails on the same instance and local NIC. A single local NIC showing high latency across peers points to that NIC or its host. The same remote NIC showing high latency from multiple senders points to the remote side. An outlier confined to one rail points to its network path or switch. **Mean latency by remote NIC across all instances** combines the selected jobs by `(peer, peer_nic)`, weighted by completions. It ignores the instance selector. The median and remote-NIC panels exclude overflow.

### RDMA validation

Run on an isolated RDMA test fabric. Set `target_host` to the target's reachable hostname or IP, and set `target_nics` and `source_nics` to comma-separated RDMA device names. Clear any TCP-only transport override. On the target, start classic `tebench` and leave it running for the whole comparison.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DWITH_METRICS=ON -DUSE_TENT=OFF -DWITH_STORE=OFF
cmake --build build --target tebench -j 8
tebench=./build/mooncake-transfer-engine/benchmark/tebench
MC_TE_FILTERS="$target_nics" MC_USE_RDMA_TWOSIDED=0 \
  "$tebench" --backend=classic --xport_type=rdma \
  --metadata_type=p2p --seg_name="$target_host" --seg_type=DRAM
```

On the initiator, set `target` to the `--target_seg_name` value printed by the target. Use the same build, target, NICs, and CPU placement for each A/B pair. Repeat with `modes='1 0'` to alternate run order. Compare the median throughput and latency across matched runs. Keep HTTP scrape activity identical for both modes.

```bash
tebench=./build/mooncake-transfer-engine/benchmark/tebench
modes='0 1'
run_rail_bench() {
  local batch=$1 rails=$2 duration=$3
  MC_TE_FILTERS="$source_nics" MC_USE_RDMA_TWOSIDED=0 \
    MC_TE_METRIC=1 MC_TE_METRIC_RAIL="$rails" \
    MC_TE_METRIC_HTTP_HOST=127.0.0.1 MC_TE_METRIC_HTTP_PORT=9400 \
    "$tebench" --backend=classic --xport_type=rdma --metadata_type=p2p \
    --target_seg_name="$target" --op_type=write --warmup_ms=5000 \
    --start_block_size=16384 --max_block_size=16384 \
    --start_batch_size="$batch" --max_batch_size="$batch" --duration="$duration" \
    --result_output_jsonl="rails-${batch}-${rails}.jsonl"
}
for batch in 65536 128; do
  for rails in $modes; do run_rail_bench "$batch" "$rails" 30; done
done
```

For the congestion check, choose working rails on the initiator with at least two different `local_nic` values. Set `hot_nic` to one of them. Run `ib_write_bw` against a dedicated receiver to isolate load on the initiator NIC. Use the same port, duration, and fabric parameters at both ends. See the [perftest device and duration options](https://github.com/linux-rdma/perftest/blob/master/README).

```bash
# Dedicated receiver. Set receiver_nic to the test device.
ib_write_bw -d "$receiver_nic" -p 18515 -D 45 --report_gbits

# Initiator. Set receiver_ip to the bandwidth test receiver's address.
run_rail_bench 128 1 120 &
bench_pid=$!
sleep 10
curl -fsS http://127.0.0.1:9400/metrics > baseline-start.prom
sleep 15
curl -fsS http://127.0.0.1:9400/metrics > baseline-end.prom
ib_write_bw -d "$hot_nic" -p 18515 -D 45 --report_gbits "$receiver_ip" &
load_pid=$!
sleep 10
curl -fsS http://127.0.0.1:9400/metrics > congested-start.prom
sleep 15
curl -fsS http://127.0.0.1:9400/metrics > congested-end.prom
wait "$load_pid"
wait "$bench_pid"
```

For each rail with completions in both windows, divide the change in `mooncake_te_rail_latency_us_sum` by the change in `mooncake_te_rail_completions_total`. Compare the two results for the rails on `hot_nic` and for the unchanged reference NIC. Record the `ib_write_bw` throughput with the comparison.
