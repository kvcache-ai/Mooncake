# TENT monitoring assets

This directory contains deployment-neutral starting points for operating TENT:

- `prometheus/tent-alerts.yaml`: Prometheus alerting rules for telemetry health,
  logical and physical transfer failures, worker stalls, admission pressure,
  staging cleanup, and RDMA errors.
- `grafana/tent-overview.json`: an importable Grafana overview dashboard.

The examples assume that the Prometheus scrape target has `job` and `instance`
labels. Adjust the `job=~"tent.*"` selector in the endpoint-down alert if the
deployment uses a different job name. Thresholds are conservative defaults and
should be tuned against the workload's SLO and normal traffic floor.

See the [TENT observability runbook](../../../docs/source/design/tent/observability-runbook.md)
for symptom-driven triage and metric-to-log-to-trace correlation.
