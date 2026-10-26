# TENT Observability Runbook

This runbook starts from an externally visible symptom and narrows it to a
TENT subsystem by combining readiness, metrics, structured logs, and retained
trace spans. It assumes TENT was built with `-DTENT_METRICS_ENABLED=ON` and the
metrics HTTP server is reachable.

The deployable dashboard and alert examples live under
`mooncake-transfer-engine/tent/monitoring/`.

## First five minutes

Capture all four responses before restarting the process:

```bash
curl -fsS http://HOST:PORT/live
curl -sS -i http://HOST:PORT/ready
curl -fsS http://HOST:PORT/diagnostics
curl -fsS http://HOST:PORT/metrics
```

Then preserve the following context:

- incident start and end time, host or pod, build version, and Git commit from
  `tent_metrics_build_info` or `/diagnostics`;
- affected operation (`read` or `write`) and the transport labels involved;
- five minutes of metrics before and after the symptom;
- structured events carrying the affected `trace_id` or `request_id`;
- configuration generation and recent configuration changes.

Do not treat `/health` as an engine-readiness check. It only proves that the
metrics HTTP event loop can serve a request. Use `/live` for liveness and
`/ready` for traffic readiness.

`/ready` is safe for frequent probes: it reads bounded aggregate state and
does not walk batches or tasks. Collect `/diagnostics` for incident detail,
but do not use it as the Kubernetes readiness probe because it may scan live
engine state.

## Triage flow

1. If `/live` is unavailable, check process health and metrics port ownership.
   Transfer metrics cannot be trusted until the telemetry plane is reachable.
2. If `/live` is `200` but `/ready` is `503`, use the readiness reason table
   below. Save `/diagnostics`; it is redacted and safe for an incident ticket.
3. If `/ready` is `200`, determine whether logical requests are failing or only
   physical attempts are degrading. Failover can hide transport failures from
   the logical success rate.
4. For latency or stalls, require both outstanding-work gauges and a stale
   worker timestamp. An idle worker is not stalled.
5. Narrow by bounded labels, find a matching structured failure event, and
   pivot on its `trace_id`. When available, a Grafana exemplar performs the
   same pivot directly from the attempt-latency histogram.

### Readiness reasons

| Reason | Meaning | Next checks |
|---|---|---|
| `engine_diagnostics_unavailable` | No engine owns the diagnostics provider | Confirm engine construction and teardown timing; do not infer transport health |
| `engine_unavailable` | Engine construction or a mandatory dependency failed | Startup errors, metadata configuration, loaded build identity |
| `control_plane_unavailable` | Metadata/control-plane client is unavailable | `tent_rpc_attempts_total`, RPC outcomes, metadata service reachability |
| `no_transport_available` | No usable transport was loaded | Loaded transports in `/diagnostics`, selection events, transport initialization logs |
| `progress_worker_unavailable` | The configured progress worker is not running | Worker startup/shutdown logs and engine availability |
| `progress_worker_stalled` | Work is old and progress has stopped | In-flight attempts, oldest work age, worker progress age, transport-specific completion metrics |

## Decide which layer is failing

Use rates over at least five minutes unless the symptom is a hard outage.

```promql
# Logical request failure ratio seen by callers
(
  sum(rate({__name__=~"tent_(read|write)_failures_total"}[5m]))
)
/
clamp_min(
  sum(rate({__name__=~"tent_(read|write)_requests_total"}[5m])),
  1
)

# Physical attempt failure ratio by transport
sum by (transport) (rate(tent_transport_attempt_failures_total[5m]))
/
clamp_min(
  sum by (transport) (rate(tent_transport_attempts_total[5m])),
  1
)

# Terminal failures by phase and bounded reason
sum by (transport, phase, reason) (
  rate(tent_terminal_failures_total[5m])
)

# Failover direction
sum by (from, to) (rate(tent_transport_failover_total[5m]))
```

Interpret the combinations as follows:

| Logical failures | Attempt failures or failover | Likely scope |
|---|---|---|
| Low | Low | Look above TENT or at latency/admission rather than reliability |
| Low | High | A transport is degrading but fallback is masking caller impact |
| High | High | Transport, retry, or fallback capacity is insufficient |
| High | Low | Failure happened before/after physical transport: metadata, admission, selection, cancellation, or cleanup |

## Symptom-to-signal map

| Symptom | Primary signals | Narrowing signals |
|---|---|---|
| Metrics target missing | Prometheus `up`, `/live` | `tent_metrics_http_server_up`, `tent_metrics_http_start_failures_total`, port owner |
| Metrics are stale or empty | `tent_metrics_last_success_timestamp_seconds` | serialization attempts/failures by `format`, build/runtime enabled gauges |
| Request errors | logical failure ratio | terminal `phase`/`reason`, task failure reason, correlated failure log |
| Latency regression | read/write and attempt p99 | queue-wait, dispatch, transport stage histograms; admission wait; stream queue wait |
| Throughput drop | read/write byte rates | request rate, in-flight work, attempt errors, failover, admission rejection |
| Engine stall | worker progress age plus in-flight attempts | oldest in-flight age, RDMA posted/worker gauges, stream/staging state |
| RDMA degradation | retry exhausted, CQ poll errors | slice events, completion outcomes, endpoint events, QP transition failures, notification events |
| TCP or HP-TCP degradation | stream terminal events | queue depth, busy workers, admitted tasks/bytes, open connections, connection/IO/timeout outcome, retry exhaustion, attempt and queue latency |
| Admission pressure | rejected admission events | queue owners/bytes, admission wait, deadline drops, owner kind |
| Staging leak or shutdown delay | cleanup pending and cleanup timeouts | staging in-flight tasks/bytes, staging worker progress, quarantined batches |
| Shared-memory failure | SHM mapping/copy events | operation/outcome and copy latency |
| Metadata or segment churn | RPC outcomes and segment events | cache miss/reset, fetch/registry errors, generation advance |

## Worker-stall checks

A worker timestamp advances only when that worker makes progress. Always join
it with outstanding work:

```promql
# Engine progress worker: actionable only when attempts are in flight
(
  time()
  - tent_worker_last_progress_timestamp_seconds{worker="engine_progress"}
  > 30
)
and on (job, instance)
sum by (job, instance) (tent_inflight_attempts) > 0

# Staging worker: actionable only with active or deferred staging work
(
  time()
  - tent_worker_last_progress_timestamp_seconds{worker="staging"}
  > 30
)
and on (job, instance)
(
  sum by (job, instance) (tent_staging_inflight_tasks) > 0
  or
  sum by (job, instance) (tent_staging_cleanup_pending) > 0
)
```

If the engine worker is stale, compare `tent_inflight_attempts` with
`tent_rdma_worker_inflight_slices` and `tent_rdma_posted_inflight_slices`.
Growth only in posted RDMA work points toward completion processing. Growth
before posting points toward endpoint selection, queueing, or post failures.

## Transport-specific checks

### RDMA

1. Split `tent_rdma_retry_exhausted_total` by `operation` and `cause`.
2. A rising `tent_rdma_cq_poll_errors_total` is a completion-queue problem, not
   a request classification issue.
3. Use `tent_rdma_completion_latency_us` outcomes to separate successful,
   work-completion error, flush, and late completions.
4. Inspect endpoint lifecycle, QP transition, and notification counters before
   blaming the data path. They identify setup or control-channel failures
   without exposing peers or QP numbers.
5. Confirm whether logical success stayed healthy because RDMA failed over to
   TCP. Do not close the incident based only on logical success.

`tent_rdma_retries_total` is a TENT software re-enqueue, not a packet retry.
Use `tent_rdma_wc_errors_total{category="retry_exceeded"}` for a QP whose
hardware retry budget was exhausted and `category="rnr_retry_exceeded"` for
receiver-not-ready exhaustion. Individual on-wire RDMA retry counts are below
the verbs WC boundary and must come from NIC/vendor counters.

### TCP and HP-TCP

Split `tent_stream_events_total` by `transport`, `operation`, and `event`.
Connection errors suggest endpoint establishment, while `io_error`, `timeout`,
`peer_closed`, and `remote_error` identify distinct terminal paths. Compare
`tent_stream_queue_wait_us` with `tent_stream_attempt_latency_us`: high queue
wait points toward local saturation, while high attempt latency points toward
the connection or remote side.

For a persistent queue, compare `tent_stream_queue_depth` with
`tent_stream_busy_workers`. HP-TCP also exposes
`tent_stream_admitted_tasks`, `tent_stream_admitted_bytes`, and client/server
`tent_stream_connections`; use them to distinguish admission pressure, lane
saturation, and connection churn. These are current-state gauges, so they
should return to zero after the corresponding work or connection closes.

For HP-TCP loss symptoms, inspect the rate of
`tent_tcp_retransmissions_total` together with `tent_tcp_rtt_us` and
`tent_tcp_congestion_window_segments`. Samples are taken every 256 completed
operations and at connection close, so short-lived connections are still
accounted while long-lived connections avoid a syscall on every request.
`tent_protocol_errors_total` separates malformed headers from response
correlation failures such as `request_id_mismatch` and
`committed_length_mismatch`. A rising protocol counter is not ordinary packet
loss: TCP delivers an ordered byte stream, so investigate mixed versions,
framing bugs, or peer corruption.

### SHM and segment metadata

Use `tent_shm_mapping_events_total` to distinguish relocation or backing-object
mapping failures from copy failures in `tent_shm_copy_events_total`. For stale
or missing segment descriptions, follow `tent_segment_events_total` from cache
miss/reset through fetch or registry outcomes and correlate with the matching
RPC method.

## Admission and deadline pressure

`tent_admission_events_total` is labeled by `owner_kind` and a bounded event.
Continuous `rejected_*` events mean configured capacity is protecting the
engine; they do not by themselves indicate a transport fault. Compare:

- `tent_admission_queue_owners` and `tent_admission_queue_bytes` against the
  configured limits;
- `tent_admission_wait_us` against the request latency increase;
- `deadline_dropped` with `tent_deadline_infeasible_total` and the deadline MLU
  histogram;
- user and staging owner kinds to identify which workload consumes capacity.

## Correlate metrics, logs, and traces

Start with the bounded metric labels and incident window. Find one of these
stable structured events:

- `event=tent.request.failed`
- `event=tent.request.slow`
- `event=tent.transport.attempt_failed`
- `event=tent.transport.failover`
- `event=tent.transport.selection_failed`

Use its `trace_id` to retrieve `event=tent.trace.span` records. A logical span
has `span_name=tent.logical_transfer`; its physical children have
`span_name=tent.transport_attempt` and a matching `parent_span_id`. The
`retention` field explains why the trace survived sampling: `head_sampled`,
`slow`, `error`, `timeout`, or `linked_child`.

When scraping `/metrics/openmetrics`, the latest retained trace for each
transport/operation attempt series is attached to the `+Inf` latency bucket as
an exemplar. Configure the metrics backend and Grafana data source to ingest
exemplars, then use the exemplar trace ID as the same log lookup key. The
legacy `/metrics` endpoint intentionally contains no exemplars.

The current trace contract is process-local and does not propagate context on
the TENT wire protocol. For a cross-process incident, align build identity,
time window, RPC method/outcome, and bounded transport labels on each side.

## Telemetry-plane failures

`tent_metrics_serialization_attempts_total` and
`tent_metrics_serialization_failures_total` show whether a scrape reached the
serializer. `tent_metrics_last_success_timestamp_seconds` distinguishes an
idle workload from a broken exporter. If the HTTP port cannot bind, TENT keeps
recording and periodically logging summaries but exposes
`tent_metrics_http_server_up 0` only through direct serialization/log evidence;
Prometheus cannot scrape an endpoint that never started.

After recovery, confirm two consecutive successful scrapes and verify that the
last-success timestamp advances. A single successful `/live` request is not
enough.

## Evidence and data-safety boundary

The diagnostics response and structured observability events intentionally
exclude payloads, keys, credentials, memory addresses, target offsets, peer
addresses, segment names, and raw runtime pointers. Metric labels use closed
sets. A normal incident bundle should contain only:

- redacted `/diagnostics` JSON;
- relevant metric ranges or screenshots;
- bounded structured events selected by trace/request ID;
- build and configuration generation;
- the observed symptom and exact time range.

If additional application data is needed, collect it under that application's
own security and retention policy rather than adding it to TENT metric labels
or generic failure logs.

## Recovery verification

Do not resolve the incident until all applicable checks hold for a stable
window:

- `/live` and `/ready` return `200` and readiness reasons are empty;
- logical failure rate and physical attempt failure rate return to baseline;
- worker timestamps advance while work remains in flight;
- admission rejections, cleanup pending, retry exhaustion, and CQ errors stop
  increasing;
- metrics serialization succeeds and the last-success timestamp advances;
- a canary transfer completes on each transport expected to be in service.
