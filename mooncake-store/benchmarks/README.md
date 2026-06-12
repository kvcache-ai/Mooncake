# Mooncake Store Benchmarks

This directory contains benchmark tools for Mooncake Store internals and public
Store APIs.

## End-to-End Store API Benchmark

`store_api_benchmark.py` exercises the Python `MooncakeDistributedStore` API
against a running `mooncake_master`. It can also start a local master from a
CMake build tree, making it suitable for local smoke tests and reproducible
baseline collection.

### Local Smoke Test

Use an existing CMake build directory that contains `mooncake_master` and the
Store Python extension:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation smoke \
  --value-size 1K
```

The smoke test runs `setup -> put -> is_exist -> get -> remove(force=True)`.
It exits with a non-zero status if any step fails.

### API Benchmark

Run all supported operations with a small local workload:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation all \
  --num-keys 1000 \
  --value-size 4K \
  --batch-size 32 \
  --concurrency 1 \
  --repeat 1 \
  --output-json /tmp/mooncake-store-api-benchmark.json
```

Supported operations:

- `put`
- `get`
- `upsert`
- `is_exist`
- `batch_is_exist`
- `get_size`
- `remove`
- `remove_by_regex`
- `remove_all`
- `put_batch`
- `get_batch`
- `upsert_batch`
- `batch_remove`
- `all`
- `mixed`
- `multi_client_mixed`
- `smoke`

The JSON output includes:

- benchmark configuration
- host, Python version, git commit, branch, dirty state, OS, CPU, memory, and
  `nvidia-smi` GPU information when available
- API calls per second
- objects per second
- throughput in MiB/s
- latency percentiles: P50, P95, P99
- failure count and error code histogram
- miss count and miss code histogram for workloads where key races are expected
- optional concurrency statistics for `put`, `get`, and `remove`
- optional repeat summaries with mean, standard deviation, CoV, and unstable flags
- RSS memory usage before, after, delta, and sampled peak when `psutil` is available
- `metrics_snapshot` with Prometheus-format master metrics from `/metrics`, plus
  `/metrics/summary` and `/health` snapshots
- optional `comparison` deltas when `--compare baseline.json` is used

`--operation all` includes `remove_all`, which removes every object visible to
the Store master. Use it with `--start-master` or a disposable benchmark master,
not against a shared production metadata service.

### Metrics Collection

By default, `--start-master` launches `mooncake_master` with metric reporting
enabled on port `9003`, and the benchmark captures:

- `metrics_snapshot.master.metrics`: raw Prometheus text and parsed metric names
- `metrics_snapshot.master.summary`: human-readable master metric summary
- `metrics_snapshot.master.health`: master admin health response

Use these options when ports differ or when external endpoints are available:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation smoke \
  --master-metrics-port 19003 \
  --master-metrics-url http://127.0.0.1:19003/metrics \
  --output-json /tmp/mooncake-store-api-smoke-with-metrics.json
```

`--client-metrics-url` and `--worker-metrics-url` are optional probes for
externally enabled endpoints. In the default Python benchmark path, the
RealClient embedded HTTP metrics server is not enabled by the script, and no
equivalent worker HTTP metrics endpoint is started. Use `--skip-metrics` to
disable HTTP metrics collection.

### Concurrent API Benchmark

Use `--concurrency` to run `put`, `get`, or `remove` from multiple Python
threads:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation put \
  --num-keys 1000 \
  --value-size 4K \
  --concurrency 8 \
  --output-json /tmp/mooncake-store-api-put-concurrency.json
```

When `--concurrency` is greater than 1, `--num-keys` remains the total object
count for the operation and keys are split across worker threads. Batch APIs
keep their existing single-threaded batch semantics; `--batch-size` controls
objects per batch API call, while `--concurrency` controls simultaneous API
callers.

Each concurrent operation result includes:

- `concurrency`: requested thread count
- `concurrency_stats.total_ops_per_s`: sum of per-thread API call throughput
- `concurrency_stats.thread_ops_per_s_mean`
- `concurrency_stats.thread_ops_per_s_stdev`
- `concurrency_stats.latency_ms_merged`: P50/P95/P99 from all thread latency samples
- `concurrency_stats.threads`: per-thread throughput, latency, and failure data

### Mixed Workload Stress Test

Use `--operation mixed` for duration-based pressure testing. The benchmark
preloads `--num-keys` objects, then runs concurrent `put`, `get`, and
`remove(force=True)` calls until `--duration` expires. Value sizes are sampled
from a uniform distribution to simulate uneven KV cache payloads.

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation mixed \
  --duration 30 \
  --concurrency 4 \
  --mix get-heavy \
  --num-keys 1000 \
  --value-size 4K \
  --value-size-min 1K \
  --value-size-max 64K \
  --output-json /tmp/mooncake-store-api-mixed.json
```

Supported mix profiles:

- `put-heavy`: 70% put, 25% get, 5% remove
- `get-heavy`: 20% put, 75% get, 5% remove
- `mixed`: 45% put, 50% get, 5% remove

The result includes aggregate throughput and latency plus per-operation metrics
under `results.mixed.operations`. In this mode, expected key misses caused by
concurrent readers/removers are counted as `misses` instead of `failures`; store
errors unrelated to a missing key are still reported as failures.

For long steady-state runs, `results.mixed.trend` contains fixed-size time
buckets with throughput, latency, miss, failure, and per-operation breakdowns.
Use `--trend-interval` to control the bucket size. `trend.analysis` compares the
first and last complete buckets and marks a possible decline when API throughput
drops by more than 10% or P99 latency grows by more than 20%.

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation mixed \
  --duration 300 \
  --trend-interval 10 \
  --concurrency 4 \
  --num-keys 1000 \
  --max-active-keys 5000 \
  --value-size 4K \
  --output-json /tmp/mooncake-store-long-steady.json
```

`--max-active-keys` is useful for long steady-state tests because it caps the
live key set. When the cap is reached, selected put operations are redirected to
get operations instead of growing metadata without bound.

### Multi-Client Mixed Workload

Use `--operation multi_client_mixed` to start several independent Python
processes, each creating its own `MooncakeDistributedStore` client. The parent
process starts one local master, launches the child clients with isolated key
prefixes, and aggregates throughput, error rate, and per-client results.

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation multi_client_mixed \
  --multi-client-count 4 \
  --duration 60 \
  --concurrency 2 \
  --num-keys 1000 \
  --max-active-keys 5000 \
  --value-size 4K \
  --output-json /tmp/mooncake-store-multi-client.json
```

By default, the parent first runs a single-client mixed baseline with the same
workload, then runs the multi-client phase. The JSON output includes
`results.multi_client_mixed.single_client_baseline`,
`results.multi_client_mixed.aggregate`, and
`results.multi_client_mixed.baseline_comparison`. Use
`--skip-single-client-baseline` when you only need the multi-client run.

### Concurrency Sweep

Use `--concurrency-scan` to run the same workload at several thread counts. With
no explicit value it scans `1,4,8,16`; pass a comma-separated list to customize
the gradient.

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation get \
  --num-keys 1000 \
  --value-size 4K \
  --concurrency-scan 1,4,8,16 \
  --output-json /tmp/mooncake-store-api-get-scan.json
```

The compact trend is available in `results.concurrency_scan.trend`; full run
details are preserved under `results.concurrency_scan.runs`.

### Repeated Runs

Use `--repeat` to run the same operation multiple times with the same
configuration:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation get \
  --num-keys 1000 \
  --value-size 4K \
  --repeat 5 \
  --output-json /tmp/mooncake-store-api-get-repeat.json
```

When `--repeat` is greater than 1, each operation result adds a `repeat` field
containing per-run throughput and P50/P95/P99 latency. The summary reports mean,
standard deviation, and coefficient of variation for throughput and latency
metrics. Metrics with CoV greater than 10% are marked with
`"unstable": true`.

### Baseline Comparison

Use `--compare` to calculate percentage deltas against a previous JSON report.
The comparison walks matching result nodes and reports deltas for throughput,
P50/P95/P99 latency, and failures.

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation all \
  --repeat 3 \
  --compare /tmp/mooncake-store-api-baseline.json \
  --output-json /tmp/mooncake-store-api-current.json
```

### Memory Tracking

If the optional `psutil` package is installed, every operation records process
RSS before and after the test, RSS delta in bytes and percent, and sampled peak
RSS. Sampling runs once per second during the operation. If `psutil` is not
installed, the script prints a warning and sets `memory.available` to `false`;
benchmark execution still continues.

### Recovery Test

`store_recovery_test.py` validates the basic recovery path for a standalone
master restart:

1. start a local `mooncake_master`
2. create a Store client and verify put/get/remove
3. stop the master and wait for `health_check()` to report
   `HC_MASTER_UNREACHABLE`
4. restart the master on the same ports
5. wait for the same client to return to `HC_HEALTHY`
6. verify put/get/remove works after reconnect

```bash
python3 mooncake-store/benchmarks/store_recovery_test.py \
  --build-dir build \
  --value-size 1K \
  --output-json /tmp/mooncake-store-recovery.json
```

This is a non-HA recovery test. It validates client disconnect detection,
reconnect, and post-restart operations; it does not require pre-restart object
metadata to survive a standalone master restart.

### External Master

If `mooncake_master` is already running, omit `--start-master` and provide the
metadata and master addresses:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --operation get_batch \
  --metadata-url http://127.0.0.1:8080/metadata \
  --master-addr 127.0.0.1:50051 \
  --num-keys 10000 \
  --value-size 16K \
  --batch-size 64
```

### RDMA Runs

For RDMA environments, pass the protocol and device list:

```bash
python3 mooncake-store/benchmarks/store_api_benchmark.py \
  --build-dir build \
  --start-master \
  --operation all \
  --protocol rdma \
  --rdma-devices mlx5_0 \
  --num-keys 10000 \
  --value-size 64K \
  --batch-size 64
```

Use the same workload parameters for baseline and optimized runs so the
reported deltas are comparable.

### Notes

- `remove()` is called with `force=True` in the benchmark path to avoid lease
  protection interfering with cleanup.
- `--skip-cleanup` leaves benchmark keys in the Store for debugging.
- The script does not require third-party Python packages for benchmark
  execution. Install optional `psutil` to enable RSS memory tracking.
