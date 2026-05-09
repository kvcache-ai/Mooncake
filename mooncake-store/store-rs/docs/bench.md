# mooncake-store-bench

`mooncake-store-bench` is a standalone benchmark and verification tool for the Mooncake store. It ships inside the wheel as an entry-point binary alongside `mooncake-store-client` and `mooncake-store-admin`.

## Commands

```
mooncake-store-bench [global options] <COMMAND>
```

### Global options

| Flag | Default | Description |
|------|---------|-------------|
| `--metadata-url <URL>` | required | Redis URL used by the bench metadata backend; also read from `MC_STORE_RS_METADATA_URL` |
| `--transport-metadata-url <URL>` | unset | Optional transfer-engine metadata endpoint; also read from `MC_STORE_RS_TRANSPORT_METADATA_URL`. Use `P2PHANDSHAKE` with `classic_te` when the transport should use peer handshake metadata instead of Redis-backed transport metadata. |
| `--keyspace <PREFIX>` | `mc/store-rs/v2` when `storage_bytes=0`; otherwise auto-generated | Metadata keyspace prefix |
| `--transport-backend <classic-te\|tent>` | `classic-te` | Data-plane transport backend |
| `--protocol <tcp\|rdma>` | `tcp` | Transport protocol |
| `--local-hostname <HOST>` | `127.0.0.1` | Transport bind hostname |
| `--storage-bytes <BYTES>` | 0 | Per-client storage allocation for bench RW clients |
| `--scratch-bytes <BYTES>` | 16 MiB | Per-client scratch allocation |
| `--tenant <NAME>` | `bench` | Default tenant scope |
| `--trace-filter <FILTER>` | `MC_STORE_RS_TRACE_FILTER`, then `RUST_LOG`, otherwise `info` | `tracing-subscriber` filter |
| `--metrics-addr <ADDR>` | off | Prometheus metrics bind address |
| `--seed <N>` | `42` | Global RNG seed for deterministic data |
| `--route-control <embedded-wrh\|metadata-only>` | `embedded-wrh` | Route control mode used by bench runtimes |
| `--route-topk <N>` | `2` | WRH route-authority fanout |
| `--replica-count <N>` | `1` | Replication factor for writes |

The default bench topology is scratch-only for RW clients
(`MC_BENCH_STORAGE_BYTES=0`). Start separate `storage=true` daemons with
matching metadata, transport, and `--keyspace` settings before running the
benchmark. This is the same storage/RW role split used by
`scripts/tests/client/test-client-rw-cli.sh`. When `--keyspace` is omitted in
scratch-only mode, bench joins the same default metadata namespace as
`mooncake-store-client`: `mc/store-rs/v2`. When `--storage-bytes > 0` and
`--keyspace` is omitted, bench generates an isolated
`mc/store-rs/bench/<unique>` keyspace for that run.

Startup safety gate for `classic_te` compatibility deployments:

- bench refuses to start if active `storage=true` daemons in the same keyspace
  mix metadata-backed logical segment names with `P2PHANDSHAKE` rpc-address
  segment names
- bench also refuses to start if its effective transport metadata mode disagrees
  with the active storage daemons already visible in that keyspace
- fix the cluster by restarting every participating storage daemon and bench
  process with one transport metadata mode before benchmarking

### Tracing behavior

`mooncake-store-bench` initializes its own tracing subscriber.

- default sink: `stderr`
- effective filter: `--trace-filter`, then `MC_STORE_RS_TRACE_FILTER`, then `RUST_LOG`, then `info`
- dedicated trace file: `MC_BENCH_TRACE_FILE=/path/to/bench.log`
- `MC_STORE_RS_TRACE_FILE` remains the real-client / standalone-client trace-file surface and does not redirect bench output
- ANSI colors are enabled only when `stderr` is a TTY and no bench trace file is selected
- span close events are disabled, so per-request `close time.busy=...` subscriber noise does not flood benchmark output

### `bench` — throughput and latency

```
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 bench \
  --mode mixed \
  --write-interface batch-put-from \
  --read-interface batch-get-into \
  --concurrency 8 \
  --value-size 65536 \
  --batch-size 16 \
  --duration 60 \
  --read-ratio 70 \
  --output-format text
```

| Flag | Default | Description |
|------|---------|-------------|
| `--mode <put\|get\|mixed>` | `mixed` | Operation mix |
| `--write-interface <put\|batch-put\|batch-put-from>` | `batch-put-from` | Write-side store API used for measured write traffic; also read from `MC_BENCH_WRITE_INTERFACE` |
| `--read-interface <get\|batch-get\|batch-get-into>` | `batch-get-into` | Read-side store API used for measured read traffic; also read from `MC_BENCH_READ_INTERFACE` |
| `--concurrency <N>` | `4` | Concurrent worker threads |
| `--value-size <BYTES>` | `4096` | Payload size per object |
| `--batch-size <N>` | `8` | Objects per `batch_put` / `batch_put_from` / `batch_get` / `batch_get_into` call; ignored by single-key `put` / `get` |
| `--iterations <N>` | `1024` | Total operations per worker |
| `--duration <SECONDS>` | off | Run for fixed wall time instead of iteration count |
| `--warmup <N>` | `32` | Warmup operations excluded from stats |
| `--read-ratio <PERCENT>` | `70` | Read fraction in mixed mode |
| `--report-interval <SECONDS>` | `5` | Periodic progress interval |
| `--key-space-size <N>` | `10000` | Distinct key count |
| `--writers <N>` | `1` | Writer `StoreClient` instances |
| `--readers <N>` | `1` | Reader `StoreClient` instances |
| `--output-format <text\|json\|csv>` | `text` | Report body format emitted through tracing |

`bench` separates traffic shape from concrete client APIs:

- `--mode` controls whether the workload is write-only, read-only, or mixed
- `--write-interface` selects `put`, `batch_put`, or `batch_put_from` for measured writes
- `--read-interface` selects `get`, `batch_get`, or `batch_get_into` for measured reads
- defaults benchmark `batch_put_from` + `batch_get_into`
- `MC_BENCH_INTERFACES` can set both env defaults at once, for example `MC_BENCH_INTERFACES=put,get`
- batched operations advance their key cursor by the batch width, so adjacent
  batch calls do not repeatedly rewrite or reread the same object window
- mixed mode splits each worker's key shard into a stable read half and a write
  half; this keeps the benchmark from turning ordinary read traffic into
  immediate read-after-overwrite route churn
- prefill, warmup, and measured phases heartbeat per-worker writer/reader
  clients so long scratch-only benchmark setup does not let route authorities
  expire before measured reads start
- writer/reader runtime stable IDs include a per-process run ID, so rerunning
  the same Kubernetes Job before old leases expire does not collide with the
  previous run's still-active bench clients
- measured reads retry bounded route-readiness misses, such as a freshly written object route
  not yet being readable; the wait is included in read latency and persistent misses still count
  as read errors

Output at the end of a run:

```
=== Benchmark Results ===
Mode: mixed | Write: batch_put_from | Read: batch_get_into | Duration: 30.12s | Workers: 8 | Value size: 65536 B | Batch size: 16

  Metric           batch_put_from   batch_get_into
  ------           ---              ---
  Total ops        3840             8960
  QPS              127.5            297.4
  Throughput       7.7 GiB/s        18.0 GiB/s
  Latency p50      1.23ms           0.84ms
  Latency p90      2.45ms           1.56ms
  Latency p99      5.12ms           3.78ms
  Latency p999     12.4ms           8.92ms
  Latency max      18.7ms           11.3ms
  Errors           0                0
```

`--output-format` changes the report body only. The final text, JSON, or CSV
payload is still emitted as `INFO` tracing events, so the normal tracing prefix
remains around the report in `stderr` or `MC_BENCH_TRACE_FILE`.

Counters always count API calls, not individual objects:

- `put` and `get` count one object per call
- `batch_put`, `batch_put_from`, `batch_get`, and `batch_get_into` count one batch call per operation
- throughput still includes the bytes of every object returned by that call

### `verify` — correctness checks

```
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 verify \
  --write-interface batch-put-from \
  --read-interface batch-get-into \
  --verify-overwrite \
  --verify-delete \
  --verify-multi-tenant
```

| Flag | Default | Description |
|------|---------|-------------|
| `--write-interface <put\|batch-put\|batch-put-from>` | `batch-put-from` | Write-side store API used for verify checks that issue writes; also read from `MC_BENCH_WRITE_INTERFACE` |
| `--read-interface <get\|batch-get\|batch-get-into>` | `batch-get-into` | Read-side store API used for verify checks that issue ordinary reads; also read from `MC_BENCH_READ_INTERFACE` |
| `--value-size <BYTES>` | `4096` | Payload size |
| `--key-count <N>` | `256` | Keys exercised in multi-key tests |
| `--batch-size <N>` | `8` | Objects per `batch_put` / `batch_put_from` / `batch_get` / `batch_get_into`; ignored by single-key `put` / `get` |
| `--verify-overwrite` | off | Enable overwrite correctness test (64 rounds) |
| `--verify-delete` | off | Enable delete + reclaim test |
| `--verify-multi-tenant` | off | Enable tenant isolation test |

Each check prints `PASS` or `FAIL` with a diagnostic. The process exits non-zero if any check fails.

`verify` defaults to `batch_put_from` + `batch_get_into`. `MC_BENCH_INTERFACES=put,get`
and the explicit `--write-interface` / `--read-interface` flags are both
supported here too.

Default checks run without flags:

1. `single-round-trip` — configured write interface, then configured read interface, byte-compare
2. `get-into-buffer` — configured write interface, then `get_into`, size and byte-compare
3. `batch-put-get` — multi-key write/read using the configured interfaces
4. `is-exist` — configured write interface, then `is_exist`
5. `multi-key-write` / `multi-key-read` — N-key write/read using the configured interfaces with a heartbeat between phases

### `soak` — long-duration stability

```
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 soak \
  --duration 3600 \
  --write-interface batch-put-from \
  --read-interface batch-get-into \
  --fault redis-jitter:5:50 \
  --fault metadata-drop:2 \
  --verify-reads
```

| Flag | Default | Description |
|------|---------|-------------|
| `--duration <SECONDS>` | `3600` | Total run time |
| `--concurrency <N>` | `2` | Worker count |
| `--write-interface <put\|batch-put\|batch-put-from>` | `batch-put-from` | Write-side store API used for soak writes; also read from `MC_BENCH_WRITE_INTERFACE` |
| `--read-interface <get\|batch-get\|batch-get-into>` | `batch-get-into` | Read-side store API used for soak reads; also read from `MC_BENCH_READ_INTERFACE` |
| `--value-size <BYTES>` | `4096` | Payload size |
| `--batch-size <N>` | `8` | Objects per `batch_put` / `batch_put_from` / `batch_get` / `batch_get_into`; ignored by single-key `put` / `get` |
| `--report-interval <SECONDS>` | `30` | Progress report cadence |
| `--fault <SPEC>` | none | Fault injection spec (repeatable) |
| `--verify-reads` | `true` | Verify every read against expected payload |
| `--heartbeat-interval-ms <MS>` | `30000` | Compatibility flag accepted by the CLI; the current soak loop still heartbeats every 64 iterations |
| `--read-ratio <PERCENT>` | `70` | Read fraction |
| `--key-space-size <N>` | `10000` | Distinct key count |

`soak` defaults to `batch_put_from` + `batch_get_into`. Override either side with the same
CLI flags or env vars used by `bench` when you want single-key traffic.
`MC_BENCH_INTERFACES=write=put,read=get` is also accepted.

Fault spec format:

| Spec | Effect |
|------|--------|
| `redis-jitter:<min_ms>:<max_ms>` | Sleep a deterministic random interval before the next store operation |
| `metadata-drop:<percent>` | Inject a synthetic pre-op failure for N% of operations |
| `transport-delay:<min_ms>:<max_ms>` | Sleep a deterministic random interval before the next store operation |
| `transport-error:<percent>` | Inject a synthetic pre-op failure for N% of operations |

Multiple `--fault` flags are applied in order before each operation.

Progress is reported on a single line per interval:

```
[60s] batch_put_from: 142 qps p50=1.1ms p99=4.8ms | batch_get_into: 331 qps p50=0.7ms p99=3.2ms | errors: 0
```

## Architecture

### Module layout

```
crates/mooncake-store-py/src/bin/mooncake_store_bench/
  main.rs      entry point: parse args, init bench-local tracing, install ctrlc, dispatch
  cli.rs       clap structs: GlobalArgs, BenchArgs, VerifyArgs, SoakArgs, FaultSpec
  datagen.rs   deterministic LCG payload generation; key naming helpers
  latency.rs   LatencyRecorder: raw-sample p50/p90/p99/p999 percentile tracker
  reporter.rs  text/json/csv report-body formatting via tracing; interval progress printer
  setup.rs     BenchCluster: metadata backend, compat runtime construction, startup snapshots
  bench.rs     concurrent worker threads (std::thread::scope)
  verify.rs    sequential correctness checks
  soak.rs      long-running loop with per-iteration fault injection and read verification
  fault.rs     FaultInjector: deterministic jitter and drop decisions from LCG
```

### Data generation

All payloads are deterministic. The seed string is `{global_seed}-{key}-{generation}` and the LCG is taken from `mooncake-store-e2e`. The same seed string always produces the same bytes regardless of when or where it is called, so read verification does not require storing the written values.

### Latency tracking

`LatencyRecorder` stores raw per-operation microsecond samples in a `Vec`. Percentiles are computed by lazy sort on first access. Workers accumulate into a thread-local recorder and the main thread merges all recorders at the end. This approach gives exact percentiles without pre-defined histogram buckets.

### Fault injection

`FaultInjector` uses the same LCG to produce deterministic random decisions per operation, seeded by a global operation counter. Each worker calls `pre_op()` before issuing a store request. Depending on the configured specs, `pre_op()` may sleep (jitter or transport-delay) or return an error string (metadata-drop or transport-error). Returned errors increment the error counter and skip the actual store call.

### Client construction

`BenchCluster` wraps one or more `StoreClient` instances for writer and reader
roles. It uses the same `CompatRuntimeArgs` / `CompatSetupArgs` path as
`mooncake-store-client`, so transport backend selection, RDMA env passthrough,
and compatibility defaults stay aligned with the normal runtime.

Each client gets its own selected transport factory (`classic-te` by default,
optional `tent`) with a unique segment name, registered against the same
`RedisMetadataBackend`.

Keyspace behavior is conditional:

- scratch-only mode (`storage_bytes=0`) reuses the configured keyspace, or `mc/store-rs/v2` when `--keyspace` is omitted
- storage-owning mode (`storage_bytes>0`) auto-generates `mc/store-rs/bench/<unique>` when `--keyspace` is omitted

## Wheel packaging

The binary is installed alongside `mooncake-store-client` and `mooncake-store-admin`:

- `build-wheel.sh` installs `mooncake-store-bench` to `dist/bin/` and injects it into the wheel zip under `mooncake/mooncake-store-bench`
- `pyproject.toml` registers `mooncake-store-bench = "mooncake.cli:main"` as an entry point
- `_runtime.py` recognises `mooncake-store-bench` in `invoked_binary_name()` and dispatches `execute_packaged_binary`

After `pip install mooncake`, the tool is available as `mooncake-store-bench` on `$PATH`.

## Next reading

- `docs/rust.md` for store client API reference
- `docs/configuration.md` for Redis and transport options
- `crates/mooncake-store-e2e/src/main.rs` for the upstream correctness test suite
