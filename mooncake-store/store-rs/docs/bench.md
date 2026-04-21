# mooncake-store-bench

`mooncake-store-bench` is a standalone benchmark and verification tool for the Mooncake store. It ships inside the wheel as an entry-point binary alongside `mooncake-store-client` and `mooncake-store-admin`.

## Commands

```
mooncake-store-bench [global options] <COMMAND>
```

### Global options

| Flag | Default | Description |
|------|---------|-------------|
| `--metadata-url <URL>` | required | Redis metadata URL |
| `--keyspace <PREFIX>` | auto-generated | Metadata keyspace prefix |
| `--transport-backend <classic-te\|tent>` | `classic-te` | Data-plane transport backend |
| `--protocol <tcp\|rdma>` | `tcp` | Transport protocol |
| `--local-hostname <HOST>` | `127.0.0.1` | Transport bind hostname |
| `--storage-bytes <BYTES>` | 0 | Per-client storage allocation for bench RW clients |
| `--scratch-bytes <BYTES>` | 16 MiB | Per-client scratch allocation |
| `--tenant <NAME>` | `bench` | Default tenant scope |
| `--trace-filter <FILTER>` | off | `tracing-subscriber` filter |
| `--metrics-addr <ADDR>` | off | Prometheus metrics bind address |
| `--seed <N>` | `42` | Global RNG seed for deterministic data |

The default bench topology is scratch-only for RW clients
(`MC_BENCH_STORAGE_BYTES=0`). Start separate `storage=true` daemons with
matching metadata, transport, and `--keyspace` settings before running the
benchmark. This is the same storage/RW role split used by
`scripts/tests/client/test-client-rw-cli.sh`. When `--keyspace` is omitted in
scratch-only mode, bench joins the same default metadata namespace as
`mooncake-store-client`: `mc/store-rs/v1`.

### `bench` — throughput and latency

```
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 bench \
  --mode mixed \
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
| `--concurrency <N>` | `4` | Concurrent worker threads |
| `--value-size <BYTES>` | `4096` | Payload size per object |
| `--batch-size <N>` | `8` | Requests per `batch_put` / `batch_get` call |
| `--iterations <N>` | `1024` | Total operations per worker |
| `--duration <SECONDS>` | off | Run for fixed wall time instead of iteration count |
| `--warmup <N>` | `32` | Warmup operations excluded from stats |
| `--read-ratio <PERCENT>` | `70` | Read fraction in mixed mode |
| `--report-interval <SECONDS>` | `5` | Periodic progress interval |
| `--key-space-size <N>` | `10000` | Distinct key count |
| `--replica-count <N>` | `1` | Replication factor for writes |
| `--writers <N>` | `1` | Writer `StoreClient` instances |
| `--readers <N>` | `1` | Reader `StoreClient` instances |
| `--output-format <text\|json\|csv>` | `text` | Report format |

Output at the end of a run:

```
=== Benchmark Results ===
Mode: mixed | Duration: 30.12s | Workers: 8 | Value size: 65536 B | Batch size: 16

  Metric           PUT              GET
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

### `verify` — correctness checks

```
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 verify \
  --verify-overwrite \
  --verify-delete \
  --verify-multi-tenant
```

| Flag | Default | Description |
|------|---------|-------------|
| `--value-size <BYTES>` | `4096` | Payload size |
| `--key-count <N>` | `256` | Keys exercised in multi-key tests |
| `--batch-size <N>` | `8` | Batch size for batch-put-get test |
| `--verify-overwrite` | off | Enable overwrite correctness test (64 rounds) |
| `--verify-delete` | off | Enable delete + reclaim test |
| `--verify-multi-tenant` | off | Enable tenant isolation test |

Each check prints `PASS` or `FAIL` with a diagnostic. The process exits non-zero if any check fails.

Default checks run without flags:

1. `single-round-trip` — `put` then `get`, byte-compare
2. `get-into-buffer` — `put` then `get_into`, size and byte-compare
3. `batch-put-get` — `batch_put` then individual `get` for each key
4. `is-exist` — `put` then `is_exist`
5. `multi-key-write` / `multi-key-read` — N-key write, heartbeat, N-key read

### `soak` — long-duration stability

```
mooncake-store-bench --metadata-url redis://127.0.0.1:6379/0 soak \
  --duration 3600 \
  --fault redis-jitter:5:50 \
  --fault metadata-drop:2 \
  --verify-reads
```

| Flag | Default | Description |
|------|---------|-------------|
| `--duration <SECONDS>` | `3600` | Total run time |
| `--concurrency <N>` | `2` | Worker count |
| `--value-size <BYTES>` | `4096` | Payload size |
| `--batch-size <N>` | `8` | Batch size |
| `--report-interval <SECONDS>` | `30` | Progress report cadence |
| `--fault <SPEC>` | none | Fault injection spec (repeatable) |
| `--verify-reads` | `true` | Verify every read against expected payload |
| `--heartbeat-interval-ms <MS>` | `30000` | Heartbeat interval |
| `--read-ratio <PERCENT>` | `70` | Read fraction |
| `--key-space-size <N>` | `10000` | Distinct key count |

Fault spec format:

| Spec | Effect |
|------|--------|
| `redis-jitter:<min_ms>:<max_ms>` | Sleep a random interval before each metadata-touching operation |
| `metadata-drop:<percent>` | Return an injected error for N% of operations |
| `transport-delay:<min_ms>:<max_ms>` | Sleep a random interval after each data-plane operation |
| `transport-error:<percent>` | Treat N% of operations as failed |

Multiple `--fault` flags are applied in order before each operation.

Progress is reported on a single line per interval:

```
[60s] put: 142 qps p50=1.1ms p99=4.8ms | get: 331 qps p50=0.7ms p99=3.2ms | errors: 0
```

## Architecture

### Module layout

```
crates/mooncake-store-py/src/bin/mooncake_store_bench/
  main.rs      entry point: parse args, init tracing, install ctrlc, dispatch
  cli.rs       clap structs: GlobalArgs, BenchArgs, VerifyArgs, SoakArgs, FaultSpec
  datagen.rs   deterministic LCG payload generation; key naming helpers
  latency.rs   LatencyRecorder: raw-sample p50/p90/p99/p999 percentile tracker
  reporter.rs  text/json/csv report formatting; one-line progress printer
  setup.rs     BenchCluster: metadata backend, selected transport, StoreClient construction
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

`BenchCluster` wraps one or more `StoreClient` instances for writer and reader roles. Each client gets its own selected transport factory (`classic-te` by default, optional `tent`) with a unique segment name, registered against the same `RedisMetadataBackend`. The keyspace is auto-generated per run using `now_ms ^ process_id`, so parallel runs do not interfere.

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
