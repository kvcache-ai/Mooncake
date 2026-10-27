# `store_kv_bench.py`

`store_kv_bench.py` is a Mooncake Store end-to-end KV benchmark tool. It talks
to a real Mooncake cluster through the Python `store` binding and can exercise
`put/get` as well as zero-copy `put_from/get_into` style APIs.

## Scope

This tool focuses on object-semantic benchmark scenarios:

- Functional verification with read-after-write validation
- Dataset fill for eviction / capacity tests
- Pure write performance
- Pure read performance
- Read/write mixed mode with "existing-object read + new-object write"
- Stateful `put/get/is_exist/remove` checks, operation journals, and replay

Fault injection, NoF register / unregister, heartbeat trigger, memory segment
unmount, and target-side operations are intentionally out of scope. The tool
supports phase gaps so external tools can finish those operations before the
next phase continues.

## Supported Scenarios

| `--scenario` | Phases and stopping condition |
|---|---|
| `verify_write` | Strict fixed-count `write_verify`, optional phase gap, then one sequential `verify_read` pass. Readback always verifies contents. |
| `fill` | Fixed-count `fill_write`; records write failures without aborting the phase. |
| `write_perf` | Writes up to the write-object budget; with positive `runtime`, also stops at the deadline. |
| `read_perf` | By default, strict `prepare_write`, optional gap, then sequential reads. `runtime=0` makes one pass; positive `runtime` loops over the prepared set. |
| `mixed_rw` | By default, strict `prepare_write`, optional gap, then random reads of prepared keys mixed with fresh-key writes. Requires positive `runtime`; stops early if the new-write budget is exhausted. |
| `metadata_smoke` | Per active lane: `put`, `exist`, `get`, `remove`, `exist` on one key, with state and payload checks. |
| `mixed_metadata` | Seeded operation mix using `put/get/exist/remove`, with one operation per object ID and a lane-local presence model. |
| `remove_perf` | Always performs strict `prepare_write`, then removes each prepared key and checks that it is absent. |
| `replay` | Replays operations from a metadata journal, preserving each lane's file order. |

Write budget means `write-objects` when positive, otherwise `nr-objects`.
Prepare budget means `prepare-objects` when positive, otherwise `nr-objects`.
The budgets are shared across lanes, not multiplied by the worker count.
`runtime` does not bound `verify_write`, `fill`, or the four metadata scenarios.

Strict prepare/write phases abort if any requested object cannot be written.
The benchmark does not automatically remove keys after performance phases;
`metadata_smoke`, `remove_perf`, and explicit replayed removes delete keys as
part of their workload. Use fresh prefixes to avoid existing-key collisions.

## APIs

- `--io-api=plain`
  - Single object:
    - `put`
    - `get`
  - Batch:
    - `put_batch`
    - `get_batch`
- `--io-api=zcopy`
  - Single object:
    - `put_from`
    - `get_into`
  - Batch:
    - `batch_put_from`
    - `batch_get_into`

Batch APIs are selected when the configured `batch-size` exceeds one, including
a final partial batch containing only one object. One batch call is counted as
one request, with a separate count for the objects in it.

`zcopy` mode acquires one native `mooncake.store.BufferPool` lease for all lanes
and gives each lane separate fixed-size buffer slots. The required lease size is:

```text
value-size × batch-size × numjobs × iodepth <= local-buffer-size
```

The Python binding must expose `BufferPool` for this mode. The benchmark copies
write payloads into these slots, clears read slots before each request, and
copies read bytes back into Python even without `--verify`. These steps are
included in request latency; `zcopy` selects the pointer-based Store API and
does not imply that the benchmark or filesystem adapter performs no copies.

The four metadata scenarios always use single-key plain Store calls, regardless
of `--io-api` or `--batch-size`. Use `--io-api plain` for them to avoid allocating
an unused zcopy pool.

## Concurrency and client ownership

One benchmark process creates one `MooncakeDistributedStore` with normal
`setup()`, so it owns one embedded real client. Every lane shares this Store
instance, connection setup, global segment, and local buffer pool.

```text
lanes = numjobs × iodepth
maximum objects in flight ≈ lanes × batch-size
```

Each lane is a Python thread that issues synchronous Store requests one at a
time. `iodepth` creates more threads; it is not a per-thread asynchronous queue.
The Store binding releases the GIL around I/O. Threads are started and joined
for each phase, while the Store and its buffers are reused across phases.
`global-segment-size` and `local-buffer-size` apply once per process, not once
per lane. The script has no dummy-client setup mode.

## How the read benchmark works

For `--scenario read_perf`, the normal flow is:

```text
Create one Store client
  → prepare_write: write the prepare budget and remember successful IDs
  → optional phase gap
  → read_perf: choose the next batch of prepared IDs
      → regenerate keys → get/get_batch or get_into/batch_get_into
      → check results and optionally verify payloads
      → record request latency, object counts, misses, and bytes
  → print/write summaries → close the Store
```

The prepare phase must succeed in full before reads start. Prepared IDs are
remembered in write-completion order, so multiple writing lanes can make that
order differ from numeric object-ID order. Read lanes share one cursor through
this list; they do not each read the entire dataset independently. Reads are
sequential over that list, not random, and consecutive batches can be in flight
at the same time on different lanes.

- `--runtime 0`: distribute one pass across all lanes. The last batch may be
  smaller than `batch-size`.
- `--runtime N` with `N > 0`: wrap the cursor at the end and repeat until the
  common deadline. Requests already started can finish after the deadline.
- `--prepare-mode auto` and `write` both write a dataset for `read_perf` and
  `mixed_rw`. There is no separate untimed read warmup or cache flush.
- `--prepare-mode none` currently skips writing **and leaves the prepared-ID
  list empty**. It does not discover previously written keys, reconstruct the
  list from `nr-objects`, or load it from a journal. `read_perf` consequently
  issues zero reads; a timed run marks `dataset_exhausted`. Do not use it to
  measure a dataset created by an earlier invocation.

Request latency starts after IDs are selected and includes key generation,
the Store call, result processing, and enabled verification. It excludes
prepare-write time and phase-gap time. Read-phase throughput uses the merged
read-thread time span. The overall summary includes the prepare phase and the
elapsed gap between phases, so use the `read_perf` phase for read-only results.

Plain reads treat `None` and empty bytes as misses. For a negative zcopy read
result, the benchmark issues `is_exist()` to distinguish an absent key from an
I/O error. That extra probe is also inside the measured request. Bytes processed
are successful objects multiplied by the configured `value-size`; without
verification, plain reads do not check the returned length.

### Which replica does a read use?

The replica-count flags configure writes and prepare writes. Reads use the
replica descriptors returned by the master and normal Store replica selection;
they do not pass that write configuration back as a read-tier selector.
For a memory-plus-DFS object, an available memory replica is preferred, even
when remote. DFS is selected when no readable memory replica remains.

To measure DFS reads, use `--memory-replica-num 0 --nof-replica-num 0
--dfs-replica-num 1 --enable-ssd-offload` with automatic preparation and a fresh
key prefix. This writes only DFS replicas, so reads select DFS without a phase
gap or memory eviction.

The script has no force-DFS-read flag and does not report which tier served a
request. Store hot-cache redirection applies only to memory replicas. DFS reads
can still benefit from filesystem or storage caches; DFS I/O does not
necessarily mean a physical disk read.

## Key Rules

- Keys are generated deterministically:
  - `{prefix padded/truncated to fit}{16-digit object id}`
- The same `key-prefix`, `key-size`, and `object-id-start` produce the same key sequence
- `verify` currently requires `pattern`
- Any write-involved scenario requires `value-size` to be 512-byte aligned
- `memory-replica-num`, `nof-replica-num`, and `dfs-replica-num` cannot all be `0`.
- `dfs-replica-num` requests a DFS replica; supported Store values are `0` or `1`.
  Set memory and NoF counts to `0` and the DFS count to `1` for DFS-only placement.
- Any process that writes or reads DFS replicas must pass `--enable-ssd-offload`,
  which maps to `setup(enable_ssd_offload=True)`; without it DFS puts fail with
  `-1601` (`DFS_SERVICE_UNAVAILABLE`)
- `prepare-objects`
  - Controls how many objects are written by the prepare phase
  - `0` means reuse `nr-objects`
- `tenant-id` currently accepts only `default`.
- Object IDs occupy at least 16 decimal digits; `key-size` must leave room for
  that suffix. Prefixes are truncated or padded with `_` to reach `key-size`,
  so distinct long prefixes can generate the same keys when truncated.
- `pattern` is repeated and truncated to fill each value; use UTF-8 text or
  even-length hex such as `0xabcd`. Without a pattern, payload bytes are
  `object_id & 0xff`. `verify_write` always verifies its readback; other read
  performance phases verify only with `--verify`.
- Keep `value-size` 512-byte aligned for prepare writes too. The CLI checks
  alignment for the explicitly write-involved scenarios, but does not reject
  an unaligned `read_perf` value before its automatic prepare phase.

## Phase Gap

Phase gaps are used when an external tool needs time to inject a fault or do an
unmount / remount operation.

- `--phase-gap-mode=none`
  - Continue immediately
- `--phase-gap-mode=sleep --phase-gap-sec=N`
  - Sleep before the next phase
- `--phase-gap-mode=manual`
  - Wait for Enter
- `--phase-gap-mode=file --phase-gap-file=/tmp/bench.ready`
  - Wait until the file exists

If `file` mode is used, make sure the marker file does not already exist before
starting the benchmark. The default timeout is 600 seconds; the file is polled
once per second and is not deleted by the benchmark.

Gaps apply between write and verification in `verify_write`, and after prepare
in `read_perf` / `mixed_rw`. `remove_perf` does not insert a gap, and skipping
prepare also skips its gap.

## Metadata scenarios and replay

These scenarios check object semantics through plain single-key Store APIs.
They are not payload-free master RPC microbenchmarks: `put` and `get` transfer
values, and `remove` performs an additional `is_exist()` check. Reads in these
scenarios always validate present payloads, independently of `--verify`.

- `metadata_smoke` uses one key per lane, up to `nr-objects` active lanes,
  and runs `put → exist → get → remove → exist`. With one lane, increasing
  `nr-objects` does not increase the five-operation sequence.
- `mixed_metadata` divides the `nr-objects` IDs between lanes and uses
  `rand-seed + lane_id` to choose an operation for each ID. The four percentage
  flags must be nonnegative integers summing to 100. Each ID gets one operation
  in this implementation; it is not repeatedly updated for `runtime` seconds.
  Each lane's model starts empty, so an expected absence is a successful check,
  not a read-performance miss. Use a fresh namespace.
- `remove_perf` always prepares its dataset first, regardless of
  `prepare-mode`, then partitions removals across lanes. It checks that each
  key is absent after removal.
- `replay` reads a schema-version-1 JSONL journal. It replays each lane's
  operations in file order, without reproducing recorded timing or comparing
  against the recorded result. Keys are regenerated from `object_id` and the
  current key settings; the recorded `key` is not used for lookup. The current
  lane count must cover all recorded lane IDs, or records for higher lanes
  are silently skipped. Match key, payload, and lane settings with the source
  run, and start from the state assumed by that run (normally an empty
  namespace). Replay does not restore a dataset snapshot.

Use `--journal all` to capture a complete metadata sequence for replay. The
default `failures` journal omits successful operations that may be prerequisites
for the failures. Journals are collected only by these four metadata scenarios;
performance/verification scenarios do not journal their Store calls.

## Command-line reference

Run `python3 mooncake-store/benchmarks/store_kv_bench.py --help` for the parser
options. Importing the script, including for help, requires the Store binding.
The tables below describe the current defaults and workload semantics.

### Connection and storage

| Option | Default | Meaning |
|---|---|---|
| `--scenario` | Required | One of the nine scenarios listed above. |
| `--local-hostname` | `127.0.0.1:50071` | Client endpoint; use a distinct endpoint for separate benchmark processes. |
| `--metadata-server` | `http://127.0.0.1:8080/metadata` | Transfer Engine metadata connection string. |
| `--master-server` | `127.0.0.1:50051` | Store master endpoint. |
| `--protocol` | `tcp` | Store transfer protocol; the installed binding must support it. |
| `--device-name` | Empty | Device list passed to Store setup. |
| `--global-segment-size` | `67108864` (64 MiB) | Storage memory contributed by this process; zero contributes none. |
| `--local-buffer-size` | `33554432` (32 MiB) | Local I/O buffer size, also the zcopy pool size limit. |
| `--enable-ssd-offload` | Disabled | Passes `enable_ssd_offload=True` to setup; required to initialize the DFS client backend too. |
| `--tenant-id` | `default` | Only `default` is accepted by this benchmark. |
| `--memory-replica-num` | `1` | Memory replicas requested by every write in the run. |
| `--nof-replica-num` | `0` | NoF replicas requested by writes. |
| `--dfs-replica-num` | `0` | DFS replicas requested by writes; supported Store values are zero or one. |

`MOONCAKE_STORE_MODULE` can select a compatible Python Store module instead of
the default `mooncake.store`. It must expose `MooncakeDistributedStore` and
`ReplicateConfig`, plus `BufferPool` for zcopy. DFS adapter, root, allocator,
and watermark settings are configured outside this CLI through the normal
Mooncake environment variables.

### Workload and dataset

| Option | Default | Meaning |
|---|---|---|
| `--io-api` | `plain` | Plain bytes APIs or `zcopy` buffer-pointer APIs. |
| `--numjobs` | `1` | Multiplier for the number of worker lanes. |
| `--iodepth` | `1` | Second lane-count multiplier; each lane is synchronous. |
| `--batch-size` | `1` | Maximum objects per performance request. |
| `--runtime` | `0` | Positive seconds bound timed read/write workloads; zero selects object-count operation. |
| `--nr-objects` | `128` | Default write/prepare budget, or ID count for metadata workloads. |
| `--write-objects` | `0` | Positive value overrides the write budget; also limits new writes in `mixed_rw`. |
| `--prepare-objects` | `0` | Positive value overrides the prepare budget. |
| `--prepare-mode` | `auto` | `auto` / `write` prepare read/mixed workloads; `none` skips prepare and leaves their read set empty. |
| `--object-id-start` | `0` | Initial ID for the run's deterministic key sequence. |
| `--key-prefix` | `kvbench` | Prefix, truncated or padded to fit the key. |
| `--key-size` | `20` | Total key character count, including the decimal ID suffix. |
| `--value-size` | `4096` | Bytes per payload and zcopy buffer slot. |
| `--rand-seed` | `1` | Seeds random mixed-workload choices per lane. `read_perf` is sequential. |
| `--pattern` | Empty | Repeated text or hex payload pattern. |
| `--verify` | Disabled | Enables read-performance payload checks; requires a nonempty pattern. |
| `--rwmixread` | `70` | Read-request percentage in `mixed_rw`; each request is a batch of one operation type. |
| `--put-pct` / `--get-pct` | `40` / `40` | Operation weights for `mixed_metadata`. |
| `--exist-pct` / `--remove-pct` | `10` / `10` | Remaining `mixed_metadata` weights; all four must sum to 100 even in other scenarios. |
| `--latency-sample-rate` | `100` | Timed runs retain the first request and every Nth request thereafter, per lane/phase; with `runtime=0`, every latency is retained. |

### Phase control and output

| Option | Default | Meaning |
|---|---|---|
| `--phase-gap-mode` | `none` | `none`, `sleep`, `manual`, or `file`. |
| `--phase-gap-sec` | `0` | Positive sleep duration when gap mode is `sleep`. |
| `--phase-gap-file` | Empty | File whose existence releases a `file` gap. |
| `--phase-gap-timeout-sec` | `600` | Maximum wait for the gap file. |
| `--output-dir` | Empty | Directory for `summary.json` and enabled `journal.jsonl`; no directory is created by default. |
| `--journal` | `failures` | `none`, `failures`, or `all`; writing requires `output-dir`. |
| `--summary-json` | Empty | Explicit summary path, overriding `output-dir/summary.json`; also works without `output-dir`. |
| `--replay-file` | Empty | Input JSONL journal; required for `replay`. |
| `--log-level` | `INFO` | Logging level. |

## Common Examples

### 1. Functional verification (`1+0`)

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario verify_write \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --global-segment-size $((64*1024*1024)) \
  --local-buffer-size $((32*1024*1024)) \
  --nr-objects 16 \
  --batch-size 4 \
  --key-prefix verify \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 1 \
  --nof-replica-num 0 \
  --verify \
  --pattern 0xab
```

### 2. NoF-only functional verification (`0+1`)

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario verify_write \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --global-segment-size 0 \
  --local-buffer-size $((8*1024*1024)) \
  --nr-objects 8 \
  --batch-size 2 \
  --key-prefix nofonly \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 0 \
  --nof-replica-num 1 \
  --verify \
  --pattern 0xcd
```

### 3. Read performance with automatic prepare phase

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario read_perf \
  --prepare-mode auto \
  --phase-gap-mode sleep \
  --phase-gap-sec 1 \
  --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --nr-objects 32 \
  --batch-size 4 \
  --runtime 5 \
  --key-prefix readperf \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 1 \
  --nof-replica-num 0 \
  --verify \
  --pattern 0xee
```

### 4. Mixed read/write with initial dataset

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario mixed_rw \
  --prepare-mode auto \
  --io-api zcopy \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 \
  --protocol tcp \
  --nr-objects 64 \
  --write-objects 4096 \
  --batch-size 4 \
  --runtime 10 \
  --rwmixread 70 \
  --key-prefix mixed \
  --key-size 20 \
  --value-size 4096 \
  --memory-replica-num 1 \
  --nof-replica-num 1 \
  --verify \
  --pattern 0x5a
```

In `mixed_rw`, reads are served from the prepared object set, while writes
always use fresh object ids. This keeps the workload as "existing-object read +
new-object write" and avoids key overlap between the read and write streams.

### 5. DFS setup

This section shows how to configure the master and benchmark client for DFS
replicas. The same setup supports both memory-plus-DFS and DFS-only placement;
the replica-count options select the placement.

Prerequisites:

- Build `mooncake_master`; it allocates DFS replicas and tracks their metadata.
- Install the matching Python binding for the benchmark client so that
  `python3 -c 'import mooncake.store'` succeeds.
- Choose an absolute DFS root directory writable by the master and clients.
  Use a dedicated directory for this example. The master preallocates four
  16 MiB shard files, for 64 MiB total capacity.
- For the native `hf3fs` adapter, mount a running 3FS cluster, install its
  client API library and headers, and build Mooncake with `-DUSE_3FS=ON`.
  See the [HF3FS USRBIO adapter guide](../src/hf3fs/README.md).

Run the following setup in **both terminals**, from the repository root.
The POSIX adapter provides a local filesystem smoke test. For native 3FS,
set `MOONCAKE_DFS_FS_ADAPTER=hf3fs` and set `MOONCAKE_DFS_ROOT_DIR` to a
writable directory on the 3FS mount in both terminals before creating the
directories. The master and every DFS client must use the same absolute root
path string, adapter, and shard layout.

```bash
export MOONCAKE_DFS_ROOT_DIR=/tmp/mooncake-kvbench-dfs
export MOONCAKE_DFS_FS_ADAPTER=posix
export MOONCAKE_DFS_SHARD_COUNT=4
export MOONCAKE_DFS_SHARD_CAPACITY=$((16*1024*1024))
export MOONCAKE_DFS_ALIGNMENT=4096
export MOONCAKE_DFS_SINGLE_TENANT=true
export MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR=distributed_storage_backend
export MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake-kvbench-filestorage
export MOONCAKE_MASTER=127.0.0.1:50051
mkdir -p "$MOONCAKE_DFS_ROOT_DIR" "$MOONCAKE_OFFLOAD_FILE_STORAGE_PATH"
```

`MOONCAKE_OFFLOAD_FILE_STORAGE_PATH` must already exist as an absolute,
writable, non-symlink directory. It initializes client FileStorage; DFS shard
data is stored under `MOONCAKE_DFS_ROOT_DIR`.

In the first terminal, enable DFS allocation on the master and start its HTTP
metadata server. Wait for the master to report that the DFS allocator is ready:

```bash
MOONCAKE_ENABLE_DFS=1 ./build/mooncake-store/src/mooncake_master \
  --rpc_address=127.0.0.1 --rpc_port=50051 \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=127.0.0.1 --http_metadata_server_port=8080
```

In the second terminal, request one memory replica and one DFS replica, then
write and verify 64 objects of 64 KiB each:

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario verify_write --io-api plain \
  --local-hostname 127.0.0.1:50071 \
  --metadata-server http://127.0.0.1:8080/metadata \
  --master-server 127.0.0.1:50051 --protocol tcp \
  --enable-ssd-offload --memory-replica-num 1 --nof-replica-num 0 --dfs-replica-num 1 \
  --nr-objects 64 --batch-size 4 --value-size 65536 \
  --key-prefix dfsverify --key-size 32 --verify --pattern 0xab
```

For DFS-only placement, change `--memory-replica-num` to `0`, keeping
`--nof-replica-num 0 --dfs-replica-num 1`. Use a fresh key prefix for each run.
Readback follows normal replica selection as described above.

Use `--io-api zcopy` to exercise the zero-copy APIs. `--enable-ssd-offload`
initializes the client's DFS backend and is required for DFS readers as well
as writers. For configuration details and the experimental DFS limitations,
see [descriptor-based DFS storage](../../docs/source/deployment/mooncake-store-deployment-guide.md#descriptor-based-dfs-storage).

### 6. Metadata smoke test, journal, and replay

With the master and metadata service at the default addresses:

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario metadata_smoke --io-api plain \
  --numjobs 2 --iodepth 2 --nr-objects 4 \
  --key-prefix metasmoke --key-size 32 --value-size 512 --pattern 0x5a \
  --journal all --output-dir /tmp/kvbench-smoke
```

This runs four lanes with one key each, producing 20 operations. All four keys
are removed by the end of a successful run. Replay with the same lane count and
key/payload settings, using a different output directory:

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario replay --io-api plain \
  --numjobs 2 --iodepth 2 --nr-objects 4 \
  --key-prefix metasmoke --key-size 32 --value-size 512 --pattern 0x5a \
  --replay-file /tmp/kvbench-smoke/journal.jsonl \
  --journal all --output-dir /tmp/kvbench-replay
```

### 7. Seeded metadata mix

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario mixed_metadata --io-api plain \
  --numjobs 2 --iodepth 2 --nr-objects 1000 \
  --key-prefix metamix --key-size 32 --value-size 512 --pattern 0xcd \
  --put-pct 40 --get-pct 30 --exist-pct 20 --remove-pct 10 \
  --rand-seed 7 --journal all --output-dir /tmp/kvbench-metamix
```

This selects 1000 single-key operations in total. A get or existence check on
a key that the lane model considers absent succeeds when it reports absence.
It is not the same workload as `mixed_rw`, which reads an existing prepared
dataset and writes fresh objects.

### 8. Removal performance

```bash
python3 mooncake-store/benchmarks/store_kv_bench.py \
  --scenario remove_perf --io-api plain \
  --numjobs 2 --iodepth 2 --nr-objects 1000 \
  --key-prefix removebench --key-size 32 --value-size 4096 \
  --journal all --output-dir /tmp/kvbench-remove
```

The `remove_perf` phase includes a post-remove existence probe in each measured
operation. Its prepare writes appear in the summary but not in the journal, so
the journal alone cannot recreate the initial dataset.

## Output and interpretation

Each phase prints request and KV success/failure counts, misses, verification
failures, processed bytes, duration, request/object throughput, latency mean and
percentiles, `dataset_exhausted`, and aggregated errors.

- `requests` counts measured operations; a data batch is one request.
  A metadata operation can contain more than one Store call, such as a remove
  followed by an existence probe.
- `kvs` counts all attempted object operations, including failures. Thus `req/s`
  and `kv/s` are attempted-operation rates. Check `successful_kvs`, misses, and
  errors alongside them; a high miss rate can make a read test appear fast.
- For data workloads, `bytes` counts successful objects times `value-size`.
  Metadata workloads count payload bytes according to the individual operation;
  existence checks and removes contribute no bytes. `MiB/s` divides by
  `1024 * 1024`.
- Latencies are measured per request, not per object in a batch, and reported
  in milliseconds. A larger batch need not have the same per-object cost as
  its request latency. With `runtime=0`, all request latencies are retained;
  with positive `runtime`, sampling uses `latency-sample-rate` per lane and
  phase. The JSON includes `latency_samples`.
- `dataset_exhausted` indicates a phase exhausted its ID budget/read set, or
  a fixed write failed to produce the full requested dataset. Timed writes can
  end before `runtime` when their finite write budget runs out.
- An overall summary merges every phase, with elapsed time from the earliest
  worker start to the latest worker finish. It includes preparation and gaps;
  use individual performance phases when comparing read/write throughput.

### Files

When requested, the benchmark writes:

- `summary.json` under `output-dir`, or the explicit `summary-json` path.
  It contains `schema_version: 1`, `scenario`, `ok`, a `phases` object keyed by
  phase name, `overall`, and `journal_records`. `ok` is false if any KV operation
  failed or verification failed. The summary is replaced atomically via a
  temporary file.
- `journal.jsonl` under `output-dir`, unless `journal=none`. Records are sorted
  by invocation time, lane, and operation ID. Each record contains
  `schema_version`, `op_id`, `lane`, `op`, `object_id`, `key`, `invoke_ns`,
  `return_ns`, `result`, `ok`, `expected_present`, and `actual_present`.
  The four metadata scenarios populate this journal; other scenarios can
  produce an empty file even with `journal=all`.

Files are written at the end of a completed run, not streamed or checkpointed
during the workload. Existing output files at those paths are replaced. Use
different output directories when retaining multiple runs or replay inputs.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Completed without one of the exit conditions below. This does not guarantee all performance operations succeeded. |
| `1` | Setup, validation, strict-write, worker, or output exception. `metadata_smoke` also raises on failed operations. |
| `2` | Argument-parser error, such as a missing required scenario or invalid choice. |
| `20` | At least one payload verification failure in a completed run. |
| `21` | `--verify` is set and a phase with `read` in its name reports misses, after checking for code 20. |
| `22` | A completed metadata scenario has failed KV operations, after the preceding checks. |
| `130` | Interrupted with Ctrl-C. |

For example, a non-strict `fill` or `write_perf` run may report write failures
and still exit zero; check the counters and JSON `ok`. A completed
`metadata_smoke` failure normally exits through its exception path with code 1,
rather than producing a summary with code 22.

If a worker raises an exception, the benchmark waits for all workers to finish,
reports the failed phase and lane, and closes the shared Store after they have
joined. It exits with code `1` without writing a new summary or journal; files
from an earlier run remain unchanged. The normal completed-run status checks
(20/21/22) happen after output files have been written.
