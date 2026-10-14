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

Fault injection, NoF register / unregister, heartbeat trigger, memory segment
unmount, and target-side operations are intentionally out of scope. The tool
supports phase gaps so external tools can finish those operations before the
next phase continues.

## Supported Scenarios

- `verify_write`
  - Fixed-count write followed by full readback verification
- `fill`
  - Fixed-count write used for filling a dataset / eviction watermark
- `write_perf`
  - Time-based or fixed-count write benchmark
- `read_perf`
  - Optional prepare-write phase, then read performance benchmark
- `mixed_rw`
  - Optional prepare-write phase, then mixed "read prepared objects + write new objects"

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

`zcopy` mode automatically allocates temporary user buffers and registers them
with `register_buffer`.

## Key Rules

- Keys are generated deterministically:
  - `{prefix padded/truncated to fit}{16-digit object id}`
- The same `key-prefix`, `key-size`, and `object-id-start` produce the same key sequence
- `verify` currently requires `pattern`
- Any write-involved scenario requires `value-size` to be 512-byte aligned
- `memory-replica-num` and `nof-replica-num` cannot both be `0`
- `dfs-replica-num` adds a DFS replica; it may be `0` or `1`, and `1` requires
  `memory-replica-num >= 1`
- Any process that writes or reads DFS replicas must pass `--enable-ssd-offload`,
  which maps to `setup(enable_ssd_offload=True)`; without it DFS puts fail with
  `-1601` (`DFS_SERVICE_UNAVAILABLE`)
- `prepare-objects`
  - Controls how many objects are written by the prepare phase
  - `0` means reuse `nr-objects`

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
starting the benchmark.

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

### 5. DFS setup and verification

This section shows how to configure the master and benchmark client for DFS
replicas, then write objects with both memory and DFS replicas and verify their
contents with a readback.

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
  --enable-ssd-offload --memory-replica-num 1 --dfs-replica-num 1 \
  --nr-objects 64 --batch-size 4 --value-size 65536 \
  --key-prefix dfsverify --key-size 32 --verify --pattern 0xab
```

Use `--io-api zcopy` to exercise the zero-copy APIs. `--enable-ssd-offload`
initializes the client's DFS backend and is required for DFS readers as well
as writers. For configuration details and the experimental DFS limitations,
see [descriptor-based DFS storage](../../docs/source/deployment/mooncake-store-deployment-guide.md#descriptor-based-dfs-storage).

## Output

Each phase prints:

- request counts
- KV counts
- miss / verify-failure counts
- bytes processed
- duration
- `req/s`
- `kv/s`
- `MiB/s`
- `lat_mean`
- `lat_p50`
- `lat_p95`
- `lat_p99`
- aggregated error counts

An overall summary is printed after all phases complete.

If a worker raises an exception, the benchmark waits for all workers to finish,
reports the failed phase and lane, and exits with code `1`. This error path does
not write a new summary or journal; any output files from an earlier run remain
unchanged.
