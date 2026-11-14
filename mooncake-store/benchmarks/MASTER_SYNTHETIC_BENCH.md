# Synthetic master workload benchmark

`master_synthetic_bench` generates a configurable, stateful workload for an
already-running Mooncake master. It does not read a trace and does not start or
stop the master.

The benchmark models independent storage segments. Each segment has:

- its own mounted memory segment and heartbeat client;
- independent Exist, Put, and Get arrival streams;
- a synchronous RPC worker pool;
- a monotonically increasing Put key sequence; and
- a bounded pool of successfully committed keys for later Exist/Get requests.

## Build

From the repository root:

```bash
cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_UNIT_TESTS=ON
cmake --build build --target master_synthetic_bench -j
```

The resulting executable is:

```text
build/mooncake-store/benchmarks/master_synthetic_bench
```

## Run

Start a Mooncake master first:

```bash
./build/mooncake-store/src/mooncake_master
```

Then launch the benchmark from another shell:

```bash
./build/mooncake-store/benchmarks/master_synthetic_bench \
  --master_server=127.0.0.1:50051 \
  --duration=300 \
  --key_tag=synthetic-run-01
```

The default profile uses three 256 GiB segments and Poisson arrivals:

```bash
./build/mooncake-store/benchmarks/master_synthetic_bench \
  --master_server=127.0.0.1:50051 \
  --duration=300 \
  --num_segments=3 \
  --segment_size=274877906944 \
  --workers_per_segment=4 \
  --arrival_model=poisson \
  --exist_qps_per_segment=6.3359 \
  --put_qps_per_segment=3.1431 \
  --get_qps_per_segment=2.0242 \
  --exist_batch_size=86 \
  --put_batch_size=45 \
  --get_batch_size=128 \
  --value_size=458752 \
  --put_commit_delay_us=1152 \
  --key_tag=synthetic-run-01 \
  --key_size=64 \
  --key_pool_size=1000000 \
  --exist_hit_ratio=0.5 \
  --get_hit_ratio=1.0 \
  --placement_mode=preferred \
  --replica_num=1 \
  --ping_interval_ms=1000 \
  --max_pending_events_per_segment=100000 \
  --seed=1 \
  --cleanup_segments=true
```

Use a unique `key_tag` for concurrent runs against the same master. The tag is
included in generated key names and segment names.

## Workload semantics

### Open-loop arrivals

Exist, Put, and Get are independent open-loop streams for every segment.
`arrival_model=poisson` samples exponential inter-arrival times, while
`arrival_model=fixed` uses exact `1 / QPS` intervals. Slow RPCs do not reduce
the configured arrival rate.

`put_qps_per_segment` counts complete Put transactions. A successful Put task
issues `BatchPutStart`, waits for `put_commit_delay_us`, and then issues
`BatchPutEnd`. Excluding heartbeats, the target RPC rate is:

```text
num_segments *
  (exist_qps_per_segment +
   2 * put_qps_per_segment +
   get_qps_per_segment)
```

Setting an operation's QPS to zero disables that stream. At least one operation
must have a positive QPS.

### Keys and hit ratios

Generated keys have the natural form:

```text
<tag>:<k-or-m>:s<8-digit-segment-index>:<16-hex-digit-id>
```

`k` identifies the monotonically increasing Put namespace. `m` identifies a
deliberate miss and cannot collide with a later Put. `key_size` pads the
natural representation to the exact requested size; zero keeps the natural
length.

Only keys that successfully complete both PutStart and PutEnd enter the
segment's committed-key pool. Exist and Get independently sample this pool
according to their configured hit ratio. The observed hit rate can be lower
than requested while the pool is empty or after the master evicts an object.

### Placement

With `placement_mode=preferred`, each Put asks the master to allocate on the
workload's segment through `ReplicateConfig::preferred_segments`. This is a
preference rather than a strict placement constraint. Use
`placement_mode=global` to omit the preference.

### Backpressure

Every segment has a bounded pending queue. If a queue reaches
`max_pending_events_per_segment`, that segment stops generating requests,
drains already scheduled work, and the process exits with code 2. Requests are
not silently dropped or rate-limited.

## Parameters

| Parameter | Default | Meaning |
| --- | ---: | --- |
| `master_server` | `127.0.0.1:50051` | Address of an already-running master. |
| `duration` | `60` | Request generation time in seconds. Queue drain time is additional. |
| `num_segments` | `3` | Number of independent synthetic workload segments. |
| `segment_size` | `274877906944` | Advertised size of each segment in bytes. |
| `workers_per_segment` | `4` | Synchronous RPC workers assigned to each segment. |
| `exist_qps_per_segment` | `6.3359` | BatchExistKey requests/s for each segment. |
| `put_qps_per_segment` | `3.1431` | Complete Put transactions/s for each segment. |
| `get_qps_per_segment` | `2.0242` | BatchGetReplicaList requests/s for each segment. |
| `arrival_model` | `poisson` | Request interval model: `poisson` or `fixed`. |
| `exist_batch_size` | `86` | Keys per BatchExistKey request. |
| `put_batch_size` | `45` | Keys per Put transaction. |
| `get_batch_size` | `128` | Keys per BatchGetReplicaList request. |
| `value_size` | `458752` | Value size recorded for every Put key, in bytes. |
| `put_commit_delay_us` | `1152` | Delay between successful PutStart and PutEnd. |
| `key_tag` | `synthetic` | Prefix in generated keys and segment names. |
| `key_size` | `64` | Exact key size in bytes; zero keeps the natural length. |
| `key_pool_size` | `1000000` | Maximum committed key IDs retained per segment. |
| `exist_hit_ratio` | `0.5` | Requested fraction of Exist keys sampled from committed keys. |
| `get_hit_ratio` | `1.0` | Requested fraction of Get keys sampled from committed keys. |
| `placement_mode` | `preferred` | Put placement mode: `preferred` or `global`. |
| `replica_num` | `1` | Memory replica count requested by PutStart. |
| `ping_interval_ms` | `1000` | Heartbeat interval for every mounted segment. |
| `max_pending_events_per_segment` | `100000` | Per-segment pending queue limit. |
| `seed` | `1` | Base seed for arrivals and key selection. |
| `cleanup_segments` | `true` | Unmount benchmark-created segments before exit. |

Use `--help` to inspect the flags compiled into the executable.

## Output and exit codes

The final log reports scheduled and completed logical tasks, RPC failure
events, attempted Put transactions, committed keys, retained keys, Exist
results, maximum queue depth, maximum scheduler lateness, overload events,
generation time, drain time, and total elapsed time.

| Code | Meaning |
| ---: | --- |
| `0` | The run completed without sender queue overload. Individual RPC failures may still be reported. |
| `1` | Flag validation, connection, mount, or another fatal operation failed. |
| `2` | A per-segment pending queue reached its configured limit. |
