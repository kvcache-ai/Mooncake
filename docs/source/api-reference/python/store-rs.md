# Store-RS Python API

The Python Store facade selects its implementation when mooncake.store is
first imported. The shared API and default C++ backend are documented in
[Mooncake Store Python API](mooncake-store.md). This page describes Store-RS
runtime and API behavior. See the
[source-checkout quickstart](../../getting_started/store-rs.md) to build and
install the unified root wheel.

## What the Python Layer Provides

- `MooncakeDistributedStore` as the main compatibility API
- `MooncakeHostMemAllocator` for caller-owned registered buffers
- real and dummy execution modes for Mooncake / HiCache-style integration
- batch I/O, registered-buffer I/O, and multi-buffer I/O
- the same storage-owner CLOCK eviction and route-owner CAS reclaim as Rust callers
- the same background watermark eviction defaults as Rust callers
- route query, lifecycle, and metrics helpers

## Execution Modes

The Python compatibility layer supports two execution styles.

### Real mode

Use `setup(...)` when Python should talk to the native distributed store runtime directly.

For tenant-scoped routing and resource policy, prefer `mooncake-store-rs-admin policy ...` and durable metadata. Python `setup(...)` route knobs are kept as compatibility/bootstrap fallbacks.

Isolation knobs:

- `keyspace` isolates metadata-backed routing, policy lookup, and object visibility
- `tenant`, `domain`, and `object_set` select the default namespace scope applied to Python compatibility read/write operations
- with `classic_te` and Redis-backed transport metadata, the default `tenant` also scopes upstream transfer-engine Redis keys under `mooncake/tenants/<tenant>/...`
- `worker_scope` isolates compat-local worker state such as the dispatcher executor and local hot-cache domain
- when `worker_scope` is omitted, the Python layer derives it from `keyspace` when present; otherwise dummy mode falls back to the legacy `worker-1` scope so omitted-scope clients keep matching standalone daemon side-channel aliases

This mode:

- constructs a Rust `StoreClient`
- dispatches compatibility calls directly into the native store object so concurrent Python threads are not serialized behind a single Python worker queue
- registers local memory and participates in route / allocator control plane RPC
- starts a background heartbeat loop after setup so long-lived Python runtimes keep their lease live and repair local metadata after Redis connectivity returns without requiring explicit `heartbeat()` calls
- participates in the same hit-report and replica-route tracking RPC used by Rust clients
- supports routed writes, replication policy, segment lifecycle, metrics, and tracing
- is the normal path for Python clients that should behave like store-rs nodes

Registered-buffer batch restores are best-effort per item in the Python compatibility layer. If
the fast batch restore path fails because one route is missing or one storage owner is unavailable,
the dispatcher retries the missed entries one by one and returns a per-key status: positive byte
counts for restored entries and negative soft-miss codes for the entries that still failed. A dead
cache replica therefore does not turn healthy keys in the same HiCache batch into misses.

Real-mode `batch_put_from(...)` keeps the caller's batch together by default so the Rust write path
can coalesce placement, remote transfer, route publication, and replica tracking. Operators that
prefer Python-side sharding for a specific workload can set
`MC_STORE_RS_PY_BATCH_PUT_FROM_FANOUT` to an explicit positive fanout width.
Real-mode callers use `register_buffer(ptr, size)` for registered source buffers,
matching the upstream Mooncake Python API. CUDA buffers are transferred as
registered sources and are not read by the CPU; writes therefore require remote
registered-transfer placement and publish routes without a CPU payload checksum.

`setup(...)` accepts `eviction_high_watermark_percent=` and
`eviction_low_watermark_percent=` for storage-role runtimes. The same values can come from
`MC_STORE_RS_EVICTION_HIGH_WATERMARK_PERCENT` and
`MC_STORE_RS_EVICTION_LOW_WATERMARK_PERCENT`. They feed the Rust `LocalMemoryConfig` directly;
the high watermark must be in `1..=100`, and the low watermark must be lower than the high
watermark.

### Dummy mode

Use `setup_dummy(...)` when Python should behave like the upstream dummy compatibility client.

`setup_dummy(...)` now also accepts optional keyword-only `keyspace=` and `worker_scope=` arguments. They do not change the standalone server's underlying distributed-store namespace by themselves; they control the Python compat worker boundary used by the dummy side channels. In practice:

- `keyspace` remains the metadata/data-plane namespace knob for the real runtime that the standalone daemon was started with
- `worker_scope` isolates dummy compat-local state such as hot-cache SHM, registered-region side channels, and worker-local dispatcher state
- when `worker_scope` is omitted, the Python layer derives it from `keyspace` when present; otherwise it allocates a unique per-setup worker scope
- a daemon started with an explicit `keyspace` or `worker_scope` also exposes a legacy `worker-1` side-channel alias so omitted-scope dummy buffer clients can still register SHM regions
- a daemon started with a wildcard `client_server_address` such as `0.0.0.0:16590` or `[::]:16590` also exposes dummy shm and hot-cache side-channel aliases for its advertised hostname plus loopback forms, so clients may connect through a concrete host address without breaking buffer registration

This mode:

- connects to a standalone `mooncake-store-rs-client` process over gRPC
- registers shm regions by passing file descriptors over a Unix socket
- only reports dummy `register_buffer(...)` success after the standalone daemon has installed the shared region in its dispatcher, so callers can issue `batch_put_from(...)` or `batch_get_into(...)` immediately without adding an extra sleep/retry fence
- derives short hashed Unix socket filenames for dummy shm and hot-cache side channels so long worker scopes stay below AF_UNIX path limits
- keeps the Python process out of the distributed control plane
- is the compatibility path used by the HiCache dummy flow

Dummy mode is intentionally narrower than real mode. It exists to preserve compatibility for callers that expect the old dummy client / standalone server split.

The standalone server behind dummy mode still uses the same Rust runtime internally, so route publication, reclaim, eviction, tracing, and metrics stay aligned with the real path.

Compatibility note:

- legacy Python alias names such as `put_batch(...)` and `get_batch(...)` remain available
- dummy mode also supports high-level `batch_put(...)` and `batch_get(...)` compatibility calls for legacy black-box tests
- registered-buffer and multi-buffer batch APIs are still the preferred throughput path for dummy-mode validation

## Local Hot Cache

The Python compatibility layer can enable a daemon-local hot read cache for both real-mode clients and standalone dummy daemons.

In real mode:

- the first remote read populates the local cache
- later reads from the same runtime can reuse the cached bytes without another remote transfer
- successful writes or deletes issued by that same runtime invalidate the matching local cache entry

In dummy mode:

- start the standalone daemon with `MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1`
- each dummy client maps the daemon hot-cache shm region on connect
- a dummy read first asks the daemon for a hot-cache handle and falls back to the regular dummy RPC path on miss
- dummy clients only share hot-cache SHM hits when they connect through the same worker-scoped dummy server boundary
- different worker scopes do not reuse each other's cached bytes, even when they point at the same underlying store client/runtime

Configuration uses the upstream environment variable names:

```bash
export MC_STORE_LOCAL_HOT_CACHE_SIZE=$((256 * 1024 * 1024))
export MC_STORE_LOCAL_HOT_BLOCK_SIZE=$((4 * 1024 * 1024))
export MC_STORE_LOCAL_HOT_CACHE_USE_SHM=1   # only needed when dummy clients should share hits
```

Design boundary:

- the cache is a local read accelerator only
- it is not written into Redis or etcd
- it does not change route ownership, replica ownership, or placement policy
- values larger than the configured block size bypass the cache

## Structured Object Store Helper

`mooncake.store.rs.structured_object_store` provides a higher-level helper for one logical object that contains multiple named members.
It is designed for cases such as rollout / batch transfer where callers want to keep their own object semantics locally while using Mooncake for fast payload movement.

The helper separates two concepts:

- **structured object path**: named members with metadata-aware materialization;
- **generic bundle path**: manifest + named payloads when the caller only needs raw grouped objects.

### Main types

```python
from mooncake.store.rs.structured_object_store import (
    MooncakeBundleTransfer,
    StructuredMemberSlice,
    StructuredObjectPayload,
)
```

- `MooncakeBundleTransfer`: public helper facade built on a `MooncakeDistributedStore`.
- `StructuredObjectPayload`: structured object to write. Members are passed in `buffers`, and optional object metadata is passed in `metadata`.
- `StructuredMemberSlice`: slice selection for one structured member during reads.

### Structured object write and full read

Use `put_structured_object()` to write one structured object. The default read path is `read_spec(ref)`, and full-object materialization is just the default case of the partial-read API.

```python
import numpy as np
from mooncake.store import MooncakeDistributedStore
from mooncake.store.rs.structured_object_store import MooncakeBundleTransfer, StructuredObjectPayload

store = MooncakeDistributedStore()
transfer = MooncakeBundleTransfer(store, key_prefix="demo/structured")

payload = StructuredObjectPayload(
    metadata={"step": 7, "layout": "rollout"},
    buffers={
        "tokens": np.array(range(24), dtype=np.int32).reshape(6, 4),
        "mask": np.ones((6, 4), dtype=np.int8),
        "prompt_ids": b"sample-ids",
    },
)

ref = transfer.put_structured_object(payload)
result = transfer.materialize(transfer.read_spec(ref))

tokens = result.objects["tokens"]
prompt_ids = result.objects["prompt_ids"]
metadata = result.metadata
```

### Partial reads

Read narrowing happens on top of `read_spec(ref)`:

- `select_members([...])` keeps only selected members;
- `slice_member(name, axis=0, start=..., end=...)` slices one ndarray member;
- `materialize(spec)` returns newly materialized objects.

```python
spec = (
    transfer.read_spec(ref)
    .select_members(["tokens"])
    .slice_member("tokens", axis=0, start=2, end=5)
)
result = transfer.materialize(spec)

selected_tokens = result.objects["tokens"]
```

Current scope:

- byte members support full-member reads;
- ndarray members support full reads and sliced reads;
- full read is the default `read_spec(ref)` case.

### Reusing caller-owned destinations

Use `materialize_into()` when the caller already owns the destination ndarray buffers and wants Mooncake to fill them directly.

```python
destination = np.empty((3, 4), dtype=np.int32)
spec = (
    transfer.read_spec(ref)
    .select_members(["tokens"])
    .slice_member("tokens", axis=0, start=2, end=5)
)
result = transfer.materialize_into(spec, {"tokens": destination})

assert result.objects["tokens"] is destination
```

`materialize_into()` is only for members whose destination layout is already known to the caller. For byte members or default object reconstruction, use `materialize()`.

### Generic bundle fallback

If the caller does not need structured member semantics, the same helper also supports raw named bundles:

- `put_bundle(...)`
- `remove_bundle(...)`

Use the bundle path when the object is just a manifest plus named payloads, and use the structured object path when callers want member selection, slicing, and ndarray-aware materialization.

## Basic Real-Mode Example

```python
from mooncake.store import MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "P2PHANDSHAKE",                      # arg2 transport_metadata_url: TE input (default for classic_te)
    128 * 1024 * 1024,
    16 * 1024 * 1024,
    "tcp",
    "",
    "redis://127.0.0.1:6380/0",          # arg7 metadata_url: Store-RS metadata backend (required)
    stable_id="py-store-a",
    tenant="default",
    domain="sglang-chat",
    object_set="deepseek-r1__2026-04-19-build-44",
    labels={"pool": "pool-a", "storage": "true"},
    transport_backend="classic_te",
    transport_rpc_port=17111,
)

store.put("hello", b"world")
assert store.get("hello") == b"world"
```

When `domain` and `object_set` are omitted from `setup(...)`, the Python wrapper also accepts `MC_STORE_RS_DOMAIN` and `MC_STORE_RS_OBJECT_SET` as startup fallbacks. The selected default scope is applied consistently to real-mode compatibility reads, writes, route queries, removes, existence checks, size checks, batch operations, registered-buffer operations, and local hot-cache keys.

## Routed Writes

```python
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "P2PHANDSHAKE",                      # arg2 transport_metadata_url: TE input (default for classic_te)
    128 * 1024 * 1024,
    16 * 1024 * 1024,
    "tcp",
    "",
    "redis://127.0.0.1:6380/0",          # arg7 metadata_url: Store-RS metadata backend
    stable_id="router-a",
    labels={"pool": "pool-a", "storage": "false"},
    routed_writes=True,
    replica_count=2,
    route_topk=2,
)

policy = ReplicateConfig(
    replica_num=2,
    prefer_local=False,
    preferred_storage_owners=["store-b"],
)
store.put("key", b"payload", config=policy)
```

## Basic Dummy-Mode Example

Start the standalone compatibility server first:

```bash
mooncake-store-rs-client \
  --local-hostname 127.0.0.1 \
  --metadata-url redis://127.0.0.1:6380/0 \
  --storage-bytes $((64 * 1024 * 1024)) \
  --scratch-bytes $((16 * 1024 * 1024)) \
  --stable-id dummy-server \
  --client-server-address 127.0.0.1:16590
```

Then connect from Python:

```python
from mooncake.store import MooncakeDistributedStore, MooncakeHostMemAllocator

store = MooncakeDistributedStore()
store.setup_dummy(
    64 * 1024 * 1024,
    16 * 1024 * 1024,
    "127.0.0.1:16590",
    keyspace="tenant-a",
    worker_scope="tenant-a-dummy-worker",
)

allocator = MooncakeHostMemAllocator()
ptr = allocator.alloc(4096)
store.register_buffer(ptr, 4096)
```

`setup_dummy(...)` only needs `client_server_address` for the remote endpoint. It does not consume `transport_rpc_port`, because the standalone server owns the real store runtime and data-plane endpoint on behalf of the dummy client. Use `keyspace` to align with the intended metadata namespace and `worker_scope` when you need an explicit compat worker boundary for dummy-side cache and shm isolation. When callers omit `keyspace`, the Python wrapper now also falls back to `MC_STORE_RS_KEYSPACE` before deriving the dummy worker scope, so upstream SGLang dummy mode can share the same scoped side-channel namespace as a `mooncake-store-rs-client --keyspace ... --client-server-address ...` gateway. A standalone daemon that binds `client_server_address` on a wildcard host also publishes side-channel aliases for its advertised host address, `127.0.0.1`, `::1`, and `localhost`, so dummy buffer clients do not need the daemon to bind the exact same host string they dial.

## Host Allocator and Hugepages

`MooncakeHostMemAllocator` is the Python-side allocator for stable registered buffers.

Default behavior:

- when the native extension is available, it allocates shm regions with `memfd` and shared `mmap`
- when the native extension is not available, it falls back to Python `mmap`
- the pure-Python fallback does not support hugepages

Hugepage options:

- `use_hugepage=True`
- `hugepage_size="2MB"` or `hugepage_size="1GB"`

Example:

```python
from mooncake.store import MooncakeHostMemAllocator

allocator = MooncakeHostMemAllocator(use_hugepage=True, hugepage_size="2MB")
ptr = allocator.alloc(2 * 1024 * 1024)
allocator.free(ptr)
```

The same hugepage knobs are also accepted by `MooncakeDistributedStore.setup(...)` and the config-dict path. They control the store client's local storage and scratch allocator.

Supported hugepage sizes:

- `2MB`
- `1GB`

Related environment variables:

- `MC_STORE_USE_HUGEPAGE`
- `MC_STORE_HUGEPAGE_SIZE`

If hugepage mode is requested, the host kernel must already have compatible hugepages reserved.

## BufferPool

`BufferPool` is the recommended Python helper for temporary buffers used with
`put_from(...)`, `get_into(...)`, and the batch buffer-oriented APIs. It allocates
from the store client's setup-time registered local buffer first, so normal pool
acquire/release operations do not repeatedly register or unregister RDMA memory.
Only when the local buffer is exhausted does it fall back to dynamically
registered overflow memory.

Import paths:

```python
from mooncake import BufferPool
# or
from mooncake.store import BufferPool
# or
from mooncake.buffer_pool import BufferPool
```

Basic usage:

```python
from mooncake.store import BufferPool, MooncakeDistributedStore

store = MooncakeDistributedStore()
store.setup(
    "127.0.0.1",
    "P2PHANDSHAKE",
    64 * 1024 * 1024,  # global segment size
    16 * 1024 * 1024,  # local buffer size used by BufferPool
    "tcp",
    "",
    "redis://127.0.0.1:6379/0",
)

pool = BufferPool(store, max_bytes=32 * 1024 * 1024)

payload = b"hello mooncake"
writer = pool.acquire(len(payload))
try:
    writer.buffer[: len(payload)] = payload
    status = store.put_from("example-key", writer.ptr, len(payload))
    if status != 0:
        raise RuntimeError(f"put_from failed: {status}")
finally:
    writer.release()

reader = pool.acquire(len(payload))
try:
    bytes_read = store.get_into("example-key", reader.ptr, reader.size)
    if bytes_read < 0:
        raise RuntimeError("get_into missed or failed")
    value = bytes(reader.buffer[:bytes_read])
finally:
    reader.release()

pool.close()
```

Context-manager usage releases the lease automatically:

```python
with pool.acquire(4096) as lease:
    n = store.get_into("example-key", lease.ptr, lease.size)
    value = bytes(lease.buffer[:n])
```

Important notes:

- `BufferPool` requires a real store that was set up with nonzero
  `local_buffer_size`; dummy stores and stores without local buffer capacity
  cannot create a pool.
- The hot path is local-buffer slice allocation and return. Dynamic
  `register_buffer(...)` / `unregister_buffer(...)` is only used for overflow
  allocations after the local buffer is exhausted.
- A lease exposes `ptr`, `size`, `buffer`, and `release()`. Pass `ptr` and the
  actual payload length to `put_from(...)`; pass `ptr` and destination capacity
  to `get_into(...)`.
- Do not call `release()` while any `memoryview` returned by `lease.buffer` is
  still alive. Drop the view first, or use short-lived slices/copies.
- `pool.acquire(size, block=True, timeout=None)` waits when the pool reaches its
  configured capacity. Use `block=False` for immediate failure, or set `timeout`
  to bound the wait.
- `max_bytes` limits total active local and overflow leases. When `max_bytes=0`,
  the pool defaults to a capacity derived from the local buffer size.
- `max_regions` can bound the number of simultaneously active leases.
- `pool.close()` fails if leases are still active. Release all leases before
  closing the pool.
- `RegisteredBufferPool` is not part of the Store-RS Python API. Use
  `BufferPool` for pooled temporary buffers, or `MooncakeHostMemAllocator` plus
  explicit `register_buffer(...)` for caller-owned long-lived memory.

## Supported Operations

### Basic I/O

- `put`, `get`, `remove`
- `batch_put`, `batch_get`, `batch_remove`
- `query_route`, `is_exist`, `get_size`

### Buffer-Oriented I/O

- `BufferPool`
- `register_buffer`, `unregister_buffer`
- `put_from`
- `batch_put_from`
- `batch_put_from_multi_buffers`
- `batch_get_into`
- `batch_get_into_multi_buffers`
- tensor-parallel `_into` reads can reconstruct full or shard targets directly into registered buffers; full batch reconstruction shares route-query results and issues combined ranged reads when possible

## Cache Soft-Fail Semantics

The Python compatibility API treats KV cache data-plane failures as best-effort misses or failed cache operations so optional HiCache traffic does not crash an inference process. Strict setup, configuration, metrics, lifecycle, and admin APIs still return normal exceptions.

- `get(...)` and `batch_get(...)` return empty byte payloads on soft failures
- `get_into(...)`, `batch_get_into(...)`, and `batch_get_into_multi_buffers(...)` return copied lengths and use `-1` for soft misses
- `put(...)`, `put_from(...)`, `batch_put_from(...)`, and related buffer write APIs return `-1` or per-item negative statuses on true write failures
- `batch_put_from(...)` reports per-key best-effort statuses; route-CAS conflicts are success, preserving insert-if-absent cache semantics under concurrent writers, and use one exact bounded recheck only when the CAS response omits the already-published active route
- `batch_is_exist(...)` returns `1` for hit, `0` for miss, and negative status codes for soft backend failures
- soft-fail downgrade covers transient native exceptions, including metadata and transport errors, at the Python compatibility boundary

### Lifecycle and Capacity

- `activate`, `enter_standby`, `enter_draining`
- `expand_local_memory`
- `drain_segment`, `retire_segment`
- `evacuate_owned_replicas`
- `plan_handoff`

### Observability

- `init_tracing()`
- `metrics_text()`
- `start_metrics_server()`
- `stop_metrics_server()`
- `metrics_server_address()`

Python real clients also auto-initialize Rust tracing before `setup(...)` when `MC_STORE_RS_TRACE=1` or `MC_STORE_RS_TRACE_FILE` is set; use `MC_STORE_RS_TRACE_FILTER` to pass a `tracing_subscriber` filter.

When `MC_STORE_RS_TRACE_FILE=/path/to/real-client.log` is set, Rust tracing appends to that file instead of writing to the process stdout/stderr stream. This is the recommended way to keep SGLang real-client logs separate from SGLang server logs.

For short profiling windows, set `MC_STORE_RS_TRACE_JSONL_FILE=/path/to/store-rs.trace.jsonl`
or use the metrics server `/tracing?enabled=on&file=...` control endpoint. The
JSONL sink records one structured span per Store-RS API or phase and can be
used alongside Jaeger OTLP export. Set `MC_STORE_RS_TRACE_ITEM_METADATA=1` to
also write `store.api_items.v1` records for `batch_put_from` and
`batch_get_into`, including per-item namespace, runtime id, hashed key,
readable key prefix, parsed SGLang TP rank and `k`/`v` suffix, size/read
length, and status. `MC_STORE_RS_TRACE_KEY_MODE=hash` is the default; use
`full` only for local debug captures.

The same metrics HTTP server also exposes `/breakdown` for SGLang/HiCache
diagnosis. Use `mooncake-store-rs-client stats --breakdown --server <host:port>`
for a readable summary, or add `--json` to keep the machine-readable API,
phase, metadata, transport, runtime, and segment snapshot. The
An external validation harness can save that endpoint after a real SGLang run.

## Replication Policy

`ReplicateConfig` currently supports:

- `replica_num`
- `preferred_segment`
- `preferred_segments`
- `preferred_storage_owner`
- `preferred_storage_owners`
- `prefer_local`
- `with_soft_pin`
- `prefer_alloc_in_same_node`

These values map to the Rust `ReplicationPolicy` used by `StoreClient`.
