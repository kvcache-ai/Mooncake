---
orphan: true
---

# SSD Owner push restore

SSD Owner push is an opt-in restore path for registered CPU destination buffers
on classic TCP or RDMA. The requester sends one control RPC with the object
ranges and destination registration views. The Owner pipelines SSD reads through
bounded staging slots and writes each completed chunk into the requester pool.
The terminal reply follows completion of all accepted writes.

The existing Store Put/Get and Master metadata protocols remain unchanged.
This initial feature is validated with a single non-HA Master. HA failover,
object generation fencing and persistent provider rebind are follow-up work.

## Enable and configure peers

Set these variables before creating both clients:

```bash
export MOONCAKE_OFFLOAD_SSD_PUSH=true
export MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES=1048576
export MOONCAKE_OFFLOAD_STREAM_SLOTS=4
# Requires at least four slots plus alignment overhead.
export MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=134217728
```

Register each destination pool once through `register_buffer`. Exchange the
JSON from `get_ssd_push_peer_info()` through a private setup channel, then call
`configure_ssd_push_peers([peer_json])` on both sides before reading. These
JSON documents contain setup credentials and must stay private. Preserve the
registered mapping until `unregister_buffer` succeeds.

Without an eligible registered destination or configured Owner, the existing
SSD restore path remains available. A preflight rejection can fall back before
any destination write; an uncertain submitted write must recover and physically
drain before the caller can reuse the buffer. A read timeout is a logical budget,
so draining accepted I/O may outlast it. If close reports pending work, retain
the Store and its destination pool and retry `recover_ssd_push` before close.

## Lightweight optimizations

Control sockets use `TCP_NODELAY` and a requester-owned cache of at most 128
idle connections. Idle streams expire after 30 seconds, and only fully decoded
responses return their connection to the cache. A broken or partially consumed
stream is discarded; a submitted request is never replayed by the cache.

The Owner combines fragments for one target into one scatter submission, keeping
the authorization check for every destination. Whole-value requests that fit
the configured slot budget use one bounded bulk read and scatter push; larger
requests keep the streaming pipeline. Exactly adjacent source and destination
fragments share a wire WRITE only when the transport registration covers the
combined range. Partial reads and explicit direct/fixed-buffer policies retain
the pipeline. Control sockets first attempt nonblocking I/O and poll only when
it would block, with the same absolute deadline. With liburing, ordinary READ
workers retain their rings across requests. File and slot references remain
owned through real completions; shutdown drains accepted work before release.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MOONCAKE_OFFLOAD_SSD_PUSH` | `false` | Enable remote Owner push |
| `MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES` | 1 MiB | Staging bytes per slot |
| `MOONCAKE_OFFLOAD_STREAM_SLOTS` | 4 | Maximum slots per pipeline |
| `MOONCAKE_OFFLOAD_STREAM_USE_URING` | Enabled for push when built with liburing | Persistent ordinary READ workers |
| `MOONCAKE_OFFLOAD_STREAM_READ_THREADS` | 4 | Read workers |
| `MOONCAKE_OFFLOAD_STREAM_WRITE_THREADS` | 2 | Write workers |
| `MOONCAKE_OFFLOAD_READ_IO_THREADS` | 64 | Dedicated legacy restore RPC executor |
| `MOONCAKE_OFFLOAD_READ_CONCURRENCY` | 1 | Concurrent Owner groups per batch, capped at 8 |

The portable `pread` driver remains available when streaming io_uring is disabled.
Some small single-value and batch shapes still have more overhead than the
existing restore path. Measure the application's object sizes and batch layout
before enabling push; do not infer cross-host RDMA throughput from loopback TCP results.
