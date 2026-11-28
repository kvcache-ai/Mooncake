# SSD Owner-Push Restore (TCP)

SSD Owner-Push is an opt-in restore path for registered host CPU destination
buffers over classic TCP (v2 acknowledged writes). The requester sends one
HMAC-SHA256-authenticated control RPC describing the requested object ranges
and destination registration views. The Owner pipelines SSD reads through
bounded staging slots and pushes each completed chunk directly into the
requester's registered memory pool. The Owner's signed terminal reply is sent
only after every accepted TCP v2 write has been acknowledged by the receiver.

Existing Store Put/Get and Master metadata protocols remain unchanged, and
TENT / RDMA paths are not modified by this feature.

## Enable and Configure Peers

Set the opt-in variable and staging parameters before creating clients:

```bash
export MOONCAKE_OFFLOAD_SSD_PUSH=true
export MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES=1048576
export MOONCAKE_OFFLOAD_STREAM_SLOTS=4
# Requires at least MOONCAKE_OFFLOAD_STREAM_SLOTS slots plus alignment overhead.
export MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES=134217728
```

Register each host destination pool once via `register_buffer`. There are two
ways to establish peer trust:

1. **Explicit bootstrap**: Exchange the canonical version-1 JSON documents
   returned by `get_ssd_push_peer_info()` over a private setup channel and pass
   them to `configure_ssd_push_peers(peers)` on participating nodes (supports up
   to 256 peers and dynamic updates). Non-secret observability counters are
   exposed separately via `get_ssd_push_stats()`.
2. **Private peer directory (single-node / local POSIX filesystem)**: Set
   `MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY` to an existing local directory owned by
   the effective UID with mode `0700` (no group or world access) on a filesystem
   that enforces local `flock()`. Each live runtime publishes its record
   alongside an exclusively `flock`-locked sidecar file so leftover files from a
   crashed process do not poison subsequent startups and excess dead leftovers
   are garbage-collected automatically. Multi-node clusters should use
   `configure_ssd_push_peers(peers)` over a private control plane.

Peer documents carry per-process 128-bit shared credentials used as HMAC-SHA256
keys and must remain private; raw credentials are never sent on the wire, are
never included in `get_ssd_push_stats()`, and are never stored in public
TransferEngine metadata.

## Security and Network Model

- **Private bind**: Readable SSD Owners bind their control RPC listener to the
  configured private `control_host` address (never wildcard `0.0.0.0` / `::`)
  with a finite idle connection timeout. Pure requesters without local SSD
  storage do not start a control listener.
- **Per-frame bidirectional HMAC-SHA256**: Every `kReadInto` and
  `kRecoverReadInto` request and reply is authenticated with HMAC-SHA256 using
  constant-time verification (including signed `kLimit` backpressure replies to
  cached authenticated peers; unauthenticated frames receive unsigned error
  replies and never increment `read_requests`). Tampered requests, forged
  replies, or unknown requesters fail closed without fabricating drain
  acknowledgments.
- **Off-I/O-loop recovery**: Both reads and recovery RPCs execute on separate
  bounded worker pools so saturated SSD reads never starve recovery queries.

## Fallback, Bounded Recovery, and Teardown Lifecycle

- **Safe pre-write fallback**: If a peer is unconfigured, a destination buffer
  is not a registered CPU fixed pool, or the Owner rejects the request before
  accepting any write, the read falls back to local streaming or the existing
  restore path.
- **Bounded recovery**: If a control reply is lost or times out after
  submission, `SsdDirectRuntime::Read` performs bounded `kRecoverReadInto`
  probes without resubmitting writes. If the Owner is unreachable or
  quarantined, the read returns `TRANSFER_FAIL` while keeping the affected
  destination range pinned in `DestinationRegistry`.
- **Buffer reuse protection**: While an uncertain write remains un-drained,
  `HasPending()` rejects overlapping `get_into`, `batch_get_into`,
  `batch_get_into_multi_buffers`, `get_into_ranges`,
  `batch_get_into_multi_buffer_ranges`, `put_from`, `put_from_with_metadata`,
  `batch_put_from`, `batch_put_from_multi_buffers`,
  `batch_put_from_multi_buffer_ranges`, `upsert_from`, `batch_upsert_from`, and
  `batch_upsert_from_multi_buffers` calls, and `unregister_buffer()` / `close()`
  return `OBJECT_HAS_LEASE`.
- **Retryable close**: When `close()` returns `OBJECT_HAS_LEASE`, the native
  client remains fully open (without closing `SsdFixedL2Requester` or
  `RpcDrainGuard`) so the caller can keep destination mappings alive, invoke
  `recover_ssd_push()` (and `unregister_buffer()` if needed), and retry
  `close()`. Final object destruction (`~RealClient()`) runs a non-retryable
  teardown that quarantines any un-drained destination registrations while still
  stopping all threads, servers, storage, and mounted segments cleanly.

## Environment Variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `MOONCAKE_OFFLOAD_SSD_PUSH` | `false` | Enable opt-in TCP SSD Owner-Push |
| `MOONCAKE_OFFLOAD_SSD_PEER_DIRECTORY` | Unset | Private `0700` peer directory for credential bootstrap |
| `MOONCAKE_OFFLOAD_LOCAL_STREAMING_READ` | `false` | Enable same-node bounded streaming restore |
| `MOONCAKE_OFFLOAD_STREAM_USE_URING` | `false` | Use persistent `io_uring` read workers when built with liburing |
| `MOONCAKE_OFFLOAD_STREAM_SLOT_BYTES` | `1048576` (1 MiB) | Staging bytes per pipeline slot |
| `MOONCAKE_OFFLOAD_STREAM_SLOTS` | `4` | Maximum concurrent slots per pipeline |
| `MOONCAKE_OFFLOAD_STREAM_READ_THREADS` | `4` | Streaming read worker count |
| `MOONCAKE_OFFLOAD_STREAM_WRITE_THREADS` | `2` | Streaming write worker count |
| `MOONCAKE_OFFLOAD_STREAM_MAX_OUTSTANDING` | `64` (`512` with push) | Maximum queued streaming operations |
| `MOONCAKE_OFFLOAD_READ_IO_THREADS` | `8` | Dedicated legacy restore RPC executor threads |
| `MOONCAKE_OFFLOAD_READ_CONCURRENCY` | `1` | Concurrent Owner groups per batch (capped at `8`) |
