# KV Event Publisher Design

## Goals

The master publishes logical cache availability for external indexers while
keeping physical replica management internal. The implementation uses the
existing RFC #1527 map protocol and supports `stored`, `removed`, and
`cleared`. It does not include an indexer, replay service, or Conductor.

## Transport

The master binds a ZeroMQ PUB socket. Each multipart message is:

1. an empty topic frame;
2. an unsigned 64-bit big-endian transport sequence;
3. a msgpack payload `[timestamp_ms, [event_maps], 0]`.

Publishing is asynchronous. A bounded in-process queue drops the oldest event
when full and reserves a sequence gap so subscribers can detect loss. Relevant
master flags are:

- `enable_kv_events`
- `kv_events_bind_endpoint`
- `kv_events_backend_id`
- `kv_events_emit_object_key`
- `kv_events_emit_legacy_compat`
- `kv_events_queue_capacity`

The feature is compiled only when `ENABLE_KV_EVENTS=ON`; public client APIs
remain available and become no-ops for event metadata in builds without ZMQ.

## Object and medium state

Event identity is based on `backend_id`, `tenant_id`, and the block identity
(`seq_hashes` and/or `object_key`). The `medium` field is one string, normally
`cpu` or `disk`. If a block is present in both tiers, the publisher emits one
event for each tier.

Replica topology is normalized to medium availability:

- the first completed replica on a medium emits `stored`;
- removing the last completed replica on a medium emits `removed`;
- changing the number or location of replicas within an available medium emits
  no event;
- a successful Put or Upsert commit emits `stored` for every current medium.

The publisher keeps a compact per-process state entry for each published
object. It stores the current medium set, model name, block size, parsed block
hash, and parsed parent hash. Token IDs are not retained after the original
stored event. A temporary zero-medium state during Upsert retains this compact
context; state is released when the object is deleted or its tenant is cleared.

## StoreEventInfo

Python `batch_put_from_multi_buffers` accepts an optional aligned list of
`StoreEventInfo` values:

| Field | Wire field | Empty behavior |
|---|---|---|
| `model_name` | `model_name` | `nil` |
| `block_size` | `block_size` | `nil` when zero |
| `block_hash` | `seq_hashes[0]` | Parse `object_key` instead |
| `parent_block_hash` | `parent_hash` | `nil` |
| `token_ids` | `token_ids` | `nil` |

Hashes accept decimal or `0x`-prefixed unsigned 64-bit strings. A non-empty
hash that cannot be parsed does not fail the Put. The numeric wire field is
left empty, `object_key` remains available when enabled, and
`invalid_event_hashes` is incremented.

`store_event_infos` must be empty or have exactly the same length as `keys`.
Only successfully committed keys publish their corresponding metadata.

## Publication points

| Operation | Event behavior |
|---|---|
| Put/BatchPut commit | `stored` for all completed media |
| Upsert of an existing block | `removed` when the old value becomes unreadable, then `stored` for all completed media on commit |
| Copy/Move completion | Medium availability delta |
| Offload/promotion completion | `stored` when the target medium first appears |
| Replica clear or eviction | `removed` when the last replica on a medium disappears |
| Stale handle/client cleanup | Medium availability delta |
| Remove/BatchRemove/regex remove | `removed` for every available medium |
| RemoveAll | Per-object `removed`, then tenant `cleared` only when empty |
| Failed uncommitted new Put | No event |

`cleared` uses `medium=nil` and clears all media for the specified
`backend_id + tenant_id`.

## Limitations

The publisher is PUB-only. It does not replay missed events, publish a startup
snapshot, or persist its compact context cache. Subscribers must detect
transport sequence gaps and recover through their own reconciliation path.
Objects restored before the publisher starts have no cached StoreEventInfo;
later events still carry tenant, backend, object key, and any parseable hash.
