# KV Event Subscriber Guide

How to consume the Mooncake Store KV event stream. For why the publisher is
built the way it is, see [publisher-design](publisher-design.md).

## Transport

Events arrive on a ZMQ `PUB` socket as a three-frame multipart message:

| Frame | Contents |
|---|---|
| 0 | Topic. Always empty, but always present. |
| 1 | Unsigned 64-bit big-endian sequence number, 8 bytes. |
| 2 | MessagePack payload. |

The payload is a 3-element array: `[timestamp_ms, [event_map, ...], dp_rank]`.
One message carries up to 64 events, so a subscriber must iterate the middle
element rather than assuming one event per message.

## Event envelope

Every event map contains these fields:

| Field | Type |
|---|---|
| `event_id` | `u64`, monotonic per publisher process |
| `timestamp` | `i64` milliseconds |
| `event_type` | `stored`, `removed`, or `cleared` |
| `model_name` | string or nil |
| `block_size` | `u32`, nil when configured as 0 |
| `additional_salt` | string or nil |
| `lora_name` | string or nil |
| `tenant_id` | string |
| `backend_id` | string |
| `medium` | string or nil |
| `dp_rank` | `u32` |

`stored` and `removed` add `group_id`, `object_key` (unless
`kv_events_emit_object_key=false`), `seq_hashes`, and `base_block_idx`. `stored`
additionally carries `parent_hash` and `token_ids`.

## Keys are not interpreted

Store never parses, splits, or interprets an object key. The raw Store key is
forwarded verbatim as `object_key`, and every field that would require
interpreting it is empty or nil:

- `seq_hashes` and the legacy `block_hashes` are always empty arrays;
- `token_ids` and `parent_hash` are always nil on `stored`;
- `base_block_idx` is always nil.

A subscriber that needs block-level identity must derive it from `object_key`
itself using whatever convention the producer applied. Do not expect Mooncake to
supply block hashes, token ids, or block depth.

## Media

`medium` is normalized to exactly two logical tiers: `cpu` for memory replicas,
`disk` for every non-memory class (local disk, NVMe-oF, DFS). One event names one
medium.

## Stored

`stored` announces that the object is readable on the event's medium. Treat
repeated `stored` for the same object/backend/medium as idempotent, and do not
infer physical replica count from event count.

An Upsert of an existing object publishes `removed` for the old value and
`stored` for the replacement, in that order. There is no separate update event.

## Removed

`removed` retracts availability for the event's medium only. Other media for the
same object may remain valid, so drop the object entirely only once no medium
remains.

Treat repeated `removed` for the same object/backend/medium as idempotent. The
publisher does not deduplicate retractions, so a subscriber that reference-counts
media instead of storing a set can decrement past zero.

## Cleared

`cleared` is envelope-only. It omits `object_key`, `group_id`, `seq_hashes`,
`block_hashes`, and `base_block_idx` entirely rather than emitting them as nil,
and carries `medium=nil`. It means every object under the event's
`backend_id + tenant_id` is gone.

Mooncake emits `cleared` when a `RemoveAll` actually empties a tenant. It is not
emitted for a tenant that held no objects, and it is not emitted when any object
was skipped (for example a still-leased object without `force`).

## Legacy compatibility

With `kv_events_emit_legacy_compat=true` (the default) each event also carries a
`type` field alongside `event_type`:

| `event_type` | legacy `type` |
|---|---|
| `stored` | `BlockStored` |
| `removed` | `BlockRemoved` |
| `cleared` | `AllBlocksCleared` |

Per-object events additionally carry an empty `block_hashes` array, and `stored`
carries a nil `parent_block_hash`. Set the flag to `false` to emit only the
RFC #1527 field names.

## Ordering, loss, and recovery

Sequence numbers are strictly monotonic and gap-free while the publisher runs.
Use frame 1 for transport ordering and `event_id` for event ordering within the
stream.

A sequence gap means events were dropped. When the publisher's async queue is
full it drops the oldest events and **reserves the sequence numbers they would
have used**, so a gap is always visible rather than silent. On seeing a gap, a
subscriber must either invalidate the affected `backend_id + tenant_id` state or
reconcile it against the master, which is the authoritative source for key
placement.

There is no replay endpoint and no startup snapshot. A subscriber that joins
after objects were stored receives nothing about them. Publisher state is
per-process and not persisted, so a master restart resets the sequence counter to
1 without emitting a reset signal; a subscriber that filters on monotonic
sequence must be prepared for the counter to move backwards and should reconcile
against the master rather than discarding the new events.

## Observability

`GET /kv_events/status` on the master admin port reports:

| Field | Meaning |
|---|---|
| `enabled` | Whether a publisher is live |
| `published_batches` | ZMQ messages sent |
| `published_events` | Events inside those messages |
| `dropped_events` | Events dropped by a full queue; each leaves a sequence gap |
| `skipped_keyless_events` | Per-object events suppressed because no `object_key` was available |

A nonzero `skipped_keyless_events` with `kv_events_emit_object_key=false` is
expected: that flag suppresses all `stored` and `removed` events, leaving only
`cleared`.
