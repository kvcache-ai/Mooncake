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
3. a msgpack payload `[timestamp_ms, [event_maps], dp_rank]`.

Publishing is asynchronous. A bounded in-process queue drops the oldest event
when full and reserves a sequence gap so subscribers can detect loss. Relevant
master flags are:

- `enable_kv_events`
- `kv_events_bind_endpoint`
- `kv_events_backend_id`
- `kv_events_model_name`
- `kv_events_block_size`
- `kv_events_additional_salt`
- `kv_events_lora_name`
- `kv_events_dp_rank`
- `kv_events_emit_object_key`
- `kv_events_emit_legacy_compat`
- `kv_events_queue_capacity`

The feature is compiled only when `ENABLE_KV_EVENTS=ON`; public client APIs
remain available and become no-ops for event metadata in builds without ZMQ.

## Object and medium state

Event identity is `backend_id`, `tenant_id`, and `object_key`. The `medium`
field is one string, either `cpu` or `disk`. If an object is present in both
tiers, the publisher emits one event per tier.

Replica types are collapsed onto those two logical tiers: memory replicas map to
`cpu`, and every non-memory replica type — disk, local disk, NOF SSD, and DFS —
maps to `disk`. Subscribers see one entry per storage class and are not exposed
to Mooncake's internal replica taxonomy, which can grow without becoming a
protocol change.

Replica topology is normalized to medium availability:

- the first completed replica on a medium emits `stored`;
- removing the last completed replica on a medium emits `removed`;
- changing the number or location of replicas within an available medium emits
  no event;
- a successful Put or Upsert commit emits `stored` for every current medium.

The publisher holds no per-object state. Every delta is computed from the
arguments of a single call: the medium set after the mutation, plus the set the
caller captured before it. The master already snapshots that set before mutating
metadata, so keeping a second copy in the publisher would only duplicate it, and
a per-object map would shadow the master's whole key space.

This makes the caller responsible for the "before" set. A path that mutates
metadata and then publishes without a snapshot cannot produce a correct delta,
which is why the snapshot and the publish call sit in the same function
throughout the master.

Duplicate `removed` events are not suppressed by the publisher. Where the same
removal can be reached twice — an eviction that drops the last replica, followed
by the erase of the now-invalid object — the master picks one publisher, not
both: the eviction path returns early when the object is no longer valid and
leaves the announcement to the erase path. Subscribers additionally treat
`removed` as idempotent, so a duplicate is harmless rather than load-bearing.

## Event payload fields

The publisher is deliberately key-agnostic. Store never parses, splits, or
interprets an object key, so no key format is privileged and no connector needs
a Mooncake-specific key convention. The raw Store key is forwarded verbatim as
`object_key`, and every field that would require interpreting that key stays
empty:

- `seq_hashes` and the legacy `block_hashes` are emitted as empty arrays;
- `token_ids` and `parent_hash` are nil on `stored`; `base_block_idx` is nil on
  both `stored` and `removed`;
- `group_id` carries the Store group identity, not a connector group field.

A `cleared` event is envelope-only. It omits `object_key`, `group_id`,
`seq_hashes`, `block_hashes`, and `base_block_idx` entirely rather than
emitting them as nil.

The remaining envelope fields come from master configuration, because one
publisher serves one fixed model, block-size, LoRA, additional-salt, and
data-parallel context: `model_name`, `block_size`, `additional_salt`,
`lora_name`, and `dp_rank`. `block_size=0` is encoded as nil, as are empty salt
and LoRA names. The configured `dp_rank` appears both in each event envelope and
in the batch trailer. Per-object `tenant_id` comes from the Store operation
rather than the global tenant config.

Setting `kv_events_emit_object_key=false` suppresses `stored` and `removed`
entirely, since without the key those events carry no identity a subscriber can
act on. Suppressed events are counted as `skipped_keyless_events`. `cleared` is
unaffected: it is tenant-scoped and needs no object identity.

With `kv_events_emit_legacy_compat=true` (the default) each event also carries a
legacy `type` alias alongside `event_type`: `BlockStored`, `BlockRemoved`, and
`AllBlocksCleared`. Legacy mode also adds the `block_hashes` array and, on
`stored`, a nil `parent_block_hash`. This lets subscribers written against the
pre-RFC field names consume the same stream unchanged.

A subscriber that needs block-level or shard-level semantics must derive them
from the key itself, using the same connector convention that produced it, plus
its own registered topology. The publisher cannot help here:
it does not know which connector wrote a key, how many layers or shards a block
spans, or how deep a block sits in a prefix chain.

## Publication points

| Operation | Event behavior |
|---|---|
| Put/BatchPut commit | `stored` for all completed media |
| Upsert of an existing object | `removed` when the old value becomes unreadable, then `stored` for all completed media on commit |
| Copy/Move completion | Medium availability delta |
| Offload/promotion completion | `stored` when the target medium first appears |
| Replica clear or eviction | `removed` when the last replica on a medium disappears |
| Stale handle/client cleanup | Medium availability delta |
| Remove/BatchRemove/regex remove | `removed` for every available medium |
| RemoveAll | Per-object `removed`, then tenant `cleared` when every object was removed |
| Failed uncommitted new Put | No event |

`cleared` uses `medium=nil` and clears all media for the specified
`backend_id + tenant_id`. It is emitted only when the tenant actually held
objects and none were skipped, so a tenant that never existed produces no
`cleared`, and a `RemoveAll` that leaves a still-leased object behind produces
none either. Under HA with the oplog the metadata erase is deferred to the
durable callback, so the decision is based on what the removal loop accepted, not
on whether the metadata map looks empty. Every skip counts, including the less
obvious ones: an object whose replicas are not all completed, an object with a
pending replication task, a failed oplog slot reservation, and a failed oplog
append.

## Limitations

The publisher is PUB-only. It does not replay missed events, publish a startup
snapshot, or persist its compact context cache. Subscribers must detect
transport sequence gaps and recover through their own reconciliation path.
Because the compact context is per-process and not persisted, a master restart
resets it: the first event for a previously known object is a fresh `stored`
rather than a delta. Objects restored before the publisher starts are only
described by tenant, backend, fixed publisher context, and object key.
