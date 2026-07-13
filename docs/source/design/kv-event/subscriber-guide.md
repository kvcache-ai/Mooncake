# KV Event Subscriber Guide

## Event envelope

All event maps contain:

| Field | Type |
|---|---|
| `event_id` | `u64` |
| `timestamp` | `i64` milliseconds |
| `event_type` | `stored`, `removed`, or `cleared` |
| `model_name` | string or nil |
| `block_size` | `u32` or nil |
| `additional_salt` | string or nil |
| `lora_name` | string or nil |
| `tenant_id` | string |
| `backend_id` | string |
| `medium` | string or nil |
| `dp_rank` | `u32` |

Per-object events also include `group_id`, optional `object_key`, and
`seq_hashes`. Recognized connector keys add `connector_block_hash`, optional
`cache_prefix`, and their encoded parallel namespace fields. With legacy
compatibility enabled, `type` and `block_hashes` are also present.

## Stored

`stored` adds or refreshes block availability for one medium. Its additional
fields are `base_block_idx`, `parent_hash`, and `token_ids`. The Mooncake master
derives model and hash metadata from connector object keys and supplies the
fixed `model_name` fallback, `block_size`, `additional_salt`, `lora_name`, and
`dp_rank` from publisher configuration. A zero block size and empty optional
strings remain nil. `parent_hash`, `token_ids`, and `base_block_idx` remain nil.
Indexers should use `seq_hashes` and consistent registration metadata for
prefix lookup.

`connector_block_hash` is the complete hexadecimal connector digest, while
`seq_hashes[0]` is its low 64 bits. Consumers matching full vLLM hashes must use
`connector_block_hash` (or parse `object_key`) and include `cache_prefix` in the
identity namespace. `base_block_idx` is nil because connector keys do not carry
block depth. For Ascend layerwise keys, `layer_id` does not by itself prove that
all layers are available. Likewise, one rank event does not prove that all
required TP/PCP/DCP/PP shards are present; use registered model topology when
building whole-block availability.

Treat repeated stored events for the same block/backend/medium as idempotent.
Do not infer physical replica count from them.

An Upsert of an existing Mooncake object publishes `removed` while the old
value is unreadable and `stored` after the replacement commits. Consumers
should not expect a separate update event type.

## Removed

`removed` removes availability only for the event's medium. Other media for the
same block may remain valid. Remove the whole block/backend entry only after no
media remain.

## Cleared

`cleared` has no block payload. Mooncake publishes it with `medium=nil` and the
fixed configured model context; it means that all media under the event's
`backend_id + tenant_id` are empty. With legacy compatibility enabled its
`type` is `AllBlocksCleared`.

## Ordering and recovery

Process transport frames in sequence order and use `event_id` for event order
inside the publisher stream. A transport sequence gap means at least one event
was dropped or missed. The Mooncake publisher has no replay endpoint, so a
subscriber must invalidate the affected backend state or reconcile it through
an external control-plane query.

Subscribers joining after objects were stored do not receive historical
events. Registration and initial-state recovery are responsibilities of the
consumer deployment.
