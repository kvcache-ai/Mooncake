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
| `additional_salt` | nil |
| `lora_name` | nil |
| `tenant_id` | string |
| `backend_id` | string |
| `medium` | string or nil |
| `dp_rank` | nil |

Per-object events also include `group_id`, optional `object_key`, and
`seq_hashes`. With legacy compatibility enabled, `type` and `block_hashes` are
also present.

## Stored

`stored` adds or refreshes block availability for one medium. Its additional
fields are `base_block_idx`, `parent_hash`, and `token_ids`. The original Put
may carry token IDs; later stored events caused by replica movement reuse only
the compact model/hash context and use `token_ids=nil`.

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

`cleared` has no block payload. Mooncake publishes it with `medium=nil`; it
means that all media under the event's `backend_id + tenant_id` are empty. With
legacy compatibility enabled its `type` is `AllBlocksCleared`.

## Ordering and recovery

Process transport frames in sequence order and use `event_id` for event order
inside the publisher stream. A transport sequence gap means at least one event
was dropped or missed. The Mooncake publisher has no replay endpoint, so a
subscriber must invalidate the affected backend state or reconcile it through
an external control-plane query.

Subscribers joining after objects were stored do not receive historical
events. Registration and initial-state recovery are responsibilities of the
consumer deployment.
