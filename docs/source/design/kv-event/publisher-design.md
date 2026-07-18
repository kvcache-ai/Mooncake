# Publish Mooncake KV Events

[中文](../../zh/design/kv-event/publisher-design.md)

Mooncake Master can publish live key-value (KV) cache changes for Conductor and
other subscribers. This page shows how to enable the publisher, check whether
it is sending, and read the exact MessagePack fields it produces. It also
explains the object-key and delivery limits that affect Conductor.

## Enable and check the publisher

From the repository root, build `mooncake_master` with KV Events enabled:

```bash
cmake -S . -B build -DENABLE_KV_EVENTS=ON
cmake --build build --target mooncake_master -j
```

Start the Master with a usable backend name, model fallback, block size, and
ZeroMQ (ZMQ) bind address. This command uses the admin HTTP port `9003`:

```bash
./build/mooncake-store/src/mooncake_master \
  --enable_kv_events=true \
  --kv_events_bind_endpoint=tcp://0.0.0.0:5557 \
  --kv_events_backend_id=pool-a \
  --kv_events_model_name=Qwen/Qwen2.5-7B-Instruct \
  --kv_events_block_size=16 \
  --kv_events_emit_object_key=true \
  --metrics_port=9003
```

A successful start logs a line containing:

```text
kv_events publisher enabled on tcp://0.0.0.0:5557 backend_id=pool-a
```

Then read the publisher status:

```bash
curl -s http://127.0.0.1:9003/kv_events/status
```

Before cache traffic begins, a healthy publisher returns the following shape.
The counters can already be nonzero if operations have run:

```json
{"enabled":true,"published_batches":0,"published_events":0,"dropped_events":0,"skipped_unparsed_keys":0,"invalid_event_hashes":0}
```

Check that `enabled` is `true`. After a cache operation, check that
`published_events` and normally `published_batches` increase. A subscriber is
not required for those counters to increase, so this check proves that the
Master sent messages to its ZMQ publisher (PUB) socket; it does not prove that
Conductor received them.

## Understand the build requirement

`-DENABLE_KV_EVENTS=ON` requires the libzmq headers and library. CMake stops
with an error if they cannot be found. A build made without this option keeps
the public Store calls available but uses a publisher stub; runtime flags
cannot turn that stub into a working publisher, and status reports
`"enabled":false`.

The publisher also changes `enabled` to `false` if
`kv_events_bind_endpoint` or `kv_events_backend_id` is empty, or if creating or
binding the ZMQ socket fails. Check the Master log when status is false.

## Choose Master settings

The settings can be supplied in the Master config file or as command-line
flags. Command-line values that are explicitly set override values loaded with
`--config_path`.

| Setting | Default | When it matters |
|---|---:|---|
| `enable_kv_events` | `false` | Must be `true` to create the publisher. |
| `kv_events_bind_endpoint` | empty | ZMQ PUB address bound by the Master, for example `tcp://0.0.0.0:5557`. Register a routable address such as `tcp://master-host:5557` in Conductor, not `0.0.0.0`. |
| `kv_events_backend_id` | empty | Required non-empty name for the Mooncake backend that reported the objects. It limits Remove and Clear cleanup in Conductor. |
| `kv_events_model_name` | empty | Fallback model for unrecognized keys and `cleared`. A model parsed from a connector key takes precedence. Conductor needs a non-empty model for `stored`. |
| `kv_events_block_size` | `0` | Fixed token count written into events. `0` is sent as `nil`; Conductor rejects a `stored` event whose block size is absent or non-positive. |
| `kv_events_lora_name` | empty | Fixed Low-Rank Adaptation (LoRA) name. Empty means the base model and is sent as `nil`. |
| `kv_events_additional_salt` | empty | Fixed string, sent as `nil` when empty. Conductor reads it but does not use it for cache sharing or lookup. |
| `kv_events_dp_rank` | `0` | Unsigned data-parallel (DP) value written into each event and the batch. Conductor keeps Mooncake DP values only for troubleshooting; they are not an inference-engine rank. |
| `kv_events_tenant_id` | `default` | Retained for config compatibility. The event tenant comes from each Store operation; an empty operation tenant becomes `default`. |
| `kv_events_emit_object_key` | `true` | Adds `object_key` to object events. Keep this `true` for Conductor, which needs the key to remove an object exactly. |
| `kv_events_emit_legacy_compat` | `true` | Adds the matching legacy `type`, `block_hashes`, and stored `parent_block_hash` fields. Conductor accepts them when they agree with the primary fields. |
| `kv_events_queue_capacity` | `65536` | Maximum pending events in the publisher queue. A positive value bounds the queue; `0` removes that bound. See [Plan for lost events](#plan-for-lost-events). |

The `kv_events_queue_capacity` command-line help currently says the setting is
ignored, but the publisher implementation does enforce it. The table above
describes the current runtime behavior.

## Read publisher status

`GET /kv_events/status` is served on the Master's admin port, configured by
`metrics_port` (default `9003`). Its fields are:

| Field | Meaning |
|---|---|
| `enabled` | The current Master has a compiled, configured, and successfully bound publisher. |
| `published_batches` | Multipart messages for which all three ZMQ sends returned success. |
| `published_events` | Events contained in those successfully sent batches. |
| `dropped_events` | Events removed because the pending queue was full, plus events in a batch whose ZMQ send failed. |
| `skipped_unparsed_keys` | Events with no sequence hash that were skipped because no object key could be sent, plus emitted events whose key had no recognized block-hash segment. The emitted form has `object_key` but no connector hash, so Conductor rejects its `stored` event. |
| `invalid_event_hashes` | Recognized connector keys whose block-hash text could not be converted to an unsigned 64-bit value. The event can still be sent with its object key, but without `connector_block_hash`. |

Zero counters do not prove that a subscriber is connected. Likewise, the PUB
socket can lose a message after a successful local send, so these counters are
not proof that Conductor received the events.

## Understand the three ZMQ frames

Every publication is one multipart ZMQ message with exactly three frames:

| Frame | Exact content |
|---:|---|
| 1 | Empty topic frame. |
| 2 | One unsigned 64-bit transport sequence number in big-endian byte order. |
| 3 | One MessagePack payload: `[timestamp_ms, [event_maps], dp_rank]`. |

The publisher groups at most 64 pending events into one payload. All events in
that payload use the same signed 64-bit Unix timestamp in milliseconds.
`dp_rank` is the configured unsigned 32-bit value. The publisher assigns one
sequence value to each batch it tries to send and reserves extra values when
queue entries are dropped. It starts at `1` for each publisher process, so
queue loss deliberately leaves a gap.

The payload items are:

| Item | MessagePack form | Meaning |
|---:|---|---|
| `timestamp_ms` | non-negative integer | Batch creation time in Unix milliseconds. |
| `event_maps` | array of maps | Events in the order the publisher dequeued them. |
| `dp_rank` | unsigned integer | Copy of `kv_events_dp_rank`; kept only for Mooncake troubleshooting in Conductor. |

## Read common event fields

Every `stored`, `removed`, and `cleared` map contains these primary fields:

| Field | MessagePack form | Source and use |
|---|---|---|
| `event_id` | unsigned 64-bit integer | Increasing event number within one publisher process. It restarts with the process. |
| `timestamp` | signed 64-bit integer | Same millisecond value as the outer `timestamp_ms`. |
| `event_type` | string | Exactly `stored`, `removed`, or `cleared`. |
| `model_name` | string or `nil` | Connector-key model when parsed; otherwise `kv_events_model_name`. |
| `block_size` | unsigned integer or `nil` | `kv_events_block_size`; `0` becomes `nil`. |
| `additional_salt` | string or `nil` | `kv_events_additional_salt`; empty becomes `nil`. |
| `lora_name` | string or `nil` | `kv_events_lora_name`; empty becomes `nil`. |
| `tenant_id` | string | Tenant on the Store operation; empty becomes `default`. |
| `backend_id` | string | `kv_events_backend_id`. |
| `medium` | string or `nil` | Object location for `stored` and `removed`; `nil` for `cleared`. |
| `dp_rank` | unsigned integer | `kv_events_dp_rank`. |

When `kv_events_emit_legacy_compat=true`, every map also has `type`:
`BlockStored`, `BlockRemoved`, or `AllBlocksCleared`, matching `event_type`.

`stored` and `removed` maps also contain:

| Field | MessagePack form | Source and use |
|---|---|---|
| `group_id` | string or `nil` | Parsed connector `group:N`, if present; otherwise the Store object's group string. Empty becomes `nil`. |
| `seq_hashes` | array of zero or one unsigned integer | Low 64 bits parsed from the object key. An unparsable key produces an empty array. |
| `base_block_idx` | `nil` | Connector keys do not record the block's depth in a token chain. |
| `object_key` | string, conditionally present | Complete Mooncake key, emitted only when `kv_events_emit_object_key=true`. |
| `block_hashes` | array, conditionally present | Legacy copy of `seq_hashes`, emitted only when compatibility fields are enabled. |

A `stored` map additionally contains `parent_hash` and `token_ids`, both
`nil`, because current connector keys do not contain either value. With legacy
fields enabled it also contains `parent_block_hash=nil`. A `removed` map does
not contain these stored-only fields. A `cleared` map contains no group, hash,
object, or topology fields.

When a connector key is recognized, the publisher also adds the fields that
the key actually provides:

| Field | When present |
|---|---|
| `connector_block_hash` | The complete block-hash text was valid hexadecimal and produced `seq_hashes[0]`. |
| `cache_prefix` | Text appeared before the connector model name. |
| `tp_rank` | A vLLM tensor-parallel rank was parsed. |
| `head_or_tp_rank` | A vLLM Ascend head or tensor-parallel rank was parsed. |
| `pcp_rank` | A prefill context-parallel rank was parsed. |
| `dcp_rank` | A decode context-parallel rank was parsed. It is not `dp_rank`. |
| `pp_rank` | A pipeline-parallel rank was parsed. |
| `layer_id` | An Ascend layerwise key contained a layer number. |

## Interpret `stored`, `removed`, and `cleared`

The three event names describe logical availability, not the number or address
of physical replicas:

| Event | What the Mooncake publisher means | Fields current Conductor needs |
|---|---|---|
| `stored` | This object is available from the named `cpu` or `disk` medium. A repeated successful Put or Upsert commit may refresh the same availability. | `backend_id`, non-empty `tenant_id` and `model_name`, positive `block_size`, supported medium and group, `object_key`, and a usable full `connector_block_hash`. `lora_name` may be empty. `seq_hashes` may be empty; if it has a value, it must match the full hash. |
| `removed` | This object is no longer available from the named medium. The other medium can remain available. | `backend_id`, supported medium and group, and `object_key`. The full hash may be absent because Conductor follows the object record saved by the earlier `stored` event. |
| `cleared` | All objects for this publisher's `backend_id` and the event's `tenant_id` have been cleared. It applies to both CPU and Disk and uses `medium=nil`. | Non-empty `backend_id` and `tenant_id`, with no object fields. |

The publisher can still send a `stored` event for a key it cannot fully parse
when object-key emission is enabled. Such an event has no usable
`connector_block_hash`; current Conductor logs a rejection and does not add it
to the shared-cache index.

## Use connector keys Conductor can match

Mooncake does not add a KV Event parameter to Store client calls. It reads
metadata from keys made by the vLLM connectors. The accepted vLLM forms are:

```text
[cache_prefix@]model_name@tp_rank:N@pcpN@dcpN@pp_rank:N@group:N@block_hash_hex
[cache_prefix@]model_name@tp_rank:N@pcpN@dcpN@pp_rank:N@block_hash_hex
```

The second form covers connector versions from before `group:N`. The accepted
vLLM Ascend forms are:

```text
model_name@pcpN@dcpN@head_or_tp_rank:N@pp_rank:N@block_hash_hex
model_name@pcpN@dcpN@head_or_tp_rank:N@block_hash_hex@layer_id
```

The separator is an unescaped `@`. In the first two forms, the segment just
before `tp_rank` is the model; earlier segments become `cache_prefix`.
Malformed or unknown keys are not rejected by Mooncake Store. They can still
be emitted as `object_key`, but they do not supply the full connector hash
that Conductor needs for `stored`.

For Conductor, `block_hash_hex` must be valid hexadecimal representing at
least eight bytes: after an optional `0x` prefix it must have at least 16
characters and an even length. The publisher preserves the full text in
`connector_block_hash` and converts the rightmost 16 hex characters to
`seq_hashes[0]`. Conductor reads those final eight bytes in big-endian order as
the 64-bit lookup value used by `/query`.

The full hash still matters. Conductor saves the full hash and object key so
that two different objects with the same final eight bytes remain separately
removable. A query uses only the 64-bit value and therefore reports a possible
cache hit when either object is present. See the [subscriber hash
rules](./subscriber-guide.md#convert-incoming-hashes) for accepted encodings
and mismatch handling.

`cache_prefix`, rank fields, and `layer_id` describe the connector namespace.
The publisher cannot infer how many layers or parallel parts make a complete
block, and current Conductor does not perform that completeness check.

## Understand CPU and Disk changes

The Master reduces replica metadata to two availability values: is the object
available in CPU, and is it available on Disk. It emits:

| Store operation | Event result |
|---|---|
| Successful Put or Upsert commit | One `stored` for every currently available medium. |
| Upsert while the old value becomes unreadable | `removed` for the old availability, followed by commit-time `stored` events. |
| Copy, move, offload, promotion, eviction, or replica cleanup | `stored` when a medium first becomes available; `removed` when its last usable replica disappears. Changing replica count or location while the medium remains available produces no availability change. |
| Remove, BatchRemove, or regex removal | One `removed` for each medium where the object was available. |
| RemoveAll | Per-object `removed` events, followed by `cleared` only when the tenant has no remaining objects. |
| Failed uncommitted new Put | No event. |

If one object is available in both CPU and Disk, the publisher sends a
separate event for each medium.

## Plan for lost events

Publishing is asynchronous. A positive `kv_events_queue_capacity` bounds the
pending queue. When that queue is full, the publisher removes the oldest
pending event, increments `dropped_events`, and reserves a transport sequence
number so subscribers can see a gap. A ZMQ send failure also increments
`dropped_events` for the affected events.

The publisher is PUB-only: it has no replay endpoint and cannot resend a
missed event. It also does not send a list of objects that were already cached
when Conductor connects. Its compact object-to-medium tracking is kept only in
the publisher process and is not persisted. Later Store changes can generate
new events for an existing object, but they do not reconstruct a complete
startup view.

Conductor warns when the transport sequence jumps and keeps its current cache
records; the warning alone does not repair the missing change. Keep
cache-producing traffic paused until the required Conductor registrations are
visible. The complete startup checklist is in the [subscriber
guide](./subscriber-guide.md#check-mooncake-to-conductor-setup).

## Maintainer source note

The message builder and queue are in
`mooncake-store/src/kv_event/kv_event_publisher.cpp`; connector-key parsing is
in `mooncake-store/include/kv_event/key_util.h`. Master flags and config loading
are in `mooncake-store/src/master.cpp`, and `GET /kv_events/status` is in
`mooncake-store/src/master_admin_service.cpp`. The matching fixtures are in
`mooncake-store/tests/kv_event_publisher_test.cpp`.
