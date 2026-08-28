# Connect KV Event Sources to Conductor

[中文](../../zh/design/kv-event/subscriber-guide.md)

Conductor reads three current KV event paths: native vLLM engine events,
native SGLang engine events, and Mooncake shared-pool map events. An SGLang
engine using Mooncake still uses the Mooncake map envelope, while its registered
SGLang handler parses the original object key. This page helps you choose the
correct registered source type, satisfy the fields Conductor checks, and
understand exactly what Store, Remove, Clear, and unregister change. It also
describes hash conversion and the limits around missed events.

## Pick the registered source type

Register each endpoint with exactly one case-sensitive `type`:

- use `"vLLM"` for an inference engine that reports its GPU cache;
- use `"SGLang"` for an inference engine that reports native SGLang events or
  SGLang keys through Mooncake;
- use `"Mooncake"` for a Mooncake Master that reports shared CPU or Disk
  objects.

The recorded type decides which MessagePack event format Conductor reads.
Topic text does not change that choice, even when both publishers use an empty
topic. The receiving endpoint also identifies the source for cleanup and logs.
See the [HTTP API reference](../conductor/indexer-api-design.md) for registration
fields and examples.

## Compare event sources

| Question | `vLLM` source | `SGLang` source | `Mooncake` source |
|---|---|---|---|
| Registered `type` | Exactly `vLLM`. | Exactly `SGLang`; it selects the native decoder or the Mooncake map decoder for an SGLang-backed Store. | Exactly `Mooncake`. |
| Event names | `BlockStored`, `BlockRemoved`, `AllBlocksCleared` in `type`. | The same names in tagged arrays, or `stored`, `removed`, `cleared` in a Mooncake map. | `stored`, `removed`, `cleared` in `event_type`; matching legacy `type` is optional. |
| Batch timestamp | Finite MessagePack float in seconds. Events have no required timestamp field. | Finite MessagePack float in seconds for native events; integer milliseconds in a Mooncake map. | Non-negative MessagePack integer in milliseconds. Every event has the same integer in `timestamp`. |
| Accepted cache location | `GPU`, case-insensitive. Other values are logged and ignored. | Native events treat `nil` as the engine-local GPU cache and also accept explicit `GPU`, `CPU_PINNED`, or `DISK`; Mooncake map events accept `CPU` or `Disk`. | `CPU` or `Disk`, case-insensitive. Other values are logged and ignored. |
| Tenant, model, LoRA, block size | Trusted registration supplies the cache-sharing fields. Event block size and Low-Rank Adaptation (LoRA) name are checked as assertions for Store. | Native events use trusted registration. vLLM requires an equal `block_size`; SGLang permits its final partial page in the range `1..registered block_size`. Mooncake map Store events supply the fields and must match registration and profile. | A Store event supplies `tenant_id`, `model_name`, `lora_name`, and `block_size`. They must match an existing vLLM or SGLang cache-sharing context and the registered hash profile. |
| Data-parallel rank | Trusted registration supplies the engine rank. A non-`nil` batch rank must match it or the complete batch is rejected. | Trusted registration supplies the engine rank. A non-`nil` batch rank must match it or the complete batch is rejected. | Batch and event `dp_rank` values are kept only for troubleshooting. They do not create a GPU record or a query instance. |
| Group | `group_idx` may be absent, `nil`, or `0`. | Native events have no group field. Mooncake map `group_id` is retained as an opaque string. | `group_id` may be `nil`, empty, or decimal string `"0"`. |
| Object fields | Uses `block_hashes`; it has no Mooncake object key. | Native events use `block_hashes`; Mooncake map Store and Remove need `object_key`, parsed by the registered handler. | Store and Remove need `object_key`; a legacy `connector_block_hash` is optional and checked when present; Clear carries neither. |
| `/query` `instances` | The registered engine appears under its `instance_id`, including its registered ranks. | The registered engine appears under its `instance_id`, including its registered ranks. | The Mooncake source is not listed as an instance. Its CPU or Disk information appears under every compatible vLLM or SGLang engine. |

The four fields that decide whether CPU or Disk information is compatible with
an engine are `tenant_id`, model, `lora_name`, and `block_size`. The
[architecture page](../conductor/conductor-architecture-design.md#what-can-share-cache)
explains how they affect query results.

Native SGLang uses the same three transport frames as vLLM but an array-like
MessagePack payload. Its `BlockStored`, `BlockRemoved`, and
`AllBlocksCleared` events are decoded by the registered `SGLang` source. Signed
wire hashes are preserved as unsigned 64-bit bit patterns. SGLang registrations
use `strategy: "sglang"` for the regular token chain or
`strategy: "sglang_bigram"` for the EAGLE/speculative bigram chain, with
`index_projection: "first64_be"`. SGLang does not use `PYTHONHASHSEED`; a
request's `cache_salt` supplies its hash-chain root.

## Match the prefix-hash profile

KV events carry neither `PYTHONHASHSEED` nor the complete first-parent digest,
so registration is a trusted deployment assertion for vLLM. Each vLLM and
Mooncake registration for a compatible vLLM context must supply the same exact
`python_hash_seed` string. SGLang registrations instead select an SGLang hash
strategy and use the request `cache_salt` during `/query`; the seed field is
retained only for the shared registration schema. Conductor canonical-CBOR
encodes the vLLM seed and calculates SHA-256 to derive the `root_digest` used
as the first parent; callers do not register the digest.

The seed must be the literal `random` or ASCII decimal text in
`0..4294967295`. Exact text matters: `"0"` and `"00"` derive different roots.
An unset `PYTHONHASHSEED` is unsupported because vLLM then selects random root
bytes that Conductor cannot reproduce. The explicit text `random` is supported
and is hashed as that exact string. vLLM `--seed` only controls model and
sampling randomness and is unrelated to this prefix-cache identity.

For the seed-zero profile, start every compatible vLLM process with:

```bash
PYTHONHASHSEED=0 vllm serve test-model \
  --enable-prefix-caching \
  --prefix-caching-hash-algo sha256_cbor
```

Register `python_hash_seed` as `"0"`. Conductor reports the resulting
`root_digest`
`4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e`
through `/services` and `/global_view` for comparison.

## Meet the vLLM event requirements

The vLLM payload is exactly
`[timestamp_seconds, [event_maps], data_parallel_rank]`. The last item may be a
non-negative integer or `nil`. Each event map follows these rules:

| Event | Recognized fields and checks | Index change |
|---|---|---|
| `BlockStored` | Requires `block_hashes`, `parent_block_hash`, `token_ids`, `block_size`, `lora_id`, `medium`, and `lora_name`. `lora_id` must be `nil`; `block_size` and `lora_name` must agree with registration; `medium` must be GPU. Optional `group_idx` must be `0` or `nil`. | Adds the supplied hashes to the registered endpoint, engine, and data-parallel (DP) rank's GPU information. |
| `BlockRemoved` | Requires `block_hashes` and `medium`; optional `group_idx` must be `0` or `nil`. It must not contain recognized stored-only fields. | Removes only that registered endpoint, engine, and rank's GPU records for the supplied hashes. |
| `AllBlocksCleared` | Carries only `type` among recognized vLLM fields. | Removes all GPU information from that registered endpoint, engine, and rank. It preserves other engines and all shared CPU/Disk information. |

`block_hashes` is an array whose items are unsigned integers or MessagePack
binary strings. Parent hashes and token IDs are read but do
not replace or rehash `block_hashes`.

`BlockStored` also recognizes optional `extra_keys`, `kv_cache_spec_kind`, and
`kv_cache_spec_sliding_window`. If present, `extra_keys` must be `nil` or have
one `nil`/array entry per block hash. Conductor accepts only
`kv_cache_spec_kind="full_attention"` and no non-`nil` sliding-window value.
Unknown map keys are ignored; duplicate or wrongly typed recognized keys make
only that event invalid.

## Meet the Mooncake event requirements

The Mooncake payload is exactly
`[timestamp_ms, [event_maps], data_parallel_rank]`. The last item may be a
non-negative integer or `nil`. Every event map requires these common fields:

| Field | Accepted form |
|---|---|
| `event_id` | Unsigned integer. It is recorded in logs, not used to sort or skip the event. |
| `timestamp` | Signed integer equal to the non-negative outer `timestamp_ms`. |
| `event_type` | `stored`, `removed`, or `cleared`. |
| `model_name` | String or `nil`. Store requires a non-empty resulting model. |
| `block_size` | Signed integer or `nil`. Store requires a positive value. |
| `additional_salt` | String or `nil`. Decoded but not used in the cache-sharing fields or hash lookup. |
| `lora_name` | String or `nil`; `nil` means the base model. |
| `tenant_id` | String. Store and Clear need a non-empty tenant for a useful mutation. |
| `backend_id` | String; Store, Remove, and Clear require it to be non-empty. |
| `medium` | String or `nil`; Store and Remove must name CPU or Disk. |
| `dp_rank` | Non-negative signed integer kept only for troubleshooting. |

An optional legacy `type` must agree with `event_type`. The event-specific
rules are:

| Event | Fields required by the decoder | Additional fields needed to change the index |
|---|---|---|
| `stored` | `group_id`, `seq_hashes`, `base_block_idx`, `parent_hash`, and `token_ids`. The last three may be `nil`; `seq_hashes` may be `[]`. | A supported group and medium, complete context, and `object_key`. Legacy Mooncake consumers may use `connector_block_hash`; SGLang registrations parse the object key directly. |
| `removed` | `group_id`, `seq_hashes`, and `base_block_idx`; `seq_hashes` may be `[]`. Stored-only parent and token fields are forbidden. | A supported group and medium plus `object_key`. Conductor follows the exact object record saved by Store; optional full or sequence hash assertions must match it. |
| `cleared` | Only the common fields. All recognized object, hash, group, parent, token, and topology fields are forbidden. | Non-empty `backend_id` and `tenant_id`. The event clears matching CPU and Disk object records from this source endpoint. |

Object events may also contain `cache_prefix`, `tp_rank`,
`head_or_tp_rank`, `pcp_rank`, `dcp_rank`, `pp_rank`, and `layer_id` with the
types documented in the [publisher guide](./publisher-design.md#read-common-event-fields).
If legacy `block_hashes` is present, it must equal `seq_hashes`; if stored
`parent_block_hash` is present, it must equal `parent_hash`.

## Convert incoming hashes

Conductor turns both source formats into the same unsigned 64-bit lookup
value. It does not hash an event's token or parent fields again.

| Incoming hash | Conversion |
|---|---|
| vLLM unsigned integer | Already the lookup value; use it directly. |
| vLLM MessagePack binary string | Require at least eight bytes, then read its final eight bytes in big-endian order. |
| Mooncake `connector_block_hash` | Remove an optional `0x`, accept upper- or lower-case hex, require an even length and at least eight bytes, normalize to lower case, then read the final eight decoded bytes in big-endian order. SGLang object keys use the first 16 hex characters instead. |

For a Mooncake Store, `seq_hashes` may be empty or contain exactly one value
equal to the lookup value from `connector_block_hash`. A different value or
more than one value rejects that event without changing its saved object
record or the cache index. For Remove, a supplied full hash or sequence value
must match the earlier Store record.

When a legacy `connector_block_hash` is supplied, its complete value and the
object key remain saved even though `/query` uses only the projected bytes.
Current publishers leave connector-derived hashes empty, so the registered
vLLM or SGLang handler projects the value from `object_key`. If two objects have
different full hashes but the same projected value, both can provide the same
possible query hit; removing either object leaves the other object's record
intact. Query-side
hash generation is documented in [How token blocks become lookup
values](../conductor/conductor-architecture-design.md#how-token-blocks-become-lookup-values).

## Understand what each event changes

| Event | Exact scope of the change |
|---|---|
| vLLM `BlockStored` | Adds GPU hashes for the registered source endpoint, `instance_id`, and DP rank in its registered tenant/model/LoRA/block-size context. |
| vLLM `BlockRemoved` | Removes only that engine and rank's listed GPU hashes. Repeating a remove is a no-op. |
| vLLM `AllBlocksCleared` | Removes every GPU hash from that engine and rank. It does not touch another rank, engine, or Mooncake object. |
| Mooncake `stored` | Saves one record identified by source endpoint, `backend_id`, `tenant_id`, `object_key`, and CPU-or-Disk medium. An identical repeat is harmless; a conflicting active record for that same object and medium is rejected. |
| Mooncake `removed` | Follows that exact saved object-and-medium record. An unknown object is a no-op, and removal never searches by the 64-bit value alone. |
| Mooncake `cleared` | Removes saved CPU and Disk records only for the reporting endpoint, `backend_id`, and `tenant_id`. It preserves GPU information and other endpoints, backends, and tenants. |

Mooncake events in a valid batch run in their received order. A Clear removes
matching records established before it; a later Store in the same batch may
add availability again. Conductor does not sort by `event_id` or
automatically ignore a repeated ID.

SGLang native events use the same three transport frames but an array-like
MessagePack payload. The registered `SGLang` source selects that decoder,
treats a missing medium as the engine-local GPU cache, and keeps signed wire
hashes as unsigned 64-bit bit patterns. A SGLang key emitted
inside a Mooncake map event instead uses the Mooncake decoder and the
engine-specific object-key parser.

## Handle invalid messages

Conductor expects exactly three ZeroMQ (ZMQ) frames: topic, an eight-byte big-endian
sequence, and one MessagePack payload. An invalid frame count, sequence frame,
MessagePack value, outer three-item shape, batch timestamp, event-array type,
or batch DP type drops the complete message without dispatching its events. A
vLLM batch whose non-`nil` DP rank conflicts with registration is also rejected
as a whole.

Once the outer payload is valid, Conductor reads and applies each map
separately. An event with bad fields or values is skipped, while valid events
before and after it remain applied in received order. A later error does not
undo earlier changes. Unknown fields are ignored so a producer may add new
optional data; duplicate recognized fields and wrong recognized-field types
reject that one event.

Changing the topic cannot make Conductor read one source as the other source
type. A registered SGLang source first attempts the native array decoder; when
the event entries are maps, it falls back to the Mooncake map decoder for an
SGLang-backed Mooncake deployment. vLLM and Mooncake registrations do not switch
protocols.

## Clean up a source

Use `POST /unregister` with the same `instance_id`, `tenant_id`, and `dp_rank`
that identify the registration. Conductor first stops and joins that
subscriber, then removes its contributions before returning success. Events
already queued by the old subscriber cannot repopulate the index after cleanup.

For vLLM, unregister removes that endpoint/engine/rank's GPU records and rank
registration. For Mooncake, it removes every saved CPU and Disk object record
from that registered endpoint, across all event contexts and backends, while
preserving other Mooncake endpoints and all vLLM GPU records. The Mooncake
registration's `instance_id` and `dp_rank` identify the service for
`/services` and unregister; they do not create a query instance or override
event context.

## Plan for sequence gaps and reconnects

Conductor records the last transport sequence received for each subscription.
When a later value jumps forward, Conductor logs the gap and updates its
cumulative dropped-event and gap counters. For a vLLM or native SGLang
subscription with a configured `replay_endpoint`, it requests messages
beginning at the first missing sequence, consumes the publisher's multipart
replay stream through its end marker, and dispatches recovered messages before
the live message that exposed the gap. Replay messages at or beyond that live
sequence are drained but not dispatched twice.

If the replay request fails, its buffer no longer contains every missing
sequence, or no `replay_endpoint` is configured, Conductor logs a warning and
continues with the live message. A failed vLLM or SGLang range remains pending
and is retried by a later live message or reconnect, while cache records can
still remain incomplete or stale if the publisher has evicted the range;
Conductor does not invalidate them automatically.

After a disconnect, Conductor reconnects the live subscription. Only when
`replay_endpoint` was configured and a previous sequence is known does it ask
that endpoint for messages beginning at the next sequence and dispatch the
returned stream. An empty replay endpoint is valid and creates a live-only
subscription.

The current Mooncake publisher has no replay endpoint and sends no startup
list of objects cached before Conductor connected. A Mooncake source should
therefore use an empty `replay_endpoint`, and its initial Conductor view must
not be treated as complete if cache traffic started earlier. Conductor also
does not reject an older/repeated transport sequence or repeated `event_id` as
a duplicate; it processes the events it receives.

## Check Mooncake-to-Conductor setup

Use this order before allowing requests that create cache objects:

1. Build Mooncake Store with `-DENABLE_KV_EVENTS=ON`, enable its publisher,
   keep `kv_events_emit_object_key=true`, and confirm
   `GET /kv_events/status` reports `"enabled":true`.
2. Register at least one `vLLM` or `SGLang` engine first. Its `tenant_id`, `modelname`,
   `lora_name`, and `block_size` must equal the values Mooncake Store events
   will carry. Start each engine with the explicit `PYTHONHASHSEED` and
   `--prefix-caching-hash-algo` settings above. Check that `/global_view` shows
   that engine and every expected rank.
3. Use the same complete `hash_profile` for the engine and the `Mooncake`
   registration: `strategy`, `algorithm`, exact `python_hash_seed`, and
   `index_projection` must all match. Use `strategy: "sglang"` for regular
   SGLang hashing or `strategy: "sglang_bigram"` for EAGLE/speculative
   bigram hashing. SGLang ignores `python_hash_seed` and uses `cache_salt` in
   each query. Conductor derives and validates the profile; publisher events
   do not reveal it.
4. Configure the Mooncake publisher with a non-empty `backend_id`, a usable
   model fallback and positive block size, and the matching LoRA name.
5. Keep `object_key` emission enabled and use the registered engine's key
   format. A Conductor-compatible `stored` event must contain `object_key`;
   vLLM keys are parsed by the vLLM handler and SGLang keys by the SGLang
   handler. A legacy `connector_block_hash`, when present, is still checked.
6. Use no group or group zero, and emit only CPU or Disk availability for the
   Mooncake source. Conductor ignores other media and rejects nonzero groups.
7. Register the Mooncake endpoint and check `/services` for all expected vLLM
   and Mooncake subscriptions. Verify the configured seed and derived root in
   both `/services` and `/global_view`. With static configuration,
   subscriptions start concurrently, so keep cache-producing traffic paused
   until both checks pass.
8. Allow cache traffic, confirm the Mooncake publisher counters increase, and
   query a known token prefix. Shared `cpu` or `disk` counts must appear under
   the compatible vLLM instance, not under the Mooncake registration name.

`/services` shows that Conductor accepted a registration. It does not prove
that the ZMQ publisher delivered an event or that objects created before the
subscription are known. The [Conductor usage guide](../conductor/usage.md)
provides the complete registration and query commands.

## Know the current completeness limit

Mooncake connector keys can carry a layer number and tensor-parallel (TP),
prefill context-parallel (PCP), decode context-parallel (DCP), and
pipeline-parallel (PP) ranks. Conductor decodes those fields but currently
does not check whether all required layers or parallel parts are present before
reporting the block as available. It also does not use `cache_prefix` or
Mooncake `additional_salt` as one of the four cache-sharing fields.

## SGLang sources

Conductor registrations may use `SGLang` as the publisher type. Native SGLang
events use an array-like MessagePack decoder and expose signed 64-bit hashes;
Conductor preserves their bit patterns as `uint64`. SGLang's projection uses
the first 16 hexadecimal characters of its SHA-256 digest. Register
`strategy: "sglang"` for ordinary token hashing or
`strategy: "sglang_bigram"` for EAGLE/speculative decoding; the latter hashes
over overlapping token pairs and includes a final partial block when needed.

When SGLang writes through Mooncake Store, the wire envelope remains the
Mooncake map protocol. The original `object_key` is parsed by the SGLang
handler. Physical suffixes (`_k`, `_v`, and sidecar suffixes) are canonicalized
to one logical hash node, and SGLang group IDs such as
`sglang-hicache:<logical_key>` are opaque strings.

## Maintainer source note

Message decoding is implemented in
`mooncake-conductor/src/zmq/msg_decoder.cpp`; transport sequence and reconnect
handling are in `mooncake-conductor/src/zmq/zmq_client.cpp`. Event checks and
cleanup are in `mooncake-conductor/src/kvevent/event_handler.cpp` and
`event_manager.cpp`. Current parser and end-to-end fixtures are in
`mooncake-conductor/tests/msg_decoder_test.cpp` and
`event_ingest_integration_test.cpp`.
