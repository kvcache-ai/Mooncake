# Mooncake KV Event Publisher

This document describes the Mooncake KV Event Publisher: the feature that lets
Mooncake emit Dynamo-compatible KV cache events so Dynamo's router can build its
prefix index from blocks stored in Mooncake. It covers both interfaces of the
feature — how the inference engine (SGLang) feeds semantic information *into*
Mooncake, and how Mooncake publishes events *out* over ZMQ — plus a summary of
the internal implementation. It is the design/reference companion for the
upcoming PR.

## 1. Overview

Mooncake is a *centralized* KV cache manager: the master knows the full state of
every logical KV block. Dynamo's router consumes KV events to maintain its
prefix index, and its reference engines (SGLang/vLLM) emit one ZMQ stream per
worker. Because Mooncake owns all state centrally, it publishes **a single ZMQ
PUB stream for all workers** and identifies the physical owner of each block via
a per-event `worker_id` field. A Mooncake-specific adapter on the Dynamo side
reads that field. (See `mooncake开发方案.md` §7.3.)

End-to-end pipeline:

```text
SGLang (knows semantics)                 Mooncake master (centralized state)            Dynamo
  put(key, value, ReplicateConfig{        - groups physical objects into a              ZMQ SUB
       group_ids, kv_event_metadata })  →   logical block (manifest)                 →  + adapter
                                           - when complete -> BlockStored             →  prefix
                                           - on removal    -> BlockRemoved                index
                                           - publishes one ZMQ PUB stream
```

Two interfaces matter:

- **Input — SGLang → Mooncake (§2):** writes carry `kv_event_metadata` describing
  the logical block's semantics (token ids, hashes, layout). This is the only
  way Mooncake learns information it cannot derive itself.
- **Output — Mooncake → Dynamo (§3):** a single ZMQ PUB stream of msgpack events.

When no `kv_event_metadata` is attached and no publisher is installed, Mooncake
behavior is completely unchanged.

## 2. Input Interface: How SGLang Passes Semantics to Mooncake

Mooncake stores opaque bytes; it does not know token ids, sequence hashes, or how
many physical objects make up one logical KV block. Only the engine knows these.
SGLang therefore attaches this semantic information to each write through an
extension of `ReplicateConfig`.

### 2.1 Logical block vs. physical objects

A single logical KV block (e.g. one SGLang prefix page) may be stored as several
physical Mooncake objects — for example the `K` and `V` objects of an MHA page,
or several per-head component objects. The model is:

- `ReplicateConfig::group_ids` — one `group_id` per physical key, tying the
  physical objects of one logical block together (grouped keys already share
  metadata routing, coalesced lease refresh, and eviction behavior).
- `ReplicateConfig::kv_event_metadata` — group-level, **not** per-key: one
  `KvBlockEventMetadata` item per `group_id` carrying the block's semantics and
  the expected physical layout, so the master knows when the logical block is
  fully stored.

### 2.2 Extended `ReplicateConfig` (`replica.h`)

```cpp
struct ReplicateConfig {
    // ... existing fields ...
    std::optional<std::vector<std::string>> group_ids{};
    // Optional group-level KV event metadata. NOT per-key: each item describes
    // one logical Dynamo KV block keyed by group_id. Absent => behavior
    // unchanged.
    std::optional<std::vector<KvBlockEventMetadata>> kv_event_metadata{};
};
```

`KvBlockComponentSpec` — one physical object of a logical block:

| Field | Type | Meaning |
|-------|------|---------|
| `object_key` | str | Physical Mooncake object key. |
| `component_role` | str | e.g. `"k"`, `"v"`, `"mla_k"`, `"head_3_k"`. |
| `component_index` | u32 | Stable ordering index within the block. |

`KvBlockEventMetadata` — one logical block (one `group_id`):

| Field | Type | Meaning |
|-------|------|---------|
| `schema_version` | u32 | Metadata schema version (currently `1`). |
| `group_id` | str | Logical block id; must match an entry in `group_ids`. |
| `block_hash` | u64 | Dynamo external sequence block hash. |
| `parent_block_hash` | u64? | Parent sequence hash; unset for the root block. |
| `token_ids` | `[u32]` | Token ids in the block (Dynamo needs them for its local token hash). |
| `block_size` | u32 | Tokens per block; must match the consumer's `kv_block_size`. |
| `dp_rank` | u32? | Data-parallel rank (routing namespace). |
| `model_name` | str | Model name (routing namespace). |
| `lora_name` | str | LoRA adapter name (routing namespace). |
| `additional_salt` | str | Extra namespace salt. |
| `expected_object_count` | u32 | How many physical objects make the block complete. |
| `expected_components` | `[KvBlockComponentSpec]` | The expected physical objects (preferred; `expected_object_count` is derived from it when set). |
| `emit_stored_event` | bool | Whether to publish `BlockStored` (default true). |
| `emit_removed_event` | bool | Whether to publish `BlockRemoved` (default true). |

Completeness is decided by `expected_components` when provided; otherwise by
`expected_object_count`. Repeated writes for the same `group_id` are accepted as
idempotent retries when semantically identical (`SameSemanticsAs`) and rejected
as conflicts otherwise.

### 2.3 What SGLang provides vs. what Mooncake derives

| Information | Provided by |
|-------------|-------------|
| `token_ids`, `block_hash`, `parent_block_hash`, `block_size` | **SGLang** (semantics it alone knows) |
| `model_name`, `lora_name`, `dp_rank`, `additional_salt` | **SGLang** (routing namespace) |
| physical layout (`group_ids`, `expected_components`) | **SGLang** |
| `worker_id` (physical owner) | **Mooncake** (from replica placement; see §4.2) |
| `event_id`, `source`, `tenant_id`, `medium` | **Mooncake** |
| completeness / dedup / stored↔removed lifecycle | **Mooncake** (manifest) |

### 2.4 Python bindings (`store_py.cpp`)

`KvBlockComponentSpec`, `KvBlockEventMetadata`, and
`ReplicateConfig.kv_event_metadata` are exposed to Python. Example: one logical
block stored as two physical objects (K and V), publishing exactly one
`BlockStored` once both are present.

```python
from mooncake.store import (
    ReplicateConfig, KvBlockEventMetadata, KvBlockComponentSpec,
)

key_k, key_v = "blk-42:k", "blk-42:v"

c_k = KvBlockComponentSpec(); c_k.object_key = key_k; c_k.component_role = "k"; c_k.component_index = 0
c_v = KvBlockComponentSpec(); c_v.object_key = key_v; c_v.component_role = "v"; c_v.component_index = 1

meta = KvBlockEventMetadata()
meta.group_id = "blk-42"
meta.block_hash = 0x8f3a_0000_0000_0001
meta.parent_block_hash = 0x8f3a_0000_0000_0000   # leave unset for the root block
meta.token_ids = [101, 102, 103, 104]
meta.block_size = 64
meta.model_name = "llama-3"
meta.dp_rank = 0
meta.expected_components = [c_k, c_v]              # or: meta.expected_object_count = 2

config = ReplicateConfig()
config.replica_num = 1
config.group_ids = ["blk-42", "blk-42"]           # one entry per physical key
config.kv_event_metadata = [meta]

store.put(key_k, value_k, config)
store.put(key_v, value_v, config)   # block now complete -> one BlockStored event
```

## 3. Output Interface: The ZMQ Event Stream

### 3.1 Transport model

**Design decision — one ZMQ stream + per-event `worker_id`.** Dynamo's ZMQ
interface was designed for *one ZMQ connection per worker*: each engine worker
runs its own PUB socket and Dynamo identifies the source by which socket the
event arrived on. Mooncake, however, is a centralized KV cache manager — the
master alone knows the state of every block across all workers, so a single ZMQ
stream is sufficient to carry everything. We deliberately keep **one ZMQ stream**
and instead add an extra **`worker_id` field to each published event** to encode
which physical storage node actually holds the KV block. On the Dynamo side a
Mooncake-specific adapter then parses this `worker_id` to attribute each event to
the right worker. This avoids standing up (and managing) one socket per worker
on a centralized component while preserving Dynamo's per-worker routing
semantics. (See `mooncake开发方案.md` §7.3.)

- **One PUB socket** owned by the master. Subscribers (the Dynamo adapter)
  connect with a SUB socket.
- Each `Publish()` sends **one single-event batch**. Mooncake does not buffer or
  coalesce events at this layer; ordering and de-duplication are handled
  upstream in the manifest logic.
- `worker_id` inside each event tells the consumer which physical node holds the
  block. The batch-level `dp_rank` trailer is also set (from the event), but the
  adapter should rely on the per-event fields.

### 3.2 Wire format (3 frames)

Byte-compatible with the SGLang/vLLM ZMQ relay format Dynamo already supports
(`dynamo_kv_event_rules.md` §"ZMQ Relay Wire Format"):

| Frame | Content | Notes |
|-------|---------|-------|
| 1 | `topic` | Matched against the subscriber's `zmq_topic` filter. Default is the empty string (matches all). The listener requires exactly 3 frames. |
| 2 | 8-byte **big-endian** sequence | Monotonic per-publisher counter. Used by Dynamo for trace/log only; it is **not** the internal `event_id`. |
| 3 | msgpack payload | `[timestamp (f64), [events], dp_rank (i32 or nil)]`. |

Payload structure:

```text
[
  timestamp,        # f64, seconds since epoch (engine batch timestamp)
  [ <map event> ],  # array of raw KV events (always length 1 here)
  dp_rank           # i32, or nil when unknown
]
```

### 3.3 Map event fields

Each event is a msgpack **map** (Dynamo parses by field name and ignores unknown
keys). `BlockStored` carries all fields; `BlockRemoved` carries only the
identity + `block_hashes` + `medium`.

Common fields (both types):

| Field | Type | Meaning |
|-------|------|---------|
| `type` | str | `"BlockStored"` or `"BlockRemoved"`. |
| `block_hashes` | `[u64]` | External sequence block hashes (prefix-cumulative). |
| `medium` | str | Storage tier, e.g. `"EXTERNAL"`. |
| `event_id` | str | Unique id assigned by the master (`mooncake:<tenant>:<group>:<kind>:<seq>`). |
| `source` | str | Always `"mooncake"`. |
| `tenant_id` | str | Mooncake tenant. |
| `group_id` | str | Logical KV block group id. |
| `worker_id` | str | Physical worker holding the block (see §4.2). |

`BlockStored`-only fields: `parent_block_hash` (u64 or nil), `token_ids`
(`[u32]`), `block_size` (u32), `lora_name` (str), `model_name` (str), and
`dp_rank` (u32, present only when set on the event).

> `source`, `tenant_id`, `group_id`, `worker_id`, `event_id` are Mooncake
> extensions on top of the base Dynamo schema. A vanilla Dynamo decoder ignores
> unknown fields; the Mooncake adapter is the component that interprets
> `worker_id`.

### 3.4 Publisher C++ API (`kv_event_zmq_publisher.h`)

```cpp
struct ZmqKvEventPublisherConfig {
    std::string endpoint{"tcp://0.0.0.0:5557"}; // "tcp://host:*" picks a free port
    std::string topic{};                         // frame-1 topic; empty matches all
    bool bind{true};                             // true: bind (master is server)
    int linger_ms{0};                            // close linger; -1 = block, 0 = drop
    int send_hwm{0};                             // send high-water-mark; 0 = unlimited
};

class ZmqKvEventPublisher : public KvEventPublisher {
   public:
    explicit ZmqKvEventPublisher(const ZmqKvEventPublisherConfig& config);
    void Publish(const KvEvent& event) override;
    const std::string& endpoint() const;   // resolved endpoint (wildcard ports too)
    uint64_t published_count() const;       // diagnostics
};
```

Installing it on the master (any `KvEventPublisher` works; `nullptr` disables
publishing):

```cpp
auto publisher = std::make_shared<ZmqKvEventPublisher>(
    ZmqKvEventPublisherConfig{.endpoint = "tcp://0.0.0.0:5557"});
master_service.SetKvEventPublisher(publisher);
```

## 4. Internal Implementation

### 4.1 Group manifest & event lifecycle (`master_service.{h,cpp}`)

- Each `group_id` that carries `kv_event_metadata` gets a **manifest** stored in
  the same tenant shard as its members (so it is covered by the existing shard
  lock). It tracks the expected object keys, the keys currently present, and the
  per-worker dedup bookkeeping.
- `PutEnd` marks an object complete; when all expected components are present the
  master publishes exactly one `BlockStored` (deduplicated via
  `stored_event_published_workers`).
- Removal/eviction marks the logical block incomplete and publishes one
  `BlockRemoved` (deduplicated via `removed_event_published_workers`); the
  manifest is erased when the last group member is removed.
- Validation rejects conflicting redefinitions of a `group_id` and malformed
  metadata (empty/duplicate group ids, missing completeness expectation).

### 4.2 segment→worker_id registry (internal)

`worker_id` must identify the **physical worker** that stores the block, not the
logical segment name used for allocation. An internal registry resolves it, in
priority order:

1. **Explicit override** — `MasterService::RegisterSegmentWorkerId(segment, id)`
   (deployment config / control plane). Wins over everything.
2. **Mount-time endpoint** — captured automatically when a segment is mounted,
   from `Segment::te_endpoint` (the node's ip:port). Falls back to the segment
   name when the endpoint is empty.
3. **Fallback** — the segment name itself, when the segment is unknown.

Implementation notes:

- Two maps (`segment_worker_explicit_`, `segment_worker_auto_`) under a dedicated
  `shared_mutex`, so they can be read while a tenant shard lock is held.
- `MountSegment` auto-registers `segment.name → te_endpoint` after the mount
  succeeds and the segment lock is released.
- `PutEnd` does `segment = DeriveSegmentNameFromMetadata(...)` →
  `worker_id = ResolveWorkerId(segment)` → stamp the event. (`GetSegmentWorkerId`
  exposes the mapping for diagnostics/tests.)
- The registry is in-memory and rebuilt on remount; it is **not** part of the HA
  snapshot yet (see §7).

### 4.3 Event model & ZMQ publisher

- `KvEvent` / `KvEventType` plus the msgpack encode/decode functions
  (`EncodeKvEventMap`, `EncodeKvEventBatchPayload`, `DecodeKvEventBatchPayload`)
  live in `kv_event.{h,cpp}` and are shared by the ZMQ publisher and the
  in-memory mock used in tests.
- `ZmqKvEventPublisher` uses the **pimpl** idiom so `zmq.hpp` never leaks into a
  public header; the `Impl` owns a `zmq::context_t` and a PUB `zmq::socket_t`,
  resolves the concrete endpoint via `ZMQ_LAST_ENDPOINT`, and on `Publish()`
  encodes a single-event batch, prepends the monotonic big-endian sequence, and
  sends the 3 frames under a mutex. ZMQ send errors are logged and swallowed
  (publishing is best-effort and must not break the data path).

## 5. Build Integration

`mooncake-store/src/CMakeLists.txt` auto-detects ZeroMQ (cppzmq header
`zmq.hpp` + `libzmq`, with `$CONDA_PREFIX` hints):

- When found: compiles `kv_event_zmq_publisher.cpp` and propagates
  `MOONCAKE_STORE_WITH_ZMQ`, the include dir, and the link library as **PUBLIC**
  usage requirements of `mooncake_store` (so tests/consumers inherit them).
- When absent: the manifest/encoder still build; only the live ZMQ publisher is
  omitted. Guard ZMQ-specific code with `#ifdef MOONCAKE_STORE_WITH_ZMQ`.

Dependencies (e.g. in a conda env): `cppzmq` + `zeromq`.

## 6. Testing

`mooncake-store/tests/master_service_test.cpp`:

- Manifest lifecycle: creation/completion, sharing across group members,
  conflict rejection, erase on group removal, config validation.
- Publishing: single `BlockStored` on completion, no duplicates on repeated
  `PutEnd`, single `BlockRemoved` on delete, no-op without a publisher.
- worker_id: `SegmentWorkerRegistryCapturesEndpointAtMount`,
  `KvEventWorkerIdReflectsSegmentEndpoint`, `KvEventWorkerIdExplicitOverrideWins`.
- Transport: `KvEventEncoderRoundTrip`, and `ZmqKvEventPublisherRoundTrip`
  (guarded by `MOONCAKE_STORE_WITH_ZMQ`) where a real SUB socket receives the 3
  frames over TCP and decodes them back into the original `KvEvent`.

## 7. Limitations / Future Work

### 7.1 Limitations in this change

- **HA snapshot**: neither the KV group manifests nor the segment→worker_id
  registry are persisted in the master HA snapshot yet. After a failover the
  registry is rebuilt from remounts; manifest dedup state still needs snapshot
  support for fully correct cross-failover behavior.
- **Batching**: one event per ZMQ message today. Batching multiple events per
  payload is possible later (the encoder already supports it) if throughput
  requires it.

The two pieces below complete the end-to-end path but live in their own repos
(SGLang / Dynamo) and are out of scope for the Mooncake PR; they are documented
here so the full data flow is clear.

### 7.2 SGLang → Mooncake wiring (engine side, separate repo)

The Mooncake side of the input interface (§2) is complete, but SGLang does not
yet populate it. Follow-up work in the SGLang repo:

- At KV-cache write time, build one `KvBlockEventMetadata` per logical block and
  attach it (with the matching `group_ids`) to the `ReplicateConfig` passed to
  `store.put` / batch put.
- Map SGLang's block manager state onto the schema: external **sequence** block
  hash → `block_hash` / `parent_block_hash` (cumulative prefix hash, not the
  per-block local hash), the block's `token_ids`, and `block_size` (must equal
  Dynamo's `kv_block_size`).
- Enumerate the physical objects of each block as `expected_components`
  (`object_key` + `component_role` + `component_index`), e.g. the K and V objects
  of an MHA page, or per-head components. Set `model_name` / `lora_name` /
  `dp_rank` / `additional_salt` for the routing namespace.
- Decide `emit_stored_event` / `emit_removed_event` per deployment, and ensure
  the same `group_id` is reused (idempotent retries) rather than redefined.

### 7.3 Dynamo Mooncake adapter (router side, separate repo)

A single ZMQ stream carrying a centralized publisher's events needs a
Mooncake-specific adapter on the Dynamo side (the counterpart to the §3.1 design
decision). Follow-up work in the Dynamo repo:

- Subscribe to Mooncake's single PUB endpoint with a SUB socket and the matching
  `zmq_topic`, decode the 3-frame message and the `[timestamp, [events],
  dp_rank]` payload.
- **Parse the per-event `worker_id`** and attribute each `BlockStored` /
  `BlockRemoved` to that physical worker, instead of inferring the worker from
  the socket/connection (the assumption baked into the default one-socket-per-
  worker path). Use the per-event `worker_id` (and `dp_rank`) rather than the
  batch trailer.
- Map the Mooncake extension fields (`source`, `tenant_id`, `group_id`,
  `event_id`) into Dynamo's internal `RouterEvent` model, and translate `medium`
  → storage tier. Honor Dynamo's own `block_size == kv_block_size` and
  re-numbering/dedup rules.
- Treat the frame-2 sequence as trace/log only; rely on Mooncake's `event_id`
  and Dynamo's own monotonic ids for ordering.

## 8. Changed / Added Files

| File | Change |
|------|--------|
| `include/replica.h` | `KvBlockComponentSpec`, `KvBlockEventMetadata`, and `ReplicateConfig::kv_event_metadata`. |
| `mooncake-integration/store/store_py.cpp` | Python bindings for the metadata types + `kv_event_metadata`. |
| `include/kv_event.h`, `src/kv_event.cpp` | Event model + msgpack encode/decode. |
| `include/kv_event_zmq_publisher.h`, `src/kv_event_zmq_publisher.cpp` | **New.** ZMQ PUB transport (pimpl). |
| `include/master_service.h`, `src/master_service.cpp` | Group manifest, event publishing/dedup, segment→worker_id registry, `SetKvEventPublisher` / `Register`/`GetSegmentWorkerId`. |
| `src/rpc_service.cpp` | Config-level `kv_event_metadata` validation at the RPC entry. |
| `src/CMakeLists.txt` | ZeroMQ detection; conditional source + PUBLIC include/lib/`MOONCAKE_STORE_WITH_ZMQ`. |
| `tests/master_service_test.cpp` | Manifest, publisher, worker_id, and real ZMQ roundtrip tests. |
