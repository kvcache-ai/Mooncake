# Mooncake Conductor Indexer API

## Overview

Mooncake Conductor is a KV cache indexer used by routers and gateways to make
cache-aware scheduling decisions. It consumes KV cache events from inference
engines and from the Mooncake Store master, maintains prefix-hit metadata
across cache tiers, and exposes HTTP APIs for service registration and
cache-hit queries. The same operations are available over RPC through the
[Python client](../python/conductor.md).

The API follows the direction of:

- [RFC #1403: Mooncake KV-Store Indexer API Standardization](https://github.com/kvcache-ai/Mooncake/issues/1403)
- [RFC #1527: KV Events API Standardization](https://github.com/kvcache-ai/Mooncake/issues/1527)

One Conductor process can serve multiple model groups. Each query is scoped by
tenant, model, LoRA adapter, and block size, and reports hits per registered
instance.

## Concepts

### Storage tiers

Query results report four cumulative tiers:

| Tier | Meaning | Event source |
|---|---|---|
| `device` | Engine accelerator memory (GPU or NPU) on the matching DP rank | vLLM `gpu`; SGLang `gpu` |
| `cpu_local` | Engine-local host memory | SGLang `cpu` / `cpu_pinned` |
| `cpu_share` | Shared host memory pool, such as Mooncake Store | Mooncake `cpu` / `cpu_pinned` |
| `disk` | Disk-backed KV storage, engine-local or shared | SGLang `disk`; Mooncake `disk` |

Medium strings are matched case-insensitively. Events with other media are
ignored.

### Identity dimensions

| Dimension | Description |
|---|---|
| `model_name` | Model identifier. KV blocks from different models are incompatible. |
| `block_size` | Tokens per KV block. |
| `lora_name` | LoRA adapter name. Empty means the base model. |
| `tenant_id` | Tenant isolation. Empty or omitted means `default`. |
| `cache_salt` | Query-side hash namespace salt. It must match the salt the engine folded into its block hashes. |
| `instance_id` | Router-facing engine instance. Query results are keyed by it. |
| `dp_rank` | Data-parallel rank of an engine publisher. |
| `backend_id` | Event-facing cache owner, such as a Mooncake Store master. |

`instance_id` and `backend_id` have different meanings. `instance_id` is the
scheduling target returned by the Indexer API. `backend_id` identifies the
owner of shared cache objects in the event stream; shared-tier hits count for
every instance registered in the same context.

### Hash profiles

Conductor recomputes prefix hashes from token IDs rather than trusting an
opaque hash format. Every registration carries a `hash_profile` describing the
publisher's hashing:

| `strategy` | `algorithm` | `index_projection` | `python_hash_seed` |
|---|---|---|---|
| `vllm_v1` | `sha256` or `sha256_cbor` | `low64_be` | Required. The publisher's `PYTHONHASHSEED` as decimal text in `0..4294967295`, or `"random"`. |
| `sglang` or `sglang_bigram` | `sha256_raw` | `first64_be` | Optional and ignored. |

`vllm_v1` hashes complete blocks only. `sglang` also hashes the final partial
block. `sglang_bigram` hashes adjacent token pairs, so a query with `n` tokens
can match at most `n - 1` positions. The server derives `root_digest` from the
profile; it appears in read responses but is not accepted as input.

All services registered for the same `(tenant_id, model_name, lora_name,
block_size)` context must use the same hash profile.

## HTTP APIs

The HTTP server listens on `http_server_port` (default `13333`).

### Encoding

Control API request and response bodies are MessagePack. These requests must
set a `Content-Type` containing `application/msgpack`, and their responses use
`Content-Type: application/msgpack`. The examples below show MessagePack maps
in JSON notation. The `/metrics` endpoint described below returns Prometheus
text instead.

Request maps are strict: unknown fields are rejected, and every field has a
fixed type.

### Errors

Validation failures return HTTP 400:

```json
{
  "error": "block_size must be positive",
  "reason": "invalid_value",
  "field": "block_size"
}
```

`reason` is one of `unsupported_content_type`, `invalid_msgpack`,
`unknown_field`, `missing`, `invalid_type`, `invalid_value`, `out_of_range`,
`unsupported`, or `invalid_registration`. `field` is present when the error
concerns one field, and `index` when it concerns one array element.

Service-level failures return `{"error": "<message>"}` with status 400 for
invalid parameters, 404 for an unknown service, 503 when the service is
unavailable, and 500 for internal errors. Calling a route with the wrong method
returns 405.

### `POST /register`

Registers a KV event publisher and starts consuming its events.

```json
{
  "endpoint": "tcp://10.0.0.1:5557",
  "replay_endpoint": "tcp://10.0.0.1:5558",
  "type": "vLLM",
  "modelname": "deepseek",
  "lora_name": "",
  "tenant_id": "default",
  "instance_id": "vllm-prefill-node1",
  "block_size": 16,
  "dp_rank": 0,
  "hash_profile": {
    "strategy": "vllm_v1",
    "algorithm": "sha256_cbor",
    "python_hash_seed": "0",
    "index_projection": "low64_be"
  }
}
```

| Field | Required | Description |
|---|---|---|
| `endpoint` | Yes | ZMQ PUB endpoint of the publisher. |
| `type` | Yes | `vLLM`, `SGLang`, or `Mooncake` (case-sensitive). |
| `modelname` | Yes | Model name. This is the HTTP wire name for `model_name`. |
| `instance_id` | Yes | Router-facing instance identity. |
| `block_size` | Yes | Positive KV block size in tokens. |
| `dp_rank` | Yes | Data-parallel rank, `0` or greater. |
| `hash_profile` | Yes | Map with `strategy`, `algorithm`, `index_projection`, and `python_hash_seed`, as described under Hash profiles above. |
| `replay_endpoint` | No | ZMQ replay endpoint for gap recovery. Used by vLLM and SGLang; ignored for Mooncake. |
| `lora_name` | No | LoRA adapter name. Defaults to empty (base model). |
| `tenant_id` | No | Defaults to `default`. |
| `cache_group` | No | Integer or nil. Only `0` is supported. |

The service key is `instance_id|tenant_id|dp_rank`. Registering an identical
configuration again succeeds without change. A different configuration for an
existing key, or an endpoint already used by another service, fails with
`invalid_registration`.

Response:

```json
{
  "status": "registered successfully",
  "instance_id": "vllm-prefill-node1"
}
```

### `POST /unregister`

Stops consuming events for a registered publisher and removes its entries from
the index.

```json
{
  "instance_id": "vllm-prefill-node1",
  "tenant_id": "default",
  "dp_rank": 0
}
```

`instance_id` and `dp_rank` are required; `tenant_id` defaults to `default`. No
other fields are accepted.

Response:

```json
{
  "status": "unregistered successfully",
  "removed_instances": ["vllm-prefill-node1|default|0"]
}
```

An unknown service key returns 404.

### `POST /query`

Queries continuous prefix-cache hits for a prompt.

```json
{
  "model": "deepseek",
  "lora_name": "",
  "token_ids": [101, 15, 100, 55, 89],
  "tenant_id": "default",
  "instance_id": "vllm-prefill-node1",
  "block_size": 16,
  "cache_salt": ""
}
```

| Field | Required | Description |
|---|---|---|
| `model` | Yes | Model name. Note this is `model`, not `modelname`. |
| `block_size` | Yes | Positive KV block size in tokens. |
| `token_ids` | Yes | Array of integers in signed 32-bit range, or a binary of little-endian `int32` values. |
| `lora_name` | No | LoRA adapter name. Defaults to empty (base model). |
| `tenant_id` | No | Defaults to `default`. |
| `instance_id` | No | Restrict results to one instance. Omitted queries every instance in the context. |
| `cache_salt` | No | String or nil. Empty means no salt. |

Response:

```json
{
  "instances": {
    "vllm-prefill-node1": {
      "longest_matched": 32,
      "dp": {"0": 16},
      "rank_matches": {
        "0": {"device": 16, "cpu_local": 16, "cpu_share": 32, "disk": 32}
      },
      "device": 16,
      "cpu_local": 16,
      "cpu_share": 32,
      "disk": 32
    }
  }
}
```

| Field | Description |
|---|---|
| `instances` | Map from instance ID to its hit counts. |
| `longest_matched` | Longest continuous prefix in tokens across all tiers and ranks. Equal to `disk`. |
| `device`, `cpu_local`, `cpu_share`, `disk` | Prefix tokens reachable using that tier and every tier before it. Each is the maximum over the instance's ranks. |
| `dp` | `device` count per DP rank. Rank keys are decimal strings. |
| `rank_matches` | Per-rank tier counts. |

Tier counts are cumulative, `device <= cpu_local <= cpu_share <= disk`, so do
not add them together. Counts are in tokens and never exceed the number of
queried tokens. A context with no registered instances, or a filter that
matches none, returns empty `instances`. Registered instances with no match
appear with all counts set to zero.

### `GET /global_view`

Returns a snapshot of indexed contexts.

```json
{
  "context_count": 1,
  "contexts": [
    {
      "model_name": "deepseek",
      "lora_name": "",
      "block_size": 16,
      "tenant_id": "default",
      "prefix_count": 42,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "root_digest": "<64 hex characters>",
        "index_projection": "low64_be"
      },
      "instances": {"vllm-prefill-node1": [0]}
    }
  ]
}
```

### `GET /services`

Lists active publisher configurations, ordered by service key.

```json
{
  "count": 1,
  "services": [
    {
      "Endpoint": "tcp://10.0.0.1:5557",
      "ReplayEndpoint": "tcp://10.0.0.1:5558",
      "Type": "vLLM",
      "ModelName": "deepseek",
      "LoraName": "",
      "TenantID": "default",
      "InstanceID": "vllm-prefill-node1",
      "BlockSize": 16,
      "DPRank": 0,
      "CacheGroup": null,
      "HashProfile": {"strategy": "vllm_v1", "...": "..."}
    }
  ]
}
```

Service keys use these PascalCase names, unlike the registration input.
`HashProfile` is the resolved profile, including `root_digest`.

## KV event input

Conductor subscribes to each registered endpoint with a ZMQ `SUB` socket. Every
message must have three frames: an empty or ignored topic, an unsigned 64-bit
big-endian sequence number, and a MessagePack payload
`[timestamp, [event_map, ...], dp_rank]`. Events must be maps; array-encoded
events are rejected.

| `type` | Event tags | Notes |
|---|---|---|
| `vLLM` | `type` = `BlockStored`, `BlockRemoved`, `AllBlocksCleared` | Only `gpu` medium is indexed. The batch `dp_rank` must match the registration. `AllBlocksCleared` clears the engine's `device` tier. |
| `SGLang` | `type` = `BlockStored`, `BlockRemoved`, `AllBlocksCleared` | Indexes `gpu`, `cpu_pinned`, and `disk`. `AllBlocksCleared` clears all of the engine's tiers. |
| `Mooncake` | `event_type` = `stored`, `removed`, `cleared` | The Mooncake Store master stream described below. |

A sequence gap is recovered through `replay_endpoint` when one is registered
(vLLM and SGLang). Otherwise the source is marked stale: its entries are
dropped from the index, and its events are ignored until the service is
registered again. See the [architecture](../../design/conductor/conductor-architecture-design.md)
for details.

(mooncake-store-master-publisher)=
### Mooncake Store master publisher

`mooncake_master` publishes RFC #1527 map events when built with
`ENABLE_KV_EVENTS=ON` and started with `--enable_kv_events=true`. The wire
format, publication rules, and loss semantics are documented in the
[subscriber guide](../../design/conductor/subscriber-guide.md) and
[publisher design](../../design/conductor/publisher-design.md). Master flags are
listed in the Store deployment guide.

The master never parses object keys. It forwards the raw Store key as
`object_key` and always emits an empty `seq_hashes`, with nil `parent_hash`,
`token_ids`, and `base_block_idx`. Conductor derives the block identity from
the key itself, using the convention selected by the registered hash profile:

- `vllm_v1`: the hex block hash in a vLLM or vLLM-Ascend Mooncake connector key
  (fields separated by `@`), or `connector_block_hash` when present, projected to
  its low 64 bits;
- `sglang` / `sglang_bigram`: the 64-hex-character hash in an SGLang HiCache key,
  projected to its first 64 bits. Several component keys of one block, such as
  separate K and V objects, count as one block until the last is removed.

To index a master stream:

1. Start the master with `--kv_events_model_name` and `--kv_events_block_size`
   (and `--kv_events_lora_name` when used) matching the engine context. Each
   event's `tenant_id`, `model_name`, `lora_name`, and `block_size` select the
   context, which must already be registered by an engine service with the same
   hash profile.
2. Register the master's reachable endpoint with `type` = `Mooncake` and the
   same hash profile as the engines that write the keys. `instance_id` and
   `dp_rank` identify the subscription only; shared-tier hits are reported for
   every instance in the context.
3. Keep `--kv_events_emit_object_key=true` (the default). Without object keys the
   master emits no `stored` or `removed` events.

The master's memory replicas feed the `cpu_share` tier and its disk-class
replicas feed `disk`. Its `group_id` must be empty or `0`. A `cleared` event
removes every object indexed for that `backend_id` and `tenant_id`. The master
has no replay endpoint, so any sequence gap marks the stream stale.

## Prometheus metrics

### `GET /metrics`

The existing HTTP server exposes Prometheus text format 0.0.4 on `/metrics`.
No separate port or metrics flag is required. Unlike the control APIs, this
endpoint returns `text/plain; version=0.0.4; charset=utf-8` and needs no
MessagePack decoding:

```bash
curl -fsS http://127.0.0.1:13333/metrics
```

For example, a fresh process with no subscriptions returns the following
capability sample, along with the metric family HELP and TYPE declarations:

```text
# HELP mooncake_conductor_metrics_info Conductor event metrics schema version.
# TYPE mooncake_conductor_metrics_info gauge
mooncake_conductor_metrics_info{schema="2"} 1
```

Configure Prometheus to scrape the Conductor HTTP listener:

```yaml
scrape_configs:
  - job_name: mooncake-conductor
    scrape_interval: 30s
    static_configs:
      - targets: ["127.0.0.1:13333"]
```

Use an address reachable from Prometheus in `targets`. The metrics endpoint
has the same network access boundary as the other Conductor HTTP endpoints.

### Metric families and labels

Except for `mooncake_conductor_metrics_info`, each sample has these labels:

- `tenant_id`: the subscription's tenant.
- `cache_domain`: the registered `modelname` value.
- `instance_id`: the registered inference instance or storage publisher.
- `dp_rank`: the registered data-parallel rank.
- `publisher_kind`: `vLLM`, `SGLang`, or `Mooncake`.

Replicas on the same host remain separate subscriptions. No request IDs,
block hashes, tokens, or error strings are used as labels.

| Metric | Type | Meaning / additional labels |
|---|---|---|
| `mooncake_conductor_metrics_info` | Gauge | Value `1`, with `schema="2"`. Present even without subscriptions. |
| `mooncake_conductor_events_supported` | Gauge | `1` for vLLM/SGLang engine event counters, `0` for Mooncake storage publishers. |
| `mooncake_conductor_events_total` | Counter | Engine event handling attempts by `event_type`, `result`, and `origin`, including redeliveries. |
| `mooncake_conductor_event_block_references_total` | Counter | Block hashes carried by successfully applied stored/removed events; `event_type` and `origin`. |
| `mooncake_conductor_last_applied_event_timestamp_seconds` | Gauge | Conductor receive time of the last successfully applied engine event, in Unix seconds. Absent until an event is applied. |
| `mooncake_conductor_event_batch_redeliveries_total` | Counter | Engine batches delivered to the handler with a non-increasing nonnegative transport sequence; label `origin`. Includes retries and duplicate deliveries. |
| `mooncake_conductor_source_stale` | Gauge | `1` when an unrecoverable gap or dispatch failure invalidates the source; otherwise `0`. This is not an idle-time detector. |
| `mooncake_conductor_batch_failures_total` | Counter | Malformed multipart frames, invalid envelopes, or failed batch dispatches. |
| `mooncake_conductor_sequence_gaps_total` | Counter | Gaps observed in the live transport sequence. |
| `mooncake_conductor_sequence_missing_messages_total` | Counter | Messages missing from those live gaps, including messages subsequently recovered by replay. |
| `mooncake_conductor_replay_requests_total` | Counter | Replay attempts with `result="success"` or `result="failed"`; failed retries are included. A successful empty replay also counts. |

Engine `event_type` is `stored`, `removed`, `cleared`, or `unknown` for an
undecodable individual event. `result` is `applied`, `ignored`, or `rejected`;
unknown events only have a rejected series. Ignored events include vLLM media
that the indexer does not track and empty removals. Whole-batch rejection is
counted by `batch_failures_total`, rather than inventing individual events.
vLLM CPU events and SGLang engine-local CPU/DISK events are included when the
indexer accepts them.

`origin` is `live` or `replay`. Event and block-reference counters describe
**handling attempts**, not unique events or final cache contents. If a rejected
event is corrected and delivered again with the same sequence, both the rejected
attempt and the later applied attempt are counted. In a partially successful
batch, already successful events that are processed again also count again.
Repeated successes and repeated failures are observable; counters never decrease.

`event_batch_redeliveries_total` separately counts batches whose nonnegative
sequence is at or below the highest sequence previously delivered to that
subscription's handler. It does not distinguish a corrected retry from an
identical duplicate. Calls without a transport sequence count their events but
do not affect this batch counter. Metrics do not suppress processing or change
transport recovery: messages discarded by transport deduplication never reach
the handler and therefore do not add handling attempts. For overlapping buffered
live/replay messages, the first buffered copy determines the origin.

Schema `2` replaces the earlier schema `1` sequence-deduplicated event counts
with these handling-attempt semantics and adds the redelivery counter. Each
metric family's HELP/TYPE declarations and all its subscriptions' samples are
emitted together for compatibility with Prometheus text parsers.

Engine event counters start at zero for each subscription. Mooncake storage
publishers expose transport metrics and `events_supported=0`; they do not
expose misleading zero engine-event counts. All per-source series disappear
on unregister and restart at zero on re-registration. A process restart resets
counters and the in-memory index. These metrics do not add startup replay or
index persistence.

### Event and block-reference rates

Use separate panels or units for event counts and the block references they
carry. For example, one applied stored event containing three block hashes
adds **one event** and **three block references**. These are event activity
counts, not inference request counts, cache-hit counts, or resident cache size.

Applied live events per minute, grouped by replica and event type:

```promql
sum by (tenant_id, cache_domain, instance_id, event_type) (
  rate(mooncake_conductor_events_total{
    origin="live", result="applied", event_type=~"stored|removed"
  }[5m])
) * 60
```

Block references in those events per minute:

```promql
sum by (tenant_id, cache_domain, instance_id, event_type) (
  rate(mooncake_conductor_event_block_references_total{
    origin="live", event_type=~"stored|removed"
  }[5m])
) * 60
```

When the subscription's effective block size is known, multiply block references
by that size to estimate tokens involved in events. For a 16-token block, three
references correspond to 48 tokens; repeated references remain included. There
is no native token counter. Do not multiply aggregates spanning different block
sizes by one shared factor. Prometheus needs multiple successful scrapes before
`rate` produces values; missing history must not be interpreted as zero.
