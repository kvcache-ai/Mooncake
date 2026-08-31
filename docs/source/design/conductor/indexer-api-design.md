# Mooncake Conductor HTTP API

[中文](../../zh/design/conductor/indexer-api-design.md)

This reference describes the five HTTP endpoints implemented by the current
C++ Conductor service. Use it to register live event sources, remove them,
inspect Conductor's in-memory state, and query reusable cache prefixes. Field
names, allowed values, response casing, and error formats below follow the
current parser and serializer.

## Choose an endpoint

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/register` | Start one vLLM or Mooncake event subscription. |
| `POST` | `/unregister` | Stop one subscription and clean up that endpoint's cache information. |
| `POST` | `/query` | Query cache availability from prompt token IDs. |
| `GET` | `/global_view` | Inspect cache-sharing groups and registered vLLM ranks. |
| `GET` | `/services` | List active event subscriptions and their exact configuration. |

## Common request and response rules

Every endpoint speaks **msgpack only**. `POST` bodies are msgpack maps sent
with `Content-Type: application/msgpack`; success responses and error bodies
are msgpack maps with `Content-Type: application/msgpack`. `GET` endpoints
(`/services`, `/global_view`) also return msgpack. Unknown fields are rejected
on every endpoint. Map key order is not part of the contract.

Most validation failures return status `400` with a msgpack map:

```json
{
  "error": "unsupported request field: root_digest",
  "reason": "unknown_field",
  "field": "root_digest"
}
```

(`json` is used here only for readability; on the wire this is a msgpack map
with the same keys.) `field` is present when one field caused the error.
`index` is also present when one array element caused it.

## `POST /register`

This endpoint starts one event subscription. Register each vLLM data-parallel
(DP) rank with a separate endpoint. A Mooncake subscription supplies shared CPU or Disk
information but does not create an inference-instance row in `/query`.

### Request fields

| Field | Required | Accepted value and when it matters |
|---|---|---|
| `endpoint` | Yes | Non-empty ZeroMQ live-publisher endpoint. It must not already belong to another active registration. |
| `type` | Yes | Exactly `vLLM`, `SGLang`, or `Mooncake`. It decides which event message format and object-key parser Conductor uses. |
| `modelname` | Yes | Non-empty registered model name. It defines vLLM context; Mooncake events carry the model that selects their actual shared context. |
| `instance_id` | Yes | Non-empty inference engine name for vLLM, or subscription name for Mooncake. It is part of the service key; a Mooncake value does not become a query instance. |
| `block_size` | Yes | Positive registered token count per block. It defines vLLM context; Mooncake events carry their actual block size. |
| `dp_rank` | Yes | Integer from `0` through the platform's maximum `int`. It selects the vLLM DP rank and is part of every service key; for Mooncake it is subscription identity only. |
| `hash_profile` | Yes | Object containing all four supported hash fields described below. |
| `replay_endpoint` | No | String endpoint used after a reconnect to ask for missed events. Defaults to `""`, which disables the replay socket. |
| `lora_name` | No | Registered Low-Rank Adaptation (LoRA) adapter name, default `""`. It defines vLLM context; Mooncake events carry their actual LoRA name. |
| `tenant_id` | No | Registered tenant name. Omitted or `""` becomes `"default"`. It defines vLLM context and the service key; Mooncake events carry their actual tenant. |
| `cache_group` | No | Integer `0` or `null`. Omission also means no explicit group. Other values and arrays are rejected. |

The only supported `hash_profile` shape is:

| Hash field | Supported value |
|---|---|
| `strategy` | `vllm_v1` |
| `algorithm` | `sha256_cbor` |
| `python_hash_seed` | String containing exactly `random` or ASCII decimal text whose numeric value is in `0..4294967295`. The original accepted text, including leading zeroes, is preserved. |
| `index_projection` | `low64_be` |

The input object must contain exactly those four fields. Empty, signed,
whitespace-padded, non-string, invalid UTF-8, and out-of-range seeds are
rejected. `"0"` and `"00"` are distinct; numeric JSON `0` is invalid.
Registration-time `root_digest` is a legacy unknown field and is rejected.

Each four-field cache-sharing group must use one identical resolved hash
profile. For vLLM those fields come from registration; for Mooncake they come
from each event, while the profile comes from the Mooncake registration.
Conductor canonical-CBOR encodes the exact seed text and calculates SHA-256 to
derive the root. The exact text must match `PYTHONHASHSEED` on every compatible
vLLM process. The explicit string `random` is supported, but leaving the
environment variable unset makes vLLM choose random root bytes that Conductor
cannot reproduce and is unsupported. vLLM `--seed` controls model and sampling
randomness, not prefix-cache hash identity.

See [how token blocks become lookup values](./conductor-architecture-design.md#how-token-blocks-become-lookup-values)
for the canonical Concise Binary Object Representation (CBOR) input order,
LoRA-before-`cache_salt` ordering, complete parent-digest chaining,
final-eight-byte big-endian lookup rule, and labelled golden vector.
`cache_salt` is a query field, not a registration field.
Mooncake event `additional_salt` is diagnostic and is not accepted by this
HTTP endpoint.

### Minimal request

```python
import msgpack
import httpx

body = msgpack.packb(
    {
        "endpoint": "tcp://127.0.0.1:5557",
        "type": "vLLM",
        "modelname": "test-model",
        "instance_id": "engine-a",
        "block_size": 16,
        "dp_rank": 0,
        "hash_profile": {
            "strategy": "vllm_v1",
            "algorithm": "sha256_cbor",
            "python_hash_seed": "0",
            "index_projection": "low64_be",
        },
    },
    use_bin_type=True,
)
response = httpx.post(
    "http://127.0.0.1:13333/register",
    content=body,
    headers={"Content-Type": "application/msgpack"},
)
```

Status `200` returns a msgpack map equivalent to:

```json
{
  "status": "registered successfully",
  "instance_id": "engine-a"
}
```

Submitting the exact same active registration again returns the same success
without starting another subscriber. A conflicting service key, endpoint, or
hash profile returns a msgpack `400` with `reason` `invalid_registration`.
Failure to start the local subscription client returns a msgpack `500` error
map.

## `POST /unregister`

This endpoint stops one exact subscription. It waits for that subscriber to
stop before removing cache information contributed by its endpoint.

### Request fields

| Field | Required | Accepted value and when it matters |
|---|---|---|
| `instance_id` | Yes | Non-empty string used when the source was registered. |
| `dp_rank` | Yes | Non-negative integer rank used when the source was registered. |
| `tenant_id` | No | String used when the source was registered. Omitted or `""` becomes `"default"`. |

No other fields are accepted.

### Minimal request

```python
import msgpack
import httpx

body = msgpack.packb({"instance_id": "engine-a", "dp_rank": 0}, use_bin_type=True)
response = httpx.post(
    "http://127.0.0.1:13333/unregister",
    content=body,
    headers={"Content-Type": "application/msgpack"},
)
```

Status `200` returns a msgpack map with the exact service key that was removed:

```json
{
  "status": "unregistered successfully",
  "removed_instances": [
    "engine-a|default|0"
  ]
}
```

An unknown service key returns a msgpack error map with status `404`. A
cleanup failure after the subscriber stops returns status `500`. Conductor
keeps that service key and endpoint reserved, so neither can be registered
again. Retry the same `/unregister` request until cleanup succeeds.

## `POST /query`

This endpoint hashes complete token blocks with the profile already registered
for the requested tenant, model, LoRA name, and block size. The request cannot
override the strategy, algorithm, `python_hash_seed`, derived root digest, or
final-eight-byte lookup rule.

**The request body is msgpack, like every other endpoint.** Send a msgpack
map with `Content-Type: application/msgpack`; any other content type is
rejected with `unsupported_content_type`. `token_ids` may be a msgpack `bin`
of little-endian signed 32-bit integers (preferred: 4 bytes per token, no
per-element parsing) or a msgpack array of integers. The response is a
msgpack map as well.

### Request fields

| Field | Required | Accepted value and when it matters |
|---|---|---|
| `model` | Yes | Non-empty model name string. It must match registration `modelname`. |
| `block_size` | Yes | Positive integer. Only complete groups of this many tokens are hashed. |
| `token_ids` | Yes | msgpack `bin` of little-endian int32, or an array of signed 32-bit integers. An empty value is valid and reports zero hits for compatible registered ranks. |
| `tenant_id` | No | String. Omitted or `""` becomes `"default"`. |
| `lora_name` | No | String. Defaults to `""` for the base model. |
| `cache_salt` | No | String, `nil`, or omitted. `nil`, `""`, and omission all select the no-salt hash path; a non-empty value must match the producer. |
| `instance_id` | No | String filter. A matching registered instance is returned alone; an unknown value returns an empty `instances` object. |

No hash-profile override fields are accepted. A missing cache-sharing group
also returns status `200` with an empty `instances` object and does not create
state.

### Minimal request

```python
import msgpack
import struct
import httpx

token_ids = list(range(16))
body = msgpack.packb(
    {
        "model": "test-model",
        "block_size": 16,
        "token_ids": struct.pack(f"<{len(token_ids)}i", *token_ids),
    },
    use_bin_type=True,
)
response = httpx.post(
    "http://127.0.0.1:13333/query",
    content=body,
    headers={"Content-Type": "application/msgpack"},
)
```

If `engine-a` rank `0` is registered and no matching cache event has arrived,
the response is:

```json
{
  "instances": {
    "engine-a": {
      "longest_matched": 0,
      "dp": {
        "0": 0
      },
      "gpu": 0,
      "cpu": 0,
      "disk": 0,
      "rank_matches": {
        "0": {
          "gpu": 0,
          "cpu": 0,
          "disk": 0
        }
      }
    }
  }
}
```

### Result fields

| Field | Meaning |
|---|---|
| `instances` | Map keyed by selected registered vLLM `instance_id`. Mooncake subscriptions are not result rows. |
| `longest_matched` | The instance `disk` boundary: the largest ordered GPU-to-CPU-to-Disk prefix realized by one registered rank. |
| `dp` | Per-rank cumulative boundary after the GPU phase. Rank keys are decimal JSON strings and values remain integers. |
| `rank_matches` | Map with the same keys as `dp`. Each rank maps to cumulative integer `gpu`, `cpu`, and `disk` boundaries. |
| `gpu` | Maximum rank-level GPU boundary for this engine. |
| `cpu` | Maximum rank-level cumulative boundary after GPU and shared CPU. |
| `disk` | Maximum rank-level cumulative boundary after GPU, shared CPU, and shared Disk. |

For each registered rank, Conductor consumes a consecutive GPU prefix, tests
the first GPU miss in shared CPU, and then tests the first CPU miss in shared
Disk. It never returns to a higher tier after entering a lower tier, and the
first Disk miss ends that rank's result. All boundaries count only complete
query blocks. The two rank maps include identical keys, including zero-hit
ranks, and every result satisfies:

```text
dp[rank] == rank_matches[rank].gpu
0 <= rank_matches[rank].gpu
  <= rank_matches[rank].cpu
  <= rank_matches[rank].disk
  <= complete_query_tokens
longest_matched == disk
```

`rank_matches` is additive and the existing field types are unchanged.
However, `cpu`, `disk`, and some `longest_matched` values now describe this
ordered cumulative path rather than independent prefixes or an unordered
per-block union.

For the current HTTP test state, instance `1` rank `0` has a 32-token GPU
prefix, instance `2` rank `1` has none, and both see 48-token shared CPU and
Disk prefixes. The exact response is:

```json
{
  "instances": {
    "1": {
      "longest_matched": 48,
      "gpu": 32,
      "dp": {
        "0": 32
      },
      "cpu": 48,
      "disk": 48,
      "rank_matches": {
        "0": {
          "gpu": 32,
          "cpu": 48,
          "disk": 48
        }
      }
    },
    "2": {
      "longest_matched": 48,
      "gpu": 0,
      "dp": {
        "1": 0
      },
      "cpu": 48,
      "disk": 48,
      "rank_matches": {
        "1": {
          "gpu": 0,
          "cpu": 48,
          "disk": 48
        }
      }
    }
  }
}
```

As an ordered tier example, suppose rank `0` has the first two 16-token blocks
only on GPU, the third only in shared CPU, and the fourth only in shared Disk.
That instance returns:

```json
{
  "instances": {
    "engine-a": {
      "longest_matched": 64,
      "gpu": 32,
      "dp": {
        "0": 32
      },
      "cpu": 48,
      "disk": 64,
      "rank_matches": {
        "0": {
          "gpu": 32,
          "cpu": 48,
          "disk": 64
        }
      }
    }
  }
}
```

See [what query fields mean](./conductor-architecture-design.md#what-query-fields-mean)
for the continuity and per-rank rules.

## `GET /global_view`

This endpoint shows the cache-sharing groups known to this Conductor process.
It takes no request body.

### Minimal request

```python
import msgpack
import httpx

view = msgpack.unpackb(
    httpx.get("http://127.0.0.1:13333/global_view").content, raw=False
)
```

After registering `engine-a` ranks `0` and `1`, before cache events arrive, a
one-context response (msgpack map, rendered as JSON here) has this shape:

```json
{
  "context_count": 1,
  "contexts": [
    {
      "model_name": "test-model",
      "lora_name": "",
      "block_size": 16,
      "tenant_id": "default",
      "prefix_count": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
        "index_projection": "low64_be"
      },
      "instances": {
        "engine-a": [
          0,
          1
        ]
      }
    }
  ]
}
```

`context_count` is the number of entries in `contexts`. `prefix_count` counts
distinct final-eight-byte lookup values that still have at least one GPU, CPU,
or Disk record. `instances` maps registered vLLM engines to numeric rank
arrays; Mooncake subscriptions are not added to this map. Context array order
is not guaranteed. The resolved `hash_profile` reports the exact configured
seed and its derived lowercase root together.

## `GET /services`

This endpoint lists active subscription configurations. It takes no request
body. Field names intentionally use the casing shown below, which differs from
the register request.

### Minimal request

```python
import msgpack
import httpx

services = msgpack.unpackb(
    httpx.get("http://127.0.0.1:13333/services").content, raw=False
)
```

After the minimal register example, status `200` returns a msgpack map:

```json
{
  "count": 1,
  "services": [
    {
      "Endpoint": "tcp://127.0.0.1:5557",
      "ReplayEndpoint": "",
      "Type": "vLLM",
      "ModelName": "test-model",
      "LoraName": "",
      "TenantID": "default",
      "InstanceID": "engine-a",
      "BlockSize": 16,
      "DPRank": 0,
      "CacheGroup": null,
      "HashProfile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "root_digest": "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e",
        "index_projection": "low64_be"
      }
    }
  ]
}
```

`count` is the number of active service keys. `CacheGroup` is `null` when the
register request omitted it. Service array order is not guaranteed.
Registration appearing here confirms local subscription setup, not remote
event delivery or recovery of earlier events. Compare both
`python_hash_seed` and derived `root_digest` with `/global_view` before sending
cache-producing traffic.

## Understand errors

All error responses are msgpack maps; the JSON rendering below is for
readability only. Validation failures carry a stable `reason`, operational
failures carry only `error`:

| Situation | Status | Body |
|---|---|---|
| Field validation for any `POST` endpoint | `400` | `error`, `reason`, and applicable `field` or `index`. |
| Any `POST` endpoint with a non-msgpack `Content-Type` | `400` | `{"error":"Content-Type must be application/msgpack","reason":"unsupported_content_type"}`. |
| Malformed msgpack body or a non-map body on any `POST` endpoint | `400` | `{"error":"Invalid msgpack object","reason":"invalid_msgpack"}`. |
| `/unregister` service key not found | `404` | `{"error":"service not found: engine-a\|default\|0"}`. |
| `GET` on `/register`, `/unregister`, or `/query`; `POST` on `/global_view` or `/services` | `405` | `{"error":"Method not allowed"}`. |
| `/register` cannot start the local subscription client | `500` | `error` beginning `Failed to subscribe: failed to start ZMQ client:`. |
| `/unregister` stops the subscriber but cache cleanup fails | `500` | `error` beginning `Failed to unregister prefix context:`. |

For example, a string element in a `token_ids` array produces an
element-specific error:

```json
{
  "error": "token_ids element must be an integer",
  "reason": "invalid_type",
  "field": "token_ids",
  "index": 0
}
```
