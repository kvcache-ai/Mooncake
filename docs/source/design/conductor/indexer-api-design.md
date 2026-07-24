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

All `POST` bodies are JSON objects. Unknown fields are rejected. Successful
responses use `application/json`; JSON object key order is not part of the
contract.

Most validation failures return status `400` with an
`application/json` object:

```json
{
  "error": "unsupported request field: root_digest",
  "reason": "unknown_field",
  "field": "root_digest"
}
```

`field` is present when one field caused the error. `index` is also present
when one array element caused it. The [error formats](#understand-errors)
section lists the cases that return plain text instead.

## `POST /register`

This endpoint starts one event subscription. Register each vLLM data-parallel
(DP) rank with a separate endpoint. A Mooncake subscription supplies shared CPU or Disk
information but does not create an inference-instance row in `/query`.

### Request fields

| Field | Required | Accepted value and when it matters |
|---|---|---|
| `endpoint` | Yes | Non-empty ZeroMQ live-publisher endpoint. It must not already belong to another active registration. |
| `type` | Yes | Exactly `vLLM` or `Mooncake`. It decides which event message format Conductor reads. |
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

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
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
      "index_projection": "low64_be"
    }
  }'
```

Status `200` returns:

```json
{
  "status": "registered successfully",
  "instance_id": "engine-a"
}
```

Submitting the exact same active registration again returns the same success
without starting another subscriber. A conflicting service key, endpoint, or
hash profile returns a JSON `400` with `reason` `invalid_registration`.
Failure to start the local subscription client returns plain-text `500`.

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

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"engine-a","dp_rank":0}'
```

Status `200` returns the exact service key that was removed:

```json
{
  "status": "unregistered successfully",
  "removed_instances": [
    "engine-a|default|0"
  ]
}
```

An unknown service key returns plain-text status `404`. A cleanup failure after
the subscriber stops returns plain-text status `500`. Conductor keeps that
service key and endpoint reserved, so neither can be registered again. Retry
the same `/unregister` request until cleanup succeeds.

## `POST /query`

This endpoint hashes complete token blocks with the profile already registered
for the requested tenant, model, LoRA name, and block size. The request cannot
override the strategy, algorithm, `python_hash_seed`, derived root digest, or
final-eight-byte lookup rule.

### Request fields

| Field | Required | Accepted value and when it matters |
|---|---|---|
| `model` | Yes | Non-empty model name. It must match registration `modelname`. |
| `block_size` | Yes | Positive integer. Only complete groups of this many tokens are hashed. |
| `token_ids` | Yes | JSON array of signed 32-bit integers. An empty array is valid and reports zero hits for compatible registered ranks. |
| `tenant_id` | No | String. Omitted or `""` becomes `"default"`. |
| `lora_name` | No | String. Defaults to `""` for the base model. |
| `cache_salt` | No | String, `null`, or omitted. `null`, `""`, and omission all select the no-salt hash path; a non-empty value must match the producer. |
| `instance_id` | No | String filter. A matching registered instance is returned alone; an unknown value returns an empty `instances` object. |

No hash-profile override fields are accepted. A missing cache-sharing group
also returns status `200` with an empty `instances` object and does not create
state.

### Minimal request

```bash
curl -sS -X POST http://127.0.0.1:13333/query \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test-model",
    "block_size": 16,
    "token_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  }'
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

```bash
curl -sS http://127.0.0.1:13333/global_view
```

After registering `engine-a` ranks `0` and `1`, before cache events arrive, a
one-context response has this shape:

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

```bash
curl -sS http://127.0.0.1:13333/services
```

After the minimal register example, status `200` returns:

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

Conductor deliberately uses both JSON validation errors and plain-text
operational errors:

| Situation | Status | Content type and body |
|---|---|---|
| Field validation for any `POST` endpoint | `400` | `application/json` with `error`, `reason`, and applicable `field` or `index`. |
| Malformed `/query` JSON or a non-object body | `400` | `application/json` with `{"error":"Invalid JSON object","reason":"invalid_json"}`. |
| Malformed `/register` or `/unregister` JSON, or a non-object body | `400` | `text/plain; charset=utf-8` with `Invalid JSON\n`. |
| `/unregister` service key not found | `404` | `text/plain; charset=utf-8`, for example `service not found: engine-a\|default\|0\n`. |
| `GET` on `/register`, `/unregister`, or `/query`; `POST` on `/global_view` or `/services` | `405` | `text/plain; charset=utf-8` with `Method not allowed\n`. |
| `/register` cannot start the local subscription client | `500` | `text/plain; charset=utf-8` beginning `Failed to subscribe: failed to start ZMQ client:`. |
| `/unregister` stops the subscriber but cache cleanup fails | `500` | `text/plain; charset=utf-8` beginning `Failed to unregister prefix context:`. |

For example, a string in `token_ids` produces an element-specific JSON error:

```json
{
  "error": "token_ids element must be a JSON integer",
  "reason": "invalid_type",
  "field": "token_ids",
  "index": 0
}
```

Conductor-generated JSON responses end with a newline except `/services`,
whose compact JSON body has no trailing newline. Plain-text errors shown with
`\n` above include that trailing newline.
