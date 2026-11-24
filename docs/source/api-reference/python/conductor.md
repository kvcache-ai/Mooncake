# Conductor Python API

## Overview

The Conductor Python API registers KV event publishers and queries prefix-cache
hits for request routing. `ConductorClient` connects to the Conductor RPC service;
cache events arrive separately through the registered ZMQ publishers.

For the HTTP interface and event format, see the
[Conductor Indexer API](../http/conductor-indexer.md).

## Installation

Use a Mooncake Python package built with `-DWITH_CONDUCTOR=ON`. The package must
include both `mooncake/conductor.py` and the native `mooncake._conductor` extension
for your Python version. Check the installed package with:

```bash
python -c 'from mooncake.conductor import ConductorClient'
```

If the native extension is absent, importing `mooncake.conductor` raises an
`ImportError` explaining that Conductor support is missing and suggesting a
package with Conductor support or a rebuild with `WITH_CONDUCTOR=ON`.

The client requires a running `mooncake_conductor` service. Connect to its
`rpc_server_port` (default `13334`), using `host:port` without an HTTP URL
scheme.

## Quick Start

Start a built Conductor binary with an empty service list and explicit ports:

```bash
cat > conductor.json <<'JSON'
{
  "http_server_port": 8080,
  "rpc_server_port": 50052,
  "kvevent_instance": {}
}
JSON
CONDUCTOR_CONFIG_PATH=conductor.json ./build/mooncake-conductor/mooncake_conductor
```

Connect, query, and release the client automatically:

```python
from mooncake.conductor import ConductorClient, OK

with ConductorClient() as client:
    rc = client.setup("127.0.0.1:50052")
    if rc != OK:
        raise RuntimeError(f"Conductor setup failed: {rc}")

    result = client.query(
        "my-model",
        block_size=16,
        token_ids=list(range(32)),
    )
    if result["ret"] != OK:
        raise RuntimeError(f"Conductor query failed: {result['ret']}")
    print(result["hits"])  # {} while no engine instances are registered
```

To populate the index, use `register()` with a running publisher's endpoint and
hash settings. Registration starts event consumption; it does not itself add
cached blocks. Allow events to reach Conductor before expecting query hits.

## API Reference

### Class: ConductorClient

#### Constructor

```python
ConductorClient()
```

Creates an unconnected client. Call `setup()` before making requests. The client
supports `with` statements: entering returns the client, and exiting calls
`close()`, including when the body raises an exception. Entering does not call
`setup()` automatically.

### Initialization Methods

#### setup()

```python
setup(conductor_addr, connect_timeout_ms=1000, request_timeout_ms=3000)
```

Connects to the Conductor RPC service. Each attempt replaces the previous
connection; a failed attempt leaves the client uninitialized. Can also reconnect
after `close()` or retry after a failed setup.

**Parameters:**

- `conductor_addr` (str): RPC address, for example `"127.0.0.1:50052"`.
- `connect_timeout_ms` (int): Connection timeout in milliseconds. Default `1000`.
- `request_timeout_ms` (int): Timeout for subsequent RPC requests in milliseconds. Default `3000`.

**Returns:**

- `int`: `OK` on success, or a negative error code. An address without a port
  separator returns `INVALID_PARAMS`; a connection failure returns `RPC_FAIL`.

#### close()

```python
close()
```

Releases the RPC connection. Repeated calls are safe. This does not unregister
publishers from the server. The client returns to the uninitialized state.

**Returns:**

- `int`: Always `OK` (`0`).

#### health_check()

```python
health_check()
```

Probes the service by listing registered services.

**Returns:**

- `int`: `HEALTH_OK` (`0`) if the probe succeeds, `HEALTH_NOT_INITIALIZED` (`1`)
  before successful setup, after `close()`, or after a failed setup attempt;
  `HEALTH_UNREACHABLE` (`2`) if an initialized client's probe fails.

### Cache Queries

#### query()

```python
query(
    model_name,
    *,
    lora_name="",
    block_size,
    tenant_id="default",
    token_ids,
    cache_salt="",
    instance_filter="",
)
```

Queries continuous prefix-cache matches for a prompt. All arguments after
`model_name` are keyword-only. Block handling follows the registered hash
strategy: `vllm_v1` considers complete blocks, while `sglang` and
`sglang_bigram` also consider a final partial block. For `sglang_bigram`,
counts measure logical KV positions from adjacent token pairs, so a query with
`n` tokens can match at most `max(n - 1, 0)` positions.

**Parameters:**

- `model_name` (str): Nonempty model name matching the registered context.
- `lora_name` (str): LoRA adapter name; `""` selects the base model.
- `block_size` (int): Positive number of tokens per block, matching the registration.
- `tenant_id` (str): Tenant to query. Defaults to `"default"`; an empty string is also normalized to `"default"`.
- `token_ids` (list[int]): Prompt token IDs, representable as signed 32-bit integers.
- `cache_salt` (str): Hash namespace salt matching the publisher's cache hashing. Default `""` means no salt.
- `instance_filter` (str): Restrict results to one instance ID. Default `""` queries all instances in the context.

**Returns:**

- `dict`: `{"ret": int, "hits": dict}`. `ret` is `OK` on success. `hits` maps
  instance IDs directly to the following fields (there is no outer tenant key):

| Field | Type | Description |
|-------|------|-------------|
| `longest_matched` | int | Longest matched prefix in tokens across ranks and storage tiers. |
| `dp` | dict[str, int] | Device-memory prefix tokens by data-parallel rank (the rank's `device` count). |
| `rank_matches` | dict[str, dict] | Per-rank prefix token counts under `device`, `cpu_local`, `cpu_share`, and `disk`. |
| `device` | int | Instance-level prefix token count in inference-engine device memory, such as GPU memory. |
| `cpu_local` | int | Prefix tokens accessible using device and local CPU memory. |
| `cpu_share` | int | Prefix tokens accessible using device, local CPU, and shared CPU memory. |
| `disk` | int | Prefix tokens accessible using all preceding tiers plus disk. |

Tier counts are cumulative: `device <= cpu_local <= cpu_share <= disk`. Each
instance-level tier count is the maximum over its DP ranks, and
`longest_matched` equals `disk`. Do not add tier counts together.

The `device` field is used by both Python and HTTP/msgpack responses, at instance
level and inside `rank_matches`.

Rank keys in `dp` and `rank_matches` are decimal strings, such as `"0"`, matching
the HTTP/msgpack response. Use `str(dp_rank)` to look up a rank. Counts are
integers in tokens, not bytes or blocks. On failure, `ret` is negative and `hits`
is `{}`. An empty `hits` with `ret == OK` is a successful query with no matching
instances. Registered instances can appear with all counts set to zero when no
cached prefix matches; a nonempty `hits` alone does not indicate a cache hit.

### Publisher Registration

#### register()

```python
register(config)
```

Registers one publisher subscription. Use Python field names such as
`model_name` and `publisher_type`; HTTP registration names such as `modelname`
and `type` are not accepted.

**Parameters:**

- `config` (dict[str, Any]): Publisher configuration with these fields:

| Field | Type | Required / default | Description |
|-------|------|--------------------|-------------|
| `instance_id` | str | Required for vLLM/SGLang | Routing instance identity. |
| `endpoint` | str | Required | Nonempty ZMQ publisher endpoint, such as `"tcp://127.0.0.1:5557"`. |
| `model_name` | str | Required | Nonempty model name. |
| `block_size` | int | Required | Positive tokens per block. |
| `hash_profile` | dict[str, str] | Required | Hash recipe described below. |
| `replay_endpoint` | str | `""` | Optional ZMQ replay endpoint for missed events. |
| `publisher_type` | str | `"vLLM"` | One of `"vLLM"`, `"SGLang"`, or `"Mooncake"` (case-sensitive). |
| `lora_name` | str | `""` | LoRA adapter name; empty selects the base model. |
| `tenant_id` | str | `"default"` | Empty or omitted values normalize to `"default"`. |
| `dp_rank` | int | `0` | Nonnegative data-parallel rank. |
| `cache_group` | int or None | `None` | Optional cache group; only `0` is currently supported. |

`hash_profile` accepts exactly four keys: `strategy`, `algorithm`,
`python_hash_seed`, and `index_projection`. Values must be strings. Supported
combinations are:

| `strategy` | `algorithm` | `index_projection` | `python_hash_seed` |
|------------|-------------|--------------------|--------------------|
| `vllm_v1` | `sha256` or `sha256_cbor` | `low64_be` | Publisher's seed as decimal text in `0..4294967295`, or `"random"`. |
| `sglang` or `sglang_bigram` | `sha256_raw` | `first64_be` | Optional; normalized to `"0"`. |

The recipe must match the publisher. Conductor derives `root_digest` on the
server; do not pass it to `register()`, even though it appears in read responses.

For example, for a vLLM publisher configured with the following hash recipe,
run this inside the `with` block above, after successful `setup()` and before
the client is closed:

```python
rc = client.register({
    "instance_id": "vllm-0",
    "endpoint": "tcp://127.0.0.1:5557",
    "publisher_type": "vLLM",
    "model_name": "my-model",
    "block_size": 16,
    "dp_rank": 0,
    "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be",
    },
})
if rc != OK:
    raise RuntimeError(f"Publisher registration failed: {rc}")
```

**Returns:**

- `int`: `OK` on success, or a negative error code. Missing required values,
  unsupported hash recipes, and invalid configuration values return
  `INVALID_PARAMS` when validated by the server.

**Raises:**

- `ValueError`: Configuration is not a dictionary, contains unknown or non-string
  keys, has invalid field types or overflowing integers, or specifies an unknown
  publisher type. The same key/type validation applies to `hash_profile`.
  Boolean values are not accepted for integer fields.

#### unregister()

```python
unregister(instance_id, tenant_id="default", dp_rank=0)
```

Stops consuming events for the subscription identified by
`(instance_id, tenant_id, dp_rank)`.

**Parameters:**

- `instance_id` (str): Registered instance ID.
- `tenant_id` (str): Tenant identity. Empty values normalize to `"default"`.
- `dp_rank` (int): Data-parallel rank to unregister. Default `0`.

**Returns:**

- `int`: `OK` on success, `SERVICE_NOT_FOUND` if the subscription does not
  exist, or another negative error code on failure.

### Service and Index Information

#### get_global_view()

```python
get_global_view()
```

Returns a snapshot of indexed model contexts.

**Returns:**

- `dict`: `{"ret": int, "context_count": int, "contexts": list}`.
  Each context contains:

| Field | Type | Description |
|-------|------|-------------|
| `model_name` | str | Model identifier. |
| `lora_name` | str | LoRA adapter name. |
| `block_size` | int | Tokens per block. |
| `tenant_id` | str | Tenant identity. |
| `prefix_count` | int | Number of indexed prefixes in the context. |
| `hash_profile` | dict[str, str] | Resolved recipe: `strategy`, `algorithm`, `python_hash_seed`, `root_digest`, and `index_projection`. |
| `instances` | dict[str, set[int]] | Instance IDs mapped to registered DP ranks. |

On failure, `ret` is negative, `context_count` is `0`, and `contexts` is `[]`.

#### list_services()

```python
list_services()
```

Lists active publisher configurations.

**Returns:**

- `dict`: `{"ret": int, "count": int, "services": list}`. Each service uses
  these case-sensitive export keys:

| Key | Type | Corresponding registration field |
|-----|------|----------------------------------|
| `Endpoint` | str | `endpoint` |
| `ReplayEndpoint` | str | `replay_endpoint` |
| `Type` | str | `publisher_type` |
| `ModelName` | str | `model_name` |
| `LoraName` | str | `lora_name` |
| `TenantID` | str | `tenant_id` |
| `InstanceID` | str | `instance_id` |
| `BlockSize` | int | `block_size` |
| `DPRank` | int | `dp_rank` |
| `CacheGroup` | int or None | `cache_group` |
| `HashProfile` | dict[str, str] | Resolved `hash_profile`, including server-derived `root_digest`. |

On failure, `ret` is negative, `count` is `0`, and `services` is `[]`.
Service entries cannot be passed directly to `register()` because the field
names and resolved hash profile differ from registration input.

## Error Codes

The following constants are exported from `mooncake.conductor`. Lifecycle and
mutating calls return integer codes; read calls place the code in `ret`.
`health_check()` uses the separate health status values documented above.

| Constant | Value | Meaning |
|----------|-------|---------|
| `OK` | `0` | Success. |
| `INTERNAL_ERROR` | `-1` | Internal service failure. |
| `INVALID_PARAMS` | `-600` | Invalid address or request/configuration values. |
| `RPC_FAIL` | `-900` | RPC connection or transport failure. |
| `RPC_TIMEOUT` | `-901` | RPC request timed out. |
| `CONDUCTOR_UNAVAILABLE` | `-2000` | Client is not connected, or the service is unavailable. |
| `SERVICE_NOT_FOUND` | `-2001` | Subscription to unregister was not found. |

Requests before successful `setup()` or after `close()` return
`CONDUCTOR_UNAVAILABLE`. Check return codes explicitly; a `with` statement only
manages cleanup and does not turn negative return codes into exceptions.
