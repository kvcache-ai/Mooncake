# Run Mooncake Conductor

[中文](../../zh/design/conductor/usage.md)

This guide takes a Conductor deployment from a source checkout to a working
HTTP query. It covers static and dynamic event-source registration, a safe
startup order for vLLM and Mooncake, and exact cleanup commands. The examples
use the current C++ service and only fields accepted by the current parsers.

## Before you start

Install the repository build dependencies described in the
[build guide](../../getting_started/build.md). The Conductor component also
requires ZeroMQ, MessagePack for C++, OpenSSL, JsonCpp, glog, and
yalantinglibs, which CMake checks during configuration.

You also need:

- one key-value (KV) Event endpoint for each vLLM data-parallel (DP) rank you
  register;
- optionally, a Mooncake Master KV Event endpoint for shared CPU or Disk
  information;
- `curl` for the checks below; and
- one explicit `PYTHONHASHSEED` value shared by every compatible vLLM
  process.

The addresses, model name, and seed below are examples. Replace them with
values from the same deployment before sending cache-producing traffic. Do not
leave `PYTHONHASHSEED` unset: vLLM then creates random root bytes that Conductor
cannot reproduce from registration.

## Build

Configure from the repository root and build only the Conductor target:

```bash
cmake -S . -B build -DWITH_CONDUCTOR=ON
cmake --build build --target mooncake_conductor
```

The resulting binary is
`build/mooncake-conductor/mooncake_conductor`. Confirm that the build produced
it:

```bash
test -x build/mooncake-conductor/mooncake_conductor
```

A zero exit status means the binary is ready to run.

## Configure

Conductor reads two environment variables:

| Variable | Default | When it matters |
|---|---|---|
| `CONDUCTOR_CONFIG_PATH` | `$HOME/.mooncake/conductor_config.json` | Selects the JSON file loaded at startup. A missing file starts Conductor with no static subscriptions and leaves the built-in HTTP port at `13333`. |
| `CONDUCTOR_LOG_LEVEL` | `INFO` | Sets `DEBUG`, `INFO`, `WARN`, or `ERROR`, case-insensitively. An empty or invalid value uses `INFO`. |

Always set `http_server_port` in a readable config file. If a config file is
loaded but omits this field, the current parser sets the port to `0` rather
than using `13333`.

The following complete static example registers two vLLM ranks for one engine
and one Mooncake shared pool. An empty `lora_name` selects the base model; a
non-empty value names a Low-Rank Adaptation (LoRA) adapter.

```json
{
  "http_server_port": 13333,
  "kvevent_instance": {
    "vllm-engine-a-rank-0": {
      "endpoint": "tcp://127.0.0.1:5557",
      "replay_endpoint": "tcp://127.0.0.1:5558",
      "type": "vLLM",
      "modelname": "test-model",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "engine-a",
      "block_size": 16,
      "dp_rank": 0,
      "cache_group": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be"
      }
    },
    "vllm-engine-a-rank-1": {
      "endpoint": "tcp://127.0.0.1:5567",
      "replay_endpoint": "tcp://127.0.0.1:5568",
      "type": "vLLM",
      "modelname": "test-model",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "engine-a",
      "block_size": 16,
      "dp_rank": 1,
      "cache_group": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be"
      }
    },
    "mooncake-shared-pool": {
      "endpoint": "tcp://127.0.0.1:6557",
      "replay_endpoint": "",
      "type": "Mooncake",
      "modelname": "test-model",
      "lora_name": "",
      "tenant_id": "default",
      "instance_id": "shared-pool",
      "block_size": 16,
      "dp_rank": 0,
      "cache_group": 0,
      "hash_profile": {
        "strategy": "vllm_v1",
        "algorithm": "sha256_cbor",
        "python_hash_seed": "0",
        "index_projection": "low64_be"
      }
    }
  }
}
```

`python_hash_seed` is the exact string in the `PYTHONHASHSEED` environment
variable on every compatible vLLM process. Conductor preserves that text,
canonical-CBOR encodes it as a text string, and calculates SHA-256 to derive the
root. For seed string `"0"`, the derived lowercase diagnostic is
`4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e`.
That digest is output in `/services` and `/global_view`; it is not registration
input or a universal default.

Use a quoted JSON string: `"0"`, `"00"`, and numeric JSON `0` are different
inputs. The explicit literal `random` is supported and derives a root from that
exact text, but an unset environment variable makes vLLM choose unregistered
random bytes and is unsupported. vLLM `--seed` controls model and sampling
randomness; it does not select the prefix-cache root. Request `cache_salt` is
sent only to `/query`, not registration. See [how query blocks are hashed](./conductor-architecture-design.md#how-token-blocks-become-lookup-values)
for the exact rule.

Launch every compatible vLLM process with the same explicit environment text
and canonical-CBOR prefix-hash algorithm. For the seed-zero examples:

```bash
PYTHONHASHSEED=0 vllm serve test-model \
  --enable-prefix-caching \
  --prefix-caching-hash-algo sha256_cbor
```

Start Conductor with an absolute config path and the desired log level:

```bash
export CONDUCTOR_CONFIG_PATH=/absolute/path/to/conductor_config.json
export CONDUCTOR_LOG_LEVEL=INFO
./build/mooncake-conductor/mooncake_conductor
```

Successful startup logs include `HTTP server listening port=13333` and the
static subscription success/failure counts. Static subscriptions start at the
same time, so the order of entries in the JSON file does not make vLLM start
before Mooncake.

## Start with vLLM only

Dynamic registration gives explicit control over startup order. Start
Conductor with this alternative config when you do not want static
subscriptions:

```json
{
  "http_server_port": 13333,
  "kvevent_instance": {}
}
```

After starting the binary, register every vLLM rank. This first request
registers rank `0`:

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:5557",
    "replay_endpoint": "tcp://127.0.0.1:5558",
    "type": "vLLM",
    "modelname": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "instance_id": "engine-a",
    "block_size": 16,
    "dp_rank": 0,
    "cache_group": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

Register rank `1` with its own event endpoint and the same engine and hash
settings:

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:5567",
    "replay_endpoint": "tcp://127.0.0.1:5568",
    "type": "vLLM",
    "modelname": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "instance_id": "engine-a",
    "block_size": 16,
    "dp_rank": 1,
    "cache_group": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

Each successful request returns `registered successfully`. Confirm the engine
context before allowing cache-producing traffic:

```bash
curl -sS http://127.0.0.1:13333/global_view
```

The matching context must show `tenant_id` `default`, `model_name`
`test-model`, empty `lora_name`, `block_size` `16`, `python_hash_seed` `"0"`,
the derived root shown above, and `"engine-a":[0,1]` under `instances`. Also
check `/services`: it must contain both vLLM subscriptions with the same seed
and derived root. For a vLLM-only deployment, you can now start
cache-producing traffic.

## Add Mooncake

Keep cache-producing traffic paused while adding the shared pool. Enable and
start the Mooncake Master publisher as described in the
[Mooncake publisher guide](../kv-event/publisher-design.md), then register its
live endpoint:

```bash
curl -sS -X POST http://127.0.0.1:13333/register \
  -H 'Content-Type: application/json' \
  -d '{
    "endpoint": "tcp://127.0.0.1:6557",
    "replay_endpoint": "",
    "type": "Mooncake",
    "modelname": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "instance_id": "shared-pool",
    "block_size": 16,
    "dp_rank": 0,
    "cache_group": 0,
    "hash_profile": {
      "strategy": "vllm_v1",
      "algorithm": "sha256_cbor",
      "python_hash_seed": "0",
      "index_projection": "low64_be"
    }
  }'
```

Configure the Mooncake publisher so each event carries the same tenant, model,
LoRA name, and block size as the vLLM context. The `hash_profile` belongs to
the Mooncake registration and must match the profile already bound to that
event context. Use the same context values in the registration as well so
`/services` is easy to compare with the publisher. Mooncake `instance_id` and
`dp_rank` identify the subscription for `/services` and `/unregister`; they do
not create a query instance or override event context. A `stored` event must
also carry an object key and a usable full connector hash; see the
[subscriber compatibility guide](../kv-event/subscriber-guide.md) for the
event checks.

Confirm that `/services` contains both vLLM ranks and the Mooncake subscription
before releasing traffic:

```bash
curl -sS http://127.0.0.1:13333/services
```

For static configuration, use the other safe choice: keep cache-producing
traffic paused until `/global_view` shows the expected vLLM context, engine,
ranks, configured seed, and derived root and `/services` shows every expected
subscription with that same resolved profile. Static subscriptions start
concurrently, regardless of JSON order.

These checks have a narrow meaning. `/services` proves that Conductor accepted
the registration and started its local subscription client; it does not prove
that the remote ZeroMQ publisher has sent an event. `/global_view` proves that
the vLLM context and ranks were registered; it does not recover Mooncake
objects cached before Conductor connected. The current Mooncake publisher does
not resend that earlier state.

## Check it works

Inspect the active subscriptions:

```bash
curl -sS http://127.0.0.1:13333/services
```

For the dynamic example, success means `count` is `3`, the two `vLLM` entries
have `InstanceID` `engine-a` with `DPRank` `0` and `1`, and the `Mooncake`
entry has `InstanceID` `shared-pool`. All three `HashProfile` values must
match, including `python_hash_seed` `"0"` and its derived `root_digest`.

Inspect the cache-sharing groups and registered inference ranks:

```bash
curl -sS http://127.0.0.1:13333/global_view
```

Success means one matching context shows `"engine-a":[0,1]`. Mooncake does not
appear as another inference instance. After live events arrive, `prefix_count`
can increase, but that count alone does not show which cache location supplied
each block.

## Query

Send token IDs produced by the tokenizer for the registered model. This
example contains one complete 16-token block:

```bash
curl -sS -X POST http://127.0.0.1:13333/query \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "test-model",
    "lora_name": "",
    "tenant_id": "default",
    "block_size": 16,
    "token_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  }'
```

A successful response has an `instances.engine-a` object with DP rank keys
`"0"` and `"1"`. Hit values may be zero until matching live events arrive.
Only complete blocks are considered. If the producer hashes requests with a
cache salt, add the matching string `cache_salt` to this query; do not add it
to registrations.

See the [HTTP API reference](./indexer-api-design.md#post-query) for the exact
response fields and the [architecture guide](./conductor-architecture-design.md#what-query-fields-mean)
for how Conductor combines GPU, CPU, and Disk availability.

## Unregister

Unregister the exact service key before replacing a publisher. The key is
formed from `instance_id`, normalized `tenant_id`, and `dp_rank`.

Remove the Mooncake subscription:

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"shared-pool","tenant_id":"default","dp_rank":0}'
```

Remove each vLLM rank separately:

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"engine-a","tenant_id":"default","dp_rank":1}'

curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"engine-a","tenant_id":"default","dp_rank":0}'
```

Each successful response contains `unregistered successfully` and the exact
removed service key, for example `engine-a|default|0`. Check `/services` to
confirm that the subscriptions are absent. Unregistering one vLLM rank removes
only that rank's GPU information; unregistering Mooncake removes only objects
reported by that Mooncake endpoint.

## Fix setup problems

| Symptom | Check | Action |
|---|---|---|
| HTTP requests cannot connect, or the log shows port `0` | Confirm that `CONDUCTOR_CONFIG_PATH` names the file you edited and that it contains numeric `http_server_port`. | Set an absolute config path, add an explicit nonzero port, and restart Conductor. |
| A source is absent from `/services` | Read the registration response and Conductor log for an unsupported type, nonzero `cache_group`, invalid hash profile, duplicate endpoint, or conflicting service key. | Use only `vLLM` or `Mooncake`, cache group `0` or omission, a unique endpoint, and the exact supported seed-based hash fields. |
| `/services` lists a source but hits remain zero | Check whether the publisher emitted events after Conductor connected. `/services` is not proof of event delivery. | Verify the publisher status, keep traffic paused during setup, and start registration before new cache-producing traffic. Earlier Mooncake events are not resent. |
| Mooncake events do not add CPU or Disk availability | Compare `/global_view` with the tenant, model, LoRA name, and block size carried by Mooncake events; compare the registered hash profiles; check that stored events include object and full connector-hash data. | Register vLLM first, make the event context and hash profile match, and correct the Mooncake publisher/key setup. |
| `/query` returns an empty `instances` object or shorter hits than expected | Compare `model`, `tenant_id`, `lora_name`, `block_size`, optional `instance_id`, token IDs, and `cache_salt` with the producer. Check for a trailing partial block. | Query the exact registered four-field group, use the producer's salt rule, and send enough tokens for complete blocks. |
| An HTTP request returns `400` | Read the JSON `reason`, `field`, and optional `index`; malformed register or unregister JSON instead returns plain text. | Remove unsupported fields and correct the named value using the [API field tables](./indexer-api-design.md). |
