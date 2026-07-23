# Conductor Cache-Aware P/D Proxy Example

This example preserves the request flow of Mooncake's vLLM v1 prefill/decode
proxy and replaces only prefill target selection. At startup the proxy
registers every configured vLLM KV-event publisher with Conductor. For each
request it calls `/tokenize`, queries Conductor, and routes prefill to the
routable `instance_id` with the largest positive `longest_matched` value.

Decode selection remains round robin. Positive cache-hit ties rotate among the
tied instances. Tokenization failures, Conductor query failures, malformed or
empty results, and all-zero hits use the proxy's independent prefill
round-robin fallback.

## Files

- `cache_aware_disagg_proxy.py`: standalone FastAPI proxy.
- `cache_aware_proxy_config.json`: proxy routing and Conductor registration
  example.
- `../tests/cache_aware_proxy/`: CPU-only configuration, client, scheduler,
  and endpoint tests.

## Install

Build Conductor as described in the main
[Conductor usage guide](../../docs/source/design/conductor/usage.md). Install
the proxy's Python dependencies:

```bash
python3 -m pip install fastapi httpx uvicorn
```

The proxy does not load a tokenizer model. It uses the root-level `/tokenize`
route exposed by a configured prefill instance.

## Configure

The proxy accepts exactly one configuration source:

- JSON mode uses `--config` with `cache_aware_proxy_config.json`.
- CLI mode keeps the existing proxy's prefiller/decoder host and port flags and
  adds instance IDs, KV-event registrations, Conductor connection settings, and
  shared Prefill settings.

Do not combine `--config` with CLI topology, connection, or shared Prefill
flags.
`--host`, `--port`, and `--log-level` configure the proxy process itself and
are valid in either mode. Both configuration modes produce the same validated
`ProxyConfig` before the application starts.

For JSON mode, edit `cache_aware_proxy_config.json` before starting the proxy.
The file uses this ownership layout:

```text
conductor
prefill
  config
  instances
decode
  instances
```

- `conductor` contains only the Conductor HTTP `address` and the query and
  registration timeouts.
- `prefill.config` contains the model, block size, tenant, LoRA name, and hash
  profile shared by every Prefill candidate.
- `prefill.instances` contains the Prefill HTTP targets and the KV-event
  registrations that the proxy sends to Conductor.
- `decode.instances` contains the Decode HTTP targets. Decode selection remains
  round robin and has no Conductor registration in this example.

The single `prefill.config` applies to every entry in `prefill.instances`, so
the scheduler compares cache locality only among instances in that shared
model/cache context.

Each prefill entry has two separate kinds of address:

- `http_endpoint` is the vLLM HTTP origin used by the proxy. It must not include
  `/v1`; the proxy calls `/tokenize` and `/v1/completions` or
  `/v1/chat/completions` explicitly.
- `registrations[].endpoint` is the connectable ZeroMQ KV-event address sent to
  Conductor's `POST /register`. Do not use `*` or `0.0.0.0` here.

`instance_id` joins the two control planes:

```text
proxy:     instance_id -> vLLM HTTP endpoint
Conductor: instance_id + dp_rank -> KV-event endpoint and cache state
```

The following values must match the vLLM deployment:

- request `model` and configured `modelname`;
- `block_size`;
- `tenant_id` and `lora_name`;
- `hash_profile`, especially the exact `PYTHONHASHSEED` string.

Every compatible vLLM process must use the same explicit seed. For example:

```bash
export PYTHONHASHSEED=0
```

For data-parallel vLLM, configure one `registrations` entry per DP rank. vLLM
offsets a TCP KV-event publisher port by rank, so list the actual connectable
port for each rank. All registrations for one routable engine use the same
`instance_id`.

CLI mode expresses the same mapping as follows:

- prefiller hosts, ports, and instance IDs correspond by position;
- repeat `--prefiller-registration INSTANCE_ID DP_RANK EVENT_ENDPOINT` once
  per publisher rank, including multiple ranks for one instance when needed;
- `--prefiller-registration` may reference only an ID listed by
  `--prefiller-instance-ids`;
- the hash strategy, algorithm, and projection are fixed to `vllm_v1`,
  `sha256_cbor`, and `low64_be`; supply the deployment-specific seed with
  `--python-hash-seed`.

The example intentionally does not configure replay. It omits
`replay_endpoint` from every registration request, and vLLM KV-event
configuration can omit it as well. Conductor's `/services` response still
serializes `ReplayEndpoint` as an empty string; that output does not mean the
proxy submitted the field.

## Start The Services

Use this order so Conductor subscribes before the proxy sends cache-producing
traffic. Events emitted before registration are not reconstructed.

### 1. Start Conductor

From the repository root:

```bash
CONDUCTOR_CONFIG_PATH=/dev/stdin \
  ./build/mooncake-conductor/mooncake_conductor <<'JSON'
{
  "http_server_port": 13333,
  "kvevent_instance": {}
}
JSON
```

`http_server_port` selects the Conductor API port. The empty
`kvevent_instance` is intentional: this example does not preload static
subscriptions because the proxy calls `POST /register` for every configured
prefill publisher during startup. The configuration is supplied inline, so no
separate empty JSON file is needed.

### 2. Start Prefill And Decode Instances

Start the Mooncake vLLM P/D deployment using the repository's current vLLM
integration instructions. Each prefill must enable prefix caching and a live
ZeroMQ KV-event publisher.

Conductor requires the current map-encoded vLLM KV-event schema. Use vLLM main
at or after commit `5b3807e862fe70f51139ac518a5dd361e57de2e5` (PR #42892,
2026-06-09), or a build with an equivalent backport. Earlier array-encoded
events can register successfully but are rejected during event decoding, so
queries remain at zero hits.

For the first sample prefill, the relevant flags are:

```bash
export PYTHONHASHSEED=0

vllm serve Qwen/Qwen2.5-7B-Instruct \
  --served-model-name Qwen/Qwen2.5-7B-Instruct \
  --port 8100 \
  --enable-prefix-caching \
  --prefix-caching-hash-algo sha256_cbor \
  --block-size 16 \
  --kv-events-config \
  '{"enable_kv_cache_events":true,"publisher":"zmq","endpoint":"tcp://*:5557"}' \
  --kv-transfer-config \
  '{"kv_connector":"MooncakeConnectorStoreV1","kv_role":"kv_producer"}'
```

The sample's second prefill uses HTTP port `8101` and KV-event port `5559`.
If both prefills run on one host, give their Mooncake connectors distinct
`VLLM_MOONCAKE_BOOTSTRAP_PORT` values, for example `8998` and `8999`, and
assign each process an available accelerator. Processes on different hosts can
reuse the same bootstrap port.

Start one or more decode instances with the matching model and Mooncake
consumer-side KV transfer configuration; the sample uses HTTP port `8200`.
Keep external inference traffic paused until proxy startup completes.

### 3. Start The Cache-Aware Proxy

Choose one of the following modes.

#### JSON configuration

```bash
python3 mooncake-conductor/example/cache_aware_disagg_proxy.py \
  --config mooncake-conductor/example/cache_aware_proxy_config.json \
  --host 127.0.0.1 \
  --port 8000 \
  --log-level INFO
```

#### Direct CLI configuration

```bash
python3 mooncake-conductor/example/cache_aware_disagg_proxy.py \
  --prefiller-hosts 127.0.0.1 127.0.0.1 \
  --prefiller-ports 8100 8101 \
  --prefiller-instance-ids prefill-a prefill-b \
  --prefiller-registration prefill-a 0 tcp://127.0.0.1:5557 \
  --prefiller-registration prefill-b 0 tcp://127.0.0.1:5559 \
  --decoder-hosts 127.0.0.1 \
  --decoder-ports 8200 \
  --conductor-address http://127.0.0.1:13333 \
  --modelname Qwen/Qwen2.5-7B-Instruct \
  --block-size 16 \
  --tenant-id default \
  --lora-name '' \
  --python-hash-seed 0 \
  --query-timeout-seconds 1.0 \
  --registration-timeout-seconds 5.0 \
  --host 127.0.0.1 \
  --port 8000 \
  --log-level INFO
```

Startup is fail-fast. The server does not begin accepting requests unless all
configured DP-rank registrations succeed. Repeating an identical registration
is accepted by Conductor's idempotent API.

## Verify Registration

List the active event subscriptions:

```bash
curl -sS http://127.0.0.1:13333/services
```

The response must contain every configured `InstanceID`, `DPRank`, `Endpoint`,
model context, and resolved hash profile. `/services` proves that Conductor
started the local subscribers; it does not prove that any cache event has
arrived.

Inspect the instance/rank grouping:

```bash
curl -sS http://127.0.0.1:13333/global_view
```

The expected context must show `prefill-a` and `prefill-b` with their configured
ranks before traffic begins.

## Send A Request

```bash
curl -N http://127.0.0.1:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "Qwen/Qwen2.5-7B-Instruct",
    "messages": [{"role": "user", "content": "Explain KV cache locality."}],
    "max_tokens": 64,
    "stream": true
  }'
```

A positive selection is logged with the query candidates and selected target:

```text
request_id=... cache_candidates={'prefill-a': 16, 'prefill-b': 48} selected_instance=prefill-b longest_matched=48
```

A fallback states its reason and selected round-robin target:

```text
request_id=... cache_aware_fallback reason=all_cache_hits_zero selected_instance=prefill-a
```

## Diagnose Empty Or Zero Hits

Tokenize a representative prompt through one prefill, then send those token IDs
to Conductor directly:

```bash
curl -sS -X POST http://127.0.0.1:13333/query \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "Qwen/Qwen2.5-7B-Instruct",
    "block_size": 16,
    "tenant_id": "default",
    "lora_name": "",
    "token_ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
  }'
```

If `instances` is empty or every `longest_matched` is zero, compare the request
model, block size, tenant, LoRA name, cache salt, `PYTHONHASHSEED`, and event
publisher addresses with the proxy configuration. Only complete token blocks
are queried. Because this example is live-only, caches created before
registration do not appear until new matching events arrive.

If `/services` and `/global_view` look correct but hits remain zero after new
cache-producing requests, confirm that the vLLM build emits the map-encoded
KV-event schema described above. Released builds that still emit positional
array events are not compatible with the current Conductor decoder.

## Stop, Replace, Or Roll Back

Stopping or restarting the proxy does not call `/unregister`. A registration
may be shared by several proxy processes and Conductor cannot tell which proxy
first created an idempotent registration.

Unregister a rank only when its publisher is removed or replaced:

```bash
curl -sS -X POST http://127.0.0.1:13333/unregister \
  -H 'Content-Type: application/json' \
  -d '{"instance_id":"prefill-a","tenant_id":"default","dp_rank":0}'
```

Repeat the command for every rank being removed. Before registering a new
publisher endpoint under the same service key, unregister the old one.

To roll back scheduling behavior, stop this example and run the existing
round-robin proxy against the same HTTP instances:

```bash
python3 -m mooncake.vllm_v1_proxy_server \
  --prefiller-hosts 127.0.0.1 127.0.0.1 \
  --prefiller-ports 8100 8101 \
  --decoder-hosts 127.0.0.1 \
  --decoder-ports 8200
```

Publisher registrations can remain active while only the proxy implementation
changes.

## Run The CPU-Only Tests

```bash
python3 -m unittest discover \
  -s mooncake-conductor/tests/cache_aware_proxy \
  -p 'test_*.py' \
  -v
```

The tests use in-process FastAPI and HTTPX transports. They do not require a
GPU, RDMA, vLLM model load, or live Conductor process.
