## mooncake_connector_v1

This is a KV connector for vllm v1.

### Usage

Add proper "--kv-transfer-config" parameters to your vLLM command. See comments in `mooncake_connector_v1.py`.

For example, a whole demo could be:

#### Prefill Node (192.168.0.2)

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct --port 8010 --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer", "kv_connector_module_path":"mooncake.mooncake_connector_v1"}'
```

#### Decode Node (192.168.0.3)

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct --port 8020 --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_consumer", "kv_connector_module_path":"mooncake.mooncake_connector_v1"}'
```

#### Proxy
```bash
python {your_vllm_path}/tests/v1/kv_connector/nixl_integration/toy_proxy_server.py --prefiller-host 192.168.0.2 --prefiller-port 8010 --decoder-host 192.168.0.3 --decoder-port 8020
```

Now you can send requests to the proxy server on default port 8000.
