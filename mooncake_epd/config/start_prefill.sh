#!/bin/bash
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
export NO_PROXY=127.0.0.1,localhost
export PYTHONPATH=/data/songbinbin/Proj/Proj_LWX:${PYTHONPATH:-}
source /data/songbinbin/Proj/Proj_LWX/venv_mooncake/bin/activate
export MOONCAKE_CONFIG_PATH=/data/songbinbin/Proj/Proj_LWX/mooncake_epd/config/mooncake.json
export MOONCAKE_MASTER=127.0.0.1:50061
export MOONCAKE_TE_META_DATA_SERVER=http://127.0.0.1:8090/metadata
export MOONCAKE_PROTOCOL=tcp
export MOONCAKE_LOCAL_HOSTNAME=127.0.0.1
export MOONCAKE_GLOBAL_SEGMENT_SIZE=1073741824
export MOONCAKE_LOCAL_BUFFER_SIZE=268435456
export VLLM_MOONCAKE_BOOTSTRAP_PORT=58998
export OPENAI_API_KEY=sk-local
MODEL_PATH="${MOONCAKE_EPD_MODEL:-/data01/LWX/Qwen3-VL-8B-Instruct}"
CUDA_VISIBLE_DEVICES=3 vllm serve "$MODEL_PATH" --port 8100 --tensor-parallel-size 1 --max-model-len 4096 --gpu-memory-utilization 0.65 --kv-transfer-config '{"kv_connector":"MooncakeConnector","kv_role":"kv_producer","engine_id":"epd-prefill","kv_connector_module_path":"mooncake_epd.core.control.vllm_mooncake_connector","kv_connector_extra_config":{"mooncake_protocol":"tcp","num_workers":4,"layered_kv_transfer":true,"layers_per_group":4,"group_delay_ms":0.0,"max_group_bytes":0,"transport_backend":"mooncake_engine_direct"}}'
