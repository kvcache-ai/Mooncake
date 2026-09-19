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
python /data/songbinbin/Proj/Proj_LWX/mooncake_epd/scripts/vllm_disagg_proxy.py --prefiller-host 127.0.0.1 --prefiller-port 8100 --decoder-host 127.0.0.1 --decoder-port 8200 --layers-per-group 4 --group-delay-ms 0.0 --max-group-bytes 0 --warn-rho 0.85 --critical-rho 0.95 --max-backpressure-delay-ms 150.0 --transport-backend mooncake_engine_direct --port 8000
