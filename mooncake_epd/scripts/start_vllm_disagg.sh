#!/bin/bash
# 启动 vLLM + MooncakeConnector 的真实本机 PD 分离服务

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONFIG_DIR="${PROJECT_DIR}/config"

python "${PROJECT_DIR}/demo/vllm_integration.py" >/dev/null

echo "Use generated scripts under ${CONFIG_DIR}:"
echo "  bash ${CONFIG_DIR}/start_metadata.sh"
echo "  bash ${CONFIG_DIR}/start_master.sh"
echo "  bash ${CONFIG_DIR}/start_prefill.sh"
echo "  bash ${CONFIG_DIR}/start_decode.sh"
echo "  bash ${CONFIG_DIR}/start_proxy.sh"
echo ""
echo "Then verify with:"
echo "  python ${PROJECT_DIR}/scripts/check_vllm_disagg.py --request ${CONFIG_DIR}/test_request.json"
