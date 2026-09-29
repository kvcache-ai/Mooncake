#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR=${MOONCAKE_STORE_RS_DIR:-}
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../lib/common.sh"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
LOG_DIR="${MC_STORE_RS_STRESS_LOG_DIR:-${ROOT_DIR}/target/stress}"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="${MC_STORE_RS_STRESS_LOG_FILE:-${LOG_DIR}/multi-client-stress-${STAMP}.log}"

usage() {
  cat <<'EOF'
Usage: scripts/e2e/run-multi-client-stress.sh

Run the Python compatibility-layer multi-process stress benchmark.

Outputs:
  target/stress/multi-client-stress-<timestamp>.log

Important environment variables:
  MC_STORE_RS_REDIS_PORT                  Redis port, default 6380
  MC_STORE_RS_REDIS_URL                   Metadata Redis URL
  MC_STORE_RS_STRESS_STORAGE_CLIENTS      Number of storage clients, default 4
  MC_STORE_RS_STRESS_WRITER_CLIENTS       Number of concurrent Python benchmark worker processes, default 8
  MC_STORE_RS_STRESS_WRITER_STORAGE_BYTES Local storage bytes per rw worker, default 0
  MC_STORE_RS_STRESS_VALUE_SIZE           Payload size in bytes, default 4096
  MC_STORE_RS_STRESS_BATCH_SIZE           Batch width, default 32
  MC_STORE_RS_STRESS_SINGLE_ITERS         Single put/get iterations per writer, default 256
  MC_STORE_RS_STRESS_BATCH_ITERS          Batch put/get iterations per writer, default 128
  MC_STORE_RS_STRESS_WARMUP_ITERS         Warmup iterations per writer, default 16
  MC_STORE_RS_STRESS_PHASES               Comma-separated phases, default put,get,batch-put,batch-get
  MC_STORE_RS_STRESS_LOG_FILE             Explicit log path override
  MOONCAKE_PYTHON_BIN                     Interpreter from a virtualenv with
                                          the root wheel installed
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

mc_scripts_require_command redis-cli
mc_scripts_require_command redis-server
mc_scripts_setup_root_store_rs_python
mc_scripts_start_local_redis_if_needed "${REDIS_PORT}"
export MC_STORE_RS_REDIS_URL="${MC_STORE_RS_REDIS_URL:-redis://127.0.0.1:${REDIS_PORT}/0}"
export MC_STORE_RS_REDIS_PORT="${REDIS_PORT}"
export MC_STORE_RS_STRESS_STORAGE_CLIENTS="${MC_STORE_RS_STRESS_STORAGE_CLIENTS:-2}"
export MC_STORE_RS_STRESS_WRITER_CLIENTS="${MC_STORE_RS_STRESS_WRITER_CLIENTS:-4}"
export MC_STORE_RS_STRESS_BATCH_SIZE="${MC_STORE_RS_STRESS_BATCH_SIZE:-16}"
export MC_STORE_RS_STRESS_SINGLE_ITERS="${MC_STORE_RS_STRESS_SINGLE_ITERS:-128}"
export MC_STORE_RS_STRESS_BATCH_ITERS="${MC_STORE_RS_STRESS_BATCH_ITERS:-64}"
export PYTHONDONTWRITEBYTECODE=1

mkdir -p "${LOG_DIR}"

cd "${ROOT_DIR}"
"${PYTHON_BIN}" "${ROOT_DIR}/python/tests/store/rs/e2e/python_multi_client_stress.py" | tee "${LOG_FILE}"

echo
echo "stress log: ${LOG_FILE}"
