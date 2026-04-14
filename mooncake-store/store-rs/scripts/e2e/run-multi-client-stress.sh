#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
UPSTREAM_DIR="${MOONCAKE_UPSTREAM_DIR:-${ROOT_DIR}/third_party/Mooncake}"
UPSTREAM_BUILD_DIR="${MOONCAKE_UPSTREAM_BUILD_DIR:-${UPSTREAM_DIR}/build-rust}"
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
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -d "${UPSTREAM_DIR}" ]]; then
  echo "Mooncake upstream submodule missing at ${UPSTREAM_DIR}" >&2
  echo "Run: git submodule update --init --recursive" >&2
  exit 1
fi

if ! redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
  redis-server \
    --port "${REDIS_PORT}" \
    --bind 127.0.0.1 \
    --daemonize yes \
    --save '' \
    --appendonly no
fi

export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${LD_LIBRARY_PATH:-}"
export MC_STORE_RS_REDIS_URL="${MC_STORE_RS_REDIS_URL:-redis://127.0.0.1:${REDIS_PORT}/0}"
export MC_STORE_RS_REDIS_PORT="${REDIS_PORT}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${ROOT_DIR}/python${PYTHONPATH:+:${PYTHONPATH}}"

if ! command -v cargo >/dev/null 2>&1; then
  CARGO_ENV="${CARGO_HOME:-${HOME}/.cargo}/env"
  if [[ -f "${CARGO_ENV}" ]]; then
    # shellcheck disable=SC1090
    source "${CARGO_ENV}"
  else
    echo "cargo not found in PATH and ${CARGO_ENV} is missing" >&2
    exit 1
  fi
fi

mkdir -p "${LOG_DIR}"

cd "${ROOT_DIR}"
cargo build --release -p mooncake-store-py
python3 "${ROOT_DIR}/scripts/e2e/python_multi_client_stress.py" | tee "${LOG_FILE}"

echo
echo "stress log: ${LOG_FILE}"
