#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
# shellcheck disable=SC1091
source "${ROOT_DIR}/scripts/lib/common.sh"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
BENCH_ITERS="${MC_STORE_RS_BENCH_ITERS:-64}"
VALUE_SIZE="${MC_STORE_RS_VALUE_SIZE:-4096}"
MAX_ATTEMPTS="${MC_STORE_RS_LOCAL_E2E_ATTEMPTS:-3}"
RETRY_DELAY_S="${MC_STORE_RS_LOCAL_E2E_RETRY_DELAY_S:-1}"
mc_scripts_require_command redis-cli
mc_scripts_require_command redis-server
mc_scripts_require_command cargo
UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${ROOT_DIR}")
mc_scripts_setup_upstream_runtime_env "${ROOT_DIR}" none "${UPSTREAM_BUILD_DIR}"
mc_scripts_start_local_redis_if_needed "${REDIS_PORT}"
export MC_STORE_RS_REDIS_URL="${MC_STORE_RS_REDIS_URL:-redis://127.0.0.1:${REDIS_PORT}/0}"
export MC_STORE_RS_REDIS_PORT="${REDIS_PORT}"
export MC_STORE_RS_BENCH_ITERS="${BENCH_ITERS}"
export MC_STORE_RS_VALUE_SIZE="${VALUE_SIZE}"

cd "${ROOT_DIR}"
attempt=1
while true; do
  run_log=$(mktemp)
  if cargo run --release -p mooncake-store-e2e 2>&1 | tee "${run_log}"; then
    rm -f "${run_log}"
    break
  fi
  rc=${PIPESTATUS[0]}
  if (( attempt >= MAX_ATTEMPTS )) || ! grep -Eqi 'address in use|address in used|Interrupted system call' "${run_log}"; then
    rm -f "${run_log}"
    exit "${rc}"
  fi
  echo "transient local e2e startup failure; retrying (${attempt}/${MAX_ATTEMPTS})" >&2
  rm -f "${run_log}"
  attempt=$((attempt + 1))
  sleep "${RETRY_DELAY_S}"
done
