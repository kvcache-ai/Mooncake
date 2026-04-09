#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
BENCH_ITERS="${MC_STORE_RS_BENCH_ITERS:-512}"
VALUE_SIZE="${MC_STORE_RS_VALUE_SIZE:-4096}"
UPSTREAM_DIR="${MOONCAKE_UPSTREAM_DIR:-${ROOT_DIR}/third_party/Mooncake}"
UPSTREAM_BUILD_DIR="${MOONCAKE_UPSTREAM_BUILD_DIR:-${UPSTREAM_DIR}/build-rust}"

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
export MC_STORE_RS_BENCH_ITERS="${BENCH_ITERS}"
export MC_STORE_RS_VALUE_SIZE="${VALUE_SIZE}"

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

cd "${ROOT_DIR}"
cargo run --release -p mooncake-store-e2e
