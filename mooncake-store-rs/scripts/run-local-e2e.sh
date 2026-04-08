#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
BENCH_ITERS="${MC_STORE_RS_BENCH_ITERS:-512}"
UPSTREAM_DIR="${MOONCAKE_UPSTREAM_DIR:-/root/Mooncake-upstream-main}"

if ! redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
  redis-server \
    --port "${REDIS_PORT}" \
    --bind 127.0.0.1 \
    --daemonize yes \
    --save '' \
    --appendonly no
fi

export LD_LIBRARY_PATH="${UPSTREAM_DIR}/build-rust/mooncake-transfer-engine/tent/src:${UPSTREAM_DIR}/build-rust/mooncake-transfer-engine/src:${LD_LIBRARY_PATH:-}"
export MC_STORE_RS_REDIS_URL="${MC_STORE_RS_REDIS_URL:-redis://127.0.0.1:${REDIS_PORT}/0}"
export MC_STORE_RS_BENCH_ITERS="${BENCH_ITERS}"

source /root/.cargo/env
cd "${ROOT_DIR}"
cargo run -p mooncake-store-e2e
