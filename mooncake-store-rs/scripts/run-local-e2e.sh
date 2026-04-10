#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
BENCH_ITERS="${MC_STORE_RS_BENCH_ITERS:-512}"
VALUE_SIZE="${MC_STORE_RS_VALUE_SIZE:-4096}"

list_upstream_dirs() {
  local primary_worktree

  if [[ -n "${MOONCAKE_UPSTREAM_DIR:-}" ]]; then
    printf '%s\n' "${MOONCAKE_UPSTREAM_DIR}"
  fi
  printf '%s\n' "${ROOT_DIR}/third_party/Mooncake"

  if primary_worktree=$(git -C "${ROOT_DIR}" worktree list --porcelain 2>/dev/null | awk '/^worktree / { print substr($0, 10); exit }'); then
    if [[ -n "${primary_worktree}" && "${primary_worktree}" != "${ROOT_DIR}" ]]; then
      printf '%s\n' "${primary_worktree}/third_party/Mooncake"
    fi
  fi
}

resolve_upstream_build_dir() {
  local candidates=()
  local candidate
  local upstream_dir

  if [[ -n "${MOONCAKE_UPSTREAM_BUILD_DIR:-}" ]]; then
    candidates+=("${MOONCAKE_UPSTREAM_BUILD_DIR}")
  fi
  while IFS= read -r upstream_dir; do
    [[ -z "${upstream_dir}" ]] && continue
    candidates+=(
      "${upstream_dir}/build-rust"
      "${upstream_dir}/build-wheel-compat"
    )
  done < <(list_upstream_dirs)

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}/mooncake-transfer-engine/src/libtransfer_engine.so" ]] \
      && [[ -f "${candidate}/mooncake-transfer-engine/tent/src/libtent_shared.so" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  echo "unable to find Mooncake runtime libraries under any known Mooncake upstream tree" >&2
  echo "checked candidates:" >&2
  printf '  %s\n' "${candidates[@]}" >&2
  echo "set MOONCAKE_UPSTREAM_BUILD_DIR to a built upstream directory" >&2
  exit 1
}

UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
UPSTREAM_DIR=$(cd -- "${UPSTREAM_BUILD_DIR}/.." && pwd)
export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"

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
