#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}

usage() {
  cat <<'EOF'
Usage: scripts/test-client-hot-upgrade-cli.sh

Run the focused unit tests that verify mooncake-store-client startup flags for
hot standby and hot upgrade orchestration.

Environment:
  MOONCAKE_UPSTREAM_DIR       Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR Explicit upstream build directory override
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_cargo() {
  if command -v cargo >/dev/null 2>&1; then
    return 0
  fi

  local cargo_env="${CARGO_HOME:-${HOME}/.cargo}/env"
  if [[ -f "${cargo_env}" ]]; then
    # shellcheck disable=SC1090
    source "${cargo_env}"
    return 0
  fi

  echo "cargo not found in PATH and ${cargo_env} is missing" >&2
  exit 1
}

resolve_upstream_build_dir() {
  local candidates=()
  local candidate

  if [[ -n "${MOONCAKE_UPSTREAM_BUILD_DIR:-}" ]]; then
    candidates+=("${MOONCAKE_UPSTREAM_BUILD_DIR}")
  fi
  candidates+=(
    "${UPSTREAM_DIR}/build-rust"
    "${UPSTREAM_DIR}/build-wheel-compat"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -f "${candidate}/mooncake-transfer-engine/src/libtransfer_engine.so" ]] \
      && [[ -f "${candidate}/mooncake-transfer-engine/tent/src/libtent_shared.so" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  echo "unable to find Mooncake runtime libraries under ${UPSTREAM_DIR}" >&2
  echo "set MOONCAKE_UPSTREAM_BUILD_DIR to a built upstream directory" >&2
  exit 1
}

require_cargo
UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${LD_LIBRARY_PATH:-}"

cd "${REPO_ROOT}"

echo "==> testing mooncake-store-client hot-upgrade startup flags"
cargo test -p mooncake-store-py --bin mooncake-store-client
