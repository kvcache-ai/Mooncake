#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

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

list_upstream_dirs() {
  local primary_worktree

  if [[ -n "${MOONCAKE_UPSTREAM_DIR:-}" ]]; then
    printf '%s\n' "${MOONCAKE_UPSTREAM_DIR}"
  fi
  printf '%s\n' "${REPO_ROOT}/third_party/Mooncake"

  if primary_worktree=$(git -C "${REPO_ROOT}" worktree list --porcelain 2>/dev/null | awk '/^worktree / { print substr($0, 10); exit }'); then
    if [[ -n "${primary_worktree}" && "${primary_worktree}" != "${REPO_ROOT}" ]]; then
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

require_cargo
UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
UPSTREAM_DIR=$(cd -- "${UPSTREAM_BUILD_DIR}/.." && pwd)
export MOONCAKE_UPSTREAM_DIR="${UPSTREAM_DIR}"
export MOONCAKE_UPSTREAM_BUILD_DIR="${UPSTREAM_BUILD_DIR}"
export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${LD_LIBRARY_PATH:-}"

cd "${REPO_ROOT}"

echo "==> testing mooncake-store-client hot-upgrade startup flags"
cargo test -p mooncake-store-py --bin mooncake-store-client
