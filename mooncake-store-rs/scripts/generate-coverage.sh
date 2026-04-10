#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

COVERAGE_DIR=${COVERAGE_DIR:-"${REPO_ROOT}/target/coverage"}
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}
SUMMARY_FILE="${COVERAGE_DIR}/summary.txt"
JSON_FILE="${COVERAGE_DIR}/workspace.json"
HTML_DIR="${COVERAGE_DIR}/html"

usage() {
  cat <<'EOF'
Usage: scripts/generate-coverage.sh

Generate workspace unit-test coverage reports.

Outputs:
  target/coverage/summary.txt
  target/coverage/workspace.json
  target/coverage/html/index.html

Notes:
  - The current default scope excludes `mooncake-store-e2e`.
  - Runtime TE libraries are resolved from the Mooncake upstream build tree.

Environment:
  COVERAGE_DIR                Output directory for coverage artifacts
  MOONCAKE_UPSTREAM_DIR       Mooncake upstream submodule path
  MOONCAKE_UPSTREAM_BUILD_DIR Explicit upstream build directory override
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

require_command() {
  local cmd=$1
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "missing required command: ${cmd}" >&2
    exit 1
  fi
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
  return 1
}

require_command cargo
if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "cargo llvm-cov is not installed; run: cargo install cargo-llvm-cov" >&2
  exit 1
fi

UPSTREAM_BUILD_DIR=$(resolve_upstream_build_dir)
export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${LD_LIBRARY_PATH:-}"

mkdir -p "${COVERAGE_DIR}"
rm -f "${SUMMARY_FILE}" "${JSON_FILE}"
rm -rf "${HTML_DIR}"

cd "${REPO_ROOT}"

COMMON_ARGS=(
  --workspace
  --exclude mooncake-store-e2e
)

echo "==> generating coverage summary"
cargo llvm-cov "${COMMON_ARGS[@]}" --summary-only | tee "${SUMMARY_FILE}"

echo "==> generating JSON report at ${JSON_FILE}"
cargo llvm-cov "${COMMON_ARGS[@]}" --json --output-path "${JSON_FILE}"

echo "==> generating HTML report at ${HTML_DIR}"
cargo llvm-cov "${COMMON_ARGS[@]}" --html --output-dir "${COVERAGE_DIR}"

echo
echo "coverage summary: ${SUMMARY_FILE}"
echo "coverage json:    ${JSON_FILE}"
echo "coverage html:    ${HTML_DIR}/index.html"
