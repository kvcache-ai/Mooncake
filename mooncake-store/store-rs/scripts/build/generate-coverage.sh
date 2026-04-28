#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)

COVERAGE_DIR=${COVERAGE_DIR:-"${REPO_ROOT}/target/coverage"}
UPSTREAM_DIR=${MOONCAKE_UPSTREAM_DIR:-"${REPO_ROOT}/third_party/Mooncake"}
SUMMARY_FILE="${COVERAGE_DIR}/summary.txt"
JSON_FILE="${COVERAGE_DIR}/workspace.json"
HTML_DIR="${COVERAGE_DIR}/html"

usage() {
  cat <<'EOF'
Usage: scripts/build/generate-coverage.sh

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

# --exclude is only supported by test/nextest subcommands, not by `report`.
# Split args: TEST_ARGS for the test run, REPORT_ARGS for report generation.
TEST_ARGS=(
  --workspace
  --exclude mooncake-store-e2e
)

# Run tests once with --no-report to compile, execute tests, and collect
# profraw data without generating any report.  Then use `cargo llvm-cov report`
# (which reuses the existing profraw files) to emit all three formats.
# This avoids recompiling and re-running tests three times.

echo "==> running instrumented tests (compile + execute once)"
cargo llvm-cov "${TEST_ARGS[@]}" --no-report --lib -- --test-threads=4

echo "==> generating coverage summary"
cargo llvm-cov report --summary-only | tee "${SUMMARY_FILE}"

echo "==> generating JSON report at ${JSON_FILE}"
cargo llvm-cov report --json --output-path "${JSON_FILE}"

# HTML report generation is slow (~30-60s) and rarely viewed in CI.
# Skip by default; set COVERAGE_HTML=1 to enable.
if [[ "${COVERAGE_HTML:-0}" == "1" ]]; then
  echo "==> generating HTML report at ${HTML_DIR}"
  cargo llvm-cov report --html --output-dir "${COVERAGE_DIR}"
else
  echo "==> skipping HTML report (set COVERAGE_HTML=1 to enable)"
fi

echo
echo "coverage summary: ${SUMMARY_FILE}"
echo "coverage json:    ${JSON_FILE}"
if [[ "${COVERAGE_HTML:-0}" == "1" ]]; then
  echo "coverage html:    ${HTML_DIR}/index.html"
fi
