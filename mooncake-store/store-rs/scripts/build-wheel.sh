#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

PYTHON_BIN=${PYTHON:-python3}
VENV_DIR=${WHEEL_VENV:-"${REPO_ROOT}/.venv-wheel"}
DIST_DIR=${DIST_DIR:-"${REPO_ROOT}/dist"}
WHEEL_DIR="${DIST_DIR}/wheels"
BIN_DIR="${DIST_DIR}/bin"

usage() {
  cat <<'EOF'
Usage: scripts/build-wheel.sh [maturin build args...]

Environment:
  PYTHON       Python interpreter used to create the build venv (default: python3)
  WHEEL_VENV   Virtualenv directory for maturin (default: .venv-wheel)
  DIST_DIR     Output directory for wheel and binary artifacts (default: dist)

Examples:
  ./scripts/build-wheel.sh
  ./scripts/build-wheel.sh --interpreter python3.11
  DIST_DIR=artifacts ./scripts/build-wheel.sh --compatibility manylinux_2_28
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

require_command git
require_command cargo
require_command "${PYTHON_BIN}"

mkdir -p "${WHEEL_DIR}" "${BIN_DIR}"

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

"${VENV_DIR}/bin/python" -m pip install --upgrade pip >/dev/null
if ! "${VENV_DIR}/bin/python" -m pip show maturin >/dev/null 2>&1; then
  "${VENV_DIR}/bin/python" -m pip install "maturin>=1.7,<2"
fi

git -C "${REPO_ROOT}" submodule update --init --recursive

"${VENV_DIR}/bin/maturin" build \
  --release \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --out "${WHEEL_DIR}" \
  "$@"

cargo build \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --bin mooncake-store-client \
  --release

install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake-store-client" \
  "${BIN_DIR}/mooncake-store-client"

LATEST_WHEEL=$(ls -1t "${WHEEL_DIR}"/*.whl 2>/dev/null | head -n 1 || true)
if [[ -z "${LATEST_WHEEL}" ]]; then
  echo "wheel build completed but no wheel was found in ${WHEEL_DIR}" >&2
  exit 1
fi

cat <<EOF
wheel:  ${LATEST_WHEEL}
client: ${BIN_DIR}/mooncake-store-client
EOF
