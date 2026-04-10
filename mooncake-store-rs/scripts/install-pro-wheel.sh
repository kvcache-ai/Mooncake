#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/.." && pwd)

PYTHON_BIN=${PYTHON:-python3}
WHEEL_DIR=${WHEEL_DIR:-"${REPO_ROOT}/dist/wheels"}
WHEEL_PATH=${1:-}

if [[ -z "${WHEEL_PATH}" ]]; then
  WHEEL_PATH=$(ls -1t "${WHEEL_DIR}"/mooncake_pro-*.whl 2>/dev/null | head -n 1 || true)
fi

if [[ -z "${WHEEL_PATH}" || ! -f "${WHEEL_PATH}" ]]; then
  echo "cannot find mooncake-pro wheel; run scripts/build-wheel.sh first" >&2
  exit 1
fi

echo "python: ${PYTHON_BIN}"
echo "wheel:  ${WHEEL_PATH}"
echo "links:  ${WHEEL_DIR}"

"${PYTHON_BIN}" -m pip install \
  --find-links "${WHEEL_DIR}" \
  "${WHEEL_PATH}"
