#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)

default_python() {
  if [[ -n "${PYTHON:-}" ]]; then
    printf '%s\n' "${PYTHON}"
    return 0
  fi
  if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
    printf '%s\n' "${VIRTUAL_ENV}/bin/python"
    return 0
  fi
  if [[ -x "${REPO_ROOT}/.venv-wheel/bin/python" ]]; then
    printf '%s\n' "${REPO_ROOT}/.venv-wheel/bin/python"
    return 0
  fi
  printf '%s\n' python3
}

PYTHON_BIN=$(default_python)
WHEEL_DIR=${WHEEL_DIR:-"${REPO_ROOT}/dist/wheels"}
WHEEL_PATH=${1:-}

if [[ -z "${WHEEL_PATH}" ]]; then
  WHEEL_PATH=$(ls -1t "${WHEEL_DIR}"/mooncake_pro-*.whl 2>/dev/null | head -n 1 || true)
fi

if [[ -z "${WHEEL_PATH}" || ! -f "${WHEEL_PATH}" ]]; then
  echo "cannot find mooncake-pro wheel; run scripts/build/build-wheel.sh first" >&2
  exit 1
fi

echo "python: ${PYTHON_BIN}"
echo "wheel:  ${WHEEL_PATH}"
echo "links:  ${WHEEL_DIR}"

PIP_INDEX=${PIP_INDEX_URL:-"https://mirrors.aliyun.com/pypi/simple/"}

"${PYTHON_BIN}" -m pip install \
  --no-cache-dir \
  -i "${PIP_INDEX}" \
  --force-reinstall \
  --find-links "${WHEEL_DIR}" \
  "${WHEEL_PATH}"
