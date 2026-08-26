#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# store-rs is a subdirectory when it lives inside the Mooncake monorepo,
# where the git toplevel is the enclosing repository rather than this tree.
[ -f "${REPO_ROOT}/Cargo.toml" ] || REPO_ROOT="${REPO_ROOT}/mooncake-store/store-rs"

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
  WHEEL_PATH=$(ls -1t "${WHEEL_DIR}"/mooncake_store_rs-*.whl 2>/dev/null | head -n 1 || true)
fi

if [[ -z "${WHEEL_PATH}" || ! -f "${WHEEL_PATH}" ]]; then
  echo "cannot find mooncake-store-rs wheel; run scripts/build/build-wheel.sh first" >&2
  exit 1
fi

echo "python: ${PYTHON_BIN}"
echo "wheel:  ${WHEEL_PATH}"
echo "links:  ${WHEEL_DIR}"

PIP_INDEX=${PIP_INDEX_URL:-"https://pypi.org/simple/"}

# Install in two steps to avoid --force-reinstall uninstalling and
# re-downloading all transitive dependencies (aiohttp, requests, etc.):
#
# Step 1: Force-reinstall only the mooncake-store-rs wheel itself (--no-deps),
#         ensuring the latest CI-built wheel replaces any cached version
#         carrying the same version number.
#
# Step 2: Install the wheel again without --force-reinstall to let pip
#         resolve and install any missing dependencies.  Already-installed
#         deps (matching version constraints) are skipped automatically.
"${PYTHON_BIN}" -m pip install \
  --no-cache-dir \
  -i "${PIP_INDEX}" \
  --force-reinstall \
  --no-deps \
  --find-links "${WHEEL_DIR}" \
  "${WHEEL_PATH}"

"${PYTHON_BIN}" -m pip install \
  --no-cache-dir \
  -i "${PIP_INDEX}" \
  --find-links "${WHEEL_DIR}" \
  "${WHEEL_PATH}"
