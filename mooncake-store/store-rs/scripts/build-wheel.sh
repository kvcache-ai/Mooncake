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

cargo build \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --bin mooncake-store-client \
  --release

"${VENV_DIR}/bin/maturin" build \
  --release \
  --manifest-path "${REPO_ROOT}/crates/mooncake-store-py/Cargo.toml" \
  --out "${WHEEL_DIR}" \
  "$@"

install -m 0755 \
  "${REPO_ROOT}/target/release/mooncake-store-client" \
  "${BIN_DIR}/mooncake-store-client"

LATEST_WHEEL=$(ls -1t "${WHEEL_DIR}"/*.whl 2>/dev/null | head -n 1 || true)
if [[ -z "${LATEST_WHEEL}" ]]; then
  echo "wheel build completed but no wheel was found in ${WHEEL_DIR}" >&2
  exit 1
fi

"${VENV_DIR}/bin/python" - <<'PY' "${LATEST_WHEEL}" "${REPO_ROOT}/target/release/mooncake-store-client"
import base64
import csv
import hashlib
import pathlib
import sys
import tempfile
import zipfile

wheel_path = pathlib.Path(sys.argv[1])
client_path = pathlib.Path(sys.argv[2])

with tempfile.TemporaryDirectory(prefix="mooncake-wheel-") as temp_dir:
    root = pathlib.Path(temp_dir)
    with zipfile.ZipFile(wheel_path) as source_wheel:
        source_wheel.extractall(root)

    packaged_client = root / "mooncake" / "mooncake-store-client"
    packaged_client.write_bytes(client_path.read_bytes())
    packaged_client.chmod(0o755)

    dist_info = next(root.glob("*.dist-info"))
    record_path = dist_info / "RECORD"
    rows = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if path == record_path:
            rows.append((relative, "", ""))
            continue
        payload = path.read_bytes()
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode().rstrip("=")
        rows.append((relative, f"sha256={digest}", str(len(payload))))

    with record_path.open("w", newline="") as record_file:
        csv.writer(record_file, lineterminator="\n").writerows(rows)

    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as target_wheel:
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo.from_file(path, arcname=relative)
            info.compress_type = zipfile.ZIP_DEFLATED
            target_wheel.writestr(info, path.read_bytes())
PY

cat <<EOF
wheel:  ${LATEST_WHEEL}
client: ${BIN_DIR}/mooncake-store-client
EOF
