#!/usr/bin/env bash
# Run Mooncake Store e2e tests in standalone mode (no mooncake_master process).
#
# Usage:
#   bash scripts/run_standalone_store_e2e.sh
#
# Requires the mooncake Python wheel (or a PYTHONPATH that provides
# mooncake.store). These tests embed master in-process.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

PYTHON="${PYTHON:-python}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib"
export PROTOCOL="${PROTOCOL:-tcp}"
export DEVICE_NAME="${DEVICE_NAME:-}"
export MC_FORCE_TCP="${MC_FORCE_TCP:-1}"

if pgrep -x mooncake_master >/dev/null 2>&1; then
    echo "ERROR: mooncake_master is running before standalone e2e" >&2
    pgrep -ax mooncake_master >&2 || true
    exit 1
fi

echo "=== Standalone Python unittest e2e ==="
"$PYTHON" "$REPO_ROOT/mooncake-wheel/tests/test_standalone_store_e2e.py" -v

if pgrep -x mooncake_master >/dev/null 2>&1; then
    echo "ERROR: mooncake_master is running after standalone e2e" >&2
    pgrep -ax mooncake_master >&2 || true
    exit 1
fi

echo "All standalone Mooncake Store e2e tests passed (no mooncake_master)."
