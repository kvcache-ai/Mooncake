#!/usr/bin/env bash
# Run Mooncake Store e2e tests with an in-process master (no mooncake_master).
#
# Usage:
#   bash scripts/run_standalone_store_e2e.sh
#
# Requires the mooncake Python wheel (or a PYTHONPATH that provides
# mooncake.store). Test plumbing only; not a user-facing deployment mode.
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
    echo "ERROR: mooncake_master is running before in-process master e2e" >&2
    pgrep -ax mooncake_master >&2 || true
    exit 1
fi

echo "=== In-process master Python unittest e2e ==="
"$PYTHON" "$REPO_ROOT/mooncake-wheel/tests/test_standalone_store_e2e.py" -v

if pgrep -x mooncake_master >/dev/null 2>&1; then
    echo "ERROR: mooncake_master is running after in-process master e2e" >&2
    pgrep -ax mooncake_master >&2 || true
    exit 1
fi

echo "All in-process master e2e tests passed (no mooncake_master)."
