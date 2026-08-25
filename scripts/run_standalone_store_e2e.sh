#!/usr/bin/env bash
# Run Mooncake Store e2e tests in standalone mode (no mooncake_master process).
#
# Usage:
#   bash scripts/run_standalone_store_e2e.sh
#
# Requires the mooncake Python wheel (or a PYTHONPATH that provides
# mooncake.store / store). These tests embed master in-process.
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

PYTHON="${PYTHON:-python}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:/usr/local/lib"
export PROTOCOL="${PROTOCOL:-tcp}"
export DEVICE_NAME="${DEVICE_NAME:-}"
export MC_FORCE_TCP="${MC_FORCE_TCP:-1}"

assert_no_master() {
    local when="$1"
    if pgrep -x mooncake_master >/dev/null 2>&1; then
        echo "ERROR: mooncake_master is running ${when} standalone e2e" >&2
        pgrep -ax mooncake_master >&2 || true
        exit 1
    fi
}

assert_no_master "before"

echo "=== Standalone Python unittest e2e ==="
"$PYTHON" "$REPO_ROOT/mooncake-wheel/tests/test_standalone_store_e2e.py" -v

echo "=== Standalone session-ranges TCP e2e ==="
export MOONCAKE_ENABLE_STANDALONE=true
export MOONCAKE_PROTOCOL="${PROTOCOL}"
export MOONCAKE_DEVICE="${DEVICE_NAME}"
export MOONCAKE_TE_META_DATA_SERVER=P2PHANDSHAKE
export MOONCAKE_LOCAL_HOSTNAME="${MOONCAKE_LOCAL_HOSTNAME:-localhost:17814}"
export MOONCAKE_GLOBAL_SEGMENT_SIZE="${MOONCAKE_GLOBAL_SEGMENT_SIZE:-$((64 * 1024 * 1024))}"
export MOONCAKE_LOCAL_BUFFER_SIZE="${MOONCAKE_LOCAL_BUFFER_SIZE:-$((32 * 1024 * 1024))}"
"$PYTHON" "$REPO_ROOT/mooncake-store/tests/e2e/session_ranges_tcp_e2e.py"

echo "=== Standalone store_client put/get workload e2e ==="
unset MOONCAKE_ENABLE_STANDALONE
"$PYTHON" "$REPO_ROOT/mooncake-store/tests/e2e/store_client_e2e.py" \
    --enable-standalone \
    --metadata-server P2PHANDSHAKE \
    --master-server "" \
    --protocol "${PROTOCOL}" \
    --device-name "${DEVICE_NAME}" \
    --local-hostname "127.0.0.1:50071" \
    --duration-sec "${STANDALONE_CLIENT_E2E_SEC:-3}" \
    --sleep-ms 100 \
    --payload-size 4096 \
    --key-prefix "standalone-e2e-$$"

assert_no_master "after"
echo "All standalone Mooncake Store e2e tests passed (no mooncake_master)."
