#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# test-rolling-upgrade-rollback-wrh-e2e.sh
#
# End-to-end rolling upgrade ROLLBACK verification for Mooncake Store using
# embedded-wrh route control mode.
#
# This is the embedded-wrh variant of test-rolling-upgrade-rollback-e2e.sh.
# The key differences from the metadata-only version:
#   - Route data is stored in client process memory (WRH authority)
#   - Clients use --label route=true to act as route authorities
#   - Driver Python code uses route_control="embedded_wrh"
#
# This script simulates a scenario where a rolling upgrade is partially
# applied and then rolled back:
#   1. Builds the current code as V1 and backs up the binary
#   2. Patches the source to create a V2
#   3. Builds V2
#   4. Starts two V1 clients, writes test data (Phase 1)
#   5. Rolling-upgrades client-a (V1 -> V2), verifies data (Phase 2)
#   6. Rolls back client-a (V2 -> V1) using V1 binary with epoch=3 (Phase 3)
#   7. Verifies all data intact and cluster is fully V1 again (Phase 4)
#   8. Restores the source to its original state
#
# Usage:
#   ./scripts/tests/rolling/test-rolling-upgrade-rollback-wrh-e2e.sh
#
# Environment:
#   MC_STORE_RS_REDIS_PORT  Redis port (default: 6380)
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"

# ── helpers ────────────────────────────────────────────────────────────────

wait_for_log() {
  local log_file=$1 needle=$2 timeout=${3:-20}
  local attempts=$((timeout * 10))
  for ((i = 0; i < attempts; i++)); do
    if [[ -f "${log_file}" ]] && grep -Fq "${needle}" "${log_file}"; then
      return 0
    fi
    sleep 0.1
  done
  echo "TIMEOUT waiting for: ${needle}" >&2
  echo "--- ${log_file} ---" >&2
  cat "${log_file}" >&2
  return 1
}

wait_for_exit() {
  local pid=$1 name=$2 timeout=${3:-30}
  local attempts=$((timeout * 10))
  for ((i = 0; i < attempts; i++)); do
    if ! kill -0 "${pid}" 2>/dev/null; then return 0; fi
    sleep 0.1
  done
  echo "${name} pid=${pid} did not exit within ${timeout}s" >&2
  return 1
}

# ── setup ──────────────────────────────────────────────────────────────────

mc_scripts_require_command cargo
mc_scripts_require_command python3
mc_scripts_require_command redis-cli
mc_scripts_require_command redis-server

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1

REDIS_URL="redis://127.0.0.1:${REDIS_PORT}/0"
RUN_ID=$(date +%s%N)
KEYSPACE="mc/store-rs/rollback-wrh-e2e/${RUN_ID}"
TEMP_DIR=$(mktemp -d)
PIDS=()
REDIS_STARTED=0
SOURCE_PATCHED=0

BIN_V1="${REPO_ROOT}/target/debug/mooncake-store-client-v1"
BIN_V2="${REPO_ROOT}/target/debug/mooncake-store-client"

COMPAT_RS="${REPO_ROOT}/crates/mooncake-store-core/src/compat.rs"
CLIENT_BIN_RS="${REPO_ROOT}/crates/mooncake-store-py/src/bin/mooncake-store-client.rs"
PROTO_FILE="${REPO_ROOT}/crates/mooncake-store-client/proto/control_plane.proto"
CODEC_RS="${REPO_ROOT}/crates/mooncake-store-client/src/control_plane/codec.rs"

cleanup() {
  local status=$?
  for pid in "${PIDS[@]:-}"; do
    kill -KILL "${pid}" 2>/dev/null || true
  done
  if [[ "${REDIS_STARTED}" == "1" ]]; then
    redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
  fi
  if [[ "${SOURCE_PATCHED}" == "1" ]]; then
    echo "==> restoring source files"
    cd "${REPO_ROOT}"
    git checkout -- \
      "${COMPAT_RS}" \
      "${CLIENT_BIN_RS}" \
      "${PROTO_FILE}" \
      "${CODEC_RS}" \
      2>/dev/null || true
  fi
  if [[ "${status}" != "0" ]]; then
    echo "=== FAILED === logs preserved in ${TEMP_DIR}" >&2
  else
    rm -rf "${TEMP_DIR}"
  fi
  exit "${status}"
}
trap cleanup EXIT

# ── redis ──────────────────────────────────────────────────────────────────

mc_scripts_start_local_redis_if_needed "${REDIS_PORT}" REDIS_STARTED

# ── build V1 ───────────────────────────────────────────────────────────────

cd "${REPO_ROOT}"

echo "============================================"
echo "  Rolling Upgrade ROLLBACK E2E Test (embedded-wrh)"
echo "  RUN_ID: ${RUN_ID}"
echo "============================================"
echo ""
echo "==> building V1 binary"
cargo build -p mooncake-store-py
cp "${BIN_V2}" "${BIN_V1}"

# ── patch source for V2 ───────────────────────────────────────────────────

echo "==> patching source for V2"

# 1) Bump store_api_minor_version from 0 to 1
sed -i 's/store_api_minor_version: 0,/store_api_minor_version: 1,/' "${COMPAT_RS}"

# 2) Add [v2] tag to startup log
sed -i 's/"mooncake-store-client started stable_id={stable_id}/"mooncake-store-client started [v2] stable_id={stable_id}/' "${CLIENT_BIN_RS}"

# 3) Add upgrade_tag field to proto
sed -i '/uint32 store_api_minor_version = 5;/a\  string upgrade_tag = 6;' "${PROTO_FILE}"

# 4) Add upgrade_tag to pb_compatibility in codec.rs
sed -i '/^pub(super) fn pb_compatibility/,/^}/{
  /capabilities: descriptor.capabilities.iter().cloned().collect(),/{
    s/$/\n        upgrade_tag: String::new(),/
  }
}' "${CODEC_RS}"

SOURCE_PATCHED=1

echo "==> building V2 binary"
cargo build -p mooncake-store-py

# ── common args ────────────────────────────────────────────────────────────

BASE_ARGS=(
  --local-hostname 127.0.0.1
  --metadata-url "${REDIS_URL}"
  --storage-bytes 1048576
  --scratch-bytes 1048576
  --protocol tcp
  --route-control embedded-wrh
  --keyspace "${KEYSPACE}"
  --lease-ttl-ms 4000
  --heartbeat-interval-ms 500
  --label pool=pool-a
  --label storage=true
  --label route=true
  --drain-on-exit
)

# ===== PHASE 1: V1 cluster read/write =====================================

echo ""
echo "=== PHASE 1: Start two V1 clients and verify read/write ==="

"${BIN_V1}" "${BASE_ARGS[@]}" \
  --stable-id client-a --initial-state active \
  --local-segment-name "seg-a-v1-${RUN_ID}" \
  >"${TEMP_DIR}/client-a-v1.log" 2>&1 &
CLIENT_A_V1_PID=$!
PIDS+=("${CLIENT_A_V1_PID}")

"${BIN_V1}" "${BASE_ARGS[@]}" \
  --stable-id client-b --initial-state active \
  --local-segment-name "seg-b-v1-${RUN_ID}" \
  >"${TEMP_DIR}/client-b-v1.log" 2>&1 &
CLIENT_B_V1_PID=$!
PIDS+=("${CLIENT_B_V1_PID}")

wait_for_log "${TEMP_DIR}/client-a-v1.log" "mooncake-store-client started stable_id=client-a epoch=1"
wait_for_log "${TEMP_DIR}/client-b-v1.log" "mooncake-store-client started stable_id=client-b epoch=1"
echo "  [OK] Both V1 clients started"

REDIS_URL="${REDIS_URL}" KEYSPACE="${KEYSPACE}" python3 - <<'PY'
import os
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1", "P2PHANDSHAKE",
    4*1024*1024, 1*1024*1024, "tcp", "", os.environ["REDIS_URL"],
    stable_id="driver-write-1",
    keyspace=os.environ["KEYSPACE"],
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    route_control="embedded_wrh",
) == 0

policy_a = ReplicateConfig(replica_num=1, preferred_storage_owners=["client-a:1"], prefer_local=False)
assert store.put("test-key-1", b"value-from-v1-client-a", config=policy_a) == 0

policy_b = ReplicateConfig(replica_num=1, preferred_storage_owners=["client-b:1"], prefer_local=False)
assert store.put("test-key-2", b"value-from-v1-client-b", config=policy_b) == 0

assert store.get("test-key-1") == b"value-from-v1-client-a"
assert store.get("test-key-2") == b"value-from-v1-client-b"
print("  [OK] Write and read verified for both keys")
store.close()
PY

echo "  [OK] PHASE 1 complete"

# ===== PHASE 2: Rolling upgrade client-a (V1 -> V2) =======================

echo ""
echo "=== PHASE 2: Rolling upgrade client-a (V1 -> V2) ==="

"${BIN_V2}" "${BASE_ARGS[@]}" \
  --stable-id client-a --initial-state standby \
  --local-segment-name "seg-a-v2-${RUN_ID}" \
  >"${TEMP_DIR}/client-a-v2.log" 2>&1 &
CLIENT_A_V2_PID=$!
PIDS+=("${CLIENT_A_V2_PID}")
wait_for_log "${TEMP_DIR}/client-a-v2.log" "mooncake-store-client started" 20
echo "  [OK] client-a V2 successor started (standby)"

if grep -q '\[v2\]' "${TEMP_DIR}/client-a-v2.log"; then
  echo "  [OK] V2 version tag confirmed in startup log"
else
  echo "  [FAIL] V2 version tag not found in startup log" >&2
  exit 1
fi

echo "  Sending SIGTERM to client-a V1 predecessor..."
kill -TERM "${CLIENT_A_V1_PID}"
wait_for_log "${TEMP_DIR}/client-a-v1.log" "mooncake-store-client handoff stable_id=client-a" 30
wait_for_log "${TEMP_DIR}/client-a-v1.log" "mooncake-store-client upgraded stable_id=client-a" 30
wait_for_log "${TEMP_DIR}/client-a-v2.log" "mooncake-store-client promoted stable_id=client-a" 30
wait_for_exit "${CLIENT_A_V1_PID}" "client-a-v1" 30
echo "  [OK] client-a V1 exited, V2 promoted"

REDIS_URL="${REDIS_URL}" KEYSPACE="${KEYSPACE}" python3 - <<'PY'
import os, time
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1", "P2PHANDSHAKE",
    4*1024*1024, 1*1024*1024, "tcp", "", os.environ["REDIS_URL"],
    stable_id="driver-verify-2",
    keyspace=os.environ["KEYSPACE"],
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    route_control="embedded_wrh",
) == 0

for _ in range(100):
    route = store.query_route("test-key-1")
    if route and [r["owner"] for r in route["replicas"]] == ["client-a:2"]:
        break
    time.sleep(0.1)
else:
    raise AssertionError("route did not converge to client-a:2")

assert store.get("test-key-1") == b"value-from-v1-client-a"
print("  [OK] test-key-1 preserved after client-a upgrade (owner: client-a:2)")

assert store.get("test-key-2") == b"value-from-v1-client-b"
print("  [OK] test-key-2 still readable from client-b V1")

policy = ReplicateConfig(replica_num=1, preferred_storage_owners=["client-a:2"], prefer_local=False)
assert store.put("test-key-3", b"value-from-v2-client-a", config=policy) == 0
assert store.get("test-key-3") == b"value-from-v2-client-a"
print("  [OK] New write test-key-3 on V2 client-a successful")

store.close()
PY

echo "  [OK] PHASE 2 complete: mixed-version cluster verified"

# In embedded-wrh mode, other clients need time to refresh their live client
# cache and discover client-a V2's new control plane address. Wait for a few
# seconds to allow the membership sync background thread to pick up the change.
echo "  Waiting for membership cache refresh..."
sleep 3

# ===== PHASE 3: Rollback client-a (V2 -> V1) ==============================

echo ""
echo "=== PHASE 3: Rollback client-a (V2 -> V1) using V1 binary ==="
echo "  Starting client-a V1 rollback with epoch=3 (standby)..."

"${BIN_V1}" "${BASE_ARGS[@]}" \
  --stable-id client-a --initial-state standby \
  --local-segment-name "seg-a-rollback-${RUN_ID}" \
  >"${TEMP_DIR}/client-a-rollback.log" 2>&1 &
CLIENT_A_ROLLBACK_PID=$!
PIDS+=("${CLIENT_A_ROLLBACK_PID}")
wait_for_log "${TEMP_DIR}/client-a-rollback.log" "mooncake-store-client started stable_id=client-a epoch=3" 20
echo "  [OK] client-a V1 rollback successor started (standby, epoch=3)"

# Verify the rollback instance does NOT have the [v2] tag
if grep -q '\[v2\]' "${TEMP_DIR}/client-a-rollback.log"; then
  echo "  [FAIL] Rollback instance should NOT have [v2] tag" >&2
  exit 1
else
  echo "  [OK] Rollback instance confirmed as V1 (no [v2] tag)"
fi

echo "  Sending SIGTERM to client-a V2 predecessor..."
kill -TERM "${CLIENT_A_V2_PID}"
wait_for_log "${TEMP_DIR}/client-a-v2.log" "mooncake-store-client handoff stable_id=client-a" 30
wait_for_log "${TEMP_DIR}/client-a-v2.log" "mooncake-store-client upgraded stable_id=client-a" 30
wait_for_log "${TEMP_DIR}/client-a-rollback.log" "mooncake-store-client promoted stable_id=client-a" 30
wait_for_exit "${CLIENT_A_V2_PID}" "client-a-v2" 30
echo "  [OK] client-a V2 exited, V1 rollback instance promoted (epoch=3)"

# ===== PHASE 4: Verify all data intact after rollback =====================

echo ""
echo "=== PHASE 4: Verify all data intact after rollback ==="

REDIS_URL="${REDIS_URL}" KEYSPACE="${KEYSPACE}" python3 - <<'PY'
import os, time
from mooncake.store import MooncakeDistributedStore, ReplicateConfig

store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1", "P2PHANDSHAKE",
    4*1024*1024, 1*1024*1024, "tcp", "", os.environ["REDIS_URL"],
    stable_id="driver-verify-rollback",
    keyspace=os.environ["KEYSPACE"],
    labels={"pool": "pool-a", "storage": "false", "route": "false"},
    route_control="embedded_wrh",
) == 0

# Wait for route to converge: test-key-1 and test-key-3 should be on client-a:3
for _ in range(100):
    route = store.query_route("test-key-1")
    if route and [r["owner"] for r in route["replicas"]] == ["client-a:3"]:
        break
    time.sleep(0.1)
else:
    raise AssertionError("route for test-key-1 did not converge to client-a:3")

# Verify test-key-1: originally written to client-a V1, migrated V1->V2->V1
data = store.get("test-key-1")
assert data == b"value-from-v1-client-a", f"test-key-1 mismatch: {data}"
print("  [OK] test-key-1 preserved after rollback (V1 -> V2 -> V1, owner: client-a:3)")

# Verify test-key-2: always on client-b V1, should be unaffected
data = store.get("test-key-2")
assert data == b"value-from-v1-client-b", f"test-key-2 mismatch: {data}"
print("  [OK] test-key-2 still intact on client-b V1")

# Verify test-key-3: written during V2 phase, migrated V2->V1
for _ in range(100):
    route = store.query_route("test-key-3")
    if route and [r["owner"] for r in route["replicas"]] == ["client-a:3"]:
        break
    time.sleep(0.1)
else:
    raise AssertionError("route for test-key-3 did not converge to client-a:3")

data = store.get("test-key-3")
assert data == b"value-from-v2-client-a", f"test-key-3 mismatch: {data}"
print("  [OK] test-key-3 preserved after rollback (written by V2, now on V1)")

# Write new data on the rolled-back V1 instance to confirm it is fully functional
policy = ReplicateConfig(replica_num=1, preferred_storage_owners=["client-a:3"], prefer_local=False)
assert store.put("test-key-4", b"value-after-rollback", config=policy) == 0
assert store.get("test-key-4") == b"value-after-rollback"
print("  [OK] test-key-4 new write on rolled-back V1 client-a successful")

# Cross-node read: verify client-b can still serve data
policy_b = ReplicateConfig(replica_num=1, preferred_storage_owners=["client-b:1"], prefer_local=False)
assert store.put("test-key-5", b"value-post-rollback-client-b", config=policy_b) == 0
assert store.get("test-key-5") == b"value-post-rollback-client-b"
print("  [OK] test-key-5 cross-node write/read verified after rollback")

print("")
print("  === All 5 keys verified: rollback is fully successful ===")
store.close()
PY

echo "  [OK] PHASE 4 complete: all data intact, cluster fully V1"

# ── teardown ───────────────────────────────────────────────────────────────

kill -TERM "${CLIENT_A_ROLLBACK_PID}" 2>/dev/null || true
kill -TERM "${CLIENT_B_V1_PID}" 2>/dev/null || true
sleep 2

echo ""
echo "============================================"
echo "  Rolling Upgrade ROLLBACK E2E Test (embedded-wrh) PASSED ✅"
echo "============================================"
