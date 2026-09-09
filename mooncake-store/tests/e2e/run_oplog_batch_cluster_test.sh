#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="$SCRIPT_DIR/run_oplog_batch_cluster.sh"
TEST_ROOT=${TEST_ROOT:-"/tmp/mooncake-oplog-cluster-script-test-$$"}
mkdir -p "$TEST_ROOT"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

expect_failure() {
  local expected_status=$1
  local expected_text=$2
  shift 2

  local output_file="$TEST_ROOT/output-$RANDOM.log"
  local status=0
  "$@" >"$output_file" 2>&1 || status=$?
  [[ "$status" -eq "$expected_status" ]] ||
    fail "expected status $expected_status, got $status: $(<"$output_file")"
  grep -Fq "$expected_text" "$output_file" ||
    fail "expected '$expected_text': $(<"$output_file")"
}

expect_success() {
  local expected_text=$1
  shift

  local output_file="$TEST_ROOT/output-$RANDOM.log"
  "$@" >"$output_file" 2>&1 ||
    fail "expected success: $(<"$output_file")"
  grep -Fq "$expected_text" "$output_file" ||
    fail "expected '$expected_text': $(<"$output_file")"
}

expect_failure 1 "unknown command" "$SCRIPT" unknown
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" up --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" failpoint-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" failpoint-crash-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" remove-boundary-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" standby-read-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" promotion-catchup-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" allocator-recovery-matrix --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "build directory does not exist" \
  "$SCRIPT" non-ha-smoke --build-dir "$TEST_ROOT/missing-build"
expect_failure 1 "master config does not exist" \
  "$SCRIPT" non-ha-smoke --build-dir "$TEST_ROOT" \
  --master-config "$TEST_ROOT/missing-master.yaml"
expect_failure 1 "non-ha-workers must be positive" \
  "$SCRIPT" non-ha-smoke --build-dir "$TEST_ROOT" --non-ha-workers 0
expect_failure 1 "ha-objects must be positive" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT" --ha-objects 0
expect_failure 1 "ha-payload-bytes must be positive" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT" --ha-payload-bytes 0
expect_failure 1 "ha-pressure-sec must be positive" \
  "$SCRIPT" ha-failover-smoke --build-dir "$TEST_ROOT" --ha-pressure-sec 0
expect_failure 1 "snapshot-chunk-object-count must be positive" \
  "$SCRIPT" up --build-dir "$TEST_ROOT" --snapshot-chunk-object-count 0
expect_failure 1 "memory-allocator must be offset or cachelib" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --memory-allocator invalid
expect_failure 1 "recovery-seed-objects must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-seed-objects 0
expect_failure 1 "recovery-refill-objects must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-refill-objects 0
expect_failure 1 "recovery-pressure-sec must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-pressure-sec 0
expect_failure 1 "recovery-segment-bytes must be positive" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-segment-bytes 0
expect_failure 1 "recovery-payload-sizes must be positive CSV integers" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" \
  --recovery-payload-sizes 64,0,4096
expect_failure 1 "allocator recovery requires exactly 3 masters" \
  "$SCRIPT" allocator-recovery-smoke --build-dir "$TEST_ROOT" --masters 2
EMPTY_BUILD="$TEST_ROOT/empty-build"
mkdir -p "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" up --build-dir "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" allocator-recovery-smoke \
  --build-dir "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" non-ha-smoke \
  --build-dir "$EMPTY_BUILD" --no-etcd-observer
expect_failure 1 "run directory does not exist" \
  "$SCRIPT" status --run-dir "$TEST_ROOT/missing-run"
expect_failure 1 "run directory does not exist" \
  "$SCRIPT" restart --run-dir "$TEST_ROOT/missing-run"

EMPTY_RUN="$TEST_ROOT/empty-run"
mkdir -p "$EMPTY_RUN/pids"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$EMPTY_RUN"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$EMPTY_RUN"

MISMATCH_RUN="$TEST_ROOT/mismatch-run"
mkdir -p "$MISMATCH_RUN/pids"
sleep 30 &
SLEEP_PID=$!
trap 'kill "$SLEEP_PID" 2>/dev/null || true' EXIT
printf '%s\n' "$SLEEP_PID" >"$MISMATCH_RUN/pids/master-0.pid"
printf '%s\n' "definitely-not-the-sleep-command" \
  >"$MISMATCH_RUN/pids/master-0.cmd"
expect_success "refusing to stop PID" \
  "$SCRIPT" down --run-dir "$MISMATCH_RUN"
kill -0 "$SLEEP_PID" 2>/dev/null || fail "mismatched PID was stopped"

MATCH_RUN="$TEST_ROOT/match-run"
mkdir -p "$MATCH_RUN/pids"
sleep 30 &
MATCH_PID=$!
printf '%s\n' "$MATCH_PID" >"$MATCH_RUN/pids/master-0.pid"
tr '\0' ' ' <"/proc/$MATCH_PID/cmdline" >"$MATCH_RUN/pids/master-0.cmd"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$MATCH_RUN"
kill -0 "$MATCH_PID" 2>/dev/null && fail "matching PID was not stopped"
expect_success "cluster is stopped" "$SCRIPT" down --run-dir "$MATCH_RUN"

# Execute start_master with a fake binary, including both environment paths.
FAKE_MASTER="$TEST_ROOT/fake-master"
cat >"$FAKE_MASTER" <<'MASTER'
#!/usr/bin/env bash
[[ "$MOONCAKE_SNAPSHOT_LOCAL_PATH" == "$EXPECTED_SNAPSHOT_PATH" ]]
[[ "${MOONCAKE_TEST_FAILPOINT_DIR:-}" == "$EXPECTED_FAILPOINT_PATH" ]]
[[ "$*" == *"--enable_oplog_snapshot=true"* ]]
MASTER
chmod +x "$FAKE_MASTER"
for failpoints in "" "$TEST_ROOT/failpoints"; do
  (
    source "$SCRIPT"
    parse_up_options --build-dir "$TEST_ROOT" --run-dir "$TEST_ROOT/env-run" \
      --enable-oplog-snapshot --snapshot-chunk-object-count 2
    MASTER_BIN="$FAKE_MASTER"
    ETCD_ENDPOINTS=127.0.0.1:1
    RPC_PORTS=(10001)
    ADMIN_PORTS=(10002)
    FAILPOINT_DIR="$failpoints"
    export EXPECTED_SNAPSHOT_PATH="$RUN_DIR/snapshots"
    export EXPECTED_FAILPOINT_PATH="$failpoints"
    start_process() { shift; "$@"; }
    start_master 0
  ) || fail "snapshot environment propagation failed"
done

(
  source "$SCRIPT"
  RUN_DIR="$TEST_ROOT/old-run"
  mkdir -p "$RUN_DIR"
  printf 'RPC_PORTS=10001\nADMIN_PORTS=10002\nBUILD_DIR=/unused\n' >"$RUN_DIR/cluster.env"
  unset ENABLE_OPLOG_SNAPSHOT SNAPSHOT_CHUNK_OBJECT_COUNT
  load_cluster_env
  [[ "$ENABLE_OPLOG_SNAPSHOT" == false ]]
  [[ "$SNAPSHOT_CHUNK_OBJECT_COUNT" == 1000000 ]]
) || fail "older cluster.env did not retain pure OpLog defaults"

echo "PASS"
