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
EMPTY_BUILD="$TEST_ROOT/empty-build"
mkdir -p "$EMPTY_BUILD"
expect_failure 1 "missing executable" "$SCRIPT" up --build-dir "$EMPTY_BUILD"
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

echo "PASS"
