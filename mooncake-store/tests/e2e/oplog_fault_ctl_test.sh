#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SCRIPT="$SCRIPT_DIR/oplog_fault_ctl.sh"
TEST_ROOT=${TEST_ROOT:-"/tmp/mooncake-oplog-fault-ctl-test-$$"}
mkdir -p "$TEST_ROOT"/{failpoints,pids}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

expect_failure() {
  local expected=$1
  shift
  local output="$TEST_ROOT/output-$RANDOM.log"
  if "$@" >"$output" 2>&1; then
    fail "command unexpectedly succeeded"
  fi
  grep -Fq "$expected" "$output" || fail "missing error '$expected'"
}

NAME=batch_txn_succeeded_before_callback
"$SCRIPT" failpoint arm --run-dir "$TEST_ROOT" --name "$NAME"
[[ -e "$TEST_ROOT/failpoints/$NAME.arm" ]] || fail "arm file missing"
: >"$TEST_ROOT/failpoints/$NAME.hit"
"$SCRIPT" failpoint wait --run-dir "$TEST_ROOT" --name "$NAME" --timeout-sec 1
"$SCRIPT" failpoint release --run-dir "$TEST_ROOT" --name "$NAME"
[[ -e "$TEST_ROOT/failpoints/$NAME.release" ]] || fail "release file missing"
expect_failure "invalid name" "$SCRIPT" failpoint arm \
  --run-dir "$TEST_ROOT" --name ../escape
expect_failure "invalid failpoint name" "$SCRIPT" failpoint arm \
  --run-dir "$TEST_ROOT" --name bad-name

sleep 30 &
PID=$!
trap 'kill -CONT "$PID" 2>/dev/null || true; kill "$PID" 2>/dev/null || true' EXIT
printf '%s\n' "$PID" >"$TEST_ROOT/pids/master-0.pid"
tr '\0' ' ' <"/proc/$PID/cmdline" >"$TEST_ROOT/pids/master-0.cmd"
"$SCRIPT" process stop --run-dir "$TEST_ROOT" --name master-0
grep -Eq '^State:[[:space:]]+T' "/proc/$PID/status" || fail "process was not stopped"
"$SCRIPT" process continue --run-dir "$TEST_ROOT" --name master-0
for _ in {1..100}; do
  grep -Eq '^State:[[:space:]]+T' "/proc/$PID/status" || break
  sleep 0.01
done
grep -Eq '^State:[[:space:]]+T' "/proc/$PID/status" && fail "process was not continued"
[[ $(wc -l <"$TEST_ROOT/faults/events.tsv") -eq 5 ]] || fail "unexpected event count"

echo "PASS"
