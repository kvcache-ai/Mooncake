#!/usr/bin/env bash
# Checks that --ha_leader_lease_ttl_sec and the JSON key ha_leader_lease_ttl_sec
# reach the lease a running HA master is granted, against a local one-member
# etcd (same approach as etcd_leader_hang_e2e.sh):
#   - no flag: the default TTL (5 s) is granted;
#   - 15 and the range boundaries 2 and 60 are granted exactly;
#   - the JSON config key overrides the default;
#   - out-of-range values (1 and 61, flag or JSON) make the master exit
#     nonzero with the validation error, before it connects to etcd.
# A master that exits during a success case fails the run.
#
# Requires mooncake_master from a normal build (no failpoints needed).
set -euo pipefail
# Case 8 refuses a bad JSON value through LOG(FATAL); do not leave a core file.
ulimit -c 0

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MASTER_BIN="${MASTER_BIN:-${ROOT_DIR}/build/mooncake-store/src/mooncake_master}"
ETCD_BIN="${ETCD_BIN:-etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-etcdctl}"
# All ports stay inside the local test range 23000-23999; every case uses a
# base of 231xx: etcd client base+1, peer base+2, master rpc base+20.
BASE=23100

work="$(mktemp -d)"
etcd_pid=""
master_pid=""
current_log=""

cleanup() {
  rc=$?
  set +e
  kill "${master_pid:-}" 2>/dev/null
  wait "${master_pid:-}" 2>/dev/null
  kill "${etcd_pid:-}" 2>/dev/null
  wait "${etcd_pid:-}" 2>/dev/null
  if [[ "${KEEP_WORK:-0}" != 1 ]]; then rm -rf "$work"; fi
  exit "$rc"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  [[ -n "$current_log" ]] && tail -n 30 "$current_log" 2>/dev/null >&2
  exit 1
}

start_etcd() {
  local port=$1 peer=$2 data=$3
  "$ETCD_BIN" --name ttl-check-etcd --data-dir "$data" \
    --listen-client-urls "http://127.0.0.1:${port}" \
    --advertise-client-urls "http://127.0.0.1:${port}" \
    --listen-peer-urls "http://127.0.0.1:${peer}" \
    --initial-advertise-peer-urls "http://127.0.0.1:${peer}" \
    --initial-cluster ttl-check-etcd=http://127.0.0.1:${peer} \
    >"$work/etcd.log" 2>&1 &
  etcd_pid=$!
  for _ in $(seq 1 30); do
    "$ETCDCTL_BIN" --endpoints="127.0.0.1:${port}" --command-timeout=1s \
      endpoint health >/dev/null 2>&1 && return 0
    sleep 1
  done
  fail "etcd on ${port} did not become healthy"
}

start_master() {
  local rpc=$1 log=$2
  shift 2
  current_log="$log"
  "$MASTER_BIN" "$@" \
    --rpc_address=127.0.0.1 --rpc_port="$rpc" \
    --logtostderr=1 >"$log" 2>&1 &
  master_pid=$!
}

view_key() { echo "mooncake-store/${CLUSTER_ID}/master_view"; }

# Reads the master_view lease id and granted TTL from etcd. Only valid
# while the master is running.
read_granted_ttl() {
  local lease granted
  lease=$("$ETCDCTL_BIN" --endpoints="127.0.0.1:${ETCD_PORT}" get \
    "$(view_key)" -w json 2>/dev/null | python3 -c \
    'import json,sys; kvs=json.load(sys.stdin).get("kvs",[]); print(kvs[0].get("lease",0) if kvs else 0)')
  [[ "$lease" != 0 ]] || fail "master_view has no lease attached"
  granted=$("$ETCDCTL_BIN" --endpoints="127.0.0.1:${ETCD_PORT}" lease \
    timetolive "$(printf '%x' "$lease")" -w json 2>/dev/null | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["granted-ttl"])')
  echo "$granted"
}

stop_master() {
  kill "${master_pid:-}" 2>/dev/null || true
  wait "${master_pid:-}" 2>/dev/null || true
  master_pid=""
}

stop_etcd() {
  kill "${etcd_pid:-}" 2>/dev/null || true
  wait "${etcd_pid:-}" 2>/dev/null || true
  etcd_pid=""
  rm -rf "$work/etcd-data"
}

case_no=0
next_case() {
  case_no=$((case_no + 1))
  ETCD_PORT=$((BASE + case_no * 10 + 1))
  stop_etcd
  start_etcd "$ETCD_PORT" $((ETCD_PORT + 1)) "$work/etcd-data"
  CLUSTER_ID="ttl-e2e-case-${case_no}-$$"
  mkdir -p "$work/case$case_no"
}

# Expected-success case: the master publishes its view under a lease whose
# granted TTL must be exactly $1. Any master exit during the wait fails.
run_granted_case() {
  local expect_ttl=$1
  shift
  next_case
  local rpc=$((BASE + case_no * 10 + 20)) log="$work/case$case_no/master.log"
  start_master "$rpc" "$log" \
    --enable_ha=true --ha_backend_type=etcd \
    --etcd_endpoints="127.0.0.1:${ETCD_PORT}" --cluster_id="${CLUSTER_ID}" "$@"
  local granted=""
  for _ in $(seq 1 90); do
    kill -0 "$master_pid" 2>/dev/null ||
      fail "case $case_no: master exited before publishing its view"
    if "$ETCDCTL_BIN" --endpoints="127.0.0.1:${ETCD_PORT}" get "$(view_key)" \
      --print-value-only 2>/dev/null | grep -q "127.0.0.1:${rpc}"; then
      granted=$(read_granted_ttl)
      break
    fi
    sleep 1
  done
  [[ -n "$granted" ]] || fail "case $case_no: no view published in 90 s"
  [[ "$granted" == "$expect_ttl" ]] ||
    fail "case $case_no: granted TTL $granted, expected $expect_ttl"
  grep -q "ha_leader_lease_ttl_sec=$expect_ttl" "$log" ||
    fail "case $case_no does not log the effective TTL"
  echo "case $case_no: granted TTL $granted - PASS"
  stop_master
}

# Expected-rejection case: the out-of-range value is refused by config
# validation before the supervisor connects to etcd (gflags validator at
# ParseCommandLineFlags time for the flag; the range check in
# LoadConfigFromCmdline for the JSON key). The contract asserted is a
# nonzero exit plus the validation error in the log.
run_refused_case() {
  next_case
  local rpc=$((BASE + case_no * 10 + 20)) log="$work/case$case_no/master.log"
  start_master "$rpc" "$log" \
    --enable_ha=true --ha_backend_type=etcd \
    --etcd_endpoints="127.0.0.1:${ETCD_PORT}" --cluster_id="${CLUSTER_ID}" "$@"
  local rc=""
  # Bounded wait for the refusal to complete: a validation exit is
  # immediate, so the bound only fails a wedged binary.
  for _ in $(seq 1 30); do
    kill -0 "$master_pid" 2>/dev/null || break
    sleep 1
  done
  kill -0 "$master_pid" 2>/dev/null &&
    fail "case $case_no: out-of-range TTL was accepted (master still running)"
  wait "$master_pid" 2>/dev/null || rc=$?
  master_pid=""
  [[ -n "$rc" && "$rc" != 0 ]] ||
    fail "case $case_no: out-of-range TTL exited ${rc:-0} instead of refusing"
  grep -q "ha_leader_lease_ttl_sec must be in \[2, 60\]" "$log" ||
    fail "case $case_no: refused master did not log the validation error"
  echo "case $case_no: refused (exit $rc) with the validation error logged - PASS"
}

# 1. Default TTL: no flag -> granted 5.
run_granted_case 5

# 2. Explicit valid values incl. both range boundaries.
for ttl in 15 2 60; do
  run_granted_case "$ttl" --ha_leader_lease_ttl_sec="$ttl"
done

# 3. JSON config value (config_path) reaches the granted lease.
printf '{"ha_leader_lease_ttl_sec": 17}\n' >"$work/config.json"
run_granted_case 17 --config_path="$work/config.json"

# 4. Out-of-range values (flag and JSON): refused at config validation.
for bad in 1 61; do
  run_refused_case --ha_leader_lease_ttl_sec="$bad"
done
printf '{"ha_leader_lease_ttl_sec": 61}\n' >"$work/config-bad.json"
run_refused_case --config_path="$work/config-bad.json"

echo "lease TTL propagation check passed in ${SECONDS}s"
