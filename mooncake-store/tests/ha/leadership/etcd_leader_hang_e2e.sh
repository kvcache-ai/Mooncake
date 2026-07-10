#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/mooncake-etcd-leader-hang-e2e}"
ETCD_BIN="${ETCD_BIN:-etcd}"
ETCDCTL_BIN="${ETCDCTL_BIN:-etcdctl}"
MASTER_BIN="${MASTER_BIN:-${ROOT_DIR}/build/mooncake-store/src/mooncake_master}"
CLUSTER_ID="${CLUSTER_ID:-mooncake_cluster}"
MASTER_VIEW_KEY="${MASTER_VIEW_KEY:-mooncake-store/${CLUSTER_ID}/master_view}"
MASTER_ADDR="${MASTER_ADDR:-127.0.0.1}"
MASTER_PORT="${MASTER_PORT:-50051}"
MASTER_METRICS_PORT="${MASTER_METRICS_PORT:-9003}"
ENDPOINTS="127.0.0.1:23791,127.0.0.1:23792,127.0.0.1:23793"
ETCD_QUORUM_SIZE="${ETCD_QUORUM_SIZE:-2}"

mkdir -p "${WORK_DIR}"

ETCD_PIDS=()
MASTER_PID=""

cleanup() {
  set +e
  if [[ -n "${MASTER_PID}" ]]; then
    kill "${MASTER_PID}" 2>/dev/null || true
  fi
  for pid in "${ETCD_PIDS[@]}"; do
    kill -CONT "${pid}" 2>/dev/null || true
    kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

start_etcd_node() {
  local name="$1"
  local client_port="$2"
  local peer_port="$3"
  local data_dir="${WORK_DIR}/${name}"

  "${ETCD_BIN}" \
    --name "${name}" \
    --data-dir "${data_dir}" \
    --listen-client-urls "http://127.0.0.1:${client_port}" \
    --advertise-client-urls "http://127.0.0.1:${client_port}" \
    --listen-peer-urls "http://127.0.0.1:${peer_port}" \
    --initial-advertise-peer-urls "http://127.0.0.1:${peer_port}" \
    --initial-cluster "etcd1=http://127.0.0.1:23891,etcd2=http://127.0.0.1:23892,etcd3=http://127.0.0.1:23893" \
    --initial-cluster-state new \
    >"${WORK_DIR}/${name}.log" 2>&1 &
  ETCD_PIDS+=("$!")
}

healthy_endpoints_csv() {
  local endpoints=()
  IFS=',' read -r -a all_endpoints <<<"${ENDPOINTS}"
  for endpoint in "${all_endpoints[@]}"; do
    if "${ETCDCTL_BIN}" --endpoints="${endpoint}" endpoint health >/dev/null 2>&1; then
      endpoints+=("${endpoint}")
    fi
  done
  local IFS=,
  echo "${endpoints[*]}"
}

healthy_endpoint_count() {
  local healthy
  healthy="$(healthy_endpoints_csv)"
  if [[ -z "${healthy}" ]]; then
    echo 0
    return
  fi
  awk -F',' '{print NF}' <<<"${healthy}"
}

dump_logs() {
  set +e
  echo "---- mooncake_master.log ----" >&2
  tail -n 80 "${WORK_DIR}/mooncake_master.log" 2>/dev/null >&2 || true
  for name in etcd1 etcd2 etcd3; do
    echo "---- ${name}.log ----" >&2
    tail -n 40 "${WORK_DIR}/${name}.log" 2>/dev/null >&2 || true
  done
}

wait_for_etcd() {
  for _ in $(seq 1 60); do
    if (( $(healthy_endpoint_count) >= ETCD_QUORUM_SIZE )); then
      return 0
    fi
    sleep 1
  done
  echo "etcd quorum did not become healthy" >&2
  dump_logs
  return 1
}

leader_pid() {
  local live_endpoints
  live_endpoints="$(healthy_endpoints_csv)"
  if [[ -z "${live_endpoints}" ]]; then
    echo "no healthy etcd endpoints" >&2
    return 1
  fi

  local leader_client_url
  leader_client_url="$("${ETCDCTL_BIN}" --endpoints="${live_endpoints}" endpoint status -w json \
    | python3 -c 'import json,sys; data=json.load(sys.stdin); leaders=[x["Endpoint"] for x in data if x["Status"].get("leader")==x["Status"].get("header",{}).get("member_id")]; print(leaders[0] if leaders else "")')"

  case "${leader_client_url}" in
    *23791*) echo "${ETCD_PIDS[0]}" ;;
    *23792*) echo "${ETCD_PIDS[1]}" ;;
    *23793*) echo "${ETCD_PIDS[2]}" ;;
    *) echo "unknown leader endpoint ${leader_client_url}" >&2; return 1 ;;
  esac
}

wait_for_master_view() {
  local expected="$1"
  for _ in $(seq 1 90); do
    local live_endpoints
    live_endpoints="$(healthy_endpoints_csv)"
    if [[ -z "${live_endpoints}" ]]; then
      sleep 1
      continue
    fi

    local value
    value="$("${ETCDCTL_BIN}" --endpoints="${live_endpoints}" get "${MASTER_VIEW_KEY}" --print-value-only 2>/dev/null || true)"
    if [[ "${value}" == "${expected}" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "master_view ${MASTER_VIEW_KEY} did not recover to ${expected}" >&2
  dump_logs
  return 1
}

start_master() {
  "${MASTER_BIN}" \
    --enable_ha=true \
    --ha_backend_type=etcd \
    --etcd_endpoints="${ENDPOINTS}" \
    --cluster_id="${CLUSTER_ID}" \
    --rpc_address="${MASTER_ADDR}" \
    --rpc_port="${MASTER_PORT}" \
    --metrics_port="${MASTER_METRICS_PORT}" \
    >"${WORK_DIR}/mooncake_master.log" 2>&1 &
  MASTER_PID="$!"
}

start_etcd_node etcd1 23791 23891
start_etcd_node etcd2 23792 23892
start_etcd_node etcd3 23793 23893
wait_for_etcd

start_master
wait_for_master_view "${MASTER_ADDR}:${MASTER_PORT}"

STOPPED_LEADER_PID="$(leader_pid)"
kill -STOP "${STOPPED_LEADER_PID}"
wait_for_etcd
wait_for_master_view "${MASTER_ADDR}:${MASTER_PORT}"
kill -CONT "${STOPPED_LEADER_PID}"

KILLED_LEADER_PID="$(leader_pid)"
kill -KILL "${KILLED_LEADER_PID}"
wait_for_etcd
wait_for_master_view "${MASTER_ADDR}:${MASTER_PORT}"

echo "etcd leader hang e2e passed"
