#!/usr/bin/env bash

set -e -o pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${RUNNER_TEMP:=${TMPDIR:-/tmp}}"
source scripts/ci/services.sh

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/usr/local/lib

# Initialized before the EXIT trap below: an exported same-name variable
# from the calling environment must never hand cleanup a path to delete.
scanmeta_run_dir=""

cleanup() {
  local status=$?
  ci_cleanup_services "$status" || true
  rm -rf /tmp/mooncake_ci_ssd_offload /tmp/mooncake_ci_promotion /tmp/mooncake_ci_prefetch /tmp/mooncake_ci_snapshot_close
  # Only remove what this run created: the scanmeta scenario uses a
  # private mktemp directory so an exit (e.g. a port-conflict refusal)
  # can never delete data it does not own.
  if [[ -n "${scanmeta_run_dir:-}" ]]; then
    rm -rf "$scanmeta_run_dir"
  fi
  exit "$status"
}
trap cleanup EXIT

# Loopback only: the smoke's clients are colocated, and the server refuses an
# unauthenticated non-loopback bind.
ci_start_service metadata "$RUNNER_TEMP/mooncake-metadata.log" \
  mooncake_http_metadata_server --host 127.0.0.1 --port 8080
ci_wait_service metadata 8080

mkdir -p /tmp/mooncake_ci_ssd_offload
ci_start_service ssd "$RUNNER_TEMP/mooncake-ssd-master.log" mooncake_master \
  --default_kv_lease_ttl=500 \
  --root_fs_dir=/tmp/mooncake_ci_ssd_offload
ci_wait_service ssd 50051
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  DEFAULT_KV_LEASE_TTL=500 \
  python mooncake-wheel/tests/test_ssd_offload_in_evict.py
ci_stop_service ssd

mkdir -p /tmp/mooncake_ci_promotion
ci_start_service promotion "$RUNNER_TEMP/mooncake-promotion-master.log" mooncake_master \
  --default_kv_lease_ttl=500 \
  --root_fs_dir=/tmp/mooncake_ci_promotion \
  --enable_offload=true \
  --offload_on_evict=true \
  --promotion_on_hit=true \
  --promotion_admission_threshold=1 \
  --promotion_max_per_heartbeat=16
ci_wait_service promotion 50051
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  DEFAULT_KV_LEASE_TTL=500 \
  MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ci_promotion \
  MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=2 \
  MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10 \
  MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
  python mooncake-wheel/tests/test_promotion_on_hit.py
ci_stop_service promotion

# Prefetch-on-exist needs promotion_on_hit=false so the negative control
# (plain is_exist must not promote) is meaningful.
mkdir -p /tmp/mooncake_ci_prefetch
ci_start_service prefetch "$RUNNER_TEMP/mooncake-prefetch-master.log" mooncake_master \
  --default_kv_lease_ttl=500 \
  --root_fs_dir=/tmp/mooncake_ci_prefetch \
  --enable_offload=true \
  --offload_on_evict=true \
  --promotion_on_hit=false
ci_wait_service prefetch 50051
# Pin address/sizing env: the test reads MASTER_SERVER/SEGMENT_SIZE_BYTES
# from the ambient environment, and a leftover value from another harness
# can silently point it at the wrong master.
MASTER_SERVER=127.0.0.1:50051 \
  LOCAL_HOSTNAME=127.0.0.1 \
  SEGMENT_SIZE_BYTES=33554432 \
  LOCAL_BUFFER_SIZE_BYTES=67108864 \
  MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  DEFAULT_KV_LEASE_TTL=500 \
  MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ci_prefetch \
  MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS=2 \
  MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT=10 \
  MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES=10485760 \
  python python/tests/ssd/test_prefetch_on_exist.py
ci_stop_service prefetch

# Snapshot-after-close: background offload only (no eviction pressure), so
# the closed writer's memory replica is the one under test.
mkdir -p /tmp/mooncake_ci_snapshot_close
ci_start_service snapshot "$RUNNER_TEMP/mooncake-snapshot-master.log" mooncake_master \
  --default_kv_lease_ttl=500 \
  --root_fs_dir=/tmp/mooncake_ci_snapshot_close \
  --enable_offload=true
ci_wait_service snapshot 50051
MASTER_SERVER=127.0.0.1:50051 \
  LOCAL_HOSTNAME=127.0.0.1 \
  MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
  MOONCAKE_OFFLOAD_FILE_STORAGE_PATH=/tmp/mooncake_ci_snapshot_close \
  python mooncake-wheel/tests/test_snapshot_after_close.py
ci_stop_service snapshot

# ScanMeta-vs-OpLog backpressure (#5563): an OpLog window below one
# registration burst makes the startup re-registration compete for
# in-flight slots and usually hit TASK_PENDING_LIMIT_EXCEEDED, which the
# client must retry instead of failing (deterministic coverage of the
# retry behavior lives in the C++ unit test). The window stays above one
# bucket so the master's own recovery traffic is not starved. Needs etcd
# for the HA OpLog writer. The master deliberately runs WITHOUT
# --root_fs_dir (but with --enable_offload, which gates the LOCAL_DISK
# segment registry): with a root_fs_dir the master registers shared DISK
# replicas for every key, and reads could be served from them instead of
# the client's LOCAL_DISK replicas that the ScanMeta re-registration is
# responsible for.
# Per-run private directory (etcd data and client SSD cache live here):
# cleanup only ever removes what this run created.
scanmeta_run_dir="$(mktemp -d /tmp/mooncake_ci_scanmeta.XXXXXX)"
mkdir -p "$scanmeta_run_dir/ssd" "$scanmeta_run_dir/etcd-data"
# Refuse pre-occupied ports up front: a stale listener answering /health
# would otherwise satisfy the probes below while our own child dies from
# the bind failure. (Bash /dev/tcp probe: success means something listens.)
port_is_free() {
  ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}
for scanmeta_port in 12379 12380 50051 15052; do
  if ! port_is_free "$scanmeta_port"; then
    echo "port $scanmeta_port already in use; refusing to start scanmeta scenario" >&2
    exit 1
  fi
done
# Prefer a preinstalled etcd binary; otherwise download a pinned one into
# RUNNER_TEMP. The distro package is avoided on purpose: its postinst
# starts a system etcd.service that competes for the default ports, and
# the runner user cannot run bare apt-get anyway.
scanmeta_etcd_bin="$(command -v etcd || true)"
if [[ -z "$scanmeta_etcd_bin" ]]; then
  scanmeta_etcd_arch="$(uname -m)"
  case "$scanmeta_etcd_arch" in
    x86_64) scanmeta_etcd_arch=amd64 ;;
    aarch64 | arm64) scanmeta_etcd_arch=arm64 ;;
  esac
  mkdir -p "$RUNNER_TEMP/etcd-download"
  curl -fsSL "https://github.com/etcd-io/etcd/releases/download/v3.5.21/etcd-v3.5.21-linux-${scanmeta_etcd_arch}.tar.gz" \
    | tar -xz -C "$RUNNER_TEMP/etcd-download" --strip-components=1
  scanmeta_etcd_bin="$RUNNER_TEMP/etcd-download/etcd"
fi
# Distinct ports: a stray system etcd must not collide with this instance.
ci_start_service scanmeta_etcd "$RUNNER_TEMP/mooncake-scanmeta-etcd.log" \
  "$scanmeta_etcd_bin" \
  --name scanmeta \
  --data-dir "$scanmeta_run_dir/etcd-data" \
  --listen-client-urls http://127.0.0.1:12379 \
  --advertise-client-urls http://127.0.0.1:12379 \
  --listen-peer-urls http://127.0.0.1:12380 \
  --initial-advertise-peer-urls http://127.0.0.1:12380 \
  --initial-cluster scanmeta=http://127.0.0.1:12380
# The HA master fails fast on an unreachable backend; port-listening alone
# is not readiness for etcd (same health check as the repo HA e2e suite).
# Each probe is bounded (--max-time) and fails fast if our own child died,
# so a stray listener on the port cannot satisfy the loop.
scanmeta_etcd_ready=0
for _ in $(seq 1 30); do
  if ! kill -0 "${CI_SERVICE_PIDS[scanmeta_etcd]}" 2>/dev/null; then
    echo "scanmeta etcd exited before becoming healthy" >&2
    exit 1
  fi
  if curl -fsS --max-time 2 http://127.0.0.1:12379/health >/dev/null 2>&1; then
    scanmeta_etcd_ready=1
    break
  fi
  sleep 1
done
[[ "$scanmeta_etcd_ready" == 1 ]] || {
  echo "scanmeta etcd did not become healthy" >&2
  exit 1
}
ci_start_service scanmeta "$RUNNER_TEMP/mooncake-scanmeta-master.log" mooncake_master \
  --rpc_address=127.0.0.1 --rpc_port=50051 --metrics_port=15052 \
  --metrics_host=127.0.0.1 \
  --default_kv_lease_ttl=500 \
  --enable_ha=true --ha_backend_type=etcd \
  --ha_backend_connstring=127.0.0.1:12379 \
  --enable_oplog=true --oplog_batch_max_entries=256 \
  --enable_offload=true
# HA readiness: the metrics server exposes /health, whose service_ready
# flips true only after leader election (same check as the repo HA e2e
# suite); a bare port check can race the election. Bounded per probe and
# fails fast if our own child died.
scanmeta_master_ready=0
for _ in $(seq 1 30); do
  if ! kill -0 "${CI_SERVICE_PIDS[scanmeta]}" 2>/dev/null; then
    echo "scanmeta master exited before becoming ready" >&2
    exit 1
  fi
  if curl -fsS --max-time 2 http://127.0.0.1:15052/health 2>/dev/null |
    grep -Eq '"service_ready"[[:space:]]*:[[:space:]]*true'; then
    scanmeta_master_ready=1
    break
  fi
  sleep 1
done
[[ "$scanmeta_master_ready" == 1 ]] || {
  echo "scanmeta master did not become ready" >&2
  exit 1
}
MASTER_SERVER=127.0.0.1:50051 \
LOCAL_HOSTNAME=127.0.0.1 \
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
MOONCAKE_OFFLOAD_FILE_STORAGE_PATH="$scanmeta_run_dir/ssd" \
python mooncake-wheel/tests/test_scanmeta_backpressure_retry.py \
  2>&1 | tee "$RUNNER_TEMP/mooncake-scanmeta-test.log"
# The test itself prints the reopen-phase registration/backpressure
# counts (it captures that phase's native stderr internally); a
# backpressure-free run is legal, deterministic retry coverage is the
# unit test's job.
ci_stop_service scanmeta
ci_stop_service scanmeta_etcd
