#!/usr/bin/env bash

set -e -o pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${RUNNER_TEMP:=${TMPDIR:-/tmp}}"
source scripts/ci/services.sh

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/usr/local/lib

cleanup() {
  local status=$?
  ci_cleanup_services "$status" || true
  rm -rf /tmp/mooncake_ci_ssd_offload /tmp/mooncake_ci_promotion /tmp/mooncake_ci_prefetch
  exit "$status"
}
trap cleanup EXIT

ci_start_service metadata "$RUNNER_TEMP/mooncake-metadata.log" \
  mooncake_http_metadata_server --port 8080
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
  python mooncake-wheel/tests/test_prefetch_on_exist.py
ci_stop_service prefetch
