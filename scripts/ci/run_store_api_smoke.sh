#!/usr/bin/env bash

set -e -o pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${RUNNER_TEMP:=${TMPDIR:-/tmp}}"
source scripts/ci/services.sh

trap 'ci_cleanup_services "$?"' EXIT

mkdir -p /tmp/mooncake_storage
ci_start_service master "$RUNNER_TEMP/mooncake-master.log" mooncake_master \
  --default_kv_lease_ttl=500 \
  --eviction_high_watermark_ratio=0.95 \
  --cluster_id=ci_store_api_smoke \
  --port 50051 \
  --enable_http_metadata_server=true
ci_wait_service master 50051 8080

python scripts/test_upsert_api.py
python -m unittest mooncake-wheel.tests.test_weight_snapshot_api
python scripts/test_async_store.py
python scripts/test_copy_move_api.py
python -m unittest mooncake-wheel.tests.test_safetensor_functions
python scripts/test_drain_http_api.py --timeout-sec 90
