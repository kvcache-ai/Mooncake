#!/usr/bin/env bash

set -e -o pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${RUNNER_TEMP:=${TMPDIR:-/tmp}}"
source scripts/ci/services.sh

trap 'ci_cleanup_services "$?"' EXIT

ci_start_service rpc "$RUNNER_TEMP/rpc-server.log" \
  python -u mooncake-transfer-engine/tests/rpc_communicator_test.py \
  server --url 127.0.0.1:9004 --data-size 1
ci_wait_service rpc 9004

client_rc=0
timeout 10 python -u \
  mooncake-transfer-engine/tests/rpc_communicator_test.py \
  client --url 127.0.0.1:9004 --threads 2 --data-size 1 \
  >"$RUNNER_TEMP/rpc-client.log" 2>&1 || \
  client_rc=$?
cat "$RUNNER_TEMP/rpc-client.log"
if [ "$client_rc" -ne 0 ] && [ "$client_rc" -ne 124 ]; then
  echo "::error::RPC communicator client failed with exit code $client_rc"
  exit "$client_rc"
fi
if ! grep -q '^bandwidth:' "$RUNNER_TEMP/rpc-client.log"; then
  echo "::error::RPC communicator did not complete a successful transfer"
  exit 1
fi
