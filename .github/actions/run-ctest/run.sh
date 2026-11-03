#!/usr/bin/env bash
set -e -o pipefail

if [ "$RESERVE_RPC_PORT" = true ]; then
  reserved_ports=$(sysctl -n net.ipv4.ip_local_reserved_ports)
  sudo sysctl -w "net.ipv4.ip_local_reserved_ports=${reserved_ports:+$reserved_ports,}50052"
fi
args=(--parallel "$(nproc)" --output-on-failure)
if [ -n "${CTEST_LABEL_EXCLUDE:-}" ]; then
  args+=(--label-exclude "$CTEST_LABEL_EXCLUDE")
fi
report="$GITHUB_WORKSPACE/${JUNIT_REPORT:-build/test-results/ctest.xml}"
mkdir -p "$(dirname "$report")"
rm -f "$report"
args+=(--output-junit "$report")
cd build
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/usr/local/lib
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
DEFAULT_KV_LEASE_TTL=500 \
  ctest "${args[@]}"
python3 "$GITHUB_ACTION_PATH/../../../scripts/ci/check_test_reports.py" \
  --allow-skipped "$report"
