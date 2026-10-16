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
if [ -n "$JUNIT_REPORT" ]; then
  report="$GITHUB_WORKSPACE/$JUNIT_REPORT"
  mkdir -p "$(dirname "$report")"
  args+=(--output-junit "$report")
fi
cd build
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/usr/local/lib
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
DEFAULT_KV_LEASE_TTL=500 \
  ctest "${args[@]}"
