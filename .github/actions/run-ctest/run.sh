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
if [ -n "${CTEST_EXCLUDE_REGEX:-}" ]; then
  args+=(--exclude-regex "$CTEST_EXCLUDE_REGEX")
fi
if [ -n "${CTEST_TIMEOUT:-}" ]; then
  args+=(--timeout "$CTEST_TIMEOUT")
fi
if [ -n "$JUNIT_REPORT" ]; then
  report="$GITHUB_WORKSPACE/$JUNIT_REPORT"
  mkdir -p "$(dirname "$report")"
  args+=(--output-junit "$report")
fi
cd build
# Seed CTest's cost data on fresh CI builds so the longest-running tests start
# immediately instead of trailing at the end of a parallel run.
cost_data="Testing/Temporary/CTestCostData.txt"
if [ ! -f "$cost_data" ]; then
  mkdir -p "$(dirname "$cost_data")"
  cat > "$cost_data" <<'EOF'
pybind_client_test 1 390
master_service_ha_test 1 217
master_service_test 1 179
rpc_timeout_test 1 96
high_availability_test 1 96
dfs_sync_client_test 1 82
file_storage_test 1 80
dummy_client_get_buffer_test 1 78
etcd_wrapper_tls_integration_test 1 76
tcp_write_visibility_test 1 73
task_integration_test 1 69
health_check_test 1 50
storage_backend_e2e_test 1 48
master_service_ssd_test 1 46
promotion_on_hit_test 1 45
master_service_evict_scenario_test 1 39
store_gather_read_test 1 31
client_integration_test 1 26
---
EOF
fi
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:/usr/local/lib
MC_METADATA_SERVER=http://127.0.0.1:8080/metadata \
DEFAULT_KV_LEASE_TTL=500 \
  ctest "${args[@]}"
