#!/usr/bin/env bash
set -euo pipefail

overlay=${MOONCAKE_PG_JIT_OVERLAY:-/workspace/task/mooncake-pg-jit-overlay}
cache=${MOONCAKE_PG_JIT_DIR:-/workspace/task/pg-jit-matrix-cache}
log_dir=${MOONCAKE_PG_JIT_LOG_DIR:-/workspace/task/pg-jit-matrix-logs}
mkdir -p "$log_dir"
rm -rf "$cache"

run_import() {
  env PYTHONPATH="$overlay" MOONCAKE_PG_JIT_DIR="$cache" \
    MOONCAKE_PG_JIT_VERBOSE=1 "$@"
}

run_timed_import() {
  local start end
  start=$(date +%s)
  env PYTHONPATH="$overlay" MOONCAKE_PG_JIT_DIR="$cache" \
    MOONCAKE_PG_JIT_VERBOSE=1 "$@"
  end=$(date +%s)
  echo "elapsed_s=$((end - start))"
}

echo "[cold]"
{
  run_timed_import python3 -c \
    'import mooncake.pg; print("PG_JIT_COLD_AUTO_OK")'
} >"$log_dir/cold-auto.log" 2>&1

echo "[warm]"
{
  run_timed_import python3 -c \
    'import mooncake.pg; print("PG_JIT_WARM_AUTO_OK")'
} >"$log_dir/warm-auto.log" 2>&1

echo "[concurrent-warm]"
run_import python3 -c 'import mooncake.pg; print("PG_JIT_CONCURRENT_1_OK")' \
  >"$log_dir/concurrent-1.log" 2>&1 &
pid1=$!
run_import python3 -c 'import mooncake.pg; print("PG_JIT_CONCURRENT_2_OK")' \
  >"$log_dir/concurrent-2.log" 2>&1 &
pid2=$!
wait "$pid1"
wait "$pid2"

grep -q 'PG_JIT_COLD_AUTO_OK' "$log_dir/cold-auto.log"
grep -q 'PG_JIT_WARM_AUTO_OK' "$log_dir/warm-auto.log"
grep -q 'PG_JIT_CONCURRENT_1_OK' "$log_dir/concurrent-1.log"
grep -q 'PG_JIT_CONCURRENT_2_OK' "$log_dir/concurrent-2.log"
echo "PG_JIT_IMPORT_MATRIX_OK"
