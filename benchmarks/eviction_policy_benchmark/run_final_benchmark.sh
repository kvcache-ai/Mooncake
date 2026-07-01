#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${REPO_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"

PYTHON_BIN="${PYTHON_BIN:-python3}"
BENCH="${BENCH:-$SCRIPT_DIR/proxy_aware_hidden_kv_bench.py}"
MASTER_BIN="${MASTER_BIN:-$REPO_ROOT/mooncake/build-docker/mooncake-store/src/mooncake_master}"
OUT_DIR="${OUT_DIR:-/tmp/mooncake_eviction_policy_benchmark_results}"
LOG_DIR="${LOG_DIR:-$OUT_DIR/logs}"
RUN_CASE_FILTER="${RUN_CASE_FILTER:-}"
BENCH_SCALE="${BENCH_SCALE:-0.0025}"

MASTER_RPC_ADDRESS="${MASTER_RPC_ADDRESS:-127.0.0.1}"
MASTER_RPC_PORT="${MASTER_RPC_PORT:-59301}"
MASTER_SERVER="${MASTER_SERVER:-$MASTER_RPC_ADDRESS:$MASTER_RPC_PORT}"
METADATA_HOST="${METADATA_HOST:-127.0.0.1}"
METADATA_PORT="${METADATA_PORT:-19301}"
METADATA_SERVER="${METADATA_SERVER:-http://$METADATA_HOST:$METADATA_PORT/metadata}"

mkdir -p "$OUT_DIR" "$LOG_DIR"
MASTER_PID=""

start_master() {
  local name="$1"
  local flags="$2"
  local log="$LOG_DIR/${name}_master.log"

  pkill -9 -x mooncake_master 2>/dev/null || true
  sleep 1
  rm -f "$log"

  "$MASTER_BIN" \
    --enable_http_metadata_server=true \
    --http_metadata_server_host="$METADATA_HOST" \
    --http_metadata_server_port="$METADATA_PORT" \
    --rpc_address="$MASTER_RPC_ADDRESS" \
    --rpc_port="$MASTER_RPC_PORT" \
    --eviction_high_watermark_ratio=0.95 \
    --eviction_ratio=0.05 \
    $flags >"$log" 2>&1 &

  MASTER_PID=$!
  sleep 5
  if ! ps -p "$MASTER_PID" >/dev/null 2>&1; then
    echo "master exited early for $name; log follows" >&2
    tail -200 "$log" >&2 || true
    return 1
  fi
}

stop_master() {
  if [[ -n "$MASTER_PID" ]]; then
    kill -9 "$MASTER_PID" 2>/dev/null || true
    wait "$MASTER_PID" 2>/dev/null || true
    MASTER_PID=""
  fi
  pkill -9 -x mooncake_master 2>/dev/null || true
  sleep 1
}

run_case() {
  local name="$1"
  local policy="$2"
  local hidden_budget="$3"
  local hidden_ttl="$4"
  local hidden_soft_pin_ttl="$5"
  local kv_ttl="$6"
  local master_flags="$7"
  local bench_flags="${8:-}"
  local output="$OUT_DIR/${name}.json"

  if [[ -n "$RUN_CASE_FILTER" && "$name" != "$RUN_CASE_FILTER" ]]; then
    echo "===== skip $name (RUN_CASE_FILTER=$RUN_CASE_FILTER) ====="
    return 0
  fi

  echo "===== $name ====="
  start_master "$name" "$master_flags"
  "$PYTHON_BIN" "$BENCH" \
    --proxy-mode session \
    --execution-mode request_eviction \
    --sessions 240 \
    --mixed-write-sessions 260 \
    --scale "$BENCH_SCALE" \
    --cooldown-sec 0.1 \
    --post-pressure-sec 0.1 \
    --request-timing-scale 0.05 \
    --global-segment-size 67108864 \
    --local-buffer-size 67108864 \
    --local-hostname "127.0.0.1:$((MASTER_RPC_PORT + 20))" \
    --metadata-server "$METADATA_SERVER" \
    --master-server "$MASTER_SERVER" \
    $bench_flags \
    --output-json "$output"
  stop_master
  "$PYTHON_BIN" - "$output" "$policy" "$hidden_budget" "$hidden_ttl" "$hidden_soft_pin_ttl" "$kv_ttl" <<'PY'
import json
import sys

path, policy, hidden_budget, hidden_ttl, hidden_soft_pin_ttl, kv_ttl = sys.argv[1:]
with open(path, encoding="utf-8") as f:
    data = json.load(f)
data["policy_name"] = policy
data["report_params"] = {
    "hidden_budget": hidden_budget,
    "hidden_ttl": hidden_ttl,
    "hidden_soft_pin_ttl": hidden_soft_pin_ttl,
    "kv_ttl": kv_ttl,
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)
PY
}

summarize() {
  "$PYTHON_BIN" - "$OUT_DIR" <<'PY'
import csv
import json
import sys
from pathlib import Path

out = Path(sys.argv[1])
rows = []
for path in sorted(out.glob("*.json")):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    summary = data["summary"]
    params = data.get("report_params", {})
    rows.append({
        "case": path.stem,
        "policy": data.get("policy_name", ""),
        "hidden_budget": params.get("hidden_budget", ""),
        "hidden_ttl": params.get("hidden_ttl", ""),
        "hidden_soft_pin_ttl": params.get("hidden_soft_pin_ttl", ""),
        "kv_ttl": params.get("kv_ttl", ""),
        "kv_hit": summary["accessed_kv_hit_objects"],
        "kv_total": summary["accessed_kv_total_objects"],
        "kv_hit_rate": summary["accessed_kv_hit_rate"],
        "hidden_hit": summary["accessed_hidden_hit_objects"],
        "hidden_total": summary["accessed_hidden_total_objects"],
        "hidden_hit_rate": summary["accessed_hidden_hit_rate"],
        "saved_compute_ms": summary["accessed_saved_compute_ms"],
        "pressure_write_bytes": summary["pressure_write_bytes"],
    })

with open(out / "summary.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
PY
}

trap stop_master EXIT

for ttl in 500ms 1s 5s 10s; do
  safe_ttl="${ttl//[^A-Za-z0-9]/_}"
  run_case "original_baseline_${safe_ttl}" "Original baseline" "" "" "" "$ttl" \
    "--enable_hidden_type_aware_eviction=false --default_kv_lease_ttl=$ttl --default_kv_soft_pin_ttl=$ttl --allow_hidden_in_global_eviction=false --allow_evict_soft_pinned_objects=false"
done

run_hidden_budget_case() {
  local budget="$1"
  local hidden_ttl="$2"
  local kv_ttl="$3"
  local budget_safe="${budget/./p}"
  local hidden_safe="${hidden_ttl//[^A-Za-z0-9]/_}"
  local kv_safe="${kv_ttl//[^A-Za-z0-9]/_}"
  run_case "hidden_budget_${budget_safe}_h${hidden_safe}_kv${kv_safe}" \
    "Hidden-TTL and Budget" "$budget" "$hidden_ttl" "" "$kv_ttl" \
    "--enable_hidden_type_aware_eviction=true --hidden_memory_budget_ratio=$budget --default_hidden_lease_ttl=$hidden_ttl --soft_pinned_hidden_lease_ttl=$hidden_ttl --default_kv_lease_ttl=$kv_ttl --default_kv_soft_pin_ttl=60s --allow_hidden_in_global_eviction=false --allow_evict_soft_pinned_objects=false"
}

run_hidden_budget_case 0.04 10s 10s
run_hidden_budget_case 0.06 10s 10s
run_hidden_budget_case 0.08 10s 10s
run_hidden_budget_case 0.08 5s 10s
run_hidden_budget_case 0.08 1s 10s
run_hidden_budget_case 0.08 500ms 10s
run_hidden_budget_case 0.10 10s 10s
run_hidden_budget_case 0.12 10s 10s
run_hidden_budget_case 1.0 10s 10s

run_soft_pin_case() {
  local budget="$1"
  local hidden_ttl="$2"
  local soft_ttl="$3"
  local kv_ttl="$4"
  local budget_safe="${budget/./p}"
  local hidden_safe="${hidden_ttl//[^A-Za-z0-9]/_}"
  local soft_safe="${soft_ttl//[^A-Za-z0-9]/_}"
  local kv_safe="${kv_ttl//[^A-Za-z0-9]/_}"
  run_case "soft_pin_budget_${budget_safe}_h${hidden_safe}_soft${soft_safe}_kv${kv_safe}" \
    "All policy" "$budget" "$hidden_ttl" "$soft_ttl" "$kv_ttl" \
    "--enable_hidden_type_aware_eviction=true --hidden_memory_budget_ratio=$budget --default_hidden_lease_ttl=$hidden_ttl --soft_pinned_hidden_lease_ttl=$soft_ttl --default_kv_lease_ttl=$kv_ttl --default_kv_soft_pin_ttl=60s --allow_hidden_in_global_eviction=false --allow_evict_soft_pinned_objects=false" \
    "--soft-pin-video-hidden"
}

run_soft_pin_case 0.08 10s 20s 10s

summarize
echo "Wrote report benchmark results to $OUT_DIR"
