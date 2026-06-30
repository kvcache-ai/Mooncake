#!/bin/bash
# E2E KVCache A/B: Baseline vs Optimized
# Swaps both master binary AND client .so for proper comparison
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASELINE_MASTER="/tmp/mooncake-baseline/builddir/mooncake-store/src/mooncake_master"
BASELINE_STORE_SO="/tmp/mooncake-baseline/builddir/mooncake-integration/store.cpython-313-x86_64-linux-gnu.so"
OPTIMIZED_MASTER="$SCRIPT_DIR/builddir/mooncake-store/src/mooncake_master"
OPTIMIZED_STORE_SO="$SCRIPT_DIR/builddir/mooncake-integration/store.cpython-313-x86_64-linux-gnu.so"
CONDA_LIB="/data/lizhijun/anaconda3/lib"
FULL_LD="$CONDA_LIB:$SCRIPT_DIR/builddir/mooncake-common:$SCRIPT_DIR/builddir/mooncake-store/src:$SCRIPT_DIR/builddir/mooncake-transfer-engine/src"

MASTER_PORT=50051
HTTP_PORT=8080
METRICS_PORT=23334
N_ROUNDS=${1:-3}

for bin in "$BASELINE_MASTER" "$OPTIMIZED_MASTER"; do
    if [ ! -x "$bin" ]; then echo "ERROR: $bin not found"; exit 1; fi
done
for so in "$BASELINE_STORE_SO" "$OPTIMIZED_STORE_SO"; do
    if [ ! -f "$so" ]; then echo "ERROR: $so not found"; exit 1; fi
done

cleanup() {
    fuser -k $MASTER_PORT/tcp 2>/dev/null || true
    fuser -k $HTTP_PORT/tcp 2>/dev/null || true
    sleep 2
}

run_one() {
    local master_bin="$1"
    local store_so="$2"
    local label="$3"
    local output="/tmp/e2e_${label}.json"

    cleanup

    echo ""
    echo "============================================"
    echo "  E2E $label"
    echo "============================================"

    # Start master
    echo -n "  Starting master..."
    LD_LIBRARY_PATH="$CONDA_LIB" "$master_bin" \
        --enable_http_metadata_server=true --http_metadata_server_port=$HTTP_PORT \
        --eviction_high_watermark_ratio=0.95 --port=$MASTER_PORT \
        --metrics_port=$METRICS_PORT --enable_metric_reporting=false \
        > "/tmp/master_e2e_${label}.log" 2>&1 &
    local master_pid=$!
    sleep 3
    if ! ss -tlnp | grep -q "$MASTER_PORT"; then
        echo " FAILED"
        tail -10 "/tmp/master_e2e_${label}.log"
        return 1
    fi
    echo " PID=$master_pid"

    # Run E2E benchmark
    echo "  Running benchmark..."
    LD_LIBRARY_PATH="$FULL_LD" \
    MOONCAKE_STORE_SO="$store_so" \
    python3 "$SCRIPT_DIR/test_kvcache_e2e.py" \
        --master-addr "127.0.0.1:$MASTER_PORT" \
        --threads "1,2,4,8,16" \
        --prefill-ratio 0.90 \
        --capacity-mb 1024 \
        --value-size 131072 \
        --ops-per-thread 500 \
        --num-keys 8000 \
        --label "$label" \
        --output "$output" 2>&1

    local bench_rc=$?
    echo "  Benchmark exit code: $bench_rc"

    # Cleanup
    kill $master_pid 2>/dev/null || true
    wait $master_pid 2>/dev/null || true
    cleanup

    if [ $bench_rc -ne 0 ]; then
        echo "  [FAILED] $label"
        return 1
    fi
    return 0
}

echo "============================================"
echo " Mooncake Store E2E A/B Comparison"
echo " Rounds: $N_ROUNDS"
echo " Baseline master: $BASELINE_MASTER"
echo " Optimized master: $OPTIMIZED_MASTER"
echo "============================================"

for i in $(seq 1 $N_ROUNDS); do
    echo ""
    echo "######## Round $i/$N_ROUNDS ########"

    run_one "$BASELINE_MASTER" "$BASELINE_STORE_SO" "baseline_r${i}" || \
        echo "  [SKIP] Baseline round $i failed"

    run_one "$OPTIMIZED_MASTER" "$OPTIMIZED_STORE_SO" "optimized_r${i}" || \
        echo "  [SKIP] Optimized round $i failed"
done

# Analysis
echo ""
echo "============================================"
echo " E2E A/B RESULTS SUMMARY"
echo "============================================"

python3 << 'PYEOF'
import json, glob, statistics, os

all_data = {"baseline": [], "optimized": []}
results_dir = "/tmp"

for prefix in ["baseline", "optimized"]:
    files = sorted(glob.glob(f"{results_dir}/e2e_{prefix}_r*.json"))
    for f in files:
        try:
            with open(f) as fh:
                data = json.load(fh)
                if isinstance(data, list):
                    all_data[prefix].extend(data)
                else:
                    all_data[prefix].append(data)
        except (json.JSONDecodeError, IOError) as e:
            print(f"  WARNING: could not read {f}: {e}")

for prefix in ["baseline", "optimized"]:
    data_list = all_data[prefix]
    if not data_list:
        print(f"\n{prefix}: NO DATA")
        continue

    # Group by test type
    by_test = {}
    for d in data_list:
        test = d.get("test", "unknown")
        if test not in by_test:
            by_test[test] = []
        by_test[test].append(d)

    print(f"\n  {prefix} ({len(data_list)} measurements):")
    for test in sorted(by_test.keys()):
        entries = by_test[test]
        ops_vals = [e.get("ops_per_sec", 0) for e in entries]
        if ops_vals:
            avg = statistics.mean(ops_vals)
            print(f"    {test:<25} {avg:>10.0f} ops/s  (n={len(entries)})")

# Cross-comparison
print(f"\n  COMPARISON:")
by_test_base = {}
by_test_opt = {}
for d in all_data["baseline"]:
    t = d.get("test", "unknown")
    by_test_base.setdefault(t, []).append(d.get("ops_per_sec", 0))
for d in all_data["optimized"]:
    t = d.get("test", "unknown")
    by_test_opt.setdefault(t, []).append(d.get("ops_per_sec", 0))

for test in sorted(set(list(by_test_base.keys()) + list(by_test_opt.keys()))):
    b_vals = by_test_base.get(test, [])
    o_vals = by_test_opt.get(test, [])
    if b_vals and o_vals:
        b_avg = statistics.mean(b_vals)
        o_avg = statistics.mean(o_vals)
        pct = (o_avg - b_avg) / b_avg * 100
        print(f"    {test:<25} {b_avg:>10.0f} -> {o_avg:>10.0f} ({pct:+.1f}%)")
    elif b_vals:
        print(f"    {test:<25} {statistics.mean(b_vals):>10.0f} -> NO OPT DATA")
    elif o_vals:
        print(f"    {test:<25} NO BASE DATA -> {statistics.mean(o_vals):>10.0f}")

print("\nDone.")
PYEOF

cleanup
echo "============================================"
echo " E2E A/B Complete"
echo "============================================"
