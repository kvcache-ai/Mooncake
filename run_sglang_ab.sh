#!/bin/bash
# SGLang HiCache A/B comparison: Baseline vs Optimized Mooncake Store
# Uses Qwen3-4B with long-context prompts to generate substantial KV cache
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASELINE_MASTER="/tmp/mooncake-baseline/builddir/mooncake-store/src/mooncake_master"
OPTIMIZED_MASTER="$SCRIPT_DIR/builddir/mooncake-store/src/mooncake_master"
CONDA_LIB="/data/lizhijun/anaconda3/lib"
FULL_LD="$CONDA_LIB:$SCRIPT_DIR/builddir_py312/mooncake-common:$SCRIPT_DIR/builddir_py312/mooncake-store/src:$SCRIPT_DIR/builddir_py312/mooncake-transfer-engine/src:$SCRIPT_DIR/builddir_py312/mooncake-integration"
SGLANG_PYTHON="/data/lizhijun/anaconda3/envs/sglang/bin/python"
MODEL_PATH="/data-ssd/lizhijun/models/Qwen/Qwen3-4B"
GPU=5
N_RUNS=${1:-5}
MASTER_PORT=50051
SGLANG_PORT=30001
HTTP_PORT=8080

# Long-context prompts to generate substantial KV cache for Store offloading
# Each prompt ~100-200 tokens input, 256 tokens output
PROMPTS=(
    '{"model":"Qwen3-4B","prompt":"Explain in detail the architecture of distributed KV-cache storage systems for large language model inference. Cover memory management strategies, data consistency models, fault tolerance mechanisms, and performance optimization techniques.","max_tokens":256,"temperature":0}'
    '{"model":"Qwen3-4B","prompt":"Write a comprehensive technical analysis comparing different cache eviction policies: LRU, LFU, ARC, LIRS, and TinyLFU. For each policy, describe the algorithm, memory overhead, computational complexity, and expected hit rate under various workload patterns including uniform, Zipfian, and scan-heavy access.","max_tokens":256,"temperature":0}'
    '{"model":"Qwen3-4B","prompt":"Describe the end-to-end process of serving a large language model with hierarchical KV-cache offloading across GPU, CPU, and SSD tiers. Include prefill and decode phase details, memory bandwidth calculations, and latency breakdowns for each tier transition.","max_tokens":256,"temperature":0}'
    '{"model":"Qwen3-4B","prompt":"Design and implement in C++ a high-performance, thread-safe concurrent hash map optimized for read-heavy workloads. Use open addressing with Robin Hood hashing, support lock-free reads via hazard pointers, and implement atomic batch insertions. Include detailed API documentation and performance benchmarks.","max_tokens":256,"temperature":0}'
    '{"model":"Qwen3-4B","prompt":"Analyze the security implications of sharing KV-cache data across tenants in a multi-tenant LLM serving system. Propose a comprehensive isolation framework covering memory encryption, access control, side-channel mitigation, and secure deletion. Compare with existing approaches in cloud database multi-tenancy.","max_tokens":256,"temperature":0}'
)

cleanup() {
    fuser -k $SGLANG_PORT/tcp 2>/dev/null || true
    fuser -k $MASTER_PORT/tcp 2>/dev/null || true
    fuser -k $HTTP_PORT/tcp 2>/dev/null || true
    sleep 2
}

run_round() {
    local master_bin="$1"
    local label="$2"
    local results_file="/tmp/sglang_ab_${label}.jsonl"

    cleanup

    echo ""
    echo "============================================"
    echo "  $label"
    echo "============================================"

    # Start master with extended lease TTL (120s) to prevent warmup key
    # expiration during model loading and CUDA graph capture
    echo -n "  Starting master..."
    LD_LIBRARY_PATH="$CONDA_LIB" "$master_bin" \
        --enable_http_metadata_server=true --http_metadata_server_port=$HTTP_PORT \
        --eviction_high_watermark_ratio=0.95 --port=$MASTER_PORT \
        --metrics_port=23334 --enable_metric_reporting=false \
        --default_kv_lease_ttl=120000 --default_kv_soft_pin_ttl=3600000 \
        > /tmp/master_${label}.log 2>&1 &
    local master_pid=$!
    sleep 3
    if ! ss -tlnp | grep -q "$MASTER_PORT"; then
        echo " FAILED"
        cat /tmp/master_${label}.log | tail -5
        return 1
    fi
    echo " PID=$master_pid"

    # Launch SGLang
    echo -n "  Starting SGLang server (Qwen3-4B, this takes ~30-60s)..."
    CUDA_VISIBLE_DEVICES=$GPU \
    LD_LIBRARY_PATH="$FULL_LD" \
    MOONCAKE_MASTER="127.0.0.1:$MASTER_PORT" \
    MOONCAKE_PROTOCOL="tcp" \
    MOONCAKE_DEVICE="" \
    MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:$HTTP_PORT/metadata" \
    MOONCAKE_GLOBAL_SEGMENT_SIZE="4294967296" \
    MOONCAKE_LOCAL_HOSTNAME="localhost" \
    "$SGLANG_PYTHON" -u -m sglang.launch_server \
        --enable-hierarchical-cache --hicache-storage-backend mooncake \
        --model-path "$MODEL_PATH" --hicache-mem-layout page_first \
        --tp-size 1 --mem-fraction-static 0.7 --max-total-tokens 4096 \
        --host 0.0.0.0 --port $SGLANG_PORT \
        > /tmp/sglang_${label}.log 2>&1 &
    local sglang_pid=$!

    # Wait for server ready (longer timeout for 4B model)
    local waited=0
    while [ $waited -lt 180 ]; do
        if curl -s http://127.0.0.1:$SGLANG_PORT/health > /dev/null 2>&1; then
            echo " ready (${waited}s)"
            break
        fi
        # Check if process died
        if ! kill -0 $sglang_pid 2>/dev/null; then
            echo " DIED"
            tail -20 /tmp/sglang_${label}.log
            kill $master_pid 2>/dev/null || true
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
        if [ $((waited % 10)) -eq 0 ]; then
            echo -n "."
        fi
    done

    if [ $waited -ge 180 ]; then
        echo " TIMEOUT"
        kill $sglang_pid 2>/dev/null || true
        kill $master_pid 2>/dev/null || true
        return 1
    fi

    # Warmup: send 2 requests first (these are cold-start, don't count)
    echo "  Warmup..."
    for i in 0 1; do
        curl -s http://127.0.0.1:$SGLANG_PORT/v1/completions \
            -H "Content-Type: application/json" \
            -d "${PROMPTS[$i]}" > /dev/null 2>&1 || true
        sleep 1
    done

    # Run measured prompts
    echo "  Benchmark prompts..."
    > "$results_file"
    local total_prompts=0
    local total_time=0

    for prompt_json in "${PROMPTS[@]}"; do
        local start=$(date +%s%N)
        local resp=$(curl -s http://127.0.0.1:$SGLANG_PORT/v1/completions \
            -H "Content-Type: application/json" -d "$prompt_json" 2>&1)
        local end=$(date +%s%N)
        local elapsed_ms=$(( (end - start) / 1000000 ))

        # Extract token count from response
        local tokens=$(echo "$resp" | python3 -c "
import sys,json
try:
    d=json.load(sys.stdin)
    print(d.get('usage',{}).get('completion_tokens',0))
except:
    print(0)
" 2>/dev/null || echo 0)

        echo "{\"label\":\"$label\",\"elapsed_ms\":$elapsed_ms,\"tokens\":$tokens}" >> "$results_file"
        total_prompts=$((total_prompts + 1))
        total_time=$((total_time + elapsed_ms))
        if [ "$tokens" -gt 0 ]; then
            echo "    Request ${total_prompts}: ${elapsed_ms}ms, ${tokens} tokens ($(( tokens * 1000 / elapsed_ms )) tok/s)"
        else
            echo "    Request ${total_prompts}: ${elapsed_ms}ms, FAILED (0 tokens)"
        fi
        sleep 0.5
    done

    local avg_ms=$((total_time / total_prompts))
    echo "  Average: ${avg_ms}ms across $total_prompts requests"

    # Cleanup
    kill $sglang_pid 2>/dev/null || true
    wait $sglang_pid 2>/dev/null || true
    kill $master_pid 2>/dev/null || true
    wait $master_pid 2>/dev/null || true
    sleep 2
    return 0
}

echo "============================================"
echo " SGLang HiCache A/B: Qwen3-4B"
echo " Baseline (origin/main) vs Optimized"
echo " Rounds: $N_RUNS"
echo " GPU: $GPU"
echo "============================================"

# Run alternating rounds to eliminate temporal bias
for i in $(seq 1 $N_RUNS); do
    echo ""
    echo "======== Round $i/$N_RUNS ========"

    # Baseline
    if ! run_round "$BASELINE_MASTER" "baseline_r${i}"; then
        echo "  [SKIP] Baseline round $i failed, retrying once..."
        sleep 5
        run_round "$BASELINE_MASTER" "baseline_r${i}" || echo "  [SKIP] Baseline round $i failed again"
    fi

    # Optimized
    if ! run_round "$OPTIMIZED_MASTER" "optimized_r${i}"; then
        echo "  [SKIP] Optimized round $i failed, retrying once..."
        sleep 5
        run_round "$OPTIMIZED_MASTER" "optimized_r${i}" || echo "  [SKIP] Optimized round $i failed again"
    fi
done

# Analysis
echo ""
echo "============================================"
echo " SGLang HiCache A/B RESULTS: Qwen3-4B"
echo "============================================"
echo ""

python3 << 'PYEOF'
import json, glob, statistics, os

results_dir = "/tmp"
all_data = {"baseline": [], "optimized": []}

for prefix in ["baseline", "optimized"]:
    files = sorted(glob.glob(f"{results_dir}/sglang_ab_{prefix}_r*.jsonl"))
    for f in files:
        try:
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    d = json.loads(line)
                    all_data[prefix].append(d)
        except (json.JSONDecodeError, IOError):
            continue

for prefix in ["baseline", "optimized"]:
    data = all_data[prefix]
    if not data:
        print(f"{prefix}: NO DATA")
        continue

    latencies = [d["elapsed_ms"] for d in data]
    tokens_list = [d["tokens"] for d in data if d["tokens"] > 0]
    tp_list = [d["tokens"] / (d["elapsed_ms"] / 1000.0) for d in data if d["tokens"] > 0]

    avg_lat = statistics.mean(latencies) if latencies else 0
    avg_tp = statistics.mean(tp_list) if tp_list else 0
    avg_tok = statistics.mean(tokens_list) if tokens_list else 0

    print(f"{prefix}: n={len(data)}, avg_latency={avg_lat:.0f}ms, "
          f"avg_tokens={avg_tok:.0f}, avg_throughput={avg_tp:.1f} tok/s")

# Comparison
if all_data["baseline"] and all_data["optimized"]:
    b_lat = statistics.mean([d["elapsed_ms"] for d in all_data["baseline"]])
    o_lat = statistics.mean([d["elapsed_ms"] for d in all_data["optimized"]])
    b_tp = statistics.mean([d["tokens"]/(d["elapsed_ms"]/1000.0)
                           for d in all_data["baseline"] if d["tokens"] > 0])
    o_tp = statistics.mean([d["tokens"]/(d["elapsed_ms"]/1000.0)
                           for d in all_data["optimized"] if d["tokens"] > 0])

    lat_pct = (o_lat - b_lat) / b_lat * 100
    tp_pct = (o_tp - b_tp) / b_tp * 100

    print(f"\nComparison (optimized vs baseline):")
    print(f"  Latency:  {b_lat:.0f}ms -> {o_lat:.0f}ms ({lat_pct:+.1f}%)")
    print(f"  Throughput: {b_tp:.1f} -> {o_tp:.1f} tok/s ({tp_pct:+.1f}%)")

print("\nDone.")
PYEOF

cleanup
echo "============================================"
echo " All results saved to /tmp/sglang_ab_*.jsonl"
echo "============================================"
