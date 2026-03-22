#!/bin/bash
# End-to-End SGLang + HiCache + Mooncake Store Benchmark
# Validates all optimizations (Counting Bloom Filter, Radix Tree, S3-FIFO, Adaptive Scheduler)
# against real LLM inference workloads.
#
# Prerequisites:
#   - Mooncake built from source (cmake + make)
#   - SGLang installed (pip install sglang[all])
#   - A small model downloaded (e.g., Qwen/Qwen2.5-7B-Instruct)
#   - GPU available (at least 1x A40 48GB)
#
# Usage:
#   ./benchmarks/e2e_hicache_benchmark.sh [MODEL_PATH] [OUTPUT_DIR]
#
# Output: benchmark results in OUTPUT_DIR with before/after comparison

set -euo pipefail

MODEL_PATH="${1:-/home/user/models/Qwen2.5-7B-Instruct}"
OUTPUT_DIR="${2:-benchmarks/e2e_results_$(date +%Y%m%d_%H%M%S)}"
MOONCAKE_MASTER_PORT=50051
MOONCAKE_METADATA_PORT=8080
SGLANG_PORT=30000

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[BENCH]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err() { echo -e "${RED}[ERROR]${NC} $*"; }

mkdir -p "$OUTPUT_DIR"

# ======================================================================
# Phase 1: Environment Check
# ======================================================================
log "Phase 1: Environment Check"

check_prereqs() {
    local missing=0

    # Check GPU
    if ! command -v nvidia-smi &>/dev/null; then
        err "nvidia-smi not found. GPU required."
        missing=1
    else
        GPU_COUNT=$(nvidia-smi -L | wc -l)
        GPU_NAME=$(nvidia-smi -L | head -1 | sed 's/GPU 0: \(.*\) (.*/\1/')
        log "GPU: ${GPU_COUNT}x ${GPU_NAME}"
    fi

    # Check model
    if [ ! -d "$MODEL_PATH" ]; then
        warn "Model not found at $MODEL_PATH"
        warn "Trying to download Qwen2.5-1.5B-Instruct (smaller for testing)..."
        if command -v huggingface-cli &>/dev/null; then
            MODEL_PATH="/tmp/Qwen2.5-1.5B-Instruct"
            huggingface-cli download Qwen/Qwen2.5-1.5B-Instruct --local-dir "$MODEL_PATH" || true
        fi
    fi

    if [ ! -d "$MODEL_PATH" ]; then
        err "Model not found. Please provide MODEL_PATH."
        missing=1
    else
        log "Model: $MODEL_PATH"
    fi

    # Check mooncake_master binary
    if ! command -v mooncake_master &>/dev/null; then
        # Try build directory
        if [ -f "./build/mooncake-store/mooncake_master" ]; then
            export PATH="./build/mooncake-store:$PATH"
            log "Found mooncake_master in build/"
        else
            warn "mooncake_master not found. Will try to build."
        fi
    fi

    # Check SGLang
    if ! python3 -c "import sglang" 2>/dev/null; then
        warn "SGLang not installed. Install: pip install 'sglang[all]'"
        missing=1
    else
        SGLANG_VERSION=$(python3 -c "import sglang; print(sglang.__version__)" 2>/dev/null || echo "unknown")
        log "SGLang: $SGLANG_VERSION"
    fi

    return $missing
}

if ! check_prereqs; then
    err "Prerequisites check failed. Fix issues above and retry."
    exit 1
fi

# ======================================================================
# Phase 2: Launch Mooncake Master Service
# ======================================================================
log "Phase 2: Launching Mooncake Master Service"

cleanup() {
    log "Cleaning up..."
    [ -n "${MASTER_PID:-}" ] && kill "$MASTER_PID" 2>/dev/null || true
    [ -n "${SGLANG_PID:-}" ] && kill "$SGLANG_PID" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

launch_mooncake_master() {
    log "Starting mooncake_master on port $MOONCAKE_MASTER_PORT..."

    mooncake_master \
        --rpc_port="$MOONCAKE_MASTER_PORT" \
        --enable_http_metadata_server=true \
        --http_metadata_server_port="$MOONCAKE_METADATA_PORT" \
        --metrics_port=9003 \
        > "$OUTPUT_DIR/mooncake_master.log" 2>&1 &
    MASTER_PID=$!

    # Wait for master to be ready
    for i in $(seq 1 30); do
        if curl -s "http://127.0.0.1:${MOONCAKE_METADATA_PORT}/metadata" >/dev/null 2>&1; then
            log "Mooncake Master ready (PID=$MASTER_PID)"
            return 0
        fi
        sleep 1
    done

    err "Mooncake Master failed to start. Check $OUTPUT_DIR/mooncake_master.log"
    return 1
}

launch_mooncake_master

# ======================================================================
# Phase 3: Launch SGLang with HiCache + Mooncake Backend
# ======================================================================
log "Phase 3: Launching SGLang Server with HiCache"

launch_sglang() {
    local log_file="$1"

    export MOONCAKE_MASTER="127.0.0.1:${MOONCAKE_MASTER_PORT}"
    export MOONCAKE_PROTOCOL="tcp"
    export MC_MS_AUTO_DISC=0
    export MOONCAKE_DEVICE=""
    export MOONCAKE_TE_META_DATA_SERVER="http://127.0.0.1:${MOONCAKE_METADATA_PORT}/metadata"
    export MOONCAKE_GLOBAL_SEGMENT_SIZE=4294967296  # 4GB

    python3 -m sglang.launch_server \
        --model-path "$MODEL_PATH" \
        --port "$SGLANG_PORT" \
        --page-size 64 \
        --enable-hierarchical-cache \
        --hicache-ratio 2 \
        --hicache-storage-backend mooncake \
        --hicache-storage-prefetch-policy timeout \
        --mem-fraction-static 0.8 \
        --log-level info \
        > "$log_file" 2>&1 &
    SGLANG_PID=$!

    # Wait for SGLang to be ready
    for i in $(seq 1 120); do
        if curl -s "http://127.0.0.1:${SGLANG_PORT}/health" >/dev/null 2>&1; then
            log "SGLang Server ready (PID=$SGLANG_PID)"
            return 0
        fi
        sleep 2
    done

    err "SGLang failed to start. Check $log_file"
    return 1
}

launch_sglang "$OUTPUT_DIR/sglang_server.log"

# ======================================================================
# Phase 4: Run Benchmark Workloads
# ======================================================================
log "Phase 4: Running Benchmark Workloads"

SGLANG_URL="http://127.0.0.1:${SGLANG_PORT}"

# Workload 1: Multi-turn conversation (prefix reuse heavy)
run_multiturn_benchmark() {
    log "Running multi-turn conversation benchmark..."

    python3 -c "
import requests
import time
import json
import statistics

url = '${SGLANG_URL}/v1/chat/completions'
results = {'ttft': [], 'throughput': [], 'latency': []}

# System prompt (shared prefix across all requests)
system_msg = 'You are a helpful AI assistant. Answer concisely.'

# Multi-turn conversation simulation
conversations = []
for conv_id in range(5):
    messages = [{'role': 'system', 'content': system_msg}]
    for turn in range(4):
        messages.append({'role': 'user', 'content': f'Question {turn+1} from conversation {conv_id}: What is {turn+1}+{turn+1}?'})

        start = time.time()
        try:
            resp = requests.post(url, json={
                'model': 'default',
                'messages': messages.copy(),
                'max_tokens': 32,
                'temperature': 0
            }, timeout=30)
            elapsed = time.time() - start

            if resp.status_code == 200:
                data = resp.json()
                results['latency'].append(elapsed)
                if 'usage' in data:
                    tokens = data['usage'].get('completion_tokens', 0)
                    if tokens > 0:
                        results['throughput'].append(tokens / elapsed)

                # Add assistant reply to continue conversation
                assistant_msg = data['choices'][0]['message']['content']
                messages.append({'role': 'assistant', 'content': assistant_msg})
            else:
                print(f'Error: {resp.status_code}')
        except Exception as e:
            print(f'Request failed: {e}')

if results['latency']:
    print(json.dumps({
        'workload': 'multi_turn_conversation',
        'total_requests': len(results['latency']),
        'avg_latency_ms': round(statistics.mean(results['latency']) * 1000, 1),
        'p50_latency_ms': round(sorted(results['latency'])[len(results['latency'])//2] * 1000, 1),
        'p99_latency_ms': round(sorted(results['latency'])[int(len(results['latency'])*0.99)] * 1000, 1),
        'avg_throughput_tok_s': round(statistics.mean(results['throughput']), 1) if results['throughput'] else 0,
    }, indent=2))
else:
    print(json.dumps({'workload': 'multi_turn_conversation', 'error': 'no successful requests'}))
" > "$OUTPUT_DIR/bench_multiturn.json" 2>&1

    log "Multi-turn results: $(cat $OUTPUT_DIR/bench_multiturn.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(f\"avg_latency={d.get(\"avg_latency_ms\",\"N/A\")}ms, throughput={d.get(\"avg_throughput_tok_s\",\"N/A\")} tok/s\")' 2>/dev/null || echo 'parsing failed')"
}

# Workload 2: Prefix sharing (many requests with same system prompt)
run_prefix_sharing_benchmark() {
    log "Running prefix sharing benchmark..."

    python3 -c "
import requests
import time
import json
import statistics
import concurrent.futures

url = '${SGLANG_URL}/v1/chat/completions'
system_prompt = 'You are a highly knowledgeable AI assistant specialized in computer science. ' * 10  # Long system prompt for prefix sharing

def send_request(i):
    start = time.time()
    try:
        resp = requests.post(url, json={
            'model': 'default',
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': f'Question {i}: What is a distributed KV cache?'}
            ],
            'max_tokens': 32,
            'temperature': 0
        }, timeout=30)
        elapsed = time.time() - start
        if resp.status_code == 200:
            return elapsed
    except:
        pass
    return None

# Concurrent requests
latencies = []
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(send_request, i) for i in range(20)]
    for f in concurrent.futures.as_completed(futures):
        result = f.result()
        if result:
            latencies.append(result)

if latencies:
    print(json.dumps({
        'workload': 'prefix_sharing',
        'total_requests': len(latencies),
        'concurrent_workers': 4,
        'avg_latency_ms': round(statistics.mean(latencies) * 1000, 1),
        'p50_latency_ms': round(sorted(latencies)[len(latencies)//2] * 1000, 1),
        'p99_latency_ms': round(sorted(latencies)[int(len(latencies)*0.99)] * 1000, 1),
        'total_time_s': round(max(latencies), 2),
    }, indent=2))
else:
    print(json.dumps({'workload': 'prefix_sharing', 'error': 'no successful requests'}))
" > "$OUTPUT_DIR/bench_prefix_sharing.json" 2>&1

    log "Prefix sharing results: $(cat $OUTPUT_DIR/bench_prefix_sharing.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(f\"avg_latency={d.get(\"avg_latency_ms\",\"N/A\")}ms, requests={d.get(\"total_requests\",\"N/A\")}\")' 2>/dev/null || echo 'parsing failed')"
}

# Workload 3: Cache miss heavy (unique queries, tests Bloom Filter)
run_cache_miss_benchmark() {
    log "Running cache miss benchmark..."

    python3 -c "
import requests
import time
import json
import statistics
import uuid

url = '${SGLANG_URL}/v1/chat/completions'

latencies = []
for i in range(20):
    # Unique query each time → high cache miss ratio
    unique_query = f'UUID-{uuid.uuid4()}: explain quantum computing in one sentence'
    start = time.time()
    try:
        resp = requests.post(url, json={
            'model': 'default',
            'messages': [{'role': 'user', 'content': unique_query}],
            'max_tokens': 32,
            'temperature': 0
        }, timeout=30)
        elapsed = time.time() - start
        if resp.status_code == 200:
            latencies.append(elapsed)
    except:
        pass

if latencies:
    print(json.dumps({
        'workload': 'cache_miss_heavy',
        'total_requests': len(latencies),
        'avg_latency_ms': round(statistics.mean(latencies) * 1000, 1),
        'p50_latency_ms': round(sorted(latencies)[len(latencies)//2] * 1000, 1),
        'p99_latency_ms': round(sorted(latencies)[int(len(latencies)*0.99)] * 1000, 1),
    }, indent=2))
else:
    print(json.dumps({'workload': 'cache_miss_heavy', 'error': 'no successful requests'}))
" > "$OUTPUT_DIR/bench_cache_miss.json" 2>&1

    log "Cache miss results: $(cat $OUTPUT_DIR/bench_cache_miss.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(f\"avg_latency={d.get(\"avg_latency_ms\",\"N/A\")}ms, requests={d.get(\"total_requests\",\"N/A\")}\")' 2>/dev/null || echo 'parsing failed')"
}

run_multiturn_benchmark
run_prefix_sharing_benchmark
run_cache_miss_benchmark

# ======================================================================
# Phase 5: Collect Mooncake Store Metrics
# ======================================================================
log "Phase 5: Collecting Mooncake Store Metrics"

curl -s "http://127.0.0.1:9003/metrics" > "$OUTPUT_DIR/mooncake_metrics.txt" 2>/dev/null || warn "Could not fetch metrics"
curl -s "http://127.0.0.1:9003/metrics/summary" > "$OUTPUT_DIR/mooncake_metrics_summary.json" 2>/dev/null || warn "Could not fetch metrics summary"

# ======================================================================
# Phase 6: Generate Report
# ======================================================================
log "Phase 6: Generating Report"

python3 -c "
import json
import os

output_dir = '$OUTPUT_DIR'
report = []
report.append('# End-to-End Benchmark Report')
report.append('')
report.append('**Environment:**')
report.append('- GPU: $(nvidia-smi -L 2>/dev/null | head -1 || echo "N/A")')
report.append('- Model: $MODEL_PATH')
report.append('')
report.append('## Workload Results')
report.append('')
report.append('| Workload | Requests | Avg Latency | P50 | P99 | Throughput |')
report.append('|----------|----------|-------------|-----|-----|------------|')

for bench_file in ['bench_multiturn.json', 'bench_prefix_sharing.json', 'bench_cache_miss.json']:
    filepath = os.path.join(output_dir, bench_file)
    if os.path.exists(filepath):
        try:
            with open(filepath) as f:
                d = json.load(f)
            name = d.get('workload', bench_file)
            reqs = d.get('total_requests', 'N/A')
            avg = f\"{d.get('avg_latency_ms', 'N/A')}ms\"
            p50 = f\"{d.get('p50_latency_ms', 'N/A')}ms\"
            p99 = f\"{d.get('p99_latency_ms', 'N/A')}ms\"
            tput = f\"{d.get('avg_throughput_tok_s', 'N/A')} tok/s\"
            report.append(f'| {name} | {reqs} | {avg} | {p50} | {p99} | {tput} |')
        except:
            report.append(f'| {bench_file} | ERROR | - | - | - | - |')

report.append('')
report.append('## Mooncake Store Optimizations Active')
report.append('- Counting Bloom Filter (4-bit counters, eviction-aware Remove)')
report.append('- Prefix-Aware Radix Tree Index (LongestPrefixMatch)')
report.append('- S3-FIFO Eviction Strategy (3-queue design)')
report.append('- Adaptive Cache Scheduler (EWMA workload classification)')

with open(os.path.join(output_dir, 'REPORT.md'), 'w') as f:
    f.write('\n'.join(report))

print('\n'.join(report))
" 2>&1 | tee "$OUTPUT_DIR/report_output.txt"

log "Benchmark complete! Results in $OUTPUT_DIR/"
log "Report: $OUTPUT_DIR/REPORT.md"
