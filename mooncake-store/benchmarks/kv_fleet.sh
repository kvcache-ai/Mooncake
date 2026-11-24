#!/usr/bin/env bash
# Runs several kv_cache_bench processes side by side and prints one aggregate
# line.
#
# One process is limited by its own transfer thread, so the data plane has to
# be measured by adding processes, not connections. Each process gets its own
# pair of listening ports; everything else is passed through unchanged.
#
# usage: kv_fleet.sh <processes> <total-rate> <connections-per-process> [options]
set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <processes> <total-rate> <connections-per-process> [options]" >&2
    exit 2
fi

BIN=${KV_BENCH_BIN:-./kv_cache_bench}
processes=$1
total_rate=$2
per_process_connections=$3
shift 3

if ! [[ $processes =~ ^[0-9]+$ ]] || ((processes < 1)); then
    echo "processes must be a positive integer" >&2
    exit 2
fi
per_process_rate=$((total_rate / processes))
if ((per_process_rate < 1)); then
    echo "total rate $total_rate leaves no requests for $processes processes" >&2
    exit 2
fi
if ! command -v python3 >/dev/null; then
    echo "python3 is required to aggregate the results" >&2
    exit 2
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

pids=()
for ((i = 0; i < processes; ++i)); do
    port=$((19101 + 2 * i))
    "$BIN" "$@" --rate "$per_process_rate" --connections "$per_process_connections" \
        --provider-port "$port" --consumer-port "$((port + 1))" \
        --key-prefix "p$i/" \
        >"$work/$i.json" 2>"$work/$i.err" &
    pids+=("$!")
done

failed=0
for pid in "${pids[@]}"; do
    wait "$pid" || failed=1
done
if ((failed)); then
    echo "at least one process failed:" >&2
    for err in "$work"/*.err; do
        [[ -s $err ]] && tail -n 2 "$err" >&2
    done
    exit 1
fi

python3 -c '
import json, sys

processes = int(sys.argv[1])
runs = []
for index in range(processes):
    with open(f"{sys.argv[2]}/{index}.json") as handle:
        runs.append(json.loads(handle.readline()))

achieved = sum(run["achieved_rps"] for run in runs)
bandwidth = sum(run["gib_per_s"] for run in runs)
errors = sum(run["errors"] for run in runs)
p99 = max(run["fetch_us"]["p99"] for run in runs)
p50 = max(run["fetch_us"]["p50"] for run in runs)
client_bound = any(run["client_bound"] for run in runs)
working_set = sum(run["working_set_bytes"] for run in runs)
print(json.dumps({
    "tool": "traffic_kv_fleet",
    "processes": processes,
    "achieved_rps": round(achieved, 1),
    "gib_per_s": round(bandwidth, 3),
    "fetch_us": {"p50": p50, "p99": p99},
    "errors": errors,
    "client_bound": client_bound,
    "working_set_bytes": working_set,
}, separators=(",", ":")))
sys.exit(1 if errors else 0)
' "$processes" "$work"
