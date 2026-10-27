#!/bin/bash

setup() {
    echo 2 > "$TONE_BM_RUN_DIR/rdma-exit-$RDMA_RUN_ID"
    test -f "${RDMA_HOSTS_FILE:-}" &&
        test -f "$TONE_BM_RUN_DIR/rdma-$RDMA_RUN_ID/scripts/ci/performance/rdma_collect.py"
}

run() {
    local source_dir="$TONE_BM_RUN_DIR/rdma-$RDMA_RUN_ID"
    timeout --signal=TERM --kill-after=45s 3000s \
        python3 -u "$source_dir/scripts/ci/performance/rdma_collect.py" \
        --hosts "$RDMA_HOSTS_FILE" --benchmark "$source_dir/mooncake-transfer-engine/benchmark" \
        --baseline-sha "$BASELINE_SHA" --candidate-sha "$CANDIDATE_SHA" \
        --harness-sha "$HARNESS_SHA" --run-id "$RDMA_RUN_ID" \
        --output "$TONE_BM_RUN_DIR/results-$RDMA_RUN_ID" &
    rdma_collector_pid=$!
    local status=0
    wait "$rdma_collector_pid" || status=$?
    echo "$status" > "$TONE_BM_RUN_DIR/rdma-exit-$RDMA_RUN_ID"
    if ! kill -0 "$rdma_collector_pid" 2>/dev/null; then
        rdma_collector_pid=""
    fi
    return "$status"
}

teardown() {
    if [[ -n ${rdma_collector_pid:-} ]]; then
        kill -TERM "$rdma_collector_pid" 2>/dev/null || true
        wait "$rdma_collector_pid" 2>/dev/null || true
        rdma_collector_pid=""
    fi
}

parse() {
    local results="$TONE_BM_RUN_DIR/results-$RDMA_RUN_ID"
    if [[ -d "$results" ]]; then
        cp -a "$results/." "$TONE_CURRENT_RESULT_DIR/" || return 1
    fi
    python3 - "$results/comparison.json" "$TONE_BM_RUN_DIR/rdma-exit-$RDMA_RUN_ID" \
        "$TONE_CURRENT_RESULT_DIR/rdma-results.log" <<'PY'
import json
from pathlib import Path
import sys
try:
    with open(sys.argv[1]) as source:
        status = json.load(source)["status"]
    expected = {"PASSED": 0, "FAILED": 1, "ERROR": 2}[status]
    valid = int(Path(sys.argv[2]).read_text()) == expected
    passed = valid and status == "PASSED"
except (OSError, ValueError, KeyError, TypeError):
    valid = passed = False
if not valid:
    # Never publish an earlier successful envelope after a failed setup/retry.
    Path(sys.argv[3]).unlink(missing_ok=True)
print("rdma_perf: " + ("Pass" if passed else "Fail"))
PY
}
