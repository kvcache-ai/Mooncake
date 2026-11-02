#!/usr/bin/env bash
#
# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -Eeuo pipefail

SCRIPT_PATH="${1:?multinode script path is required}"
SCRIPT_PATH="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)/$(basename "$SCRIPT_PATH")"
TEST_DIR="$(mktemp -d)"
RUN_ID="tebench-script-test-$$"
REMOTE_DIR="/tmp/mooncake-tebench-${RUN_ID}"

cleanup() {
    rm -rf "$TEST_DIR" "$REMOTE_DIR" "${REMOTE_DIR}-failure" \
        "${REMOTE_DIR}-timeout" "${REMOTE_DIR}-invalid-json"
}
trap cleanup EXIT

assert_contains() {
    local text="$1"
    local expected="$2"
    if ! grep -Fq -- "$expected" <<<"$text"; then
        echo "Expected output to contain: $expected" >&2
        echo "$text" >&2
        exit 1
    fi
}

help="$("$SCRIPT_PATH" --help)"
assert_contains "$help" \
    "This launcher builds target plans from tebench's comma-separated"
assert_contains "$help" \
    "weighted targets, hotspot routing, or arbitrary custom target plans."

plan="$("$SCRIPT_PATH" --dry-run --run-id="$RUN_ID" \
    --topology=m2n --targets=target-a,target-b \
    --initiators=initiator-a,initiator-b --range-policy=per_initiator \
    --total-buffer-size=1024 --verify-data)"
assert_contains "$plan" \
    "phase=seed operation=write_seed initiator[0] host=initiator-a targets=SEGMENT_0,SEGMENT_1 offset=0 range=512 threads=2"
assert_contains "$plan" \
    "phase=verify operation=read_verify initiator[1] host=initiator-b targets=SEGMENT_0,SEGMENT_1 offset=512 range=512 threads=2"

plan="$("$SCRIPT_PATH" --dry-run --run-id="$RUN_ID" \
    --topology=all_to_all --nodes=node-a,node-b,node-c \
    --total-buffer-size=1024)"
assert_contains "$plan" \
    "initiator[0] host=node-a targets=SEGMENT_1,SEGMENT_2"
assert_contains "$plan" \
    "initiator[1] host=node-b targets=SEGMENT_0,SEGMENT_2"
assert_contains "$plan" \
    "initiator[2] host=node-c targets=SEGMENT_0,SEGMENT_1"

if "$SCRIPT_PATH" --dry-run --run-id="$RUN_ID" --topology=fanout \
    --targets=target-a,target-b --initiators=initiator-a,initiator-b \
    >"$TEST_DIR/invalid.log" 2>&1; then
    echo "fanout accepted multiple initiators" >&2
    exit 1
fi
assert_contains "$(cat "$TEST_DIR/invalid.log")" \
    "fanout requires exactly one initiator"

if "$SCRIPT_PATH" --dry-run --run-id="$RUN_ID" --topology=hotspot \
    --targets=target-a --initiators=initiator-a \
    >"$TEST_DIR/unsupported.log" 2>&1; then
    echo "unsupported hotspot topology was accepted" >&2
    exit 1
fi
assert_contains "$(cat "$TEST_DIR/unsupported.log")" \
    "--topology must be fanout, incast, m2n, or all_to_all"

MOCK_TEBENCH="$TEST_DIR/mock-tebench"
cat >"$MOCK_TEBENCH" <<'MOCK'
#!/usr/bin/env bash
set -Eeuo pipefail

target_segment=""
operation=""
result_file=""
mock_pid_file=""
mock_exit_code=0
mock_invalid_json=0
for arg in "$@"; do
    case "$arg" in
        --target_seg_name=*) target_segment="${arg#*=}" ;;
        --op_type=*) operation="${arg#*=}" ;;
        --result_output_jsonl=*) result_file="${arg#*=}" ;;
        --mock_pid_file=*) mock_pid_file="${arg#*=}" ;;
        --mock_exit_code=*) mock_exit_code="${arg#*=}" ;;
        --mock_invalid_json=*) mock_invalid_json="${arg#*=}" ;;
    esac
done

if [[ -z "$target_segment" ]]; then
    echo "To start initiators, run"
    echo "  ./tebench --target_seg_name=mock-segment --seg_type=DRAM --backend=tent"
    trap 'exit 0' TERM INT
    while true; do sleep 1; done
fi

if [[ -n "$mock_pid_file" ]]; then
    printf '%s\n' "$$" >"$mock_pid_file"
    sleep 30
fi

[[ "$mock_exit_code" -eq 0 ]] || exit "$mock_exit_code"
if [[ "$mock_invalid_json" -eq 1 ]]; then
    printf 'invalid json\n' >"$result_file"
    exit 0
fi
echo "operation=$operation target=$target_segment"
printf '{"operation":"%s","target":"%s"}\n' \
    "$operation" "$target_segment" >"$result_file"
printf '{"operation":"%s","target":"%s","case":2}\n' \
    "$operation" "$target_segment" >>"$result_file"
MOCK
chmod +x "$MOCK_TEBENCH"

for tebench_path in "" "./mock-tebench"; do
    tebench_args=()
    [[ -z "$tebench_path" ]] || tebench_args+=("--tebench=$tebench_path")
    if (cd "$TEST_DIR" && "$SCRIPT_PATH" --run-id="$RUN_ID" \
        --topology=m2n --targets=local --initiators=local \
        "${tebench_args[@]}" \
        --result-dir="$TEST_DIR/path-results") \
        >"$TEST_DIR/path.log" 2>&1; then
        echo "missing or relative tebench path was accepted" >&2
        exit 1
    fi
    assert_contains "$(cat "$TEST_DIR/path.log")" \
        "--tebench must be an absolute path"
    test ! -e "$TEST_DIR/path-results"
    test ! -e "$REMOTE_DIR"
done

RESULT_DIR="$TEST_DIR/results"
"$SCRIPT_PATH" --run-id="$RUN_ID" --topology=m2n \
    --targets=local --initiators=local --tebench="$MOCK_TEBENCH" \
    --verify-data --duration=1 --total-buffer-size=4096 \
    --result-dir="$RESULT_DIR"

test -f "$RESULT_DIR/local/target-0.log"
test -f "$RESULT_DIR/local/seed-initiator-0.jsonl"
test -f "$RESULT_DIR/local/verify-initiator-0.jsonl"
grep -Fq '"operation":"write_seed"' \
    "$RESULT_DIR/local/seed-initiator-0.jsonl"
grep -Fq '"operation":"read_verify"' \
    "$RESULT_DIR/local/verify-initiator-0.jsonl"
test ! -e "$REMOTE_DIR"

python3 - "$RESULT_DIR" "$RUN_ID" <<'PYTHON'
import json
import sys
from pathlib import Path

result_dir = Path(sys.argv[1])
summary = json.loads((result_dir / "results.json").read_text())
assert summary["schema_version"] == 1
assert summary["run_id"] == sys.argv[2]
assert summary["topology"] == "m2n"
assert summary["backend"] == "tent"
assert len(summary["results"]) == 2
for result, phase in zip(summary["results"], ["seed", "verify"]):
    assert result["host"] == "local"
    assert result["phase"] == phase
    assert result["initiator_index"] == 0
    assert result["exit_code"] == 0
    path = result_dir / "local" / (phase + "-initiator-0.jsonl")
    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert result["records"] == records
    assert len(records) == 2
PYTHON

mkdir "$REMOTE_DIR"
printf 'existing run\n' >"$REMOTE_DIR/marker"
if "$SCRIPT_PATH" --run-id="$RUN_ID" --topology=m2n \
    --targets=local --initiators=local --tebench="$MOCK_TEBENCH" \
    --duration=1 --total-buffer-size=4096 \
    --result-dir="$TEST_DIR/collision-results" \
    >"$TEST_DIR/collision.log" 2>&1; then
    echo "existing run directory was accepted" >&2
    exit 1
fi
assert_contains "$(cat "$TEST_DIR/collision.log")" \
    "Cannot create run directory"
test "$(cat "$REMOTE_DIR/marker")" = "existing run"
test ! -e "$REMOTE_DIR/target-0.log"
rm -rf "$REMOTE_DIR"

if "$SCRIPT_PATH" --run-id="${RUN_ID}-failure" --topology=m2n \
    --targets=local --initiators=local --tebench="$MOCK_TEBENCH" \
    --verify-data --duration=1 --total-buffer-size=4096 \
    --initiator-arg=--mock_exit_code=7 \
    --result-dir="$TEST_DIR/failure-results" \
    >"$TEST_DIR/failure.log" 2>&1; then
    echo "failed seed phase unexpectedly succeeded" >&2
    exit 1
fi
test "$(cat "$TEST_DIR/failure-results/local/seed-initiator-0.rc")" = 7
test -f "$TEST_DIR/failure-results/local/target-0.log"
test ! -e "$TEST_DIR/failure-results/local/verify.start"
test ! -e "${REMOTE_DIR}-failure"
python3 - "$TEST_DIR/failure-results/results.json" <<'PYTHON'
import json
import sys

results = json.load(open(sys.argv[1]))["results"]
assert len(results) == 1
assert results[0]["phase"] == "seed"
assert results[0]["exit_code"] == 7
assert results[0]["records"] == []
PYTHON

if "$SCRIPT_PATH" --run-id="${RUN_ID}-invalid-json" --topology=m2n \
    --targets=local --initiators=local --tebench="$MOCK_TEBENCH" \
    --duration=1 --total-buffer-size=4096 \
    --initiator-arg=--mock_invalid_json=1 \
    --result-dir="$TEST_DIR/invalid-json-results" \
    >"$TEST_DIR/invalid-json.log" 2>&1; then
    echo "invalid JSON result unexpectedly succeeded" >&2
    exit 1
fi
test -f "$TEST_DIR/invalid-json-results/local/benchmark-initiator-0.jsonl"
test ! -e "$TEST_DIR/invalid-json-results/results.json"
test ! -e "$TEST_DIR/invalid-json-results/results.json.tmp"
test ! -e "${REMOTE_DIR}-invalid-json"

TIMEOUT_RUN_ID="${RUN_ID}-timeout"
MOCK_PID_FILE="$TEST_DIR/mock-initiator.pid"
if "$SCRIPT_PATH" --run-id="$TIMEOUT_RUN_ID" --topology=m2n \
    --targets=local --initiators=local --tebench="$MOCK_TEBENCH" \
    --duration=30 --run-timeout=1 --total-buffer-size=4096 \
    "--initiator-arg=--mock_pid_file=$MOCK_PID_FILE" \
    --result-dir="$TEST_DIR/timeout-results"; then
    echo "timed-out initiator unexpectedly succeeded" >&2
    exit 1
fi
test -f "$MOCK_PID_FILE"
if kill -0 "$(cat "$MOCK_PID_FILE")" 2>/dev/null; then
    echo "timed-out initiator was not terminated" >&2
    exit 1
fi
test ! -e "/tmp/mooncake-tebench-${TIMEOUT_RUN_ID}"
python3 - "$TEST_DIR/timeout-results/results.json" <<'PYTHON'
import json
import sys

results = json.load(open(sys.argv[1]))["results"]
assert len(results) == 1
assert results[0]["phase"] == "benchmark"
assert results[0]["exit_code"] is None
assert results[0]["records"] == []
PYTHON
