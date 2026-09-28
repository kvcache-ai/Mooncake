#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=/results
MASTER_LOG=$ROOT/mooncake-master.log
HOLDER_LOG=$ROOT/mooncake-holder.log
HOLDER2_LOG=$ROOT/mooncake-holder-restarted.log
TEST_LOG=$ROOT/hicache-api-test.log
STATE=$ROOT/state.json

MASTER_PID_FILE=/tmp/mooncake-master.pid
HOLDER_PID_FILE=/tmp/mooncake-holder.pid

MASTER_ADDR=127.0.0.1:50051
METADATA_URL=http://127.0.0.1:8080/metadata
CLIENT_HOST=${MOONCAKE_CLIENT_HOST:-}

if [[ -z "$CLIENT_HOST" ]]; then
    CLIENT_HOST=$(hostname -I 2>/dev/null | tr ' ' '\n' |
        awk '$1 != "" && $1 !~ /^127\./ {print $1; exit}')
fi
if [[ -z "$CLIENT_HOST" ]]; then
    CLIENT_HOST=$(python3 - <<'PY'
import socket

for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
    address = item[4][0]
    if not address.startswith("127."):
        print(address)
        break
PY
    )
fi
[[ "$CLIENT_HOST" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
    { echo "ERROR: no valid client LAN address; set MOONCAKE_CLIENT_HOST" >&2; exit 1; }

HOLDER_ENDPOINT=$CLIENT_HOST:12355
HOLDER2_ENDPOINT=$CLIENT_HOST:12356

stop_pidfile() {
    local pid_file=$1
    local pid=""
    [[ -s "$pid_file" ]] && pid=$(cat "$pid_file" 2>/dev/null || true)
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 20); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
}

cleanup_endpoint() {
    local endpoint=$1
    python3 - "$METADATA_URL" "$endpoint" <<'PY'
import sys
import urllib.parse
import urllib.request

metadata_url, endpoint = sys.argv[1:]
for prefix in ("rpc_meta", "ram"):
    url = metadata_url + "?" + urllib.parse.urlencode(
        {"key": f"mooncake/{prefix}/{endpoint}"}
    )
    try:
        urllib.request.urlopen(
            urllib.request.Request(url, method="DELETE"),
            timeout=5,
        ).read()
    except Exception:
        pass
PY
}

wait_http() {
    local pid=$1
    local url=$2
    local log=$3
    for _ in $(seq 1 60); do
        if ! kill -0 "$pid" 2>/dev/null; then
            tail -200 "$log" >&2 || true
            return 1
        fi
        if python3 - "$url" 2>/dev/null <<'PY'
import sys
import urllib.request

with urllib.request.urlopen(sys.argv[1], timeout=5) as response:
    response.read()
PY
        then
            return 0
        fi
        sleep 1
    done
    tail -200 "$log" >&2 || true
    return 1
}

cleanup_inner() {
    local rc=$?
    stop_pidfile "$HOLDER_PID_FILE"
    stop_pidfile "$MASTER_PID_FILE"
    chmod -R a+rX "$ROOT" 2>/dev/null || true
    exit "$rc"
}
trap cleanup_inner EXIT

test -S "$MOONCAKE_KVCS_EFC_SOCKET"
python3 - <<'PY'
import kvcs
import mooncake.engine
import mooncake.store

print("kvcs:", getattr(kvcs, "__version__", "<unknown>"))
print("kvcs module:", kvcs.__file__)
print("mooncake store:", mooncake.store.__file__)
for name in ("vllm", "sglang", "torch"):
    try:
        __import__(name)
    except ModuleNotFoundError:
        print(f"{name}: absent")
    else:
        raise AssertionError(f"{name} must be absent in the framework-free image")
PY

MOONCAKE_DIR=$(python3 - <<'PY'
import mooncake
import os
print(os.path.dirname(mooncake.__file__))
PY
)
MASTER_BIN=$MOONCAKE_DIR/mooncake_master
CLIENT_BIN=$MOONCAKE_DIR/mooncake_client
test -x "$MASTER_BIN"
test -x "$CLIENT_BIN"

echo "========== start Mooncake master / KVCS backend ==========" | tee "$TEST_LOG"
: >"$MASTER_LOG"
"$MASTER_BIN" \
    --rpc_address=0.0.0.0 --rpc_port=50051 \
    --metrics_host=0.0.0.0 --metrics_port=9001 \
    --enable_http_metadata_server=true \
    --http_metadata_server_host=0.0.0.0 \
    --http_metadata_server_port=8080 \
    >"$MASTER_LOG" 2>&1 &
echo $! >"$MASTER_PID_FILE"
wait_http "$(cat "$MASTER_PID_FILE")" \
    http://127.0.0.1:9001/metrics "$MASTER_LOG"
grep -q "KVCS distributed object storage initialized" "$MASTER_LOG"
echo "RESULT MOONCAKE_KVCS_MASTER: PASS" | tee -a "$TEST_LOG"

start_holder() {
    local endpoint=$1
    local metrics_port=$2
    local log=$3
    stop_pidfile "$HOLDER_PID_FILE"
    cleanup_endpoint "$endpoint"
    : >"$log"
    "$CLIENT_BIN" \
        --host="$endpoint" \
        --metadata_server="$METADATA_URL" \
        --master_server_address="$MASTER_ADDR" \
        --protocol=tcp --port=50052 --threads=8 \
        --global_segment_size="32 MB" \
        --local_buffer_size="64 MB" \
        --enable_http_server=true --http_port="$metrics_port" \
        >"$log" 2>&1 &
    echo $! >"$HOLDER_PID_FILE"
    wait_http "$(cat "$HOLDER_PID_FILE")" \
        "http://127.0.0.1:${metrics_port}/metrics" "$log"
    grep -q "DistributedStorageBackend initialized" "$log"
    grep -Eq "mode=low-level|target=.*mountpoint_index" "$log"
}

echo "========== start HiCache-style Mooncake holder ==========" | tee -a "$TEST_LOG"
echo "client_host=$CLIENT_HOST" | tee -a "$TEST_LOG"
start_holder "$HOLDER_ENDPOINT" 9401 "$HOLDER_LOG"
echo "RESULT MOONCAKE_KVCS_HOLDER: PASS" | tee -a "$TEST_LOG"

cat >/tmp/mooncake-kvcs-hicache-api.py <<'PY'
import argparse
import atexit
import ctypes
import json
import os
import time
import uuid

from mooncake.store import MooncakeDistributedStore, ReplicateConfig


def setup_store(endpoint, metrics_port):
    store = MooncakeDistributedStore()
    rc = store.setup(
        endpoint,
        os.environ["METADATA_URL"],
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        "tcp",
        "",
        os.environ["MASTER_ADDR"],
        None,
        False,
        "",
        "default",
        True,
        metrics_port,
    )
    assert rc == 0, f"Mooncake store setup failed: {rc}"
    return store


def metric_sum(text, name, labels):
    total = 0.0
    for line in text.splitlines():
        if not line.startswith(name):
            continue
        if any(f'{key}="{value}"' not in line for key, value in labels.items()):
            continue
        try:
            total += float(line.rsplit(None, 1)[1])
        except (IndexError, ValueError):
            pass
    return total


def metrics(url):
    import urllib.request

    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


parser = argparse.ArgumentParser()
parser.add_argument("phase", choices=("write", "read"))
args = parser.parse_args()

count = int(os.environ["OBJECT_COUNT"])
value_bytes = int(os.environ["VALUE_BYTES"])
state_path = os.environ["STATE"]
metrics_url = os.environ["METRICS_URL"]

if args.phase == "write":
    store = setup_store(os.environ["REQUESTER_ENDPOINT"], 9403)
    registered_ptr = 0

    def cleanup():
        if registered_ptr:
            try:
                store.unregister_buffer(registered_ptr)
            except Exception:
                pass
        try:
            store.close()
        except Exception:
            pass

    atexit.register(cleanup)

    total = count * value_bytes
    source = ctypes.create_string_buffer(total)
    source_ptr = ctypes.addressof(source)
    for i in range(count):
        pattern = bytes([(i * 17 + 3) & 0xFF])
        begin = i * value_bytes
        ctypes.memset(source_ptr + begin, pattern[0], value_bytes)

    assert store.register_buffer(source_ptr, total) == 0
    registered_ptr = source_ptr

    run_id = uuid.uuid4().hex
    keys = [f"hicache-api-kvcs-{run_id}-{i:04d}" for i in range(count)]
    ptrs = [source_ptr + i * value_bytes for i in range(count)]
    sizes = [value_bytes] * count

    config = ReplicateConfig()
    config.replica_num = 1
    config.dfs_replica_num = 1
    config.preferred_segments = [os.environ["HOLDER_ENDPOINT"]]

    before = metrics(metrics_url)
    results = store.batch_put_from(keys, ptrs, sizes, config)
    assert results == [0] * count, results

    current = metrics(metrics_url)
    put_delta = metric_sum(
        current,
        "mooncake_kvcs_operations_total",
        {"operation": "put", "result": "ok"},
    ) - metric_sum(
        before,
        "mooncake_kvcs_operations_total",
        {"operation": "put", "result": "ok"},
    )

    with open(state_path, "w", encoding="utf-8") as output:
        json.dump(
            {
                "keys": keys,
                "count": count,
                "value_bytes": value_bytes,
            },
            output,
        )
    print(
        "RESULT KVCS_HICACHE_API_WRITE: PASS "
        f"objects={count} put_delta={put_delta:.0f}"
    )
else:
    with open(state_path, encoding="utf-8") as source_file:
        state = json.load(source_file)
    keys = state["keys"]
    assert len(keys) == count

    store = setup_store(os.environ["REQUESTER_ENDPOINT"], 9404)
    registered_ptr = 0

    def cleanup():
        if registered_ptr:
            try:
                store.unregister_buffer(registered_ptr)
            except Exception:
                pass
        try:
            store.close()
        except Exception:
            pass

    atexit.register(cleanup)

    total = count * value_bytes
    destination = ctypes.create_string_buffer(total)
    destination_ptr = ctypes.addressof(destination)
    assert store.register_buffer(destination_ptr, total) == 0
    registered_ptr = destination_ptr

    before = metrics(metrics_url)
    exists = store.batch_is_exist(keys)
    assert exists == [1] * count, exists
    after_exists = metrics(metrics_url)

    ptrs = [destination_ptr + i * value_bytes for i in range(count)]
    sizes = [value_bytes] * count
    got_sizes = store.batch_get_into(keys, ptrs, sizes)
    assert got_sizes == sizes, got_sizes

    for i in range(count):
        expected = bytes([(i * 17 + 3) & 0xFF]) * value_bytes
        actual = destination.raw[i * value_bytes : (i + 1) * value_bytes]
        assert actual == expected, f"payload mismatch: index={i}"

    current = metrics(metrics_url)
    get_delta = metric_sum(
        current,
        "mooncake_kvcs_operations_total",
        {"operation": "get", "result": "ok"},
    ) - metric_sum(
        after_exists,
        "mooncake_kvcs_operations_total",
        {"operation": "get", "result": "ok"},
    )

    print(
        "RESULT KVCS_HICACHE_API_READ: PASS "
        f"objects={count} get_delta={get_delta:.0f}"
    )

    delete_before = metrics(metrics_url)
    remove_results = store.batch_remove(keys, True)
    assert remove_results == [0] * count, remove_results

    current = metrics(metrics_url)
    delete_delta = metric_sum(
        current,
        "mooncake_kvcs_operations_total",
        {"operation": "delete", "result": "ok"},
    ) - metric_sum(
        delete_before,
        "mooncake_kvcs_operations_total",
        {"operation": "delete", "result": "ok"},
    )

    for _ in range(30):
        if store.batch_is_exist(keys) == [0] * count:
            break
        time.sleep(0.2)
    assert store.batch_is_exist(keys) == [0] * count
    print(
        "RESULT KVCS_HICACHE_API_DELETE: PASS "
        f"objects={count} delete_delta={delete_delta:.0f}"
    )

PY

echo "========== HiCache-style Mooncake API write ==========" | tee -a "$TEST_LOG"
env \
    MASTER_ADDR="$MASTER_ADDR" METADATA_URL="$METADATA_URL" \
    STATE="$STATE" HOLDER_ENDPOINT="$HOLDER_ENDPOINT" \
    METRICS_URL=http://127.0.0.1:9401/metrics \
    REQUESTER_ENDPOINT=127.0.0.1:12357 \
    python3 /tmp/mooncake-kvcs-hicache-api.py write 2>&1 | tee -a "$TEST_LOG"

echo "========== restart holder to force KVCS retrieval ==========" | tee -a "$TEST_LOG"
stop_pidfile "$HOLDER_PID_FILE"
cleanup_endpoint "$HOLDER_ENDPOINT"
sleep "$STALE_WAIT_SECONDS"
start_holder "$HOLDER2_ENDPOINT" 9402 "$HOLDER2_LOG"

echo "========== HiCache-style Mooncake API read/delete ==========" | tee -a "$TEST_LOG"
env \
    MASTER_ADDR="$MASTER_ADDR" METADATA_URL="$METADATA_URL" \
    STATE="$STATE" HOLDER_ENDPOINT="$HOLDER2_ENDPOINT" \
    METRICS_URL=http://127.0.0.1:9404/metrics \
    REQUESTER_ENDPOINT=127.0.0.1:12358 \
    python3 /tmp/mooncake-kvcs-hicache-api.py read 2>&1 | tee -a "$TEST_LOG"

grep -E "mode=low-level|target=.*mountpoint_index|DistributedStorageBackend initialized" \
    "$HOLDER_LOG" "$HOLDER2_LOG" | tail -30
echo "RESULT STATUS: PASS" | tee -a "$TEST_LOG"
