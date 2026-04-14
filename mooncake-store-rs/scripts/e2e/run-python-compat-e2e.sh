#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)"
REDIS_PORT="${MC_STORE_RS_REDIS_PORT:-6380}"
UPSTREAM_DIR="${MOONCAKE_UPSTREAM_DIR:-${ROOT_DIR}/third_party/Mooncake}"
UPSTREAM_BUILD_DIR="${MOONCAKE_UPSTREAM_BUILD_DIR:-${UPSTREAM_DIR}/build-rust}"

if [[ ! -d "${UPSTREAM_DIR}" ]]; then
  echo "Mooncake upstream submodule missing at ${UPSTREAM_DIR}" >&2
  echo "Run: git submodule update --init --recursive" >&2
  exit 1
fi

if ! redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
  redis-server \
    --port "${REDIS_PORT}" \
    --bind 127.0.0.1 \
    --daemonize yes \
    --save '' \
    --appendonly no
fi

export LD_LIBRARY_PATH="${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/tent/src:${UPSTREAM_BUILD_DIR}/mooncake-transfer-engine/src:${LD_LIBRARY_PATH:-}"
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="${ROOT_DIR}/python"

if ! command -v cargo >/dev/null 2>&1; then
  CARGO_ENV="${CARGO_HOME:-${HOME}/.cargo}/env"
  if [[ -f "${CARGO_ENV}" ]]; then
    # shellcheck disable=SC1090
    source "${CARGO_ENV}"
  else
    echo "cargo not found in PATH and ${CARGO_ENV} is missing" >&2
    exit 1
  fi
fi

cd "${ROOT_DIR}"
cargo build -p mooncake-store-py

python3 - <<'PY'
import ctypes
import subprocess
import time
import urllib.request

from mooncake.store import (
    MooncakeDistributedStore,
    ReplicateConfig,
    metrics_server_address,
    metrics_text,
    stop_metrics_server,
)

stamp = int(time.time() * 1000)
keyspace = f"mc/store-rs/py-compat/{stamp}"


def wait_for_embedded_wrh_replication(writer, reader) -> None:
    config = ReplicateConfig(replica_num=2)
    key = "__wrh-ready__"
    payload = b"wrh-ready"
    deadline = time.time() + 10.0
    last_error = None
    while time.time() < deadline:
        try:
            assert writer.put(key, payload, config=config) == 0
            route = writer.query_route(key)
            if route is None or len(route["replicas"]) != 2:
                raise AssertionError(f"replication route not converged: {route}")
            if writer.get(key) == payload and reader.get(key) == payload:
                return
        except Exception as error:
            last_error = error
        time.sleep(0.1)
    raise AssertionError(
        f"embedded WRH replication did not converge before timeout: {last_error!r}"
    )


store = MooncakeDistributedStore()
assert store.setup(
    "127.0.0.1",
    "redis://127.0.0.1:6380/0",
    4 * 1024 * 1024,
    1 * 1024 * 1024,
    "tcp",
    "",
    "",
    stable_id=f"py-compat-{stamp}",
    keyspace=keyspace,
    labels={"pool": "pool-a", "storage": "true"},
) == 0
store_peer = MooncakeDistributedStore()
assert store_peer.setup(
    "127.0.0.1",
    "redis://127.0.0.1:6380/0",
    4 * 1024 * 1024,
    1 * 1024 * 1024,
    "tcp",
    "",
    "",
    stable_id=f"py-compat-peer-{stamp}",
    keyspace=keyspace,
    labels={"pool": "pool-a", "storage": "true"},
) == 0

wait_for_embedded_wrh_replication(store, store_peer)

assert store.put("py-key", b"hello-python") == 0
assert store.get("py-key") == b"hello-python"
assert store_peer.get("py-key") == b"hello-python"
route = store.query_route("py-key")
assert route is not None and route["key"] == "default::py-key"
route_key = f"{keyspace}/objects/default::py-key"
route_exists = subprocess.check_output(
    ["redis-cli", "-u", "redis://127.0.0.1:6380/0", "EXISTS", route_key],
    text=True,
).strip()
assert route_exists == "0"
assert store.is_exist("py-key") is True
assert store.get_size("py-key") == len(b"hello-python")
assert store.batch_is_exist(["py-key", "missing-key"]) == [1, 0]
assert store.get_hostname().startswith("127.0.0.1:")

default_config = ReplicateConfig()
assert default_config.with_soft_pin is False
assert store.put_batch(
    ["put-batch-a", "put-batch-b"],
    [b"alpha", b"beta"],
    config=default_config,
) == 0
assert store.batch_get(["put-batch-a", "put-batch-b"]) == [b"alpha", b"beta"]

replicated_config = ReplicateConfig(replica_num=2)
assert store.put("replicated-key", b"replicated", config=replicated_config) == 0
replicated_route = store.query_route("replicated-key")
assert replicated_route is not None
assert len(replicated_route["replicas"]) == 2
assert len({replica["owner"] for replica in replicated_route["replicas"]}) == 2

preferred_segment = store_peer.list_segments()[0]["segment_name"]
preferred_config = ReplicateConfig(
    replica_num=1,
    preferred_segment=preferred_segment,
)
assert store.batch_put(
    [("policy-batch-a", b"left"), ("policy-batch-b", b"right")],
    config=preferred_config,
) == 0
preferred_route = store.query_route("policy-batch-a")
assert preferred_route is not None
assert preferred_route["replicas"][0]["segment_name"] == preferred_segment

send_buf = ctypes.create_string_buffer(b"zero-copy-payload")
assert store.register_buffer(ctypes.addressof(send_buf), len(send_buf.raw)) == 0
assert store.put_from("py-zc", ctypes.addressof(send_buf), len(send_buf.raw)) == 0
recv_buf = ctypes.create_string_buffer(len(send_buf.raw))
assert store.register_buffer(ctypes.addressof(recv_buf), len(recv_buf.raw)) == 0
bytes_read = store.get_into("py-zc", ctypes.addressof(recv_buf), len(recv_buf.raw))
assert bytes_read == len(send_buf.raw)
assert recv_buf.raw[:bytes_read] == send_buf.raw
assert store.unregister_buffer(ctypes.addressof(send_buf)) == 0
assert store.unregister_buffer(ctypes.addressof(recv_buf)) == 0

assert store.batch_put([("batch-a", b"aaa"), ("batch-b", b"bbb")]) == 0
assert store.batch_get(["batch-a", "batch-b"]) == [b"aaa", b"bbb"]

buf_a = ctypes.create_string_buffer(b"1111")
buf_b = ctypes.create_string_buffer(b"2222")
assert store.register_buffer(ctypes.addressof(buf_a), len(buf_a.raw)) == 0
assert store.register_buffer(ctypes.addressof(buf_b), len(buf_b.raw)) == 0
assert store.batch_put_from([
    ("from-a", ctypes.addressof(buf_a), len(buf_a.raw)),
    ("from-b", ctypes.addressof(buf_b), len(buf_b.raw)),
]) == 0
out_a = ctypes.create_string_buffer(len(buf_a.raw))
out_b = ctypes.create_string_buffer(len(buf_b.raw))
assert store.register_buffer(ctypes.addressof(out_a), len(out_a.raw)) == 0
assert store.register_buffer(ctypes.addressof(out_b), len(out_b.raw)) == 0
sizes = store.batch_get_into([
    ("from-a", ctypes.addressof(out_a), len(out_a.raw)),
    ("from-b", ctypes.addressof(out_b), len(out_b.raw)),
])
assert sizes == [len(buf_a.raw), len(buf_b.raw)]
assert out_a.raw[:sizes[0]] == buf_a.raw
assert out_b.raw[:sizes[1]] == buf_b.raw
assert store.batch_put_from(
    ["raw-from-a", "raw-from-b"],
    [ctypes.addressof(buf_a), ctypes.addressof(buf_b)],
    [len(buf_a.raw), len(buf_b.raw)],
    config=default_config,
) == 0
raw_out_a = ctypes.create_string_buffer(len(buf_a.raw))
raw_out_b = ctypes.create_string_buffer(len(buf_b.raw))
assert store.register_buffer(ctypes.addressof(raw_out_a), len(raw_out_a.raw)) == 0
assert store.register_buffer(ctypes.addressof(raw_out_b), len(raw_out_b.raw)) == 0
raw_sizes = store.batch_get_into(
    ["raw-from-a", "raw-from-b"],
    [ctypes.addressof(raw_out_a), ctypes.addressof(raw_out_b)],
    [len(raw_out_a.raw), len(raw_out_b.raw)],
)
assert raw_sizes == [len(buf_a.raw), len(buf_b.raw)]
assert raw_out_a.raw[:raw_sizes[0]] == buf_a.raw
assert raw_out_b.raw[:raw_sizes[1]] == buf_b.raw
for current in (buf_a, buf_b, out_a, out_b):
    assert store.unregister_buffer(ctypes.addressof(current)) == 0
for current in (raw_out_a, raw_out_b):
    assert store.unregister_buffer(ctypes.addressof(current)) == 0

assert store.batch_put_from_multi_buffers([
    ("mb-a", [b"left-", b"right"]),
    ("mb-b", [b"up", b"stream"]),
]) == 0
assert store.batch_get_buffer(["mb-a", "mb-b"]) == [b"left-right", b"upstream"]

mb_src_a = ctypes.create_string_buffer(b"left-right")
mb_src_b = ctypes.create_string_buffer(b"upstream")
mb_dst_a = ctypes.create_string_buffer(len(mb_src_a.raw))
mb_dst_b = ctypes.create_string_buffer(len(mb_src_b.raw))
for current in (mb_src_a, mb_src_b, mb_dst_a, mb_dst_b):
    assert store.register_buffer(ctypes.addressof(current), len(current.raw)) == 0
raw_multi_put = store.batch_put_from_multi_buffers(
    ["raw-mb-a", "raw-mb-b"],
    [
        [ctypes.addressof(mb_src_a), ctypes.addressof(mb_src_a) + 5],
        [ctypes.addressof(mb_src_b), ctypes.addressof(mb_src_b) + 2],
    ],
    [
        [5, len(mb_src_a.raw) - 5],
        [2, len(mb_src_b.raw) - 2],
    ],
    config=default_config,
)
assert raw_multi_put == [0, 0]
raw_multi_sizes = store.batch_get_into_multi_buffers(
    ["raw-mb-a", "raw-mb-b"],
    [
        [ctypes.addressof(mb_dst_a), ctypes.addressof(mb_dst_a) + 5],
        [ctypes.addressof(mb_dst_b), ctypes.addressof(mb_dst_b) + 2],
    ],
    [
        [5, len(mb_dst_a.raw) - 5],
        [2, len(mb_dst_b.raw) - 2],
    ],
    prefer_alloc_in_same_node=True,
)
assert raw_multi_sizes == [len(mb_src_a.raw), len(mb_src_b.raw)]
assert mb_dst_a.raw[:raw_multi_sizes[0]] == mb_src_a.raw
assert mb_dst_b.raw[:raw_multi_sizes[1]] == mb_src_b.raw
for current in (mb_src_a, mb_src_b, mb_dst_a, mb_dst_b):
    assert store.unregister_buffer(ctypes.addressof(current)) == 0

segments = store.list_segments()
assert len(segments) >= 1

assert store.remove("py-key") == 0
assert store.is_exist("py-key") is False
assert store.get_size("py-key") == 0
assert store.query_route("py-key") is None
assert store.batch_remove(["put-batch-a", "put-batch-b"]) == [0, 0]
assert store.batch_is_exist(["put-batch-a", "put-batch-b"]) == [0, 0]

metrics = metrics_text()
assert "mooncake_store_client_operation_total" in metrics
assert 'operation="put",status="ok"' in metrics
assert 'operation="batch_get",status="ok"' in metrics

metrics_addr = store.start_metrics_server()
assert metrics_server_address() == metrics_addr
with urllib.request.urlopen(f"http://{metrics_addr}/metrics") as response:
    http_metrics = response.read().decode()
assert "mooncake_store_client_operation_total" in http_metrics
assert 'operation="batch_get_into_multi_buffers",status="ok"' in http_metrics
store.stop_metrics_server()
assert metrics_server_address() is None
stop_metrics_server()

store_peer.close()
store.close()
print("python compat ok")
PY
