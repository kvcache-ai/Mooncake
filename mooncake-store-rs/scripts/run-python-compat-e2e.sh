#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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

source /root/.cargo/env
cd "${ROOT_DIR}"
cargo build -p mooncake-store-py

python3 - <<'PY'
import ctypes
import time

from mooncake.store import MooncakeDistributedStore, metrics_text

stamp = int(time.time() * 1000)
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
    keyspace=f"mc/store-rs/py-compat/{stamp}",
) == 0

assert store.put("py-key", b"hello-python") == 0
assert store.get("py-key") == b"hello-python"
assert store.is_exist("py-key") is True

send_buf = ctypes.create_string_buffer(b"zero-copy-payload")
assert store.register_buffer(ctypes.addressof(send_buf), len(send_buf.raw)) == 0
assert store.put_from("py-zc", ctypes.addressof(send_buf), len(send_buf.raw)) == 0
recv_buf = ctypes.create_string_buffer(len(send_buf.raw))
assert store.register_buffer(ctypes.addressof(recv_buf), len(recv_buf.raw)) == 0
bytes_read = store.get_into("py-zc", ctypes.addressof(recv_buf), len(recv_buf.raw))
assert bytes_read == len(send_buf.raw)
assert recv_buf.raw[:bytes_read] == send_buf.raw
assert store.unregister_buffer(ctypes.addressof(send_buf), len(send_buf.raw)) == 0
assert store.unregister_buffer(ctypes.addressof(recv_buf), len(recv_buf.raw)) == 0

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
for current in (buf_a, buf_b, out_a, out_b):
    assert store.unregister_buffer(ctypes.addressof(current), len(current.raw)) == 0

assert store.batch_put_from_multi_buffers([
    ("mb-a", [b"left-", b"right"]),
    ("mb-b", [b"up", b"stream"]),
]) == 0
assert store.batch_get_buffer(["mb-a", "mb-b"]) == [b"left-right", b"upstream"]

segments = store.list_segments()
assert len(segments) >= 1
route = store.query_route("py-key")
assert route is not None and route["key"] == "default::py-key"

metrics = metrics_text()
assert "mooncake_store_client_operation_total" in metrics
assert 'operation="put",status="ok"' in metrics
assert 'operation="batch_get",status="ok"' in metrics

store.close()
print("python compat ok")
PY
