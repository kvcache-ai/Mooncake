#!/usr/bin/env bash
# Verify that an installed Mooncake wheel ships PEP 561 typing for
# mooncake.engine and, when the wheel contains the C++ Store extension, for
# mooncake._store and the mooncake.store facade: the stubs match the compiled
# modules (mypy.stubtest) and downstream-style consumers type-check under
# mypy --strict.
#
# Usage: scripts/ci/check_wheel_typing.sh <python>
#   <python> is the interpreter of a venv where the wheel is installed and
#   `import mooncake.engine` works (LD_LIBRARY_PATH already configured).
set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <python>" >&2
    exit 2
fi

python_bin=$1
typing_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/wheel_typing"
mypy_version="${MYPY_VERSION:-2.4.0}"

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
cp "$typing_dir/engine_consumer.py" "$typing_dir/store_consumer.py" "$work_dir/"
cd "$work_dir"
unset PYTHONPATH

"$python_bin" -m pip install --disable-pip-version-check --quiet "mypy==${mypy_version}"

package_dir=$("$python_bin" -I -c \
    'import importlib.util, os; print(os.path.dirname(importlib.util.find_spec("mooncake").origin))')
for marker in py.typed engine.pyi; do
    if [ ! -f "$package_dir/$marker" ]; then
        echo "::error::Installed wheel is missing mooncake/$marker ($package_dir)"
        exit 1
    fi
done

"$python_bin" -I - <<'PYTHON'
from operator import index

from mooncake import engine

for value in (
    engine.TransferOpcode.Read,
    engine.TransferStatus.COMPLETED,
    engine.ScatterTransferCompletionStatus.COMPLETED,
):
    assert int(value) == index(value) == value.value
    assert value != value.value

assert engine.TransferOpcode.Read < engine.TransferOpcode.Write

assert callable(engine.create_rpc_client)
assert callable(engine.create_rpc_server)
assert callable(engine.SharedSegment.create)
assert isinstance(engine.SharedSegment.supported(), bool)
PYTHON

"$python_bin" -m mypy.stubtest mooncake.engine \
    --allowlist "$typing_dir/engine_stubtest_allowlist.txt" \
    --ignore-unused-allowlist

"$python_bin" -m mypy --strict --no-incremental engine_consumer.py

if ! "$python_bin" -I -c \
    'import importlib.util, sys; sys.exit(importlib.util.find_spec("mooncake._store") is None)'; then
    echo "mooncake._store is not installed; skipping the Store typing checks"
    exit 0
fi

for marker in _store.pyi store/__init__.pyi; do
    if [ ! -f "$package_dir/$marker" ]; then
        echo "::error::Installed wheel is missing mooncake/$marker ($package_dir)"
        exit 1
    fi
done

"$python_bin" -I - "$package_dir/store/__init__.pyi" <<'PYTHON'
import ast
import sys
from enum import Enum

from mooncake import _store, store

assert store._BACKEND == "cpp", store._BACKEND
for value in (_store.KVCACHE, _store.FAILED, _store.REPLICA_COPY):
    assert isinstance(value, Enum) and isinstance(value.value, int)
# TaskStatus is exported after ReplicaStatus, so it owns the shared names.
assert _store.FAILED is _store.TaskStatus.FAILED
assert _store.PROCESSING is _store.TaskStatus.PROCESSING
assert _store.RegisteredBufferPool is _store.BufferPool

# The facade stub must list exactly the names the runtime facade exports, and
# every name it re-exports from mooncake._store must be that same object.
stub = ast.parse(open(sys.argv[1]).read())
stub_all = next(
    ast.literal_eval(node.value)
    for node in stub.body
    if isinstance(node, ast.Assign) and node.targets[0].id == "__all__"
)
assert sorted(stub_all) == store.__all__, set(stub_all) ^ set(store.__all__)
reexports = {
    alias.asname
    for node in stub.body
    if isinstance(node, ast.ImportFrom) and node.module == "mooncake._store"
    for alias in node.names
}
assert reexports == store._CPP_EXPORTS - {"EngramStore"} | store._CPP_INTERNAL_EXPORTS
for name in reexports:
    assert getattr(store, name) is getattr(_store, name), name
PYTHON

"$python_bin" -m mypy.stubtest mooncake._store \
    --allowlist "$typing_dir/store_stubtest_allowlist.txt" \
    --ignore-unused-allowlist

"$python_bin" -m mypy --strict --no-incremental store_consumer.py
