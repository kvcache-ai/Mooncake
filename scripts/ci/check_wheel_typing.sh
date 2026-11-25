#!/usr/bin/env bash
# Verify that an installed Mooncake wheel ships PEP 561 typing for
# mooncake.engine: the stub matches the compiled module (mypy.stubtest) and a
# downstream-style consumer type-checks under mypy --strict.
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
cp "$typing_dir/engine_consumer.py" "$work_dir/"
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
