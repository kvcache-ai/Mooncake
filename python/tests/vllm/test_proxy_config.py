from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import textwrap


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PYTHON_ROOT = REPOSITORY_ROOT / "python"


def test_proxy_command_aliases_and_instance_pairing() -> None:
    script = f"""
from pathlib import Path
from types import ModuleType
import sys

sys.path.insert(0, {str(PYTHON_ROOT)!r})

def install(name, **attributes):
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module

buffer_pool = install("mooncake.buffer_pool")
buffer_pool.BufferPool = object()
buffer_pool.RegisteredBufferPool = buffer_pool.BufferPool

class FakeFastAPI:
    def __init__(self, **kwargs):
        self.lifespan = kwargs.get("lifespan")

    def _route(self, *args, **kwargs):
        def decorate(function):
            return function
        return decorate

    get = _route
    post = _route

class Request:
    pass

class StreamingResponse:
    pass

install("httpx")
install("fastapi", FastAPI=FakeFastAPI, Request=Request)
install("fastapi.responses", StreamingResponse=StreamingResponse)

import mooncake.vllm_v1_proxy_server as proxy

assert Path(proxy.__file__).resolve() == Path(
    {str(PYTHON_ROOT / "mooncake" / "vllm_v1_proxy_server.py")!r}
)

sys.argv = [
    "proxy",
    "--host", "0.0.0.0",
    "--port", "9000",
    "--prefiller-host", "prefill-a", "prefill-b",
    "--prefiller-port", "8010", "8011",
    "--decoder-host", "decode-a", "decode-b",
    "--decoder-port", "8020", "8021",
]
args = proxy.parse_args()
assert args.host == "0.0.0.0"
assert args.port == 9000
assert args.prefiller_instances == [("prefill-a", 8010), ("prefill-b", 8011)]
assert args.decoder_instances == [("decode-a", 8020), ("decode-b", 8021)]

sys.argv = [
    "proxy",
    "--prefiller-hosts", "prefill-a", "prefill-b",
    "--prefiller-ports", "8010",
]
try:
    proxy.parse_args()
except ValueError as error:
    assert str(error) == "Number of prefiller hosts must match number of prefiller ports"
else:
    raise AssertionError("mismatched proxy endpoints were accepted")
"""
    subprocess.run(
        [sys.executable, "-I", "-c", textwrap.dedent(script)],
        cwd=REPOSITORY_ROOT,
        check=True,
    )
