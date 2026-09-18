from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import textwrap


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
PYTHON_ROOT = REPOSITORY_ROOT / "python"


def _run_source_script(script: str) -> None:
    bootstrap = f"""
import sys
sys.path.insert(0, {str(PYTHON_ROOT)!r})
{textwrap.dedent(script)}
"""
    subprocess.run(
        [sys.executable, "-I", "-c", bootstrap],
        cwd=REPOSITORY_ROOT,
        check=True,
    )


def test_core_import_does_not_load_vllm_dependencies() -> None:
    _run_source_script(
        f"""
        from importlib.abc import MetaPathFinder
        from pathlib import Path
        import sys
        import types

        optional_roots = {{
            "fastapi",
            "httpx",
            "msgspec",
            "numpy",
            "torch",
            "uvicorn",
            "vllm",
            "zmq",
        }}

        class RejectOptionalImports(MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.partition(".")[0] in optional_roots:
                    raise AssertionError(f"core import loaded optional module: {{fullname}}")
                return None

        buffer_pool = types.ModuleType("mooncake.buffer_pool")
        buffer_pool.BufferPool = object()
        buffer_pool.RegisteredBufferPool = buffer_pool.BufferPool
        sys.modules[buffer_pool.__name__] = buffer_pool
        sys.meta_path.insert(0, RejectOptionalImports())

        import mooncake

        assert Path(mooncake.__file__).resolve() == Path(
            {str(PYTHON_ROOT / "mooncake" / "__init__.py")!r}
        )
        assert not (optional_roots & {{name.partition(".")[0] for name in sys.modules}})
        """
    )


def test_connector_import_and_side_channel_configuration() -> None:
    _run_source_script(
        f"""
        from pathlib import Path
        from types import ModuleType, SimpleNamespace
        import logging
        import os
        import sys

        def install(name, **attributes):
            parent = None
            qualified = ""
            for part in name.split("."):
                qualified = f"{{qualified}}.{{part}}" if qualified else part
                module = sys.modules.get(qualified)
                if module is None:
                    module = ModuleType(qualified)
                    module.__path__ = []
                    sys.modules[qualified] = module
                if parent is not None:
                    setattr(parent, part, module)
                parent = module
            for key, value in attributes.items():
                setattr(parent, key, value)
            return parent

        buffer_pool = ModuleType("mooncake.buffer_pool")
        buffer_pool.BufferPool = object()
        buffer_pool.RegisteredBufferPool = buffer_pool.BufferPool
        sys.modules[buffer_pool.__name__] = buffer_pool

        class MsgspecStruct:
            def __init_subclass__(cls, **kwargs):
                super().__init_subclass__()

        class Tensor:
            pass

        class Socket:
            pass

        class Context:
            pass

        class ContextTerminated(Exception):
            pass

        install("msgspec", Struct=MsgspecStruct)
        install("msgspec.msgpack", Encoder=object, Decoder=object)
        install("numpy")
        install("torch", Tensor=Tensor)
        zmq = install(
            "zmq",
            Context=Context,
            ContextTerminated=ContextTerminated,
            REQ=1,
            RCVTIMEO=2,
            ROUTER=3,
            Socket=Socket,
        )
        zmq.asyncio = install("zmq.asyncio", Context=Context, Socket=Socket)

        class KVConnectorBaseV1:
            pass

        class KVConnectorMetadata:
            pass

        class KVConnectorRole:
            SCHEDULER = "scheduler"
            WORKER = "worker"

        class SupportsHMA:
            pass

        install("vllm.attention.selector", get_attn_backend=lambda *_: None)
        install("vllm.config", VllmConfig=object)
        install(
            "vllm.distributed.kv_transfer.kv_connector.v1.base",
            KVConnectorBase_V1=KVConnectorBaseV1,
            KVConnectorMetadata=KVConnectorMetadata,
            KVConnectorRole=KVConnectorRole,
            SupportsHMA=SupportsHMA,
        )
        install(
            "vllm.distributed.parallel_state",
            get_tensor_model_parallel_rank=lambda: 0,
            get_tp_group=lambda: None,
        )
        install("vllm.forward_context", ForwardContext=object)
        install("vllm.logger", init_logger=lambda name: logging.getLogger(name))
        install(
            "vllm.utils",
            get_ip=lambda: "127.0.0.1",
            make_zmq_path=lambda *args: "",
            make_zmq_socket=lambda *args, **kwargs: None,
        )
        install(
            "vllm.v1.attention.backends.utils",
            get_kv_cache_layout=lambda *_: None,
        )
        install("vllm.v1.core.sched.output", SchedulerOutput=object)
        install("vllm.v1.request", RequestStatus=object)

        os.environ["VLLM_MOONCAKE_SIDE_CHANNEL_PORT"] = "7000"
        import mooncake.mooncake_connector_v1 as connector

        assert Path(connector.__file__).resolve() == Path(
            {str(PYTHON_ROOT / "mooncake" / "mooncake_connector_v1.py")!r}
        )
        config = SimpleNamespace(
            parallel_config=SimpleNamespace(
                data_parallel_rank=3,
                tensor_parallel_size=8,
            )
        )
        assert connector.get_mooncake_side_channel_port(config) == 7024
        assert connector.MooncakeConnector.__module__ == connector.__name__
        """
    )
