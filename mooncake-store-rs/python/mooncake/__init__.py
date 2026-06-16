from __future__ import annotations

import importlib.util
import pathlib
import sys

_STORE_EXPORTS = {
    "MooncakeDistributedStore",
    "MooncakeHostMemAllocator",
    "BufferPool",
    "ReplicateConfig",
    "init_tracing",
    "metrics_server_address",
    "metrics_text",
    "start_metrics_server",
    "stop_metrics_server",
}
_BUILD_EXPORTS = {
    "__build_branch__",
    "__build_commit__",
    "__build_info__",
    "__build_time__",
}

__edition__ = "pro"
__version__ = "1.0.0+pro.1"

__all__ = sorted(
    _STORE_EXPORTS
    | _BUILD_EXPORTS
    | {
        "__edition__",
        "__version__",
    }
)


def _load_store_rs_module_alias():
    name = f"{__name__}.store"
    existing = sys.modules.get(name)
    if (
        existing is not None
        and pathlib.Path(getattr(existing, "__file__", "")).suffix == ".py"
    ):
        return existing

    store_py = pathlib.Path(__file__).with_name("store.py")
    spec = importlib.util.spec_from_file_location(name, store_py)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {name} from {store_py}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_build_exports() -> None:
    from ._build_info import BUILD_INFO

    info = dict(BUILD_INFO)
    globals()["__build_branch__"] = info["branch"]
    globals()["__build_commit__"] = info["commit"]
    globals()["__build_time__"] = info["build_time"]
    globals()["__build_info__"] = info


def __getattr__(name: str):
    if name in _STORE_EXPORTS:
        store_module = _load_store_rs_module_alias()
        return getattr(store_module, name)
    if name in _BUILD_EXPORTS:
        _load_build_exports()
        return globals()[name]
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _STORE_EXPORTS | _BUILD_EXPORTS)
