from __future__ import annotations

import importlib.util
import pathlib
import sys
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version

from ._build_info import BUILD_INFO

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

_DISTRIBUTION_NAME = "mooncake"
__edition__ = "pro"

try:
    __version__ = package_version(_DISTRIBUTION_NAME)
except PackageNotFoundError:
    __version__ = "1.0.0+pro.1"

__build_branch__ = BUILD_INFO["branch"]
__build_commit__ = BUILD_INFO["commit"]
__build_time__ = BUILD_INFO["build_time"]
__build_info__ = dict(BUILD_INFO)

__all__ = sorted(
    _STORE_EXPORTS
    | {
        "__build_branch__",
        "__build_commit__",
        "__build_info__",
        "__build_time__",
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


def __getattr__(name: str):
    if name in _STORE_EXPORTS:
        store_module = _load_store_rs_module_alias()
        return getattr(store_module, name)
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _STORE_EXPORTS)
