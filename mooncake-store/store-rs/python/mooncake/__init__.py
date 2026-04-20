from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version

from ._build_info import BUILD_INFO

_STORE_EXPORTS = {
    "MooncakeDistributedStore",
    "MooncakeHostMemAllocator",
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


def __getattr__(name: str):
    if name in _STORE_EXPORTS:
        from . import store as store_module

        return getattr(store_module, name)
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _STORE_EXPORTS)
