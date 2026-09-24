from __future__ import annotations

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

__edition__ = "rs"
__version__ = "0.1.0"

__all__ = sorted(
    _STORE_EXPORTS
    | _BUILD_EXPORTS
    | {
        "__edition__",
        "__version__",
    }
)


def _load_build_exports() -> None:
    from ._build_info import BUILD_INFO

    info = dict(BUILD_INFO)
    globals()["__build_branch__"] = info["branch"]
    globals()["__build_commit__"] = info["commit"]
    globals()["__build_time__"] = info["build_time"]
    globals()["__build_info__"] = info


def __getattr__(name: str):
    if name in _STORE_EXPORTS:
        from . import store

        return getattr(store, name)
    if name in _BUILD_EXPORTS:
        _load_build_exports()
        return globals()[name]
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _STORE_EXPORTS | _BUILD_EXPORTS)
