from __future__ import annotations

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

__all__ = sorted(_STORE_EXPORTS)


def __getattr__(name: str):
    if name in _STORE_EXPORTS:
        from . import store as store_module

        return getattr(store_module, name)
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _STORE_EXPORTS)
