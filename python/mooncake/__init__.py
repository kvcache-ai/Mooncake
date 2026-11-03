"""Public Python package for Mooncake."""

from __future__ import annotations

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

_BUFFER_POOL_EXPORTS = {"BufferPool", "RegisteredBufferPool"}
__all__ = sorted(_BUFFER_POOL_EXPORTS)


def __getattr__(name: str):
    if name == "BufferPool":
        from .store import BufferPool

        return BufferPool
    if name == "RegisteredBufferPool":
        from .buffer_pool import RegisteredBufferPool

        return RegisteredBufferPool
    raise AttributeError(name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _BUFFER_POOL_EXPORTS)
