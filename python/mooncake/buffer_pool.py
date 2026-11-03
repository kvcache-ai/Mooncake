"""Public Python entrypoint for Mooncake local-buffer pools."""

from .store import BufferPool

RegisteredBufferPool = BufferPool

__all__ = ["BufferPool", "RegisteredBufferPool"]
