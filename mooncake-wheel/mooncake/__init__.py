"""Mooncake public Python package."""

__all__ = ["BufferPool", "RegisteredBufferPool"]


def __getattr__(name):
    if name in __all__:
        from mooncake.buffer_pool import BufferPool, RegisteredBufferPool

        return {"BufferPool": BufferPool, "RegisteredBufferPool": RegisteredBufferPool}[name]
    raise AttributeError(name)

