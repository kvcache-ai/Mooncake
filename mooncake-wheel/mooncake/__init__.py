"""Mooncake public Python package."""

from mooncake.buffer_pool import BufferPool, RegisteredBufferPool

try:
    from mooncake.mooncake_ep_buffer import Buffer
    from mooncake.mooncake_elastic_buffer import ElasticBuffer, EPHandle, EventOverlap
except Exception:
    # EP native extensions are optional for storage-only installations.
    Buffer = None
    ElasticBuffer = None
    EPHandle = None
    EventOverlap = None

__all__ = [
    "BufferPool",
    "RegisteredBufferPool",
    "Buffer",
    "ElasticBuffer",
    "EPHandle",
    "EventOverlap",
]
