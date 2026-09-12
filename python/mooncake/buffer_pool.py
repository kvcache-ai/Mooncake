from __future__ import annotations

"""Public Python entrypoint for Mooncake local-buffer pools."""

try:
    from mooncake.store import BufferPool
except (ImportError, AttributeError):  # pragma: no cover - depends on built extension
    BufferPool = None  # type: ignore[assignment]

RegisteredBufferPool = BufferPool

__all__ = ["BufferPool", "RegisteredBufferPool"]
