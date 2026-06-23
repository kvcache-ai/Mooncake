"""Public Python entrypoint for Mooncake local-buffer pools."""

from __future__ import annotations

try:
    from mooncake.store import BufferPool
except (ImportError, AttributeError):
    BufferPool = None  # type: ignore[assignment]

__all__ = ["BufferPool"]
