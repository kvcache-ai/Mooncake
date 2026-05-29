"""Public Python entrypoint for Mooncake registered buffer pools."""

from __future__ import annotations

try:
    from mooncake.store import RegisteredBufferLease, RegisteredBufferPool
except (ImportError, AttributeError):
    raise ImportError(
        "mooncake native extension not available; "
        "run `cargo build -p mooncake-store-py` first"
    )

BufferPool = RegisteredBufferPool

__all__ = ["BufferPool", "RegisteredBufferPool", "RegisteredBufferLease"]
