"""Expose the selected P2P extension to the public Store facade."""

import mooncake._store_p2p as _native


def __getattr__(name: str):
    return getattr(_native, name)
