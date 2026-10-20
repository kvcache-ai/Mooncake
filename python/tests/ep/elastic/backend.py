# SPDX-License-Identifier: Apache-2.0
"""Lazy backend selection for the shared elastic EP test harness.

Importing the Mooncake backend never imports NIXL bindings or loads a NIXL
extension. Each optional adapter is imported only after its backend is chosen.
"""

from typing import Any


def _adapter_type(name: str):
    if name == "mooncake":
        from mooncake_adapter import MooncakeAdapter

        return MooncakeAdapter
    if name == "nixl":
        from nixl_adapter import NixlAdapter

        return NixlAdapter
    raise ValueError(f"unsupported elastic EP backend: {name}")


def size_hint(name: str, **kwargs: Any) -> int:
    return _adapter_type(name).size_hint(**kwargs)


def create_backend(name: str, **kwargs: Any):
    return _adapter_type(name)(**kwargs)
