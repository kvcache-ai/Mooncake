"""Host-layout byte geometry for Store-resident KV-cache parts.

A Store Part holds one rank's complete K/V contribution to one page. Which
axes are innermost is decided by the writer's host layout, so a reader that
plans byte ranges must derive every offset, stride and length from a single
description instead of assuming its own layout.

Three host layouts are supported (SGLang host-layout names):

- ``PLHD``: page, layer, head, dim  (``page_first``)
- ``LPHD``: layer, page, head, dim  (``page_first_direct``, ``layer_first``)
- ``HPLD``: head, page, layer, dim  (``page_head``)

``dim`` is always innermost and one (layer, head) element is
``head_dim * itemsize`` bytes wide, so only the relative order of page,
layer and head varies. Everything else (strides, part length, run lengths)
is derived from that order and the logical descriptor.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum

from ..types import require_integer

__all__ = [
    "KVCacheHostLayout",
    "HostLayoutGeometry",
]


class KVCacheHostLayout(str, Enum):
    """Axis order of one rank's KV bytes inside a Store Part."""

    PLHD = "plhd"
    LPHD = "lphd"
    HPLD = "hpld"


# Physical order of the three varying axes; dim is implicit and innermost.
_AXIS_ORDER: dict[KVCacheHostLayout, tuple[str, str, str]] = {
    KVCacheHostLayout.PLHD: ("page", "layer", "head"),
    KVCacheHostLayout.LPHD: ("layer", "page", "head"),
    KVCacheHostLayout.HPLD: ("head", "page", "layer"),
}

# Full extent of one logical axis inside a part.
_AXIS_ATTR = {"page": "page_size", "layer": "layer_count", "head": "kv_heads"}


@dataclass(frozen=True)
class HostLayoutGeometry:
    """Derived byte geometry of one Part under one host layout.

    All values are in bytes and are derived from the logical descriptor
    (page size, layer count, head count, head dim, itemsize). There is no
    independent byte-geometry representation that could drift from it.
    """

    layout: KVCacheHostLayout
    page_size: int
    layer_count: int
    kv_heads: int
    head_dim: int
    itemsize: int

    def __post_init__(self) -> None:
        if not isinstance(self.layout, KVCacheHostLayout):
            raise ValueError("layout must be a KVCacheHostLayout")  # noqa: TRY004
        for name in ("page_size", "layer_count", "kv_heads", "head_dim", "itemsize"):
            require_integer(getattr(self, name), name, minimum=1)

    @property
    def axis_order(self) -> tuple[str, ...]:
        """Physical axis order, dim innermost and implicit."""

        return (*_AXIS_ORDER[self.layout], "dim")

    @property
    def element_bytes(self) -> int:
        """Bytes of one (page, layer, head) element of one component."""

        return self.head_dim * self.itemsize

    @property
    def total_bytes(self) -> int:
        """Bytes of one page's single component (K or V) in this layout."""

        return self.page_size * self.layer_count * self.kv_heads * self.element_bytes

    def axis_extent(self, axis: str) -> int:
        """Full extent of ``axis`` inside this part."""

        if axis not in _AXIS_ATTR:
            raise ValueError(f"unknown axis: {axis}")  # noqa: TRY003
        return int(getattr(self, _AXIS_ATTR[axis]))

    def stride_bytes(self, axis: str) -> int:
        """Stride (bytes) of one step along ``axis``.

        stride(a) = element_bytes * product of extents of axes inner to a.
        """

        order = self.axis_order
        if axis not in order:
            raise ValueError(f"unknown axis: {axis}")  # noqa: TRY003
        if axis == "dim":
            return self.itemsize
        stride = self.element_bytes
        for inner_axis in order[order.index(axis) + 1 :]:
            if inner_axis == "dim":
                continue
            stride *= self.axis_extent(inner_axis)
        return stride

    def byte_offset(
        self, *, page: int, layer: int, head: int, component: int = 0
    ) -> int:
        """Byte offset of one (page, layer, head) element of one component.

        ``component`` is 0 for K and 1 for V; a Part stores K first, then V,
        each ``total_bytes`` long.
        """

        coordinates = (("page", page), ("layer", layer), ("head", head))
        for name, value in coordinates:
            require_integer(value, name)
            if value >= self.axis_extent(name):
                raise ValueError(f"{name} outside this part's geometry")  # noqa: TRY003
        require_integer(component, "component")
        offset = 0
        for axis, value in coordinates:
            offset += value * self.stride_bytes(axis)
        return component * self.total_bytes + offset

    def contiguous_run_bytes(
        self,
        *,
        pages: int = 1,
        layers: int = 1,
        heads: int = 1,
        component: int = 0,
    ) -> int | None:
        """Bytes of the (pages x layers x heads) box, or None if strided.

        A box is a single contiguous byte run exactly when, for every axis it
        spans, that axis's stride equals the box's own inner span at that
        axis; otherwise consecutive steps along the axis skip over bytes the
        box does not cover. The planner merges ranges only when both sides
        report a contiguous run.
        """

        spans = {"page": pages, "layer": layers, "head": heads}
        for name, value in spans.items():
            require_integer(value, name, minimum=1)
            if value > self.axis_extent(name):
                return None
        require_integer(component, "component")
        order = self.axis_order
        for axis, extent in spans.items():
            if extent == 1:
                continue
            inner_span = self.element_bytes
            for inner_axis in order[order.index(axis) + 1 :]:
                if inner_axis == "dim":
                    continue
                inner_span *= spans[inner_axis]
            if self.stride_bytes(axis) != inner_span:
                return None
        return pages * layers * heads * self.element_bytes

    def digest(self) -> str:
        """Stable content digest of this derived geometry description."""

        content = {
            "schema": "kv-cache-host-layout-geometry",
            "layout": self.layout.value,
            "page_size": self.page_size,
            "layer_count": self.layer_count,
            "kv_heads": self.kv_heads,
            "head_dim": self.head_dim,
            "itemsize": self.itemsize,
        }
        encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
        return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
