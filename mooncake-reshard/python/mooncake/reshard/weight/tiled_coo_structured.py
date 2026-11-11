"""Adapt tiled COO read plans to Structured Object member ranges."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .tiled_coo import BoundTiledCOOReadPlan, TiledCOOReadPlan

if TYPE_CHECKING:
    from mooncake.structured_object_store import StructuredMemberRange
else:
    StructuredMemberRange = Any


@dataclass(frozen=True)
class TiledCOOStructuredMembers:
    """Structured Object member names for one tiled COO payload."""

    tile_offsets: str
    coordinates: str
    values: str

    def __post_init__(self) -> None:
        names = (self.tile_offsets, self.coordinates, self.values)
        if any(type(name) is not str or not name for name in names):
            raise ValueError("tiled COO member names must be non-empty strings")
        if len(set(names)) != len(names):
            raise ValueError("tiled COO member names must be distinct")


def tiled_coo_directory_ranges(
    plan: TiledCOOReadPlan,
    members: TiledCOOStructuredMembers,
    itemsize: int,
) -> tuple[StructuredMemberRange, ...]:
    """Convert static directory windows to Structured Object byte ranges."""

    if not isinstance(plan, TiledCOOReadPlan):
        raise TypeError("plan must be a TiledCOOReadPlan")
    if not isinstance(members, TiledCOOStructuredMembers):
        raise TypeError("members must be TiledCOOStructuredMembers")
    itemsize = _positive_itemsize(itemsize, "directory")
    member_range = _structured_member_range()
    return tuple(
        member_range(
            member_name=members.tile_offsets,
            source_byte_offset=item.source_index * itemsize,
            byte_length=item.count * itemsize,
            destination_byte_offset=item.destination_index * itemsize,
            alignment=itemsize,
        )
        for item in plan.offset_ranges
    )


def tiled_coo_payload_ranges(
    bound: BoundTiledCOOReadPlan,
    members: TiledCOOStructuredMembers,
    coordinate_itemsize: int,
    value_itemsize: int,
) -> tuple[StructuredMemberRange, ...]:
    """Convert generation-bound COO spans to coordinate and value ranges."""

    if not isinstance(bound, BoundTiledCOOReadPlan):
        raise TypeError("bound must be a BoundTiledCOOReadPlan")
    if not isinstance(members, TiledCOOStructuredMembers):
        raise TypeError("members must be TiledCOOStructuredMembers")
    coordinate_itemsize = _positive_itemsize(coordinate_itemsize, "coordinate")
    value_itemsize = _positive_itemsize(value_itemsize, "value")
    member_range = _structured_member_range()
    ranges: list[StructuredMemberRange] = []
    for item in bound.payload_ranges:
        ranges.extend(
            (
                member_range(
                    member_name=members.coordinates,
                    source_byte_offset=item.source_entry * coordinate_itemsize,
                    byte_length=item.count * coordinate_itemsize,
                    destination_byte_offset=item.destination_entry
                    * coordinate_itemsize,
                    alignment=coordinate_itemsize,
                ),
                member_range(
                    member_name=members.values,
                    source_byte_offset=item.source_entry * value_itemsize,
                    byte_length=item.count * value_itemsize,
                    destination_byte_offset=item.destination_entry * value_itemsize,
                    alignment=value_itemsize,
                ),
            )
        )
    return tuple(ranges)


def _positive_itemsize(value: int, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label} itemsize must be a positive integer")
    return value


def _structured_member_range() -> type[StructuredMemberRange]:
    # Keep the core Reshard planner importable without the optional Store wheel.
    from mooncake.structured_object_store import StructuredMemberRange

    return StructuredMemberRange


__all__ = [
    "TiledCOOStructuredMembers",
    "tiled_coo_directory_ranges",
    "tiled_coo_payload_ranges",
]
