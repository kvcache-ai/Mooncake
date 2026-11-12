from __future__ import annotations

import sys
from types import ModuleType

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    TiledCOOLayout,
    TiledCOOStructuredMembers,
    plan_tiled_coo_read,
    tiled_coo_directory_ranges,
    tiled_coo_payload_ranges,
)

StructuredMemberRange = pytest.importorskip(
    "mooncake._structured_ranges"
).StructuredMemberRange


MEMBERS = TiledCOOStructuredMembers("tile_offsets", "coordinates", "values")


@pytest.fixture(autouse=True)
def _structured_object_contract(monkeypatch) -> None:
    module = ModuleType("mooncake.structured_object_store")
    module.StructuredMemberRange = StructuredMemberRange
    monkeypatch.setitem(sys.modules, module.__name__, module)


def _plan():
    layout = TiledCOOLayout.build(
        ("w",), ((256, 128),), ((0, 0),), tile_shape=(128, 64)
    )
    target = PlacementFragment("w", (0, 0), (256, 64), 256 * 64 * 2, ParallelRank(tp=0))
    return plan_tiled_coo_read(layout, (target,))


def test_directory_ranges_preserve_static_packed_windows() -> None:
    ranges = tiled_coo_directory_ranges(_plan(), MEMBERS, 4)

    assert ranges == (StructuredMemberRange("tile_offsets", 0, 16, 0, alignment=4),)


def test_payload_ranges_bind_fresh_coordinate_and_value_spans() -> None:
    bound = _plan().bind_offsets((0, 2, 4, 7))

    assert tiled_coo_payload_ranges(bound, MEMBERS, 4, 2) == (
        StructuredMemberRange("coordinates", 0, 8, 0, alignment=4),
        StructuredMemberRange("values", 0, 4, 0, alignment=2),
        StructuredMemberRange("coordinates", 16, 12, 8, alignment=4),
        StructuredMemberRange("values", 8, 6, 4, alignment=2),
    )


def test_empty_generation_produces_no_payload_ranges() -> None:
    bound = _plan().bind_offsets((0, 0, 0, 0))

    assert tiled_coo_payload_ranges(bound, MEMBERS, 4, 2) == ()


@pytest.mark.parametrize(
    "names",
    [
        ("", "coordinates", "values"),
        ("same", "same", "values"),
        (False, "coordinates", "values"),
    ],
)
def test_member_names_must_be_nonempty_and_distinct(names) -> None:
    with pytest.raises(ValueError, match="member names"):
        TiledCOOStructuredMembers(*names)


@pytest.mark.parametrize("itemsize", [0, -1, False, 1.0])
def test_range_adapters_reject_invalid_itemsize(itemsize) -> None:
    plan = _plan()
    bound = plan.bind_offsets((0, 0, 0, 0))

    with pytest.raises(ValueError, match="itemsize"):
        tiled_coo_directory_ranges(plan, MEMBERS, itemsize)
    with pytest.raises(ValueError, match="itemsize"):
        tiled_coo_payload_ranges(bound, MEMBERS, itemsize, 2)
    with pytest.raises(ValueError, match="itemsize"):
        tiled_coo_payload_ranges(bound, MEMBERS, 4, itemsize)
