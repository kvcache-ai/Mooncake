from __future__ import annotations

from dataclasses import replace
import random

import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    TiledCOOLayout,
    plan_tiled_coo_read,
)


def _target(index: int, name: str, offset: tuple[int, int], shape: tuple[int, int]):
    return PlacementFragment(
        name,
        offset,
        shape,
        shape[0] * shape[1] * 2,
        ParallelRank(tp=index),
    )


def test_column_target_reads_only_intersecting_tile_runs() -> None:
    layout = TiledCOOLayout.build(
        ("w",),
        ((512, 4096),),
        ((0, 0),),
        tile_shape=(128, 64),
    )
    target = _target(0, "w", (0, 1024), (512, 512))

    plan = plan_tiled_coo_read(layout, (target,))

    assert layout.total_tiles == 4 * 64
    assert [(run.first_tile, run.last_tile) for run in plan.tile_runs] == [
        (16, 24),
        (80, 88),
        (144, 152),
        (208, 216),
    ]
    assert len(plan.offset_indices) == 8
    assert [
        (r.source_index, r.count, r.destination_index) for r in plan.offset_ranges
    ] == [(16, 201, 0)]
    assert plan.offset_buffer_elements == 201
    assert plan.offset_gather_indices == (0, 8, 64, 72, 128, 136, 192, 200)
    bound = plan.bind_offsets(list(plan.offset_indices))
    assert bound.entry_count == 32
    assert len(bound.payload_ranges) == 4


def test_metadata_gap_projection_is_static_bounded_and_generation_independent():
    import random

    rng = random.Random(20261017)
    for _ in range(100):
        columns = rng.randrange(65, 1025)
        rows = rng.randrange(129, 1025)
        width = rng.randrange(1, columns)
        column = rng.randrange(columns - width + 1)
        layout = TiledCOOLayout.build(("w",), ((rows, columns),), ((0, 0),))
        plan = plan_tiled_coo_read(
            layout, (_target(0, "w", (0, column), (rows, width)),)
        )
        assert len(plan.offset_gather_indices) == len(plan.offset_indices)
        assert plan.offset_buffer_elements - len(plan.offset_indices) <= 64 * (
            len(plan.offset_indices) - len(plan.offset_ranges)
        )
        for generation in range(3):
            directory = [0]
            for _tile in range(layout.total_tiles):
                directory.append(directory[-1] + rng.randrange(5))
            packed = [0] * plan.offset_buffer_elements
            for item in plan.offset_ranges:
                packed[item.destination_index : item.destination_index + item.count] = (
                    directory[item.source_index : item.source_index + item.count]
                )
            projected = [packed[i] for i in plan.offset_gather_indices]
            assert projected == [directory[i] for i in plan.offset_indices]
            bound = plan.bind_offsets(projected)
            assert bound.counts == tuple(
                directory[r.last_tile] - directory[r.first_tile] for r in plan.tile_runs
            )
            assert bound.entry_count == sum(bound.counts)


def test_metadata_gap_limit_and_empty_plan_do_not_read_unbounded_directory():
    from mooncake.reshard.weight.tiled_coo import (
        _coalesced_index_ranges,
        _index_gather_indices,
    )

    indices = (0, 65, 131, 132)
    ranges = _coalesced_index_ranges(indices)
    assert [(r.source_index, r.count, r.destination_index) for r in ranges] == [
        (0, 66, 0),
        (131, 2, 66),
    ]
    assert _index_gather_indices(indices, ranges) == (0, 65, 66, 67)
    layout = TiledCOOLayout.build(("a",), ((128, 128),), ((0, 0),))
    empty = plan_tiled_coo_read(layout, (_target(0, "b", (0, 0), (128, 128)),))
    assert empty.offset_buffer_elements == 0
    assert (
        empty.offset_ranges == empty.offset_gather_indices == empty.offset_indices == ()
    )


def test_unaligned_boxes_include_boundary_tiles_and_bind_exact_payload() -> None:
    layout = TiledCOOLayout.build(
        ("w",),
        ((130, 130),),
        ((63, 63),),
        tile_shape=(64, 64),
    )
    target = _target(7, "w", (100, 100), (40, 40))
    plan = plan_tiled_coo_read(layout, (target,))
    full_offsets = [0, 2, 2, 5, 7, 8, 11, 11, 14, 20, 21, 21, 25, 28, 30, 31, 33]
    compact = [full_offsets[index] for index in plan.offset_indices]

    bound = plan.bind_offsets(compact)

    assert bound.read_plan is plan
    assert bound.counts == (3, 1)
    assert bound.compact_entries == (0, 3)
    assert [
        (item.source_entry, item.count, item.destination_entry)
        for item in bound.payload_ranges
    ] == [
        (8, 3, 0),
        (20, 1, 3),
    ]
    assert bound.entry_count == 4


def test_layout_round_trips_the_compact_wire_table() -> None:
    layout = TiledCOOLayout.build(
        ("a", "b"),
        ((128, 65), (3, 1)),
        ((0, 63), (256, 0)),
        tile_shape=(128, 64),
    )

    restored = TiledCOOLayout.from_dict(layout.to_dict())

    assert restored == layout
    assert restored.records[0].tile_grid == (1, 2)
    assert restored.records[1].tile_base == 2


@pytest.mark.parametrize("row_offset,row_count", [(0, 512), (128, 256), (129, 129)])
def test_full_columns_coalesce_rows_without_reading_additional_tiles(
    row_offset, row_count
):
    layout = TiledCOOLayout.build(("w",), ((512, 4096),), ((0, 0),))
    target = _target(0, "w", (row_offset, 0), (row_count, 4096))
    plan = plan_tiled_coo_read(layout, (target,))
    first = row_offset // 128 * 64
    last = (row_offset + row_count + 127) // 128 * 64

    assert [(run.first_tile, run.last_tile) for run in plan.tile_runs] == [
        (first, last)
    ]
    assert plan.offset_indices == (first, last)
    bound = plan.bind_offsets((7, 19))
    assert bound.read_plan is plan
    assert bound.counts == (12,)
    assert bound.compact_entries == (0,)
    assert [(span.source_entry, span.count) for span in bound.payload_ranges] == [
        (7, 12)
    ]
    assert plan.bind_offsets((7, 7)).entry_count == 0


def test_full_columns_with_unaligned_records_keep_record_and_target_boundaries():
    layout = TiledCOOLayout.build(
        ("a", "b"),
        ((260, 130), (260, 130)),
        ((63, 63), (63, 63)),
        tile_shape=(64, 64),
    )
    targets = (
        _target(0, "a", (63, 63), (260, 130)),
        _target(1, "a", (100, 63), (80, 130)),
        _target(0, "b", (63, 63), (260, 130)),
    )
    plan = plan_tiled_coo_read(layout, targets)

    assert len(plan.tile_runs) == 3
    for run in plan.tile_runs:
        record = run.record
        first_row = max(record.global_offset[0], run.target.global_offset[0]) // 64
        last_row = (
            min(323, run.target.global_offset[0] + run.target.local_shape[0]) + 63
        ) // 64
        assert run.first_tile == record.tile_base + first_row * record.tile_grid[1]
        assert run.last_tile == record.tile_base + last_row * record.tile_grid[1]


def test_layout_rejects_a_second_wire_table_shape() -> None:
    layout = TiledCOOLayout.build(("w",), ((128, 64),), ((0, 0),))
    table = layout.to_dict()
    table["shapes"] = [[128, 64]]

    with pytest.raises(ValueError, match="flat rank-two integer table"):
        TiledCOOLayout.from_dict(table)


def test_bind_rejects_non_monotonic_selected_offsets() -> None:
    layout = TiledCOOLayout.build(("w",), ((128, 64),), ((0, 0),))
    plan = plan_tiled_coo_read(layout, (_target(0, "w", (0, 0), (128, 64)),))

    with pytest.raises(ValueError, match="not monotonic"):
        plan.bind_offsets((4, 3))


def test_bind_rejects_non_monotonic_offsets_across_tile_runs() -> None:
    layout = TiledCOOLayout.build(
        ("w",), ((256, 128),), ((0, 0),), tile_shape=(128, 64)
    )
    plan = plan_tiled_coo_read(layout, (_target(0, "w", (0, 0), (256, 64)),))

    assert plan.offset_indices == (0, 1, 2, 3)
    with pytest.raises(ValueError, match="not monotonic"):
        plan.bind_offsets((0, 5, 4, 8))


@pytest.mark.parametrize("values", [(0,), (0, -1), (False, 1), (0.0, 1), ("0", 1)])
def test_column_binding_keeps_strict_offset_validation(values):
    layout = TiledCOOLayout.build(("w",), ((128, 64),), ((0, 0),))
    plan = plan_tiled_coo_read(layout, (_target(0, "w", (0, 0), (128, 64)),))
    with pytest.raises(ValueError):
        plan.bind_offsets(values)


def test_dynamic_columns_are_fresh_and_keep_empty_runs():
    layout = TiledCOOLayout.build(("w",), ((256, 128),), ((0, 0),))
    plan = plan_tiled_coo_read(layout, (_target(0, "w", (0, 0), (256, 64)),))
    first = plan.bind_offsets((0, 2, 2, 2))
    second = plan.bind_offsets((0, 0, 3, 7))
    assert first.counts == (2, 0)
    assert first.compact_entries == (0, 0)
    assert second.counts == (0, 4)
    assert second.compact_entries == (0, 0)
    assert first.entry_count == 2 and second.entry_count == 4
    assert [
        (r.source_entry, r.count, r.destination_entry) for r in second.payload_ranges
    ] == [(3, 4, 0)]
    with pytest.raises(AttributeError):
        first.counts = (0, 0)


def test_dynamic_columns_coalesce_only_contiguous_source_intervals():
    layout = TiledCOOLayout.build(("w",), ((512, 128),), ((0, 0),))
    plan = plan_tiled_coo_read(layout, (_target(0, "w", (0, 0), (512, 64)),))
    bound = plan.bind_offsets((0, 2, 2, 4, 6, 6, 6, 9))
    assert bound.counts == (2, 2, 0, 3)
    assert bound.compact_entries == (0, 2, 0, 4)
    assert [
        (r.source_entry, r.count, r.destination_entry) for r in bound.payload_ranges
    ] == [(0, 4, 0), (6, 3, 4)]


def test_sdk_binding_preserves_arbitrary_precision_integer_offsets():
    layout = TiledCOOLayout.build(("w",), ((128, 64),), ((0, 0),))
    plan = plan_tiled_coo_read(layout, (_target(0, "w", (0, 0), (128, 64)),))
    bound = plan.bind_offsets((10**40, 2 * 10**40))
    assert bound.counts == (10**40,)
    assert bound.entry_count == 10**40
    assert bound.payload_ranges[0].source_entry == 10**40


@pytest.mark.parametrize(
    "field,value", [("tile_origin", (1, 0)), ("tile_grid", (2, 1))]
)
def test_direct_layout_construction_rejects_inconsistent_tile_geometry(field, value):
    layout = TiledCOOLayout.build(("w",), ((128, 64),), ((0, 0),))
    record = replace(layout.records[0], **{field: value})
    with pytest.raises(ValueError, match="tile geometry"):
        TiledCOOLayout(layout.tile_shape, (record,), record.tile_count)


def test_random_coo_payload_reads_and_boundary_filter_match_coordinate_oracle():
    """Execute both read stages without a Store and compare actual coordinates."""
    rng = random.Random(3747)
    for _ in range(100):
        tile_shape = (rng.randint(1, 16), rng.randint(1, 16))
        shapes = tuple((rng.randint(1, 40), rng.randint(1, 40)) for _ in range(2))
        offsets = tuple((rng.randint(0, 20), rng.randint(0, 20)) for _ in range(2))
        layout = TiledCOOLayout.build(
            ("w", "w"), shapes, offsets, tile_shape=tile_shape
        )
        source_entries = []
        directory = [0]
        for record in layout.records:
            buckets = [[] for _ in range(record.tile_count)]
            for _entry in range(rng.randint(0, 100)):
                row = record.global_offset[0] + rng.randrange(record.local_shape[0])
                column = record.global_offset[1] + rng.randrange(record.local_shape[1])
                tile = (
                    (row // tile_shape[0] - record.tile_origin[0]) * record.tile_grid[1]
                    + column // tile_shape[1]
                    - record.tile_origin[1]
                )
                # Values and record IDs distinguish overlapping source boxes.
                buckets[tile].append(
                    (row, column, rng.randrange(10000), record.record_index)
                )
            for bucket in buckets:
                source_entries.extend(bucket)
                directory.append(len(source_entries))
        targets = tuple(
            _target(
                index,
                "w",
                (rng.randrange(40), rng.randrange(40)),
                (rng.randint(1, 40), rng.randint(1, 40)),
            )
            for index in range(2)
        )
        plan = plan_tiled_coo_read(layout, targets)
        packed_offsets = [0] * plan.offset_buffer_elements
        for span in plan.offset_ranges:
            packed_offsets[
                span.destination_index : span.destination_index + span.count
            ] = directory[span.source_index : span.source_index + span.count]
        bound = plan.bind_offsets(
            [packed_offsets[index] for index in plan.offset_gather_indices]
        )
        compact = [None] * bound.entry_count
        for span in bound.payload_ranges:
            compact[span.destination_entry : span.destination_entry + span.count] = (
                source_entries[span.source_entry : span.source_entry + span.count]
            )
        for target in targets:

            def inside(entry):
                return all(
                    start <= coordinate < start + length
                    for coordinate, start, length in zip(
                        entry[:2], target.global_offset, target.local_shape
                    )
                )

            actual = []
            for run, start, count in zip(
                plan.tile_runs, bound.compact_entries, bound.counts
            ):
                if run.target == target:
                    actual.extend(
                        entry
                        for entry in compact[start : start + count]
                        if inside(entry)
                    )
            expected = [entry for entry in source_entries if inside(entry)]
            assert sorted(actual) == sorted(expected)
