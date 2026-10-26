"""Property tests for the tiled COO layout and read planner.

The planner is a pure function from a layout and target boxes to tile runs,
directory windows and, once offsets are bound, payload ranges.  Each test
states one contract of that function as an invariant over random layouts,
boxes and payloads and checks it against an independent coordinate oracle
that only knows global tile boxes.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import FrozenInstanceError
from itertools import product

from hypothesis import given, settings, strategies as st
import pytest

from mooncake.reshard.weight import (
    ParallelRank,
    PlacementFragment,
    TiledCOOLayout,
    plan_tiled_coo_read,
)
from mooncake.reshard.weight.tiled_coo import _INDEX_READ_GAP_ENTRIES

Box = tuple[tuple[int, int], tuple[int, int]]
Entry = tuple[int, int, int, int]

_TENSOR_NAMES = ("a", "b")


def _target(index, name, offset, shape):
    return PlacementFragment(
        name, offset, shape, shape[0] * shape[1] * 2, ParallelRank(tp=index)
    )


def _box(fragment) -> Box:
    return tuple(fragment.global_offset), tuple(fragment.local_shape)


def _intersect(left: Box, right: Box) -> Box | None:
    begin = tuple(
        max(left_begin, right_begin)
        for left_begin, right_begin in zip(left[0], right[0])
    )
    end = tuple(
        min(lo + ls, ro + rs)
        for lo, ls, ro, rs in zip(left[0], left[1], right[0], right[1])
    )
    if any(b >= e for b, e in zip(begin, end)):
        return None
    return begin, tuple(e - b for b, e in zip(begin, end))


def _contains(outer: Box, inner: Box) -> bool:
    return _intersect(outer, inner) == inner


def _inside(entry: Entry, box: Box) -> bool:
    return all(b <= c < b + s for c, b, s in zip(entry[:2], box[0], box[1]))


def _tile_boxes(layout, record):
    """Yield (global tile index, tile box clipped to the record) in directory order."""
    rows, columns = layout.tile_shape
    for local in range(record.tile_count):
        tile_row, tile_column = divmod(local, record.tile_grid[1])
        origin = (
            (record.tile_origin[0] + tile_row) * rows,
            (record.tile_origin[1] + tile_column) * columns,
        )
        clipped = _intersect((origin, (rows, columns)), _box(record))
        assert clipped is not None
        yield record.tile_base + local, clipped


def _expected_tiles(layout, record, target) -> list[int]:
    if record.tensor_id != target.tensor_id:
        return []
    overlap = _intersect(_box(record), _box(target))
    if overlap is None:
        return []
    return [
        tile
        for tile, clipped in _tile_boxes(layout, record)
        if _intersect(clipped, overlap) is not None
    ]


def _payload(layout, fill: bytes):
    """Pack entries by record then row-major tile and return the cumulative directory."""
    entries: list[Entry] = []
    directory = [0]
    for record in layout.records:
        for tile, clipped in _tile_boxes(layout, record):
            cells = [
                (row, column)
                for row in range(clipped[0][0], clipped[0][0] + clipped[1][0])
                for column in range(clipped[0][1], clipped[0][1] + clipped[1][1])
            ]
            byte = fill[tile]
            for item in range(byte & 3):
                row, column = cells[((byte >> 2) + 7 * item) % len(cells)]
                entries.append(
                    (row, column, (byte * 31 + item) % 1000, record.record_index)
                )
            directory.append(len(entries))
    return entries, directory


def _execute(plan, directory, entries):
    """Run both read stages the way an executor would, without a Store."""
    packed = [0] * plan.offset_buffer_elements
    for item in plan.offset_ranges:
        packed[item.destination_index : item.destination_index + item.count] = (
            directory[item.source_index : item.source_index + item.count]
        )
    projected = [packed[index] for index in plan.offset_gather_indices]
    assert projected == [directory[index] for index in plan.offset_indices]
    bound = plan.bind_offsets(projected)
    compact = [None] * bound.entry_count
    for span in bound.payload_ranges:
        compact[span.destination_entry : span.destination_entry + span.count] = entries[
            span.source_entry : span.source_entry + span.count
        ]
    return bound, compact


def _raw_read(plan, bound, compact, target):
    raw = []
    for run, start, count in zip(plan.tile_runs, bound.compact_entries, bound.counts):
        if run.target == target:
            raw.extend(compact[start : start + count])
    return raw


def _oracle(layout, entries, target):
    """Entries of the target's tensor whose coordinates fall inside its box."""
    return sorted(
        entry
        for entry in entries
        if layout.records[entry[3]].tensor_id == target.tensor_id
        and _inside(entry, _box(target))
    )


def _intervals(plan, target=None):
    return [
        (run.record.record_index, run.first_tile, run.last_tile)
        for run in plan.tile_runs
        if target is None or run.target == target
    ]


@st.composite
def _layouts(draw, *, tile_shape=None):
    if tile_shape is None:
        tile_shape = (draw(st.integers(1, 5)), draw(st.integers(1, 5)))
    count = draw(st.integers(1, 3))
    names = tuple(draw(st.sampled_from(_TENSOR_NAMES)) for _ in range(count))
    shapes = tuple(
        (draw(st.integers(1, 8)), draw(st.integers(1, 8))) for _ in range(count)
    )
    offsets = tuple(
        (draw(st.integers(0, 8)), draw(st.integers(0, 8))) for _ in range(count)
    )
    return TiledCOOLayout.build(names, shapes, offsets, tile_shape=tile_shape)


@st.composite
def _target_batches(draw, *, names=_TENSOR_NAMES + ("c",)):
    count = draw(st.integers(0, 3))
    return tuple(
        _target(
            index,
            draw(st.sampled_from(names)),
            (draw(st.integers(0, 12)), draw(st.integers(0, 12))),
            (draw(st.integers(1, 8)), draw(st.integers(1, 8))),
        )
        for index in range(count)
    )


@st.composite
def _scenarios(draw, *, tile_shape=None):
    layout = draw(_layouts(tile_shape=tile_shape))
    targets = draw(_target_batches())
    fill = draw(st.binary(min_size=layout.total_tiles, max_size=layout.total_tiles))
    entries, directory = _payload(layout, fill)
    return layout, targets, entries, directory, fill


@st.composite
def _partitions(draw):
    tile_shape = (draw(st.integers(1, 5)), draw(st.integers(1, 5)))
    shape = (draw(st.integers(1, 12)), draw(st.integers(1, 12)))
    offset = (draw(st.integers(0, 6)), draw(st.integers(0, 6)))
    layout = TiledCOOLayout.build(("w",), (shape,), (offset,), tile_shape=tile_shape)
    cuts = []
    for extent in shape:
        if extent > 1:
            cuts.append(
                sorted(set(draw(st.lists(st.integers(1, extent - 1), max_size=3))))
            )
        else:
            cuts.append([])
    rows = [offset[0], *(offset[0] + cut for cut in cuts[0]), offset[0] + shape[0]]
    columns = [offset[1], *(offset[1] + cut for cut in cuts[1]), offset[1] + shape[1]]
    targets = tuple(
        _target(
            index,
            "w",
            (row_begin, column_begin),
            (row_end - row_begin, column_end - column_begin),
        )
        for index, ((row_begin, row_end), (column_begin, column_end)) in enumerate(
            product(zip(rows, rows[1:]), zip(columns, columns[1:]))
        )
    )
    fill = draw(st.binary(min_size=layout.total_tiles, max_size=layout.total_tiles))
    entries, directory = _payload(layout, fill)
    aligned = all(row % tile_shape[0] == 0 for row in rows[1:-1]) and all(
        column % tile_shape[1] == 0 for column in columns[1:-1]
    )
    return layout, targets, entries, directory, aligned


@settings(max_examples=200, deadline=None)
@given(_layouts())
def test_layouts_are_canonical_immutable_and_round_trip(layout):
    assert TiledCOOLayout.from_dict(layout.to_dict()) == layout
    assert (
        TiledCOOLayout(layout.tile_shape, layout.records, layout.total_tiles) == layout
    )
    rows, columns = layout.tile_shape
    base = 0
    for index, record in enumerate(layout.records):
        assert (record.record_index, record.tile_base) == (index, base)
        origin = (record.global_offset[0] // rows, record.global_offset[1] // columns)
        end = (
            -(-(record.global_offset[0] + record.local_shape[0]) // rows),
            -(-(record.global_offset[1] + record.local_shape[1]) // columns),
        )
        assert record.tile_origin == origin
        assert record.tile_grid == (end[0] - origin[0], end[1] - origin[1])
        clipped = [box for _tile, box in _tile_boxes(layout, record)]
        assert all(_contains(_box(record), box) for box in clipped)
        assert sum(box[1][0] * box[1][1] for box in clipped) == (
            record.local_shape[0] * record.local_shape[1]
        )
        base += record.tile_count
    assert layout.total_tiles == base
    with pytest.raises(FrozenInstanceError):
        layout.records = ()
    with pytest.raises(ValueError, match="total_tiles"):
        TiledCOOLayout(layout.tile_shape, layout.records, layout.total_tiles + 1)
    if len(layout.records) > 1:
        swapped = (layout.records[1], layout.records[0], *layout.records[2:])
        with pytest.raises(ValueError, match="canonical directory order"):
            TiledCOOLayout(layout.tile_shape, swapped, layout.total_tiles)


@settings(max_examples=200, deadline=None)
@given(_layouts(), _target_batches())
def test_tile_runs_select_exactly_the_tiles_meeting_each_overlap(layout, targets):
    plan = plan_tiled_coo_read(layout, targets)
    keys = [
        (
            run.record.record_index,
            run.target.placement_fragment_id,
            run.first_tile,
            run.last_tile,
        )
        for run in plan.tile_runs
    ]
    assert keys == sorted(keys)
    covered = defaultdict(list)
    for run in plan.tile_runs:
        record = layout.records[run.record.record_index]
        assert run.record == record
        assert record.tile_base <= run.first_tile < run.last_tile
        assert run.last_tile <= record.tile_base + record.tile_count
        assert plan.offset_indices[run.first_offset_position] == run.first_tile
        assert plan.offset_indices[run.last_offset_position] == run.last_tile
        covered[(record.record_index, run.target.placement_fragment_id)].append(
            range(run.first_tile, run.last_tile)
        )
    for record in layout.records:
        for target in targets:
            runs = covered.get((record.record_index, target.placement_fragment_id), [])
            tiles = [tile for interval in runs for tile in interval]
            assert len(tiles) == len(set(tiles))
            assert sorted(tiles) == _expected_tiles(layout, record, target)
            if tiles:
                width = record.tile_grid[1]
                tile_rows = {(tile - record.tile_base) // width for tile in tiles}
                tile_columns = {(tile - record.tile_base) % width for tile in tiles}
                full_width = tile_columns == set(range(width))
                assert len(runs) == (1 if full_width else len(tile_rows))


@settings(max_examples=200, deadline=None)
@given(_layouts(), _target_batches())
def test_directory_windows_cover_every_endpoint_once_and_bridge_only_short_gaps(
    layout, targets
):
    plan = plan_tiled_coo_read(layout, targets)
    indices = plan.offset_indices
    assert plan.empty == (not plan.tile_runs)
    assert list(indices) == sorted(set(indices))
    assert set(indices) == {
        endpoint
        for run in plan.tile_runs
        for endpoint in (run.first_tile, run.last_tile)
    }
    assert all(0 <= index <= layout.total_tiles for index in indices)
    destination = 0
    previous_last = None
    seen = []
    for item in plan.offset_ranges:
        assert item.count >= 1
        assert item.destination_index == destination
        assert 0 <= item.source_index
        assert item.source_index + item.count <= layout.total_tiles + 1
        if previous_last is not None:
            assert item.source_index - previous_last - 1 > _INDEX_READ_GAP_ENTRIES
        window = [
            index
            for index in indices
            if item.source_index <= index < item.source_index + item.count
        ]
        assert window[0] == item.source_index
        assert window[-1] == item.source_index + item.count - 1
        assert all(
            right - left - 1 <= _INDEX_READ_GAP_ENTRIES
            for left, right in zip(window, window[1:])
        )
        seen.extend(window)
        destination += item.count
        previous_last = item.source_index + item.count - 1
    assert seen == list(indices)
    assert plan.offset_buffer_elements == destination
    assert len(plan.offset_gather_indices) == len(indices)
    assert all(0 <= position < destination for position in plan.offset_gather_indices)


@settings(max_examples=150, deadline=None)
@given(_scenarios())
def test_payload_reads_are_exact_per_run_and_filtering_restores_the_oracle(scenario):
    layout, targets, entries, directory, fill = scenario
    plan = plan_tiled_coo_read(layout, targets)
    for generation in (fill, bytes(reversed(fill))):
        entries, directory = _payload(layout, generation)
        bound, compact = _execute(plan, directory, entries)
        assert bound.read_plan is plan
        for run, start, count in zip(
            plan.tile_runs, bound.compact_entries, bound.counts
        ):
            assert count == directory[run.last_tile] - directory[run.first_tile]
            assert (
                compact[start : start + count]
                == (entries[directory[run.first_tile] : directory[run.last_tile]])
            )
        position = 0
        for span in bound.payload_ranges:
            assert span.count >= 1
            assert span.destination_entry == position
            assert 0 <= span.source_entry
            assert span.source_entry + span.count <= len(entries)
            position += span.count
        assert position == bound.entry_count == sum(bound.counts)
        assert None not in compact
        for left, right in zip(bound.payload_ranges, bound.payload_ranges[1:]):
            assert left.source_entry + left.count != right.source_entry
        for target in targets:
            raw = _raw_read(plan, bound, compact, target)
            needs_filter = False
            for run in plan.tile_runs:
                if run.target != target:
                    continue
                for tile, clipped in _tile_boxes(layout, run.record):
                    if run.first_tile <= tile < run.last_tile:
                        needs_filter |= not _contains(_box(target), clipped)
            expected = _oracle(layout, entries, target)
            assert sorted(entry for entry in raw if _inside(entry, _box(target))) == (
                expected
            )
            if not needs_filter:
                assert sorted(raw) == expected


@settings(max_examples=150, deadline=None)
@given(_scenarios())
def test_one_target_reads_the_same_entries_alone_or_in_any_batch(scenario):
    layout, targets, entries, directory, _fill = scenario
    batch = plan_tiled_coo_read(layout, targets)
    assert plan_tiled_coo_read(layout, targets) == batch
    assert plan_tiled_coo_read(layout, tuple(reversed(targets))) == batch
    bound, compact = _execute(batch, directory, entries)
    assert (
        batch.bind_offsets([directory[index] for index in batch.offset_indices])
        == bound
    )
    for target in targets:
        alone = plan_tiled_coo_read(layout, (target,))
        assert _intervals(alone) == _intervals(batch, target)
        _alone_bound, alone_compact = _execute(alone, directory, entries)
        assert alone_compact == _raw_read(batch, bound, compact, target)
        twin = PlacementFragment(
            target.tensor_id,
            target.global_offset,
            target.local_shape,
            target.nbytes + 8,
            ParallelRank(dp=3, tp=11, ep=2),
        )
        assert _intervals(plan_tiled_coo_read(layout, (twin,))) == _intervals(alone)


@settings(max_examples=150, deadline=None)
@given(_partitions())
def test_a_partition_of_one_record_reads_every_entry_once_after_filtering(partition):
    layout, targets, entries, directory, aligned = partition
    plan = plan_tiled_coo_read(layout, targets)
    bound, compact = _execute(plan, directory, entries)
    union = []
    for target in targets:
        union.extend(
            entry
            for entry in _raw_read(plan, bound, compact, target)
            if _inside(entry, _box(target))
        )
    assert sorted(union) == sorted(entries)
    if aligned:
        assert bound.entry_count == len(entries)
    else:
        assert bound.entry_count >= len(entries)


@settings(max_examples=100, deadline=None)
@given(_scenarios(tile_shape=(1, 1)))
def test_unit_tiles_never_need_the_boundary_filter(scenario):
    layout, targets, entries, directory, _fill = scenario
    plan = plan_tiled_coo_read(layout, targets)
    bound, compact = _execute(plan, directory, entries)
    for target in targets:
        assert sorted(_raw_read(plan, bound, compact, target)) == (
            _oracle(layout, entries, target)
        )


@settings(max_examples=100, deadline=None)
@given(_scenarios(tile_shape=(64, 64)))
def test_one_tile_per_record_reads_whole_intersecting_records(scenario):
    layout, targets, entries, directory, _fill = scenario
    assert all(record.tile_count == 1 for record in layout.records)
    plan = plan_tiled_coo_read(layout, targets)
    bound, compact = _execute(plan, directory, entries)
    for target in targets:
        expected = [
            entry
            for entry in entries
            if _expected_tiles(layout, layout.records[entry[3]], target)
        ]
        assert sorted(_raw_read(plan, bound, compact, target)) == sorted(expected)


@settings(max_examples=100, deadline=None)
@given(_scenarios(), st.data())
def test_bind_rejects_mismatched_or_non_monotone_directories(scenario, data):
    layout, targets, _entries, directory, _fill = scenario
    plan = plan_tiled_coo_read(layout, targets)
    projected = [directory[index] for index in plan.offset_indices]
    assert plan.bind_offsets(projected) == plan.bind_offsets(tuple(projected))
    with pytest.raises(ValueError, match="count differs"):
        plan.bind_offsets([*projected, projected[-1] if projected else 0])
    if not projected:
        assert plan.bind_offsets(()).entry_count == 0
        return
    position = data.draw(st.integers(0, len(projected) - 1))
    for bad in (-1, float(projected[position]), True, None):
        broken = list(projected)
        broken[position] = bad
        with pytest.raises(ValueError):
            plan.bind_offsets(broken)
    if len(projected) > 1:
        broken = list(projected)
        index = position if position < len(projected) - 1 else 0
        broken[index] = projected[-1] + 1
        with pytest.raises(ValueError, match="not monotonic"):
            plan.bind_offsets(broken)


def test_empty_batches_and_foreign_boxes_plan_nothing() -> None:
    layout = TiledCOOLayout.build(("a",), ((5, 7),), ((2, 3),), tile_shape=(4, 4))
    for targets in (
        (),
        (_target(0, "b", (2, 3), (5, 7)),),
        (_target(0, "a", (100, 100), (1, 1)),),
        (_target(0, "a", (7, 0), (3, 3)),),
    ):
        plan = plan_tiled_coo_read(layout, targets)
        assert plan.empty
        assert plan.tile_runs == plan.offset_indices == plan.offset_ranges == ()
        assert plan.offset_gather_indices == ()
        assert plan.offset_buffer_elements == 0
        bound = plan.bind_offsets(())
        assert bound.entry_count == 0
        assert bound.counts == bound.compact_entries == bound.payload_ranges == ()
