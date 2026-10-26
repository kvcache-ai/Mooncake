"""Target-independent tiled COO layout and placement-aware read planning.

The sparse payload stays a pair of flat coordinate/value arrays.  This module
owns their physical tile order and lowers source/target placement overlap into
the two reads needed by a consumer: a small tile-offset read followed by exact
coordinate/value payload ranges.  Framework integrations only provide record
and target boxes and execute the returned plan.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence, cast

from ..geometry import box_intersection
from .types import PlacementFragment


DEFAULT_TILE_SHAPE = (128, 64)

# A tiny index read can cost more than transferring a nearby directory gap.
# This bounds extra metadata to 64 int32 entries per merged gap (256 bytes).
# Coordinate/value payload ranges remain exact and are never merged over gaps.
_INDEX_READ_GAP_ENTRIES = 64


def _pair(value: object, label: str, *, minimum: int) -> tuple[int, int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label} must contain two integers")
    items = tuple(cast(Sequence[object], value))
    if len(items) != 2 or any(
        type(item) is not int or item < minimum for item in items
    ):
        raise ValueError(f"{label} must contain two integers >= {minimum}")
    return cast(tuple[int, int], items)


@dataclass(frozen=True)
class TiledCOORecord:
    """One writer-local tensor box and its slice of the global tile directory."""

    record_index: int
    tensor_id: str
    global_offset: tuple[int, int]
    local_shape: tuple[int, int]
    tile_origin: tuple[int, int]
    tile_grid: tuple[int, int]
    tile_base: int

    def __post_init__(self) -> None:
        if type(self.record_index) is not int or self.record_index < 0:
            raise ValueError("record_index must be a non-negative integer")
        if type(self.tensor_id) is not str or not self.tensor_id:
            raise ValueError("record tensor_id must not be empty")
        if type(self.tile_base) is not int or self.tile_base < 0:
            raise ValueError("tile_base must be a non-negative integer")
        object.__setattr__(
            self,
            "global_offset",
            _pair(self.global_offset, "record global_offset", minimum=0),
        )
        object.__setattr__(
            self,
            "local_shape",
            _pair(self.local_shape, "record local_shape", minimum=1),
        )
        object.__setattr__(
            self,
            "tile_origin",
            _pair(self.tile_origin, "record tile_origin", minimum=0),
        )
        object.__setattr__(
            self, "tile_grid", _pair(self.tile_grid, "record tile_grid", minimum=1)
        )

    @property
    def tile_count(self) -> int:
        return self.tile_grid[0] * self.tile_grid[1]


@dataclass(frozen=True)
class TiledCOOLayout:
    """Canonical row-major 2-D tile directory shared by writer and readers."""

    tile_shape: tuple[int, int]
    records: tuple[TiledCOORecord, ...]
    total_tiles: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "tile_shape", _pair(self.tile_shape, "tile_shape", minimum=1)
        )
        records = tuple(self.records)
        if not records or any(
            not isinstance(record, TiledCOORecord) for record in records
        ):
            raise ValueError("tiled COO layout must contain TiledCOORecord values")
        tile_base = 0
        for index, record in enumerate(records):
            if record.record_index != index or record.tile_base != tile_base:
                raise ValueError(
                    "tiled COO records are not in canonical directory order"
                )
            origin = tuple(
                offset // size
                for offset, size in zip(record.global_offset, self.tile_shape)
            )
            grid = tuple(
                (offset + length + size - 1) // size - start
                for offset, length, size, start in zip(
                    record.global_offset, record.local_shape, self.tile_shape, origin
                )
            )
            if record.tile_origin != origin or record.tile_grid != grid:
                raise ValueError("record tile geometry differs from its tensor box")
            tile_base += record.tile_count
        if type(self.total_tiles) is not int or self.total_tiles != tile_base:
            raise ValueError("tiled COO total_tiles differs from its record directory")
        object.__setattr__(self, "records", records)

    @classmethod
    def build(
        cls,
        names: Sequence[str],
        shapes: Sequence[Sequence[int]],
        offsets: Sequence[Sequence[int]],
        *,
        tile_shape: tuple[int, int] = DEFAULT_TILE_SHAPE,
    ) -> "TiledCOOLayout":
        tile_rows, tile_columns = _pair(tile_shape, "tile_shape", minimum=1)
        if not (len(names) == len(shapes) == len(offsets)) or not names:
            raise ValueError("tiled COO record columns differ in length or are empty")
        records: list[TiledCOORecord] = []
        tile_base = 0
        for record_index, (name, raw_shape, raw_offset) in enumerate(
            zip(names, shapes, offsets)
        ):
            if type(name) is not str or not name:
                raise ValueError("record tensor_id must not be empty")
            shape = _pair(raw_shape, "record local_shape", minimum=1)
            offset = _pair(raw_offset, "record global_offset", minimum=0)
            origin = (offset[0] // tile_rows, offset[1] // tile_columns)
            end = (
                (offset[0] + shape[0] + tile_rows - 1) // tile_rows,
                (offset[1] + shape[1] + tile_columns - 1) // tile_columns,
            )
            grid = (end[0] - origin[0], end[1] - origin[1])
            record = TiledCOORecord(
                record_index=record_index,
                tensor_id=name,
                global_offset=offset,
                local_shape=shape,
                tile_origin=origin,
                tile_grid=grid,
                tile_base=tile_base,
            )
            records.append(record)
            tile_base += record.tile_count
        return cls((tile_rows, tile_columns), tuple(records), tile_base)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "TiledCOOLayout":
        names = value.get("names")
        shapes = value.get("shapes")
        offsets = value.get("offsets")
        tile_shape = value.get("tile_shape")
        if not isinstance(names, Sequence) or isinstance(
            names, (str, bytes, bytearray)
        ):
            raise ValueError("tiled COO names must be a sequence")
        if not isinstance(shapes, Sequence) or isinstance(
            shapes, (str, bytes, bytearray)
        ):
            raise ValueError("tiled COO shapes must be a sequence")
        if not isinstance(offsets, Sequence) or isinstance(
            offsets, (str, bytes, bytearray)
        ):
            raise ValueError("tiled COO offsets must be a sequence")
        names = cast(Sequence[object], names)
        shapes = cast(Sequence[object], shapes)
        offsets = cast(Sequence[object], offsets)
        if any(type(name) is not str or not name for name in names):
            raise ValueError("record tensor_id must not be empty")
        if len(shapes) != len(names) * 2 or any(
            type(item) is not int for item in shapes
        ):
            raise ValueError("tiled COO shapes must be a flat rank-two integer table")
        if len(offsets) != len(names) * 2 or any(
            type(item) is not int for item in offsets
        ):
            raise ValueError("tiled COO offsets must be a flat rank-two integer table")
        shape_pairs = tuple(
            _pair(shapes[index : index + 2], "record local_shape", minimum=1)
            for index in range(0, len(shapes), 2)
        )
        offset_pairs = tuple(
            _pair(offsets[index : index + 2], "record global_offset", minimum=0)
            for index in range(0, len(offsets), 2)
        )
        return cls.build(
            cast(tuple[str, ...], tuple(names)),
            shape_pairs,
            offset_pairs,
            tile_shape=_pair(tile_shape, "tile_shape", minimum=1),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "tile_shape": list(self.tile_shape),
            "names": [record.tensor_id for record in self.records],
            "shapes": [
                value for record in self.records for value in record.local_shape
            ],
            "offsets": [
                value for record in self.records for value in record.global_offset
            ],
        }


@dataclass(frozen=True)
class TiledCOOIndexRange:
    """A contiguous tile-offset slice copied into a compact local index buffer."""

    source_index: int
    count: int
    destination_index: int


@dataclass(frozen=True)
class TiledCOOTileRun:
    """A contiguous tile interval intersecting a source-record/target overlap."""

    record: TiledCOORecord
    target: PlacementFragment
    first_tile: int
    last_tile: int
    first_offset_position: int
    last_offset_position: int


@dataclass(frozen=True)
class TiledCOOPayloadRange:
    """One exact entry interval copied into a compact coord/value staging buffer."""

    source_entry: int
    count: int
    destination_entry: int


@dataclass(frozen=True)
class BoundTiledCOOReadPlan:
    """Fresh integer columns in read_plan.tile_runs order, including empty runs.

    Geometry remains in the static read plan. Binding a new directory does not
    allocate a record/target wrapper for every active run or retain old values.
    """

    read_plan: TiledCOOReadPlan
    counts: tuple[int, ...]
    compact_entries: tuple[int, ...]
    payload_ranges: tuple[TiledCOOPayloadRange, ...]
    entry_count: int


@dataclass(frozen=True)
class TiledCOOReadPlan:
    """Static placement plan and bounded-gap directory reads.

    offset_indices names the exact logical endpoints consumed by bind_offsets.
    offset_ranges packs physical reads, possibly including small metadata gaps;
    offset_gather_indices projects that packed buffer back to those endpoints.
    """

    tile_runs: tuple[TiledCOOTileRun, ...]
    offset_indices: tuple[int, ...]
    offset_ranges: tuple[TiledCOOIndexRange, ...]
    offset_gather_indices: tuple[int, ...]
    offset_buffer_elements: int
    _offset_positions: tuple[tuple[int, int], ...]

    @property
    def empty(self) -> bool:
        return not self.tile_runs

    def bind_offsets(self, values: Sequence[int]) -> BoundTiledCOOReadPlan:
        if len(values) != len(self.offset_indices):
            raise ValueError(
                "tile offset value count differs from the planned compact index buffer"
            )
        normalized: list[int] = []
        for value in values:
            if type(value) is not int or value < 0:
                raise ValueError("tile offsets must be non-negative integers")
            normalized.append(value)
        if any(
            current > following
            for current, following in zip(normalized, normalized[1:])
        ):
            raise ValueError("tile offsets are not monotonic")

        counts: list[int] = []
        compact_entries: list[int] = []
        payload_ranges: list[TiledCOOPayloadRange] = []
        compact_entry = 0
        pending_source = pending_count = pending_destination = 0
        for first_position, last_position in self._offset_positions:
            first = normalized[first_position]
            count = normalized[last_position] - first
            counts.append(count)
            compact_entries.append(compact_entry if count else 0)
            if count == 0:
                continue
            # Compact destinations are consecutive by construction. Defer the
            # immutable span object until its complete coalesced interval ends.
            if pending_count and pending_source + pending_count == first:
                pending_count += count
            else:
                if pending_count:
                    payload_ranges.append(
                        TiledCOOPayloadRange(
                            pending_source, pending_count, pending_destination
                        )
                    )
                pending_source, pending_count, pending_destination = (
                    first,
                    count,
                    compact_entry,
                )
            compact_entry += count
        if pending_count:
            payload_ranges.append(
                TiledCOOPayloadRange(pending_source, pending_count, pending_destination)
            )
        return BoundTiledCOOReadPlan(
            self,
            tuple(counts),
            tuple(compact_entries),
            tuple(payload_ranges),
            compact_entry,
        )


def _coalesced_index_ranges(indices: tuple[int, ...]) -> tuple[TiledCOOIndexRange, ...]:
    if not indices:
        return ()
    ranges: list[TiledCOOIndexRange] = []
    first = previous = indices[0]
    destination = 0
    for index in indices[1:]:
        if index - previous - 1 > _INDEX_READ_GAP_ENTRIES:
            ranges.append(TiledCOOIndexRange(first, previous - first + 1, destination))
            destination += previous - first + 1
            first = index
        previous = index
    ranges.append(TiledCOOIndexRange(first, previous - first + 1, destination))
    return tuple(ranges)


def _index_gather_indices(
    indices: tuple[int, ...],
    ranges: tuple[TiledCOOIndexRange, ...],
) -> tuple[int, ...]:
    """Map exact endpoints into packed metadata windows, once per geometry."""
    positions: list[int] = []
    selected = 0
    for item in ranges:
        end = item.source_index + item.count
        while selected < len(indices) and indices[selected] < end:
            positions.append(
                item.destination_index + indices[selected] - item.source_index
            )
            selected += 1
    return tuple(positions)


def plan_tiled_coo_read(
    layout: TiledCOOLayout,
    targets: Sequence[PlacementFragment],
) -> TiledCOOReadPlan:
    """Lower source/target boxes into tile runs and minimal offset-index reads."""

    if not isinstance(layout, TiledCOOLayout):
        raise TypeError("layout must be a TiledCOOLayout")
    normalized_targets = tuple(targets)
    if any(not isinstance(target, PlacementFragment) for target in normalized_targets):
        raise TypeError("targets must contain PlacementFragment values")
    if any(
        len(target.global_offset) != 2 or len(target.local_shape) != 2
        for target in normalized_targets
    ):
        raise ValueError("tiled COO targets must be rank two")
    target_ids = [target.placement_fragment_id for target in normalized_targets]
    if len(target_ids) != len(set(target_ids)):
        raise ValueError("targets must not contain duplicate placement fragments")

    tile_rows, tile_columns = layout.tile_shape
    raw_runs: list[tuple[TiledCOORecord, PlacementFragment, int, int]] = []
    targets_by_tensor: dict[str, list[PlacementFragment]] = {}
    for target in normalized_targets:
        targets_by_tensor.setdefault(target.tensor_id, []).append(target)
    for record in layout.records:
        for target in targets_by_tensor.get(record.tensor_id, ()):
            intersection = box_intersection(
                record.global_offset,
                record.local_shape,
                target.global_offset,
                target.local_shape,
            )
            if intersection is None:
                continue
            lo, hi = intersection
            first_row = lo[0] // tile_rows - record.tile_origin[0]
            last_row = (hi[0] + tile_rows - 1) // tile_rows - record.tile_origin[0]
            first_column = lo[1] // tile_columns - record.tile_origin[1]
            last_column = (
                hi[1] + tile_columns - 1
            ) // tile_columns - record.tile_origin[1]
            if not (
                0 <= first_row < last_row <= record.tile_grid[0]
                and 0 <= first_column < last_column <= record.tile_grid[1]
            ):
                raise ValueError("planned tile overlap exceeds the source record grid")
            if first_column == 0 and last_column == record.tile_grid[1]:
                # Full-column rows are contiguous in the physical tile stream.
                # Whole-owned experts need only the two interval endpoints.
                raw_runs.append(
                    (
                        record,
                        target,
                        record.tile_base + first_row * record.tile_grid[1],
                        record.tile_base + last_row * record.tile_grid[1],
                    )
                )
                continue
            for tile_row in range(first_row, last_row):
                first_tile = (
                    record.tile_base + tile_row * record.tile_grid[1] + first_column
                )
                last_tile = (
                    record.tile_base + tile_row * record.tile_grid[1] + last_column
                )
                raw_runs.append((record, target, first_tile, last_tile))

    raw_runs.sort(
        key=lambda item: (
            item[0].record_index,
            item[1].placement_fragment_id,
            item[2],
            item[3],
        )
    )
    offset_indices = tuple(
        sorted(
            {
                index
                for _record, _target, first, last in raw_runs
                for index in (first, last)
            }
        )
    )
    positions = {index: position for position, index in enumerate(offset_indices)}
    runs = tuple(
        TiledCOOTileRun(
            record,
            target,
            first,
            last,
            positions[first],
            positions[last],
        )
        for record, target, first, last in raw_runs
    )
    offset_ranges = _coalesced_index_ranges(offset_indices)
    return TiledCOOReadPlan(
        runs,
        offset_indices,
        offset_ranges,
        _index_gather_indices(offset_indices, offset_ranges),
        sum(item.count for item in offset_ranges),
        tuple((run.first_offset_position, run.last_offset_position) for run in runs),
    )


__all__ = [
    "DEFAULT_TILE_SHAPE",
    "BoundTiledCOOReadPlan",
    "TiledCOOIndexRange",
    "TiledCOOLayout",
    "TiledCOOPayloadRange",
    "TiledCOOReadPlan",
    "TiledCOORecord",
    "TiledCOOTileRun",
    "plan_tiled_coo_read",
]
