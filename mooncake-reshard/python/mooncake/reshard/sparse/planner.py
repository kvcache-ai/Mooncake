"""Address-free planner for COO sparse structured objects.

The contract is intentionally generic rather than tied to a framework or a
specific model family.  It supports arbitrary-rank COO coordinates and named
placement axes, and is independent from the dense ``TransferRegion`` planner:
regions here are ranges in the ``indices``/``values`` members of a structured
object and carry additive (``scatter_add``) semantics.

The planner has no transport dependency.  Callers provide compact source tile
indexes and logical source/target descriptors; the planner returns aligned
member ranges plus boundary-filter metadata.  Store integrations own RPCs,
buffer allocation, caching, and target-object materialization.

For replicated source participants, ``canonical_source_placement`` provides a
deterministic admission gate before the caller computes and publishes COO.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from ..geometry import Box, box_intersection, boxes_exactly_cover
from ..weight import RuntimeTensorOwner
from ..weight._planner.geometry import _CandidateBoxIndex
from ..weight.types import OwnershipAxis, TensorDescriptor


# Reuse the canonical reshard owner-coordinate representation.  Sparse
# placement rules remain sparse-specific, but the wire shape is shared with
# dense weight manifests.
Placement = RuntimeTensorOwner


def _placement_map(placement: Placement) -> dict[str, int]:
    result: dict[str, int] = {}
    for axis, ordinal in placement:
        if not isinstance(axis, str) or not axis or axis in result:
            raise ValueError(f"invalid placement axis: {placement!r}")
        if type(ordinal) is not int or ordinal < 0:
            raise ValueError(f"invalid placement ordinal: {placement!r}")
        result[axis] = ordinal
    return result


def normalize_placement(placement: Placement) -> Placement:
    """Return the canonical ordering for a named placement."""

    return tuple(sorted(_placement_map(tuple(placement)).items()))


def canonical_source_placement(source_placements: Iterable[Placement]) -> Placement:
    """Choose a deterministic source owner for one logical object.

    Replicated source participants (for example, non-EP weights visible on
    every training EP rank) can call this before computing COO.  The owner is
    selected from the advertised placements, so the rule is independent of
    EP/TP names and remains deterministic across processes.  An empty
    placement is a valid canonical owner when all participants are otherwise
    indistinguishable.
    """

    normalized = {
        normalize_placement(tuple(placement)) for placement in source_placements
    }
    if not normalized:
        raise ValueError("at least one source placement is required")
    return min(normalized)


def is_canonical_source(
    source_placement: Placement, source_placements: Iterable[Placement]
) -> bool:
    """Return whether ``source_placement`` should publish the object."""

    canonical = canonical_source_placement(source_placements)
    normalized = normalize_placement(tuple(source_placement))
    return normalized == canonical


def placement_matches(
    tensor: TensorDescriptor, source: Placement, target: Placement
) -> bool:
    """Match source/target owners using the canonical weight descriptor.

    Ownership is already part of Mooncake's weight reshard manifest.  Sparse
    planning only adds COO range selection; it must not create a second EP/TP
    placement language.  Split and replicated axes are handled by the logical
    boxes, while an ``OwnershipAxis`` requires the same ordinal on both sides.
    """

    source_map = _placement_map(source)
    target_map = _placement_map(target)
    for axis in tensor.parallel_axes:
        if not isinstance(axis, OwnershipAxis):
            continue
        if (
            axis.kind not in source_map
            or axis.kind not in target_map
            or source_map[axis.kind] != target_map[axis.kind]
        ):
            return False
    return True


def object_ref_key(ref: object) -> str:
    """Normalize a structured-object reference for range-cache keys."""

    if isinstance(ref, Mapping):
        key = ref.get("key")
        if key is None:
            key = ref.get("manifest_key")
    else:
        key = getattr(ref, "key", None)
        if key is None:
            # Mooncake's production RemoteBundleRef calls this field
            # ``manifest_key``; the tiny demo double uses ``key``.
            key = getattr(ref, "manifest_key", ref)
    if not isinstance(key, str) or not key:
        raise ValueError("sparse object reference must expose a non-empty string key")
    return key


def normalize_shape(
    value: Sequence[int], name: str, *, positive: bool
) -> tuple[int, ...]:
    if isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{name} must be a sequence of integers")
    try:
        result = tuple(value)
    except TypeError as error:
        raise ValueError(f"{name} must be a sequence of integers") from error
    if not result or any(type(item) is not int for item in result):
        raise ValueError(f"{name} must be a non-empty integer sequence")
    if positive and any(item <= 0 for item in result):
        raise ValueError(f"{name} must contain positive integers")
    if not positive and any(item < 0 for item in result):
        raise ValueError(f"{name} must contain non-negative integers")
    return result


def _normalize_box(value: Box, name: str) -> Box:
    if isinstance(value, (str, bytes, bytearray)) or len(value) != 2:
        raise ValueError(f"{name} must be (begin, end)")
    begin = normalize_shape(value[0], f"{name}.begin", positive=False)
    end = normalize_shape(value[1], f"{name}.end", positive=False)
    if len(begin) != len(end) or any(a >= b for a, b in zip(begin, end)):
        raise ValueError(f"{name} must be a non-empty box")
    return begin, end


def _member_dtype(member: Mapping[str, Any], name: str) -> str:
    """Return a declared dtype without reimplementing store-integration types.

    The integration layer is the authority for mapping framework dtypes to
    transport/storage types.  The planner only checks that the structured
    object carries a declaration and that duplicated declarations agree.
    """

    dtype = member.get("dtype")
    if not isinstance(dtype, str) or not dtype:
        raise ValueError(f"{name} member must declare a non-empty dtype")
    return dtype


def _merge_sorted_ranges(
    ranges: Iterable[tuple[int, int]],
) -> tuple[tuple[int, int], ...]:
    """Coalesce ranges already ordered by their source COO entry offsets."""

    merged: list[tuple[int, int]] = []
    for start, end in ranges:
        if type(start) is not int or type(end) is not int:
            raise ValueError("COO ranges must contain integers")
        if start < 0 or end < start:
            raise ValueError(f"invalid COO range: {(start, end)}")
        if start == end:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return tuple(merged)


@dataclass(frozen=True)
class SparseObjectIndex:
    """Tile index for an object whose COO members are sorted by tile.

    ``tile_coords[i]`` owns entries in ``tile_ptr[i]:tile_ptr[i + 1]``.  The
    same entry range applies to both ``indices`` (``[nnz, ndim]``) and
    ``values`` (``[nnz]``), making structured-object axis-0 slices safe.
    """

    object_ref: str
    tensor_id: str
    global_shape: tuple[int, ...]
    tile_shape: tuple[int, ...]
    tile_coords: tuple[tuple[int, ...], ...]
    tile_ptr: tuple[int, ...]
    nnz: int
    base_generation: int
    delta_generation: int

    def __post_init__(self) -> None:
        if not isinstance(self.object_ref, str) or not self.object_ref:
            raise ValueError("object_ref must be a non-empty string")
        if not isinstance(self.tensor_id, str) or not self.tensor_id:
            raise ValueError("tensor_id must be a non-empty string")
        shape = normalize_shape(self.global_shape, "global_shape", positive=True)
        tile_shape = normalize_shape(self.tile_shape, "tile_shape", positive=True)
        if len(shape) != len(tile_shape):
            raise ValueError("global_shape and tile_shape rank differ")
        coords = tuple(
            normalize_shape(coord, "tile_coords", positive=False)
            for coord in self.tile_coords
        )
        if any(len(coord) != len(shape) for coord in coords):
            raise ValueError("tile_coords rank differs from global_shape")
        if any(
            coords[position] >= coords[position + 1]
            for position in range(len(coords) - 1)
        ):
            raise ValueError("tile_coords must be strictly sorted by tile coordinate")
        if any(
            coord[dimension] * tile_shape[dimension] >= shape[dimension]
            for coord in coords
            for dimension in range(len(shape))
        ):
            raise ValueError("tile_coords contains a tile outside global_shape")
        ptr = tuple(self.tile_ptr)
        if (
            not ptr
            or len(ptr) != len(coords) + 1
            or ptr[0] != 0
            or any(type(item) is not int or item < 0 for item in ptr)
            or any(ptr[i] > ptr[i + 1] for i in range(len(ptr) - 1))
        ):
            raise ValueError(
                "tile_ptr must be monotonic and have num_tiles + 1 entries"
            )
        if type(self.nnz) is not int or self.nnz < 0 or ptr[-1] != self.nnz:
            raise ValueError("tile_ptr does not cover nnz")
        for name in ("base_generation", "delta_generation"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.delta_generation <= self.base_generation:
            raise ValueError("delta_generation must be newer than base_generation")
        object.__setattr__(self, "global_shape", shape)
        object.__setattr__(self, "tile_shape", tile_shape)
        object.__setattr__(self, "tile_coords", coords)
        object.__setattr__(self, "tile_ptr", ptr)

    @classmethod
    def from_metadata(
        cls,
        *,
        object_ref: object,
        metadata: Mapping[str, Any],
        tile_coords: Sequence[Sequence[int]],
        tile_ptr: Sequence[int],
    ) -> "SparseObjectIndex":
        """Decode metadata after reading only the compact index members."""

        if metadata.get("schema") != "mooncake.sparse_object":
            raise ValueError("unsupported sparse structured-object schema")
        if metadata.get("version") != 1:
            raise ValueError("unsupported sparse structured-object version")
        if metadata.get("coordinate_space") != "global":
            raise ValueError("source sparse object must use global coordinates")
        members = metadata.get("members")
        if not isinstance(members, Mapping):
            raise ValueError("sparse object metadata is missing members")
        for member_name in ("indices", "values", "tile_coords", "tile_ptr"):
            if not isinstance(members.get(member_name), Mapping):
                raise ValueError(f"sparse object metadata is missing {member_name}")
        indices_dtype = _member_dtype(members["indices"], "indices")
        values_dtype = _member_dtype(members["values"], "values")
        tile_coords_dtype = _member_dtype(members["tile_coords"], "tile_coords")
        _member_dtype(members["tile_ptr"], "tile_ptr")
        if tile_coords_dtype != indices_dtype:
            raise ValueError("tile_coords member dtype must match indices member dtype")
        for metadata_name, member_dtype in (
            ("index_dtype", indices_dtype),
            ("value_dtype", values_dtype),
        ):
            declared_dtype = metadata.get(metadata_name)
            if declared_dtype != member_dtype:
                raise ValueError(
                    f"{metadata_name} does not match the corresponding member dtype"
                )
        shape = members["indices"].get("shape")
        if (
            not isinstance(shape, Sequence)
            or len(shape) != 2
            or type(shape[0]) is not int
            or type(shape[1]) is not int
            or shape[0] < 0
            or shape[1] <= 0
        ):
            raise ValueError("indices member must have shape [nnz, ndim]")
        values_shape = members["values"].get("shape")
        if (
            not isinstance(values_shape, Sequence)
            or len(values_shape) != 1
            or type(values_shape[0]) is not int
            or values_shape[0] != shape[0]
        ):
            raise ValueError("values member must have shape [nnz]")
        tile_coords_shape = members["tile_coords"].get("shape")
        tile_ptr_shape = members["tile_ptr"].get("shape")
        if (
            not isinstance(tile_coords_shape, Sequence)
            or len(tile_coords_shape) != 2
            or type(tile_coords_shape[0]) is not int
            or type(tile_coords_shape[1]) is not int
            or tile_coords_shape[0] < 0
            or tile_coords_shape[1] != shape[1]
            or not isinstance(tile_ptr_shape, Sequence)
            or len(tile_ptr_shape) != 1
            or type(tile_ptr_shape[0]) is not int
            or tile_ptr_shape[0] != tile_coords_shape[0] + 1
        ):
            raise ValueError("sparse tile index member shapes are inconsistent")
        coordinate_rank = metadata.get("coordinate_rank", shape[1])
        if type(coordinate_rank) is not int or coordinate_rank != shape[1]:
            raise ValueError("coordinate_rank does not match indices member")
        if "nnz" in metadata and (
            type(metadata["nnz"]) is not int or metadata["nnz"] != shape[0]
        ):
            raise ValueError("nnz does not match indices member")
        global_shape = normalize_shape(
            metadata.get("global_shape", ()), "global_shape", positive=True
        )
        if len(global_shape) != shape[1]:
            raise ValueError("indices rank does not match global_shape")
        global_offset = normalize_shape(
            metadata.get("global_offset", ()), "global_offset", positive=False
        )
        local_shape = normalize_shape(
            metadata.get("local_shape", ()), "local_shape", positive=True
        )
        if len(global_offset) != len(global_shape) or len(local_shape) != len(
            global_shape
        ):
            raise ValueError("source geometry rank differs from global_shape")
        if any(
            global_offset[dimension] + local_shape[dimension] > global_shape[dimension]
            for dimension in range(len(global_shape))
        ):
            raise ValueError("source sparse object geometry exceeds global_shape")
        tile_shape = normalize_shape(
            metadata.get("tile_shape", ()), "tile_shape", positive=True
        )
        if len(tile_shape) != len(global_shape):
            raise ValueError("tile_shape rank differs from global_shape")
        for name in ("base_generation", "delta_generation"):
            if type(metadata.get(name)) is not int or metadata[name] < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if metadata["delta_generation"] <= metadata["base_generation"]:
            raise ValueError("delta_generation must be newer than base_generation")
        tensor_id = metadata.get("tensor_id")
        if not isinstance(tensor_id, str) or not tensor_id:
            raise ValueError("sparse object metadata is missing tensor_id")
        normalized_tile_coords = tuple(tuple(coord) for coord in tile_coords)
        normalized_tile_ptr = tuple(tile_ptr)
        if len(normalized_tile_coords) != tile_coords_shape[0]:
            raise ValueError("tile_coords payload shape differs from metadata")
        if len(normalized_tile_ptr) != tile_ptr_shape[0]:
            raise ValueError("tile_ptr payload shape differs from metadata")
        return cls(
            object_ref=object_ref_key(object_ref),
            tensor_id=tensor_id,
            global_shape=global_shape,
            tile_shape=tile_shape,
            tile_coords=normalized_tile_coords,
            tile_ptr=normalized_tile_ptr,
            nnz=int(shape[0]),
            base_generation=int(metadata.get("base_generation", -1)),
            delta_generation=int(metadata.get("delta_generation", -1)),
        )


@dataclass(frozen=True)
class SparseObjectRegion:
    """One aligned range read and its target logical box."""

    tensor_id: str
    source_object_ref: str
    source_global_box: Box
    target_global_offset: tuple[int, ...]
    target_local_shape: tuple[int, ...]
    target_placement: Placement
    indices_range: tuple[int, int]
    values_range: tuple[int, int]
    exact_coordinate_filter: bool

    def __post_init__(self) -> None:
        if not isinstance(self.tensor_id, str) or not self.tensor_id:
            raise ValueError("tensor_id must be a non-empty string")
        if not isinstance(self.source_object_ref, str) or not self.source_object_ref:
            raise ValueError("source_object_ref must be a non-empty string")
        box = _normalize_box(self.source_global_box, "source_global_box")
        offset = normalize_shape(
            self.target_global_offset, "target_global_offset", positive=False
        )
        shape = normalize_shape(
            self.target_local_shape, "target_local_shape", positive=True
        )
        if len(offset) != len(shape) or len(offset) != len(box[0]):
            raise ValueError("target geometry rank differs from source box")
        if self.indices_range != self.values_range:
            raise ValueError("indices and values ranges must be identical")
        start, end = self.indices_range
        if type(start) is not int or type(end) is not int or start < 0 or end <= start:
            raise ValueError("COO ranges must be non-empty")
        object.__setattr__(self, "source_global_box", box)
        object.__setattr__(self, "target_global_offset", offset)
        object.__setattr__(self, "target_local_shape", shape)

    @property
    def target_global_box(self) -> Box:
        end = tuple(
            self.target_global_offset[d] + self.target_local_shape[d]
            for d in range(len(self.target_global_offset))
        )
        return self.target_global_offset, end

    @property
    def source_member_ranges(self) -> dict[str, tuple[int, int]]:
        return {"indices": self.indices_range, "values": self.values_range}

    @property
    def apply(self) -> str:
        """The only valid consumer operation for a sparse delta."""

        return "scatter_add"


@dataclass(frozen=True)
class SparseObjectPlan:
    """All source ranges required to materialize one target object."""

    tensor_id: str
    target_placement: Placement
    target_global_offset: tuple[int, ...]
    target_local_shape: tuple[int, ...]
    base_generation: int
    delta_generation: int
    regions: tuple[SparseObjectRegion, ...]

    @property
    def source_ranges(self) -> tuple[tuple[str, tuple[int, int]], ...]:
        return tuple(
            (region.source_object_ref, region.indices_range) for region in self.regions
        )

    @property
    def range_request_count(self) -> int:
        return len(self.regions)

    @property
    def apply(self) -> str:
        return "scatter_add"


class SparseObjectStorePlanner:
    """Plan sparse COO range reads for arbitrary named-axis placement."""

    def __init__(self) -> None:
        # ``tile_coords`` is sorted by tile row/column when the structured
        # object is committed.  Cache only tiny row/column-group indexes
        # derived from that member; never cache the source COO payload here.
        self._tile_row_cache: dict[
            str, tuple[tuple[int, ...], tuple[tuple[int, int, tuple[int, ...]], ...]]
        ] = {}
        self._tile_col_cache: dict[
            str,
            tuple[tuple[int, ...], tuple[tuple[tuple[int, ...], tuple[int, ...]], ...]],
        ] = {}

    @staticmethod
    def _intersection(source: Any, target: Any) -> Box | None:
        return box_intersection(
            tuple(source.global_offset),
            tuple(source.local_shape),
            tuple(target.global_offset),
            tuple(target.local_shape),
        )

    def select_source_fragments(
        self,
        *,
        tensor_id: str,
        target: Any,
        source_fragments: Iterable[Any],
        tensor: TensorDescriptor,
    ) -> tuple[Any, ...]:
        """Filter source inventory before any source-object index is read.

        The source manifest can contain all tensors and all placements.  This
        method is intentionally part of the address-free planner so Store
        adapters do not duplicate placement and box-intersection rules.
        """

        if tensor.tensor_id != tensor_id:
            raise ValueError("tensor descriptor does not match tensor_id")
        self._validate_target_fragment(target, tensor=tensor)
        target_placement = normalize_placement(tuple(target.target_placement))
        groups: dict[tuple[tuple[int, ...], tuple[int, ...]], list[Any]] = {}
        for source in source_fragments:
            if source.tensor_id != tensor_id:
                continue
            self._validate_source_fragment(source, tensor=tensor)
            geometry_key = (
                tuple(source.global_offset),
                tuple(source.local_shape),
            )
            groups.setdefault(geometry_key, []).append(source)
        return self._select_from_groups(
            target=target,
            tensor=tensor,
            target_placement=target_placement,
            groups=groups,
        )

    def _select_from_groups(
        self,
        *,
        target: Any,
        tensor: TensorDescriptor,
        target_placement: Placement,
        groups: Mapping[tuple[tuple[int, ...], tuple[int, ...]], list[Any]],
        candidate_index: _CandidateBoxIndex | None = None,
    ) -> tuple[Any, ...]:
        """Select one replica per intersecting source geometry."""

        if not groups:
            return ()
        candidate_index = candidate_index or _CandidateBoxIndex.build(
            tuple(tuple(group) for group in groups.values())
        )
        selected: list[Any] = []
        for group in candidate_index.query(target):
            eligible = [
                source
                for source in group
                if placement_matches(
                    tensor,
                    normalize_placement(tuple(source.source_placement)),
                    target_placement,
                )
                and self._intersection(source, target) is not None
            ]
            if eligible:
                selected.append(min(eligible, key=self._source_sort_key))
        return tuple(selected)

    @staticmethod
    def _source_sort_key(source: Any) -> tuple[Any, ...]:
        """Stable representative order matching dense source selection."""

        return (
            normalize_placement(tuple(source.source_placement)),
            object_ref_key(source.object_ref),
        )

    @staticmethod
    def _validate_source_fragment(source: Any, *, tensor: TensorDescriptor) -> None:
        """Validate source geometry without imposing a single-source layout."""

        if not hasattr(source, "global_shape"):
            raise ValueError("sparse source fragment is missing geometry")
        SparseObjectStorePlanner._fragment_geometry(
            source, tensor=tensor, label="source"
        )
        if not isinstance(source.source_placement, Sequence):
            raise ValueError("sparse source fragment placement is invalid")
        normalize_placement(tuple(source.source_placement))
        object_ref_key(source.object_ref)

    @staticmethod
    def _validate_target_fragment(target: Any, *, tensor: TensorDescriptor) -> None:
        SparseObjectStorePlanner._fragment_geometry(
            target, tensor=tensor, label="target"
        )

    @staticmethod
    def _fragment_geometry(
        fragment: Any, *, tensor: TensorDescriptor, label: str
    ) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
        """Normalize and validate one source/target logical fragment."""

        try:
            declared_shape = getattr(fragment, "global_shape", tensor.global_shape)
            shape = normalize_shape(
                declared_shape, f"{label}.global_shape", positive=True
            )
            offset = normalize_shape(
                fragment.global_offset, f"{label}.global_offset", positive=False
            )
            local = normalize_shape(
                fragment.local_shape, f"{label}.local_shape", positive=True
            )
        except AttributeError as error:
            raise ValueError(f"sparse {label} fragment is missing geometry") from error
        if len(offset) != len(local) or len(offset) != len(shape):
            raise ValueError(f"sparse {label} fragment geometry rank differs")
        if shape != tuple(tensor.global_shape):
            raise ValueError(
                f"sparse {label} fragment for {tensor.tensor_id!r} has a different global shape"
            )
        if any(offset[d] + local[d] > shape[d] for d in range(len(shape))):
            raise ValueError(f"sparse {label} fragment exceeds global tensor geometry")
        return shape, offset, local

    def _row_groups(
        self, index: SparseObjectIndex
    ) -> tuple[tuple[int, ...], tuple[tuple[int, int, tuple[int, ...]], ...]]:
        """Return a cached 2-D tile-row index derived from ``tile_coords``.

        Each group is ``(start, end, columns)`` in the sorted tile table.  The
        auxiliary index is metadata-only and is built once per source object;
        it lets a row-oriented target locate tiles with two binary searches
        instead of scanning every tile in the object.
        """

        key = index.object_ref
        cached = self._tile_row_cache.get(key)
        if cached is not None:
            return cached
        rows: list[int] = []
        groups: list[tuple[int, int, tuple[int, ...]]] = []
        coords = index.tile_coords
        position = 0
        while position < len(coords):
            row = coords[position][0]
            end = position + 1
            while end < len(coords) and coords[end][0] == row:
                end += 1
            rows.append(row)
            groups.append(
                (position, end, tuple(coords[item][1] for item in range(position, end)))
            )
            position = end
        result = (tuple(rows), tuple(groups))
        self._tile_row_cache[key] = result
        return result

    def _column_groups(
        self, index: SparseObjectIndex
    ) -> tuple[tuple[int, ...], tuple[tuple[tuple[int, ...], tuple[int, ...]], ...]]:
        """Return a cached column-group index for a 2-D COO tile table.

        The committed tile table is row-major, so tiles belonging to one
        column are not contiguous.  The compact secondary index stores the
        sorted row coordinates and the corresponding tile-table positions for
        each column.  A column-oriented target can therefore binary-search
        rows without scanning unrelated columns or COO entries.
        """

        key = index.object_ref
        cached = self._tile_col_cache.get(key)
        if cached is not None:
            return cached
        by_column: dict[int, list[tuple[int, int]]] = {}
        for position, (row, column) in enumerate(index.tile_coords):
            by_column.setdefault(column, []).append((row, position))
        columns: list[int] = []
        groups: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
        for column in sorted(by_column):
            rows_and_positions = by_column[column]
            columns.append(column)
            groups.append(
                (
                    tuple(row for row, _position in rows_and_positions),
                    tuple(position for _row, position in rows_and_positions),
                )
            )
        result = (tuple(columns), tuple(groups))
        self._tile_col_cache[key] = result
        return result

    def _tile_ranges(
        self, index: SparseObjectIndex, overlap: Box
    ) -> tuple[tuple[int, int], ...]:
        begin, end = overlap
        if len(begin) != len(index.tile_shape):
            raise ValueError("overlap rank differs from tile index")

        # Two-dimensional tensor parallel boxes can be row slices or column
        # slices. Keep both secondary indexes and use the cheaper candidate
        # direction. This avoids an O(num_tiles) scan for every target while
        # preserving the compact COO contract.
        if len(begin) == 2:
            row_begin = begin[0] // index.tile_shape[0]
            row_end = (end[0] - 1) // index.tile_shape[0]
            col_begin = begin[1] // index.tile_shape[1]
            col_end = (end[1] - 1) // index.tile_shape[1]
            rows, row_groups = self._row_groups(index)
            row_group_begin = bisect_left(rows, row_begin)
            row_group_end = bisect_right(rows, row_end)
            columns, column_groups = self._column_groups(index)
            column_group_begin = bisect_left(columns, col_begin)
            column_group_end = bisect_right(columns, col_end)
            # A row slice usually touches fewer row groups; a column slice
            # usually touches fewer column groups.  Select the smaller outer
            # loop in O(1), then use binary search for the other coordinate.
            # The tile table is never walked twice just to choose a direction.
            if (row_group_end - row_group_begin) <= (
                column_group_end - column_group_begin
            ):
                positions: list[int] = []
                for start, _stop, row_columns in row_groups[
                    row_group_begin:row_group_end
                ]:
                    col_start = bisect_left(row_columns, col_begin)
                    col_stop = bisect_right(row_columns, col_end)
                    positions.extend(range(start + col_start, start + col_stop))
            else:
                positions = []
                for rows_for_column, column_positions in column_groups[
                    column_group_begin:column_group_end
                ]:
                    row_start = bisect_left(rows_for_column, row_begin)
                    row_stop = bisect_right(rows_for_column, row_end)
                    positions.extend(column_positions[row_start:row_stop])

            # Column groups are not contiguous in the row-major tile table;
            # sort before turning entry offsets into coalesced ranges.
            ranges = [
                (index.tile_ptr[position], index.tile_ptr[position + 1])
                for position in sorted(set(positions))
            ]
            return _merge_sorted_ranges(ranges)

        # Generic N-D fallback.  The object contract remains N-D, while the
        # 2-D path above is the performance path used by sparse weights.
        ranges: list[tuple[int, int]] = []
        for tile, start, stop in zip(
            index.tile_coords, index.tile_ptr[:-1], index.tile_ptr[1:]
        ):
            tile_begin = tuple(tile[d] * index.tile_shape[d] for d in range(len(tile)))
            tile_end = tuple(
                tile_begin[d] + index.tile_shape[d] for d in range(len(tile))
            )
            if all(
                tile_begin[d] < end[d] and begin[d] < tile_end[d]
                for d in range(len(tile))
            ):
                ranges.append((start, stop))
        return _merge_sorted_ranges(ranges)

    @staticmethod
    def _needs_exact_filter(index: SparseObjectIndex, overlap: Box) -> bool:
        begin, end = overlap
        return any(
            value % tile != 0
            for coordinate in (begin, end)
            for value, tile in zip(coordinate, index.tile_shape)
        )

    def plan_target(
        self,
        *,
        tensor_id: str,
        tensor: TensorDescriptor,
        target: Any,
        source_fragments: Iterable[Any],
        source_indexes: Mapping[str, SparseObjectIndex],
        base_generation: int,
        delta_generation: int,
    ) -> SparseObjectPlan:
        """Plan one target without materializing ``indices`` or ``values``."""

        self._validate_target_fragment(target, tensor=tensor)
        candidates = self.select_source_fragments(
            tensor_id=tensor_id,
            target=target,
            source_fragments=source_fragments,
            tensor=tensor,
        )
        return self._plan_target_candidates(
            tensor_id=tensor_id,
            target=target,
            candidates=candidates,
            source_indexes=source_indexes,
            base_generation=base_generation,
            delta_generation=delta_generation,
        )

    def _plan_target_candidates(
        self,
        *,
        tensor_id: str,
        target: Any,
        candidates: Iterable[Any],
        source_indexes: Mapping[str, SparseObjectIndex],
        base_generation: int,
        delta_generation: int,
    ) -> SparseObjectPlan:
        regions: list[SparseObjectRegion] = []
        target_offset = normalize_shape(
            target.global_offset, "target.global_offset", positive=False
        )
        target_shape = normalize_shape(
            target.local_shape, "target.local_shape", positive=True
        )
        target_placement = normalize_placement(tuple(target.target_placement))
        overlap_boxes: list[tuple[tuple[int, ...], tuple[int, ...]]] = []
        for source in candidates:
            overlap = self._intersection(source, target)
            if overlap is None:
                continue
            overlap_boxes.append(
                (
                    overlap[0],
                    tuple(
                        overlap[1][dimension] - overlap[0][dimension]
                        for dimension in range(len(overlap[0]))
                    ),
                )
            )
            key = object_ref_key(source.object_ref)
            try:
                index = source_indexes[key]
            except KeyError as error:
                raise ValueError(
                    f"missing sparse tile index for source object {key!r}"
                ) from error
            if index.tensor_id != tensor_id:
                raise ValueError("tile index tensor_id does not match source fragment")
            if (index.base_generation, index.delta_generation) != (
                base_generation,
                delta_generation,
            ):
                raise ValueError("tile index generation does not match manifest")
            if tuple(index.global_shape) != tuple(source.global_shape):
                raise ValueError(
                    "tile index global_shape does not match source fragment"
                )
            for start, stop in self._tile_ranges(index, overlap):
                regions.append(
                    SparseObjectRegion(
                        tensor_id=tensor_id,
                        source_object_ref=key,
                        source_global_box=overlap,
                        target_global_offset=target_offset,
                        target_local_shape=target_shape,
                        target_placement=target_placement,
                        indices_range=(start, stop),
                        values_range=(start, stop),
                        exact_coordinate_filter=self._needs_exact_filter(
                            index, overlap
                        ),
                    )
                )
        if not boxes_exactly_cover(target_offset, target_shape, overlap_boxes):
            raise ValueError(
                f"target sparse fragment is not fully covered: {tensor_id}"
            )
        regions.sort(key=lambda item: (item.source_object_ref, item.indices_range))
        return SparseObjectPlan(
            tensor_id=tensor_id,
            target_placement=target_placement,
            target_global_offset=target_offset,
            target_local_shape=target_shape,
            base_generation=base_generation,
            delta_generation=delta_generation,
            regions=tuple(regions),
        )

    def plan_targets(
        self,
        *,
        targets: Iterable[Any],
        source_fragments: Iterable[Any],
        tensors: Mapping[str, TensorDescriptor],
        source_indexes: Mapping[str, SparseObjectIndex],
        base_generation: int,
        delta_generation: int,
    ) -> tuple[SparseObjectPlan, ...]:
        """Plan a target set while reusing the caller's index cache."""

        grouped: dict[str, list[Any]] = {}
        for source in source_fragments:
            grouped.setdefault(source.tensor_id, []).append(source)
        groups_by_tensor: dict[
            str, dict[tuple[tuple[int, ...], tuple[int, ...]], list[Any]]
        ] = {}
        for tensor_id, items in grouped.items():
            tensor = tensors.get(tensor_id)
            if tensor is None:
                continue
            groups: dict[tuple[tuple[int, ...], tuple[int, ...]], list[Any]] = {}
            for source in items:
                self._validate_source_fragment(source, tensor=tensor)
                groups.setdefault(
                    (tuple(source.global_offset), tuple(source.local_shape)), []
                ).append(source)
            groups_by_tensor[tensor_id] = groups
        candidate_indexes = {
            tensor_id: _CandidateBoxIndex.build(
                tuple(tuple(group) for group in groups.values())
            )
            for tensor_id, groups in groups_by_tensor.items()
            if groups
        }
        plans: list[SparseObjectPlan] = []
        for target in targets:
            tensor_id = target.tensor_id
            try:
                tensor = tensors[tensor_id]
            except KeyError as error:
                raise ValueError(
                    f"missing tensor descriptor for {tensor_id!r}"
                ) from error
            self._validate_target_fragment(target, tensor=tensor)
            target_placement = normalize_placement(tuple(target.target_placement))
            candidates = self._select_from_groups(
                target=target,
                tensor=tensor,
                target_placement=target_placement,
                groups=groups_by_tensor.get(tensor_id, {}),
                candidate_index=candidate_indexes.get(tensor_id),
            )
            plans.append(
                self._plan_target_candidates(
                    tensor_id=tensor_id,
                    target=target,
                    candidates=candidates,
                    source_indexes=source_indexes,
                    base_generation=base_generation,
                    delta_generation=delta_generation,
                )
            )
        return tuple(plans)


def plan_sparse_object_target(**kwargs: Any) -> SparseObjectPlan:
    """Functional facade for planning one target sparse object."""

    return SparseObjectStorePlanner().plan_target(**kwargs)


__all__ = [
    "Box",
    "Placement",
    "placement_matches",
    "SparseObjectIndex",
    "SparseObjectPlan",
    "SparseObjectRegion",
    "SparseObjectStorePlanner",
    "canonical_source_placement",
    "is_canonical_source",
    "normalize_placement",
    "normalize_shape",
    "object_ref_key",
    "plan_sparse_object_target",
]
