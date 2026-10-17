"""Target-driven reshard planning: source manifest to byte ranges.

Planning runs in the reader process and is driven by the *target* rank's
requirements. For every global layer and K/V component the target needs,
the planner intersects the target's KV-head interval with the coverage
described by the source manifest, and emits ordered byte ranges: one source
range inside the covering Part and one target range in the reader's
registered memory. Either side's layout may differ from the other's; the
two only meet at logical (layer, head) coordinates.

Ranges are merged only when the merged run is contiguous on both sides, so
a plan never reads or overwrites unrelated data just to enlarge I/O, and a
stided side simply yields more, smaller ranges.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from ..types import (
    KVCacheComponent,
    require_integer,
    require_integer_tuple,
)
from .layout import HostLayoutGeometry, KVCacheHostLayout
from .source_manifest import SourceLayoutManifest, SourcePartCoverage

__all__ = [
    "ReshardPlanningError",
    "TargetRequirement",
    "PlannedByteRange",
    "ReshardReadPlan",
    "part_geometry",
    "target_geometry",
    "plan_reshard_read",
]


class ReshardPlanningError(Exception):
    """The requirement cannot be planned against the source manifest."""


@dataclass(frozen=True)
class TargetRequirement:
    """What one reading rank needs from one page object.

    ``layer_ids`` is the target's own ordered global layer ownership (PP
    assignments may be uneven); ``head_start``/``head_count`` is the target
    rank's KV-head interval of the component.
    """

    component: KVCacheComponent
    layer_ids: tuple[int, ...]
    head_start: int
    head_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.component, KVCacheComponent):
            raise ValueError("component must be a KVCacheComponent")  # noqa: TRY004
        layers = require_integer_tuple(self.layer_ids, "layer_ids")
        if not layers:
            raise ValueError("layer_ids must not be empty")
        if len(layers) != len(set(layers)):
            raise ValueError("layer_ids must be unique")
        object.__setattr__(self, "layer_ids", tuple(layers))
        require_integer(self.head_start, "head_start")
        require_integer(self.head_count, "head_count", minimum=1)

    def fingerprint(self) -> str:
        """Stable identity of the requirement for template caching."""

        return (
            f"{self.component.value}:"
            f"{','.join(str(layer) for layer in self.layer_ids)}:"
            f"{self.head_start}+{self.head_count}"
        )


@dataclass(frozen=True)
class PlannedByteRange:
    """One contiguous source run into one contiguous target run.

    Both offsets are relative: the source offset is inside the covering
    Part's component region, the target offset inside the reader's
    destination buffer. No live address is recorded, so a plan can be
    cached and reused across requests and after the writer exits.
    """

    source_part_index: int
    source_offset: int
    source_length: int
    target_offset: int
    target_length: int

    def __post_init__(self) -> None:
        require_integer(self.source_part_index, "source_part_index")
        for name in (
            "source_offset",
            "source_length",
            "target_offset",
            "target_length",
        ):
            require_integer(getattr(self, name), name)
        if self.source_length != self.target_length:
            raise ValueError(  # noqa: TRY003
                "a byte range moves the same number of bytes on both sides"
            )
        if self.source_length <= 0:
            raise ValueError("a byte range must move at least one byte")


@dataclass(frozen=True)
class ReshardReadPlan:
    """Ordered byte ranges plus the coverage accounting of one plan."""

    ranges: tuple[PlannedByteRange, ...]
    required_element_count: int
    planned_element_count: int

    @property
    def planned_bytes(self) -> int:
        return sum(item.source_length for item in self.ranges)


@dataclass(frozen=True)
class _MergedRun:
    """A run of boxes that may collapse into one byte range."""

    part_index: int
    source_layer_first: int
    source_layer_count: int
    target_layer_first: int
    target_layer_count: int
    head_start: int
    head_count: int


def _component_head_dim(
    manifest: SourceLayoutManifest, component: KVCacheComponent
) -> int:
    if component is KVCacheComponent.KEY:
        return manifest.domain.key_head_dim
    return manifest.domain.value_head_dim


def _part_geometry(
    manifest: SourceLayoutManifest,
    part: SourcePartCoverage,
    component: KVCacheComponent,
) -> HostLayoutGeometry:
    return HostLayoutGeometry(
        layout=manifest.layout,
        page_size=manifest.domain.page_size,
        layer_count=len(part.layer_ids),
        kv_heads=part.head_count,
        head_dim=_component_head_dim(manifest, component),
        itemsize=manifest.domain.itemsize,
    )


def _target_geometry(
    manifest: SourceLayoutManifest,
    requirement: TargetRequirement,
    target_layout: KVCacheHostLayout,
) -> HostLayoutGeometry:
    return HostLayoutGeometry(
        layout=target_layout,
        page_size=manifest.domain.page_size,
        layer_count=len(requirement.layer_ids),
        kv_heads=requirement.head_count,
        head_dim=_component_head_dim(manifest, requirement.component),
        itemsize=manifest.domain.itemsize,
    )


def _atomic_boxes(
    manifest: SourceLayoutManifest, requirement: TargetRequirement
) -> list[_MergedRun]:
    """Initial one-(layer, head) boxes, in target layer order then head order.

    The atomic unit is a single (layer, head) element because every layout
    keeps at least that unit contiguous; bigger runs are rebuilt by
    ``_merge_runs`` where the geometry actually allows them. Starting from
    part-split head intervals instead would embed the assumption that a
    part's head interval is contiguous, which fails in HPLD (head is the
    outermost axis there).
    """

    runs: list[_MergedRun] = []
    for target_layer_index, layer_id in enumerate(requirement.layer_ids):
        for part, sub_start, sub_count in manifest.head_split(
            layer_id, requirement.head_start, requirement.head_count
        ):
            if not part.contains_layer(layer_id):
                raise ReshardPlanningError(  # noqa: TRY003
                    f"no part covers layer {layer_id} with heads "
                    f"[{sub_start}, {sub_start + sub_count})"
                )
            for head in range(sub_start, sub_start + sub_count):
                runs.append(
                    _MergedRun(
                        part_index=part.part_index,
                        source_layer_first=part.layer_ids.index(layer_id),
                        source_layer_count=1,
                        target_layer_first=target_layer_index,
                        target_layer_count=1,
                        head_start=head,
                        head_count=1,
                    )
                )
    return runs


def _run_bytes(
    run: _MergedRun,
    source_geometry: HostLayoutGeometry,
    target_geometry: HostLayoutGeometry,
) -> int | None:
    """Byte length if the run is contiguous on both sides, else None."""

    source_run = source_geometry.contiguous_run_bytes(
        layers=run.source_layer_count, heads=run.head_count
    )
    if source_run is None:
        return None
    target_run = target_geometry.contiguous_run_bytes(
        layers=run.target_layer_count, heads=run.head_count
    )
    if target_run is None or target_run != source_run:
        return None
    return source_run


def _try_layer_extend(last: _MergedRun, nxt: _MergedRun) -> _MergedRun | None:
    """Extend a run by the next layer when the head span is identical."""

    if last.part_index != nxt.part_index:
        return None
    if (
        last.source_layer_first + last.source_layer_count != nxt.source_layer_first
        or last.target_layer_first + last.target_layer_count != nxt.target_layer_first
        or nxt.source_layer_count != 1
        or nxt.target_layer_count != 1
        or (last.head_start, last.head_count) != (nxt.head_start, nxt.head_count)
    ):
        return None
    return replace(
        last,
        source_layer_count=last.source_layer_count + 1,
        target_layer_count=last.target_layer_count + 1,
    )


def _try_head_extend(last: _MergedRun, nxt: _MergedRun) -> _MergedRun | None:
    """Extend a run by adjacent heads inside the same layer."""

    if last.part_index != nxt.part_index:
        return None
    if (
        last.source_layer_count != 1
        or nxt.source_layer_count != 1
        or last.source_layer_first != nxt.source_layer_first
        or last.target_layer_first != nxt.target_layer_first
        or last.head_start + last.head_count != nxt.head_start
        or nxt.head_count != 1
    ):
        return None
    return replace(last, head_count=last.head_count + nxt.head_count)


def _merge_runs(
    boxes: list[_MergedRun],
    source_geometry: HostLayoutGeometry,
    target_geometry: HostLayoutGeometry,
) -> list[_MergedRun]:
    """Greedy left-to-right merge with a final tail-merge pass.

    Adjacent runs merge when the merged run stays contiguous on both sides.
    The tail pass lets whole-layer runs that were separated by an
    intervening head split (different layers split heads at different part
    boundaries) still collapse.
    """

    merged: list[_MergedRun] = []
    for box in boxes:
        if merged:
            last = merged[-1]
            extended = _try_layer_extend(last, box)
            if extended is None:
                extended = _try_head_extend(last, box)
            if extended is not None and _run_bytes(
                extended, source_geometry, target_geometry
            ):
                merged[-1] = extended
                continue
        merged.append(box)
    changed = True
    while changed and len(merged) > 1:
        changed = False
        for index in range(len(merged) - 1, 0, -1):
            extended = _try_layer_extend(merged[index - 1], merged[index])
            if extended is None:
                continue
            if _run_bytes(extended, source_geometry, target_geometry):
                merged[index - 1 : index + 1] = [extended]
                changed = True
    return merged


def part_geometry(
    manifest: SourceLayoutManifest, part_index: int, component: KVCacheComponent
) -> HostLayoutGeometry:
    """Derived byte geometry of one Part for one K/V component.

    Part-local coordinates: layers are indexed by position in the part's
    ordered ``layer_ids``, heads by ``global_head - part.head_start``.
    """

    return _part_geometry(manifest, manifest.part(part_index), component)


def target_geometry(
    manifest: SourceLayoutManifest,
    requirement: TargetRequirement,
    target_layout: KVCacheHostLayout,
) -> HostLayoutGeometry:
    """Derived byte geometry of the reader's destination buffer.

    Target-local coordinates: layers by position in the requirement's
    ordered ``layer_ids``, heads by ``global_head - requirement.head_start``.
    """

    return _target_geometry(manifest, requirement, target_layout)


def plan_reshard_read(
    manifest: SourceLayoutManifest,
    requirement: TargetRequirement,
    target_layout: KVCacheHostLayout,
) -> ReshardReadPlan:
    """Plan the byte ranges that satisfy ``requirement`` from ``manifest``.

    ``target_layout`` is the reader's own host layout; the target buffer is
    one page slot holding the requirement's own layers and heads, K before
    V. All offsets in the returned plan are relative to the start of the
    respective region.

    Raises ``ReshardPlanningError`` when the manifest cannot cover the
    requirement, or when the produced ranges do not account for every
    required element exactly once.
    """

    for layer_id in requirement.layer_ids:
        if layer_id not in manifest.domain.global_layer_ids:
            raise ReshardPlanningError(  # noqa: TRY003
                f"target requires unknown global layer {layer_id}"
            )
    if requirement.head_start + requirement.head_count > manifest.domain.total_kv_heads:
        raise ReshardPlanningError("target head interval exceeds total_kv_heads")

    boxes = _atomic_boxes(manifest, requirement)
    target_geometry = _target_geometry(manifest, requirement, target_layout)
    part_geometries = {
        part.part_index: _part_geometry(manifest, part, requirement.component)
        for part in manifest.parts
    }

    # Merge per part (runs never cross parts), then keep target-layer order.
    per_part: dict[int, list[_MergedRun]] = {}
    for box in boxes:
        per_part.setdefault(box.part_index, []).append(box)
    merged: list[_MergedRun] = []
    for part_index in sorted(per_part):
        merged.extend(
            _merge_runs(
                per_part[part_index],
                part_geometries[part_index],
                target_geometry,
            )
        )

    element_bytes = target_geometry.element_bytes
    component_index = 0 if requirement.component is KVCacheComponent.KEY else 1
    ranges: list[PlannedByteRange] = []
    for run in merged:
        part = manifest.part(run.part_index)
        source_geometry = part_geometries[run.part_index]
        length = _run_bytes(run, source_geometry, target_geometry)
        if length is None:
            raise ReshardPlanningError(  # noqa: TRY003
                f"run in part {run.part_index} is strided on at least one side"
            )
        source_offset = source_geometry.byte_offset(
            page=0,
            layer=run.source_layer_first,
            head=run.head_start - part.head_start,
            component=component_index,
        )
        target_offset = target_geometry.byte_offset(
            page=0,
            layer=run.target_layer_first,
            head=run.head_start - requirement.head_start,
            component=component_index,
        )
        ranges.append(
            PlannedByteRange(
                source_part_index=run.part_index,
                source_offset=source_offset,
                source_length=length,
                target_offset=target_offset,
                target_length=length,
            )
        )

    required_element_count = len(requirement.layer_ids) * requirement.head_count
    planned_element_count = sum(item.source_length for item in ranges) // element_bytes
    if planned_element_count != required_element_count:
        raise ReshardPlanningError(  # noqa: TRY003
            f"plan covers {planned_element_count} elements, requirement asks "
            f"for {required_element_count}"
        )
    return ReshardReadPlan(
        ranges=tuple(ranges),
        required_element_count=required_element_count,
        planned_element_count=planned_element_count,
    )
