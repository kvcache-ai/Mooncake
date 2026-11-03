from __future__ import annotations

from dataclasses import dataclass, field
from math import prod
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generic,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

from ..._compat import _strict_zip
from ..._typing import TypeAlias
from ...contracts import (
    LeaseId,
    ParticipantId,
    PlacementFragmentId,
    PlacementId,
    ResourceId,
    RevisionId,
    RuntimeFragmentId,
    RuntimeInstanceId,
    TensorId,
)
from ...geometry import box_contains as _box_contains
from ..manifest import (
    ParallelRank,
    PlacementFragment,
    TensorDescriptor,
    WeightPlacementManifest,
)
from ..storage_manifest import (
    StoredFragmentSnapshot,
    StoredManifestIdentity,
    StoredWeightManifest,
    validate_weight_manifest_snapshot,
)
from .geometry import (
    _derive_region_geometry,
    _fragment_itemsize,
    _validate_outer_strides,
)
from .fragments import (
    BoundWeightFragment,
    ExecutableSourceFragment,
    ExecutableTargetFragment,
    GeometryFragment,
    LogicalSourceFragment,
    LogicalTargetFragment,
)

if TYPE_CHECKING:
    from .bound_contracts import ExecutorTransferPlan


RuntimeTensorOwner = tuple[tuple[str, int], ...]
_SourceFragmentT = TypeVar("_SourceFragmentT", bound=GeometryFragment)
_TargetFragmentT = TypeVar("_TargetFragmentT", bound=GeometryFragment)


@dataclass(frozen=True)
class PipelineRouteGroup:
    source_pp: Optional[int]
    source_pipeline_stage_id: Optional[int]
    target_pp: int
    target_pipeline_stage_id: Optional[int]
    operation_indices: tuple[int, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation_indices", tuple(self.operation_indices))
        if self.source_pp is not None and (
            type(self.source_pp) is not int or self.source_pp < 0
        ):
            raise ValueError("pipeline route source_pp must be non-negative")
        if type(self.target_pp) is not int or self.target_pp < 0:
            raise ValueError("pipeline route target_pp must be non-negative")
        for name in (
            "source_pipeline_stage_id",
            "target_pipeline_stage_id",
        ):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"pipeline route {name} must be non-negative")
        if any(type(index) is not int or index < 0 for index in self.operation_indices):
            raise ValueError("pipeline route indices must be non-negative integers")
        if len(self.operation_indices) != len(set(self.operation_indices)):
            raise ValueError("pipeline route has duplicate operation indices")


@dataclass(frozen=True)
class PlanningLimits:
    """Fail-closed limits for canonical N-D planning and lowering."""

    max_transfer_regions: int = 1_000_000
    max_segments_per_region: int = 1_000_000
    max_total_lowered_segments: int = 10_000_000

    def __post_init__(self) -> None:
        for name in (
            "max_transfer_regions",
            "max_segments_per_region",
            "max_total_lowered_segments",
        ):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"planning limit {name} must be a positive integer")


@dataclass(frozen=True)
class TransferRegion(Generic[_SourceFragmentT, _TargetFragmentT]):
    tensor_id: TensorId
    source: _SourceFragmentT
    target: _TargetFragmentT
    overlap_offset: tuple[int, ...]
    overlap_shape: tuple[int, ...]
    source_base_offset: int
    target_base_offset: int
    inner_bytes: int
    outer_loop_counts: tuple[int, ...]
    source_strides: tuple[int, ...]
    target_strides: tuple[int, ...]

    def __post_init__(self) -> None:
        for name in (
            "overlap_offset",
            "overlap_shape",
            "outer_loop_counts",
            "source_strides",
            "target_strides",
        ):
            value = getattr(self, name)
            if isinstance(value, (str, bytes, bytearray)):
                raise ValueError(f"transfer region {name} must contain integers")
            try:
                normalized = tuple(value)
            except TypeError as error:
                raise ValueError(
                    f"transfer region {name} must contain integers"
                ) from error
            if any(type(item) is not int for item in normalized):
                raise ValueError(f"transfer region {name} must contain integers")
            object.__setattr__(self, name, normalized)

        if not self.tensor_id:
            raise ValueError("transfer region tensor_id must not be empty")
        if (
            self.source.tensor_id != self.tensor_id
            or self.target.tensor_id != self.tensor_id
        ):
            raise ValueError("transfer region tensor mismatch")
        ndim = len(self.overlap_offset)
        if (
            ndim == 0
            or len(self.overlap_shape) != ndim
            or len(self.source.global_offset) != ndim
            or len(self.target.global_offset) != ndim
        ):
            raise ValueError("transfer region logical rank mismatch")
        if any(offset < 0 for offset in self.overlap_offset) or any(
            extent <= 0 for extent in self.overlap_shape
        ):
            raise ValueError("transfer region logical box is invalid")
        if not _box_contains(
            self.source.global_offset,
            self.source.local_shape,
            self.overlap_offset,
            self.overlap_shape,
        ):
            raise ValueError("transfer region exceeds source logical fragment")
        if not _box_contains(
            self.target.global_offset,
            self.target.local_shape,
            self.overlap_offset,
            self.overlap_shape,
        ):
            raise ValueError("transfer region exceeds target logical fragment")

        for name in ("source_base_offset", "target_base_offset", "inner_bytes"):
            value = getattr(self, name)
            if type(value) is not int:
                raise ValueError(f"transfer region {name} must be an integer")
        if self.source_base_offset < 0 or self.target_base_offset < 0:
            raise ValueError("transfer region base offsets must be non-negative")
        if self.inner_bytes <= 0:
            raise ValueError("transfer region inner_bytes must be positive")
        if not (
            len(self.outer_loop_counts)
            == len(self.source_strides)
            == len(self.target_strides)
        ):
            raise ValueError("transfer region outer loop rank mismatch")
        if any(count <= 0 for count in self.outer_loop_counts):
            raise ValueError("transfer region outer loop counts must be positive")
        if any(stride < 0 for stride in self.source_strides) or any(
            stride < 0 for stride in self.target_strides
        ):
            raise ValueError("transfer region strides must be non-negative")

        source_itemsize = _fragment_itemsize(self.source)
        target_itemsize = _fragment_itemsize(self.target)
        if source_itemsize != target_itemsize:
            raise ValueError("transfer region source and target itemsize differ")
        (
            expected_source_offset,
            expected_target_offset,
            expected_inner_bytes,
            expected_outer_loop_counts,
            expected_source_strides,
            expected_target_strides,
        ) = _derive_region_geometry(
            self.source,
            self.target,
            self.overlap_offset,
            self.overlap_shape,
        )
        expected_bytes = prod(self.overlap_shape) * source_itemsize
        if self.total_bytes != expected_bytes:
            raise ValueError("transfer region loop geometry does not match overlap")
        if self.source_base_offset != expected_source_offset:
            raise ValueError("transfer region source base offset is inconsistent")
        if self.target_base_offset != expected_target_offset:
            raise ValueError("transfer region target base offset is inconsistent")
        if (
            self.inner_bytes,
            self.outer_loop_counts,
            self.source_strides,
            self.target_strides,
        ) != (
            expected_inner_bytes,
            expected_outer_loop_counts,
            expected_source_strides,
            expected_target_strides,
        ):
            raise ValueError("transfer region loop geometry is not canonical")

        _validate_outer_strides(
            self.outer_loop_counts,
            self.source_strides,
            self.inner_bytes,
            "source",
        )
        _validate_outer_strides(
            self.outer_loop_counts,
            self.target_strides,
            self.inner_bytes,
            "target",
        )
        self.validate_bounds()

    @property
    def segment_count(self) -> int:
        return prod(self.outer_loop_counts)

    @property
    def total_bytes(self) -> int:
        return self.inner_bytes * self.segment_count

    @property
    def source_offset(self) -> int:
        return self.source_base_offset

    @property
    def target_offset(self) -> int:
        return self.target_base_offset

    @property
    def nbytes(self) -> int:
        return self.inner_bytes

    @property
    def repeat(self) -> int:
        return self.segment_count

    @property
    def source_stride(self) -> int:
        if not self.source_strides:
            return 0
        if len(self.source_strides) == 1:
            return self.source_strides[0]
        raise ValueError("N-D transfer region has multiple source strides")

    @property
    def target_stride(self) -> int:
        if not self.target_strides:
            return 0
        if len(self.target_strides) == 1:
            return self.target_strides[0]
        raise ValueError("N-D transfer region has multiple target strides")

    def validate_bounds(self) -> None:
        source_end = (
            self.source_base_offset
            + sum(
                (count - 1) * stride
                for count, stride in _strict_zip(
                    self.outer_loop_counts, self.source_strides
                )
            )
            + self.inner_bytes
        )
        if source_end > self.source.nbytes:
            raise ValueError("transfer region exceeds source fragment")
        target_end = (
            self.target_base_offset
            + sum(
                (count - 1) * stride
                for count, stride in _strict_zip(
                    self.outer_loop_counts, self.target_strides
                )
            )
            + self.inner_bytes
        )
        if target_end > self.target.nbytes:
            raise ValueError("transfer region exceeds target fragment")

    def iter_segments(self, *, max_segments: int) -> Iterable[tuple[int, int, int]]:
        if type(max_segments) is not int or max_segments <= 0:
            raise ValueError("max_segments must be a positive integer")
        if self.segment_count > max_segments:
            raise ValueError(
                "transfer region exceeds max_segments: "
                f"{self.segment_count} > {max_segments}"
            )
        if not self.outer_loop_counts:
            yield self.source_base_offset, self.target_base_offset, self.inner_bytes
            return
        indices = [0] * len(self.outer_loop_counts)
        while True:
            yield (
                self.source_base_offset
                + sum(
                    index * stride
                    for index, stride in _strict_zip(indices, self.source_strides)
                ),
                self.target_base_offset
                + sum(
                    index * stride
                    for index, stride in _strict_zip(indices, self.target_strides)
                ),
                self.inner_bytes,
            )
            for dim in range(len(indices) - 1, -1, -1):
                indices[dim] += 1
                if indices[dim] < self.outer_loop_counts[dim]:
                    break
                indices[dim] = 0
            else:
                return


LogicalTransferRegion: TypeAlias = TransferRegion[
    LogicalSourceFragment,
    LogicalTargetFragment,
]
LogicalTransferOperation: TypeAlias = LogicalTransferRegion

ExecutableTransferRegion: TypeAlias = TransferRegion[
    ExecutableSourceFragment,
    ExecutableTargetFragment,
]
ExecutableTransferOperation: TypeAlias = ExecutableTransferRegion

LiveTransferOperation: TypeAlias = TransferRegion[
    BoundWeightFragment, BoundWeightFragment
]
StoredLoadOperation: TypeAlias = TransferRegion[
    StoredFragmentSnapshot, BoundWeightFragment
]


def _is_canonical_operation(value: object) -> bool:
    return isinstance(value, TransferRegion)


def _validate_logical_operation(
    value: object,
    source_has_placement: bool,
) -> None:
    if not _is_canonical_operation(value):
        raise ValueError(
            "logical transfer plan requires a canonical transfer operation"
        )
    operation = cast(LogicalTransferOperation, value)
    if not isinstance(operation.target, PlacementFragment):
        raise ValueError("logical transfer plan target must be a placement")
    if source_has_placement:
        if not isinstance(operation.source, PlacementFragment):
            raise ValueError("logical transfer plan source must be a placement")
    elif not isinstance(operation.source, StoredFragmentSnapshot):
        raise ValueError("logical transfer plan source has no placement")
    operation.validate_bounds()


@dataclass(frozen=True)
class PlacementExecutorPlan:
    placement_id: PlacementId
    participant_id: ParticipantId
    rank: ParallelRank
    placement_fragment_ids: tuple[PlacementFragmentId, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "placement_fragment_ids", tuple(self.placement_fragment_ids)
        )
        if (
            not self.placement_id
            or not self.participant_id
            or not self.placement_fragment_ids
        ):
            raise ValueError("placement executor identifiers must not be empty")
        if len(self.placement_fragment_ids) != len(set(self.placement_fragment_ids)):
            raise ValueError("placement executor has duplicate fragment IDs")


RuntimeExecutorProjectionKey: TypeAlias = tuple[
    RuntimeInstanceId,
    PlacementId,
    ParticipantId,
    str,
    Optional[LeaseId],
    str,
    ParallelRank,
    tuple[RuntimeFragmentId, ...],
]
PlacementExecutorProjectionKey: TypeAlias = tuple[
    PlacementId,
    ParticipantId,
    ParallelRank,
    tuple[PlacementFragmentId, ...],
]
ExecutorProjectionKey: TypeAlias = Union[
    RuntimeExecutorProjectionKey,
    PlacementExecutorProjectionKey,
]
FragmentProjectionKey: TypeAlias = tuple[str, str]


@dataclass(frozen=True)
class _OperationViews:
    source_indices: Mapping[ExecutorProjectionKey, tuple[int, ...]]
    target_indices: Mapping[ExecutorProjectionKey, tuple[int, ...]]
    pipeline_routes: tuple[PipelineRouteGroup, ...]

    def operation_indices_for(
        self,
        executor: Union[ExecutorTransferPlan, PlacementExecutorPlan],
        side: str,
    ) -> tuple[int, ...]:
        if side == "source":
            return self.source_indices.get(_executor_projection_key(executor), ())
        if side == "target":
            return self.target_indices.get(_executor_projection_key(executor), ())
        raise ValueError(f"invalid executor side: {side}")


@dataclass(frozen=True)
class LogicalTransferPlan:
    _operation_views: ClassVar[_OperationViews]

    resource_id: ResourceId
    revision: RevisionId
    source_placement: Optional[WeightPlacementManifest]
    target_placement: WeightPlacementManifest
    source_tensors: tuple[TensorDescriptor, ...]
    target_tensors: tuple[TensorDescriptor, ...]
    operations: tuple[LogicalTransferOperation, ...]
    source_manifest: Optional[StoredWeightManifest] = None
    planning_limits: PlanningLimits = field(default_factory=PlanningLimits)
    source_executors: tuple[PlacementExecutorPlan, ...] = ()
    target_executors: tuple[PlacementExecutorPlan, ...] = ()
    source_manifest_identity: Optional[StoredManifestIdentity] = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "source_tensors", tuple(self.source_tensors))
        object.__setattr__(self, "target_tensors", tuple(self.target_tensors))
        object.__setattr__(self, "operations", tuple(self.operations))
        object.__setattr__(self, "source_executors", tuple(self.source_executors))
        object.__setattr__(self, "target_executors", tuple(self.target_executors))
        if not self.resource_id or not self.revision:
            raise ValueError("logical transfer plan identifiers must not be empty")
        if not isinstance(self.target_placement, WeightPlacementManifest):
            raise ValueError("logical transfer plan target placement is invalid")
        if not isinstance(self.planning_limits, PlanningLimits):
            raise ValueError("logical transfer plan planning_limits is invalid")
        if len(self.operations) > self.planning_limits.max_transfer_regions:
            raise ValueError("logical transfer plan exceeds max_transfer_regions")
        total_lowered_segments = 0
        if self.source_placement is not None and not isinstance(
            self.source_placement, WeightPlacementManifest
        ):
            raise ValueError("logical transfer plan source placement is invalid")
        if self.source_manifest is not None and not isinstance(
            self.source_manifest, StoredWeightManifest
        ):
            raise ValueError("logical transfer plan source manifest is invalid")
        if (self.source_placement is None) == (self.source_manifest is None):
            raise ValueError(
                "logical transfer plan requires exactly one source provenance"
            )
        if self.source_manifest is not None:
            source_manifest = validate_weight_manifest_snapshot(self.source_manifest)
            object.__setattr__(self, "source_manifest", source_manifest)
            object.__setattr__(
                self,
                "source_manifest_identity",
                source_manifest.manifest_identity,
            )
        else:
            object.__setattr__(self, "source_manifest_identity", None)
        for side, placement in (
            ("source", self.source_placement),
            ("target", self.target_placement),
        ):
            if placement is not None and (
                placement.resource_id != self.resource_id
                or placement.revision != self.revision
            ):
                raise ValueError(
                    f"logical transfer plan {side} placement identity differs"
                )
        if self.source_placement is not None and (
            self.source_placement.weight_generation
            != self.target_placement.weight_generation
        ):
            raise ValueError(
                "logical transfer plan source and target weight_generation differs"
            )
        if self.source_manifest is not None and (
            self.source_manifest.resource_id != self.resource_id
            or self.source_manifest.revision != self.revision
            or self.source_manifest.weight_generation
            != self.target_placement.weight_generation
        ):
            raise ValueError("logical transfer plan source manifest identity differs")
        for operation in self.operations:
            _validate_logical_operation(operation, self.source_placement is not None)
            if operation.segment_count > self.planning_limits.max_segments_per_region:
                raise ValueError(
                    "logical transfer plan exceeds max_segments_per_region"
                )
            total_lowered_segments += operation.segment_count
            if total_lowered_segments > self.planning_limits.max_total_lowered_segments:
                raise ValueError(
                    "logical transfer plan exceeds max_total_lowered_segments"
                )
        if not all(
            isinstance(executor, PlacementExecutorPlan)
            for executor in (*self.source_executors, *self.target_executors)
        ):
            raise ValueError(
                "logical transfer plan has invalid canonical executor metadata"
            )
        self.validate_source_manifest_snapshot()
        object.__setattr__(
            self,
            "_operation_views",
            _build_operation_views(
                self.operations,
                self.source_executors,
                self.target_executors,
            ),
        )
        # Keep construction strict without making the data-contract module own
        # planner geometry validation.
        from .validation import _validate_logical_target_coverage

        _validate_logical_target_coverage(self)
        self.validate_source_placement_snapshot()

    @property
    def total_bytes(self) -> int:
        return sum(operation.total_bytes for operation in self.operations)

    @property
    def source_placement_id(self) -> Optional[PlacementId]:
        return (
            self.source_placement.placement_id
            if self.source_placement is not None
            else None
        )

    @property
    def target_placement_id(self) -> PlacementId:
        return self.target_placement.placement_id

    def validate_source_manifest_snapshot(self) -> None:
        """Fail closed if a stored source no longer matches its plan snapshot."""

        if self.source_manifest is None:
            if self.source_manifest_identity is not None:
                raise ValueError("logical plan has unexpected source manifest identity")
            return
        source_manifest = validate_weight_manifest_snapshot(self.source_manifest)
        if self.source_manifest_identity != source_manifest.manifest_identity:
            raise ValueError("logical plan source manifest identity differs")
        if self.source_tensors != _canonical_tensor_catalog(source_manifest.tensors):
            raise ValueError("logical plan source tensor catalog differs")
        source_by_id = {
            fragment.fragment_id: fragment for fragment in source_manifest.fragments
        }
        for operation in self.operations:
            source = operation.source
            if (
                not isinstance(source, StoredFragmentSnapshot)
                or source_by_id.get(source.fragment_id) != source
            ):
                raise ValueError(
                    "logical plan and source manifest fragment snapshots differ"
                )

    def validate_source_placement_snapshot(self) -> None:
        """Fail closed if a placement source no longer matches its plan snapshot."""

        if self.source_placement is None:
            return

        from .validation import _validate_logical_source_placement

        _validate_logical_source_placement(self)

    @property
    def pipeline_routes(self) -> tuple[PipelineRouteGroup, ...]:
        return self._operation_views.pipeline_routes

    def operation_indices_for_executor(
        self,
        executor: PlacementExecutorPlan,
        side: str,
    ) -> tuple[int, ...]:
        executors = self.source_executors if side == "source" else self.target_executors
        if side not in ("source", "target"):
            raise ValueError(f"invalid executor side: {side}")
        if executor not in executors:
            raise ValueError(f"{side} executor is not part of this logical plan")
        return self._operation_views.operation_indices_for(executor, side)

    def __getstate__(self) -> dict[str, object]:
        return {
            name: value
            for name, value in self.__dict__.items()
            if name != "_operation_views"
        }

    def __setstate__(self, state: dict[str, object]) -> None:
        for name, value in state.items():
            object.__setattr__(self, name, value)
        self.__post_init__()


def _canonical_tensor_catalog(
    tensors: Sequence[TensorDescriptor],
) -> tuple[TensorDescriptor, ...]:
    """Compare tensor catalogs by canonical identity, not exporter order."""

    return tuple(sorted(tensors, key=lambda item: item.tensor_id))


def _executor_projection_key(
    executor: Union[ExecutorTransferPlan, PlacementExecutorPlan],
) -> ExecutorProjectionKey:
    if isinstance(executor, PlacementExecutorPlan):
        return (
            executor.placement_id,
            executor.participant_id,
            executor.rank,
            executor.placement_fragment_ids,
        )
    return (
        executor.instance_id,
        executor.placement_id,
        executor.participant_id,
        executor.placement_digest,
        executor.runtime_lease_id,
        executor.worker_id,
        executor.rank,
        executor.fragment_ids,
    )


def _fragment_projection_key(
    fragment: GeometryFragment,
) -> Optional[FragmentProjectionKey]:
    if isinstance(fragment, BoundWeightFragment):
        return "runtime", fragment.fragment_id
    if isinstance(fragment, PlacementFragment):
        return "placement", fragment.placement_fragment_id
    return None


def _index_executors_by_fragment(
    executors: Sequence[Union[ExecutorTransferPlan, PlacementExecutorPlan]],
) -> tuple[
    dict[ExecutorProjectionKey, list[int]],
    dict[FragmentProjectionKey, list[ExecutorProjectionKey]],
]:
    indices_by_executor: dict[ExecutorProjectionKey, list[int]] = {}
    executors_by_fragment: dict[FragmentProjectionKey, list[ExecutorProjectionKey]] = {}
    for executor in executors:
        executor_key = _executor_projection_key(executor)
        if executor_key in indices_by_executor:
            raise ValueError("transfer plan has duplicate executor projection key")
        indices_by_executor[executor_key] = []
        fragment_ids: Sequence[str]
        fragment_kind: str
        if isinstance(executor, PlacementExecutorPlan):
            fragment_ids = executor.placement_fragment_ids
            fragment_kind = "placement"
        else:
            fragment_ids = executor.fragment_ids
            fragment_kind = "runtime"
        for fragment_id in fragment_ids:
            executors_by_fragment.setdefault((fragment_kind, fragment_id), []).append(
                executor_key
            )
    return indices_by_executor, executors_by_fragment


def _build_operation_views(
    operations: Sequence[Union[LogicalTransferOperation, ExecutableTransferOperation]],
    source_executors: Sequence[Union[ExecutorTransferPlan, PlacementExecutorPlan]],
    target_executors: Sequence[Union[ExecutorTransferPlan, PlacementExecutorPlan]],
) -> _OperationViews:
    source_indices, source_by_fragment = _index_executors_by_fragment(source_executors)
    target_indices, target_by_fragment = _index_executors_by_fragment(target_executors)
    for index, operation in enumerate(operations):
        source_key = _fragment_projection_key(operation.source)
        if source_key is not None:
            for executor_key in source_by_fragment.get(source_key, ()):
                source_indices[executor_key].append(index)
        target_key = _fragment_projection_key(operation.target)
        if target_key is not None:
            for executor_key in target_by_fragment.get(target_key, ()):
                target_indices[executor_key].append(index)
    return _OperationViews(
        source_indices=MappingProxyType(
            {key: tuple(indices) for key, indices in source_indices.items()}
        ),
        target_indices=MappingProxyType(
            {key: tuple(indices) for key, indices in target_indices.items()}
        ),
        pipeline_routes=_pipeline_routes(operations),
    )


def _pipeline_routes(
    operations: Sequence[Union[LogicalTransferOperation, ExecutableTransferOperation]],
) -> tuple[PipelineRouteGroup, ...]:
    indices_by_route: dict[
        tuple[Optional[int], Optional[int], int, Optional[int]],
        list[int],
    ] = {}
    for index, operation in enumerate(operations):
        source_pp = (
            operation.source.rank.pp
            if isinstance(operation.source, (BoundWeightFragment, PlacementFragment))
            else None
        )
        source_pipeline_stage_id = (
            operation.source.pipeline_stage_id
            if isinstance(operation.source, (BoundWeightFragment, PlacementFragment))
            else None
        )
        indices_by_route.setdefault(
            (
                source_pp,
                source_pipeline_stage_id,
                operation.target.rank.pp,
                operation.target.pipeline_stage_id,
            ),
            [],
        ).append(index)
    return tuple(
        PipelineRouteGroup(
            source_pp=source_pp,
            source_pipeline_stage_id=source_pipeline_stage_id,
            target_pp=target_pp,
            target_pipeline_stage_id=target_pipeline_stage_id,
            operation_indices=tuple(indices),
        )
        for (
            source_pp,
            source_pipeline_stage_id,
            target_pp,
            target_pipeline_stage_id,
        ), indices in sorted(
            indices_by_route.items(),
            key=lambda item: (
                -1 if item[0][0] is None else item[0][0],
                -1 if item[0][1] is None else item[0][1],
                item[0][2],
                -1 if item[0][3] is None else item[0][3],
            ),
        )
    )


__all__ = [
    "BoundWeightFragment",
    "LogicalTransferPlan",
    "PlanningLimits",
    "PipelineRouteGroup",
    "PlacementExecutorPlan",
    "RuntimeTensorOwner",
    "ExecutableTransferOperation",
    "ExecutableTransferRegion",
    "LiveTransferOperation",
    "LogicalTransferOperation",
    "LogicalTransferRegion",
    "StoredLoadOperation",
    "TransferRegion",
]
