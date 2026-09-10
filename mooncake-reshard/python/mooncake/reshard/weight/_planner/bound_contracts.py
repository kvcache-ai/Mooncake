"""Runtime-bound transfer plan contracts checked against live bindings."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Optional, cast

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
    StoredFragmentSnapshotId,
    TensorId,
)
from ..manifest import (
    ParallelRank,
    PlacementFragment,
    RuntimeBindingFragment,
    WeightPlacementManifest,
)
from ..storage_manifest import (
    StoredFragmentSnapshot,
    StoredManifestIdentity,
    StoredWeightManifest,
    validate_weight_manifest_snapshot,
)
from . import contracts as _contracts
from .attestation import RuntimeBindingAttestation
from .contracts import (
    ExecutableTransferOperation,
    PlanningLimits,
    PipelineRouteGroup,
    _is_canonical_operation,
    _OperationViews,
)
from .fragments import BoundWeightFragment


@dataclass(frozen=True)
class RuntimeFragmentSnapshot:
    placement_fragment_id: PlacementFragmentId
    fragment_id: RuntimeFragmentId
    tensor_id: TensorId
    global_offset: tuple[int, ...]
    local_shape: tuple[int, ...]
    address: int
    nbytes: int
    worker_id: str
    endpoint: str
    device: str
    itemsize: int
    strides_bytes: tuple[int, ...]
    storage_address: int
    storage_nbytes: int
    storage_offset_bytes: int
    lease_generation: int

    @classmethod
    def from_fragment(cls, fragment: BoundWeightFragment) -> RuntimeFragmentSnapshot:
        return cls(
            placement_fragment_id=fragment.placement_fragment_id,
            fragment_id=fragment.fragment_id,
            tensor_id=fragment.tensor_id,
            global_offset=fragment.global_offset,
            local_shape=fragment.local_shape,
            address=fragment.address,
            nbytes=fragment.nbytes,
            worker_id=fragment.worker_id,
            endpoint=fragment.endpoint,
            device=fragment.device,
            itemsize=fragment.binding.itemsize,
            strides_bytes=fragment.binding.strides_bytes,
            storage_address=fragment.storage_address,
            storage_nbytes=fragment.storage_nbytes,
            storage_offset_bytes=fragment.storage_offset_bytes,
            lease_generation=fragment.lease_generation,
        )

    @classmethod
    def from_attested_pair(
        cls,
        placement_fragment: PlacementFragment,
        runtime_fragment: RuntimeBindingFragment,
        *,
        lease_generation: int,
    ) -> RuntimeFragmentSnapshot:
        return cls(
            placement_fragment_id=placement_fragment.placement_fragment_id,
            fragment_id=runtime_fragment.fragment_id,
            tensor_id=placement_fragment.tensor_id,
            global_offset=placement_fragment.global_offset,
            local_shape=placement_fragment.local_shape,
            address=runtime_fragment.address,
            nbytes=runtime_fragment.nbytes,
            worker_id=runtime_fragment.worker_id,
            endpoint=runtime_fragment.endpoint,
            device=runtime_fragment.device,
            itemsize=runtime_fragment.itemsize,
            strides_bytes=runtime_fragment.strides_bytes,
            storage_address=runtime_fragment.storage_address,
            storage_nbytes=runtime_fragment.storage_nbytes,
            storage_offset_bytes=runtime_fragment.storage_offset_bytes,
            lease_generation=lease_generation,
        )


@dataclass(frozen=True)
class ExecutorTransferPlan:
    instance_id: RuntimeInstanceId
    placement_id: PlacementId
    participant_id: ParticipantId
    placement_digest: str
    runtime_lease_id: Optional[LeaseId]
    worker_id: str
    rank: ParallelRank
    fragment_ids: tuple[RuntimeFragmentId, ...]
    fragment_snapshots: tuple[RuntimeFragmentSnapshot, ...]
    attestation: Optional[RuntimeBindingAttestation] = field(
        default=None,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "fragment_ids", tuple(self.fragment_ids))
        object.__setattr__(self, "fragment_snapshots", tuple(self.fragment_snapshots))
        if (
            not self.instance_id
            or not self.placement_id
            or not self.participant_id
            or not self.worker_id
            or not self.fragment_ids
        ):
            raise ValueError("executor plan identifiers must not be empty")
        if len(self.placement_digest) != 64 or any(
            character not in "0123456789abcdef" for character in self.placement_digest
        ):
            raise ValueError("executor plan placement digest must be SHA-256")
        if self.runtime_lease_id is not None and (
            type(self.runtime_lease_id) is not str or not self.runtime_lease_id
        ):
            raise ValueError("executor plan runtime lease ID must be non-empty")
        if len(self.fragment_ids) != len(set(self.fragment_ids)):
            raise ValueError("executor plan has duplicate fragment IDs")
        if not all(
            isinstance(snapshot, RuntimeFragmentSnapshot)
            for snapshot in self.fragment_snapshots
        ):
            raise ValueError("executor plan has invalid runtime fragment snapshots")
        if (
            tuple(snapshot.fragment_id for snapshot in self.fragment_snapshots)
            != self.fragment_ids
        ):
            raise ValueError("executor plan fragment snapshot IDs do not match")
        if self.attestation is not None and not isinstance(
            self.attestation, RuntimeBindingAttestation
        ):
            raise ValueError("executor plan runtime attestation is invalid")


ExecutorKey: TypeAlias = tuple[
    RuntimeInstanceId,
    PlacementId,
    ParticipantId,
    str,
    LeaseId,
    str,
    ParallelRank,
]


def _validate_executable_operation(value: object) -> None:
    if not _is_canonical_operation(value):
        raise ValueError("transfer plan requires a canonical transfer operation")
    operation = cast(ExecutableTransferOperation, value)
    if not isinstance(operation.target, BoundWeightFragment):
        raise ValueError("executable transfer plan target must be runtime-bound")
    if not isinstance(operation.source, (BoundWeightFragment, StoredFragmentSnapshot)):
        raise ValueError("executable transfer plan source must be runtime-bound")
    operation.validate_bounds()


def _validate_source_target_allocations_do_not_overlap(
    operations: tuple[ExecutableTransferOperation, ...],
) -> None:
    source_allocations: dict[
        tuple[RuntimeInstanceId, str, str], set[tuple[int, int]]
    ] = {}
    target_allocations: dict[
        tuple[RuntimeInstanceId, str, str], set[tuple[int, int]]
    ] = {}
    for operation in operations:
        if isinstance(operation.source, BoundWeightFragment):
            source_allocations.setdefault(
                (
                    operation.source.instance_id,
                    operation.source.worker_id,
                    operation.source.device,
                ),
                set(),
            ).add(
                (
                    operation.source.binding.storage_address,
                    operation.source.binding.storage_nbytes,
                )
            )
        if isinstance(operation.target, BoundWeightFragment):
            target_allocations.setdefault(
                (
                    operation.target.instance_id,
                    operation.target.worker_id,
                    operation.target.device,
                ),
                set(),
            ).add(
                (
                    operation.target.binding.storage_address,
                    operation.target.binding.storage_nbytes,
                )
            )

    for address_space in sorted(set(source_allocations) & set(target_allocations)):
        sources = sorted(source_allocations[address_space])
        targets = sorted(target_allocations[address_space])
        source_index = 0
        target_index = 0
        while source_index < len(sources) and target_index < len(targets):
            source_start, source_size = sources[source_index]
            target_start, target_size = targets[target_index]
            source_end = source_start + source_size
            target_end = target_start + target_size
            if source_start < target_end and target_start < source_end:
                raise ValueError(
                    "source and target runtime storage allocations overlap in "
                    f"address space {address_space}; in-place reshard is unsupported"
                )
            if source_end <= target_end:
                source_index += 1
            else:
                target_index += 1


def _validate_execution_provenance(
    *,
    resource_id: ResourceId,
    revision: RevisionId,
    weight_generation: int,
    operations: tuple[ExecutableTransferOperation, ...],
) -> None:
    """Require live execution fragments to come from verified bindings."""

    for operation in operations:
        fragments: tuple[tuple[str, BoundWeightFragment], ...] = (
            ("target", operation.target),
        )
        if isinstance(operation.source, BoundWeightFragment):
            fragments = (("source", operation.source), *fragments)
        for side, fragment in fragments:
            if fragment.owner is not None or fragment.binding.owner is not None:
                raise ValueError(
                    f"transfer plan {side} fragment retains runtime allocation owner"
                )
            attestation = fragment.attestation
            if not isinstance(attestation, RuntimeBindingAttestation):
                raise ValueError(
                    f"transfer plan {side} fragment lacks an attested runtime binding"
                )
            placement = attestation.placement
            if (
                placement.resource_id != resource_id
                or placement.revision != revision
                or placement.weight_generation != weight_generation
            ):
                raise ValueError(
                    f"transfer plan identity differs from {side} placement"
                )
            if not attestation.validates(fragment.placement, fragment.binding):
                raise ValueError(
                    f"transfer plan {side} fragment differs from attested runtime binding"
                )


def _validate_executor_provenance(
    *,
    resource_id: ResourceId,
    revision: RevisionId,
    weight_generation: int,
    operations: tuple[ExecutableTransferOperation, ...],
    executors: tuple[ExecutorTransferPlan, ...],
    operation_views: _OperationViews,
    side: str,
) -> None:
    """Validate live executor routing against attested operation fragments."""

    fragments = tuple(
        operation.source if side == "source" else operation.target
        for operation in operations
    )
    if not fragments:
        return
    has_stored_source = any(
        isinstance(fragment, StoredFragmentSnapshot) for fragment in fragments
    )
    if has_stored_source:
        if side != "source" or not all(
            isinstance(fragment, StoredFragmentSnapshot) for fragment in fragments
        ):
            raise ValueError("transfer plan mixes stored and live source fragments")
        if executors:
            raise ValueError("stored source must not have live executor provenance")
        return
    live_fragments = tuple(
        fragment for fragment in fragments if isinstance(fragment, BoundWeightFragment)
    )
    if len(live_fragments) != len(fragments):
        raise ValueError(f"transfer plan {side} fragment is not runtime-bound")
    if not executors:
        raise ValueError(f"live {side} requires executor provenance")

    expected_indices: dict[ExecutorKey, list[int]] = {}
    for index, fragment in enumerate(live_fragments):
        attestation = fragment.attestation
        if not isinstance(attestation, RuntimeBindingAttestation):
            raise ValueError(f"transfer plan {side} fragment lacks runtime attestation")
        binding = attestation.evidence
        placement = attestation.placement
        key: ExecutorKey = (
            binding.instance_id,
            placement.placement_id,
            binding.participant_id,
            placement.digest,
            binding.lease_id,
            fragment.worker_id,
            fragment.rank,
        )
        expected_indices.setdefault(key, []).append(index)

    active_placement_ids = {
        fragment.attestation.placement.placement_id
        for fragment in live_fragments
        if isinstance(fragment.attestation, RuntimeBindingAttestation)
    }
    actual_indices: dict[ExecutorKey, list[int]] = {}
    executor_snapshots_by_fragment_id: dict[
        RuntimeFragmentId, RuntimeFragmentSnapshot
    ] = {}
    for executor in executors:
        if executor.runtime_lease_id is None:
            raise ValueError(f"{side} executor is missing runtime lease provenance")
        key: ExecutorKey = (
            executor.instance_id,
            executor.placement_id,
            executor.participant_id,
            executor.placement_digest,
            executor.runtime_lease_id,
            executor.worker_id,
            executor.rank,
        )
        if key in actual_indices:
            raise ValueError(f"transfer plan has duplicate {side} executor provenance")
        attestation = executor.attestation
        if not isinstance(attestation, RuntimeBindingAttestation):
            raise ValueError(f"{side} executor lacks runtime attestation")
        placement = attestation.placement
        binding = attestation.evidence
        if (
            placement.resource_id != resource_id
            or placement.revision != revision
            or placement.weight_generation != weight_generation
            or placement.placement_id not in active_placement_ids
        ):
            raise ValueError(f"{side} executor attestation identity differs")
        expected_key: ExecutorKey = (
            binding.instance_id,
            placement.placement_id,
            binding.participant_id,
            placement.digest,
            binding.lease_id,
            executor.worker_id,
            executor.rank,
        )
        if key != expected_key:
            raise ValueError(f"{side} executor provenance differs from attestation")
        actual_indices[key] = list(
            operation_views.operation_indices_for(executor, side)
        )
        expected_fragment_snapshots = tuple(
            sorted(
                (
                    RuntimeFragmentSnapshot.from_attested_pair(
                        placement_fragment,
                        runtime,
                        lease_generation=binding.generation,
                    )
                    for placement_fragment, runtime in attestation.worker_fragment_pairs(
                        executor.worker_id
                    )
                    if runtime.fragment_id in executor.fragment_ids
                ),
                key=lambda item: item.fragment_id,
            )
        )
        if executor.fragment_snapshots != expected_fragment_snapshots:
            raise ValueError(f"{side} executor fragment provenance differs")
        for snapshot in executor.fragment_snapshots:
            if snapshot.fragment_id in executor_snapshots_by_fragment_id:
                raise ValueError(f"{side} executor has duplicate fragment provenance")
            executor_snapshots_by_fragment_id[snapshot.fragment_id] = snapshot

    for key, actual in actual_indices.items():
        expected = expected_indices.get(key)
        if expected is None:
            if actual:
                raise ValueError(f"{side} executor provenance differs from operations")
            continue
        if sorted(actual) != sorted(expected):
            raise ValueError(f"{side} executor provenance differs from operations")
    if set(expected_indices) - set(actual_indices):
        raise ValueError(f"{side} executor provenance differs from operations")
    operation_fragment_ids = {fragment.fragment_id for fragment in live_fragments}
    if set(executor_snapshots_by_fragment_id) != operation_fragment_ids:
        raise ValueError(f"{side} executor fragment provenance differs from operations")
    for fragment in live_fragments:
        if executor_snapshots_by_fragment_id.get(fragment.fragment_id) != (
            RuntimeFragmentSnapshot.from_fragment(fragment)
        ):
            raise ValueError(f"{side} executor fragment provenance differs")


def _validated_stored_source_fragment_snapshots(
    operations: tuple[ExecutableTransferOperation, ...],
    source_manifest: StoredWeightManifest,
) -> tuple[StoredFragmentSnapshot, ...]:
    """Derive selected Store ranges from the committed manifest snapshot."""

    snapshots_by_id: dict[StoredFragmentSnapshotId, StoredFragmentSnapshot] = {}
    for fragment in source_manifest.fragments:
        snapshots_by_id[fragment.fragment_id] = fragment

    operation_ids: set[StoredFragmentSnapshotId] = set()
    for operation in operations:
        source = operation.source
        if not isinstance(source, StoredFragmentSnapshot):
            raise ValueError("transfer plan mixes stored and live source fragments")
        if snapshots_by_id.get(source.fragment_id) != source:
            raise ValueError(
                "transfer plan stored source fragment differs from committed manifest"
            )
        operation_ids.add(source.fragment_id)
    return tuple(snapshots_by_id[fragment_id] for fragment_id in sorted(operation_ids))


@dataclass(frozen=True)
class TransferPlan:
    _operation_views: ClassVar[_OperationViews]

    resource_id: ResourceId
    revision: RevisionId
    weight_generation: int
    target_placement: WeightPlacementManifest
    operations: tuple[ExecutableTransferOperation, ...]
    planning_limits: PlanningLimits = field(default_factory=PlanningLimits)
    source_executors: tuple[ExecutorTransferPlan, ...] = ()
    target_executors: tuple[ExecutorTransferPlan, ...] = ()
    source_manifest: Optional[StoredWeightManifest] = None
    source_manifest_identity: Optional[StoredManifestIdentity] = None
    source_stored_fragment_snapshots: tuple[StoredFragmentSnapshot, ...] = field(
        init=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "operations", tuple(self.operations))
        object.__setattr__(self, "source_executors", tuple(self.source_executors))
        object.__setattr__(self, "target_executors", tuple(self.target_executors))
        if not self.resource_id or not self.revision:
            raise ValueError("transfer plan identifiers must not be empty")
        if type(self.weight_generation) is not int or self.weight_generation < 0:
            raise ValueError("transfer plan weight_generation must be non-negative")
        if not isinstance(self.planning_limits, PlanningLimits):
            raise ValueError("transfer plan planning_limits is invalid")
        if self.source_manifest_identity is not None and not isinstance(
            self.source_manifest_identity,
            StoredManifestIdentity,
        ):
            raise ValueError("transfer plan source manifest identity is invalid")
        if self.source_manifest is not None and not isinstance(
            self.source_manifest,
            StoredWeightManifest,
        ):
            raise ValueError("transfer plan source manifest is invalid")
        if len(self.operations) > self.planning_limits.max_transfer_regions:
            raise ValueError("transfer plan exceeds max_transfer_regions")
        total_lowered_segments = 0
        for operation in self.operations:
            _validate_executable_operation(operation)
            if operation.segment_count > self.planning_limits.max_segments_per_region:
                raise ValueError("transfer plan exceeds max_segments_per_region")
            total_lowered_segments += operation.segment_count
            if total_lowered_segments > self.planning_limits.max_total_lowered_segments:
                raise ValueError("transfer plan exceeds max_total_lowered_segments")
        has_stored_source = any(
            isinstance(operation.source, StoredFragmentSnapshot)
            for operation in self.operations
        )
        if has_stored_source:
            if self.source_manifest is None:
                raise ValueError("stored transfer plan requires a source manifest")
            source_manifest = validate_weight_manifest_snapshot(self.source_manifest)
            object.__setattr__(self, "source_manifest", source_manifest)
            if self.source_manifest_identity is None:
                raise ValueError(
                    "stored transfer plan requires a source manifest identity"
                )
            if (
                self.source_manifest_identity.resource_id != self.resource_id
                or self.source_manifest_identity.revision != self.revision
                or self.source_manifest_identity.weight_generation
                != self.weight_generation
            ):
                raise ValueError("transfer plan source manifest identity differs")
            if source_manifest.manifest_identity != self.source_manifest_identity:
                raise ValueError("transfer plan source manifest commitment differs")
            object.__setattr__(
                self,
                "source_stored_fragment_snapshots",
                _validated_stored_source_fragment_snapshots(
                    self.operations,
                    source_manifest,
                ),
            )
        elif self.source_manifest is not None:
            raise ValueError("runtime transfer plan must not have a source manifest")
        elif self.source_manifest_identity is not None:
            raise ValueError(
                "runtime transfer plan must not have a source manifest identity"
            )
        else:
            object.__setattr__(self, "source_stored_fragment_snapshots", ())
        if not all(
            isinstance(executor, ExecutorTransferPlan)
            for executor in (*self.source_executors, *self.target_executors)
        ):
            raise ValueError("transfer plan has invalid canonical executor metadata")
        # Resolve the shared view builder through the logical contracts module
        # so its canonical definition remains the single patch point for both
        # plan types.
        object.__setattr__(
            self,
            "_operation_views",
            _contracts._build_operation_views(
                self.operations,
                self.source_executors,
                self.target_executors,
            ),
        )
        _validate_execution_provenance(
            resource_id=self.resource_id,
            revision=self.revision,
            weight_generation=self.weight_generation,
            operations=self.operations,
        )
        if not isinstance(self.target_placement, WeightPlacementManifest):
            raise ValueError("transfer plan target placement is invalid")
        if (
            self.target_placement.resource_id != self.resource_id
            or self.target_placement.revision != self.revision
            or self.target_placement.weight_generation != self.weight_generation
        ):
            raise ValueError("transfer plan target placement identity differs")
        _validate_executor_provenance(
            resource_id=self.resource_id,
            revision=self.revision,
            weight_generation=self.weight_generation,
            operations=self.operations,
            executors=self.source_executors,
            operation_views=self._operation_views,
            side="source",
        )
        _validate_executor_provenance(
            resource_id=self.resource_id,
            revision=self.revision,
            weight_generation=self.weight_generation,
            operations=self.operations,
            executors=self.target_executors,
            operation_views=self._operation_views,
            side="target",
        )
        _validate_source_target_allocations_do_not_overlap(self.operations)
        # Re-check the complete target placement at the public executable-plan
        # boundary. Binding output can be serialized or reconstructed, so the
        # executor snapshot alone cannot be trusted as a coverage proof.
        from .bound_validation import (
            _validate_bound_target_coverage,
            _validate_target_physical_ranges,
        )

        _validate_target_physical_ranges(
            self.operations,
            max_segment_checks=self.planning_limits.max_total_lowered_segments,
        )
        _validate_bound_target_coverage(self)

    @property
    def total_bytes(self) -> int:
        return sum(operation.total_bytes for operation in self.operations)

    @property
    def regions(self) -> tuple[ExecutableTransferOperation, ...]:
        return self.operations

    @property
    def pipeline_routes(self) -> tuple[PipelineRouteGroup, ...]:
        return self._operation_views.pipeline_routes

    def operation_indices_for_executor(
        self,
        executor: ExecutorTransferPlan,
        side: str,
    ) -> tuple[int, ...]:
        executors = self.source_executors if side == "source" else self.target_executors
        if side not in ("source", "target"):
            raise ValueError(f"invalid executor side: {side}")
        if executor not in executors:
            raise ValueError(f"{side} executor is not part of this transfer plan")
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


__all__ = [
    "ExecutorTransferPlan",
    "RuntimeFragmentSnapshot",
    "TransferPlan",
]
