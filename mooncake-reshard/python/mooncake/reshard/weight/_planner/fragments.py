"""Nominal fragment types for each weight-transfer planning stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Union

from ..._typing import TypeAlias

from ...contracts import (
    LeaseId,
    PlacementFragmentId,
    RuntimeFragmentId,
    RuntimeInstanceId,
    TensorId,
)
from ..manifest import ParallelRank, PlacementFragment, RuntimeBindingFragment
from ..storage_manifest import StoredFragment
from .attestation import RuntimeBindingAttestation


_MAX_U64 = (1 << 64) - 1


@dataclass(frozen=True)
class BoundWeightFragment:
    """A placement fragment bound to a live runtime allocation and lease."""

    placement: PlacementFragment
    binding: RuntimeBindingFragment
    instance_id: RuntimeInstanceId
    runtime_lease_id: LeaseId
    lease_generation: int
    owner: Optional[object] = field(default=None, compare=False, repr=False)
    attestation: Optional[RuntimeBindingAttestation] = field(
        default=None,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.placement, PlacementFragment):
            raise ValueError("bound fragment placement is invalid")
        if not isinstance(self.binding, RuntimeBindingFragment):
            raise ValueError("bound fragment runtime binding is invalid")
        if self.placement.placement_fragment_id != self.binding.placement_fragment_id:
            raise ValueError("bound fragment placement identity differs")
        if self.placement.nbytes != self.binding.nbytes:
            raise ValueError("bound fragment byte size differs")
        if type(self.instance_id) is not str or not self.instance_id:
            raise ValueError("bound fragment instance_id must be non-empty")
        if type(self.runtime_lease_id) is not str or not self.runtime_lease_id:
            raise ValueError("bound fragment runtime_lease_id must be non-empty")
        if (
            type(self.lease_generation) is not int
            or self.lease_generation < 0
            or self.lease_generation > _MAX_U64
        ):
            raise ValueError(
                "bound fragment lease_generation must fit in an unsigned 64-bit integer"
            )
        if self.owner is not self.binding.owner:
            raise ValueError("bound fragment owner differs from runtime binding")
        if self.attestation is not None:
            if not isinstance(self.attestation, RuntimeBindingAttestation):
                raise ValueError("bound fragment runtime attestation is invalid")
            if not self.attestation.validates(self.placement, self.binding):
                raise ValueError(
                    "bound fragment does not match its runtime binding attestation"
                )
            if (
                self.instance_id != self.attestation.binding.instance_id
                or self.runtime_lease_id != self.attestation.binding.lease_id
                or self.lease_generation != self.attestation.binding.generation
            ):
                raise ValueError(
                    "bound fragment runtime fence differs from attestation"
                )

    def __reduce__(self):
        """Serialize attested runtime evidence without a local allocation owner."""

        return (
            type(self),
            (
                self.placement,
                self.binding,
                self.instance_id,
                self.runtime_lease_id,
                self.lease_generation,
                None,
                self.attestation,
            ),
        )

    @property
    def placement_fragment_id(self) -> PlacementFragmentId:
        return self.placement.placement_fragment_id

    @property
    def fragment_id(self) -> RuntimeFragmentId:
        return self.binding.fragment_id

    @property
    def tensor_id(self) -> TensorId:
        return self.placement.tensor_id

    @property
    def global_offset(self) -> tuple[int, ...]:
        return self.placement.global_offset

    @property
    def local_shape(self) -> tuple[int, ...]:
        return self.placement.local_shape

    @property
    def nbytes(self) -> int:
        return self.binding.nbytes

    @property
    def rank(self) -> ParallelRank:
        return self.placement.rank

    @property
    def pipeline_stage_id(self) -> Optional[int]:
        return self.placement.pipeline_stage_id

    @property
    def aliases(self) -> tuple[TensorId, ...]:
        return self.placement.aliases

    @property
    def address(self) -> int:
        return self.binding.address

    @property
    def worker_id(self) -> str:
        return self.binding.worker_id

    @property
    def endpoint(self) -> str:
        return self.binding.endpoint

    @property
    def device(self) -> str:
        return self.binding.device

    @property
    def storage_address(self) -> int:
        return self.binding.storage_address

    @property
    def storage_nbytes(self) -> int:
        return self.binding.storage_nbytes

    @property
    def storage_offset_bytes(self) -> int:
        return self.binding.storage_offset_bytes


# These aliases are intentionally stage-specific. A placement is immutable and
# address-free; a bound fragment carries runtime lease/address state; a stored
# fragment is a Store object range. Executors may only see the last two kinds.
LogicalSourceFragment: TypeAlias = Union[PlacementFragment, StoredFragment]
LogicalTargetFragment: TypeAlias = PlacementFragment
ExecutableSourceFragment: TypeAlias = Union[BoundWeightFragment, StoredFragment]
ExecutableTargetFragment: TypeAlias = BoundWeightFragment
LiveSourceFragment: TypeAlias = BoundWeightFragment
LiveTargetFragment: TypeAlias = BoundWeightFragment
StoredLoadSourceFragment: TypeAlias = StoredFragment
StoredLoadTargetFragment: TypeAlias = BoundWeightFragment
GeometryFragment: TypeAlias = Union[
    PlacementFragment,
    StoredFragment,
    BoundWeightFragment,
]


__all__ = [
    "BoundWeightFragment",
    "ExecutableSourceFragment",
    "ExecutableTargetFragment",
    "GeometryFragment",
    "LiveSourceFragment",
    "LiveTargetFragment",
    "LogicalSourceFragment",
    "LogicalTargetFragment",
    "RuntimeBindingAttestation",
    "StoredLoadSourceFragment",
    "StoredLoadTargetFragment",
]
