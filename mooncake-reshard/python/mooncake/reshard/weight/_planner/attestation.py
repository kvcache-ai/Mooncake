"""Validated runtime-binding provenance for executable reshard plans."""

from __future__ import annotations

from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Mapping

from ...contracts import (
    LeaseId,
    ParticipantId,
    PlacementFragmentId,
    PlacementId,
    ResourceId,
    RevisionId,
    RuntimeInstanceId,
)
from ..binding import validate_runtime_binding
from ..manifest import (
    PlacementFragment,
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)


@dataclass(frozen=True)
class RuntimeBindingEvidence:
    """Allocation-owner-free physical evidence for one runtime binding."""

    resource_id: ResourceId
    revision: RevisionId
    placement_id: PlacementId
    placement_digest: str
    instance_id: RuntimeInstanceId
    participant_id: ParticipantId
    generation: int
    lease_id: LeaseId
    fragments: tuple[RuntimeBindingFragment, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "fragments", tuple(self.fragments))
        if any(fragment.owner is not None for fragment in self.fragments):
            raise ValueError(
                "runtime binding evidence must not retain allocation owners"
            )
        binding = self.to_binding()
        object.__setattr__(self, "fragments", binding.fragments)

    @classmethod
    def from_binding(
        cls,
        binding: WeightRuntimeBindingManifest,
    ) -> RuntimeBindingEvidence:
        return cls(
            resource_id=binding.resource_id,
            revision=binding.revision,
            placement_id=binding.placement_id,
            placement_digest=binding.placement_digest,
            instance_id=binding.instance_id,
            participant_id=binding.participant_id,
            generation=binding.generation,
            lease_id=binding.lease_id,
            fragments=tuple(
                replace(fragment, owner=None) for fragment in binding.fragments
            ),
        )

    def to_binding(self) -> WeightRuntimeBindingManifest:
        return WeightRuntimeBindingManifest(
            resource_id=self.resource_id,
            revision=self.revision,
            placement_id=self.placement_id,
            placement_digest=self.placement_digest,
            instance_id=self.instance_id,
            participant_id=self.participant_id,
            generation=self.generation,
            lease_id=self.lease_id,
            fragments=self.fragments,
        )


@dataclass(frozen=True, init=False)
class RuntimeBindingAttestation:
    """One validated, allocation-owner-free binding evidence snapshot."""

    placement: WeightPlacementManifest
    evidence: RuntimeBindingEvidence
    _placement_by_id: Mapping[PlacementFragmentId, PlacementFragment]
    _binding_by_id: Mapping[PlacementFragmentId, RuntimeBindingFragment]

    def __init__(
        self,
        placement: WeightPlacementManifest,
        binding: WeightRuntimeBindingManifest,
    ) -> None:
        if not isinstance(binding, WeightRuntimeBindingManifest):
            raise ValueError("runtime attestation binding is invalid")
        self._set_validated_state(
            placement,
            RuntimeBindingEvidence.from_binding(binding),
        )

    def _set_validated_state(
        self,
        placement: WeightPlacementManifest,
        evidence: RuntimeBindingEvidence,
    ) -> None:
        if not isinstance(placement, WeightPlacementManifest):
            raise ValueError("runtime attestation placement is invalid")
        if not isinstance(evidence, RuntimeBindingEvidence):
            raise ValueError("runtime attestation evidence is invalid")
        if any(fragment.owner is not None for fragment in evidence.fragments):
            raise ValueError(
                "runtime binding evidence must not retain allocation owners"
            )
        binding = evidence.to_binding()
        validate_runtime_binding(placement, binding)
        placement_part = next(
            part
            for part in placement.parts
            if part.participant_id == evidence.participant_id
        )
        object.__setattr__(self, "placement", placement)
        object.__setattr__(self, "evidence", evidence)
        object.__setattr__(
            self,
            "_placement_by_id",
            MappingProxyType(
                {item.placement_fragment_id: item for item in placement_part.fragments}
            ),
        )
        object.__setattr__(
            self,
            "_binding_by_id",
            MappingProxyType(
                {item.placement_fragment_id: item for item in evidence.fragments}
            ),
        )

    def validates(
        self,
        placement: PlacementFragment,
        binding: RuntimeBindingFragment,
    ) -> bool:
        """Return whether one bound fragment belongs to this exact attestation."""

        placement_id = placement.placement_fragment_id
        return (
            self._placement_by_id.get(placement_id) == placement
            and self._binding_by_id.get(placement_id) == binding
        )

    def __getstate__(
        self,
    ) -> tuple[WeightPlacementManifest, RuntimeBindingEvidence]:
        """Serialize only canonical inputs; indexes are derived state."""

        return self.placement, self.evidence

    def __setstate__(
        self,
        state: tuple[WeightPlacementManifest, RuntimeBindingEvidence],
    ) -> None:
        """Revalidate canonical inputs while rebuilding derived indexes."""

        placement, evidence = state
        self._set_validated_state(placement, evidence)

    def worker_fragment_pairs(
        self,
        worker_id: str,
    ) -> tuple[tuple[PlacementFragment, RuntimeBindingFragment], ...]:
        """Return the canonical placement/runtime pairs for one worker."""

        return tuple(
            (self._placement_by_id[item.placement_fragment_id], item)
            for item in self.evidence.fragments
            if item.worker_id == worker_id
        )

    def binding_fragment(
        self,
        placement_fragment_id: PlacementFragmentId,
    ) -> RuntimeBindingFragment:
        """Return the canonical owner-free evidence for one placement fragment."""

        return self._binding_by_id[placement_fragment_id]
