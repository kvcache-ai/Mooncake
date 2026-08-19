"""Validated runtime-binding provenance for executable reshard plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from ...contracts import PlacementFragmentId
from ..binding import validate_runtime_binding
from ..manifest import (
    PlacementFragment,
    RuntimeBindingFragment,
    WeightPlacementManifest,
    WeightRuntimeBindingManifest,
)


@dataclass(frozen=True)
class RuntimeBindingAttestation:
    """One validated binding of a placement participant to live storage."""

    placement: WeightPlacementManifest
    binding: WeightRuntimeBindingManifest
    _placement_by_id: Mapping[PlacementFragmentId, PlacementFragment] = field(
        init=False,
        compare=False,
        repr=False,
    )
    _binding_by_id: Mapping[PlacementFragmentId, RuntimeBindingFragment] = field(
        init=False,
        compare=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.placement, WeightPlacementManifest):
            raise ValueError("runtime attestation placement is invalid")
        if not isinstance(self.binding, WeightRuntimeBindingManifest):
            raise ValueError("runtime attestation binding is invalid")
        validate_runtime_binding(self.placement, self.binding)
        placement_part = next(
            part
            for part in self.placement.parts
            if part.participant_id == self.binding.participant_id
        )
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
                {item.placement_fragment_id: item for item in self.binding.fragments}
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
    ) -> tuple[WeightPlacementManifest, WeightRuntimeBindingManifest]:
        """Serialize only canonical inputs; indexes are derived state."""

        return self.placement, self.binding

    def __setstate__(
        self,
        state: tuple[WeightPlacementManifest, WeightRuntimeBindingManifest],
    ) -> None:
        """Revalidate canonical inputs while rebuilding derived indexes."""

        placement, binding = state
        object.__setattr__(self, "placement", placement)
        object.__setattr__(self, "binding", binding)
        self.__post_init__()

    def worker_fragment_pairs(
        self,
        worker_id: str,
    ) -> tuple[tuple[PlacementFragment, RuntimeBindingFragment], ...]:
        """Return the canonical placement/runtime pairs for one worker."""

        return tuple(
            (self._placement_by_id[item.placement_fragment_id], item)
            for item in self.binding.fragments
            if item.worker_id == worker_id
        )
