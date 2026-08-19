"""Canonical ephemeral runtime binding contracts."""

from __future__ import annotations

from dataclasses import dataclass

from ..contracts import (
    LeaseId,
    ParticipantId,
    PlacementId,
    ResourceId,
    ResourceKind,
    RevisionId,
    RuntimeBindingFragment,
    RuntimeInstanceId,
)
from .types import (
    _require_nonempty_string,
    _require_u64,
    require_manifest_items,
    require_sha256_digest,
)


@dataclass(frozen=True)
class WeightRuntimeBindingManifest:
    """Ephemeral physical locations and lifetime fence for one placement."""

    resource_id: ResourceId
    placement_id: PlacementId
    placement_digest: str
    instance_id: RuntimeInstanceId
    generation: int
    lease_id: LeaseId
    revision: RevisionId
    participant_id: ParticipantId
    fragments: tuple[RuntimeBindingFragment, ...]

    @property
    def resource_kind(self) -> ResourceKind:
        """Identify this binding as model weight data."""

        return ResourceKind.MODEL_WEIGHT

    def __post_init__(self) -> None:
        for name in (
            "resource_id",
            "placement_id",
            "instance_id",
            "lease_id",
            "revision",
            "participant_id",
        ):
            _require_nonempty_string(getattr(self, name), name)
        require_sha256_digest(self.placement_digest, "placement_digest")
        _require_u64(self.generation, "generation")
        fragments = require_manifest_items(
            self.fragments,
            "WeightRuntimeBindingManifest fragments",
            RuntimeBindingFragment,
        )
        object.__setattr__(
            self,
            "fragments",
            tuple(
                sorted(
                    fragments,
                    key=lambda item: item.placement_fragment_id,
                )
            ),
        )
        placement_ids = [item.placement_fragment_id for item in self.fragments]
        if len(placement_ids) != len(set(placement_ids)):
            raise ValueError("duplicate placement fragment in runtime binding")
        fragment_ids = [item.fragment_id for item in self.fragments]
        if len(fragment_ids) != len(set(fragment_ids)):
            raise ValueError("duplicate runtime fragment_id in runtime binding")


__all__ = ["WeightRuntimeBindingManifest"]
