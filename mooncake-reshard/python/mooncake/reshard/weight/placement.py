"""Complete logical weight placement and canonical identity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from typing import Optional

from ..contracts import (
    ParticipantId,
    PlacementId,
    PlacementSetId,
    ResourceId,
    ResourceKind,
    RevisionId,
    TensorId,
)
from .part import WeightPlacementPart
from .topology import ParallelTopology
from .types import (
    OwnershipAxis,
    PlacementFragment,
    ReplicatedAxis,
    SplitAxis,
    TensorDescriptor,
    _require_nonempty_string,
    _require_u64,
    require_manifest_items,
)
from .validation import (
    _validate_complete_weight_placement,
    _validate_fragments,
)


@dataclass(frozen=True, init=False)
class WeightPlacementManifest:
    """One complete address-free placement of a model-weight generation."""

    resource_id: ResourceId
    placement_id: PlacementId
    revision: RevisionId
    weight_generation: int
    placement_set_id: PlacementSetId
    topology: ParallelTopology
    parts: tuple[WeightPlacementPart, ...]
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[PlacementFragment, ...]
    _digest_cache: Optional[str] = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        resource_id: ResourceId,
        revision: RevisionId,
        weight_generation: int,
        placement_set_id: PlacementSetId,
        topology: ParallelTopology,
        parts: tuple[WeightPlacementPart, ...],
        placement_id: Optional[PlacementId] = None,
    ) -> None:
        _require_nonempty_string(resource_id, "resource_id")
        _require_nonempty_string(revision, "revision")
        _require_u64(weight_generation, "weight_generation")
        _require_nonempty_string(placement_set_id, "placement_set_id")
        if not isinstance(topology, ParallelTopology):
            raise ValueError("topology must be a ParallelTopology")  # noqa: TRY004
        if placement_id is not None:
            _require_nonempty_string(placement_id, "placement_id")

        normalized_parts = tuple(
            sorted(
                require_manifest_items(
                    parts,
                    "WeightPlacementManifest parts",
                    WeightPlacementPart,
                ),
                key=lambda item: item.participant_id,
            )
        )
        _validate_parts(
            normalized_parts,
            topology=topology,
            resource_id=resource_id,
            revision=revision,
            weight_generation=weight_generation,
            placement_set_id=placement_set_id,
        )

        tensors_by_id: dict[TensorId, TensorDescriptor] = {}
        collected_fragments: list[PlacementFragment] = []
        for part in normalized_parts:
            for tensor in part.tensors:
                previous = tensors_by_id.setdefault(tensor.tensor_id, tensor)
                if previous != tensor:
                    raise ValueError(
                        f"placement part tensor descriptor mismatch: {tensor.tensor_id}"
                    )
            collected_fragments.extend(part.fragments)
        tensors = tuple(sorted(tensors_by_id.values(), key=lambda item: item.tensor_id))
        fragments = tuple(
            sorted(collected_fragments, key=lambda item: item.placement_fragment_id)
        )
        _validate_fragments(
            tensors,
            fragments,
            require_complete_alias_groups=True,
        )
        _validate_complete_weight_placement(
            tensors,
            fragments,
            topology=topology,
        )
        canonical_placement_id = _logical_placement_id(
            resource_id=resource_id,
            revision=revision,
            weight_generation=weight_generation,
            placement_set_id=placement_set_id,
            topology=topology,
            tensors=tensors,
            parts=normalized_parts,
        )
        if placement_id is not None and placement_id != canonical_placement_id:
            raise ValueError("placement_id does not match canonical logical content")

        object.__setattr__(self, "resource_id", resource_id)
        object.__setattr__(self, "placement_id", canonical_placement_id)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "weight_generation", weight_generation)
        object.__setattr__(self, "placement_set_id", placement_set_id)
        object.__setattr__(self, "topology", topology)
        object.__setattr__(self, "parts", normalized_parts)
        object.__setattr__(self, "tensors", tensors)
        object.__setattr__(self, "fragments", fragments)
        object.__setattr__(self, "_digest_cache", None)

    @property
    def resource_kind(self) -> ResourceKind:
        """Identify this placement as model weight data."""

        return ResourceKind.MODEL_WEIGHT

    @property
    def digest(self) -> str:
        """Return the stable SHA-256 digest of the canonical JSON form."""

        digest = self._digest_cache
        if digest is None:
            from .serde import weight_placement_to_json

            digest = hashlib.sha256(weight_placement_to_json(self).encode()).hexdigest()
            object.__setattr__(self, "_digest_cache", digest)
        return digest

    @classmethod
    def from_fragments(
        cls,
        *,
        resource_id: ResourceId,
        revision: RevisionId,
        weight_generation: int,
        placement_set_id: PlacementSetId,
        topology: ParallelTopology,
        tensors: Sequence[TensorDescriptor],
        fragments: Sequence[PlacementFragment],
        placement_id: Optional[PlacementId] = None,
    ) -> WeightPlacementManifest:
        """Group a complete flat fragment inventory by topology participant."""

        tensor_items = require_manifest_items(
            tensors, "placement tensors", TensorDescriptor
        )
        fragment_items = require_manifest_items(
            fragments, "placement fragments", PlacementFragment
        )
        tensor_by_id = {tensor.tensor_id: tensor for tensor in tensor_items}
        if len(tensor_by_id) != len(tensor_items):
            raise ValueError("duplicate tensor_id in placement tensors")
        referenced_tensor_ids = {fragment.tensor_id for fragment in fragment_items}
        unknown_tensor_ids = sorted(referenced_tensor_ids - set(tensor_by_id))
        if unknown_tensor_ids:
            raise ValueError(f"unknown tensor_id: {unknown_tensor_ids[0]}")
        unreferenced_tensor_ids = sorted(set(tensor_by_id) - referenced_tensor_ids)
        if unreferenced_tensor_ids:
            raise ValueError(
                "global placement contains an unreferenced tensor: "
                f"{unreferenced_tensor_ids[0]}"
            )
        participant_by_rank = {
            participant.rank: participant for participant in topology.participants
        }
        fragments_by_participant: dict[ParticipantId, list[PlacementFragment]] = {
            participant.participant_id: [] for participant in topology.participants
        }
        for fragment in fragment_items:
            participant = participant_by_rank.get(fragment.rank)
            if participant is None:
                raise ValueError(
                    "placement fragment rank has no topology participant: "
                    f"{fragment.fragment_id}"
                )
            fragments_by_participant[participant.participant_id].append(fragment)

        parts: list[WeightPlacementPart] = []
        for participant in topology.participants:
            local_fragments = tuple(
                fragments_by_participant[participant.participant_id]
            )
            local_tensor_ids = {fragment.tensor_id for fragment in local_fragments}
            parts.append(
                WeightPlacementPart(
                    resource_id=resource_id,
                    revision=revision,
                    weight_generation=weight_generation,
                    placement_set_id=placement_set_id,
                    topology_id=topology.topology_id,
                    participant_id=participant.participant_id,
                    rank=participant.rank,
                    tensors=tuple(
                        tensor_by_id[tensor_id]
                        for tensor_id in sorted(local_tensor_ids)
                    ),
                    fragments=local_fragments,
                )
            )
        return cls(
            resource_id=resource_id,
            revision=revision,
            weight_generation=weight_generation,
            placement_set_id=placement_set_id,
            topology=topology,
            parts=tuple(parts),
            placement_id=placement_id,
        )


def _validate_parts(
    parts: tuple[WeightPlacementPart, ...],
    *,
    topology: ParallelTopology,
    resource_id: ResourceId,
    revision: RevisionId,
    weight_generation: int,
    placement_set_id: PlacementSetId,
) -> None:
    participant_ids = [part.participant_id for part in parts]
    if len(participant_ids) != len(set(participant_ids)):
        raise ValueError("duplicate placement participant")

    expected = {
        participant.participant_id: participant for participant in topology.participants
    }
    actual = set(participant_ids)
    missing = sorted(set(expected) - actual)
    if missing:
        raise ValueError(f"missing topology participant: {missing[0]}")
    unknown = sorted(actual - set(expected))
    if unknown:
        raise ValueError(f"unknown topology participant: {unknown[0]}")

    for part in parts:
        if part.resource_id != resource_id:
            raise ValueError("placement part resource_id differs")
        if part.revision != revision:
            raise ValueError("placement part revision differs")
        if part.weight_generation != weight_generation:
            raise ValueError("placement part weight_generation differs")
        if part.placement_set_id != placement_set_id:
            raise ValueError("placement part placement_set_id differs")
        if part.topology_id != topology.topology_id:
            raise ValueError("placement part topology_id differs")
        if part.rank != expected[part.participant_id].rank:
            raise ValueError("placement part rank differs from topology")


def _canonical_json_digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _logical_placement_id(
    *,
    resource_id: ResourceId,
    revision: RevisionId,
    weight_generation: int,
    placement_set_id: PlacementSetId,
    topology: ParallelTopology,
    tensors: Sequence[TensorDescriptor],
    parts: Sequence[WeightPlacementPart],
) -> PlacementId:
    content = {
        "schema": "complete-weight-placement",
        "resource_id": resource_id,
        "revision": revision,
        "weight_generation": weight_generation,
        "placement_set_id": placement_set_id,
        "topology": {
            "tp_size": topology.tp_size,
            "pp_size": topology.pp_size,
            "ep_size": topology.ep_size,
            "dp_size": topology.dp_size,
            "topology_id": topology.topology_id,
            "participants": [asdict(item) for item in topology.participants],
        },
        "tensors": [
            {
                "tensor_id": tensor.tensor_id,
                "global_shape": tensor.global_shape,
                "dtype": tensor.dtype,
                "itemsize": tensor.itemsize,
                "shard_dims": tensor.shard_dims,
                "layout_fingerprint": tensor.layout_fingerprint,
                "parallel_axes": [
                    _parallel_axis_identity(axis) for axis in tensor.parallel_axes
                ],
                "layer_id": tensor.layer_id,
                "expert_id": tensor.expert_id,
            }
            for tensor in tensors
        ],
        "parts": [
            {
                "participant_id": part.participant_id,
                "rank": asdict(part.rank),
                "fragments": [
                    _placement_fragment_identity(fragment)
                    for fragment in part.fragments
                ],
            }
            for part in parts
        ],
    }
    return PlacementId(f"sha256:{_canonical_json_digest(content)}")


def _parallel_axis_identity(axis: object) -> dict[str, object]:
    if isinstance(axis, SplitAxis):
        return {"semantics": "split", "kind": axis.kind, "dim": axis.dim}
    if isinstance(axis, ReplicatedAxis):
        return {"semantics": "replicated", "kind": axis.kind}
    if isinstance(axis, OwnershipAxis):
        return {"semantics": "ownership", "kind": axis.kind}
    raise ValueError("unsupported parallel axis value")


def _placement_fragment_identity(fragment: PlacementFragment) -> dict[str, object]:
    identity: dict[str, object] = {
        "placement_fragment_id": fragment.placement_fragment_id,
        "tensor_id": fragment.tensor_id,
        "global_offset": fragment.global_offset,
        "local_shape": fragment.local_shape,
        "nbytes": fragment.nbytes,
        "rank": asdict(fragment.rank),
        "aliases": fragment.aliases,
    }
    # Keep the pre-virtual-stage canonical payload byte-for-byte stable. A
    # stage participates in identity only when a framework explicitly assigns
    # one, so legacy placements keep their existing placement IDs.
    if fragment.pipeline_stage_id is not None:
        identity["pipeline_stage_id"] = fragment.pipeline_stage_id
    return identity
