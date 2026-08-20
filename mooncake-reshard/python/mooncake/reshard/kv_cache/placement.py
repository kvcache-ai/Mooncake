"""Complete address-free KV-cache placement and canonical identity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field

from ..contracts import (
    ParticipantId,
    PlacementId,
    PlacementSetId,
    ResourceId,
    ResourceKind,
    RevisionId,
)
from .part import KVCachePlacementPart
from .topology import KVCacheTopology, KVCacheTopologyParticipant
from .types import (
    KVCacheDescriptor,
    descriptor_identity,
    require_manifest_items,
    require_nonempty_string,
)


@dataclass(frozen=True, init=False)
class KVCachePlacementManifest:
    """One complete logical KV-cache buffer placement."""

    resource_id: ResourceId
    placement_id: PlacementId
    revision: RevisionId
    placement_set_id: PlacementSetId
    topology: KVCacheTopology
    descriptor: KVCacheDescriptor
    parts: tuple[KVCachePlacementPart, ...]
    _digest_cache: str | None = field(init=False, repr=False, compare=False)

    def __init__(
        self,
        *,
        resource_id: ResourceId,
        revision: RevisionId,
        placement_set_id: PlacementSetId,
        topology: KVCacheTopology,
        descriptor: KVCacheDescriptor,
        parts: tuple[KVCachePlacementPart, ...],
        placement_id: PlacementId | None = None,
    ) -> None:
        for value, name in (
            (resource_id, "resource_id"),
            (revision, "revision"),
            (placement_set_id, "placement_set_id"),
        ):
            require_nonempty_string(value, name)
        if not isinstance(topology, KVCacheTopology):
            raise ValueError("topology must be a KVCacheTopology")  # noqa: TRY004
        if not isinstance(descriptor, KVCacheDescriptor):
            raise ValueError("descriptor must be a KVCacheDescriptor")  # noqa: TRY004
        normalized_parts = tuple(
            sorted(
                require_manifest_items(
                    parts,
                    "KVCachePlacementManifest parts",
                    KVCachePlacementPart,
                ),
                key=lambda item: item.participant_id,
            )
        )
        _validate_parts(
            normalized_parts,
            resource_id=resource_id,
            revision=revision,
            placement_set_id=placement_set_id,
            topology=topology,
            descriptor=descriptor,
        )
        _validate_logical_coverage(normalized_parts, topology, descriptor)
        canonical_id = _logical_placement_id(
            resource_id=resource_id,
            revision=revision,
            placement_set_id=placement_set_id,
            topology=topology,
            descriptor=descriptor,
            parts=normalized_parts,
        )
        if placement_id is not None and placement_id != canonical_id:
            raise ValueError("placement_id does not match canonical logical content")
        object.__setattr__(self, "resource_id", resource_id)
        object.__setattr__(self, "placement_id", canonical_id)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "placement_set_id", placement_set_id)
        object.__setattr__(self, "topology", topology)
        object.__setattr__(self, "descriptor", descriptor)
        object.__setattr__(self, "parts", normalized_parts)
        object.__setattr__(self, "_digest_cache", None)

    @property
    def resource_kind(self) -> ResourceKind:
        return ResourceKind.KV_CACHE

    @property
    def digest(self) -> str:
        digest = self._digest_cache
        if digest is None:
            content = _placement_content(
                resource_id=self.resource_id,
                revision=self.revision,
                placement_set_id=self.placement_set_id,
                topology=self.topology,
                descriptor=self.descriptor,
                parts=self.parts,
            )
            content["placement_id"] = self.placement_id
            digest = _canonical_json_digest(content)
            object.__setattr__(self, "_digest_cache", digest)
        return digest

    @property
    def dp_ranks(self) -> tuple[int, ...]:
        return tuple(sorted({part.rank.dp for part in self.parts}))

    def part(self, participant_id: ParticipantId) -> KVCachePlacementPart:
        for part in self.parts:
            if part.participant_id == participant_id:
                return part
        raise ValueError(f"unknown KV-cache participant: {participant_id}")


def assemble_kv_cache_placement(
    parts: Sequence[KVCachePlacementPart],
    *,
    dp_size: int,
    pp_size: int,
    tp_size: int,
) -> KVCachePlacementManifest:
    """Assemble any server's selected participants into one placement.

    The selected participants may contain one or more complete DP replicas.
    Source/target and Prefill/Decode roles are deliberately not part of this
    contract; framework discovery code decides which participants to provide.
    """

    normalized = require_manifest_items(
        parts,
        "KV-cache placement parts",
        KVCachePlacementPart,
    )
    if not normalized:
        raise ValueError("KV-cache placement must contain at least one part")
    first = normalized[0]
    topology = KVCacheTopology(
        dp_size=dp_size,
        pp_size=pp_size,
        tp_size=tp_size,
        participants=tuple(
            KVCacheTopologyParticipant(part.participant_id, part.rank)
            for part in normalized
        ),
    )
    return KVCachePlacementManifest(
        resource_id=first.resource_id,
        revision=first.revision,
        placement_set_id=first.placement_set_id,
        topology=topology,
        descriptor=first.descriptor,
        parts=normalized,
    )


def _validate_parts(
    parts: tuple[KVCachePlacementPart, ...],
    *,
    resource_id: ResourceId,
    revision: RevisionId,
    placement_set_id: PlacementSetId,
    topology: KVCacheTopology,
    descriptor: KVCacheDescriptor,
) -> None:
    participant_ids = [part.participant_id for part in parts]
    if len(participant_ids) != len(set(participant_ids)):
        raise ValueError("duplicate KV-cache placement participant")
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
        checks = {
            "resource_id": resource_id,
            "revision": revision,
            "placement_set_id": placement_set_id,
            "topology_id": topology.topology_id,
            "descriptor": descriptor,
            "rank": expected[part.participant_id].rank,
        }
        for name, expected_value in checks.items():
            if getattr(part, name) != expected_value:
                raise ValueError(f"placement part {name} differs")


def _validate_logical_coverage(
    parts: tuple[KVCachePlacementPart, ...],
    topology: KVCacheTopology,
    descriptor: KVCacheDescriptor,
) -> None:
    selected_dp_ranks = sorted({item.rank.dp for item in topology.participants})
    for dp_rank in selected_dp_ranks:
        dp_parts = tuple(part for part in parts if part.rank.dp == dp_rank)
        for layer_id in descriptor.global_layer_ids:
            for head in range(descriptor.total_kv_heads):
                owners = [
                    part
                    for part in dp_parts
                    if layer_id in part.layer_ids
                    and part.head_start <= head < part.head_start + part.head_count
                ]
                if not owners:
                    raise ValueError(
                        "KV-cache placement misses "
                        f"dp={dp_rank} layer={layer_id} head={head}"
                    )
                if len(owners) > 1:
                    ordinals = {part.replica_ordinal for part in owners}
                    counts = {part.replica_count for part in owners}
                    if counts != {len(owners)} or ordinals != set(range(len(owners))):
                        raise ValueError(
                            "overlapping KV heads must be an exact declared replica set"
                        )
                elif owners[0].replica_count != 1:
                    raise ValueError("incomplete declared KV-head replica set")


def _placement_content(
    *,
    resource_id: ResourceId,
    revision: RevisionId,
    placement_set_id: PlacementSetId,
    topology: KVCacheTopology,
    descriptor: KVCacheDescriptor,
    parts: tuple[KVCachePlacementPart, ...],
) -> dict[str, object]:
    return {
        "schema": "complete-kv-cache-placement",
        "resource_kind": ResourceKind.KV_CACHE.value,
        "resource_id": resource_id,
        "revision": revision,
        "placement_set_id": placement_set_id,
        "topology": {
            "dp_size": topology.dp_size,
            "pp_size": topology.pp_size,
            "tp_size": topology.tp_size,
            "topology_id": topology.topology_id,
            "participants": [asdict(item) for item in topology.participants],
        },
        "descriptor": descriptor_identity(descriptor),
        "parts": [
            {
                "resource_id": part.resource_id,
                "revision": part.revision,
                "placement_set_id": part.placement_set_id,
                "topology_id": part.topology_id,
                "participant_id": part.participant_id,
                "rank": asdict(part.rank),
                "descriptor": descriptor_identity(part.descriptor),
                "layer_ids": part.layer_ids,
                "head_start": part.head_start,
                "head_count": part.head_count,
                "replica_ordinal": part.replica_ordinal,
                "replica_count": part.replica_count,
            }
            for part in parts
        ],
    }


def _canonical_json_digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _logical_placement_id(
    *,
    resource_id: ResourceId,
    revision: RevisionId,
    placement_set_id: PlacementSetId,
    topology: KVCacheTopology,
    descriptor: KVCacheDescriptor,
    parts: tuple[KVCachePlacementPart, ...],
) -> PlacementId:
    return PlacementId(
        "sha256:"
        + _canonical_json_digest(
            _placement_content(
                resource_id=resource_id,
                revision=revision,
                placement_set_id=placement_set_id,
                topology=topology,
                descriptor=descriptor,
                parts=parts,
            )
        )
    )


__all__ = ["KVCachePlacementManifest", "assemble_kv_cache_placement"]
