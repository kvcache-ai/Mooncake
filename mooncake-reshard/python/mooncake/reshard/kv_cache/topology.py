"""Complete participant topology for one KV-cache placement."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass

from ..contracts import ParticipantId, TopologyId
from .types import (
    KVCacheRank,
    require_integer,
    require_manifest_items,
    require_nonempty_string,
)


@dataclass(frozen=True)
class KVCacheTopologyParticipant:
    participant_id: ParticipantId
    rank: KVCacheRank

    def __post_init__(self) -> None:
        require_nonempty_string(self.participant_id, "participant_id")
        if not isinstance(self.rank, KVCacheRank):
            raise ValueError("topology rank must be a KVCacheRank")  # noqa: TRY004


@dataclass(frozen=True, init=False)
class KVCacheTopology:
    dp_size: int
    pp_size: int
    tp_size: int
    participants: tuple[KVCacheTopologyParticipant, ...]
    topology_id: TopologyId

    def __init__(
        self,
        *,
        dp_size: int,
        pp_size: int,
        tp_size: int,
        participants: tuple[KVCacheTopologyParticipant, ...],
        topology_id: TopologyId | None = None,
    ) -> None:
        for value, name in (
            (dp_size, "dp_size"),
            (pp_size, "pp_size"),
            (tp_size, "tp_size"),
        ):
            require_integer(value, name, minimum=1)
        normalized = tuple(
            sorted(
                require_manifest_items(
                    participants,
                    "KVCacheTopology participants",
                    KVCacheTopologyParticipant,
                ),
                key=lambda item: item.participant_id,
            )
        )
        if not normalized:
            raise ValueError("KV-cache topology participants must not be empty")
        participant_ids = [item.participant_id for item in normalized]
        if len(participant_ids) != len(set(participant_ids)):
            raise ValueError("duplicate topology participant_id")
        ranks = [item.rank for item in normalized]
        if len(ranks) != len(set(ranks)):
            raise ValueError("duplicate topology parallel rank")
        for participant in normalized:
            for axis, size in (
                ("dp", dp_size),
                ("pp", pp_size),
                ("tp", tp_size),
            ):
                if getattr(participant.rank, axis) >= size:
                    raise ValueError(
                        f"{axis} rank is out of range for participant "
                        f"{participant.participant_id}"
                    )
        canonical = _topology_id(
            dp_size=dp_size,
            pp_size=pp_size,
            tp_size=tp_size,
            participants=normalized,
        )
        if topology_id is not None and topology_id != canonical:
            raise ValueError("topology_id does not match canonical topology")
        object.__setattr__(self, "dp_size", dp_size)
        object.__setattr__(self, "pp_size", pp_size)
        object.__setattr__(self, "tp_size", tp_size)
        object.__setattr__(self, "participants", normalized)
        object.__setattr__(self, "topology_id", canonical)

    @property
    def world_size(self) -> int:
        return len(self.participants)

    def participant(
        self, participant_id: ParticipantId
    ) -> KVCacheTopologyParticipant:
        for participant in self.participants:
            if participant.participant_id == participant_id:
                return participant
        raise ValueError(f"unknown topology participant: {participant_id}")


def _topology_id(
    *,
    dp_size: int,
    pp_size: int,
    tp_size: int,
    participants: tuple[KVCacheTopologyParticipant, ...],
) -> TopologyId:
    content = {
        "schema": "kv-cache-topology",
        "dp_size": dp_size,
        "pp_size": pp_size,
        "tp_size": tp_size,
        "participants": [asdict(item) for item in participants],
    }
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    return TopologyId(f"sha256:{hashlib.sha256(encoded).hexdigest()}")


__all__ = ["KVCacheTopology", "KVCacheTopologyParticipant"]
