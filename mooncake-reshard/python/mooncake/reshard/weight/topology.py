"""Global parallel topology for one model-weight placement."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Optional

from ..contracts import ParticipantId, TopologyId
from .types import (
    ParallelRank,
    _require_integer,
    _require_nonempty_string,
    require_manifest_items,
)


@dataclass(frozen=True)
class TopologyParticipant:
    """One runtime participant and its framework-defined parallel coordinates."""

    participant_id: ParticipantId
    rank: ParallelRank

    def __post_init__(self) -> None:
        _require_nonempty_string(self.participant_id, "participant_id")
        if not isinstance(self.rank, ParallelRank):
            raise ValueError(  # noqa: TRY004
                "topology participant rank must be a ParallelRank"
            )


@dataclass(frozen=True, init=False)
class ParallelTopology:
    """Complete TP/PP/EP/DP dimensions and selected participant mapping.

    ``world_size`` is the number of explicit participants. It is deliberately
    not inferred from the product of the four axis sizes because frameworks may
    map TP and EP onto the same workers, and a placement may select one DP
    replica while retaining the source runtime's declared ``dp_size``.
    """

    tp_size: int
    pp_size: int
    ep_size: int
    dp_size: int
    participants: tuple[TopologyParticipant, ...]
    topology_id: TopologyId

    def __init__(
        self,
        *,
        tp_size: int,
        pp_size: int,
        ep_size: int,
        dp_size: int,
        participants: tuple[TopologyParticipant, ...],
        topology_id: Optional[TopologyId] = None,
    ) -> None:
        for value, name in (
            (tp_size, "tp_size"),
            (pp_size, "pp_size"),
            (ep_size, "ep_size"),
            (dp_size, "dp_size"),
        ):
            _require_integer(value, name, minimum=1)
        normalized_participants = require_manifest_items(
            participants,
            "ParallelTopology participants",
            TopologyParticipant,
        )
        if not normalized_participants:
            raise ValueError("parallel topology participants must not be empty")
        normalized_participants = tuple(
            sorted(normalized_participants, key=lambda item: item.participant_id)
        )

        participant_ids = [item.participant_id for item in normalized_participants]
        if len(participant_ids) != len(set(participant_ids)):
            raise ValueError("duplicate topology participant_id")
        ranks = [item.rank for item in normalized_participants]
        if len(ranks) != len(set(ranks)):
            raise ValueError("duplicate topology parallel rank")

        axis_sizes = {
            "tp": tp_size,
            "pp": pp_size,
            "ep": ep_size,
            "dp": dp_size,
        }
        for participant in normalized_participants:
            for axis, size in axis_sizes.items():
                value = getattr(participant.rank, axis)
                if value >= size:
                    raise ValueError(
                        f"{axis} rank is out of range for participant "
                        f"{participant.participant_id}"
                    )

        canonical_id = _topology_id(
            tp_size=tp_size,
            pp_size=pp_size,
            ep_size=ep_size,
            dp_size=dp_size,
            participants=normalized_participants,
        )
        if topology_id is not None and topology_id != canonical_id:
            raise ValueError("topology_id does not match canonical topology")
        object.__setattr__(self, "tp_size", tp_size)
        object.__setattr__(self, "pp_size", pp_size)
        object.__setattr__(self, "ep_size", ep_size)
        object.__setattr__(self, "dp_size", dp_size)
        object.__setattr__(self, "participants", normalized_participants)
        object.__setattr__(self, "topology_id", canonical_id)

    @property
    def world_size(self) -> int:
        """Return the number of actual runtime participants."""

        return len(self.participants)

    def participant(self, participant_id: ParticipantId) -> TopologyParticipant:
        """Return one declared participant or reject an unknown identifier."""

        for participant in self.participants:
            if participant.participant_id == participant_id:
                return participant
        raise ValueError(f"unknown topology participant: {participant_id}")


def _topology_id(
    *,
    tp_size: int,
    pp_size: int,
    ep_size: int,
    dp_size: int,
    participants: tuple[TopologyParticipant, ...],
) -> TopologyId:
    content = {
        "schema": "weight-parallel-topology",
        "tp_size": tp_size,
        "pp_size": pp_size,
        "ep_size": ep_size,
        "dp_size": dp_size,
        "participants": [asdict(item) for item in participants],
    }
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    return TopologyId(f"sha256:{hashlib.sha256(encoded).hexdigest()}")


__all__ = ["ParallelTopology", "TopologyParticipant"]
