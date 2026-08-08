"""Global parallel topology for one model-weight placement."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Optional

from .types import (
    ParallelRank,
    _read_field,
    _require_integer,
    _require_manifest_items,
    _require_nonempty_string,
)


@dataclass(frozen=True)
class TopologyParticipant:
    """One runtime participant and its framework-defined parallel coordinates."""

    participant_id: str
    rank: ParallelRank

    def __post_init__(self) -> None:
        _require_nonempty_string(self.participant_id, "participant_id")
        if not isinstance(self.rank, ParallelRank):
            raise ValueError("topology participant rank must be a ParallelRank")


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
    topology_id: str

    def __init__(
        self,
        *,
        tp_size: int,
        pp_size: int,
        ep_size: int,
        dp_size: int,
        participants: tuple[TopologyParticipant, ...],
        topology_id: Optional[str] = None,
    ) -> None:
        object.__setattr__(self, "tp_size", tp_size)
        object.__setattr__(self, "pp_size", pp_size)
        object.__setattr__(self, "ep_size", ep_size)
        object.__setattr__(self, "dp_size", dp_size)
        object.__setattr__(self, "participants", participants)
        object.__setattr__(self, "topology_id", topology_id)
        self.__post_init__()

    def __post_init__(self) -> None:
        for name in ("tp_size", "pp_size", "ep_size", "dp_size"):
            _require_integer(getattr(self, name), name, minimum=1)
        participants = _require_manifest_items(
            self.participants,
            "ParallelTopology participants",
            TopologyParticipant,
        )
        if not participants:
            raise ValueError("parallel topology participants must not be empty")
        participants = tuple(sorted(participants, key=lambda item: item.participant_id))
        object.__setattr__(self, "participants", participants)

        participant_ids = [item.participant_id for item in participants]
        if len(participant_ids) != len(set(participant_ids)):
            raise ValueError("duplicate topology participant_id")
        ranks = [item.rank for item in participants]
        if len(ranks) != len(set(ranks)):
            raise ValueError("duplicate topology parallel rank")

        for participant in participants:
            for axis in ("tp", "pp", "ep", "dp"):
                value = getattr(participant.rank, axis)
                size = getattr(self, f"{axis}_size")
                if value >= size:
                    raise ValueError(
                        f"{axis} rank is out of range for participant "
                        f"{participant.participant_id}"
                    )

        canonical_id = _topology_id(
            tp_size=self.tp_size,
            pp_size=self.pp_size,
            ep_size=self.ep_size,
            dp_size=self.dp_size,
            participants=participants,
        )
        if self.topology_id is None:
            object.__setattr__(self, "topology_id", canonical_id)
        elif self.topology_id != canonical_id:
            raise ValueError("topology_id does not match canonical topology")

    @property
    def world_size(self) -> int:
        """Return the number of actual runtime participants."""

        return len(self.participants)

    def participant(self, participant_id: str) -> TopologyParticipant:
        """Return one declared participant or reject an unknown identifier."""

        for participant in self.participants:
            if participant.participant_id == participant_id:
                return participant
        raise ValueError(f"unknown topology participant: {participant_id}")

    def to_dict(self) -> dict[str, Any]:
        """Return the strict serializable topology form."""

        return {
            "tp_size": self.tp_size,
            "pp_size": self.pp_size,
            "ep_size": self.ep_size,
            "dp_size": self.dp_size,
            "topology_id": self.topology_id,
            "participants": [asdict(item) for item in self.participants],
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ParallelTopology:
        """Parse a strict topology object."""

        expected = {
            "tp_size",
            "pp_size",
            "ep_size",
            "dp_size",
            "topology_id",
            "participants",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("parallel topology schema fields do not match contract")
        participants = []
        raw_participants = _read_field(value, "participants")
        if isinstance(raw_participants, (str, bytes, bytearray)):
            raise ValueError("parallel topology participants must be a sequence")
        try:
            iterator = iter(raw_participants)
        except TypeError as error:
            raise ValueError(
                "parallel topology participants must be a sequence"
            ) from error
        for index, item in enumerate(iterator):
            if not isinstance(item, Mapping) or set(item) != {"participant_id", "rank"}:
                raise ValueError(
                    f"parallel topology participant {index} schema fields do not "
                    "match contract"
                )
            rank = item["rank"]
            if not isinstance(rank, Mapping) or set(rank) != {"dp", "tp", "pp", "ep"}:
                raise ValueError(
                    f"parallel topology rank {index} schema fields do not match "
                    "contract"
                )
            participants.append(
                TopologyParticipant(
                    participant_id=item["participant_id"],
                    rank=ParallelRank(**rank),
                )
            )
        return cls(
            tp_size=value["tp_size"],
            pp_size=value["pp_size"],
            ep_size=value["ep_size"],
            dp_size=value["dp_size"],
            participants=tuple(participants),
            topology_id=value["topology_id"],
        )


def _topology_id(
    *,
    tp_size: int,
    pp_size: int,
    ep_size: int,
    dp_size: int,
    participants: tuple[TopologyParticipant, ...],
) -> str:
    content = {
        "schema": "weight-parallel-topology",
        "tp_size": tp_size,
        "pp_size": pp_size,
        "ep_size": ep_size,
        "dp_size": dp_size,
        "participants": [asdict(item) for item in participants],
    }
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


__all__ = ["ParallelTopology", "TopologyParticipant"]
