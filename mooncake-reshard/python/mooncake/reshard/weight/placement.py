"""Complete logical weight placement and canonical identity."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from functools import cached_property
from typing import Any, Mapping, Optional, Sequence

from ..contracts import PlacementManifest, ResourceKind
from .part import WeightPlacementPart
from .topology import ParallelTopology
from .types import (
    ParallelRank,
    PlacementFragment,
    TensorParallelAxis,
    TensorDescriptor,
    _canonical_tensor_descriptor,
    _require_manifest_items,
    _require_nonempty_string,
    _require_sequence,
    _require_u64,
)
from .validation import (
    _validate_complete_weight_placement,
    _validate_fragments,
)


@dataclass(frozen=True, init=False)
class WeightPlacementManifest(PlacementManifest):
    """One complete address-free placement of a model-weight generation."""

    revision: str
    weight_generation: int
    placement_set_id: str
    topology: ParallelTopology
    parts: tuple[WeightPlacementPart, ...]
    tensors: tuple[TensorDescriptor, ...]
    fragments: tuple[PlacementFragment, ...]

    def __init__(
        self,
        *,
        resource_id: str,
        revision: str,
        weight_generation: int,
        placement_set_id: str,
        topology: ParallelTopology,
        parts: tuple[WeightPlacementPart, ...],
        placement_id: Optional[str] = None,
    ) -> None:
        object.__setattr__(self, "resource_id", resource_id)
        object.__setattr__(self, "placement_id", placement_id)
        object.__setattr__(self, "revision", revision)
        object.__setattr__(self, "weight_generation", weight_generation)
        object.__setattr__(self, "placement_set_id", placement_set_id)
        object.__setattr__(self, "topology", topology)
        object.__setattr__(self, "parts", parts)
        object.__setattr__(self, "tensors", ())
        object.__setattr__(self, "fragments", ())
        self.__post_init__()

    @property
    def resource_kind(self) -> ResourceKind:
        """Identify this placement as model weight data."""

        return ResourceKind.MODEL_WEIGHT

    @property
    def model_id(self) -> str:
        """Return the weight-specific name for the common resource ID."""

        return self.resource_id

    def __post_init__(self) -> None:
        super().__post_init__()
        _require_nonempty_string(self.revision, "revision")
        _require_u64(self.weight_generation, "weight_generation")
        _require_nonempty_string(self.placement_set_id, "placement_set_id")
        if not isinstance(self.topology, ParallelTopology):
            raise ValueError("topology must be a ParallelTopology")

        parts = _require_manifest_items(
            self.parts,
            "WeightPlacementManifest parts",
            WeightPlacementPart,
        )
        parts = tuple(sorted(parts, key=lambda item: item.participant_id))
        object.__setattr__(self, "parts", parts)
        self._validate_parts(parts)

        tensors_by_id: dict[str, TensorDescriptor] = {}
        fragments = []
        for part in parts:
            for tensor in part.tensors:
                tensor = _canonical_tensor_descriptor(tensor)
                previous = tensors_by_id.setdefault(tensor.tensor_id, tensor)
                if previous != tensor:
                    raise ValueError(
                        f"placement part tensor descriptor mismatch: {tensor.tensor_id}"
                    )
            fragments.extend(part.fragments)
        tensors = tuple(sorted(tensors_by_id.values(), key=lambda item: item.tensor_id))
        fragments = tuple(
            sorted(fragments, key=lambda item: item.placement_fragment_id)
        )
        object.__setattr__(self, "tensors", tensors)
        object.__setattr__(self, "fragments", fragments)

        _validate_fragments(tensors, fragments)
        _validate_complete_weight_placement(
            tensors,
            fragments,
            selected_dp_ranks=frozenset(
                participant.rank.dp for participant in self.topology.participants
            ),
        )

        canonical_placement_id = _logical_placement_id(
            resource_id=self.resource_id,
            revision=self.revision,
            weight_generation=self.weight_generation,
            placement_set_id=self.placement_set_id,
            topology=self.topology,
            tensors=tensors,
            parts=parts,
        )
        if self.placement_id is None:
            object.__setattr__(self, "placement_id", canonical_placement_id)
        elif self.placement_id != canonical_placement_id:
            raise ValueError("placement_id does not match canonical logical content")

    def _validate_parts(self, parts: tuple[WeightPlacementPart, ...]) -> None:
        participant_ids = [part.participant_id for part in parts]
        if len(participant_ids) != len(set(participant_ids)):
            raise ValueError("duplicate placement participant")

        expected = {
            participant.participant_id: participant
            for participant in self.topology.participants
        }
        actual = set(participant_ids)
        missing = sorted(set(expected) - actual)
        if missing:
            raise ValueError(f"missing topology participant: {missing[0]}")
        unknown = sorted(actual - set(expected))
        if unknown:
            raise ValueError(f"unknown topology participant: {unknown[0]}")

        for part in parts:
            if part.resource_id != self.resource_id:
                raise ValueError("placement part resource_id differs")
            if part.revision != self.revision:
                raise ValueError("placement part revision differs")
            if part.weight_generation != self.weight_generation:
                raise ValueError("placement part weight_generation differs")
            if part.placement_set_id != self.placement_set_id:
                raise ValueError("placement part placement_set_id differs")
            if part.topology_id != self.topology.topology_id:
                raise ValueError("placement part topology_id differs")
            if part.rank != expected[part.participant_id].rank:
                raise ValueError("placement part rank differs from topology")

    @cached_property
    def digest(self) -> str:
        """Return the stable SHA-256 digest of the canonical JSON form."""

        return hashlib.sha256(self.to_json().encode()).hexdigest()

    def to_json(self) -> str:
        """Serialize the complete placement without physical locations."""

        payload = {
            "resource_kind": self.resource_kind.value,
            "resource_id": self.resource_id,
            "revision": self.revision,
            "weight_generation": self.weight_generation,
            "placement_set_id": self.placement_set_id,
            "placement_id": self.placement_id,
            "topology": self.topology.to_dict(),
            "tensors": [asdict(tensor) for tensor in self.tensors],
            "parts": [
                {
                    "participant_id": part.participant_id,
                    "rank": asdict(part.rank),
                    "fragments": [asdict(fragment) for fragment in part.fragments],
                }
                for part in self.parts
            ],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, value: str) -> WeightPlacementManifest:
        """Parse a strict complete placement manifest."""

        manifest = _require_exact_fields(
            _load_json_object(value, "placement manifest"),
            frozenset(
                {
                    "resource_kind",
                    "resource_id",
                    "revision",
                    "weight_generation",
                    "placement_set_id",
                    "placement_id",
                    "topology",
                    "tensors",
                    "parts",
                }
            ),
            "placement manifest",
        )
        if manifest["resource_kind"] != ResourceKind.MODEL_WEIGHT.value:
            raise ValueError("placement manifest resource_kind must be model_weight")
        topology = ParallelTopology.from_dict(manifest["topology"])

        tensors = tuple(
            _tensor_from_json(item, index)
            for index, item in enumerate(
                _require_sequence(manifest["tensors"], "placement tensors")
            )
        )
        tensor_by_id = {tensor.tensor_id: tensor for tensor in tensors}
        if len(tensor_by_id) != len(tensors):
            raise ValueError("duplicate tensor_id in placement JSON")

        parts = []
        for index, item in enumerate(
            _require_sequence(manifest["parts"], "placement parts")
        ):
            part = _require_exact_fields(
                item,
                frozenset({"participant_id", "rank", "fragments"}),
                f"placement part {index}",
            )
            rank = _rank_from_json(part["rank"], f"placement part rank {index}")
            fragments = tuple(
                _fragment_from_json(fragment, fragment_index)
                for fragment_index, fragment in enumerate(
                    _require_sequence(part["fragments"], "placement part fragments")
                )
            )
            local_tensor_ids = {fragment.tensor_id for fragment in fragments}
            unknown = sorted(local_tensor_ids - set(tensor_by_id))
            if unknown:
                raise ValueError(f"unknown tensor_id in placement part: {unknown[0]}")
            parts.append(
                WeightPlacementPart(
                    resource_id=manifest["resource_id"],
                    revision=manifest["revision"],
                    weight_generation=manifest["weight_generation"],
                    placement_set_id=manifest["placement_set_id"],
                    topology_id=topology.topology_id,
                    participant_id=part["participant_id"],
                    rank=rank,
                    tensors=tuple(
                        tensor_by_id[tensor_id]
                        for tensor_id in sorted(local_tensor_ids)
                    ),
                    fragments=fragments,
                )
            )
        return cls(
            resource_id=manifest["resource_id"],
            revision=manifest["revision"],
            weight_generation=manifest["weight_generation"],
            placement_set_id=manifest["placement_set_id"],
            placement_id=manifest["placement_id"],
            topology=topology,
            parts=tuple(parts),
        )

    @classmethod
    def from_runtime_inventories(
        cls,
        inventories: Sequence[Any],
        *,
        topology: ParallelTopology,
        placement_id: Optional[str] = None,
    ) -> WeightPlacementManifest:
        """Collect local runtime inventories into one complete placement."""

        parts = tuple(
            WeightPlacementPart.from_runtime_inventory(inventory)
            for inventory in _require_sequence(
                inventories, "placement runtime inventories"
            )
        )
        if not parts:
            raise ValueError("placement runtime inventories must not be empty")
        first = parts[0]
        return cls(
            resource_id=first.resource_id,
            revision=first.revision,
            weight_generation=first.weight_generation,
            placement_set_id=first.placement_set_id,
            placement_id=placement_id,
            topology=topology,
            parts=parts,
        )

    @classmethod
    def from_fragments(
        cls,
        *,
        resource_id: str,
        revision: str,
        weight_generation: int,
        placement_set_id: str,
        topology: ParallelTopology,
        tensors: Sequence[TensorDescriptor],
        fragments: Sequence[PlacementFragment],
        placement_id: Optional[str] = None,
    ) -> WeightPlacementManifest:
        """Group a complete flat fragment inventory by topology participant."""

        tensor_items = tuple(
            _canonical_tensor_descriptor(tensor)
            for tensor in _require_manifest_items(
                tensors, "placement tensors", TensorDescriptor
            )
        )
        fragment_items = _require_manifest_items(
            fragments, "placement fragments", PlacementFragment
        )
        tensor_by_id = {tensor.tensor_id: tensor for tensor in tensor_items}
        if len(tensor_by_id) != len(tensor_items):
            raise ValueError("duplicate tensor_id in placement tensors")
        participant_by_rank = {
            participant.rank: participant for participant in topology.participants
        }
        fragments_by_participant: dict[str, list[PlacementFragment]] = {
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

        parts = []
        for participant in topology.participants:
            local_fragments = tuple(
                fragments_by_participant[participant.participant_id]
            )
            local_tensor_ids = {fragment.tensor_id for fragment in local_fragments}
            unknown = sorted(local_tensor_ids - set(tensor_by_id))
            if unknown:
                raise ValueError(f"unknown tensor_id: {unknown[0]}")
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


def _tensor_from_json(value: Any, index: int) -> TensorDescriptor:
    tensor = _require_exact_fields(
        value,
        frozenset(
            {
                "tensor_id",
                "global_shape",
                "dtype",
                "itemsize",
                "partition_dim",
                "layer_id",
                "expert_id",
                "layout_fingerprint",
                "shard_dims",
                "parallel_axes",
            }
        ),
        f"placement tensor {index}",
    )
    return _canonical_tensor_descriptor(
        TensorDescriptor(
            tensor_id=tensor["tensor_id"],
            global_shape=tensor["global_shape"],
            dtype=tensor["dtype"],
            itemsize=tensor["itemsize"],
            partition_dim=tensor["partition_dim"],
            layer_id=tensor["layer_id"],
            expert_id=tensor["expert_id"],
            layout_fingerprint=tensor["layout_fingerprint"],
            shard_dims=tensor["shard_dims"],
            parallel_axes=tuple(
                _tensor_parallel_axis_from_json(axis, axis_index)
                for axis_index, axis in enumerate(
                    _require_sequence(
                        tensor["parallel_axes"],
                        "placement tensor parallel_axes",
                    )
                )
            ),
        )
    )


def _tensor_parallel_axis_from_json(value: Any, index: int) -> TensorParallelAxis:
    axis = _require_exact_fields(
        value,
        frozenset({"kind", "split_dim"}),
        f"placement tensor parallel axis {index}",
    )
    return TensorParallelAxis(
        kind=axis["kind"],
        split_dim=axis["split_dim"],
    )


def _fragment_from_json(value: Any, index: int) -> PlacementFragment:
    fragment = _require_exact_fields(
        value,
        frozenset(
            {
                "placement_fragment_id",
                "tensor_id",
                "global_offset",
                "local_shape",
                "nbytes",
                "rank",
                "aliases",
            }
        ),
        f"placement fragment {index}",
    )
    return PlacementFragment(
        placement_fragment_id=fragment["placement_fragment_id"],
        tensor_id=fragment["tensor_id"],
        global_offset=fragment["global_offset"],
        local_shape=fragment["local_shape"],
        nbytes=fragment["nbytes"],
        rank=_rank_from_json(fragment["rank"], f"placement rank {index}"),
        aliases=fragment["aliases"],
    )


def _rank_from_json(value: Any, label: str) -> ParallelRank:
    rank = _require_exact_fields(
        value,
        frozenset({"dp", "tp", "pp", "ep"}),
        label,
    )
    return ParallelRank(**rank)


def _canonical_json_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _logical_placement_id(
    *,
    resource_id: str,
    revision: str,
    weight_generation: int,
    placement_set_id: str,
    topology: ParallelTopology,
    tensors: Sequence[TensorDescriptor],
    parts: Sequence[WeightPlacementPart],
) -> str:
    content = {
        "schema": "complete-weight-placement",
        "resource_id": resource_id,
        "revision": revision,
        "weight_generation": weight_generation,
        "placement_set_id": placement_set_id,
        "topology": topology.to_dict(),
        "tensors": [asdict(tensor) for tensor in tensors],
        "parts": [
            {
                "participant_id": part.participant_id,
                "rank": asdict(part.rank),
                "fragments": [asdict(fragment) for fragment in part.fragments],
            }
            for part in parts
        ],
    }
    return f"sha256:{_canonical_json_digest(content)}"


def _require_exact_fields(
    value: Any, expected: frozenset[str], label: str
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f"{label} schema fields do not match contract")
    return value


def _load_json_object(value: str, label: str) -> Mapping[str, Any]:
    def reject_constant(constant: str) -> None:
        raise ValueError(f"non-finite JSON number is unsupported: {constant}")

    def reject_duplicate_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON field: {key}")
            result[key] = item
        return result

    try:
        raw = json.loads(
            value,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_fields,
        )
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"{label} is not valid JSON") from error
    if not isinstance(raw, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    return raw


SourcePlacementManifest = PlacementManifest
TargetPlacementManifest = PlacementManifest
